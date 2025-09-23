import asyncio
import datetime
import io
import time
from asyncio import Task
from http import HTTPStatus
from urllib.parse import urlencode

import httpx
import pandas as pd

from config.settings import app_settings
from models import KeywordRank, Product, Region
from pretty_print import StepTimer, err, info, ok, warn
from spreadsheets import get_first_row_data

HTTP_MAX_RETRIES = 3
HTTP_TIMEOUT_SEC = 30
CONCURRENCY = 10

OTHER_SALES_COLUMN = "other sales"
TARGET_SEARCH_TERM_COLUMN = "target search term:"
EXCLUDED_PREFIXES = ("kw", "search")


def load_weekly_rank_data(products: list[Product], target_date: str) -> None:
    """
    Fetch weekly organic rank data from Helium (Pacvue API) and update products.

    Args:
        products (list[Product]): Products to update. Each must have `product.helium.id`
            (non-zero) and predefined keywords in `product.helium.ranks[*].word`.
        target_date (str): Tuesday in format '%d/%b/%y' (e.g., '07/Jan/25'). The
            function computes a 7-day window (Mon–Sun) ending the week before this date.

    Returns:
        None

    Raises:
        RuntimeError: When no ranks were updated for any product (no matches/empty data).
        ValueError: For bad/empty API responses that make processing impossible.
        httpx.HTTPError: For non-retriable HTTP failures (propagated).
    """
    start_ts, end_ts = _week_window_from_target(target_date)
    with StepTimer(f"[H10] Fetch weekly ranks for {len(products)} product(s)"):
        updated = asyncio.run(_fetch_organic_ranks_for_products(products, start_ts, end_ts))
        if updated == 0:
            raise RuntimeError("[H10] No rank updates across all products")
        ok(f"[H10] Updated ranks for {updated} product(s)")


def update_keywords(products: list[Product], region: Region) -> None:
    """
    Read first-row keyword data from Sheets and attach keywords to products.

    Args:
        products (list[Product]): Products to update with keywords in-memory.
        region (Region): Region tab to read.

    Returns:
        None

    Raises:
        ValueError: If sheet data has no keyword rows for all products.
    """
    with StepTimer(f"[H10] Update keywords from Sheets ({region.value})"):
        sheet_data = get_first_row_data(products, region)
        keywords = _extract_keywords(sheet_data)
        _update_product_keywords(products, keywords)

        total_assigned = sum(1 for _, kws in zip(products, keywords) if kws)
        if total_assigned == 0:
            raise ValueError("[H10] No keywords found in the sheet")
        ok(f"[H10] Assigned keywords for {total_assigned} product(s)")


def _week_window_from_target(target_date: str) -> tuple[int, int]:
    dt = datetime.datetime.strptime(target_date, "%d/%b/%y").date()
    weekday = dt.weekday()
    end = dt - datetime.timedelta(days=weekday + 1)
    start = end - datetime.timedelta(days=6)
    start_ts = int(time.mktime(start.timetuple()))
    end_ts = int(time.mktime(end.timetuple()))
    info(f"[H10] Window: {start:%d/%b/%y}–{end:%d/%b/%y}")
    return start_ts, end_ts


async def _fetch_organic_ranks_for_products(products: list[Product], _start: int, _end: int) -> int:
    settings = app_settings.get()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/123.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {settings.helium.auth_token.get_secret_value()}",
        "X-Pacvue-Token": f"Bearer {settings.helium.pacvue_access_token.get_secret_value()}",
    }
    account_id = settings.helium.account_id
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(headers=headers, timeout=HTTP_TIMEOUT_SEC) as client:
        async def _one(product: Product) -> int:
            async with sem:
                helium_id = getattr(product.helium, "id", 0)
                if not helium_id:
                    warn(f"[H10] Empty Helium ID for {product.asin} — skip")
                    return 0
                params = {
                    "accountId": account_id,
                    "dateEnd": _end,
                    "dateStart": _start,
                    # "rawData": True, # json format
                }
                url = f"https://h10api.pacvue.com/rta/kt/v1/products/{helium_id}/export?{urlencode(params)}"
                return await _fetch_product_rank_data(client, product, url)

        tasks: list[Task] = [asyncio.create_task(_one(p)) for p in products]
        results: list[int] = await asyncio.gather(*tasks)
        updated_products = sum(1 for r in results if r > 0)
        return updated_products


async def _fetch_product_rank_data(
    client: httpx.AsyncClient,
    product: Product,
    url: str,
    max_retries: int = HTTP_MAX_RETRIES,
) -> int:
    payload = await _http_get_with_retries(client, url, product, max_retries)
    csv_data = _extract_csv_from_payload(payload, product.asin)
    return await _parse_rank_from_csv_data(product, csv_data)


async def _http_get_with_retries(
    client: httpx.AsyncClient, url: str, product: Product, max_retries: int,
) -> dict:
    for attempt in range(1, max_retries + 1):
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
        except httpx.ConnectTimeout as e:
            if attempt == max_retries:
                err(f"[H10] ConnectTimeout for {product.asin}: {e}")
                raise
            warn(f"[H10] Timeout for {product.asin} (attempt {attempt}/{max_retries}) — retrying...")
            await asyncio.sleep(1 * (2 ** (attempt - 1)))
        except httpx.HTTPStatusError as e:
            err(f"[H10] HTTPStatusError for {product.asin}: {e}")
            return {}
        except httpx.HTTPError as e:
            err(f"[H10] HTTPError for {product.asin}: {e}")
            return {}
    return {}


def _extract_csv_from_payload(payload: dict, asin: str) -> str:
    if not payload:
        return ""
    code = payload.get("code")
    if code != HTTPStatus.OK.value:
        err(f"[H10] API status {code} for {asin}")
        return ""
    data = payload.get("data") or {}
    return (data.get("results") or {}).get("csv") or ""


async def _parse_rank_from_csv_data(product: Product, csv_data: str) -> int:
    try:
        df = _read_rank_csv(product.asin, csv_data)
        return _apply_ranks_from_df(product, df)
    except Exception:
        return 0


def _read_rank_csv(asin: str, csv_data: str) -> pd.DataFrame:
    if not csv_data or not str(csv_data).strip():
        warn(f"[H10] Empty CSV for {asin}")
        raise ValueError(f"Empty CSV for {asin}")
    try:
        df = pd.read_csv(io.StringIO(csv_data))
    except pd.errors.EmptyDataError as e:
        warn(f"[H10] CSV EmptyDataError for {asin}: {e}")
        raise
    except Exception as e:
        err(f"[H10] CSV read error for {asin}: {e}")
        raise

    required = {"Keyword", "Organic Rank"}
    if not required.issubset(df.columns):
        warn(f"[H10] Missing columns {required - set(df.columns)} for {asin}")
        raise ValueError(f"Missing columns {required - set(df.columns)} for {asin}")

    df["Keyword"] = df["Keyword"].astype(str)
    df["Organic Rank"] = pd.to_numeric(df["Organic Rank"], errors="coerce")
    return df


def _apply_ranks_from_df(product: Product, df: pd.DataFrame) -> int:
    updated = 0
    series = df.set_index("Keyword")["Organic Rank"].dropna()
    for kw_rank in getattr(product.helium, "ranks", []):
        kw = getattr(kw_rank, "word", None)
        if not kw or kw not in series.index:
            continue
        mean_rank = float(series.loc[kw].mean()) if hasattr(series.loc[kw], "mean") else float(series.loc[kw])
        kw_rank.rank = round(mean_rank, 1)
        updated = 1
    return updated


def _extract_keywords(sheet_data) -> list[list[str]]:
    return [
        _process_values(value_range.get('values', [[]])[0])
        for value_range in sheet_data.get('valueRanges', [])
    ]


def _process_values(values) -> list[str]:
    for i, value in enumerate(values):
        s = str(value).strip().lower() if value else ""
        if s == OTHER_SALES_COLUMN:
            kws = [
                str(word).strip()
                for word in values[i + 1: i + 11]
                if isinstance(word, str)
                   and word
                   and not str(word).lower().startswith(EXCLUDED_PREFIXES)
            ]
            if kws:
                return kws
        elif s.startswith(TARGET_SEARCH_TERM_COLUMN):
            if i + 2 < len(values) and isinstance(values[i + 2], str):
                return [values[i + 2].strip()]
            return []
    return []


def _update_product_keywords(products: list[Product], keywords: list[list[str]]) -> None:
    for product, kws in zip(products, keywords):
        if not kws:
            continue
        product.helium.ranks.extend(
            KeywordRank(word=keyword.lower().strip()) for keyword in kws
        )
