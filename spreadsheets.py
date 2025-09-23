import datetime
from typing import Any

import gspread

from config.settings import AppSettings, app_settings
from models import Product, Region
from pretty_print import StepTimer, err, ok

__all__ = (
    "get_authenticated_gspread_client",
    "assign_update_row_numbers",
    "add_current_sellerboard_data",
    "add_historical_sellerboard_data",
    "add_sns_data",
    "add_current_business_data",
    "add_historical_business_data",
    "add_amazon_campaigns_data",
    "add_helium_data",
    "get_first_row_data",
)


# --- Public API ---------------------------------------------------------------

def get_authenticated_gspread_client() -> gspread.Client:
    """
    Initialize and return an authenticated `gspread.Client`.

    This function loads application credentials from `app_settings` and
    creates a Service Account client for Google Sheets API access.

    Returns:
        gspread.Client: Authorized Sheets client ready for reading/writing.

    Raises:
        Exception: Propagates any error that occurs while loading credentials
            or constructing the client.

    Side Effects:
        - Logs a timed step and success/error messages via `StepTimer`, `ok`, and `err`.
    """
    with StepTimer("[SHEETS] Initialize gspread client"):
        try:
            credentials = app_settings.get().credentials
            client = gspread.service_account(credentials)
            ok("[SHEETS] gspread client ready")
            return client
        except Exception as e:
            err(f"[SHEETS] Failed to initialize gspread client: {e}")
            raise


def assign_update_row_numbers(products: list[Product], region: Region, search_date: str) -> None:
    """
    Resolve and set `row_index` for each product by matching a date in column A.

    For every product with a non-empty `sheet_title`, this function scans
    column A (`A:A`) of the product's sheet to find the first row whose
    first cell equals `search_date` (typically `DD/MM/YYYY`). The found
    1-based row index is stored in `product.row_index`. If a row cannot
    be found, the value is set to `-1`.

    Args:
        products: The products whose `row_index` should be resolved.
        region: Region whose spreadsheet ID will be used.
        search_date: A textual date value to match against column A.

    Raises:
        Exception: If the Sheets API batch get fails.

    Notes:
        - Products with empty `sheet_title` are skipped.
        - Matching is case-sensitive exact string compare for the first cell.

    Side Effects:
        - Logs progress and counts via `StepTimer`, `ok`, `err`.
        - Mutates `product.row_index` in-place.
    """
    with StepTimer(f"[SHEETS] Locate row indices for date={search_date} in region={getattr(region, 'name', region)}"):
        _, spreadsheet = _settings_and_spreadsheet_by_region(region)
        products_with_titles = [p for p in products if p.sheet_title]
        ranges = [f"{p.sheet_title}!A:A" for p in products_with_titles]
        if not ranges:
            err("[SHEETS] No sheet titles found among products")
            return

        data = _values_batch_get(spreadsheet, ranges)
        found = 0
        for product, value_range in zip(products_with_titles, data.get("valueRanges", [])):
            product.row_index = _locate_date_row_index(value_range, search_date, product)
            if product.row_index > 0:
                found += 1

        ok(f"[SHEETS] Row indices resolved for {found}/{len(products)} products")


def add_current_sellerboard_data(products: list[Product], region: Region) -> None:
    """
    Write current Sellerboard metrics (profit, margin) into product sheets.

    Data are written into the range defined by
    `settings.sellerboard.current_cells_range` at `product.row_index` for each product.

    Args:
        products: Products that contain `sb_current` (profit, margin) and `row_index`.
        region: Region whose spreadsheet ID will be used.

    Raises:
        Exception: If the batch update request fails.

    Side Effects:
        - Skips products without `sheet_title` or with invalid `row_index` (<= 1).
        - Performs a batched update using "USER_ENTERED".
        - Logs counts and errors via `StepTimer`, `ok`, `err`.
    """
    with StepTimer(f"[SHEETS] Write CURRENT Sellerboard data to region={getattr(region, 'name', region)}"):
        settings, spreadsheet = _settings_and_spreadsheet_by_region(region)
        c1, c2 = settings.sellerboard.current_cells_range
        batch_data = []
        skipped = 0

        for p in products:
            if not p.sheet_title or p.row_index <= 1:
                skipped += 1
                continue
            batch_data.append({
                "range": f"{p.sheet_title}!{c1}{p.row_index}:{c2}{p.row_index}",
                "values": [[p.sb_current.profit, p.sb_current.margin]],
            })

        _batch_update(spreadsheet, batch_data, context="[SHEETS][SB][CURRENT]")
        ok(f"[SHEETS][SB][CURRENT] Updated {len(batch_data)} ranges; skipped {skipped}")


def add_historical_sellerboard_data(products: list[Product], region: Region) -> None:
    """
    Write historical Sellerboard metrics (profit, margin, formula) with update offset.

    For each product, the target row is `product.row_index - settings.update_offset`.
    A third cell is filled with the formula `=1-AK{row}/AI{row}` (legacy logic).

    Args:
        products: Products that contain `sb_historical` and `row_index`.
        region: Region whose spreadsheet ID will be used.

    Raises:
        Exception: If the batch update request fails.

    Side Effects:
        - Skips products without `sheet_title` or with target row <= 1.
        - Performs a batched update using "USER_ENTERED".
        - Logs counts and errors via `StepTimer`, `ok`, `err`.
    """
    with StepTimer(f"[SHEETS] Write HISTORICAL Sellerboard data to region={getattr(region, 'name', region)}"):
        settings, spreadsheet = _settings_and_spreadsheet_by_region(region)
        c1, c2 = settings.sellerboard.historical_cells_range
        batch_data = []
        skipped = 0

        for p in products:
            if not p.sheet_title:
                skipped += 1
                continue
            row = p.row_index - settings.update_offset
            if row <= 1:
                skipped += 1
                continue
            batch_data.append({
                "range": f"{p.sheet_title}!{c1}{row}:{c2}{row}",
                "values": [[p.sb_historical.profit, p.sb_historical.margin, f"=1-AK{row}/AI{row}"]],
            })

        _batch_update(spreadsheet, batch_data, context="[SHEETS][SB][HIST]")
        ok(f"[SHEETS][SB][HIST] Updated {len(batch_data)} ranges; skipped {skipped}")


def add_sns_data(products: list[Product], region: Region) -> None:
    """
    Write Subscribe & Save (SnS) metrics (subscriptions, shipped units).

    Data are written into the range defined by `settings.sns.cells_range`
    at `product.row_index` for each product.

    Args:
        products: Products containing `sns.subscriptions` and `sns.shipped_units`.
        region: Region whose spreadsheet ID will be used.

    Raises:
        Exception: If the batch update request fails.

    Side Effects:
        - Skips products without `sheet_title` or invalid `row_index`.
        - Performs a batched update using "USER_ENTERED".
        - Logs counts and errors via `StepTimer`, `ok`, `err`.
    """
    with StepTimer(f"[SHEETS] Write SnS data to region={getattr(region, 'name', region)}"):
        settings, spreadsheet = _settings_and_spreadsheet_by_region(region)
        c1, c2 = settings.sns.cells_range
        batch_data = []
        skipped = 0

        for p in products:
            if not p.sheet_title or p.row_index <= 0:
                skipped += 1
                continue
            batch_data.append({
                "range": f"{p.sheet_title}!{c1}{p.row_index}:{c2}{p.row_index}",
                "values": [[p.sns.subscriptions, p.sns.shipped_units]],
            })

        _batch_update(spreadsheet, batch_data, context="[SHEETS][SNS]")
        ok(f"[SHEETS][SNS] Updated {len(batch_data)} ranges; skipped {skipped}")


def add_current_business_data(products: list[Product], region: Region) -> None:
    """
    Append current Business metrics to the region-level Business sheet.

    The function:
      1) Computes the adjusted date (Tuesday of the current ISO week).
      2) Uses week number = (current ISO week - 1).
      3) Appends one row per product that has `business_current`.

    Args:
        products: Products containing `business_current` info.
        region: Region determining the Business sheet title and spreadsheet ID.

    Raises:
        Exception: If the batch update request fails or reading last row fails.

    Side Effects:
        - Appends rows to the sheet `settings.business.title_by_region(region)`.
        - Performs a batched update using "USER_ENTERED".
        - Logs counts and errors via `StepTimer`, `ok`, `err`.
    """
    with StepTimer(f"[SHEETS] Append CURRENT Business data to region={getattr(region, 'name', region)}"):
        settings, spreadsheet = _settings_and_spreadsheet_by_region(region)
        cells_range = settings.business.current_cells_range
        title = settings.business.title_by_region(region)

        today = _get_adjusted_date()
        week = today.isocalendar().week - 1
        row_start = _get_last_row_index(spreadsheet, title)

        values = [
            _create_business_row(p, today, week)
            for p in products
            if p.business_current
        ]

        if not values:
            err("[SHEETS][BUS][CURRENT] No products with current business data")
            return

        _batch_update(spreadsheet, [{
            "range": f"{title}!{cells_range[0]}{row_start}:{cells_range[1]}{row_start + len(values) - 1}",
            "values": values,
        }], context="[SHEETS][BUS][CURRENT]")

        ok(f"[SHEETS][BUS][CURRENT] Appended {len(values)} rows to '{title}'")


def add_historical_business_data(products: list[Product], region: Region) -> None:
    """
    Write historical Business metric (units) per product using update offset.

    For each product, the target cell is at
    `settings.business.historical_cells_range[0]` and
    row `product.row_index - settings.update_offset`.

    Args:
        products: Products containing `business_update.units` if available.
        region: Region whose spreadsheet ID will be used.

    Raises:
        Exception: If the batch update request fails.

    Side Effects:
        - Writes `0` if `business_update` is missing.
        - Skips products without `sheet_title` or with target row <= 1.
        - Logs counts and errors via `StepTimer`, `ok`, `err`.
    """
    with StepTimer(f"[SHEETS] Write HISTORICAL Business data to region={getattr(region, 'name', region)}"):
        settings, spreadsheet = _settings_and_spreadsheet_by_region(region)
        cell = settings.business.historical_cells_range[0]
        batch_data = []
        skipped = 0

        for p in products:
            if not p.sheet_title:
                skipped += 1
                continue
            row = p.row_index - settings.update_offset
            if row <= 1:
                skipped += 1
                continue
            units = p.business_update.units if p.business_update else 0
            batch_data.append({
                "range": f"{p.sheet_title}!{cell}{row}",
                "values": [[units]],
            })

        _batch_update(spreadsheet, batch_data, context="[SHEETS][BUS][HIST]")
        ok(f"[SHEETS][BUS][HIST] Updated {len(batch_data)} ranges; skipped {skipped}")


def add_amazon_campaigns_data(products: list[Product], region: Region) -> None:
    """
    Write Amazon Ads campaign metrics for each product at `product.row_index`.

    The written fields are (in order): spend, clicks, CTR, CPC, orders, ACOS.

    Args:
        products: Products containing a populated `campaign` section.
        region: Region whose spreadsheet ID will be used.

    Raises:
        Exception: If the batch update request fails.

    Side Effects:
        - Skips products without `sheet_title` or invalid `row_index`.
        - Logs counts and errors via `StepTimer`, `ok`, `err`.
    """
    with StepTimer(f"[SHEETS] Write Amazon Campaigns data to region={getattr(region, 'name', region)}"):
        settings, spreadsheet = _settings_and_spreadsheet_by_region(region)
        c1, c2 = settings.campaigns.cells_range
        batch_data = []
        skipped = 0

        for p in products:
            if not p.sheet_title or p.row_index <= 0:
                skipped += 1
                continue
            batch_data.append({
                "range": f"{p.sheet_title}!{c1}{p.row_index}:{c2}{p.row_index}",
                "values": [_create_amazon_campaign_row(p)],
            })

        _batch_update(spreadsheet, batch_data, context="[SHEETS][CAMPAIGNS]")
        ok(f"[SHEETS][CAMPAIGNS] Updated {len(batch_data)} ranges; skipped {skipped}")


def add_helium_data(products: list[Product], region: Region) -> None:
    """
    Write Helium10 keyword rank data horizontally from a configured start column.

    If a product has a single rank value, it is written to a single cell.
    If multiple ranks exist, they are written across consecutive columns
    starting at `settings.helium.cells_range[0]`.

    Args:
        products: Products containing `helium.ranks` and `row_index`.
        region: Region whose spreadsheet ID will be used.

    Raises:
        Exception: If the batch update request fails.

    Side Effects:
        - Skips products without ranks or without `sheet_title`.
        - Logs counts and errors via `StepTimer`, `ok`, `err`.
    """
    with StepTimer(f"[SHEETS] Write Helium ranks to region={getattr(region, 'name', region)}"):
        settings, spreadsheet = _settings_and_spreadsheet_by_region(region)
        start_col = settings.helium.cells_range[0]
        batch_data = [
            _make_helium_range(p, start_col)
            for p in products
            if p.sheet_title and p.helium.ranks
        ]

        if not batch_data:
            err("[SHEETS][HELIUM] No ranges to update")
            return

        _batch_update(spreadsheet, batch_data, context="[SHEETS][HELIUM]")
        ok(f"[SHEETS][HELIUM] Updated {len(batch_data)} ranges")


def get_first_row_data(products: list[Product], region: Region) -> dict:
    """
    Fetch the first row (headers) from each product sheet in a region.

    Args:
        products: Products whose `sheet_title` values will be used to build ranges.
        region: Region whose spreadsheet ID will be used.

    Returns:
        dict: Raw response from `values_batch_get`, containing `valueRanges`.

    Raises:
        Exception: If the Sheets API call fails.

    Side Effects:
        - Logs timing and counts via `StepTimer`, `ok`, `err`.
    """
    with StepTimer(f"[SHEETS] Fetch first-row data for region={getattr(region, 'name', region)}"):
        _, spreadsheet = _settings_and_spreadsheet_by_region(region)
        sheet_ranges = [f"{p.sheet_title}!1:1" for p in products if p.sheet_title]
        data = _values_batch_get(spreadsheet, sheet_ranges)
        ok(f"[SHEETS] Retrieved first rows for {len(sheet_ranges)} sheets")
        return data


# --- Internal helpers (no docstrings by request) ------------------------------

def _settings_and_spreadsheet_by_region(region: Region) -> tuple[AppSettings, gspread.Spreadsheet]:
    setting = app_settings.get()
    key = setting.spreadsheet_id.by_region(region)
    with StepTimer(f"[SHEETS] Open spreadsheet by key ({getattr(region, 'name', region)})"):
        ss = get_authenticated_gspread_client().open_by_key(key)
        ok(f"[SHEETS] Spreadsheet opened: {key}")
    return setting, ss


def _values_batch_get(spreadsheet: gspread.Spreadsheet, ranges: list[str]) -> dict:
    try:
        return spreadsheet.values_batch_get(ranges)
    except Exception as e:
        err(f"[SHEETS] values_batch_get failed: {e}")
        raise


def _locate_date_row_index(value_range: dict, search_date: str, product: Product) -> int:
    try:
        values = value_range.get("values", [])
        if not values:
            raise ValueError(f"No data found for product {product.asin}")
        for row_index, row_values in enumerate(values, 1):
            if row_values and row_values[0] == search_date:
                return row_index
        raise ValueError(f"Search date {search_date} for product {product.asin} was not found")
    except (ValueError, IndexError) as e:
        err(f"[SHEETS] {e}")
        return -1


def _get_adjusted_date() -> datetime.date:
    today = datetime.date.today()
    return today - datetime.timedelta(days=today.weekday() - 1)


def _get_last_row_index(spreadsheet: gspread.Spreadsheet, title: str) -> int:
    try:
        data = spreadsheet.values_get(f"{title}!A:A")
        return len(data.get("values", [])) + 1
    except Exception as e:
        err(f"[SHEETS] values_get failed for '{title}!A:A': {e}")
        raise


def _create_business_row(product: Product, date: datetime.date, week: int) -> list:
    return [
        date.strftime("%d/%m/%Y"),
        product.asin,
        product.business_current.title,
        product.business_current.sku,
        product.business_current.sessions,
        product.business_current.units,
        product.business_current.sales,
        product.business_current.orders,
        "",
        week,
    ]


def _create_amazon_campaign_row(product: Product) -> list:
    return [
        product.campaign.spend,
        product.campaign.clicks,
        product.campaign.ctr,
        product.campaign.cpc,
        product.campaign.orders,
        product.campaign.acos,
    ]


def _batch_update(ss: gspread.Spreadsheet, batch_data: list, *, context: str = "[SHEETS]") -> None:
    if not batch_data:
        err(f"{context} Nothing to update (empty batch)")
        return
    try:
        ss.values_batch_update({
            "data": batch_data,
            "valueInputOption": "USER_ENTERED",
        })
    except Exception as e:
        err(f"{context} Batch update failed: {e}")
        raise


def _col_to_num(col: str) -> int:
    col = col.strip().upper()
    num = 0
    for ch in col:
        if not ("A" <= ch <= "Z"):
            raise ValueError(f"Invalid column label: {col}")
        num = num * 26 + (ord(ch) - ord("A") + 1)
    return num


def _num_to_col(num: int) -> str:
    if num <= 0:
        raise ValueError("Column number must be >= 1")
    letters = []
    while num:
        num, rem = divmod(num - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def _range_end_col(start_col: str, width: int) -> str:
    return _num_to_col(_col_to_num(start_col) + width - 1)


def _make_helium_range(product: Product, start_col: str) -> dict[str, Any]:
    row = product.row_index
    ranks = product.helium.ranks

    if len(ranks) == 1:
        cells_range = f"{product.sheet_title}!{start_col}{row}"
        values = [[ranks[0].rank]]
    else:
        end_col = _range_end_col(start_col, len(ranks))
        cells_range = f"{product.sheet_title}!{start_col}{row}:{end_col}{row}"
        values = [[kw.rank for kw in ranks]]

    return {"range": cells_range, "values": values}
