from pathlib import Path

import pandas as pd

from models import Campaign, Product
from pretty_print import StepTimer, err, info, ok

__all__ = ("load_campaigns",)


class CampaignState:
    ENABLED = "ENABLED"
    PAUSED = "PAUSED"
    ARCHIVED = "ARCHIVED"


def load_campaigns(filepath: Path, products: list[Product]) -> None:
    """
    Load Amazon PPC campaign performance and update in-memory Product objects.

    Args:
        filepath (Path): Path to Campaigns.csv export.
        products (list[Product]): Products to update in-memory. For each product
            having a non-empty `product.campaign.name`, the function aggregates
            rows whose "Campaigns" contains that name (case-insensitive) among
            campaigns with State in {ENABLED, PAUSED}.

    Returns:
        None

    Raises:
        FileNotFoundError: If the CSV file is missing.
        pandas.errors.EmptyDataError: If the CSV is unreadable/empty at parser level.
        ValueError:
            - If the file has zero rows after reading.
            - If required columns are missing.
            - If no applicable rows (after filtering by State) remain.
        RuntimeError:
            If no products were updated (no campaign name matched any row).
    """
    with StepTimer(f"[PPC] Load campaigns: {filepath.name}"):
        try:
            df = pd.read_csv(filepath)
        except (FileNotFoundError, pd.errors.EmptyDataError) as e:
            err(f"[PPC] {type(e).__name__}: {e}")
            raise

        df, spend_col, sales_col = _prepare_campaign_df(df, filepath.name)
        updated = 0
        for product in products:
            if not (product.campaign and product.campaign.name):
                continue
            metrics = _aggregate_campaign_metrics(df, product.campaign.name, spend_col, sales_col)
            if metrics is None:
                continue
            _update_product_campaign(product, product.campaign.name, metrics)
            updated += 1

        if updated == 0:
            raise RuntimeError(f"[PPC] No campaign matches for any product in {filepath.name}")

        ok(f"[PPC] Filled metrics for {updated} product(s)")


def _prepare_campaign_df(raw: pd.DataFrame, filename: str) -> tuple[pd.DataFrame, str, str]:
    rows, cols = raw.shape
    info(f"[PPC] Rows read: {rows}, Cols: {cols}")
    if rows == 0:
        raise ValueError(f"[PPC] {filename} has no data rows")
    spend_col, sales_col = _detect_currency_columns(raw, filename)
    required = ["State", "Campaigns", "Clicks", "Orders", "Impressions", spend_col, sales_col]
    _ensure_columns(raw, required, filename)
    df = raw[required].copy()
    df = df[df["State"].isin([CampaignState.ENABLED, CampaignState.PAUSED])]
    info(f"[PPC] Rows after State filter: {len(df)}")
    if df.empty:
        raise ValueError(f"[PPC] No ENABLED/PAUSED rows in {filename}")
    to_numeric = [spend_col, sales_col, "Clicks", "Orders", "Impressions"]
    df.loc[:, to_numeric] = (
        df[to_numeric]
        .replace({",": "", r"\$": "", "£": "", "€": ""}, regex=True)
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
    )
    return df, spend_col, sales_col


def _detect_currency_columns(df: pd.DataFrame, filename: str) -> tuple[str, str]:
    spend_col = "Spend(GBP)" if "Spend(GBP)" in df.columns else ("Spend" if "Spend" in df.columns else None)
    sales_col = "Sales(GBP)" if "Sales(GBP)" in df.columns else ("Sales" if "Sales" in df.columns else None)
    if not spend_col or not sales_col:
        raise ValueError(
            f"[PPC] Missing spend/sales columns in {filename} "
            f"(looked for Spend/Spend(GBP) and Sales/Sales(GBP))",
        )
    return spend_col, sales_col


def _ensure_columns(df: pd.DataFrame, required: list[str], filename: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[PPC] Missing columns in {filename}: {missing}")


def _aggregate_campaign_metrics(
    df: pd.DataFrame,
    campaign_name: str,
    spend_col: str,
    sales_col: str,
) -> dict[str, int | float] | None:
    mask = df["Campaigns"].astype(str).str.contains(str(campaign_name), case=False, na=False)
    filtered = df[mask]
    if filtered.empty:
        return None

    spend = float(filtered[spend_col].sum())
    clicks = int(filtered["Clicks"].sum())
    orders = int(filtered["Orders"].sum())
    sales = float(filtered[sales_col].sum())
    impressions = float(filtered["Impressions"].sum())

    return {
        "spend": round(spend, 2),
        "clicks": clicks,
        "orders": orders,
        "ctr": round((clicks / impressions), 4) if impressions else 0.0,
        "cpc": round((spend / clicks), 2) if clicks else 0.0,
        "acos": round((spend / sales), 4) if sales else 0.0,
    }


def _update_product_campaign(product: Product, name: str, metrics: dict) -> None:
    product.campaign = Campaign(
        name=name,
        spend=metrics["spend"],
        clicks=metrics["clicks"],
        ctr=metrics["ctr"],
        cpc=metrics["cpc"],
        orders=metrics["orders"],
        acos=metrics["acos"],
    )
