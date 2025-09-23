from pathlib import Path

import pandas as pd
from pandas import DataFrame

from models import Product
from pretty_print import StepTimer, err, ok

__all__ = ("load_sns_data",)

SHIPPED_UNITS_COL = "SnS shipped units"
SUBSCRIPTIONS_COUNT_COL = "Subscriptions Count"
ASIN_COL = "ASIN"


def load_sns_data(
    performance_path: Path,
    products_path: Path,
    products: list[Product],
) -> None:
    """
    Load Amazon Subscribe & Save (SnS) performance and product subscription data,
    then update in-memory Product objects.

    Args:
        performance_path (Path): CSV with shipped units per ASIN; must contain
            columns ["ASIN", "SnS shipped units"].
        products_path (Path): CSV with subscriptions per ASIN; must contain
            columns ["ASIN", "Subscriptions Count"].
        products (list[Product]): Products to update (in-memory).

    Returns:
        None

    Raises:
        FileNotFoundError: If any CSV file is missing.
        pandas.errors.EmptyDataError: If any CSV is unreadable/empty.
        ValueError: If CSVs have zero rows after reading or expected columns are missing.
    """
    with StepTimer(f"[SNS] Load data: {performance_path.name} & {products_path.name}"):
        perf_df = _read_csv_safe(performance_path)
        subs_df = _read_csv_safe(products_path)

        _validate_columns(perf_df, [ASIN_COL, SHIPPED_UNITS_COL], performance_path.name)
        _validate_columns(subs_df, [ASIN_COL, SUBSCRIPTIONS_COUNT_COL], products_path.name)

        perf_df[SHIPPED_UNITS_COL] = _to_int_series(perf_df[SHIPPED_UNITS_COL])
        subs_df[SUBSCRIPTIONS_COUNT_COL] = _to_int_series(subs_df[SUBSCRIPTIONS_COUNT_COL])

        shipped_lookup = _sum_by_asin(perf_df, SHIPPED_UNITS_COL)
        subs_lookup = _sum_by_asin(subs_df, SUBSCRIPTIONS_COUNT_COL)

        updated = 0
        for p in products:
            p.sns.shipped_units = int(shipped_lookup.get(p.asin, 0))
            p.sns.subscriptions = int(subs_lookup.get(p.asin, 0))
            if p.sns.shipped_units or p.sns.subscriptions:
                updated += 1

        if updated == 0:
            raise ValueError("[SNS] No ASIN matches in provided CSVs")
        ok(f"[SNS] Filled for {updated} / {len(products)} products")


def _read_csv_safe(path: Path) -> DataFrame:
    try:
        df = pd.read_csv(path)
    except (FileNotFoundError, pd.errors.EmptyDataError) as e:
        err(f"[SNS] {type(e).__name__}: {e}")
        raise
    if df.empty:
        raise ValueError(f"[SNS] {path.name} has no data rows")
    return df


def _validate_columns(df: DataFrame, required: list[str], filename: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[SNS] Missing columns in {filename}: {missing}")


def _to_int_series(s: pd.Series) -> pd.Series:
    # normalize "1,234", "$1,234", "€1.234", etc. -> int
    cleaned = (
        s.astype(str)
        .replace({",": "", r"\$": "", "£": "", "€": ""}, regex=True)
        .str.replace("\u00a0", "", regex=False)  # non-breaking space just in case
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0).astype(int)


def _sum_by_asin(df: DataFrame, value_col: str) -> dict[str, int]:
    try:
        grouped = df.groupby(ASIN_COL, as_index=True)[value_col].sum()
    except KeyError as e:
        err(f"[SNS] Missing column while grouping ({e})")
        return {}
    # ensure plain ints
    return {str(asin): int(val) for asin, val in grouped.items()}
