from pathlib import Path

import pandas as pd

from models import Product
from pretty_print import StepTimer, err, info, ok

__all__ = (
    "load_current_data",
    "load_historical_data",
)


def load_current_data(filepath: Path, products: list[Product]) -> None:
    """
    Load current Sellerboard data from an Excel file and update product objects.

    Args:
        filepath (Path): Path to the Sellerboard *current* Excel file
            containing columns ["ASIN", "Sales", "Net profit"].
        products (list[Product]): Product objects to update in-memory.

    Workflow:
        - Read the Excel file and aggregate by ASIN (sum Sales and Net profit).
        - For each product whose ASIN is present, update:
          `product.sb_current.profit` and `product.sb_current.margin`.

    Returns:
        None

    Raises:
        FileNotFoundError: If the file does not exist.
        pandas.errors.EmptyDataError: If the file is unreadable/empty.
        ValueError: If the file has no rows after reading.
        RuntimeError: If no products were updated (no ASIN matches).

    Notes:
        - Margin = Net profit / Sales (rounded to 4 decimals).
        - Prints progress and timing via pretty-print utilities.
        - If the function completes without exception, at least one product
          was updated.
    """
    with StepTimer(f"[SB] Load current data: {filepath.name}"):
        grouped = _get_grouped_dataframe(filepath)
        updated = 0
        for product in products:
            if product.asin in grouped.index:
                profit, margin = _get_profit_margin(grouped.loc[[product.asin]])
                product.sb_current.profit = profit
                product.sb_current.margin = margin
                updated += 1
        if updated == 0:
            raise RuntimeError(f"[SB] No matching ASINs found in {filepath.name}")
        ok(f"[SB] Current filled for {updated} / {len(products)} products")


def load_historical_data(filepath: Path, products: list[Product]) -> None:
    """
    Load historical Sellerboard data from an Excel file and update product objects.

    Args:
        filepath (Path): Path to the Sellerboard *historical* Excel file
            containing columns ["ASIN", "Sales", "Net profit"].
        products (list[Product]): Product objects to update in-memory.

    Workflow:
        - Read the Excel file and aggregate by ASIN (sum Sales and Net profit).
        - For each product whose ASIN is present, update:
          `product.sb_historical.profit` and `product.sb_historical.margin`.

    Returns:
        None

    Raises:
        FileNotFoundError: If the file does not exist.
        pandas.errors.EmptyDataError: If the file is unreadable/empty.
        ValueError: If the file has no rows after reading.
        RuntimeError: If no products were updated (no ASIN matches).

    Notes:
        - Margin = Net profit / Sales (rounded to 4 decimals).
        - Prints progress and timing via pretty-print utilities.
        - If the function completes without exception, at least one product
          was updated.
    """
    with StepTimer(f"[SB] Load historical data: {filepath.name}"):
        grouped = _get_grouped_dataframe(filepath)
        updated = 0
        for product in products:
            if product.asin in grouped.index:
                profit, margin = _get_profit_margin(grouped.loc[[product.asin]])
                product.sb_historical.profit = profit
                product.sb_historical.margin = margin
                updated += 1
        if updated == 0:
            raise RuntimeError(f"[SB] No matching ASINs found in {filepath.name}")
        ok(f"[SB] Historical filled for {updated} / {len(products)} products")


def _get_grouped_dataframe(filepath: Path) -> pd.DataFrame:
    try:
        info(f"[SB] Reading file: {filepath}")
        df = pd.read_excel(filepath, usecols=["ASIN", "Sales", "Net profit"])
        rows = len(df)
        info(f"[SB] Rows read: {rows}")
        if rows == 0:
            raise ValueError(f"[SB] {filepath.name} has no data rows")
        grouped = df.groupby("ASIN").agg({"Sales": "sum", "Net profit": "sum"})
        info(f"[SB] Groups: {len(grouped)}")
        return grouped
    except (FileNotFoundError, pd.errors.EmptyDataError) as error:
        err(f"[SB] {type(error).__name__}: {error}")
        raise


def _get_profit_margin(df: pd.DataFrame) -> tuple[float, float]:
    totals = df[["Sales", "Net profit"]].fillna(0).sum()
    sales, profit = totals["Sales"], totals["Net profit"]
    margin = round(profit / sales, 4) if sales else 0.0

    return profit, margin
