from pathlib import Path

import pandas as pd

from models import Business, BusinessUpdate, Product
from pretty_print import StepTimer, err, info, ok

__all__ = (
    "load_current_data",
    "load_historical_data",
)

ASIN_COL = 1
TITLE_COL = 2
SKU_COL = 3
SESSIONS_COL = 4
UNITS_COL = 14
SALES_COL = 18
ORDERS_COL = 20


def load_current_data(filepath: Path, products: list[Product]) -> None:
    """
    Load current Business Report data and update product objects.

    Args:
        filepath (Path): Path to *current* BusinessReport.csv.
        products (list[Product]): In-memory products to update or create.

    Workflow:
        - Reads and aggregates the CSV (see _get_aggregated_products_data).
        - Ensures each ASIN from the report exists in `products`
          (creates a Product if missing).
        - Fills `product.business_current` (title, sku, sessions, units, sales, orders).

    Returns:
        None

    Raises:
        FileNotFoundError, pandas.errors.EmptyDataError, ValueError:
            bubbled up from the reader when the file is missing/empty/invalid.
        RuntimeError:
            when data is present but resulted in zero applied updates (should be rare).
    """
    with StepTimer(f"[BR] Load current data: {filepath.name}"):
        data = _get_aggregated_products_data(filepath)
        asins = data.get("asin", [])
        updated = 0
        for i, asin in enumerate(asins):
            product = _get_product_by_asin(products, asin)
            if not product:
                product = Product(asin=asin)
                products.append(product)

            product.business_current = Business(
                title=data.get("title")[i],
                sku=data.get("sku")[i],
                sessions=data.get("sessions")[i],
                units=data.get("units")[i],
                sales=round(data.get("sales")[i], 2),
                orders=data.get("orders")[i],
            )
            updated += 1
        if updated == 0:
            raise RuntimeError(f"[BR] No rows applied from {filepath.name}")
    ok(f"[BR] Current filled for {updated} products")


def load_historical_data(filepath: Path, products: list[Product]) -> None:
    """
    Load historical Business Report data and update product objects.

    Args:
        filepath (Path): Path to *historical* BusinessReport_update.csv.
        products (list[Product]): In-memory products to update or create.

    Workflow:
        - Reads and aggregates the CSV.
        - Ensures each ASIN from the report exists in `products`
          (creates a Product if missing).
        - Fills `product.business_update` with weekly units only.

    Returns:
        None

    Raises:
        FileNotFoundError, pandas.errors.EmptyDataError, ValueError:
            bubbled up from the reader when the file is missing/empty/invalid.
        RuntimeError:
            when data is present but resulted in zero applied updates.
    """
    with StepTimer(f"[BR] Load historical data: {filepath.name}"):
        data = _get_aggregated_products_data(filepath)
        asins = data.get("asin", [])
        updated = 0
        for i, asin in enumerate(asins):
            product = _get_product_by_asin(products, asin)
            if not product:
                product = Product(asin=asin)
                products.append(product)

            product.business_update = BusinessUpdate(
                units=data.get("units")[i],
            )
            updated += 1
        if updated == 0:
            raise RuntimeError(f"[BR] No rows applied from {filepath.name}")
    ok(f"[BR] Historical filled for {updated} products")


def _get_product_by_asin(products, asin):
    return next((product for product in products if product.asin == asin), None)


def _get_aggregated_products_data(filepath: Path) -> dict[str, list]:
    try:
        info(f"[BR] Reading file: {filepath}")
        df = pd.read_csv(filepath)
        rows, cols = df.shape
        if rows == 0:
            raise ValueError(f"[BR] {filepath.name} has no data rows")

        needed_idxs = [ASIN_COL, TITLE_COL, SKU_COL, SESSIONS_COL, UNITS_COL, SALES_COL, ORDERS_COL]
        if any(i >= cols for i in needed_idxs):
            raise ValueError(f"[BR] Unexpected column layout in {filepath.name} (cols={cols})")

        columns = ["asin", "title", "sku", "sessions", "units", "sales", "orders"]
        # Переименовываем все столбцы, т.к. для UK и US есть отличия в названиях
        df = df.iloc[:, needed_idxs].copy()
        df.columns = columns

        num_cols = ["units", "orders", "sessions", "sales"]
        df.loc[:, num_cols] = df[num_cols].replace({",": "", r"\$": "", "£": "", "€": ""}, regex=True)
        df.loc[:, num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        # Группируем по Child ASIN, т.к. могут быть повторения
        grouped = df.groupby("asin", as_index=False).agg(
            {
                "asin": "first",
                "title": "first",
                "sku": "first",
                "sessions": "first",
                "units": "sum",
                "sales": "sum",
                "orders": "sum",
            },
        )
        info(f"[BR] Unique ASINs: {len(grouped)}")
        return {col: grouped[col].to_list() for col in columns}
    except (FileNotFoundError, pd.errors.EmptyDataError) as error:
        err(f"[BR] {type(error).__name__}: {error}")
        raise
