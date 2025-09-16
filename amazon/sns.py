from pathlib import Path

import pandas as pd
from pandas import DataFrame

from models import Product

__all__ = (
    "load_sns_data",
)

SHIPPED_UNITS_COL = 'SnS shipped units'
SUBSCRIPTIONS_COUNT_COL = 'Subscriptions Count'


def load_sns_data(
    performance_path: Path,  # shipped units
    products_path: Path,  # subscriptions
    products: list[Product],
) -> None:
    try:
        performance_df = pd.read_csv(performance_path)
        products_df = pd.read_csv(products_path)
        for product in products:
            product.sns.shipped_units = _get_shipped_units(performance_df, product.asin)
            product.sns.subscriptions = _get_subscriptions_count(products_df, product.asin)
    except (FileNotFoundError, pd.errors.EmptyDataError) as e:
        print(f'ERROR: {e}')


def _get_shipped_units(df: DataFrame, asin: str) -> int:
    try:
        return int(df[df['ASIN'] == asin][SHIPPED_UNITS_COL].sum())
    except KeyError as e:
        print(f'ERROR: Column ASIN or "SnS shipped units" is not found in the data ({e})')
        return 0


def _get_subscriptions_count(df: DataFrame, asin: str) -> int:
    try:
        return int(df[df['ASIN'] == asin][SUBSCRIPTIONS_COUNT_COL].sum())
    except KeyError as e:
        print(f'ERROR: Column ASIN or "Subscriptions Count" is not found in the data ({e})')
        return 0
