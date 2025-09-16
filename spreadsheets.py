import gspread

from config.settings import app_settings
from models import Product, Region


def get_authenticated_gspread_client() -> gspread.Client:
    """Initialize and return authenticated gspread client."""
    try:
        credentials = app_settings.get().credentials
        return gspread.service_account(credentials)
    except Exception as e:
        print(f"Failed to initialize gspread client: {e}")
        raise


def assign_update_row_numbers(products: list[Product], region: Region, search_date: str) -> None:
    print("Assigning update row numbers to products...")
    settings = app_settings.get().spreadsheet_id
    ss = get_authenticated_gspread_client().open_by_key(settings.by_region(region))
    ranges = [f"{product.sheet_title}!A:A" for product in products]
    data = ss.values_batch_get(ranges)

    for product, value_range in zip(products, data.get("valueRanges", [])):
        product.row_index = _locate_date_row_index(value_range, search_date, product)


def _locate_date_row_index(value_range: dict, search_date: str, product: Product) -> int:
    try:
        values = value_range.get("values", [])
        if not values:
            raise ValueError(f"No data found for product {product.asin}")
        for row_index, row_values in enumerate(values, 1):
            if row_values and row_values[0] == search_date:
                return row_index
        raise ValueError(f'Search date "{search_date}" for product {product.asin} was not found')
    except (ValueError, IndexError) as e:
        print(f"ERROR: {e}")
        return -1
