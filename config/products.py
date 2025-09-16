import datetime

from config.settings import app_settings
from models import Campaign, Helium, Product, Region
from spreadsheets import get_authenticated_gspread_client


def get_tuesday_target_date() -> str:
    """Returns the Tuesday date for weekly data updates in 'DD/MMM/YY' format.

    The target is always Tuesday of the current week. If today is Monday,
    returns tomorrow's date. If today is Tuesday, returns today's date.

    Returns:
        String in format '07/Jan/25'
    """
    today = datetime.date.today()
    days_to_tuesday = (1 - today.weekday()) % 7
    target_date = today + datetime.timedelta(days=days_to_tuesday)
    return target_date.strftime("%d/%b/%y")


def load_products(region: Region) -> list[Product]:
    print("Loading products...")
    settings = app_settings.get()
    ss = get_authenticated_gspread_client().open_by_key(settings.spreadsheet_id.products)
    data = ss.worksheet(region.value).get_all_records()
    products: list = []
    for item in data:
        helium_id = item.get("helium")
        helium_id = int(helium_id) if helium_id else 0
        products.append(Product(
            asin=item.get("asin"),
            sheet_title=item.get("sheet"),
            campaign=Campaign(name=item.get("campaign")),
            helium=Helium(id=helium_id),
        ))
    return products
