import datetime

from config.settings import app_settings
from models import Campaign, Helium, Product, Region
from pretty_print import BOLD, DIM, RESET, StepTimer, info, warn
from spreadsheets import get_authenticated_gspread_client


def get_tuesday_target_date(announce: bool = False) -> str:
    """Returns the Tuesday date for weekly data updates in 'DD/MMM/YY' format.

    The target is always Tuesday of the current week. If today is Monday,
    returns tomorrow's date. If today is Tuesday, returns today's date.

    Returns:
        String in format '07/Jan/25'
    """
    today = datetime.date.today()
    days_to_tuesday = (1 - today.weekday()) % 7
    target_date = today + datetime.timedelta(days=days_to_tuesday)
    result = target_date.strftime("%d/%b/%y")
    if announce:
        weekday_map = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        delta_text = ("today" if days_to_tuesday == 0
                      else f"+{days_to_tuesday} day(s)")
        info(
            f"{BOLD}Target Tuesday:{RESET} {result} "
            f"{DIM}(today: {today:%d/%b/%y} {weekday_map[today.weekday()]}, {delta_text}){RESET}",
        )
    return result


def load_products(region: Region) -> list[Product]:
    with StepTimer(f"{BOLD}Loading products for region:{RESET} {region.value}"):
        settings = app_settings.get()
        ss = get_authenticated_gspread_client().open_by_key(settings.spreadsheet_id.products)
        data = ss.worksheet(region.value).get_all_records()

        products: list[Product] = []
        missing_asin = 0
        missing_sheet = 0

        for idx, item in enumerate(data, start=1):
            asin = (item.get("asin") or "").strip()
            if not asin:
                missing_asin += 1
                warn(f"{region.value}: row {idx}: empty ASIN - skipped")
                continue

            sheet_title = (item.get("sheet") or "").strip()
            if not sheet_title:
                missing_sheet += 1

            campaign_name = item.get("campaign") or ""

            helium_id_raw = item.get("helium_id")
            try:
                helium_id = int(helium_id_raw) if helium_id_raw not in {"", None} else 0
            except (TypeError, ValueError):
                warn(f"{region.value}: row {idx}: invalid helium_id={helium_id_raw!r} → set to 0")
                helium_id = 0

            products.append(Product(
                asin=asin,
                sheet_title=sheet_title,
                campaign=Campaign(name=campaign_name),
                helium=Helium(id=helium_id),
            ))

        total = len(products)
        if total == 0:
            raise RuntimeError(f"Product list is empty for region {region.value}")

        info(
            f"{region.value}: loaded {BOLD}{total}{RESET} products; "
            f"missing_asin: {missing_asin}, missing_sheet: {missing_sheet}",
        )

        return products
