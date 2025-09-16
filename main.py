import calendar
import datetime
from pathlib import Path

from amazon.sns import load_sns_data
from cmd_args import parse_arguments
from config.products import load_products
from config.settings import AppSettings, app_settings
from models import Product, Region

app_settings.set(AppSettings.load())

# Get current date for default file paths
today = datetime.date.today()
DEFAULT_FILEPATH = Path.home().joinpath(
    "Downloads",
    f"{today.strftime('%m')} {calendar.month_name[today.month].lower()}",
    f"{today.strftime('%d.%m.%Y')}",
)

# Default filenames for various data sources
DEFAULT_SELLERBOARD_FILENAME = 'dashboard_entries.xlsx'
DEFAULT_SELLERBOARD_UPDATE_FILENAME = 'dashboard_entries_update.xlsx'
DEFAULT_BUSINESS_REPORT_FILENAME = 'BusinessReport.csv'
DEFAULT_BUSINESS_REPORT_UPDATE_FILENAME = 'BusinessReport_update.csv'
DEFAULT_CAMPAIGNS_FILENAME = 'Campaigns.csv'
DEFAULT_SNS_PERFORMANCE_FILENAME = 'sns_performance_report.csv'
DEFAULT_SNS_PRODUCTS_FILENAME = 'sns_manage_products.csv'


def update_sellerboard_data(filepath: str, products: list[Product], region: Region, is_update=False):
    """
    Load and process Sellerboard data for revenue and advertising metrics.

    Args:
        filepath: Directory path containing Sellerboard files
        products: List of Product objects to update
        region: Target region for data processing
        is_update: Whether to process update data or ongoing data
    """
    filename = f'{filepath}{region.name}_{DEFAULT_SELLERBOARD_UPDATE_FILENAME if is_update else DEFAULT_SELLERBOARD_FILENAME}'
    # load_sellerboard_data(filename, products, is_update)
    # add_sellerboard_data(products, region, is_update)


def process_sellerboard_data(args, filepath: str, products: list[Product], region: Region):
    """
    Conditionally process Sellerboard data based on command line arguments.

    Args:
        args: Parsed command line arguments
        filepath: Directory path for data files
        products: List of Product objects
        region: Target region for processing
    """
    if args.sellerboard or not any([args.business, args.helium, args.campaigns, args.sns]):
        if args.ongoing or not args.update:
            update_sellerboard_data(filepath, products, region, False)
        if args.update or not args.ongoing:
            update_sellerboard_data(filepath, products, region, True)


def update_business_data(filepath: str, products: list[Product], region: Region, is_update=False):
    """
    Load and process Amazon Business Report data for sales and inventory metrics.

    Args:
        filepath: Directory path containing Business Report files
        products: List of Product objects to update
        region: Target region for data processing
        is_update: Whether to process update data or ongoing data
    """
    if not is_update:
        # Load and process current business report data
        filename = f'{filepath}{region.name}_{DEFAULT_BUSINESS_REPORT_FILENAME}'
        # load_business_data(filename, products)
        # add_business_data(products, region)
    else:
        # Load and process business report update data
        filename = f'{filepath}{region.name}_{DEFAULT_BUSINESS_REPORT_UPDATE_FILENAME}'
        # load_business_data(filename, products, is_update)
        # add_real_units(products, region)


def process_business_data(args, filepath: str, products: list[Product], region: Region):
    """
    Conditionally process Amazon Business Report data based on command line arguments.

    Args:
        args: Parsed command line arguments
        filepath: Directory path for data files
        products: List of Product objects
        region: Target region for processing
    """
    if args.business or not any([args.sellerboard, args.helium, args.campaigns, args.sns]):
        if args.ongoing or not args.update:
            update_business_data(filepath, products, region, False)
        if args.update or not args.ongoing:
            update_business_data(filepath, products, region, True)


def process_campaigns_data(args, filepath: str, products: list[Product], region: Region):
    """
    Process Amazon PPC campaigns data for advertising performance.

    Args:
        args: Parsed command line arguments
        filepath: Directory path for campaign data files
        products: List of Product objects
        region: Target region for processing
    """
    if args.campaigns or not any([args.sellerboard, args.business, args.helium, args.sns]):
        pass
        # filename = f'{filepath}{region.name}_{DEFAULT_CAMPAIGNS_FILENAME}'
        # load_campaigns_data(filename, products)
        # add_amazon_campaign_data(products, region)


def process_helium_data(args, products: list[Product], region: Region, target_date: str):
    """
    Process Helium 10 keyword ranking and weekly performance data.

    Args:
        args: Parsed command line arguments
        products: List of Product objects
        region: Target region for processing
        target_date: Target date for data processing
    """
    if args.helium or not any([args.sellerboard, args.business, args.campaigns, args.sns]):
        pass
        # products_to_update = products_with_sheet_title(products)
        # update_keywords(products_to_update, region)
        # load_weekly_data(products_to_update, str_to_date(target_date))
        # add_helium_data(products_to_update, region)


def process_sns_data(args, filepath: Path, products: list[Product], region: Region):
    """
    Process Amazon Subscribe & Save performance data.

    Args:
       args: Parsed command line arguments
       filepath: Directory path for SNS data files
       products: List of Product objects
       region: Target region for processing
    """
    if args.sns or not any([args.sellerboard, args.business, args.campaigns, args.helium]):
        performance_file = filepath.joinpath(f"{region.value}_{DEFAULT_SNS_PERFORMANCE_FILENAME}")
        products_file = filepath.joinpath(f"{region.value}_{DEFAULT_SNS_PRODUCTS_FILENAME}")
        load_sns_data(performance_file, products_file, products)


def main():
    args = parse_arguments()
    products = load_products(Region.uk)
    # target_date = get_tuesday_target_date()
    # assign_update_row_numbers(products, Region.uk, target_date)
    process_sns_data(args, DEFAULT_FILEPATH, products, Region.uk)
    print(products)


if __name__ == "__main__":
    main()
