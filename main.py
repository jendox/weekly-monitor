import datetime
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Optional

import helium
import sellerboard
import spreadsheets
from amazon import business, campaign, sns
from cmd_args import parse_arguments
from config.products import get_tuesday_target_date, load_products
from config.settings import AppSettings, app_settings
from models import Product, Region
from pretty_print import StepTimer, err, info, ok

app_settings.set(AppSettings.load())

# Get current date for default file paths
today = datetime.date.today()
DEFAULT_FILEPATH = Path.home().joinpath(
    "Downloads",
    f"{today.strftime('%m %B').lower()}",
    f"{today.strftime('%d.%m.%Y')}",
)

# Default filenames for various data sources
DEFAULT_SELLERBOARD_FILENAME = "dashboard_entries.xlsx"
DEFAULT_SELLERBOARD_UPDATE_FILENAME = "dashboard_entries_update.xlsx"
DEFAULT_BUSINESS_REPORT_FILENAME = "BusinessReport.csv"
DEFAULT_BUSINESS_REPORT_UPDATE_FILENAME = "BusinessReport_update.csv"
DEFAULT_CAMPAIGNS_FILENAME = "Campaigns.csv"
DEFAULT_SNS_PERFORMANCE_FILENAME = "sns_performance_report.csv"
DEFAULT_SNS_PRODUCTS_FILENAME = "sns_manage_products.csv"


def _run_phase(
    region: Region,
    label: str,
    load_fn: Callable[[], None],
    add_fn: Optional[Callable[[], None]] = None,
) -> bool:
    try:
        with StepTimer(f"[{region.value}] {label} — load"):
            load_fn()
    except Exception as e:
        err(f"[{region.value}][{label}] load failed: {e}")
        return False
    if add_fn is None:
        ok(f"[{region.value}][{label}] load ok")
        return True
    try:
        with StepTimer(f"[{region.value}] {label} — add"):
            add_fn()
    except Exception as e:
        err(f"[{region.value}][{label}] add failed: {e}")
        return False

    ok(f"[{region.value}][{label}] completed")
    return True


def process_sellerboard(args, filepath: Path, products: list[Product], region: Region) -> None:
    """
    Conditionally process Sellerboard data based on command line arguments.

    Args:
        args: Parsed command line arguments
        filepath: Directory path for data files
        products: List of Product objects
        region: Target region for processing
    """
    if not (args.sellerboard or not any([args.business, args.helium, args.campaigns, args.sns])):
        return

    if args.ongoing or not args.update:
        _run_phase(
            region,
            "SB current",
            lambda: sellerboard.load_current_data(
                filepath.joinpath(f"{region.value}_{DEFAULT_SELLERBOARD_FILENAME}"), products,
            ),
            lambda: spreadsheets.add_current_sellerboard_data(products, region),
        )
    if args.update or not args.ongoing:
        _run_phase(
            region,
            "SB historical",
            lambda: sellerboard.load_historical_data(
                filepath.joinpath(f"{region.value}_{DEFAULT_SELLERBOARD_UPDATE_FILENAME}"), products,
            ),
            lambda: spreadsheets.add_historical_sellerboard_data(products, region),
        )


def process_business(args, filepath: Path, products: list[Product], region: Region) -> None:
    """
    Conditionally process Amazon Business Report data based on command line arguments.

    Args:
        args: Parsed command line arguments
        filepath: Directory path for data files
        products: List of Product objects
        region: Target region for processing
    """
    if not (args.business or not any([args.sellerboard, args.helium, args.campaigns, args.sns])):
        return

    if args.ongoing or not args.update:
        _run_phase(
            region,
            "BR current",
            lambda: business.load_current_data(
                filepath.joinpath(f"{region.value}_{DEFAULT_BUSINESS_REPORT_FILENAME}"),
                products,
            ),
            lambda: spreadsheets.add_current_business_data(products, region),
        )
    if args.update or not args.ongoing:
        _run_phase(
            region,
            "BR historical",
            lambda: business.load_historical_data(
                filepath.joinpath(f"{region.value}_{DEFAULT_BUSINESS_REPORT_UPDATE_FILENAME}"),
                products,
            ),
            lambda: spreadsheets.add_historical_business_data(products, region),
        )


def process_sns(args, filepath: Path, products: list[Product], region: Region) -> None:
    """
    Process Amazon Subscribe & Save performance data.

    Args:
       args: Parsed command line arguments
       filepath: Directory path for SNS data files
       products: List of Product objects
       region: Target region for processing
    """
    if not (args.sns or not any([args.sellerboard, args.business, args.campaigns, args.helium])):
        return

    performance_file = filepath.joinpath(f"{region.value}_{DEFAULT_SNS_PERFORMANCE_FILENAME}")
    products_file = filepath.joinpath(f"{region.value}_{DEFAULT_SNS_PRODUCTS_FILENAME}")
    _run_phase(
        region,
        "SNS",
        lambda: sns.load_sns_data(performance_file, products_file, products),
        lambda: spreadsheets.add_sns_data(products, region),
    )


def process_campaigns(args, filepath: Path, products: list[Product], region: Region):
    """
    Process Amazon PPC campaigns data for advertising performance.

    Args:
        args: Parsed command line arguments
        filepath: Directory path for campaign data files
        products: List of Product objects
        region: Target region for processing
    """
    if not (args.campaigns or not any([args.sellerboard, args.business, args.helium, args.sns])):
        return

    _run_phase(
        region,
        "PPC",
        lambda: campaign.load_campaigns(
            filepath.joinpath(f"{region.value}_{DEFAULT_CAMPAIGNS_FILENAME}"), products,
        ),
        lambda: spreadsheets.add_amazon_campaigns_data(products, region),
    )


def process_helium(args, products: list[Product], region: Region, target_date: str):
    """
    Process Helium 10 keyword ranking and weekly performance data.

    Args:
        args: Parsed command line arguments
        products: List of Product objects
        region: Target region for processing
        target_date: Target date for data processing
    """
    if not (args.helium or not any([args.sellerboard, args.business, args.campaigns, args.sns])):
        return

    products_to_update = [product for product in products if product.sheet_title]
    if _run_phase(
        region,
        "H10 keywords",
        lambda: helium.update_keywords(products_to_update, region),
        None,
    ):
        _run_phase(
            region,
            "H10 ranks",
            lambda: helium.load_weekly_rank_data(products_to_update, target_date),
            lambda: spreadsheets.add_helium_data(products_to_update, region),
        )


def main():
    args = parse_arguments()

    target_date = args.date if args.date else get_tuesday_target_date(announce=True)
    filepath = Path(args.filepath) if args.filepath else DEFAULT_FILEPATH
    regions = [Region(r) for r in args.region] if args.region else list(Region)

    info(f"Starting Weekly Monitor for {len(regions)} region(s) → {filepath}")
    for region in regions:
        info(f"--- Region {region.value} ---")
        try:
            products = load_products(region)
        except Exception as e:
            err(f"Failed to load products for {region.value}: {e}")
            traceback.print_exc()
            continue

        try:
            with StepTimer(f"[{region.value}] Assign row numbers"):
                spreadsheets.assign_update_row_numbers(products, region, target_date)
        except Exception as e:
            err(f"Row assignment failed for {region.value}: {e}")
            traceback.print_exc()
            continue

        process_sellerboard(args, filepath, products, region)
        process_business(args, filepath, products, region)
        if region.value in {"uk", "fr", "it", "es", "de"}:
            process_sns(args, filepath, products, region)
            process_campaigns(args, filepath, products, region)
            process_helium(args, products, region, target_date)

        ok(f"Region {region.value} completed")
    info("All regions processed.")


if __name__ == "__main__":
    main()
