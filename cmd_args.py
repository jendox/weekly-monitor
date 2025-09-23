import argparse

from models import Region


def parse_arguments():
    """
    Parse command line arguments for the SuperSelf monitor data processing tool.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Process weekly SuperSelf monitor data for Amazon sales analytics",
        epilog="Example: python monitor.py -d '25/Feb/25' -r uk -s -b",
    )

    parser.add_argument(
        "-d", "--date",
        help="Target date to process in format 'DD/Mon/YY' (e.g., '25/Feb/25')",
    )
    parser.add_argument(
        "-p", "--filepath",
        help="Custom filepath for data files (overrides default download location)",
    )
    parser.add_argument(
        "-r", "--region",
        choices=[r.name for r in Region],
        nargs="+",
        help="One or more regions (e.g. uk us de)",
    )
    parser.add_argument(
        "-s", "--sellerboard",
        action="store_true",
        help="Process only Sellerboard revenue and advertising data",
    )
    parser.add_argument(
        "-b", "--business",
        action="store_true",
        help="Process only Amazon Business Report data (sales and units)",
    )
    parser.add_argument(
        "-m", "--helium",
        action="store_true",
        help="Process only Helium 10 keyword ranking data",
    )
    parser.add_argument(
        "-c", "--campaigns",
        action="store_true",
        help="Process only Amazon PPC campaigns data",
    )
    parser.add_argument(
        "-u", "--update",
        action="store_true",
        help="Process only update data (historical corrections)",
    )
    parser.add_argument(
        "-o", "--ongoing",
        action="store_true",
        help="Process only ongoing data (current week)",
    )
    parser.add_argument(
        "-n", "--sns",
        action="store_true",
        help="Process only Subscribe & Save performance data",
    )

    return parser.parse_args()
