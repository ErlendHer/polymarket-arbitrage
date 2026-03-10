"""
Full analysis pipeline orchestrator.

Runs all steps in sequence:
1. Download historical data
2. Build 5-minute windows
3. Run statistical analysis
4. Generate visualizations

Usage:
    python run_pipeline.py --start 2024-03-10 --end 2026-03-10
    python run_pipeline.py --start 2024-03-10 --end 2026-03-10 --skip-download
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Paths relative to this file
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "parquet"
CHARTS_DIR = DATA_DIR / "charts"
LOOKUP_DB = DATA_DIR / "lookup.db"


def run_download(start_date: str, end_date: str):
    from download_data import download_range

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    logger.info("=" * 60)
    logger.info("STEP 1: Downloading historical data")
    logger.info("=" * 60)

    results = download_range(start, end, RAW_DIR, max_workers=8)
    logger.info(f"Download complete: {results}")

    if results.get("error", 0) > 0:
        logger.warning(f"{results['error']} download errors occurred. Check logs above.")

    return results


def run_build_windows():
    from build_windows import load_raw_csvs_to_parquet, build_windows

    logger.info("=" * 60)
    logger.info("STEP 2: Building 5-minute windows")
    logger.info("=" * 60)

    parquet_path = load_raw_csvs_to_parquet(RAW_DIR, PARQUET_DIR)
    windows_path = PARQUET_DIR / "windows_5m.parquet"
    stats = build_windows(parquet_path, windows_path)
    logger.info(f"Window stats: {stats}")

    return stats


def run_analysis(n_buckets: int = 15):
    from statistical_analysis import run_full_analysis, save_to_sqlite, print_summary_table

    logger.info("=" * 60)
    logger.info("STEP 3: Running statistical analysis")
    logger.info("=" * 60)

    windows_path = PARQUET_DIR / "windows_5m.parquet"
    results = run_full_analysis(windows_path, n_change_buckets=n_buckets)
    print_summary_table(results)
    save_to_sqlite(results, LOOKUP_DB)

    return results


def run_visualize():
    from visualize import generate_all_charts

    logger.info("=" * 60)
    logger.info("STEP 4: Generating visualizations")
    logger.info("=" * 60)

    generate_all_charts(LOOKUP_DB, CHARTS_DIR)


def main():
    parser = argparse.ArgumentParser(description="Run full analysis pipeline")
    parser.add_argument("--start", type=str, default="2024-03-10", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2026-03-10", help="End date (YYYY-MM-DD)")
    parser.add_argument("--skip-download", action="store_true", help="Skip download step")
    parser.add_argument("--skip-windows", action="store_true", help="Skip window building step")
    parser.add_argument("--buckets", type=int, default=15, help="Number of price change buckets")
    parser.add_argument("--analysis-only", action="store_true", help="Only run analysis + viz")
    args = parser.parse_args()

    if args.analysis_only:
        args.skip_download = True
        args.skip_windows = True

    if not args.skip_download:
        run_download(args.start, args.end)

    if not args.skip_windows:
        run_build_windows()

    run_analysis(n_buckets=args.buckets)
    run_visualize()

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Lookup database: {LOOKUP_DB}")
    logger.info(f"Charts: {CHARTS_DIR}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
