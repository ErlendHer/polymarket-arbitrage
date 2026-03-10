"""
Download historical 1-second BTC/USDT kline data from Binance.

Source: https://data.binance.vision/
Downloads daily ZIP files containing 1s OHLCV candles for BTCUSDT.
Each file is ~3-5MB compressed, ~15-20MB uncompressed.
Total for 2 years: ~2-3GB compressed.

Usage:
    python download_data.py --start 2024-03-10 --end 2026-03-10 --output ../data/raw
    python download_data.py --start 2024-03-10 --end 2026-03-10 --output ../data/raw --verify-only
"""

import argparse
import hashlib
import io
import logging
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s"
CHECKSUM_SUFFIX = ".CHECKSUM"
PAIR = "BTCUSDT"
INTERVAL = "1s"

# Expected columns in Binance kline CSVs
KLINE_COLUMNS = [
    "open_time",    # ms timestamp
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",   # ms timestamp
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def date_range(start: date, end: date):
    """Yield dates from start to end (exclusive)."""
    current = start
    while current < end:
        yield current
        current += timedelta(days=1)


def build_filename(d: date) -> str:
    """Build the expected ZIP filename for a given date."""
    return f"{PAIR}-{INTERVAL}-{d.isoformat()}.zip"


def build_url(d: date) -> str:
    """Build the download URL for a given date."""
    return f"{BASE_URL}/{build_filename(d)}"


def build_checksum_url(d: date) -> str:
    """Build the checksum URL for a given date."""
    return f"{BASE_URL}/{build_filename(d)}{CHECKSUM_SUFFIX}"


def verify_checksum(zip_path: Path, expected_sha256: str) -> bool:
    """Verify SHA256 checksum of a downloaded file."""
    sha256 = hashlib.sha256()
    with open(zip_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest() == expected_sha256


def download_day(d: date, output_dir: Path, session: requests.Session) -> dict:
    """
    Download one day of kline data.

    Returns dict with status info:
        {"date": str, "status": "ok"|"skipped"|"not_available"|"error", "message": str}
    """
    zip_filename = build_filename(d)
    csv_filename = zip_filename.replace(".zip", ".csv")
    csv_path = output_dir / csv_filename
    zip_path = output_dir / zip_filename

    # Skip if CSV already exists
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return {"date": d.isoformat(), "status": "skipped", "message": "CSV exists"}

    url = build_url(d)

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return {"date": d.isoformat(), "status": "not_available", "message": "No data for this date"}
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"date": d.isoformat(), "status": "error", "message": str(e)}

    # Verify checksum if available
    try:
        checksum_resp = session.get(build_checksum_url(d), timeout=10)
        if checksum_resp.status_code == 200:
            # Format: "<sha256>  <filename>"
            expected_hash = checksum_resp.text.strip().split()[0]
            actual_hash = hashlib.sha256(resp.content).hexdigest()
            if actual_hash != expected_hash:
                return {"date": d.isoformat(), "status": "error", "message": "Checksum mismatch"}
    except requests.RequestException:
        pass  # Checksum verification is best-effort

    # Extract CSV from ZIP
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            names = zf.namelist()
            if not names:
                return {"date": d.isoformat(), "status": "error", "message": "Empty ZIP"}
            # Extract the CSV (usually the only file)
            csv_name = [n for n in names if n.endswith(".csv")]
            if not csv_name:
                return {"date": d.isoformat(), "status": "error", "message": "No CSV in ZIP"}

            csv_data = zf.read(csv_name[0])
            csv_path.write_bytes(csv_data)
    except zipfile.BadZipFile:
        return {"date": d.isoformat(), "status": "error", "message": "Corrupt ZIP"}

    return {"date": d.isoformat(), "status": "ok", "message": f"{csv_path.stat().st_size:,} bytes"}


def download_range(
    start: date,
    end: date,
    output_dir: Path,
    max_workers: int = 8,
    verify_only: bool = False,
) -> dict:
    """
    Download kline data for a date range.

    Returns summary statistics.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    dates = list(date_range(start, end))

    if verify_only:
        logger.info(f"Verifying {len(dates)} days of data in {output_dir}")
        missing = []
        for d in dates:
            csv_path = output_dir / build_filename(d).replace(".zip", ".csv")
            if not csv_path.exists() or csv_path.stat().st_size == 0:
                missing.append(d.isoformat())
        logger.info(f"Missing: {len(missing)} days")
        if missing:
            logger.info(f"First missing: {missing[0]}, Last missing: {missing[-1]}")
        return {"total": len(dates), "missing": len(missing), "missing_dates": missing}

    logger.info(f"Downloading {len(dates)} days of {PAIR} {INTERVAL} data to {output_dir}")

    results = {"ok": 0, "skipped": 0, "not_available": 0, "error": 0, "errors": []}

    session = requests.Session()
    session.headers.update({"User-Agent": "polymarket-arbitrage-research/1.0"})

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_day, d, output_dir, session): d for d in dates}

        with tqdm(total=len(dates), desc="Downloading", unit="day") as pbar:
            for future in as_completed(futures):
                result = future.result()
                results[result["status"]] += 1
                if result["status"] == "error":
                    results["errors"].append(result)
                    logger.warning(f"Error for {result['date']}: {result['message']}")
                pbar.update(1)

    logger.info(
        f"Done: {results['ok']} downloaded, {results['skipped']} skipped, "
        f"{results['not_available']} unavailable, {results['error']} errors"
    )
    return results


def main():
    parser = argparse.ArgumentParser(description="Download Binance 1s kline data for BTCUSDT")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="../data/raw", help="Output directory")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent downloads")
    parser.add_argument("--verify-only", action="store_true", help="Only check for missing data")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    output_dir = Path(args.output)

    results = download_range(start, end, output_dir, args.workers, args.verify_only)

    if args.verify_only and results["missing"] > 0:
        print(f"\nTo download missing data, run without --verify-only")


if __name__ == "__main__":
    main()
