"""
Build 5-minute windows from raw 1-second kline data.

Takes raw Binance CSVs and constructs:
1. Parquet files of clean 1s OHLCV data
2. 5-minute windows with intermediate snapshots at configurable intervals
3. Realized volatility calculations for each window

Output schema (per window):
    window_id:          int64   - sequential window identifier
    window_start:       int64   - ms timestamp of window open
    window_end:         int64   - ms timestamp of window close
    open_price:         float64 - price at window open
    close_price:        float64 - price at window close
    outcome:            int8    - 1 = UP (close >= open), 0 = DOWN
    prior_volatility:   float64 - realized vol over prior 60 minutes (annualized)
    vol_regime:         int8    - 0=low, 1=medium, 2=high (tercile-based)
    snapshots:          binary  - msgpack of {elapsed_s: relative_change} dict

Intermediate snapshots schema (stored per window):
    elapsed_seconds:    [5, 10, 15, 20, 25, 30, 45, 60, 90, 120, 150, 180, 210, 240, 270, 285, 295]
    relative_change:    (price_at_t - open_price) / open_price

Usage:
    python build_windows.py --input ../data/raw --output ../data/parquet
"""

import argparse
import json
import logging
import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Snapshot intervals in seconds from window open
# Dense early (where user hypothesizes edge), sparser later
SNAPSHOT_INTERVALS = [5, 10, 15, 20, 25, 30, 45, 60, 90, 120, 150, 180, 210, 240, 270, 285, 295]

WINDOW_DURATION_S = 300  # 5 minutes

# Prior volatility lookback in seconds (1 hour)
VOL_LOOKBACK_S = 3600

# Binance kline CSV columns
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
]


def load_raw_csvs_to_parquet(input_dir: Path, output_dir: Path) -> Path:
    """
    Load all raw CSVs into a single sorted Parquet file.
    Returns path to the consolidated Parquet file.
    """
    consolidated_path = output_dir / "btcusdt_1s.parquet"

    if consolidated_path.exists():
        logger.info(f"Consolidated Parquet already exists: {consolidated_path}")
        return consolidated_path

    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(input_dir.glob("BTCUSDT-1s-*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    logger.info(f"Loading {len(csv_files)} CSV files...")

    # Use DuckDB for fast CSV -> Parquet conversion
    con = duckdb.connect()

    # Build UNION ALL query across all CSVs
    csv_paths_str = ", ".join(f"'{str(f)}'" for f in csv_files)

    # Binance changed timestamp format from milliseconds to microseconds
    # around Jan 1, 2025. Normalize all timestamps to milliseconds.
    con.execute(f"""
        COPY (
            SELECT
                CASE
                    WHEN column0::BIGINT > 1e15 THEN (column0::BIGINT / 1000)
                    ELSE column0::BIGINT
                END AS open_time,
                column1::DOUBLE AS open,
                column2::DOUBLE AS high,
                column3::DOUBLE AS low,
                column4::DOUBLE AS close,
                column5::DOUBLE AS volume,
                CASE
                    WHEN column6::BIGINT > 1e15 THEN (column6::BIGINT / 1000)
                    ELSE column6::BIGINT
                END AS close_time,
                column7::DOUBLE AS quote_volume,
                column8::INTEGER AS trade_count,
                column9::DOUBLE AS taker_buy_base_volume,
                column10::DOUBLE AS taker_buy_quote_volume
            FROM read_csv(
                [{csv_paths_str}],
                header=false,
                auto_detect=false,
                columns={{
                    'column0': 'VARCHAR',
                    'column1': 'VARCHAR',
                    'column2': 'VARCHAR',
                    'column3': 'VARCHAR',
                    'column4': 'VARCHAR',
                    'column5': 'VARCHAR',
                    'column6': 'VARCHAR',
                    'column7': 'VARCHAR',
                    'column8': 'VARCHAR',
                    'column9': 'VARCHAR',
                    'column10': 'VARCHAR',
                    'column11': 'VARCHAR'
                }}
            )
            ORDER BY column0::BIGINT
        ) TO '{consolidated_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    con.close()

    file_size = consolidated_path.stat().st_size / (1024 * 1024)
    logger.info(f"Consolidated Parquet: {consolidated_path} ({file_size:.1f} MB)")
    return consolidated_path


def compute_rolling_volatility(prices: np.ndarray, timestamps: np.ndarray, lookback_ms: int) -> np.ndarray:
    """
    Compute rolling realized volatility (annualized) for each timestamp.

    Uses log returns over 1-minute intervals within the lookback window.
    Returns array of same length as prices, with NaN where insufficient data.
    """
    n = len(prices)
    vol = np.full(n, np.nan)

    # Compute 60-second returns (every 60th element)
    # For efficiency, precompute log prices
    log_prices = np.log(prices)

    # We need at least lookback_ms worth of data
    lookback_samples = lookback_ms // 1000  # 1s candles

    for i in range(lookback_samples, n):
        # Get 1-minute returns within lookback window
        window_start = i - lookback_samples
        window_log_prices = log_prices[window_start:i+1]

        # Sample every 60s for minute returns
        minute_indices = np.arange(0, len(window_log_prices), 60)
        if len(minute_indices) < 3:
            continue

        minute_prices = window_log_prices[minute_indices]
        minute_returns = np.diff(minute_prices)

        if len(minute_returns) > 1:
            # Annualized volatility (525600 minutes per year)
            vol[i] = np.std(minute_returns, ddof=1) * np.sqrt(525600)

    return vol


def compute_rolling_volatility_fast(con: duckdb.DuckDBPyConnection, parquet_path: str) -> None:
    """
    Compute rolling 1-hour realized volatility using DuckDB window functions.
    Much faster than the numpy approach for large datasets.

    Adds vol_1h column to a new table.
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE klines AS
        SELECT
            open_time,
            close,
            -- Compute 1-minute log returns
            LN(close / LAG(close, 60) OVER (ORDER BY open_time)) AS log_return_1m,
        FROM '{parquet_path}'
        ORDER BY open_time
    """)

    # Compute rolling std dev of 1-minute returns over 1-hour window (3600 rows)
    con.execute("""
        CREATE OR REPLACE TABLE klines_with_vol AS
        SELECT
            open_time,
            close,
            -- Rolling stddev of 1-min returns over prior 3600 seconds, annualized
            STDDEV_SAMP(log_return_1m) OVER (
                ORDER BY open_time
                ROWS BETWEEN 3600 PRECEDING AND CURRENT ROW
            ) * SQRT(525600) AS vol_1h
        FROM klines
        ORDER BY open_time
    """)


def build_windows(parquet_path: Path, output_path: Path) -> dict:
    """
    Build 5-minute windows with snapshots and volatility from the consolidated Parquet.

    Returns summary statistics.
    """
    if output_path.exists():
        logger.info(f"Windows file already exists: {output_path}")
        # Return basic stats
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM '{output_path}'").fetchone()[0]
        con.close()
        return {"total_windows": count}

    con = duckdb.connect()

    logger.info("Computing rolling volatility...")
    compute_rolling_volatility_fast(con, str(parquet_path))

    # Get data range
    result = con.execute("SELECT MIN(open_time), MAX(open_time), COUNT(*) FROM klines_with_vol").fetchone()
    min_ts, max_ts, total_rows = result
    logger.info(f"Data range: {pd.Timestamp(min_ts, unit='ms')} to {pd.Timestamp(max_ts, unit='ms')}")
    logger.info(f"Total 1s candles: {total_rows:,}")

    # Build snapshot query columns
    snapshot_selects = []
    for s in SNAPSHOT_INTERVALS:
        offset_ms = s * 1000
        snapshot_selects.append(f"""
            (SELECT close FROM klines_with_vol
             WHERE open_time = w.window_start + {offset_ms}) AS price_at_{s}s""")

    snapshot_cols = ",\n".join(snapshot_selects)

    # Generate window boundaries aligned to 5-minute intervals
    # Polymarket uses exact 5-minute boundaries (e.g., 00:00, 00:05, 00:10...)
    # We align to the same boundaries for our analysis
    logger.info("Building 5-minute windows...")

    con.execute(f"""
        CREATE OR REPLACE TABLE windows AS
        WITH window_boundaries AS (
            -- Generate 5-minute aligned timestamps
            SELECT
                open_time AS window_start,
                open_time + {WINDOW_DURATION_S * 1000} AS window_end
            FROM klines_with_vol
            WHERE open_time % {WINDOW_DURATION_S * 1000} = 0
              AND open_time + {WINDOW_DURATION_S * 1000} <= {max_ts}
              -- Skip first hour (need volatility lookback)
              AND open_time >= {min_ts} + {VOL_LOOKBACK_S * 1000}
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY w.window_start) AS window_id,
            w.window_start,
            w.window_end,
            open_k.close AS open_price,
            close_k.close AS close_price,
            CASE WHEN close_k.close >= open_k.close THEN 1 ELSE 0 END AS outcome,
            open_k.vol_1h AS prior_volatility,
            {snapshot_cols}
        FROM window_boundaries w
        JOIN klines_with_vol open_k ON open_k.open_time = w.window_start
        JOIN klines_with_vol close_k ON close_k.open_time = w.window_end - 1000
        ORDER BY w.window_start
    """)

    total_windows = con.execute("SELECT COUNT(*) FROM windows").fetchone()[0]
    up_count = con.execute("SELECT COUNT(*) FROM windows WHERE outcome = 1").fetchone()[0]
    logger.info(f"Total windows: {total_windows:,}")
    logger.info(f"UP outcomes: {up_count:,} ({100*up_count/total_windows:.1f}%)")
    logger.info(f"DOWN outcomes: {total_windows - up_count:,} ({100*(total_windows-up_count)/total_windows:.1f}%)")

    # Compute volatility regime terciles
    con.execute("""
        CREATE OR REPLACE TABLE windows_with_regime AS
        SELECT
            *,
            NTILE(3) OVER (ORDER BY prior_volatility) - 1 AS vol_regime
        FROM windows
        WHERE prior_volatility IS NOT NULL
    """)

    # Now build the snapshot JSON column for compact storage
    snapshot_json_parts = []
    for s in SNAPSHOT_INTERVALS:
        snapshot_json_parts.append(
            f"'{s}', CASE WHEN price_at_{s}s IS NOT NULL "
            f"THEN (price_at_{s}s - open_price) / open_price ELSE NULL END"
        )
    snapshot_json_expr = "json_object(" + ", ".join(snapshot_json_parts) + ")"

    # Select final columns and write to Parquet
    price_cols = ", ".join(f"price_at_{s}s" for s in SNAPSHOT_INTERVALS)
    relative_change_cols = []
    for s in SNAPSHOT_INTERVALS:
        relative_change_cols.append(
            f"(price_at_{s}s - open_price) / open_price AS change_at_{s}s"
        )
    change_cols_sql = ", ".join(relative_change_cols)

    con.execute(f"""
        COPY (
            SELECT
                window_id,
                window_start,
                window_end,
                open_price,
                close_price,
                outcome,
                prior_volatility,
                vol_regime,
                (close_price - open_price) / open_price AS total_return,
                {change_cols_sql}
            FROM windows_with_regime
            ORDER BY window_start
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Summary stats
    stats = con.execute(f"""
        SELECT
            COUNT(*) AS total_windows,
            SUM(outcome) AS up_count,
            COUNT(*) - SUM(outcome) AS down_count,
            AVG(prior_volatility) AS avg_vol,
            PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY prior_volatility) AS vol_p33,
            PERCENTILE_CONT(0.66) WITHIN GROUP (ORDER BY prior_volatility) AS vol_p66,
            MIN(window_start) AS first_window,
            MAX(window_start) AS last_window
        FROM '{output_path}'
    """).fetchdf()

    con.close()

    logger.info(f"\nWindows written to {output_path}")
    logger.info(f"Volatility tercile boundaries: P33={stats['vol_p33'].iloc[0]:.4f}, P66={stats['vol_p66'].iloc[0]:.4f}")

    return stats.to_dict(orient="records")[0]


def main():
    parser = argparse.ArgumentParser(description="Build 5-minute windows from raw kline data")
    parser.add_argument("--input", type=str, default="../data/raw", help="Raw CSV directory")
    parser.add_argument("--output", type=str, default="../data/parquet", help="Parquet output directory")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Consolidate CSVs to Parquet
    parquet_path = load_raw_csvs_to_parquet(input_dir, output_dir)

    # Step 2: Build windows
    windows_path = output_dir / "windows_5m.parquet"
    stats = build_windows(parquet_path, windows_path)

    logger.info(f"\nSummary: {json.dumps(stats, indent=2, default=str)}")


if __name__ == "__main__":
    main()
