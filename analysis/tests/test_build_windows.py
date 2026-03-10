"""Tests for the window building module."""

import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.build_windows import (
    SNAPSHOT_INTERVALS,
    WINDOW_DURATION_S,
    compute_rolling_volatility_fast,
)


class TestComputeRollingVolatilityFast:
    def test_produces_volatility_values(self, tmp_path):
        """Volatility computation should produce non-null values after lookback period."""
        # Create synthetic 1s price data: random walk
        np.random.seed(42)
        n = 7200  # 2 hours of 1s data
        base_price = 50000.0
        returns = np.random.normal(0, 0.0001, n)
        prices = base_price * np.exp(np.cumsum(returns))

        # Create timestamps (1s apart, ms precision)
        start_ts = 1704067200000  # 2024-01-01 00:00:00 UTC
        timestamps = [start_ts + i * 1000 for i in range(n)]

        # Write to parquet
        df = pd.DataFrame({"open_time": timestamps, "close": prices})
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(parquet_path, index=False)

        # Run volatility computation
        con = duckdb.connect()
        compute_rolling_volatility_fast(con, str(parquet_path))

        result = con.execute("SELECT COUNT(*), COUNT(vol_1h) FROM klines_with_vol").fetchone()
        total, non_null = result
        con.close()

        assert total == n
        # After 3600s lookback + 60s for first return, most should have vol
        assert non_null > n - 3700

    def test_volatility_scales_with_price_movement(self, tmp_path):
        """Higher price movement should produce higher volatility."""
        np.random.seed(42)
        n = 7200

        start_ts = 1704067200000

        # Low volatility period
        low_vol_returns = np.random.normal(0, 0.00005, n // 2)
        # High volatility period
        high_vol_returns = np.random.normal(0, 0.0005, n // 2)

        returns = np.concatenate([low_vol_returns, high_vol_returns])
        prices = 50000.0 * np.exp(np.cumsum(returns))
        timestamps = [start_ts + i * 1000 for i in range(n)]

        df = pd.DataFrame({"open_time": timestamps, "close": prices})
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(parquet_path, index=False)

        con = duckdb.connect()
        compute_rolling_volatility_fast(con, str(parquet_path))

        # Compare volatility in first half vs second half (with buffer for lookback)
        result = con.execute("""
            SELECT
                AVG(CASE WHEN open_time < ? THEN vol_1h END) AS low_vol_avg,
                AVG(CASE WHEN open_time >= ? THEN vol_1h END) AS high_vol_avg
            FROM klines_with_vol
            WHERE vol_1h IS NOT NULL
        """, [start_ts + (n // 2) * 1000, start_ts + (n // 2 + 3600) * 1000]).fetchone()

        low_vol_avg, high_vol_avg = result
        con.close()

        # High vol period should have meaningfully higher realized vol
        if low_vol_avg is not None and high_vol_avg is not None:
            assert high_vol_avg > low_vol_avg * 2, (
                f"High vol ({high_vol_avg:.4f}) should be > 2x low vol ({low_vol_avg:.4f})"
            )


class TestSnapshotIntervals:
    def test_intervals_sorted(self):
        assert SNAPSHOT_INTERVALS == sorted(SNAPSHOT_INTERVALS)

    def test_intervals_within_window(self):
        for interval in SNAPSHOT_INTERVALS:
            assert 0 < interval < WINDOW_DURATION_S

    def test_dense_early_sparse_late(self):
        """Early intervals should be more dense than late ones."""
        early = [s for s in SNAPSHOT_INTERVALS if s <= 30]
        late = [s for s in SNAPSHOT_INTERVALS if s > 30]
        # More intervals in first 30s than average density of the rest
        early_density = len(early) / 30
        late_density = len(late) / 270
        assert early_density > late_density
