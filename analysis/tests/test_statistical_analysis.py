"""Tests for the statistical analysis module."""

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analysis.statistical_analysis import (
    BucketStats,
    compute_bayesian_stats,
    compute_bucket_boundaries,
    analyze_single_snapshot,
    query_lookup,
    save_to_sqlite,
)


class TestComputeBayesianStats:
    def test_equal_counts_near_fifty(self):
        """50/50 split should give posterior near 0.5."""
        mean, ci_low, ci_high = compute_bayesian_stats(500, 1000)
        assert abs(mean - 0.5) < 0.01
        assert ci_low < 0.5
        assert ci_high > 0.5

    def test_strong_up_bias(self):
        """900/1000 UP should give high posterior."""
        mean, ci_low, ci_high = compute_bayesian_stats(900, 1000)
        assert mean > 0.85
        assert ci_low > 0.5  # Significant

    def test_strong_down_bias(self):
        """100/1000 UP should give low posterior."""
        mean, ci_low, ci_high = compute_bayesian_stats(100, 1000)
        assert mean < 0.15
        assert ci_high < 0.5  # Significant

    def test_small_sample_shrinks_to_prior(self):
        """With few samples, posterior should be close to 0.5 (prior)."""
        mean, _, _ = compute_bayesian_stats(3, 5, prior_alpha=2, prior_beta=2)
        # With Beta(2,2) prior and 3/5 data: Beta(5, 4), mean = 5/9 ≈ 0.556
        assert 0.5 < mean < 0.6

    def test_zero_counts(self):
        """Zero observations should return prior."""
        mean, ci_low, ci_high = compute_bayesian_stats(0, 0, prior_alpha=2, prior_beta=2)
        assert abs(mean - 0.5) < 0.01

    def test_credible_interval_ordering(self):
        """CI lower should always be less than CI upper."""
        for up, total in [(10, 100), (50, 100), (90, 100), (1, 5), (499, 500)]:
            _, ci_low, ci_high = compute_bayesian_stats(up, total)
            assert ci_low < ci_high

    def test_credible_interval_contains_mean(self):
        """95% CI should contain the posterior mean."""
        for up, total in [(10, 100), (50, 100), (90, 100)]:
            mean, ci_low, ci_high = compute_bayesian_stats(up, total)
            assert ci_low <= mean <= ci_high


class TestComputeBucketBoundaries:
    def test_basic_quantiles(self):
        values = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], dtype=float)
        boundaries = compute_bucket_boundaries(values, 5)
        assert len(boundaries) == 6  # n_buckets + 1
        assert boundaries[0] == float("-inf")
        assert boundaries[-1] == float("inf")

    def test_handles_nan(self):
        values = np.array([1, np.nan, 3, np.nan, 5, 6, 7, 8, np.nan, 10], dtype=float)
        boundaries = compute_bucket_boundaries(values, 3)
        assert len(boundaries) == 4
        assert not np.any(np.isnan(boundaries[1:-1]))

    def test_empty_array(self):
        values = np.array([], dtype=float)
        boundaries = compute_bucket_boundaries(values, 5)
        assert len(boundaries) == 0

    def test_all_nan(self):
        values = np.array([np.nan, np.nan, np.nan], dtype=float)
        boundaries = compute_bucket_boundaries(values, 3)
        assert len(boundaries) == 0


class TestAnalyzeSingleSnapshot:
    @pytest.fixture
    def sample_df(self):
        """Create a sample DataFrame mimicking window data."""
        np.random.seed(42)
        n = 3000

        # Simulate: when price is up, slightly higher chance of UP outcome
        changes = np.random.normal(0, 0.001, n)
        # Base 50% probability, shifted by change direction
        prob_up = 0.5 + 0.05 * np.sign(changes)
        outcomes = (np.random.random(n) < prob_up).astype(int)

        return pd.DataFrame({
            "change_at_30s": changes,
            "outcome": outcomes,
            "vol_regime": np.random.choice([0, 1, 2], n),
        })

    def test_returns_results(self, sample_df):
        results = analyze_single_snapshot(sample_df, 30, n_change_buckets=10, by_vol_regime=False)
        assert len(results) > 0
        assert all(isinstance(r, BucketStats) for r in results)

    def test_all_fields_populated(self, sample_df):
        results = analyze_single_snapshot(sample_df, 30, n_change_buckets=10, by_vol_regime=False)
        for r in results:
            assert r.total_count >= 30  # MIN_SAMPLES
            assert r.up_count + r.down_count == r.total_count
            assert 0 <= r.empirical_prob_up <= 1
            assert 0 <= r.bayesian_prob_up <= 1
            assert r.credible_lower <= r.bayesian_prob_up <= r.credible_upper

    def test_with_vol_regime(self, sample_df):
        results = analyze_single_snapshot(sample_df, 30, n_change_buckets=5, by_vol_regime=True)
        vol_regimes = set(r.vol_regime for r in results)
        assert -1 in vol_regimes  # "all" should always be present
        # At least some regime-specific results
        assert len(vol_regimes) > 1

    def test_missing_column_returns_empty(self, sample_df):
        results = analyze_single_snapshot(sample_df, 999, n_change_buckets=10)
        assert results == []

    def test_detects_bias(self, sample_df):
        """The synthetic data has a directional bias - analysis should detect it."""
        results = analyze_single_snapshot(sample_df, 30, n_change_buckets=10, by_vol_regime=False)
        # Extreme buckets (far positive or negative changes) should show bias
        extreme_results = [r for r in results if r.vol_regime == -1 and abs(r.edge_vs_50) > 1]
        assert len(extreme_results) > 0, "Should detect some bias in synthetic data"


class TestSaveAndQueryLookup:
    @pytest.fixture
    def sample_results(self):
        return [
            BucketStats(
                time_elapsed_s=30,
                change_bucket_lower=-0.001,
                change_bucket_upper=0.0,
                vol_regime=-1,
                total_count=500,
                up_count=220,
                down_count=280,
                empirical_prob_up=0.44,
                bayesian_prob_up=0.441,
                credible_lower=0.396,
                credible_upper=0.487,
                edge_vs_50=-5.9,
                is_significant=True,
            ),
            BucketStats(
                time_elapsed_s=30,
                change_bucket_lower=0.0,
                change_bucket_upper=0.001,
                vol_regime=-1,
                total_count=500,
                up_count=280,
                down_count=220,
                empirical_prob_up=0.56,
                bayesian_prob_up=0.559,
                credible_lower=0.513,
                credible_upper=0.604,
                edge_vs_50=5.9,
                is_significant=True,
            ),
            BucketStats(
                time_elapsed_s=60,
                change_bucket_lower=-0.001,
                change_bucket_upper=0.001,
                vol_regime=-1,
                total_count=1000,
                up_count=505,
                down_count=495,
                empirical_prob_up=0.505,
                bayesian_prob_up=0.505,
                credible_lower=0.473,
                credible_upper=0.536,
                edge_vs_50=0.5,
                is_significant=False,
            ),
        ]

    def test_save_and_query(self, tmp_path, sample_results):
        db_path = tmp_path / "test_lookup.db"
        save_to_sqlite(sample_results, db_path)

        # Query for a price down at T+30s
        result = query_lookup(db_path, 30, -0.0005, vol_regime=-1)
        assert result is not None
        assert result["prob_up"] < 0.5
        assert result["significant"] is True

    def test_query_price_up(self, tmp_path, sample_results):
        db_path = tmp_path / "test_lookup.db"
        save_to_sqlite(sample_results, db_path)

        result = query_lookup(db_path, 30, 0.0005, vol_regime=-1)
        assert result is not None
        assert result["prob_up"] > 0.5

    def test_query_nearest_time(self, tmp_path, sample_results):
        db_path = tmp_path / "test_lookup.db"
        save_to_sqlite(sample_results, db_path)

        # Query for T+35s, should snap to T+30s (nearest available)
        result = query_lookup(db_path, 35, 0.0005, vol_regime=-1)
        assert result is not None
        assert result["time_elapsed_s"] == 30

    def test_query_no_match(self, tmp_path, sample_results):
        db_path = tmp_path / "test_lookup.db"
        save_to_sqlite(sample_results, db_path)

        # Query for vol_regime=2 which doesn't exist in sample
        result = query_lookup(db_path, 30, 0.0005, vol_regime=2)
        assert result is None

    def test_metadata_saved(self, tmp_path, sample_results):
        db_path = tmp_path / "test_lookup.db"
        save_to_sqlite(sample_results, db_path)

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM metadata WHERE key='total_entries'")
        total = int(cursor.fetchone()[0])
        conn.close()

        assert total == 3
