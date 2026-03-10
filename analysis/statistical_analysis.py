"""
Statistical analysis engine for 5-minute BTC windows.

Builds a conditional probability knowledge base:
    P(UP | time_elapsed, price_change_bucket, volatility_regime)

Uses empirical Bayesian estimation with Beta-Binomial conjugate priors.
Implements adaptive quantile-based bucketing for optimal sample distribution.

Key concepts:
    - Beta(alpha, beta) prior for each bucket's up-probability
    - Posterior: Beta(alpha + ups, beta + downs)
    - Posterior mean: (alpha + ups) / (alpha + beta + n)
    - 95% credible interval from Beta distribution quantiles
    - Bayesian shrinkage naturally handles sparse buckets

Usage:
    python statistical_analysis.py --input ../data/parquet/windows_5m.parquet --output ../data/lookup.db
"""

import argparse
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Snapshot intervals we have data for
SNAPSHOT_INTERVALS = [5, 10, 15, 20, 25, 30, 45, 60, 90, 120, 150, 180, 210, 240, 270, 285, 295]

# Bayesian prior parameters (weakly informative: near 50/50)
# Beta(2, 2) gives slight regularization toward 50% without being too strong
PRIOR_ALPHA = 2.0
PRIOR_BETA = 2.0

# Minimum samples for a bucket to be considered actionable
MIN_SAMPLES = 30

# Minimum edge (in percentage points) to flag as potentially exploitable
MIN_EDGE_PP = 1.0


@dataclass
class BucketStats:
    """Statistics for a single bucket in the knowledge base."""
    time_elapsed_s: int
    change_bucket_lower: float
    change_bucket_upper: float
    vol_regime: int  # 0=low, 1=med, 2=high, -1=all
    total_count: int
    up_count: int
    down_count: int
    empirical_prob_up: float
    bayesian_prob_up: float
    credible_lower: float  # 2.5th percentile
    credible_upper: float  # 97.5th percentile
    edge_vs_50: float      # bayesian_prob_up - 0.5 (in pp)
    is_significant: bool   # credible interval excludes 0.5


def compute_bucket_boundaries(values: np.ndarray, n_buckets: int) -> np.ndarray:
    """
    Compute quantile-based bucket boundaries for roughly equal sample sizes.

    Returns array of n_buckets+1 boundary values (including -inf and +inf edges).
    """
    # Remove NaN
    clean = values[~np.isnan(values)]
    if len(clean) == 0:
        return np.array([])

    # Use quantiles for interior boundaries
    quantiles = np.linspace(0, 1, n_buckets + 1)
    boundaries = np.quantile(clean, quantiles)

    # Replace first and last with -inf/+inf
    boundaries[0] = -np.inf
    boundaries[-1] = np.inf

    return boundaries


def compute_bayesian_stats(
    up_count: int,
    total_count: int,
    prior_alpha: float = PRIOR_ALPHA,
    prior_beta: float = PRIOR_BETA,
) -> tuple[float, float, float]:
    """
    Compute Bayesian posterior statistics for up-probability.

    Returns: (posterior_mean, credible_lower_2.5, credible_upper_97.5)
    """
    down_count = total_count - up_count
    post_alpha = prior_alpha + up_count
    post_beta = prior_beta + down_count

    posterior_mean = post_alpha / (post_alpha + post_beta)

    # 95% credible interval
    ci_lower = scipy_stats.beta.ppf(0.025, post_alpha, post_beta)
    ci_upper = scipy_stats.beta.ppf(0.975, post_alpha, post_beta)

    return posterior_mean, ci_lower, ci_upper


def analyze_single_snapshot(
    df: pd.DataFrame,
    elapsed_s: int,
    n_change_buckets: int = 15,
    by_vol_regime: bool = True,
) -> list[BucketStats]:
    """
    Analyze a single snapshot interval across all windows.

    Args:
        df: DataFrame with columns: outcome, change_at_{elapsed_s}s, vol_regime
        elapsed_s: seconds from window open
        n_change_buckets: number of price change buckets
        by_vol_regime: whether to split by volatility regime

    Returns: list of BucketStats
    """
    col = f"change_at_{elapsed_s}s"
    if col not in df.columns:
        return []

    # Drop rows with missing snapshot data
    mask = df[col].notna()
    working = df.loc[mask, [col, "outcome", "vol_regime"]].copy()

    if len(working) < MIN_SAMPLES:
        return []

    # Compute bucket boundaries from data distribution
    boundaries = compute_bucket_boundaries(working[col].values, n_change_buckets)
    if len(boundaries) == 0:
        return []

    # Remove duplicate boundaries (happens when many values are identical, e.g., 0.0 at T+5s)
    boundaries = np.unique(boundaries)
    actual_n_buckets = len(boundaries) - 1
    if actual_n_buckets < 2:
        return []

    # Assign buckets
    working["change_bucket"] = pd.cut(
        working[col],
        bins=boundaries,
        labels=range(actual_n_buckets),
        include_lowest=True,
    )

    results = []
    vol_regimes = [-1]  # -1 means "all regimes combined"
    if by_vol_regime:
        vol_regimes.extend([0, 1, 2])

    for vr in vol_regimes:
        if vr == -1:
            subset = working
        else:
            subset = working[working["vol_regime"] == vr]

        if len(subset) < MIN_SAMPLES:
            continue

        for bucket_idx in range(actual_n_buckets):
            bucket_mask = subset["change_bucket"] == bucket_idx
            bucket_data = subset[bucket_mask]

            total = len(bucket_data)
            if total < MIN_SAMPLES:
                continue

            ups = int(bucket_data["outcome"].sum())
            downs = total - ups

            empirical = ups / total
            bayesian, ci_lower, ci_upper = compute_bayesian_stats(ups, total)

            # Is the credible interval entirely above or below 0.5?
            significant = ci_lower > 0.5 or ci_upper < 0.5

            edge = (bayesian - 0.5) * 100  # in percentage points

            bucket_lower = float(boundaries[bucket_idx])
            bucket_upper = float(boundaries[bucket_idx + 1])

            results.append(BucketStats(
                time_elapsed_s=elapsed_s,
                change_bucket_lower=bucket_lower,
                change_bucket_upper=bucket_upper,
                vol_regime=vr,
                total_count=total,
                up_count=ups,
                down_count=downs,
                empirical_prob_up=empirical,
                bayesian_prob_up=bayesian,
                credible_lower=ci_lower,
                credible_upper=ci_upper,
                edge_vs_50=edge,
                is_significant=significant,
            ))

    return results


def run_full_analysis(
    windows_path: Path,
    n_change_buckets: int = 15,
    by_vol_regime: bool = True,
) -> list[BucketStats]:
    """
    Run the full statistical analysis across all snapshot intervals.

    Returns list of all BucketStats.
    """
    logger.info(f"Loading windows from {windows_path}...")
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{windows_path}'").fetchdf()
    con.close()

    logger.info(f"Loaded {len(df):,} windows")
    logger.info(f"Overall UP rate: {df['outcome'].mean():.4f}")
    logger.info(f"Vol regime distribution: {df['vol_regime'].value_counts().to_dict()}")

    all_results = []

    for elapsed_s in SNAPSHOT_INTERVALS:
        logger.info(f"Analyzing T+{elapsed_s}s...")
        results = analyze_single_snapshot(
            df, elapsed_s,
            n_change_buckets=n_change_buckets,
            by_vol_regime=by_vol_regime,
        )
        all_results.extend(results)

        # Log summary for this interval
        significant = [r for r in results if r.is_significant and r.vol_regime == -1]
        if significant:
            max_edge = max(significant, key=lambda r: abs(r.edge_vs_50))
            logger.info(
                f"  {len(significant)} significant buckets (all regimes). "
                f"Max edge: {max_edge.edge_vs_50:+.2f}pp at "
                f"[{max_edge.change_bucket_lower:.6f}, {max_edge.change_bucket_upper:.6f}]"
            )
        else:
            logger.info(f"  No significant buckets at this interval (all regimes)")

    logger.info(f"\nTotal bucket entries: {len(all_results)}")
    significant_total = sum(1 for r in all_results if r.is_significant)
    logger.info(f"Significant entries: {significant_total}")

    return all_results


def save_to_sqlite(results: list[BucketStats], db_path: Path) -> None:
    """Save analysis results to SQLite for fast lookups."""
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS lookup")
    cursor.execute("""
        CREATE TABLE lookup (
            time_elapsed_s      INTEGER NOT NULL,
            change_bucket_lower REAL NOT NULL,
            change_bucket_upper REAL NOT NULL,
            vol_regime          INTEGER NOT NULL,
            total_count         INTEGER NOT NULL,
            up_count            INTEGER NOT NULL,
            down_count          INTEGER NOT NULL,
            empirical_prob_up   REAL NOT NULL,
            bayesian_prob_up    REAL NOT NULL,
            credible_lower      REAL NOT NULL,
            credible_upper      REAL NOT NULL,
            edge_vs_50          REAL NOT NULL,
            is_significant      INTEGER NOT NULL
        )
    """)

    cursor.execute("""
        CREATE INDEX idx_lookup_query ON lookup (
            time_elapsed_s, vol_regime, change_bucket_lower, change_bucket_upper
        )
    """)

    for r in results:
        cursor.execute("""
            INSERT INTO lookup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r.time_elapsed_s,
            r.change_bucket_lower,
            r.change_bucket_upper,
            r.vol_regime,
            r.total_count,
            r.up_count,
            r.down_count,
            r.empirical_prob_up,
            r.bayesian_prob_up,
            r.credible_lower,
            r.credible_upper,
            r.edge_vs_50,
            int(r.is_significant),
        ))

    conn.commit()

    # Also save metadata
    cursor.execute("DROP TABLE IF EXISTS metadata")
    cursor.execute("""
        CREATE TABLE metadata (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    cursor.execute("INSERT INTO metadata VALUES ('prior_alpha', ?)", (str(PRIOR_ALPHA),))
    cursor.execute("INSERT INTO metadata VALUES ('prior_beta', ?)", (str(PRIOR_BETA),))
    cursor.execute("INSERT INTO metadata VALUES ('min_samples', ?)", (str(MIN_SAMPLES),))
    cursor.execute("INSERT INTO metadata VALUES ('snapshot_intervals', ?)",
                   (json.dumps(SNAPSHOT_INTERVALS),))
    cursor.execute("INSERT INTO metadata VALUES ('total_entries', ?)", (str(len(results)),))
    cursor.execute("INSERT INTO metadata VALUES ('significant_entries', ?)",
                   (str(sum(1 for r in results if r.is_significant)),))

    conn.commit()
    conn.close()

    logger.info(f"Saved {len(results)} entries to {db_path}")


def print_summary_table(results: list[BucketStats]) -> None:
    """Print a formatted summary of the most significant findings."""
    significant = [r for r in results if r.is_significant and r.vol_regime == -1]
    significant.sort(key=lambda r: abs(r.edge_vs_50), reverse=True)

    if not significant:
        logger.info("\nNo statistically significant edges found (all regimes combined).")
        return

    print("\n" + "=" * 100)
    print("TOP SIGNIFICANT EDGES (All Vol Regimes Combined)")
    print("=" * 100)
    print(f"{'T+sec':>6} {'Change Range':>24} {'Samples':>8} {'P(UP)':>8} "
          f"{'Bayes':>8} {'CI 95%':>16} {'Edge':>8}")
    print("-" * 100)

    for r in significant[:30]:
        lower = f"{r.change_bucket_lower*100:+.4f}%" if r.change_bucket_lower != float('-inf') else "  -inf"
        upper = f"{r.change_bucket_upper*100:+.4f}%" if r.change_bucket_upper != float('inf') else "  +inf"
        ci = f"[{r.credible_lower:.3f}, {r.credible_upper:.3f}]"
        print(f"{r.time_elapsed_s:>5}s [{lower}, {upper}] {r.total_count:>7} "
              f"{r.empirical_prob_up:>7.3f} {r.bayesian_prob_up:>7.3f} {ci:>16} "
              f"{r.edge_vs_50:>+7.2f}pp")

    print()

    # Also show vol-regime-specific edges
    for vr, vr_name in [(0, "LOW VOL"), (1, "MED VOL"), (2, "HIGH VOL")]:
        vr_significant = [r for r in results if r.is_significant and r.vol_regime == vr]
        vr_significant.sort(key=lambda r: abs(r.edge_vs_50), reverse=True)

        if not vr_significant:
            continue

        print(f"\nTOP EDGES - {vr_name} REGIME")
        print("-" * 100)
        for r in vr_significant[:10]:
            lower = f"{r.change_bucket_lower*100:+.4f}%" if r.change_bucket_lower != float('-inf') else "  -inf"
            upper = f"{r.change_bucket_upper*100:+.4f}%" if r.change_bucket_upper != float('inf') else "  +inf"
            ci = f"[{r.credible_lower:.3f}, {r.credible_upper:.3f}]"
            print(f"{r.time_elapsed_s:>5}s [{lower}, {upper}] {r.total_count:>7} "
                  f"{r.empirical_prob_up:>7.3f} {r.bayesian_prob_up:>7.3f} {ci:>16} "
                  f"{r.edge_vs_50:>+7.2f}pp")


def query_lookup(
    db_path: Path,
    time_elapsed_s: int,
    price_change: float,
    vol_regime: int = -1,
) -> dict | None:
    """
    Query the lookup table for a specific scenario.

    Args:
        db_path: path to SQLite database
        time_elapsed_s: seconds elapsed in current window
        price_change: relative price change from open (e.g., 0.0005 = 0.05%)
        vol_regime: 0=low, 1=med, 2=high, -1=all

    Returns: dict with probability and stats, or None if no matching bucket
    """
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Find the nearest snapshot interval
    intervals = json.loads(
        cursor.execute("SELECT value FROM metadata WHERE key='snapshot_intervals'").fetchone()[0]
    )
    nearest = min(intervals, key=lambda x: abs(x - time_elapsed_s))

    cursor.execute("""
        SELECT * FROM lookup
        WHERE time_elapsed_s = ?
          AND vol_regime = ?
          AND change_bucket_lower <= ?
          AND change_bucket_upper > ?
        LIMIT 1
    """, (nearest, vol_regime, price_change, price_change))

    row = cursor.fetchone()
    conn.close()

    if row is None:
        return None

    return {
        "time_elapsed_s": row[0],
        "change_range": (row[1], row[2]),
        "vol_regime": row[3],
        "sample_count": row[4],
        "prob_up": row[8],  # bayesian
        "credible_interval": (row[9], row[10]),
        "edge_pp": row[11],
        "significant": bool(row[12]),
    }


def main():
    parser = argparse.ArgumentParser(description="Run statistical analysis on 5-minute windows")
    parser.add_argument("--input", type=str, default="../data/parquet/windows_5m.parquet")
    parser.add_argument("--output", type=str, default="../data/lookup.db")
    parser.add_argument("--buckets", type=int, default=15, help="Number of price change buckets")
    parser.add_argument("--no-vol-regime", action="store_true", help="Skip volatility regime analysis")
    args = parser.parse_args()

    results = run_full_analysis(
        Path(args.input),
        n_change_buckets=args.buckets,
        by_vol_regime=not args.no_vol_regime,
    )

    print_summary_table(results)
    save_to_sqlite(results, Path(args.output))


if __name__ == "__main__":
    main()
