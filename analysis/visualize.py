"""
Visualization suite for the 5-minute window knowledge base.

Generates:
1. Heatmap: P(UP) by time_elapsed vs price_change
2. Heatmap: Edge (vs 50%) by time_elapsed vs price_change
3. Per-volatility-regime heatmaps
4. Calibration plot: predicted vs actual
5. Statistical significance map
6. Edge distribution histogram
7. Sample count distribution
8. Volatility regime comparison

Usage:
    python visualize.py --db ../data/lookup.db --output ../data/charts
"""

import argparse
import logging
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Consistent styling
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "font.size": 10,
    "figure.dpi": 150,
})


def load_lookup_data(db_path: Path) -> pd.DataFrame:
    """Load lookup table from SQLite into DataFrame."""
    conn = sqlite3.connect(str(db_path))
    df = pd.read_sql_query("SELECT * FROM lookup", conn)
    conn.close()

    # Create a readable label for change buckets
    df["change_mid"] = (df["change_bucket_lower"] + df["change_bucket_upper"]) / 2
    # Handle inf values
    df.loc[df["change_bucket_lower"] == float("-inf"), "change_mid"] = df.loc[
        df["change_bucket_lower"] == float("-inf"), "change_bucket_upper"
    ]
    df.loc[df["change_bucket_upper"] == float("inf"), "change_mid"] = df.loc[
        df["change_bucket_upper"] == float("inf"), "change_bucket_lower"
    ]

    # Format labels
    df["change_label"] = df.apply(
        lambda r: f"{r['change_mid']*100:.3f}%", axis=1
    )

    return df


def plot_probability_heatmap(df: pd.DataFrame, output_dir: Path, vol_regime: int = -1):
    """
    Heatmap of P(UP) across time elapsed and price change buckets.
    """
    subset = df[df["vol_regime"] == vol_regime].copy()
    if len(subset) == 0:
        return

    regime_name = {-1: "All Regimes", 0: "Low Vol", 1: "Med Vol", 2: "High Vol"}[vol_regime]

    # Pivot for heatmap
    pivot = subset.pivot_table(
        values="bayesian_prob_up",
        index="change_mid",
        columns="time_elapsed_s",
        aggfunc="first",
    )
    pivot = pivot.sort_index(ascending=False)

    fig, ax = plt.subplots(figsize=(16, 10))
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".3f",
        cmap="RdYlGn",
        center=0.5,
        vmin=0.35,
        vmax=0.65,
        ax=ax,
        cbar_kws={"label": "P(UP)"},
        annot_kws={"size": 7},
    )

    # Format y-axis labels as percentages
    yticklabels = [f"{float(t.get_text())*100:.3f}%" for t in ax.get_yticklabels()]
    ax.set_yticklabels(yticklabels, rotation=0)

    ax.set_xlabel("Seconds Elapsed in Window")
    ax.set_ylabel("Price Change from Open")
    ax.set_title(f"P(UP) by Time Elapsed and Price Change — {regime_name}\n"
                 f"Green = UP bias, Red = DOWN bias, Yellow = ~50/50")

    plt.tight_layout()
    suffix = {-1: "all", 0: "lowvol", 1: "medvol", 2: "highvol"}[vol_regime]
    fig.savefig(output_dir / f"heatmap_prob_up_{suffix}.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved probability heatmap ({regime_name})")


def plot_edge_heatmap(df: pd.DataFrame, output_dir: Path, vol_regime: int = -1):
    """
    Heatmap of edge (pp above/below 50%) with significance markers.
    """
    subset = df[df["vol_regime"] == vol_regime].copy()
    if len(subset) == 0:
        return

    regime_name = {-1: "All Regimes", 0: "Low Vol", 1: "Med Vol", 2: "High Vol"}[vol_regime]

    pivot_edge = subset.pivot_table(
        values="edge_vs_50",
        index="change_mid",
        columns="time_elapsed_s",
        aggfunc="first",
    )
    pivot_edge = pivot_edge.sort_index(ascending=False)

    pivot_sig = subset.pivot_table(
        values="is_significant",
        index="change_mid",
        columns="time_elapsed_s",
        aggfunc="first",
    )
    pivot_sig = pivot_sig.sort_index(ascending=False)

    fig, ax = plt.subplots(figsize=(16, 10))

    sns.heatmap(
        pivot_edge,
        annot=True,
        fmt="+.1f",
        cmap="RdBu",
        center=0,
        vmin=-8,
        vmax=8,
        ax=ax,
        cbar_kws={"label": "Edge (pp)"},
        annot_kws={"size": 7},
    )

    # Mark significant cells with a bold border
    for i in range(pivot_sig.shape[0]):
        for j in range(pivot_sig.shape[1]):
            if pivot_sig.iloc[i, j] == 1:
                ax.add_patch(plt.Rectangle(
                    (j, i), 1, 1, fill=False, edgecolor="black", linewidth=2
                ))

    yticklabels = [f"{float(t.get_text())*100:.3f}%" for t in ax.get_yticklabels()]
    ax.set_yticklabels(yticklabels, rotation=0)

    ax.set_xlabel("Seconds Elapsed in Window")
    ax.set_ylabel("Price Change from Open")
    ax.set_title(f"Edge vs 50% (pp) — {regime_name}\n"
                 f"Blue = UP edge, Red = DOWN edge. Black border = statistically significant (p<0.05)")

    plt.tight_layout()
    suffix = {-1: "all", 0: "lowvol", 1: "medvol", 2: "highvol"}[vol_regime]
    fig.savefig(output_dir / f"heatmap_edge_{suffix}.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved edge heatmap ({regime_name})")


def plot_significance_map(df: pd.DataFrame, output_dir: Path):
    """
    Binary map showing which buckets have statistically significant edges.
    """
    subset = df[df["vol_regime"] == -1].copy()
    if len(subset) == 0:
        return

    pivot = subset.pivot_table(
        values="is_significant",
        index="change_mid",
        columns="time_elapsed_s",
        aggfunc="first",
    )
    pivot = pivot.sort_index(ascending=False)

    fig, ax = plt.subplots(figsize=(16, 10))
    sns.heatmap(
        pivot,
        annot=False,
        cmap=["#f0f0f0", "#2ecc71"],
        ax=ax,
        cbar_kws={"label": "Significant (1=Yes)"},
    )

    yticklabels = [f"{float(t.get_text())*100:.3f}%" for t in ax.get_yticklabels()]
    ax.set_yticklabels(yticklabels, rotation=0)

    ax.set_xlabel("Seconds Elapsed in Window")
    ax.set_ylabel("Price Change from Open")
    ax.set_title("Statistical Significance Map\n"
                 "Green = 95% credible interval excludes 50%")

    plt.tight_layout()
    fig.savefig(output_dir / "significance_map.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved significance map")


def plot_edge_distribution(df: pd.DataFrame, output_dir: Path):
    """
    Histogram of edge sizes across all buckets.
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # All buckets
    all_edges = df[df["vol_regime"] == -1]["edge_vs_50"]
    axes[0].hist(all_edges, bins=50, color="#3498db", alpha=0.7, edgecolor="white")
    axes[0].axvline(0, color="red", linestyle="--", linewidth=1)
    axes[0].set_xlabel("Edge (percentage points)")
    axes[0].set_ylabel("Count")
    axes[0].set_title("Distribution of All Bucket Edges")

    # Significant only
    sig_edges = df[(df["vol_regime"] == -1) & (df["is_significant"] == 1)]["edge_vs_50"]
    if len(sig_edges) > 0:
        axes[1].hist(sig_edges, bins=30, color="#e74c3c", alpha=0.7, edgecolor="white")
        axes[1].axvline(0, color="black", linestyle="--", linewidth=1)
        axes[1].set_title(f"Significant Edges Only (n={len(sig_edges)})")
    else:
        axes[1].text(0.5, 0.5, "No significant edges found",
                     transform=axes[1].transAxes, ha="center", va="center")
        axes[1].set_title("Significant Edges Only")

    axes[1].set_xlabel("Edge (percentage points)")
    axes[1].set_ylabel("Count")

    plt.tight_layout()
    fig.savefig(output_dir / "edge_distribution.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved edge distribution")


def plot_sample_counts(df: pd.DataFrame, output_dir: Path):
    """
    Heatmap of sample counts to assess data density.
    """
    subset = df[df["vol_regime"] == -1].copy()
    if len(subset) == 0:
        return

    pivot = subset.pivot_table(
        values="total_count",
        index="change_mid",
        columns="time_elapsed_s",
        aggfunc="first",
    )
    pivot = pivot.sort_index(ascending=False)

    fig, ax = plt.subplots(figsize=(16, 10))
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".0f",
        cmap="YlOrRd",
        ax=ax,
        cbar_kws={"label": "Sample Count"},
        annot_kws={"size": 7},
    )

    yticklabels = [f"{float(t.get_text())*100:.3f}%" for t in ax.get_yticklabels()]
    ax.set_yticklabels(yticklabels, rotation=0)

    ax.set_xlabel("Seconds Elapsed in Window")
    ax.set_ylabel("Price Change from Open")
    ax.set_title("Sample Count per Bucket\n"
                 "Higher = more reliable estimates")

    plt.tight_layout()
    fig.savefig(output_dir / "sample_counts.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved sample count heatmap")


def plot_volatility_comparison(df: pd.DataFrame, output_dir: Path):
    """
    Compare edge distributions across volatility regimes.
    """
    fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharey=True)

    colors = {0: "#2ecc71", 1: "#f39c12", 2: "#e74c3c"}
    names = {0: "Low Volatility", 1: "Medium Volatility", 2: "High Volatility"}

    for idx, vr in enumerate([0, 1, 2]):
        vr_data = df[df["vol_regime"] == vr]
        if len(vr_data) == 0:
            continue

        edges = vr_data["edge_vs_50"]
        sig_count = (vr_data["is_significant"] == 1).sum()

        axes[idx].hist(edges, bins=40, color=colors[vr], alpha=0.7, edgecolor="white")
        axes[idx].axvline(0, color="black", linestyle="--", linewidth=1)
        axes[idx].set_xlabel("Edge (pp)")
        axes[idx].set_title(f"{names[vr]}\n"
                           f"n={len(vr_data)}, significant={sig_count}\n"
                           f"mean={edges.mean():.2f}pp, std={edges.std():.2f}pp")

    axes[0].set_ylabel("Count")
    plt.tight_layout()
    fig.savefig(output_dir / "volatility_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved volatility comparison")


def plot_edge_by_time(df: pd.DataFrame, output_dir: Path):
    """
    Line plot showing average absolute edge by time elapsed, split by vol regime.
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    for vr, name, color in [(-1, "All", "#333"), (0, "Low Vol", "#2ecc71"),
                             (1, "Med Vol", "#f39c12"), (2, "High Vol", "#e74c3c")]:
        subset = df[df["vol_regime"] == vr]
        if len(subset) == 0:
            continue

        grouped = subset.groupby("time_elapsed_s").agg(
            mean_abs_edge=("edge_vs_50", lambda x: np.mean(np.abs(x))),
            max_edge=("edge_vs_50", lambda x: np.max(np.abs(x))),
            sig_count=("is_significant", "sum"),
        ).reset_index()

        style = "-" if vr == -1 else "--"
        width = 2.5 if vr == -1 else 1.5
        ax.plot(grouped["time_elapsed_s"], grouped["mean_abs_edge"],
                style, color=color, label=f"{name} (mean |edge|)", linewidth=width)

    ax.set_xlabel("Seconds Elapsed in Window")
    ax.set_ylabel("Mean Absolute Edge (pp)")
    ax.set_title("Predictive Power Over Time\n"
                 "How much edge exists at each point in the 5-minute window?")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "edge_by_time.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved edge by time plot")


def plot_calibration(df: pd.DataFrame, output_dir: Path):
    """
    Calibration plot: binned Bayesian probability vs actual observed frequency.
    Tests if our probability estimates are well-calibrated.
    """
    subset = df[df["vol_regime"] == -1].copy()
    if len(subset) == 0:
        return

    # Bin the Bayesian probabilities
    bins = np.arange(0.35, 0.66, 0.02)
    subset["prob_bin"] = pd.cut(subset["bayesian_prob_up"], bins=bins)

    cal = subset.groupby("prob_bin", observed=True).agg(
        predicted=("bayesian_prob_up", "mean"),
        observed=("empirical_prob_up", "mean"),
        count=("total_count", "sum"),
    ).dropna()

    fig, ax = plt.subplots(figsize=(8, 8))

    # Perfect calibration line
    ax.plot([0.35, 0.65], [0.35, 0.65], "k--", alpha=0.5, label="Perfect calibration")

    # Actual calibration
    sizes = cal["count"] / cal["count"].max() * 200
    ax.scatter(cal["predicted"], cal["observed"], s=sizes, c="#3498db", alpha=0.7, edgecolors="white")

    ax.set_xlabel("Bayesian Predicted P(UP)")
    ax.set_ylabel("Observed P(UP)")
    ax.set_title("Calibration Plot\nAre our probability estimates accurate?")
    ax.set_xlim(0.35, 0.65)
    ax.set_ylim(0.35, 0.65)
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "calibration.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved calibration plot")


def generate_all_charts(db_path: Path, output_dir: Path):
    """Generate all visualization charts."""
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_lookup_data(db_path)
    logger.info(f"Loaded {len(df)} entries from {db_path}")

    # Generate all charts
    plot_probability_heatmap(df, output_dir, vol_regime=-1)
    plot_probability_heatmap(df, output_dir, vol_regime=0)
    plot_probability_heatmap(df, output_dir, vol_regime=1)
    plot_probability_heatmap(df, output_dir, vol_regime=2)

    plot_edge_heatmap(df, output_dir, vol_regime=-1)
    plot_edge_heatmap(df, output_dir, vol_regime=0)
    plot_edge_heatmap(df, output_dir, vol_regime=1)
    plot_edge_heatmap(df, output_dir, vol_regime=2)

    plot_significance_map(df, output_dir)
    plot_edge_distribution(df, output_dir)
    plot_sample_counts(df, output_dir)
    plot_volatility_comparison(df, output_dir)
    plot_edge_by_time(df, output_dir)
    plot_calibration(df, output_dir)

    logger.info(f"\nAll charts saved to {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Generate visualization charts")
    parser.add_argument("--db", type=str, default="../data/lookup.db", help="SQLite lookup database")
    parser.add_argument("--output", type=str, default="../data/charts", help="Chart output directory")
    args = parser.parse_args()

    generate_all_charts(Path(args.db), Path(args.output))


if __name__ == "__main__":
    main()
