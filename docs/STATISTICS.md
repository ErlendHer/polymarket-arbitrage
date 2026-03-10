# Statistical Methodology

## Problem Statement

Given intermediate BTC price movements within a 5-minute window, estimate the probability
that BTC closes UP (close >= open) at the end of the window.

## Data

- ~210,000 five-minute windows from 2 years of 1-second BTC/USDT data (Binance)
- 17 snapshot intervals per window: 5, 10, 15, 20, 25, 30, 45, 60, 90, 120, 150, 180, 210, 240, 270, 285, 295 seconds
- Each snapshot records relative price change from window open: `(price_t - open) / open`

## Bayesian Framework

### Model
Each bucket's UP probability is modeled as a Beta-Binomial:

- **Prior:** Beta(2, 2) — weakly informative, centered at 0.5
- **Likelihood:** Binomial(n, p) where p = true probability of UP
- **Posterior:** Beta(2 + ups, 2 + downs)
- **Point estimate:** Posterior mean = (2 + ups) / (4 + n)
- **Uncertainty:** 95% credible interval from Beta quantiles

### Why Bayesian?
1. **Natural shrinkage:** Sparse buckets are pulled toward 50%, preventing overfitting
2. **Uncertainty quantification:** Credible intervals tell us which edges are reliable
3. **Composable:** Can incorporate additional priors from adjacent buckets (hierarchical model)

## Bucketing Strategy

### Dimensions
1. **Time elapsed** (seconds from window open): 17 discrete intervals
2. **Relative price change** (from window open): 15 quantile-based buckets per interval
3. **Volatility regime** (prior 1-hour realized vol): 3 tercile-based regimes (low/med/high)

### Quantile-Based Boundaries
Buckets are defined by quantiles of the observed price change distribution,
not fixed intervals. This ensures roughly equal sample sizes per bucket.

### Sample Size Budget
- 210,000 windows total
- 17 time intervals x 15 change buckets x 4 vol groups (3 regimes + "all") = 1,020 cells
- Average: ~206 samples per cell (all regimes), ~69 per regime-specific cell
- With Bayesian smoothing, this is workable down to ~30 samples

## Statistical Significance

A bucket is "significant" when its 95% credible interval excludes 0.5.

### Detectable Effect Sizes (approximate)
| Samples per bucket | Min detectable edge (pp) |
|---|---|
| 100 | ~10 |
| 500 | ~4.5 |
| 1,000 | ~3.2 |
| 5,000 | ~1.4 |

### For Arbitrage
- Minimum edge to overcome maker fees: 0% (any edge works)
- Minimum edge to overcome taker fees: ~1.56% at midprice
- Practical minimum: ~2pp to justify operational risk
- **Target:** Identify buckets with 3-5pp edges at >95% confidence

## Volatility Conditioning

**Realized volatility** is computed as:
1. Take 1-minute log returns over the prior 60 minutes
2. Compute standard deviation
3. Annualize: `vol = std(returns_1m) * sqrt(525600)`

**Tercile split:**
- Low vol: bottom third of all windows
- Medium vol: middle third
- High vol: top third

**Hypothesis:** Same price movement has different predictive power depending on volatility.
A 0.02% move in low-vol is more informative than in high-vol.

## Validation

### Out-of-Sample Testing
- Training: first 75% of data chronologically
- Validation: last 25%
- Check for: edge persistence, calibration stability, regime shifts

### Calibration
Plot binned predicted probability vs observed frequency.
A well-calibrated model has points near the diagonal.

### Multiple Testing Correction
With 1,020 cells, we expect ~51 false positives at p=0.05.
Focus on: large edges (>3pp), consistent across adjacent buckets, stable across time.

## Known Limitations

1. **Non-stationarity:** BTC market behavior changes over time. Patterns from 2024 may not hold in 2026.
2. **Oracle mismatch:** We use Binance data but Polymarket resolves on Chainlink. Small discrepancies possible.
3. **Adverse selection:** As a maker, informed counterparties will selectively trade against us when our quotes are stale.
4. **Market impact:** Our own trading will move the market, especially in thin books.
5. **Survivorship bias:** We analyze data from a period where Polymarket's 5-min markets existed. The market structure may have already adapted.
