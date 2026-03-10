/**
 * Statistical lookup table reader.
 *
 * Reads the SQLite knowledge base built by the Python analysis pipeline
 * and provides O(1) lookups for conditional probabilities.
 *
 * Usage:
 *   const lookup = new LookupTable("../data/lookup.db");
 *   const result = lookup.query(30, 0.0005, -1); // T+30s, +0.05%, all vol regimes
 *   // result.probUp = 0.524, result.edge = +2.4pp
 */

import Database from "better-sqlite3";

export interface LookupResult {
  timeElapsedS: number;
  changeRange: [number, number];
  volRegime: number;
  sampleCount: number;
  probUp: number;
  credibleInterval: [number, number];
  edgePp: number;
  significant: boolean;
}

export interface LookupMetadata {
  snapshotIntervals: number[];
  totalEntries: number;
  significantEntries: number;
  priorAlpha: number;
  priorBeta: number;
}

export class LookupTable {
  private db: Database.Database;
  private queryStmt: Database.Statement;
  private snapshotIntervals: number[];

  constructor(dbPath: string) {
    this.db = new Database(dbPath, { readonly: true });

    // Prepare the query statement for reuse
    this.queryStmt = this.db.prepare(`
      SELECT * FROM lookup
      WHERE time_elapsed_s = ?
        AND vol_regime = ?
        AND change_bucket_lower <= ?
        AND change_bucket_upper > ?
      LIMIT 1
    `);

    // Load snapshot intervals
    const row = this.db
      .prepare("SELECT value FROM metadata WHERE key = 'snapshot_intervals'")
      .get() as { value: string } | undefined;

    this.snapshotIntervals = row ? JSON.parse(row.value) : [];
  }

  /**
   * Find the nearest available snapshot interval to the given time.
   */
  nearestInterval(timeElapsedS: number): number {
    if (this.snapshotIntervals.length === 0) return timeElapsedS;

    let nearest = this.snapshotIntervals[0];
    let minDiff = Math.abs(timeElapsedS - nearest);

    for (const interval of this.snapshotIntervals) {
      const diff = Math.abs(timeElapsedS - interval);
      if (diff < minDiff) {
        minDiff = diff;
        nearest = interval;
      }
    }
    return nearest;
  }

  /**
   * Query the knowledge base for a specific scenario.
   *
   * @param timeElapsedS - seconds elapsed in current 5-min window
   * @param priceChange - relative price change from window open (e.g., 0.0005 = +0.05%)
   * @param volRegime - volatility regime: 0=low, 1=med, 2=high, -1=all
   * @returns LookupResult or null if no matching bucket
   */
  query(
    timeElapsedS: number,
    priceChange: number,
    volRegime: number = -1
  ): LookupResult | null {
    const nearest = this.nearestInterval(timeElapsedS);

    const row = this.queryStmt.get(
      nearest,
      volRegime,
      priceChange,
      priceChange
    ) as Record<string, number> | undefined;

    if (!row) return null;

    return {
      timeElapsedS: row.time_elapsed_s,
      changeRange: [row.change_bucket_lower, row.change_bucket_upper],
      volRegime: row.vol_regime,
      sampleCount: row.total_count,
      probUp: row.bayesian_prob_up,
      credibleInterval: [row.credible_lower, row.credible_upper],
      edgePp: row.edge_vs_50,
      significant: row.is_significant === 1,
    };
  }

  /**
   * Compute the edge between our statistical probability and the market price.
   *
   * @param timeElapsedS - seconds into the window
   * @param priceChange - relative BTC price change from open
   * @param marketProbUp - market-implied probability (e.g., UP token price in 0-1)
   * @param volRegime - volatility regime
   * @returns edge in percentage points (positive = market overprices UP)
   */
  computeEdge(
    timeElapsedS: number,
    priceChange: number,
    marketProbUp: number,
    volRegime: number = -1
  ): { edge: number; statProb: number; significant: boolean } | null {
    const result = this.query(timeElapsedS, priceChange, volRegime);
    if (!result) return null;

    const edge = (marketProbUp - result.probUp) * 100; // in pp
    return {
      edge,
      statProb: result.probUp,
      significant: result.significant,
    };
  }

  getMetadata(): LookupMetadata {
    const getMeta = (key: string): string => {
      const row = this.db
        .prepare("SELECT value FROM metadata WHERE key = ?")
        .get(key) as { value: string } | undefined;
      return row?.value ?? "";
    };

    return {
      snapshotIntervals: JSON.parse(getMeta("snapshot_intervals") || "[]"),
      totalEntries: parseInt(getMeta("total_entries") || "0"),
      significantEntries: parseInt(getMeta("significant_entries") || "0"),
      priorAlpha: parseFloat(getMeta("prior_alpha") || "2"),
      priorBeta: parseFloat(getMeta("prior_beta") || "2"),
    };
  }

  getSnapshotIntervals(): number[] {
    return [...this.snapshotIntervals];
  }

  close(): void {
    this.db.close();
  }
}
