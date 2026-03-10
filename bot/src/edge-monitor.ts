/**
 * Edge Monitor - Phase 2 Core
 *
 * Streams BTC price + Polymarket orderbook simultaneously.
 * Compares market-implied probability against our statistical model.
 * Logs EVERY tick to JSONL for offline backtesting.
 * Console output is filtered to show only actionable edges.
 *
 * This is a READ-ONLY monitor. No orders are placed.
 *
 * Usage:
 *   LOOKUP_DB=../data/lookup.db npx tsx src/edge-monitor.ts
 */

import { BtcPriceStream } from "./streams/btc-price.js";
import {
  PolymarketOrderbook,
  OrderbookSnapshot,
} from "./streams/polymarket-orderbook.js";
import {
  findCurrentWindow,
  getTokenIds,
  computeWindowBounds,
  PolymarketMarket,
} from "./streams/polymarket-discovery.js";
import { LookupTable } from "./signals/lookup.js";
import path from "path";
import fs from "fs";

// Configuration
const LOOKUP_DB_PATH =
  process.env.LOOKUP_DB || path.join(__dirname, "../../data/lookup.db");
const LOG_DIR = process.env.LOG_DIR || path.join(__dirname, "../../data/logs");
const SCAN_INTERVAL_MS = 1000;
const MARKET_REFRESH_INTERVAL_MS = 30_000;

// Console display filters (file logging captures everything regardless)
const MIN_EDGE_PP = 3.0; // Minimum edge to show in console (pp)
const MAX_SPREAD = 0.06; // Max spread to consider actionable ($0.06 = 6 cents)
const MAX_ELAPSED_S = 240; // Stop showing console edges after this (liquidity dries up)

/**
 * Every-tick log record. Logged to JSONL for backtesting.
 */
interface TickRecord {
  // Identifiers
  windowSlug: string;
  conditionId: string;
  upTokenId: string;

  // Timing
  timestamp: number;
  elapsedS: number;

  // BTC price data
  btcPrice: number;
  openPrice: number;
  priceChange: number; // relative

  // Market data (from orderbook)
  marketProbUp: number | null; // midpoint of UP token
  bestBidUp: number;
  bestAskUp: number;
  spreadUp: number;
  bestBidDown: number;
  bestAskDown: number;
  spreadDown: number;

  // Model data
  statProbUp: number | null;
  edgePp: number | null; // market - stat
  significant: boolean | null;

  // Quality flags
  bookAvailable: boolean;
  actionable: boolean; // passes spread + time filters
}

/**
 * Window summary logged at end of each window.
 */
interface WindowSummary {
  type: "window_summary";
  windowSlug: string;
  conditionId: string;
  question: string;
  windowStartMs: number;
  windowEndMs: number;
  openPrice: number;
  finalPrice: number;
  finalPriceChange: number;
  totalTicks: number;
  actionableTicks: number;
  actionableEdges: number; // actionable ticks with edge >= MIN_EDGE_PP
  avgAbsEdge: number; // across actionable edges
  maxEdgePp: number;
  maxEdgeTime: number;
  avgSpread: number; // across actionable ticks
}

interface WindowState {
  market: PolymarketMarket;
  upTokenId: string;
  downTokenId: string;
  openPrice: number | null;
  windowStartMs: number;
  windowEndMs: number;
  ticks: TickRecord[];
}

class EdgeMonitor {
  private btcStream: BtcPriceStream;
  private polyOrderbook: PolymarketOrderbook;
  private lookup: LookupTable;
  private currentWindow: WindowState | null = null;
  private scanTimer: ReturnType<typeof setInterval> | null = null;
  private marketRefreshTimer: ReturnType<typeof setInterval> | null = null;
  private logStream: fs.WriteStream | null = null;
  private totalEdgeCount = 0;
  private totalWindowCount = 0;
  private scanCount = 0;

  constructor() {
    this.btcStream = new BtcPriceStream();
    this.polyOrderbook = new PolymarketOrderbook();

    if (!fs.existsSync(LOOKUP_DB_PATH)) {
      console.error(`Lookup DB not found at ${LOOKUP_DB_PATH}`);
      console.error(
        "Run the analysis pipeline first: cd analysis && python run_pipeline.py"
      );
      process.exit(1);
    }

    this.lookup = new LookupTable(LOOKUP_DB_PATH);
    const meta = this.lookup.getMetadata();
    console.log(
      `Loaded knowledge base: ${meta.totalEntries} entries, ${meta.significantEntries} significant`
    );
    console.log(`Snapshot intervals: ${meta.snapshotIntervals.join(", ")}s`);
    console.log(
      `Console filters: edge >= ${MIN_EDGE_PP}pp, spread <= $${MAX_SPREAD}, T <= ${MAX_ELAPSED_S}s`
    );
  }

  async start(): Promise<void> {
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const logFile = path.join(
      LOG_DIR,
      `edge-monitor-${new Date().toISOString().slice(0, 10)}.jsonl`
    );
    this.logStream = fs.createWriteStream(logFile, { flags: "a" });
    console.log(`Logging ALL ticks to: ${logFile}`);

    // Connect BTC price stream
    console.log("Connecting to Binance BTC stream...");
    this.btcStream.on("connected", () => console.log("Binance WS connected"));
    this.btcStream.on("disconnected", (code, reason) =>
      console.log(`Binance WS disconnected: ${code} ${reason}`)
    );
    this.btcStream.on("error", (err) =>
      console.error("Binance WS error:", err.message)
    );
    this.btcStream.connect();

    // Set up orderbook stream
    this.polyOrderbook.on("connected", () =>
      console.log("Polymarket WS connected")
    );
    this.polyOrderbook.on("disconnected", (code, reason) =>
      console.log(`Polymarket WS disconnected: ${code} ${reason}`)
    );
    this.polyOrderbook.on("error", (err) =>
      console.error("Polymarket WS error:", err.message)
    );

    await this.refreshMarkets();

    this.marketRefreshTimer = setInterval(
      () => this.refreshMarkets(),
      MARKET_REFRESH_INTERVAL_MS
    );

    this.scanTimer = setInterval(() => this.scan(), SCAN_INTERVAL_MS);
    console.log("\nEdge monitor running. Press Ctrl+C to stop.\n");

    process.on("SIGINT", () => this.stop());
    process.on("SIGTERM", () => this.stop());
  }

  private async refreshMarkets(): Promise<void> {
    try {
      const active = await findCurrentWindow();
      const now = Date.now();

      if (!active) {
        const bounds = computeWindowBounds(now);
        const waitS = Math.round((bounds.endMs - now) / 1000);
        console.log(`No active market found. Next window in ~${waitS}s`);
        this.currentWindow = null;
        return;
      }

      const { upTokenId, downTokenId } = getTokenIds(active);
      if (!upTokenId || !downTokenId) {
        console.log("Could not find UP/DOWN token IDs for active market");
        return;
      }

      if (this.currentWindow?.market.conditionId === active.conditionId) {
        return;
      }

      // Finalize previous window if still active
      if (this.currentWindow) {
        this.finalizeWindow();
      }

      console.log(`\n--- NEW WINDOW ---`);
      console.log(`Market: ${active.question}`);
      console.log(`Slug: ${active.slug}`);
      console.log(`UP token: ${upTokenId}`);
      console.log(`DOWN token: ${downTokenId}`);
      console.log(
        `Window: ${new Date(active.startTime).toISOString()} to ${new Date(active.endTime).toISOString()}`
      );

      this.currentWindow = {
        market: active,
        upTokenId,
        downTokenId,
        openPrice: null,
        windowStartMs: active.startTime,
        windowEndMs: active.endTime,
        ticks: [],
      };

      this.polyOrderbook.disconnect();
      this.polyOrderbook.connect([upTokenId, downTokenId]);

      if (this.btcStream.currentPrice > 0) {
        this.currentWindow.openPrice = this.btcStream.currentPrice;
        console.log(
          `Open price (from current): $${this.currentWindow.openPrice}`
        );
      }
    } catch (err) {
      console.error("Market refresh error:", (err as Error).message);
    }
  }

  private scan(): void {
    if (!this.currentWindow) return;
    this.scanCount++;

    const now = Date.now();
    const window = this.currentWindow;

    if (now >= window.windowEndMs) {
      this.finalizeWindow();
      return;
    }

    // Set open price if not yet set
    if (!window.openPrice && this.btcStream.currentPrice > 0) {
      window.openPrice = this.btcStream.currentPrice;
      console.log(`Open price set: $${window.openPrice}`);
    }

    if (!window.openPrice) return;

    const elapsedMs = now - window.windowStartMs;
    const elapsedS = Math.floor(elapsedMs / 1000);
    const currentPrice = this.btcStream.currentPrice;

    if (currentPrice <= 0) return;

    const priceChange =
      (currentPrice - window.openPrice) / window.openPrice;

    // Get orderbook data for both tokens
    const upBook = this.polyOrderbook.getBook(window.upTokenId);
    const downBook = this.polyOrderbook.getBook(window.downTokenId);
    const marketProbUp = this.polyOrderbook.getImpliedProbUp(
      window.upTokenId
    );

    // Get model prediction
    let statProbUp: number | null = null;
    let edgePp: number | null = null;
    let significant: boolean | null = null;

    if (marketProbUp !== null) {
      const edgeResult = this.lookup.computeEdge(
        elapsedS,
        priceChange,
        marketProbUp
      );
      if (edgeResult) {
        statProbUp = edgeResult.statProb;
        edgePp = edgeResult.edge;
        significant = edgeResult.significant;
      }
    }

    const spreadUp = upBook?.spread ?? 1;
    const actionable =
      upBook !== undefined &&
      spreadUp <= MAX_SPREAD &&
      elapsedS <= MAX_ELAPSED_S &&
      marketProbUp !== null;

    // Build tick record — ALWAYS log to file
    const tick: TickRecord = {
      windowSlug: window.market.slug,
      conditionId: window.market.conditionId,
      upTokenId: window.upTokenId,
      timestamp: now,
      elapsedS,
      btcPrice: currentPrice,
      openPrice: window.openPrice,
      priceChange,
      marketProbUp,
      bestBidUp: upBook?.bestBid ?? 0,
      bestAskUp: upBook?.bestAsk ?? 0,
      spreadUp: upBook?.spread ?? 0,
      bestBidDown: downBook?.bestBid ?? 0,
      bestAskDown: downBook?.bestAsk ?? 0,
      spreadDown: downBook?.spread ?? 0,
      statProbUp,
      edgePp,
      significant,
      bookAvailable: upBook !== undefined,
      actionable,
    };

    window.ticks.push(tick);

    // Always write to log file
    if (this.logStream) {
      this.logStream.write(JSON.stringify(tick) + "\n");
    }

    // Console output: only actionable edges above threshold
    const absEdge = edgePp !== null ? Math.abs(edgePp) : 0;
    if (actionable && absEdge >= MIN_EDGE_PP && edgePp !== null && statProbUp !== null) {
      this.totalEdgeCount++;

      const direction = edgePp > 0 ? "SELL UP" : "BUY UP";
      const changeStr =
        priceChange >= 0
          ? `+${(priceChange * 100).toFixed(4)}%`
          : `${(priceChange * 100).toFixed(4)}%`;

      console.log(
        `[T+${elapsedS.toString().padStart(3)}s] ` +
          `BTC ${changeStr}  ` +
          `Mkt: ${(marketProbUp! * 100).toFixed(1)}%  ` +
          `Mdl: ${(statProbUp * 100).toFixed(1)}%  ` +
          `Edge: ${edgePp > 0 ? "+" : ""}${edgePp.toFixed(1)}pp  ` +
          `Sprd: $${spreadUp.toFixed(2)}  ` +
          `=> ${direction}` +
          `${significant ? " ***" : ""}`
      );
    }

    // Periodic status every 30s
    if (this.scanCount % 30 === 0) {
      const changeStr =
        priceChange >= 0
          ? `+${(priceChange * 100).toFixed(4)}%`
          : `${(priceChange * 100).toFixed(4)}%`;

      const actionableTicks = window.ticks.filter((t) => t.actionable);
      const actionableEdges = actionableTicks.filter(
        (t) => t.edgePp !== null && Math.abs(t.edgePp) >= MIN_EDGE_PP
      );

      console.log(
        `  [status] T+${elapsedS}s | BTC $${currentPrice.toFixed(2)} (${changeStr}) | ` +
          `Mkt: ${marketProbUp !== null ? (marketProbUp * 100).toFixed(1) + "%" : "n/a"} | ` +
          `Sprd: $${spreadUp.toFixed(2)} | ` +
          `Ticks: ${window.ticks.length} (${actionableEdges.length} actionable edges)`
      );
    }
  }

  private finalizeWindow(): void {
    if (!this.currentWindow) return;

    const window = this.currentWindow;
    this.totalWindowCount++;

    const actionableTicks = window.ticks.filter((t) => t.actionable);
    const actionableEdges = actionableTicks.filter(
      (t) => t.edgePp !== null && Math.abs(t.edgePp) >= MIN_EDGE_PP
    );

    const finalPrice = this.btcStream.currentPrice;
    const finalPriceChange = window.openPrice
      ? (finalPrice - window.openPrice) / window.openPrice
      : 0;

    const avgAbsEdge =
      actionableEdges.length > 0
        ? actionableEdges.reduce((s, e) => s + Math.abs(e.edgePp!), 0) /
          actionableEdges.length
        : 0;

    const avgSpread =
      actionableTicks.length > 0
        ? actionableTicks.reduce((s, t) => s + t.spreadUp, 0) /
          actionableTicks.length
        : 0;

    let maxEdgePp = 0;
    let maxEdgeTime = 0;
    for (const t of actionableEdges) {
      if (Math.abs(t.edgePp!) > Math.abs(maxEdgePp)) {
        maxEdgePp = t.edgePp!;
        maxEdgeTime = t.elapsedS;
      }
    }

    const summary: WindowSummary = {
      type: "window_summary",
      windowSlug: window.market.slug,
      conditionId: window.market.conditionId,
      question: window.market.question,
      windowStartMs: window.windowStartMs,
      windowEndMs: window.windowEndMs,
      openPrice: window.openPrice || 0,
      finalPrice,
      finalPriceChange,
      totalTicks: window.ticks.length,
      actionableTicks: actionableTicks.length,
      actionableEdges: actionableEdges.length,
      avgAbsEdge,
      maxEdgePp,
      maxEdgeTime,
      avgSpread,
    };

    // Log summary to file
    if (this.logStream) {
      this.logStream.write(JSON.stringify(summary) + "\n");
    }

    const outcome = finalPriceChange >= 0 ? "UP" : "DOWN";

    console.log(`\n--- WINDOW COMPLETE ---`);
    console.log(`Market: ${window.market.question}`);
    console.log(
      `BTC: $${(window.openPrice || 0).toFixed(2)} → $${finalPrice.toFixed(2)} (${finalPriceChange >= 0 ? "+" : ""}${(finalPriceChange * 100).toFixed(4)}%) => ${outcome}`
    );
    console.log(
      `Ticks: ${window.ticks.length} total, ${actionableTicks.length} actionable`
    );
    console.log(
      `Actionable edges (>= ${MIN_EDGE_PP}pp): ${actionableEdges.length}`
    );
    if (actionableEdges.length > 0) {
      console.log(`  Avg |edge|: ${avgAbsEdge.toFixed(1)}pp`);
      console.log(
        `  Max edge: ${maxEdgePp > 0 ? "+" : ""}${maxEdgePp.toFixed(1)}pp at T+${maxEdgeTime}s`
      );
      console.log(`  Avg spread: $${avgSpread.toFixed(3)}`);
    }
    console.log(
      `Windows completed: ${this.totalWindowCount} | Total actionable edges: ${this.totalEdgeCount}\n`
    );

    this.currentWindow = null;
  }

  stop(): void {
    console.log("\nShutting down...");
    if (this.scanTimer) clearInterval(this.scanTimer);
    if (this.marketRefreshTimer) clearInterval(this.marketRefreshTimer);
    this.btcStream.disconnect();
    this.polyOrderbook.disconnect();
    this.lookup.close();
    if (this.logStream) this.logStream.end();
    console.log(
      `Total scans: ${this.scanCount} | Windows: ${this.totalWindowCount} | Actionable edges: ${this.totalEdgeCount}`
    );
    process.exit(0);
  }
}

// Run
const monitor = new EdgeMonitor();
monitor.start().catch((err) => {
  console.error("Failed to start:", err);
  process.exit(1);
});
