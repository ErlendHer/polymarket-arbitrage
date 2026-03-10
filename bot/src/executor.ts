/**
 * Executor - Phase 3 Core
 *
 * Places maker limit orders on Polymarket when the statistical model
 * detects an edge vs the market price.
 *
 * Strategy:
 *   - Maker orders only (0% fee + 20% rebate)
 *   - GTD orders that expire at window end
 *   - One order at a time per side (UP token)
 *   - Cancels & replaces when edge shifts
 *   - Position limits enforced per window
 *   - Orderbook-aware sizing (don't exceed available liquidity)
 *   - Balance-aware (checks USDC balance before ordering)
 *
 * Reads .env from project root automatically.
 *
 * Usage:
 *   npx tsx src/executor.ts           # dry-run (default)
 *   DRY_RUN=false npx tsx src/executor.ts  # live
 */

import { ClobClient, Side, OrderType, AssetType } from "@polymarket/clob-client";
import type {
  ApiKeyCreds,
  OrderResponse,
  CreateOrderOptions,
  OpenOrder,
  BalanceAllowanceResponse,
} from "@polymarket/clob-client";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";
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
import fs, { readFileSync } from "fs";
import { resolve } from "path";

// ---- Load .env ----
function loadEnv() {
  const envPath = resolve(__dirname, "../../.env");
  try {
    const content = readFileSync(envPath, "utf-8");
    for (const line of content.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = trimmed.indexOf("=");
      if (eq === -1) continue;
      const key = trimmed.slice(0, eq).trim();
      const val = trimmed.slice(eq + 1).trim();
      if (!process.env[key]) process.env[key] = val;
    }
  } catch {
    // .env not found, rely on existing env vars
  }
}
loadEnv();

// ---- Configuration ----
const LOOKUP_DB_PATH =
  process.env.LOOKUP_DB || path.join(__dirname, "../../data/lookup.db");
const LOG_DIR = process.env.LOG_DIR || path.join(__dirname, "../../data/logs");

const CLOB_HOST = "https://clob.polymarket.com";
const CHAIN_ID = 137; // Polygon mainnet
const USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

// Strategy params
const MIN_EDGE_PP = 5.0; // Minimum edge to place order (pp)
const FEE_BUFFER_PP = 1.0; // Safety buffer for fees/slippage (pp) — maker fees are 0% but keep margin
const EFFECTIVE_MIN_EDGE_PP = MIN_EDGE_PP + FEE_BUFFER_PP; // Actual threshold
const MAX_SPREAD = 0.06; // Max spread to trade ($0.06)
const MAX_ELAPSED_S = 240; // Stop trading after this many seconds
const MIN_ELAPSED_S = 5; // Don't trade in first 5 seconds (unstable)
const CANCEL_EDGE_PP = 2.0; // Cancel order if edge drops below this
const CANCEL_HYSTERESIS = 3; // Require N consecutive scans below threshold before cancelling
const SCAN_INTERVAL_MS = 1000;
const MARKET_REFRESH_INTERVAL_MS = 30_000;
const ORDER_EXPIRY_BUFFER_S = 10; // Expire orders 10s before window end
const BALANCE_CHECK_INTERVAL_MS = 60_000; // Check balance every 60s
const FILL_CHECK_INTERVAL_MS = 3_000; // Check for fills every 3s

// Kelly-fraction position sizing
// We use fractional Kelly (quarter-Kelly) for conservative bankroll management.
// Full Kelly: f* = edge / odds. For a binary market at price p with edge e:
//   Win probability = p + e, payout odds = (1-p)/p
//   f* = ((p+e)*(1/p - 1) - (1-p-e)) / (1/p - 1) ≈ e / (1-p) for small e
// Quarter Kelly divides by 4 for safety.
const KELLY_FRACTION = 0.25; // Quarter-Kelly — conservative
const MAX_BET_FRACTION = 0.05; // Never risk more than 5% of bankroll on one order
const MIN_ORDER_USDC = 5; // Minimum order size
const MAX_POSITION_USDC = 50; // Max total position per window
const DRY_RUN_BANKROLL = 1000; // Simulated bankroll for dry-run sizing

// Dry run mode - log orders but don't place them
const DRY_RUN = process.env.DRY_RUN !== "false"; // Default: true (safe)

interface ActiveOrder {
  orderId: string;
  tokenId: string;
  side: Side;
  price: number;
  size: number;
  placedAt: number;
  edgePp: number;
  weakEdgeCount: number; // consecutive scans where edge is below threshold
}

// A filled position we're tracking through to resolution
interface FilledPosition {
  orderId: string;
  side: Side; // BUY or SELL (of UP token)
  price: number;
  size: number;
  filledAt: number;
  edgePpAtFill: number;
}

interface WindowTradeState {
  market: PolymarketMarket;
  upTokenId: string;
  downTokenId: string;
  openPrice: number | null;
  windowStartMs: number;
  windowEndMs: number;
  activeOrder: ActiveOrder | null;
  filledPositions: FilledPosition[]; // positions awaiting resolution
  totalOrdersPlaced: number;
  totalFilled: number;
  filledSizeUsdc: number;
  positionSizeUsdc: number; // cumulative position
  tickSize: string;
  negRisk: boolean;
}

interface TradeLogEntry {
  type:
    | "order_placed"
    | "order_cancelled"
    | "order_filled"
    | "position_resolved"
    | "edge_signal"
    | "window_start"
    | "window_end"
    | "balance_update"
    | "session_summary";
  timestamp: number;
  windowSlug: string;
  elapsedS: number;
  btcPrice?: number;
  priceChange?: number;
  marketProbUp?: number;
  statProbUp?: number;
  edgePp?: number;
  spread?: number;
  orderId?: string;
  orderSide?: string;
  orderPrice?: number;
  orderSize?: number;
  reason?: string;
  dryRun: boolean;
  // Extended fields
  balanceUsdc?: number;
  pnlUsdc?: number;
  fillPrice?: number;
  fillSize?: number;
  windowsTotal?: number;
  windowsWithFills?: number;
  totalFilledUsdc?: number;
  totalPnlUsdc?: number;
}

// Session-level P&L tracking
interface SessionStats {
  startTime: number;
  startBalanceUsdc: number;
  currentBalanceUsdc: number;
  windowsTotal: number;
  windowsWithFills: number;
  totalOrdersPlaced: number;
  totalOrdersFilled: number;
  totalFilledUsdc: number;
  estimatedPnlUsdc: number; // estimated from edge * size
  actualPnlUsdc: number; // actual P&L from resolved positions
  wins: number; // positions that resolved profitably
  losses: number; // positions that resolved at a loss
}

class Executor {
  private btcStream: BtcPriceStream;
  private polyOrderbook: PolymarketOrderbook;
  private lookup: LookupTable;
  private clobClient!: ClobClient;
  private currentWindow: WindowTradeState | null = null;
  private scanTimer: ReturnType<typeof setInterval> | null = null;
  private marketRefreshTimer: ReturnType<typeof setInterval> | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private fillCheckTimer: ReturnType<typeof setInterval> | null = null;
  private balanceCheckTimer: ReturnType<typeof setInterval> | null = null;
  private heartbeatId: string = "";
  private logStream: fs.WriteStream | null = null;
  private stats: SessionStats = {
    startTime: Date.now(),
    startBalanceUsdc: 0,
    currentBalanceUsdc: 0,
    windowsTotal: 0,
    windowsWithFills: 0,
    totalOrdersPlaced: 0,
    totalOrdersFilled: 0,
    totalFilledUsdc: 0,
    estimatedPnlUsdc: 0,
    actualPnlUsdc: 0,
    wins: 0,
    losses: 0,
  };

  constructor() {
    this.btcStream = new BtcPriceStream();
    this.polyOrderbook = new PolymarketOrderbook();

    if (!fs.existsSync(LOOKUP_DB_PATH)) {
      console.error(`Lookup DB not found at ${LOOKUP_DB_PATH}`);
      process.exit(1);
    }
    this.lookup = new LookupTable(LOOKUP_DB_PATH);
  }

  async start(): Promise<void> {
    // Validate credentials
    const privateKey = process.env.PRIVATE_KEY;
    const apiKey = process.env.API_KEY;
    const apiSecret = process.env.API_SECRET;
    const apiPassphrase = process.env.API_PASSPHRASE;

    if (!DRY_RUN && (!privateKey || !apiKey || !apiSecret || !apiPassphrase)) {
      console.error(
        "Missing credentials. Set PRIVATE_KEY, API_KEY, API_SECRET, API_PASSPHRASE"
      );
      console.error("Or set DRY_RUN=true to run in simulation mode");
      process.exit(1);
    }

    // Initialize CLOB client
    if (!DRY_RUN) {
      const key = privateKey!.startsWith("0x")
        ? (privateKey! as `0x${string}`)
        : (`0x${privateKey!}` as `0x${string}`);
      const account = privateKeyToAccount(key);
      const walletClient = createWalletClient({
        account,
        chain: polygon,
        transport: http(),
      });

      const creds: ApiKeyCreds = {
        key: apiKey!,
        secret: apiSecret!,
        passphrase: apiPassphrase!,
      };

      this.clobClient = new ClobClient(
        CLOB_HOST,
        CHAIN_ID,
        walletClient,
        creds
      );

      console.log(`Wallet address: ${account.address}`);

      // Verify connectivity
      try {
        const ok = await this.clobClient.getOk();
        console.log(`CLOB API: ${ok}`);
      } catch (err) {
        console.error(
          "Failed to connect to CLOB API:",
          (err as Error).message
        );
        process.exit(1);
      }

      // Get initial balance
      await this.updateBalance();
      this.stats.startBalanceUsdc = this.stats.currentBalanceUsdc;

      // Start heartbeat to prevent auto-cancellation of orders
      // Polymarket cancels all orders if no heartbeat within 10s
      this.heartbeatTimer = setInterval(async () => {
        try {
          const resp = await this.clobClient.postHeartbeat(this.heartbeatId);
          this.heartbeatId = resp.heartbeat_id;
        } catch (err) {
          console.error("Heartbeat error:", (err as Error).message);
        }
      }, 5000);
      console.log("Heartbeat keepalive started (5s interval)");

      // Periodic balance check
      this.balanceCheckTimer = setInterval(
        () => this.updateBalance(),
        BALANCE_CHECK_INTERVAL_MS
      );

      // Periodic fill check
      this.fillCheckTimer = setInterval(
        () => this.checkFills(),
        FILL_CHECK_INTERVAL_MS
      );
    }

    const meta = this.lookup.getMetadata();
    console.log(`\n=== POLYMARKET EXECUTOR ===`);
    console.log(
      `Mode: ${DRY_RUN ? "DRY RUN (no real orders)" : "LIVE TRADING"}`
    );
    console.log(`Knowledge base: ${meta.totalEntries} entries`);
    console.log(
      `Strategy: Maker orders, edge >= ${EFFECTIVE_MIN_EDGE_PP}pp (${MIN_EDGE_PP} + ${FEE_BUFFER_PP} buffer), spread <= $${MAX_SPREAD}`
    );
    console.log(
      `Sizing: ${KELLY_FRACTION * 100}% Kelly, max ${MAX_BET_FRACTION * 100}% of bankroll/order, max $${MAX_POSITION_USDC}/window`
    );
    console.log(
      `Cancel: immediate on edge flip, ${CANCEL_HYSTERESIS} consecutive scans below ${CANCEL_EDGE_PP}pp otherwise`
    );
    console.log(`Time window: ${MIN_ELAPSED_S}s - ${MAX_ELAPSED_S}s`);
    if (!DRY_RUN) {
      console.log(
        `Balance: $${this.stats.currentBalanceUsdc.toFixed(2)} USDC`
      );
    }
    console.log();

    // Set up logging
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const logFile = path.join(
      LOG_DIR,
      `executor-${new Date().toISOString().slice(0, 10)}.jsonl`
    );
    this.logStream = fs.createWriteStream(logFile, { flags: "a" });
    console.log(`Trade log: ${logFile}`);

    // Connect streams
    this.btcStream.on("connected", () => console.log("Binance WS connected"));
    this.btcStream.on("disconnected", (code, reason) =>
      console.log(`Binance WS disconnected: ${code} ${reason}`)
    );
    this.btcStream.on("error", (err) =>
      console.error("Binance WS error:", err.message)
    );
    this.btcStream.connect();

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
    console.log("\nExecutor running. Press Ctrl+C to stop.\n");

    // Print periodic status
    setInterval(() => this.printStatus(), 60_000);

    process.on("SIGINT", () => this.stop());
    process.on("SIGTERM", () => this.stop());
  }

  private async updateBalance(): Promise<void> {
    if (DRY_RUN) return;
    try {
      const resp: BalanceAllowanceResponse =
        await this.clobClient.getBalanceAllowance({
          asset_type: AssetType.COLLATERAL,
        });
      const balance = parseFloat(resp.balance) / 1e6; // USDC has 6 decimals
      this.stats.currentBalanceUsdc = balance;
    } catch (err) {
      console.error("Balance check error:", (err as Error).message);
    }
  }

  private async checkFills(): Promise<void> {
    if (DRY_RUN || !this.currentWindow?.activeOrder) return;

    const w = this.currentWindow;
    const order = w.activeOrder;

    try {
      const openOrders = await this.clobClient.getOpenOrders({
        id: order.orderId,
      });

      const found = (openOrders as OpenOrder[]).find(
        (o) => o.id === order.orderId
      );

      if (!found) {
        // Order is gone — could be filled, expired, or cancelled externally.
        // Check trades API to see if it actually filled.
        let filledSize = 0;
        try {
          const trades = await this.clobClient.getTrades({
            id: order.orderId,
          });
          if (Array.isArray(trades)) {
            for (const t of trades) {
              filledSize += parseFloat((t as any).size || "0");
            }
          }
        } catch {
          // Trades API failed — order likely expired/cancelled unfilled
        }

        if (filledSize > 0) {
          this.recordFill(w, order, filledSize);
        } else {
          console.log(
            `  [EXPIRED] ${order.side} ${order.size}@${order.price.toFixed(3)} (no fill detected)`
          );
        }

        w.activeOrder = null;
      } else {
        // Order still open — check for partial fills via size_matched
        const matched = parseFloat(found.size_matched || "0");
        if (matched > 0) {
          console.log(
            `  [PARTIAL] ${order.orderId.slice(0, 12)}... matched ${matched}/${order.size}`
          );
        }
      }
    } catch (err) {
      // Don't spam errors — order might just have expired
    }
  }

  private recordFill(
    w: WindowTradeState,
    order: ActiveOrder,
    filledSize: number
  ): void {
    const fillUsdc = order.price * filledSize;
    w.totalFilled++;
    w.filledSizeUsdc += fillUsdc;
    this.stats.totalOrdersFilled++;
    this.stats.totalFilledUsdc += fillUsdc;

    // Estimated profit based on edge at time of fill
    const estimatedProfit = (Math.abs(order.edgePp) / 100) * fillUsdc;
    this.stats.estimatedPnlUsdc += estimatedProfit;

    // Track position for resolution at window end
    w.filledPositions.push({
      orderId: order.orderId,
      side: order.side,
      price: order.price,
      size: filledSize,
      filledAt: Date.now(),
      edgePpAtFill: order.edgePp,
    });

    console.log(
      `  [FILLED] ${order.side} ${filledSize}@${order.price.toFixed(3)} ` +
        `($${fillUsdc.toFixed(2)}) est.profit: $${estimatedProfit.toFixed(2)}`
    );

    this.log({
      type: "order_filled",
      timestamp: Date.now(),
      windowSlug: w.market.slug,
      elapsedS: Math.floor((Date.now() - w.windowStartMs) / 1000),
      orderId: order.orderId,
      orderSide: order.side,
      fillPrice: order.price,
      fillSize: filledSize,
      edgePp: order.edgePp,
      pnlUsdc: estimatedProfit,
      dryRun: false,
    });
  }

  private printStatus(): void {
    const uptimeS = Math.floor((Date.now() - this.stats.startTime) / 1000);
    const uptimeM = Math.floor(uptimeS / 60);
    const uptimeH = Math.floor(uptimeM / 60);

    console.log(`\n--- STATUS (uptime: ${uptimeH}h${uptimeM % 60}m) ---`);
    console.log(
      `Windows: ${this.stats.windowsTotal} total, ${this.stats.windowsWithFills} with fills`
    );
    console.log(
      `Orders: ${this.stats.totalOrdersPlaced} placed, ${this.stats.totalOrdersFilled} filled`
    );
    console.log(
      `Volume: $${this.stats.totalFilledUsdc.toFixed(2)} | Est.PnL: $${this.stats.estimatedPnlUsdc.toFixed(2)} | Actual: $${this.stats.actualPnlUsdc.toFixed(2)}`
    );
    console.log(
      `Record: ${this.stats.wins}W / ${this.stats.losses}L`
    );
    if (!DRY_RUN) {
      console.log(
        `Balance: $${this.stats.currentBalanceUsdc.toFixed(2)} USDC (started: $${this.stats.startBalanceUsdc.toFixed(2)})`
      );
    }
    console.log();
  }

  private async refreshMarkets(): Promise<void> {
    try {
      const active = await findCurrentWindow();
      const now = Date.now();

      if (!active) {
        if (this.currentWindow) {
          await this.finalizeWindow();
        }
        const bounds = computeWindowBounds(now);
        const waitS = Math.round((bounds.endMs - now) / 1000);
        console.log(`No active market. Next window in ~${waitS}s`);
        this.currentWindow = null;
        return;
      }

      const { upTokenId, downTokenId } = getTokenIds(active);
      if (!upTokenId || !downTokenId) {
        console.log("Could not find UP/DOWN token IDs");
        return;
      }

      if (this.currentWindow?.market.conditionId === active.conditionId) {
        return;
      }

      // Finalize previous window
      if (this.currentWindow) {
        await this.finalizeWindow();
      }

      // Get market config from CLOB
      let tickSize = "0.01";
      let negRisk = false;
      if (!DRY_RUN) {
        try {
          const book = await this.clobClient.getOrderBook(upTokenId);
          tickSize = book.tick_size || "0.01";
          negRisk = book.neg_risk || false;
        } catch {
          console.log("Could not fetch tick size, using default 0.01");
        }
      }

      console.log(`\n--- NEW WINDOW ---`);
      console.log(`Market: ${active.question}`);
      console.log(`UP: ${upTokenId.slice(0, 20)}...`);
      console.log(`Tick: ${tickSize} | NegRisk: ${negRisk}`);
      if (!DRY_RUN) {
        console.log(
          `Balance: $${this.stats.currentBalanceUsdc.toFixed(2)} USDC`
        );
      }

      this.currentWindow = {
        market: active,
        upTokenId,
        downTokenId,
        openPrice: null,
        windowStartMs: active.startTime,
        windowEndMs: active.endTime,
        activeOrder: null,
        filledPositions: [],
        totalOrdersPlaced: 0,
        totalFilled: 0,
        filledSizeUsdc: 0,
        positionSizeUsdc: 0,
        tickSize,
        negRisk,
      };

      this.log({
        type: "window_start",
        timestamp: now,
        windowSlug: active.slug,
        elapsedS: 0,
        dryRun: DRY_RUN,
        balanceUsdc: this.stats.currentBalanceUsdc,
      });

      this.polyOrderbook.disconnect();
      this.polyOrderbook.connect([upTokenId, downTokenId]);

      if (this.btcStream.currentPrice > 0) {
        this.currentWindow.openPrice = this.btcStream.currentPrice;
        console.log(`Open price: $${this.currentWindow.openPrice}`);
      }
    } catch (err) {
      console.error("Market refresh error:", (err as Error).message);
    }
  }

  private async scan(): Promise<void> {
    if (!this.currentWindow) return;

    const now = Date.now();
    const w = this.currentWindow;

    // Window expired
    if (now >= w.windowEndMs) {
      await this.finalizeWindow();
      return;
    }

    // Set open price
    if (!w.openPrice && this.btcStream.currentPrice > 0) {
      w.openPrice = this.btcStream.currentPrice;
      console.log(`Open price set: $${w.openPrice}`);
    }
    if (!w.openPrice) return;

    const elapsedS = Math.floor((now - w.windowStartMs) / 1000);
    const currentPrice = this.btcStream.currentPrice;
    if (currentPrice <= 0) return;

    const priceChange = (currentPrice - w.openPrice) / w.openPrice;

    // Get market data
    const upBook = this.polyOrderbook.getBook(w.upTokenId);
    const marketProbUp = this.polyOrderbook.getImpliedProbUp(w.upTokenId);
    if (marketProbUp === null || !upBook) return;

    const spread = upBook.spread;

    // Get model prediction
    const edgeResult = this.lookup.computeEdge(
      elapsedS,
      priceChange,
      marketProbUp
    );
    if (!edgeResult) return;

    const { edge: edgePp, statProb: statProbUp, significant } = edgeResult;
    const absEdge = Math.abs(edgePp);

    // Check if we should cancel existing order
    if (w.activeOrder) {
      const shouldCancel = await this.shouldCancelOrder(
        w,
        edgePp,
        elapsedS,
        spread
      );
      if (shouldCancel) {
        await this.cancelActiveOrder(w, "edge_disappeared");
      }
    }

    // Check if conditions are right for a new order
    const hasBalance =
      DRY_RUN || this.stats.currentBalanceUsdc >= ORDER_SIZE_USDC;
    const isActionable =
      elapsedS >= MIN_ELAPSED_S &&
      elapsedS <= MAX_ELAPSED_S &&
      spread <= MAX_SPREAD &&
      absEdge >= EFFECTIVE_MIN_EDGE_PP &&
      significant &&
      w.positionSizeUsdc < MAX_POSITION_USDC &&
      w.activeOrder === null &&
      hasBalance;

    if (!isActionable) return;

    // Determine order side and price
    // Negative edge = market underprices UP = BUY UP
    // Positive edge = market overprices UP = SELL UP (buy DOWN)
    const orderSide = edgePp < 0 ? Side.BUY : Side.SELL;

    // Price: place at the best bid/ask to be a maker
    let orderPrice: number;
    const tick = parseFloat(w.tickSize);

    if (orderSide === Side.BUY) {
      // BUY UP at best bid + 1 tick (aggressive maker)
      orderPrice = upBook.bestBid + tick;
      // Don't exceed our model's fair value
      orderPrice = Math.min(orderPrice, statProbUp);
    } else {
      // SELL UP at best ask - 1 tick (aggressive maker)
      orderPrice = upBook.bestAsk - tick;
      // Don't go below our model's fair value
      orderPrice = Math.max(orderPrice, statProbUp);
    }

    // Round to tick size
    orderPrice = Math.round(orderPrice / tick) * tick;
    orderPrice = Math.max(tick, Math.min(1 - tick, orderPrice));

    // ---- Kelly-fraction position sizing ----
    // Edge as a decimal (e.g. 8pp → 0.08)
    const edgeDecimal = absEdge / 100;
    // Binary market: if we buy at price p, we pay p to win 1.
    // Win prob ≈ statProbUp (our model), payout = 1/p - 1 on win, lose p on loss.
    // Kelly fraction: f* = (p_win * b - p_loss) / b, where b = (1-p)/p
    //   Simplifies to: f* ≈ edge / (1 - price) for buying, edge / price for selling
    const payoutOdds =
      orderSide === Side.BUY
        ? (1 - orderPrice) / orderPrice // BUY: pay p, win (1-p)
        : orderPrice / (1 - orderPrice); // SELL: pay (1-p), win p
    const kellyFull = (statProbUp > 0 && statProbUp < 1)
      ? Math.max(0, (edgeDecimal * (1 + payoutOdds)) / payoutOdds)
      : 0;
    const kellyFraction = kellyFull * KELLY_FRACTION; // Quarter Kelly

    // Bankroll: actual balance or simulated
    const bankroll = DRY_RUN
      ? DRY_RUN_BANKROLL
      : this.stats.currentBalanceUsdc;

    // Kelly-sized bet in USDC
    let sizeBudgetUsdc = kellyFraction * bankroll;

    // Hard cap: never risk more than MAX_BET_FRACTION of bankroll
    sizeBudgetUsdc = Math.min(sizeBudgetUsdc, bankroll * MAX_BET_FRACTION);

    // Cap by remaining position limit for this window
    sizeBudgetUsdc = Math.min(
      sizeBudgetUsdc,
      MAX_POSITION_USDC - w.positionSizeUsdc
    );

    // Cap by available balance (leave $5 buffer for gas/fees)
    if (!DRY_RUN) {
      const availableBalance = Math.max(
        0,
        this.stats.currentBalanceUsdc - 5
      );
      sizeBudgetUsdc = Math.min(sizeBudgetUsdc, availableBalance);
    }

    // Cap by orderbook depth (don't exceed 80% of visible liquidity)
    const bookDepthUsdc = this.getBookDepthUsdc(upBook, orderSide, orderPrice);
    if (bookDepthUsdc > 0) {
      sizeBudgetUsdc = Math.min(sizeBudgetUsdc, bookDepthUsdc * 0.8);
    }

    // Floor: minimum order size
    sizeBudgetUsdc = Math.max(sizeBudgetUsdc, MIN_ORDER_USDC);

    // But don't exceed balance/position caps we already applied
    if (!DRY_RUN) {
      const availableBalance = Math.max(0, this.stats.currentBalanceUsdc - 5);
      sizeBudgetUsdc = Math.min(sizeBudgetUsdc, availableBalance);
    }
    sizeBudgetUsdc = Math.min(sizeBudgetUsdc, MAX_POSITION_USDC - w.positionSizeUsdc);

    // Convert to shares
    const orderSize = Math.floor(sizeBudgetUsdc / orderPrice);
    if (orderSize < 5) return; // Polymarket min order ~5 shares

    // Log the signal
    this.log({
      type: "edge_signal",
      timestamp: now,
      windowSlug: w.market.slug,
      elapsedS,
      btcPrice: currentPrice,
      priceChange,
      marketProbUp,
      statProbUp,
      edgePp,
      spread,
      orderSide: orderSide,
      orderPrice,
      orderSize,
      dryRun: DRY_RUN,
      balanceUsdc: this.stats.currentBalanceUsdc,
    });

    // Place the order
    await this.placeOrder(
      w,
      orderSide,
      orderPrice,
      orderSize,
      elapsedS,
      edgePp
    );
  }

  /**
   * Estimate available liquidity on the opposite side of the book
   * at or better than our limit price.
   */
  private getBookDepthUsdc(
    book: OrderbookSnapshot,
    ourSide: Side,
    limitPrice: number
  ): number {
    // If we're BUYing, we want to see asks at/below our price
    // If we're SELLing, we want to see bids at/above our price
    // But since we're placing a maker order, we actually don't need
    // to cross the spread — we just need the book to not be empty.
    // Use total visible book depth as a proxy for market activity.
    const bidDepth = book.bids
      ? book.bids.reduce((sum, lvl) => sum + lvl.size, 0)
      : 0;
    const askDepth = book.asks
      ? book.asks.reduce((sum, lvl) => sum + lvl.size, 0)
      : 0;

    // Convert to USDC (rough estimate using midpoint)
    const mid = book.midpoint || 0.5;
    return (bidDepth + askDepth) * mid;
  }

  private async shouldCancelOrder(
    w: WindowTradeState,
    edgePp: number,
    elapsedS: number,
    spread: number
  ): Promise<boolean> {
    if (!w.activeOrder) return false;

    // Always cancel immediately if edge has flipped direction (wrong side of market)
    const orderIsBuy = w.activeOrder.side === Side.BUY;
    const edgeSaysBuy = edgePp < 0;
    if (orderIsBuy !== edgeSaysBuy) {
      w.activeOrder.weakEdgeCount = 0;
      return true;
    }

    // Always cancel immediately if spread blew out (market is unreliable)
    if (spread > MAX_SPREAD * 1.5) {
      w.activeOrder.weakEdgeCount = 0;
      return true;
    }

    // For edge decay: use hysteresis — require N consecutive weak scans
    // This prevents cancelling on momentary noise
    const edgeIsWeak =
      Math.abs(edgePp) < CANCEL_EDGE_PP || spread > MAX_SPREAD;
    if (edgeIsWeak) {
      w.activeOrder.weakEdgeCount++;
      if (w.activeOrder.weakEdgeCount >= CANCEL_HYSTERESIS) return true;
    } else {
      // Edge recovered — reset counter
      w.activeOrder.weakEdgeCount = 0;
    }

    // Cancel if past time limit
    if (elapsedS > MAX_ELAPSED_S) return true;

    // Cancel if approaching window end (60s before)
    const timeToEnd = (w.windowEndMs - Date.now()) / 1000;
    if (timeToEnd < 60) return true;

    return false;
  }

  private async cancelActiveOrder(
    w: WindowTradeState,
    reason: string
  ): Promise<void> {
    if (!w.activeOrder) return;

    const order = w.activeOrder;
    console.log(
      `  [CANCEL] ${order.side} ${order.size}@${order.price.toFixed(3)} (${reason})`
    );

    if (!DRY_RUN) {
      try {
        await this.clobClient.cancelOrder({ orderID: order.orderId });
      } catch (err) {
        console.error("Cancel error:", (err as Error).message);
      }
    }

    this.log({
      type: "order_cancelled",
      timestamp: Date.now(),
      windowSlug: w.market.slug,
      elapsedS: Math.floor((Date.now() - w.windowStartMs) / 1000),
      orderId: order.orderId,
      orderSide: order.side,
      orderPrice: order.price,
      orderSize: order.size,
      reason,
      dryRun: DRY_RUN,
    });

    w.activeOrder = null;
  }

  private async placeOrder(
    w: WindowTradeState,
    side: Side,
    price: number,
    size: number,
    elapsedS: number,
    edgePp: number
  ): Promise<void> {
    const direction = side === Side.BUY ? "BUY UP" : "SELL UP";
    const expirationUnix =
      Math.floor(w.windowEndMs / 1000) - ORDER_EXPIRY_BUFFER_S;
    const costUsdc = (price * size).toFixed(2);
    const bankroll = DRY_RUN ? DRY_RUN_BANKROLL : this.stats.currentBalanceUsdc;
    const betPct = bankroll > 0 ? ((price * size) / bankroll * 100).toFixed(1) : "?";

    console.log(
      `  [ORDER] ${direction} ${size}@${price.toFixed(3)} ($${costUsdc}, ${betPct}% of bankroll) ` +
        `edge: ${edgePp > 0 ? "+" : ""}${edgePp.toFixed(1)}pp ` +
        `${DRY_RUN ? "[DRY RUN]" : ""}`
    );

    let orderId = `dry-${Date.now()}`;

    if (!DRY_RUN) {
      try {
        const options: CreateOrderOptions = {
          tickSize: w.tickSize as "0.01" | "0.001" | "0.0001" | "0.1",
          negRisk: w.negRisk,
        };

        const result: OrderResponse = await this.clobClient.createAndPostOrder(
          {
            tokenID: w.upTokenId,
            price,
            side,
            size,
            feeRateBps: 0,
            expiration: expirationUnix,
          },
          options,
          OrderType.GTD
        );

        if (!result.success) {
          console.error(`  Order failed: ${result.errorMsg}`);
          return;
        }

        orderId = result.orderID;
        console.log(`  Order placed: ${orderId}`);
      } catch (err) {
        console.error("  Order error:", (err as Error).message);
        return;
      }
    }

    w.activeOrder = {
      orderId,
      tokenId: w.upTokenId,
      side,
      price,
      size,
      placedAt: Date.now(),
      edgePp,
      weakEdgeCount: 0,
    };

    w.totalOrdersPlaced++;
    w.positionSizeUsdc += price * size;
    this.stats.totalOrdersPlaced++;

    this.log({
      type: "order_placed",
      timestamp: Date.now(),
      windowSlug: w.market.slug,
      elapsedS,
      orderId,
      orderSide: side,
      orderPrice: price,
      orderSize: size,
      edgePp,
      dryRun: DRY_RUN,
      balanceUsdc: this.stats.currentBalanceUsdc,
    });
  }

  private async finalizeWindow(): Promise<void> {
    if (!this.currentWindow) return;

    const w = this.currentWindow;

    // Cancel any active order
    if (w.activeOrder) {
      await this.cancelActiveOrder(w, "window_end");
    }

    // Cancel all orders for this market (safety net)
    if (!DRY_RUN) {
      try {
        await this.clobClient.cancelMarketOrders({
          market: w.market.conditionId,
        });
      } catch {
        // Ignore - may have no orders
      }
    }

    this.stats.windowsTotal++;

    // Resolve filled positions using actual BTC price
    // The market resolves based on whether BTC went up or down vs open price
    if (w.filledPositions.length > 0 && w.openPrice) {
      this.stats.windowsWithFills++;
      const currentBtc = this.btcStream.currentPrice;
      const btcWentUp = currentBtc > w.openPrice;
      // UP token pays $1 if BTC went up, $0 if down
      const upTokenPayout = btcWentUp ? 1 : 0;

      let windowPnl = 0;
      for (const pos of w.filledPositions) {
        let pnl: number;
        if (pos.side === Side.BUY) {
          // Bought UP token at pos.price → payout is upTokenPayout per share
          pnl = (upTokenPayout - pos.price) * pos.size;
        } else {
          // Sold UP token at pos.price → we receive pos.price, owe upTokenPayout
          pnl = (pos.price - upTokenPayout) * pos.size;
        }
        windowPnl += pnl;

        if (pnl >= 0) {
          this.stats.wins++;
        } else {
          this.stats.losses++;
        }

        this.log({
          type: "position_resolved",
          timestamp: Date.now(),
          windowSlug: w.market.slug,
          elapsedS: 300,
          orderId: pos.orderId,
          orderSide: pos.side,
          fillPrice: pos.price,
          fillSize: pos.size,
          edgePp: pos.edgePpAtFill,
          pnlUsdc: pnl,
          btcPrice: currentBtc,
          dryRun: DRY_RUN,
          reason: btcWentUp ? "UP_wins" : "DOWN_wins",
        });
      }

      this.stats.actualPnlUsdc += windowPnl;
      const winLoss = windowPnl >= 0 ? "WIN" : "LOSS";
      console.log(
        `  [RESOLVED] BTC ${btcWentUp ? "UP" : "DOWN"} ($${w.openPrice.toFixed(2)} → $${currentBtc.toFixed(2)}) ` +
          `${w.filledPositions.length} positions → ${winLoss} $${windowPnl.toFixed(2)}`
      );
    }

    this.log({
      type: "window_end",
      timestamp: Date.now(),
      windowSlug: w.market.slug,
      elapsedS: 300,
      dryRun: DRY_RUN,
      reason: `orders=${w.totalOrdersPlaced} filled=${w.totalFilled} position=$${w.positionSizeUsdc.toFixed(2)}`,
      balanceUsdc: this.stats.currentBalanceUsdc,
      windowsTotal: this.stats.windowsTotal,
      windowsWithFills: this.stats.windowsWithFills,
      totalFilledUsdc: this.stats.totalFilledUsdc,
      totalPnlUsdc: this.stats.actualPnlUsdc,
    });

    console.log(`\n--- WINDOW COMPLETE ---`);
    console.log(`Market: ${w.market.question}`);
    console.log(
      `Orders: ${w.totalOrdersPlaced} placed, ${w.totalFilled} filled ($${w.filledSizeUsdc.toFixed(2)})`
    );
    console.log(
      `Session: ${this.stats.windowsTotal} windows | ${this.stats.totalOrdersFilled} fills`
    );
    console.log(
      `  Est.PnL: $${this.stats.estimatedPnlUsdc.toFixed(2)} | Actual PnL: $${this.stats.actualPnlUsdc.toFixed(2)} | ${this.stats.wins}W/${this.stats.losses}L`
    );
    if (!DRY_RUN) {
      console.log(
        `Balance: $${this.stats.currentBalanceUsdc.toFixed(2)} USDC`
      );
    }
    console.log();

    this.currentWindow = null;
  }

  private log(entry: TradeLogEntry): void {
    if (this.logStream) {
      this.logStream.write(JSON.stringify(entry) + "\n");
    }
  }

  async stop(): Promise<void> {
    console.log("\nShutting down executor...");
    if (this.scanTimer) clearInterval(this.scanTimer);
    if (this.marketRefreshTimer) clearInterval(this.marketRefreshTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.fillCheckTimer) clearInterval(this.fillCheckTimer);
    if (this.balanceCheckTimer) clearInterval(this.balanceCheckTimer);

    // Cancel all active orders
    if (this.currentWindow?.activeOrder && !DRY_RUN) {
      try {
        await this.clobClient.cancelAll();
        console.log("All orders cancelled");
      } catch (err) {
        console.error("Cancel all error:", (err as Error).message);
      }
    }

    this.btcStream.disconnect();
    this.polyOrderbook.disconnect();
    this.lookup.close();

    // Final session summary
    const uptimeS = Math.floor((Date.now() - this.stats.startTime) / 1000);
    console.log(`\n=== SESSION SUMMARY ===`);
    console.log(`Uptime: ${Math.floor(uptimeS / 3600)}h${Math.floor((uptimeS % 3600) / 60)}m`);
    console.log(`Windows: ${this.stats.windowsTotal} total, ${this.stats.windowsWithFills} with fills`);
    console.log(`Orders: ${this.stats.totalOrdersPlaced} placed, ${this.stats.totalOrdersFilled} filled`);
    console.log(`Volume: $${this.stats.totalFilledUsdc.toFixed(2)}`);
    console.log(`Est.PnL: $${this.stats.estimatedPnlUsdc.toFixed(2)} | Actual PnL: $${this.stats.actualPnlUsdc.toFixed(2)}`);
    console.log(`Record: ${this.stats.wins}W / ${this.stats.losses}L`);
    if (!DRY_RUN) {
      const balanceChange = this.stats.currentBalanceUsdc - this.stats.startBalanceUsdc;
      console.log(`Balance: $${this.stats.currentBalanceUsdc.toFixed(2)} (${balanceChange >= 0 ? "+" : ""}${balanceChange.toFixed(2)})`);
    }

    this.log({
      type: "session_summary",
      timestamp: Date.now(),
      windowSlug: "",
      elapsedS: uptimeS,
      dryRun: DRY_RUN,
      balanceUsdc: this.stats.currentBalanceUsdc,
      totalPnlUsdc: this.stats.estimatedPnlUsdc,
      totalFilledUsdc: this.stats.totalFilledUsdc,
      windowsTotal: this.stats.windowsTotal,
      windowsWithFills: this.stats.windowsWithFills,
    });

    if (this.logStream) this.logStream.end();
    process.exit(0);
  }
}

// Run
const executor = new Executor();
executor.start().catch((err) => {
  console.error("Failed to start executor:", err);
  process.exit(1);
});
