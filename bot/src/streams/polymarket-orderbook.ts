/**
 * Polymarket orderbook websocket stream.
 *
 * Connects to the CLOB websocket and streams real-time orderbook updates
 * for specific token IDs (UP/DOWN outcomes).
 *
 * WebSocket endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
 * Requires PING every 10 seconds to keep alive.
 */

import WebSocket from "ws";
import { EventEmitter } from "events";

export interface OrderbookLevel {
  price: number;
  size: number;
}

export interface OrderbookSnapshot {
  tokenId: string;
  bids: OrderbookLevel[];
  asks: OrderbookLevel[];
  bestBid: number;
  bestAsk: number;
  midpoint: number;
  spread: number;
  timestamp: number;
}

const WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL_MS = 9000; // slightly under 10s requirement

export class PolymarketOrderbook extends EventEmitter {
  private ws: WebSocket | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private tokenIds: string[] = [];
  private connected: boolean = false;
  private shouldReconnect: boolean = true;

  // Current orderbook state per token
  private books: Map<string, OrderbookSnapshot> = new Map();

  constructor() {
    super();
  }

  get isConnected(): boolean {
    return this.connected;
  }

  /**
   * Get the current orderbook for a token.
   */
  getBook(tokenId: string): OrderbookSnapshot | undefined {
    return this.books.get(tokenId);
  }

  /**
   * Get market-implied probability of UP outcome.
   * Uses midpoint of best bid/ask as the implied probability.
   */
  getImpliedProbUp(upTokenId: string): number | null {
    const book = this.books.get(upTokenId);
    if (!book || book.bestBid === 0 || book.bestAsk === 0) return null;
    return book.midpoint;
  }

  /**
   * Connect and subscribe to orderbook updates for given token IDs.
   */
  connect(tokenIds: string[]): void {
    this.tokenIds = tokenIds;
    this.shouldReconnect = true;
    this.doConnect();
  }

  private doConnect(): void {
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close();
    }

    this.ws = new WebSocket(WS_URL);

    this.ws.on("open", () => {
      this.connected = true;

      // Subscribe to market data for our tokens
      const subscribeMsg = JSON.stringify({
        assets_ids: this.tokenIds,
        type: "market",
        custom_feature_enabled: true,
      });
      this.ws!.send(subscribeMsg);

      // Start keepalive pings
      this.startPing();

      this.emit("connected");
    });

    this.ws.on("message", (data: Buffer) => {
      const raw = data.toString();

      // Server responds to our PING with a text "PONG" - not JSON
      if (raw === "PONG") return;

      try {
        const messages = JSON.parse(raw);
        // Messages can be arrays
        const msgArray = Array.isArray(messages) ? messages : [messages];

        for (const msg of msgArray) {
          this.handleMessage(msg);
        }
      } catch (err) {
        this.emit(
          "error",
          new Error(`Failed to parse Polymarket WS message: ${err}`)
        );
      }
    });

    this.ws.on("close", (code, reason) => {
      this.connected = false;
      this.stopPing();
      this.emit("disconnected", code, reason.toString());
      if (this.shouldReconnect) {
        this.scheduleReconnect();
      }
    });

    this.ws.on("error", (err) => {
      this.emit("error", err);
    });
  }

  private handleMessage(msg: Record<string, unknown>): void {
    // Handle price_changes wrapper: { price_changes: [{asset_id, price, side, ...}] }
    const priceChanges = msg.price_changes as Array<Record<string, string>> | undefined;
    if (priceChanges && Array.isArray(priceChanges)) {
      for (const pc of priceChanges) {
        this.handlePriceChange(pc.asset_id, pc.price, pc.side);
      }
      return;
    }

    const eventType = msg.event_type as string;
    const assetId = msg.asset_id as string;

    if (!assetId || !this.tokenIds.includes(assetId)) return;

    // Messages with bids/asks array but no event_type are book snapshots
    if (msg.bids && Array.isArray(msg.bids)) {
      const bids = this.parseLevels(msg.bids as Array<Record<string, string>>);
      const asks = this.parseLevels(msg.asks as Array<Record<string, string>>);
      this.updateBook(assetId, bids, asks);
      return;
    }

    // Messages with best_bid/best_ask directly (no event_type needed)
    if (msg.best_bid !== undefined && msg.best_ask !== undefined) {
      const bid = parseFloat(msg.best_bid as string);
      const ask = parseFloat(msg.best_ask as string);
      if (!isNaN(bid) && !isNaN(ask) && bid > 0 && ask > 0) {
        const existing = this.books.get(assetId);
        if (existing) {
          existing.bestBid = bid;
          existing.bestAsk = ask;
          existing.midpoint = (bid + ask) / 2;
          existing.spread = ask - bid;
          existing.timestamp = Date.now();
          this.emit("update", existing);
        }
      }
      return;
    }

    switch (eventType) {
      case "book": {
        const bids = this.parseLevels(msg.bids as Array<Record<string, string>>);
        const asks = this.parseLevels(msg.asks as Array<Record<string, string>>);
        this.updateBook(assetId, bids, asks);
        break;
      }
      case "price_change": {
        this.handlePriceChange(assetId, msg.price as string, msg.side as string);
        break;
      }
      case "tick_size_change":
      case "last_trade_price":
        break;
    }
  }

  private handlePriceChange(assetId: string, priceStr: string, side: string): void {
    if (!assetId || !this.tokenIds.includes(assetId)) return;
    const price = parseFloat(priceStr);
    if (isNaN(price)) return;
    const book = this.books.get(assetId);
    if (book) {
      if (side === "BUY") book.bestBid = price;
      else if (side === "SELL") book.bestAsk = price;
      book.midpoint = (book.bestBid + book.bestAsk) / 2;
      book.spread = book.bestAsk - book.bestBid;
      book.timestamp = Date.now();
      this.emit("update", book);
    }
  }

  private parseLevels(
    levels: Array<Record<string, string>> | undefined
  ): OrderbookLevel[] {
    if (!levels) return [];
    return levels
      .map((l) => ({
        price: parseFloat(l.price),
        size: parseFloat(l.size),
      }))
      .filter((l) => !isNaN(l.price) && !isNaN(l.size) && l.size > 0)
      .sort((a, b) => b.price - a.price); // highest first
  }

  private updateBook(
    tokenId: string,
    bids: OrderbookLevel[],
    asks: OrderbookLevel[]
  ): void {
    const bestBid = bids.length > 0 ? bids[0].price : 0;
    const bestAsk =
      asks.length > 0 ? asks[asks.length - 1].price : 1; // asks sorted desc

    // Sort asks ascending for correct best ask
    const sortedAsks = [...asks].sort((a, b) => a.price - b.price);
    const correctedBestAsk =
      sortedAsks.length > 0 ? sortedAsks[0].price : 1;

    const midpoint = (bestBid + correctedBestAsk) / 2;
    const spread = correctedBestAsk - bestBid;

    const snapshot: OrderbookSnapshot = {
      tokenId,
      bids,
      asks: sortedAsks,
      bestBid,
      bestAsk: correctedBestAsk,
      midpoint,
      spread,
      timestamp: Date.now(),
    };

    const isFirst = !this.books.has(tokenId);
    this.books.set(tokenId, snapshot);
    if (isFirst) {
      console.log(
        `  [orderbook] ${tokenId.slice(0, 16)}... bid=${bestBid} ask=${correctedBestAsk} mid=${midpoint.toFixed(4)} (${bids.length} bids, ${sortedAsks.length} asks)`
      );
    }
    this.emit("book", snapshot);
    this.emit("update", snapshot);
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send("PING");
      }
    }, PING_INTERVAL_MS);
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      this.doConnect();
    }, 3000);
  }

  disconnect(): void {
    this.shouldReconnect = false;
    this.stopPing();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.connected = false;
  }
}
