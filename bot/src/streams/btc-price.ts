/**
 * BTC price websocket stream via Binance.
 *
 * Connects to Binance's trade stream for BTCUSDT and emits price updates.
 * Uses the aggTrade stream for lowest latency (~100ms updates).
 *
 * This is NOT the resolution oracle (that's Chainlink), but serves as a
 * fast leading indicator. Binance BTCUSDT is the most liquid BTC pair
 * and closely tracks what Chainlink will report.
 */

import WebSocket from "ws";
import { EventEmitter } from "events";

export interface PriceUpdate {
  price: number;
  timestamp: number; // ms
  volume: number;
  source: "binance";
}

export interface BtcPriceStreamEvents {
  price: (update: PriceUpdate) => void;
  connected: () => void;
  disconnected: (code: number, reason: string) => void;
  error: (error: Error) => void;
}

export class BtcPriceStream extends EventEmitter {
  private ws: WebSocket | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private lastPrice: number = 0;
  private lastTimestamp: number = 0;
  private connected: boolean = false;
  private shouldReconnect: boolean = true;

  // Binance aggTrade stream - lowest latency price updates
  private readonly url =
    "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";

  constructor() {
    super();
  }

  get currentPrice(): number {
    return this.lastPrice;
  }

  get isConnected(): boolean {
    return this.connected;
  }

  connect(): void {
    this.shouldReconnect = true;
    this.doConnect();
  }

  private doConnect(): void {
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close();
    }

    this.ws = new WebSocket(this.url);

    this.ws.on("open", () => {
      this.connected = true;
      this.emit("connected");
    });

    this.ws.on("message", (data: Buffer) => {
      try {
        const msg = JSON.parse(data.toString());
        // aggTrade format: { e: "aggTrade", p: "price", T: timestamp, q: "quantity" }
        if (msg.e === "aggTrade") {
          const update: PriceUpdate = {
            price: parseFloat(msg.p),
            timestamp: msg.T,
            volume: parseFloat(msg.q),
            source: "binance",
          };
          this.lastPrice = update.price;
          this.lastTimestamp = update.timestamp;
          this.emit("price", update);
        }
      } catch (err) {
        this.emit("error", new Error(`Failed to parse Binance message: ${err}`));
      }
    });

    this.ws.on("close", (code, reason) => {
      this.connected = false;
      this.emit("disconnected", code, reason.toString());
      if (this.shouldReconnect) {
        this.scheduleReconnect();
      }
    });

    this.ws.on("error", (err) => {
      this.emit("error", err);
    });
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      this.doConnect();
    }, 2000);
  }

  disconnect(): void {
    this.shouldReconnect = false;
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
