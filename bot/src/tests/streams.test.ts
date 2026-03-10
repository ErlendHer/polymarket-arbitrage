import { describe, it, expect } from "vitest";
import { BtcPriceStream } from "../streams/btc-price.js";
import { PolymarketOrderbook } from "../streams/polymarket-orderbook.js";
import {
  computeWindowSlug,
  computeWindowBounds,
  getTokenIds,
} from "../streams/polymarket-discovery.js";

describe("BtcPriceStream", () => {
  it("should initialize with zero price", () => {
    const stream = new BtcPriceStream();
    expect(stream.currentPrice).toBe(0);
    expect(stream.isConnected).toBe(false);
  });

  it("should be an EventEmitter", () => {
    const stream = new BtcPriceStream();
    expect(typeof stream.on).toBe("function");
    expect(typeof stream.emit).toBe("function");
  });
});

describe("PolymarketOrderbook", () => {
  it("should initialize empty", () => {
    const ob = new PolymarketOrderbook();
    expect(ob.isConnected).toBe(false);
    expect(ob.getBook("fake-token")).toBeUndefined();
  });

  it("should return null implied prob for unknown token", () => {
    const ob = new PolymarketOrderbook();
    expect(ob.getImpliedProbUp("fake-token")).toBeNull();
  });
});

describe("PolymarketDiscovery", () => {
  describe("computeWindowSlug", () => {
    it("should compute correct slug from user-provided example", () => {
      // 1773140400 = the unix timestamp from the user's URL
      const slug = computeWindowSlug(1773140400 * 1000);
      expect(slug).toBe("btc-updown-5m-1773140400");
    });

    it("should round down to 5-minute boundary", () => {
      // 1773140400 + 120s (2 min into window)
      const slug = computeWindowSlug((1773140400 + 120) * 1000);
      expect(slug).toBe("btc-updown-5m-1773140400");
    });

    it("should advance to next window at boundary", () => {
      const slug = computeWindowSlug((1773140400 + 300) * 1000);
      expect(slug).toBe("btc-updown-5m-1773140700");
    });
  });

  describe("computeWindowBounds", () => {
    it("should compute correct start and end", () => {
      const bounds = computeWindowBounds(1773140400 * 1000);
      expect(bounds.startMs).toBe(1773140400 * 1000);
      expect(bounds.endMs).toBe((1773140400 + 300) * 1000);
      expect(bounds.startUnix).toBe(1773140400);
    });

    it("should compute bounds for mid-window time", () => {
      const midWindow = (1773140400 + 150) * 1000; // 2.5 min in
      const bounds = computeWindowBounds(midWindow);
      expect(bounds.startMs).toBe(1773140400 * 1000);
      expect(bounds.endMs).toBe((1773140400 + 300) * 1000);
    });
  });

  describe("getTokenIds", () => {
    it("should extract UP and DOWN tokens", () => {
      const market = {
        conditionId: "test",
        questionId: "test",
        slug: "test",
        question: "test",
        startTime: 0,
        endTime: 0,
        active: true,
        closed: false,
        acceptingOrders: true,
        tokens: [
          { tokenId: "token-up", outcome: "Up", price: 0.5 },
          { tokenId: "token-down", outcome: "Down", price: 0.5 },
        ],
      };
      const { upTokenId, downTokenId } = getTokenIds(market);
      expect(upTokenId).toBe("token-up");
      expect(downTokenId).toBe("token-down");
    });

    it("should return null for missing outcomes", () => {
      const market = {
        conditionId: "test",
        questionId: "test",
        slug: "test",
        question: "test",
        startTime: 0,
        endTime: 0,
        active: true,
        closed: false,
        acceptingOrders: true,
        tokens: [],
      };
      const { upTokenId, downTokenId } = getTokenIds(market);
      expect(upTokenId).toBeNull();
      expect(downTokenId).toBeNull();
    });
  });
});
