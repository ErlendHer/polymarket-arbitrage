/**
 * Polymarket market discovery service.
 *
 * Discovers active BTC 5-minute up/down markets via the Gamma API.
 * Each 5-minute window is a separate event with slug pattern:
 *   btc-updown-5m-{unix_timestamp}
 *
 * The timestamp is the start time of the window, aligned to 5-minute boundaries.
 * We can compute the slug deterministically from the current time.
 */

import https from "https";

export interface PolymarketToken {
  tokenId: string;
  outcome: string; // "Up" or "Down"
  price: number;
}

export interface PolymarketMarket {
  conditionId: string;
  questionId: string;
  slug: string;
  question: string;
  startTime: number; // ms timestamp
  endTime: number; // ms timestamp
  tokens: PolymarketToken[];
  active: boolean;
  closed: boolean;
  acceptingOrders: boolean;
}

const GAMMA_API = "https://gamma-api.polymarket.com";
const WINDOW_DURATION_S = 300; // 5 minutes

/**
 * Fetch JSON from Gamma API.
 */
function fetchJson(url: string): Promise<unknown> {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { Accept: "application/json" } }, (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(new Error(`Failed to parse response from ${url}: ${e}`));
          }
        });
      })
      .on("error", reject);
  });
}

/**
 * Compute the slug for the BTC 5-min window that contains a given timestamp.
 *
 * The slug is: btc-updown-5m-{floor(unixSeconds / 300) * 300}
 */
export function computeWindowSlug(timestampMs: number): string {
  const unixS = Math.floor(timestampMs / 1000);
  const windowStart = Math.floor(unixS / WINDOW_DURATION_S) * WINDOW_DURATION_S;
  return `btc-updown-5m-${windowStart}`;
}

/**
 * Compute the start/end timestamps for the window containing a given time.
 */
export function computeWindowBounds(timestampMs: number): {
  startMs: number;
  endMs: number;
  startUnix: number;
} {
  const unixS = Math.floor(timestampMs / 1000);
  const windowStart = Math.floor(unixS / WINDOW_DURATION_S) * WINDOW_DURATION_S;
  return {
    startMs: windowStart * 1000,
    endMs: (windowStart + WINDOW_DURATION_S) * 1000,
    startUnix: windowStart,
  };
}

/**
 * Parse a Gamma API event response into our PolymarketMarket format.
 */
function parseEvent(event: Record<string, unknown>): PolymarketMarket | null {
  const eventMarkets =
    (event.markets as Array<Record<string, unknown>>) || [];
  if (eventMarkets.length === 0) return null;

  const market = eventMarkets[0]; // BTC up/down events have exactly 1 market

  const tokens: PolymarketToken[] = [];

  // These fields come as JSON strings from the API, e.g. "[\"id1\", \"id2\"]"
  const rawTokenIds = market.clobTokenIds;
  const rawOutcomes = market.outcomes;
  const rawPrices = market.outcomePrices;

  const parseJsonOrArray = (val: unknown): string[] => {
    if (Array.isArray(val)) return val;
    if (typeof val === "string") {
      try { return JSON.parse(val); } catch { return []; }
    }
    return [];
  };

  const clobTokenIds = parseJsonOrArray(rawTokenIds);
  const parsedOutcomes = parseJsonOrArray(rawOutcomes);
  const parsedPrices = parseJsonOrArray(rawPrices);

  if (clobTokenIds.length > 0) {
    for (let i = 0; i < clobTokenIds.length; i++) {
      tokens.push({
        tokenId: clobTokenIds[i],
        outcome: parsedOutcomes[i] || `outcome_${i}`,
        price: parsedPrices[i] ? parseFloat(parsedPrices[i]) : 0,
      });
    }
  }

  // Parse start/end times from event or market level
  const startTime =
    (event.startTime as string) ||
    (market.eventStartTime as string) ||
    (market.startDate as string) ||
    "";
  const endTime =
    (event.endDate as string) ||
    (market.endDate as string) ||
    "";

  return {
    conditionId: (market.conditionId as string) || "",
    questionId: (market.questionID as string) || (market.questionId as string) || "",
    slug: (event.slug as string) || "",
    question: (market.question as string) || (event.title as string) || "",
    startTime: startTime ? new Date(startTime).getTime() : 0,
    endTime: endTime ? new Date(endTime).getTime() : 0,
    tokens,
    active: (market.active as boolean) ?? (event.active as boolean) ?? true,
    closed: (market.closed as boolean) ?? (event.closed as boolean) ?? false,
    acceptingOrders: (market.acceptingOrders as boolean) ?? false,
  };
}

/**
 * Fetch a specific BTC 5-min market by its window slug.
 */
export async function fetchMarketBySlug(
  slug: string
): Promise<PolymarketMarket | null> {
  try {
    const data = (await fetchJson(
      `${GAMMA_API}/events?slug=${slug}`
    )) as Array<Record<string, unknown>>;

    if (!Array.isArray(data) || data.length === 0) return null;

    return parseEvent(data[0]);
  } catch (err) {
    console.error(`Failed to fetch market ${slug}:`, (err as Error).message);
    return null;
  }
}

/**
 * Fetch the current and next BTC 5-minute markets.
 *
 * Uses deterministic slug computation - no search required.
 */
export async function discoverBtc5MinMarkets(): Promise<PolymarketMarket[]> {
  const now = Date.now();
  const currentBounds = computeWindowBounds(now);

  // Fetch current window and next window in parallel
  const currentSlug = `btc-updown-5m-${currentBounds.startUnix}`;
  const nextSlug = `btc-updown-5m-${currentBounds.startUnix + WINDOW_DURATION_S}`;

  const [current, next] = await Promise.all([
    fetchMarketBySlug(currentSlug),
    fetchMarketBySlug(nextSlug),
  ]);

  const markets: PolymarketMarket[] = [];
  if (current) markets.push(current);
  if (next) markets.push(next);

  return markets;
}

/**
 * Find the market for the current 5-minute window.
 */
export async function findCurrentWindow(): Promise<PolymarketMarket | null> {
  const now = Date.now();
  const bounds = computeWindowBounds(now);
  const slug = `btc-updown-5m-${bounds.startUnix}`;

  const market = await fetchMarketBySlug(slug);
  if (market && !market.closed) return market;

  // If current is closed, try next
  const nextSlug = `btc-updown-5m-${bounds.startUnix + WINDOW_DURATION_S}`;
  return fetchMarketBySlug(nextSlug);
}

/**
 * Find the UP and DOWN token IDs for a market.
 */
export function getTokenIds(market: PolymarketMarket): {
  upTokenId: string | null;
  downTokenId: string | null;
} {
  const upToken = market.tokens.find(
    (t) => t.outcome.toLowerCase() === "up"
  );
  const downToken = market.tokens.find(
    (t) => t.outcome.toLowerCase() === "down"
  );

  return {
    upTokenId: upToken?.tokenId || null,
    downTokenId: downToken?.tokenId || null,
  };
}
