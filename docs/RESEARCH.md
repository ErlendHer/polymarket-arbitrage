# Research Findings

## Oracle & Resolution

**Oracle:** Chainlink Data Streams
**Feed:** `BTC/USD-RefPrice-DS-Premium-Global-003`
**Feed ID:** `0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8`
**Node Operators:** 16 (Chainlink Labs, Galaxy, DexTrac, Fiews, Inotel, etc.)

**Resolution Rule:** `close_price >= open_price` = UP wins. Flat = UP.

**Settlement:** Polygon blockchain, ~2 minute delay (64-block confirmation requirement).

**Price Feed Access (fastest to slowest):**
1. Raw exchange websockets (Binance/Coinbase) - leading indicator, NOT the resolution source
2. Chainlink Data Streams WS (`wss://ws.dataengine.chain.link`) - the actual source, sub-second, requires API key
3. Polymarket RTDS WS (`wss://ws-live-data.polymarket.com`) - relayed through Polymarket, adds latency

**Chainlink API Access:** Apply through Polymarket's sponsored program or directly from Chainlink (paid).

## Polymarket Infrastructure

**CLOB Servers:** AWS eu-west-2 (London), secondary eu-west-1 (Ireland)
**API Base:** `https://clob.polymarket.com`
**Orderbook WS:** `wss://ws-subscriptions-clob.polymarket.com/ws/market`
**User WS:** `wss://ws-subscriptions-clob.polymarket.com/ws/user` (authenticated)
**Market Discovery:** `https://gamma-api.polymarket.com`

**Authentication:** Dual-layer
- L1: EIP-712 signing (wallet-based, generates API credentials)
- L2: HMAC-SHA256 (for trading operations)

**Rate Limits (key):**
- `POST /order`: 3,500 req/10s burst
- `DELETE /order`: 3,000 req/10s burst
- `GET /book`: 1,500 req/10s

**SDKs:** TypeScript (`@polymarket/clob-client`), Python (`py-clob-client`), Rust (`rs-clob-client`)

**Settlement:** Polygon (Chain ID 137), collateral is USDC.e. Gas is zero (relayer-sponsored).

## Fee Structure (Critical for Strategy)

**Taker Fees on Crypto Markets:**
```
fee = C * p * feeRate * (p * (1 - p))^exponent
```
- feeRate = 0.25, exponent = 2
- Maximum at 50/50 odds: ~1.56%
- Decreases toward price extremes (near 0 or 1)

**Maker Fees:** 0% + 20% rebate on taker fees collected

**No gas costs.** Polymarket relayer sponsors all transactions.

**Dynamic fees were introduced specifically to counter latency arbitrage.**

## Strategy Implications

### Why Maker > Taker
| Factor | Taker | Maker |
|--------|-------|-------|
| Fee | ~1.56% at 50/50 | 0% + rebates |
| Required edge | >1.56% | >0% |
| Competitive advantage | Speed | Better information |

### The Edge
User hypothesis: market overindexes on early price movements (psychological bias).
If BTC is up 0.05% after 30s, market might price UP at 58% when statistical reality is ~53%.
This 5pp gap exceeds the 0% maker fee but may not exceed the 1.56% taker fee.

### Volatility Conditioning
Same price movement has different implications depending on market volatility:
- Low vol + 0.02% move = strong signal (unusual move)
- High vol + 0.02% move = noise (within expected range)

## Historical Data

**Source:** Binance bulk 1s klines from `data.binance.vision`
- Pair: BTCUSDT (most liquid)
- 1s klines available from mid-2023
- ~86,400 rows per day, ~63M rows for 2 years
- Free, no API key needed for bulk download

**Data Quality:** Generally excellent. Known gaps during Binance maintenance (~2-5 outages/year).

**Important:** Resolution is based on Chainlink, not Binance. Small discrepancies possible.
We use Binance data for statistical analysis because it's freely available at 1s resolution
and closely tracks what Chainlink reports.

## Hosting Recommendation

**Primary:** AWS eu-west-2 (London) - co-located with Polymarket CLOB
- Instance: c7g.medium (~$25/mo) or Hetzner Ashburn ($40-80/mo)
- Latency to CLOB: 1-5ms (same region)
- Latency to Binance WS: 60-80ms (acceptable for price feed)

## Tech Stack Decision

**Analysis Pipeline:** Python 3.12+ / DuckDB / Parquet / pandas / numpy / matplotlib
**Trading Bot:** TypeScript (official SDK) or Rust (official SDK)
**Network latency (10-50ms) >> compute latency (0.1-1ms).** Language choice is secondary.
