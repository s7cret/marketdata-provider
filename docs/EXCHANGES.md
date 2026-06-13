# Exchange registry

Version 4.0.0 ships a dependency-free exchange capability registry used by the CLI, tests, and future adapter planning.

The registry separates two concepts:

- **Native adapter** — this package can fetch normalized bars today through tested code paths.
- **Planned metadata** — the exchange is included in the top-10 data-source roadmap, but live fetching is intentionally not enabled until a dedicated adapter and tests land.

## CLI

```bash
marketdata exchanges
marketdata exchanges --native-only
marketdata exchanges --exchange binance --format table
marketdata market-types
marketdata market-types --exchange bybit --format table
```

All commands default to JSON output so automation can consume them directly. `--format table` is for operator/debug use.

## Native adapters in 4.0.0

| Exchange | Native markets | Recommended source |
|---|---|---|
| Binance | `spot`, `usdm` | Official bulk archive first for deep history; REST/WebSocket for recent/live gaps. |
| Bybit | `spot`, `linear` | REST/WebSocket for candles/live data; official history downloads where the dataset is available. |

## Top-10 exchange roadmap

| Rank | Exchange | Status | Market types | Archive posture |
|---:|---|---|---|---|
| 1 | Binance | native | spot, margin, USDT futures, coin futures, options | official bulk archive |
| 2 | Bybit | native | spot, USDT futures, coin futures, options | official partial downloads |
| 3 | OKX | planned | spot, margin, swaps, futures, options | API-first |
| 4 | Coinbase Exchange | planned | spot | API-first |
| 5 | Kraken | planned | spot, margin, futures | official partial downloads + REST recent windows |
| 6 | KuCoin | planned | spot, margin, futures | API-first |
| 7 | Bitget | planned | spot, USDT/coin futures | API-first with historical endpoint limits |
| 8 | Gate.io | planned | spot, margin, futures, options | API-first; third-party archive for full replay |
| 9 | HTX / Huobi | planned | spot, margin, futures | official partial/history pages + third-party archive for full replay |
| 10 | MEXC | planned | spot, futures | official partial spot history + API range pagination |

## Canonical market types

| ID | Meaning |
|---|---|
| `spot` | Immediate settlement spot candles/trades/order book. |
| `margin` | Spot-margin markets; public market data usually mirrors spot. |
| `usdt_futures` | Linear stablecoin-margined swaps/futures. |
| `coin_futures` | Inverse coin-margined swaps/futures. |
| `delivery_futures` | Dated futures with expiry/delivery. |
| `options` | Listed options market data. |

## Adapter policy

Adding a planned exchange to this registry does **not** enable live fetches automatically. A new native adapter must add:

1. symbol normalization tests;
2. REST pagination tests;
3. response normalization tests;
4. rate-limit/error tests;
5. archive/cache integration tests;
6. CLI smoke coverage;
7. documentation update that moves the exchange from `planned` to `native`.
