# MarketData Provider 4.0.2

> Normalized exchange market-data contracts, cache/storage workflows, archive adapters, and streaming helpers for OpenPine.

[![Version](https://img.shields.io/badge/version-4.0.2-blue)](https://github.com/s7cret/marketdata-provider) [![Python](https://img.shields.io/badge/python-%3E%3D3.10-blue)](https://github.com/s7cret/marketdata-provider) [![License](https://img.shields.io/badge/license-MIT-green)](https://github.com/s7cret/marketdata-provider)


**GitHub description:** MarketData Provider normalizes OHLCV, footprint, cache, archive, and live-streaming data for OpenPine, with native Binance/Bybit adapters and explicit exchange capability metadata.

**Suggested topics:** `market-data`, `crypto`, `binance`, `bybit`, `ohlcv`, `candles`, `backtesting`, `algorithmic-trading`, `python`, `openpine`.

## What MarketData Provider is

MarketData Provider is the data boundary of the OpenPine stack. It defines canonical instrument, timeframe, bar, series, footprint, store, and provider contracts, then wraps offline files, cache stores, archive sources, native exchange adapters, and streaming clients behind those contracts.

```text
exchange/archive/offline data -> marketdata-provider -> normalized bars/streams -> backtest-engine/openpine
```

Default tests are hermetic and do not require live external services.

## Main capabilities

- Canonical `InstrumentKey`, `Timeframe`, `BarQuery`, `BarSeries`, and coverage contracts.
- Normalized OHLCV bars from offline, cache, archive, REST, and streaming sources.
- SQLite/segment-based candle, raw, footprint, current, checkpoint, and repair stores.
- Binance and Bybit native adapters in the 4.0.2 line.
- Exchange capability registry for native/planned adapter discovery.
- Footprint aggregation foundations for trade-derived volume-at-price data.
- Durable reconciliation and repair logs for cache health workflows.
- CLI utilities for validation, fetch, export, coverage, streams, compaction, prehistory, and exchange discovery.

## Exchange support

| Exchange | 4.0.2 status | Native market types |
|---|---|---|
| Binance | Native adapter | `spot`, `usdm` |
| Bybit | Native adapter | `spot`, `linear` |
| OKX, Coinbase Exchange, Kraken, KuCoin, Bitget, Gate.io, HTX/Huobi, MEXC | Planned metadata only | Listed for roadmap/discovery, not live fetch. |

Adding an exchange to the registry does not automatically enable live fetching. A native adapter requires symbol normalization, pagination, response normalization, error/rate-limit tests, archive/cache coverage, CLI smoke checks, and documentation updates.

## Boundaries

MarketData Provider does not parse Pine, execute strategies, simulate broker fills, optimize parameters, or guarantee external data completeness. It provides data contracts and adapters. Downstream systems must still validate gaps, sessions, fees, slippage, and execution assumptions.

## Install

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Install from GitHub tag:

```bash
python -m pip install 'git+https://github.com/s7cret/marketdata-provider.git@v4.0.2'
```

Optional extras:

```bash
python -m pip install -e '.[parquet]'
python -m pip install -e '.[stream]'
python -m pip install -e '.[zstd]'
```

## Python quick start

```python
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe

query = BarQuery(
    instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
    timeframe=parse_timeframe("1m"),
    start_ms=1704067200000,
    end_ms=1704070800000,
)

print(query.instrument.symbol)
print(query.timeframe.canonical)
```

Offline provider setup:

```python
from marketdata_provider import MarketDataConfig, OfflineDataConfig, create_provider

provider = create_provider(MarketDataConfig(offline=OfflineDataConfig(root="./data")))
series = provider.get_bars(query)
print(len(series.bars))
```

## CLI quick start

```bash
marketdata validate --path candles.csv --symbol BTCUSDT --timeframe 1m --exchange binance --market spot
marketdata fetch --symbol BTCUSDT --timeframe 1m --start 1704067200000 --end 1704070800000 --exchange binance --market spot
marketdata export --path candles.csv --symbol BTCUSDT --timeframe 1m --output candles.normalized.csv --format csv
marketdata coverage --path candles.csv --symbol BTCUSDT --timeframe 1m
```

Exchange discovery:

```bash
marketdata exchanges
marketdata exchanges --native-only
marketdata exchanges --exchange binance --format table
marketdata market-types
marketdata market-types --exchange bybit --format table
```

Storage and maintenance helpers:

```bash
marketdata current --store-dir .marketdata-store --symbol BTCUSDT --timeframe 1m --exchange binance --market spot
marketdata checkpoints --store-dir .marketdata-store
marketdata stream --store-dir .marketdata-store --symbol BTCUSDT --timeframe 1m --exchange binance --market spot
marketdata compact --store-dir .marketdata-store --symbol BTCUSDT --timeframe 1m --exchange binance --market spot --format csv
marketdata repair-logs --store-dir .marketdata-store
marketdata raw-inspect --raw-dir .marketdata-raw
```

## Repository layout

```text
marketdata_provider/
  contracts/              canonical bar/query/series/footprint contracts
  core/                   bar model, protocols, timeframe helpers, validation
  exchanges/              Binance/Bybit adapters and exchange registry
  providers/              offline provider and provider boundaries
  store/                  candle/current/footprint/raw/segment stores
  streaming/              kline/live streaming helpers and supervisor
  footprint/              trade-to-footprint aggregation
  cache/                  local cache helpers
  transport/              async HTTP transport helpers
  cli/                    operator and automation CLI
```

## Release checks

```bash
python -m compileall -q marketdata_provider tests
python -m ruff check marketdata_provider tests scripts --select F,E9
python -m mypy marketdata_provider
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -m "not live_network" -p pytest_cov tests --cov=marketdata_provider --cov-report=term
python -m marketdata_provider.quality duplicates marketdata_provider
python -m marketdata_provider.quality architecture marketdata_provider --max-lines 700
python -m marketdata_provider.distribution manifest --root .
python -m marketdata_provider.release --root .
python -m marketdata_provider exchanges --native-only
python -m marketdata_provider market-types --exchange binance
bash scripts/smoke_import_parse.sh
```

## Documentation

- `docs/ARCHITECTURE.md` — package boundary, hardening layout, exchange registry boundary.
- `docs/EXCHANGES.md` — native adapters, top-10 exchange roadmap, canonical market types.
- `docs/DEVELOPMENT.md` — local checks and exchange-registry smoke tests.
- `docs/RELEASE_4_0.md` — 4.0.2 release gate and hardening notes.

## License

MIT. See `LICENSE`.

## Support

OpenPine development is independent and MIT-licensed. Support is optional and does not change license terms, feature access, or project guarantees.

- Telegram: https://t.me/OpenPine
- TON: `UQAyIr2sQ4-_Q5L-4VINcU18khDas5GPbAlYEkQN6S_qzui2`
- SOL: `EbxMUK2W4RGeQZCTRFrdgpEJvnqtyczPZvBrQa1cYJnQ`