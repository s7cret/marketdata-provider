# MarketData Provider

MarketData Provider is the exchange data layer for the Pine stack. It normalizes OHLCV candles, symbols, timeframes, cache metadata, archive policy, and optional streams across exchange adapters.

The package is intentionally independent from Pine2AST, AST2Python, PineLib, Backtest Engine, Optimizer, and OpenPine. Callers consume it through explicit provider and storage APIs.

## Features

- Binance and Bybit adapter foundations.
- Normalized candle models and timeframe validation.
- REST pagination helpers for historical candles.
- Optional local cache/archive support.
- Optional WebSocket streaming support via the `stream` extra.
- Optional Parquet export support via the `parquet` extra.
- CLI entry point: `marketdata`.

## Requirements

- Python 3.10 or newer.
- Runtime dependency: `httpx>=0.25`.
- Optional extras:
  - `parquet`: `pyarrow>=14`
  - `stream`: `websockets>=12`
  - `zstd`: `zstandard>=0.22`

## Install

Verbose installer:

```bash
./scripts/install.sh --dev
```

Manual install:

```bash
python -m pip install -e .
python -m pip install -e ".[dev,stream,parquet,zstd]"
```

## CLI

```bash
marketdata --help
```

The exact commands are adapter-dependent; use `--help` for current offline/online fetch, cache, and diagnostic options.

## Docker Compose

```bash
docker compose run --rm marketdata-provider
```

The default container command runs the test suite. For interactive CLI use:

```bash
docker compose run --rm marketdata-provider marketdata --help
```

## Development Gates

```bash
python -m pytest
python -m ruff check .
python -m mypy marketdata_provider
```

## GitHub Publication

See `docs/GITHUB_PUBLICATION.md`.

## License

MIT. See `LICENSE`.

## Support / Donations

OpenPine development is independent and MIT-licensed. Donations are optional and help keep the public tooling maintained.

- Telegram: https://t.me/OpenPine
- TON: `UQAyIr2sQ4-_Q5L-4VINcU18khDas5GPbAlYEkQN6S_qzui2`
- SOL: `EbxMUK2W4RGeQZCTRFrdgpEJvnqtyczPZvBrQa1cYJnQ`

Support does not affect license terms, feature access, or project guarantees.
