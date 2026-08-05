# Release 4.0.0

`marketdata-provider` is prepared as part of the OpenPine 4.x package family.

## Scope

The package provides market-data contracts, REST/archive/live-provider adapters, cache and segment storage, and OpenPine-facing service helpers. External live services and sibling repositories are intentionally outside the default hermetic gate; run full OpenPine stack smoke tests before coordinated tags.

## Release gate

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

## Final hardening notes

- Coverage gate: 100%.
- Current measured coverage: 100.00%.
- Architecture budget: no Python module above 700 lines.
- Duplicate implementation groups: 0.
- Deterministic distribution builder excludes caches, build artifacts, coverage files, bytecode, and egg-info metadata.
- Added third-pass tests for cache integrity, Binance archive fallback, provider branches, contract adapters, timeframe contracts, service helpers, and distribution hygiene.
- Fixed deterministic distribution file discovery when the selected root is not the current working directory.

## Exchange registry CLI

The release includes network-free discovery commands for available native exchanges, planned top-10 exchange metadata, and canonical market types. Native fetching remains enabled only for tested adapters.
