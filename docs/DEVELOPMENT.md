# Development

Run the local release gate from a clean checkout:

```bash
bash scripts/release_gate.sh
```

Equivalent expanded checks:

```bash
python -m compileall -q marketdata_provider tests
python -m ruff check marketdata_provider tests scripts --select F,E9
python -m mypy marketdata_provider
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -p pytest_cov tests --cov=marketdata_provider --cov-report=term
python -m marketdata_provider.quality duplicates marketdata_provider
python -m marketdata_provider.quality architecture marketdata_provider --max-lines 700
python -m marketdata_provider.distribution manifest --root .
python -m marketdata_provider.release --root .
python -m marketdata_provider exchanges --native-only
python -m marketdata_provider market-types --exchange binance
bash scripts/smoke_import_parse.sh
```

The 4.0.0 hardening gate requires 100% package coverage and no Python module above 700 lines. Network/live-provider and sibling-repository checks are intentionally outside the default gate and should be run as OpenPine integration smoke tests.

## Exchange registry smoke

```bash
python -m marketdata_provider exchanges --native-only
python -m marketdata_provider market-types --exchange binance
```

These commands must stay hermetic and network-free.
