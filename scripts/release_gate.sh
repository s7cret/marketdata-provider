#!/usr/bin/env bash
set -euo pipefail
PYTHON="${PYTHON:-python}"
"$PYTHON" -m compileall -q marketdata_provider tests
"$PYTHON" -m ruff check marketdata_provider tests scripts --select F,E9
"$PYTHON" -m mypy marketdata_provider
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 "$PYTHON" -m pytest -q -p pytest_cov tests --cov=marketdata_provider --cov-report=term
"$PYTHON" -m marketdata_provider.quality duplicates marketdata_provider
"$PYTHON" -m marketdata_provider.quality architecture marketdata_provider --max-lines 700
"$PYTHON" -m marketdata_provider.distribution manifest --root .
"$PYTHON" -m marketdata_provider.release --root .
"$PYTHON" -m marketdata_provider exchanges --native-only > /dev/null
"$PYTHON" -m marketdata_provider market-types --exchange binance > /dev/null
bash scripts/smoke_import_parse.sh
