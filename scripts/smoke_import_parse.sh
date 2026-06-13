#!/usr/bin/env bash
set -euo pipefail
PYTHON="${PYTHON:-python}"
"$PYTHON" - <<'PY'
import marketdata_provider
print(marketdata_provider.__name__, getattr(marketdata_provider, "__version__", "unknown"))
PY
