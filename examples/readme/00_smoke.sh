#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

set +e
source ./activate >/dev/null 2>&1
ACTIVATE_STATUS=$?
set -e
if [ "$ACTIVATE_STATUS" -ne 0 ]; then
    exit "$ACTIVATE_STATUS"
fi

tapdb --help >/dev/null
tapdb version >/dev/null

python - <<'PY'
from daylily_tapdb import __version__

assert __version__
print(f"daylily-tapdb {__version__}")
PY

printf 'Smoke example completed.\n'
