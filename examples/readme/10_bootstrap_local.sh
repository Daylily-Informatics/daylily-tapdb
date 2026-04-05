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

for tool in initdb pg_ctl createdb psql pg_isready; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        printf 'Missing required PostgreSQL tool: %s\n' "$tool" >&2
        printf 'Install PostgreSQL binaries and ensure they are on PATH.\n' >&2
        exit 2
    fi
done

WORKDIR="${TAPDB_DOCS_WORKDIR:-$HOME/.tapdb-docs}"
CLIENT_ID="${TAPDB_DOCS_CLIENT_ID:-docs}"
DATABASE_NAME="${TAPDB_DOCS_DATABASE_NAME:-demo}"
EUID_CLIENT_CODE="${TAPDB_DOCS_EUID_CLIENT_CODE:-C}"
DB_PORT="${TAPDB_DOCS_DB_PORT:-15533}"
UI_PORT="${TAPDB_DOCS_UI_PORT:-18911}"
CONFIG_PATH="${TAPDB_DOCS_CONFIG:-$WORKDIR/.config/tapdb/$CLIENT_ID/$DATABASE_NAME/tapdb-config.yaml}"

mkdir -p "$(dirname "$CONFIG_PATH")"

tapdb --config "$CONFIG_PATH" config init \
    --client-id "$CLIENT_ID" \
    --database-name "$DATABASE_NAME" \
    --euid-client-code "$EUID_CLIENT_CODE" \
    --env dev \
    --db-port "dev=$DB_PORT" \
    --ui-port "dev=$UI_PORT" \
    --force

tapdb --config "$CONFIG_PATH" --env dev bootstrap local --no-gui

printf '\nBootstrap example completed.\n'
printf 'Config: %s\n' "$CONFIG_PATH"
printf 'Runtime root: %s\n' "$(dirname "$CONFIG_PATH")/dev"
printf 'Next: TAPDB_DOCS_WORKDIR=%s python examples/readme/20_python_api.py\n' "$WORKDIR"
