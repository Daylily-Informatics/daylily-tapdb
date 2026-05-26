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
OWNER_REPO_NAME="${TAPDB_DOCS_OWNER_REPO_NAME:-daylily-tapdb}"
DOMAIN_CODE="${TAPDB_DOCS_DOMAIN_CODE:-Z}"
PHYSICAL_DATABASE="${TAPDB_DOCS_PHYSICAL_DATABASE:-tapdb_docs_demo}"
SCHEMA_NAME="${TAPDB_DOCS_SCHEMA_NAME:-tapdb_docs_demo}"
DB_PORT="${TAPDB_DOCS_DB_PORT:-15533}"
UI_PORT="${TAPDB_DOCS_UI_PORT:-18911}"
CONFIG_PATH="${TAPDB_DOCS_CONFIG:-$WORKDIR/.config/tapdb/$CLIENT_ID/$DATABASE_NAME/tapdb-config.yaml}"
DOMAIN_REGISTRY_PATH="${TAPDB_DOCS_DOMAIN_REGISTRY_PATH:-$WORKDIR/.config/tapdb/domain_code_registry.json}"
PREFIX_OWNERSHIP_REGISTRY_PATH="${TAPDB_DOCS_PREFIX_OWNERSHIP_REGISTRY_PATH:-$WORKDIR/.config/tapdb/prefix_ownership_registry.json}"

mkdir -p "$(dirname "$CONFIG_PATH")" "$(dirname "$DOMAIN_REGISTRY_PATH")" "$(dirname "$PREFIX_OWNERSHIP_REGISTRY_PATH")"

export TAPDB_DOCS_DOMAIN_CODE_EFFECTIVE="$DOMAIN_CODE"
export TAPDB_DOCS_OWNER_REPO_NAME_EFFECTIVE="$OWNER_REPO_NAME"
export TAPDB_DOCS_DOMAIN_REGISTRY_PATH_EFFECTIVE="$DOMAIN_REGISTRY_PATH"
export TAPDB_DOCS_PREFIX_OWNERSHIP_REGISTRY_PATH_EFFECTIVE="$PREFIX_OWNERSHIP_REGISTRY_PATH"

python - <<'PY'
import json
import os
from pathlib import Path

domain_code = os.environ["TAPDB_DOCS_DOMAIN_CODE_EFFECTIVE"]
owner_repo_name = os.environ["TAPDB_DOCS_OWNER_REPO_NAME_EFFECTIVE"]
domain_registry_path = Path(os.environ["TAPDB_DOCS_DOMAIN_REGISTRY_PATH_EFFECTIVE"])
prefix_registry_path = Path(
    os.environ["TAPDB_DOCS_PREFIX_OWNERSHIP_REGISTRY_PATH_EFFECTIVE"]
)

domain_registry_path.write_text(
    json.dumps(
        {
            "version": "0.4.0",
            "domains": {domain_code: {"name": "tapdb-readme-local"}},
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
prefix_registry_path.write_text(
    json.dumps(
        {
            "version": "0.4.0",
            "ownership": {
                domain_code: {
                    "TPX": {"issuer_app_code": owner_repo_name},
                    "EDG": {"issuer_app_code": owner_repo_name},
                    "ADT": {"issuer_app_code": owner_repo_name},
                    "SYS": {"issuer_app_code": owner_repo_name},
                    "MSG": {"issuer_app_code": owner_repo_name},
                }
            },
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY

tapdb --config "$CONFIG_PATH" config init \
    --client-id "$CLIENT_ID" \
    --database-name "$DATABASE_NAME" \
    --owner-repo-name "$OWNER_REPO_NAME" \
    --domain-code "$DOMAIN_CODE" \
    --domain-registry-path "$DOMAIN_REGISTRY_PATH" \
    --prefix-ownership-registry-path "$PREFIX_OWNERSHIP_REGISTRY_PATH" \
    --engine-type local \
    --host localhost \
    --port "$DB_PORT" \
    --ui-port "$UI_PORT" \
    --user tapdb \
    --database "$PHYSICAL_DATABASE" \
    --schema-name "$SCHEMA_NAME" \
    --force

tapdb --config "$CONFIG_PATH" bootstrap local --no-gui

printf '\nBootstrap example completed.\n'
printf 'Config: %s\n' "$CONFIG_PATH"
printf 'Runtime root: %s\n' "$(dirname "$CONFIG_PATH")/runtime"
printf 'Next: TAPDB_DOCS_WORKDIR=%s python examples/readme/20_python_api.py\n' "$WORKDIR"
