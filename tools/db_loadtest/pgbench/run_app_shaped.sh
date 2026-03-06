#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5533}"
DB_NAME="${DB_NAME:-tapdb_tapdb_dev}"
DB_USER="${DB_USER:-${USER:-postgres}}"
DB_PASSWORD="${DB_PASSWORD:-${PGPASSWORD:-}}"

CLIENTS="${CLIENTS:-16}"
JOBS="${JOBS:-4}"
DURATION="${DURATION:-120}"
PROGRESS_EVERY="${PROGRESS_EVERY:-5}"

TEMPLATE_UID="${TEMPLATE_UID:-}"
TENANT_COUNT="${TENANT_COUNT:-5}"
MIN_INSTANCE_UID="${MIN_INSTANCE_UID:-1}"
MAX_INSTANCE_UID="${MAX_INSTANCE_UID:-1000}"
MAX_DEPTH="${MAX_DEPTH:-4}"
LATEST_LIMIT="${LATEST_LIMIT:-50}"

if [[ -z "${TEMPLATE_UID}" ]]; then
  echo "ERROR: TEMPLATE_UID is required"
  echo "Run tools/db_loadtest/generate_synthetic_data.py and use emitted vars."
  exit 1
fi

if [[ -n "${DB_PASSWORD}" ]]; then
  export PGPASSWORD="${DB_PASSWORD}"
fi

if [[ -z "${PGOPTIONS:-}" ]]; then
  export PGOPTIONS="-c client_min_messages=warning"
fi

cmd=(
  pgbench
  -h "${DB_HOST}"
  -p "${DB_PORT}"
  -U "${DB_USER}"
  -n
  -c "${CLIENTS}"
  -j "${JOBS}"
  -T "${DURATION}"
  -P "${PROGRESS_EVERY}"
  -D "template_uid=${TEMPLATE_UID}"
  -D "tenant_count=${TENANT_COUNT}"
  -D "min_instance_uid=${MIN_INSTANCE_UID}"
  -D "max_instance_uid=${MAX_INSTANCE_UID}"
  -D "max_depth=${MAX_DEPTH}"
  -D "latest_limit=${LATEST_LIMIT}"
  -f "${SCRIPT_DIR}/read_graph_recursive.sql@50"
  -f "${SCRIPT_DIR}/list_latest_revisions.sql@20"
  -f "${SCRIPT_DIR}/insert_lineage_edge.sql@20"
  -f "${SCRIPT_DIR}/insert_instance.sql@10"
  "${DB_NAME}"
)

echo "Running app-shaped mixed workload with command:"
printf '  %q' "${cmd[@]}"
printf '\n'

"${cmd[@]}"
