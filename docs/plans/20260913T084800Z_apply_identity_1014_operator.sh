#!/usr/bin/env bash
set -euo pipefail
umask 077
IDENTITY_ROOT=/home/ubuntu/bloom_ops/tapdb101-20260911
IDENTITY_TAPDB="$IDENTITY_ROOT/venv/bin/tapdb"
IDENTITY_PYTHON="$IDENTITY_ROOT/venv/bin/python"
IDENTITY_OPERATOR="$IDENTITY_ROOT/operator.yaml"
IDENTITY_RECEIPTS="$IDENTITY_ROOT/receipts"
IDENTITY_PLAN="$IDENTITY_RECEIPTS/schema-identity-1014-plan.json"
IDENTITY_RESULT="$IDENTITY_RECEIPTS/schema-identity-1014-result.json"
IDENTITY_GRANT="$IDENTITY_RECEIPTS/ursa-owner-3364-identity-access-plan.json"
IDENTITY_LOG="$IDENTITY_RECEIPTS/identity-1014-operator-20260913.log"
IDENTITY_WHEEL=/home/ubuntu/tapdb-identity-release-10.1.4/dist/daylily_tapdb-10.1.4-py3-none-any.whl
if [[ -e "$IDENTITY_LOG" ]]; then
  printf 'exit_code=2 existing_log=%s\n' "$IDENTITY_LOG"
  exit 2
fi
exec 3>&1
exec >"$IDENTITY_LOG" 2>&1
trap 'rc=$?; printf "exit_code=%s log=%s migration_plan=%s migration_result=%s identity_plan=%s\n" "$rc" "$IDENTITY_LOG" "$IDENTITY_PLAN" "$IDENTITY_RESULT" "$IDENTITY_GRANT" >&3' EXIT
"$IDENTITY_PYTHON" -m pip install --no-deps "$IDENTITY_WHEEL"
sudo "$IDENTITY_TAPDB" --config "$IDENTITY_OPERATOR" --client-id bloom --database-name bloom-day \
  db schema migrate --dry-run --receipt "$IDENTITY_PLAN" \
  --sequence-mappings "$IDENTITY_RECEIPTS/source-sequence-mappings.json" \
  --recovery-family "$IDENTITY_RECEIPTS/recovery-family.json" --receipts-dir "$IDENTITY_ROOT/journal"
sudo "$IDENTITY_PYTHON" - "$IDENTITY_PLAN" <<'PY'
import json,sys
plan=json.load(open(sys.argv[1]))
assert [row["filename"] for row in plan["pending_migrations"]] == ["20260913_082300_runtime_identity_authorization.sql"], "Unexpected migration set; stop"
assert plan["target"]["database"] == "tapdb_bloom_prod", "Unexpected database; stop"
assert plan["target"]["schema_name"] == "tapdb_bloom_lsmcok1_local", "Unexpected schema; stop"
PY
sudo "$IDENTITY_TAPDB" --config "$IDENTITY_OPERATOR" --client-id bloom --database-name bloom-day \
  db schema migrate --apply --preflight-receipt "$IDENTITY_PLAN" --receipt "$IDENTITY_RESULT" \
  --sequence-mappings "$IDENTITY_RECEIPTS/source-sequence-mappings.json" \
  --recovery-family "$IDENTITY_RECEIPTS/recovery-family.json" --receipts-dir "$IDENTITY_ROOT/journal" \
  --establish-writer-fence --control-config "$IDENTITY_ROOT/control-operator.yaml" \
  --provider-contract "$IDENTITY_RECEIPTS/aurora-provider-contract.json"
sudo "$IDENTITY_TAPDB" --config "$IDENTITY_OPERATOR" --client-id bloom --database-name bloom-day \
  db runtime-principal identity-access --user-uid 3364 --user-euid M-SYS-9SX2 --grant \
  --reason 'Restore authorization lookup for the existing Ursa token owner' --receipt "$IDENTITY_GRANT"
sudo "$IDENTITY_PYTHON" - "$IDENTITY_GRANT" <<'PY'
import json,sys
plan=json.load(open(sys.argv[1]))
assert plan["operation"] == "identity-access" and plan["status"] == "planned" and plan["enabled"] is True
target=plan["target"]
assert target["database"] == "tapdb_bloom_prod" and target["schema_name"] == "tapdb_bloom_lsmcok1_local"
assert target["user"] == "bloom_runtime_10" and target["owner_repo_name"] == "bloom" and target["domain_code"] == "M"
identity=plan["identity"]
assert identity["uid"] == 3364 and identity["euid"] == "M-SYS-9SX2"
assert identity["issuer_app_code"] == "daylily-tapdb" and identity["tenant_id"] is None
assert identity["is_active"] is True and identity["role"] == "ADMIN"
PY
sudo "$IDENTITY_TAPDB" --config "$IDENTITY_OPERATOR" --client-id bloom --database-name bloom-day \
  db runtime-principal identity-access --user-uid 3364 --user-euid M-SYS-9SX2 --grant \
  --reason 'Restore authorization lookup for the existing Ursa token owner' --receipt "$IDENTITY_GRANT" --apply
