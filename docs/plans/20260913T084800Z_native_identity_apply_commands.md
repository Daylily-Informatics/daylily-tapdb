# Lead execution commands — Bloom identity repair

Prepared from the existing Bloom maintenance SOP and native TapDB command definitions. These commands have not been executed by this agent. Lead reported TapDB 10.1.4 tag commit `0d8efff0` published and its final EC2 wheel building. Use the existing operator venv after that exact package is installed. Keep the lead's interactive SSM session exclusive to the lead.

## Existing context and new receipt paths

Run as the existing ubuntu operator; use targeted sudo only where the existing protected registry/config permissions require it. Native commands keep credentials in the existing references. Do not create new credentials or change runtime config.

```bash
IDENTITY_ROOT=/home/ubuntu/bloom_ops/tapdb101-20260911
IDENTITY_TAPDB="$IDENTITY_ROOT/venv/bin/tapdb"
IDENTITY_OPERATOR="$IDENTITY_ROOT/operator.yaml"
IDENTITY_RECEIPTS="$IDENTITY_ROOT/receipts"
IDENTITY_PLAN="$IDENTITY_RECEIPTS/schema-identity-1014-plan.json"
IDENTITY_RESULT="$IDENTITY_RECEIPTS/schema-identity-1014-result.json"
IDENTITY_GRANT="$IDENTITY_RECEIPTS/ursa-owner-3364-identity-access-plan.json"
```

These receipt paths are create-exclusive. If one exists, inspect its existing result rather than overwrite or replay it. The mappings, recovery family, journal, control config and provider contract below are the actual retained paths from the established maintenance workflow. Do not recapture or invent a recovery family or pass the obsolete pre-migration frozen source contract.

## Native migration plan and apply

The plan is the required native mutation receipt, not a test campaign. It must select only `20260913_082300_runtime_identity_authorization.sql`. No prior 10.1.x migration may be replayed. Omit global `--json` for the migrate command.

```bash
"$IDENTITY_TAPDB" --config "$IDENTITY_OPERATOR" --client-id bloom --database-name bloom-day \
  db schema migrate --dry-run \
  --receipt "$IDENTITY_PLAN" \
  --sequence-mappings "$IDENTITY_RECEIPTS/source-sequence-mappings.json" \
  --recovery-family "$IDENTITY_RECEIPTS/recovery-family.json" \
  --receipts-dir "$IDENTITY_ROOT/journal"
```

Lead closes Bloom admission/writers through the established service workflow before apply. Native fencing remains required and is scoped to this database. Apply consumes the unchanged plan and releases the writer fence through the supported workflow; an ambiguous result is reconciled from its receipt/journal, never blindly retried.

```bash
"$IDENTITY_TAPDB" --config "$IDENTITY_OPERATOR" --client-id bloom --database-name bloom-day \
  db schema migrate --apply \
  --preflight-receipt "$IDENTITY_PLAN" --receipt "$IDENTITY_RESULT" \
  --sequence-mappings "$IDENTITY_RECEIPTS/source-sequence-mappings.json" \
  --recovery-family "$IDENTITY_RECEIPTS/recovery-family.json" \
  --receipts-dir "$IDENTITY_ROOT/journal" \
  --establish-writer-fence \
  --control-config "$IDENTITY_ROOT/control-operator.yaml" \
  --provider-contract "$IDENTITY_RECEIPTS/aurora-provider-contract.json"
```

The new additive asset creates only its private authorization table, operator policy and minimal function. It does not run historical audit UPDATE or reapply ordinary object RLS. Existing application issuer/tenant binding remains intact.

## Exact existing-owner grant

Only after the migration is terminal, plan the selected runtime grant. Its receipt must identify existing `bloom_runtime_10`, database `tapdb_bloom_prod`, schema `tapdb_bloom_lsmcok1_local`, domain `M`, owner scope `bloom`, UID `3364`, EUID `M-SYS-9SX2`, issuer `daylily-tapdb`, and current active canonical owner/template. The command validates these through the native operator/catalog contract, without exposing auth properties or credentials.

```bash
"$IDENTITY_TAPDB" --config "$IDENTITY_OPERATOR" --client-id bloom --database-name bloom-day \
  db runtime-principal identity-access \
  --user-uid 3364 --user-euid M-SYS-9SX2 --grant \
  --reason 'Restore authorization lookup for the existing Ursa token owner' \
  --receipt "$IDENTITY_GRANT"
```

Apply the exact reviewed grant:

```bash
"$IDENTITY_TAPDB" --config "$IDENTITY_OPERATOR" --client-id bloom --database-name bloom-day \
  db runtime-principal identity-access \
  --user-uid 3364 --user-euid M-SYS-9SX2 --grant \
  --reason 'Restore authorization lookup for the existing Ursa token owner' \
  --receipt "$IDENTITY_GRANT" --apply
```

The result is `$IDENTITY_RECEIPTS/ursa-owner-3364-identity-access-plan.result.json`. This operation grants only the exact identity projection and function EXECUTE. It does not replace the token, change its owner/scope, change the user role, or expose the global user row through normal object queries. A general runtime rebind is not needed for this additive operation.

## Bloom adoption and one real request

Lead adopts the final Bloom release with exact TapDB 10.1.4 dependency and real frozen lock, using the final EC2-built image. After deployment/admission resumes, repeat the already selected authenticated Ursa sequencing search `ONT` once with the existing token/session. Retain the actual response. Expected authorization uses original owner 3364 and internal_ro, with no write/admin permissions. No broader test campaign or new token is required. A failure remains an explicit blocker; do not fabricate success or bypass the native identity guard to meet the deadline.
