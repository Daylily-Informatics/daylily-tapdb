# Root-run native 10.1.8 migration handoff

## Current continuation state

Command preparation only. Root reports operator TapDB 10.1.8 released at `78af162d917938a14927d009d4a1472f29ebb016`, annotated tag `3f4feefc2df8ce39a3a97e8db16e7f757d57bd0b`, wheel SHA256 `7f48fb183ff9e0787e9bede90ca8dba9935c26dc56241fb1f8edfaed5c64ac76`. All seven final application images are published; no migration or deployment is claimed here. Root installs the exact operator package into the existing isolated venv and alone executes production. Source/app/runtime remain 10.1.7 except already approved sibling versions; no new native source change is required for this handoff.

Use the public native CLI directly. It already retains both physical sessions for the complete fence/apply/recovery/release sequence; an extra apply wrapper would add no safety. Commands below are sequential stages with review gates, not one pasteable whole-window script. No command has been executed by this reviewer. No tests were run for this docs-only preparation.

## Exact targets and staged inputs

Operator root: `/home/ubuntu/atlas-globaldag-20260926-operator`, EC2 `i-07df3a933e4839f52`, account `108782052779`, region `us-west-2`, operator OS account `ubuntu`. Run in a retained interactive bash login session; no SSM fire-and-forget migration command.

| Service | Client / logical database | Existing physical database / schema | Source version | Operator evidence mode |
|---|---|---|---|---|
| Atlas | `atlas` / `lsmc-atlas-day` | `lsmc_atlas_prod` / `tapdb_atlas_lsmcok1_local` | `9.0.10` | Existing complete per-row format |
| Kahlo | `kahlo` / `kahlo-day` | `kahlo_prod` / `tapdb_kahlo_lsmcok1_local` | `9.0.9` | Explicit `audit_log_ordered_digest/v1` |
| Zebra | `zebra-day` / `zebra-day-day` | `zebra_day_prod` / `tapdb_zebra_day_lsmcok1_local` | `9.0.9` | Existing complete per-row format |

Existing private service paths: `$ROOT/<service>/operator.yaml`, `runtime-prepared.yaml`, `receipts/`, `journal/`. Operator configs use actual operator `dayhoff`; service runtime roles remain separate. All paths must resolve to the reviewed existing files/directories with private permissions. Never dump config or credential values to the terminal. Frozen source and recovery family bind the operator config path as their source identity; later runtime binding instead uses the canonical deployed runtime config path with only operator credential fields overlaid, as documented separately.

Kahlo operator YAML alone receives root fields `inventory_mode: audit_log_ordered_digest/v1` and `inventory_limits: {max_rows: 3000000, max_row_bytes: 8388608, max_receipt_bytes: 134217728}`. The package also caps streamed audit input at 16 GiB. Atlas/Zebra must have no compact mode field. These finite limits may fail closed; do not silently enlarge them or skip tables. Source version is an explicit declaration, independently checked against owning deployment evidence; it never waives content preservation.

Each service needs its own exact source mapping file, staged from the reviewed central `source-sequence-mappings-<service>.json`. Its absolute host path and SHA256 are root inputs. Do not infer a prefix from a sequence name. Existing sealed family and historical journal roots, if any, must be retained; inspect before choosing a new family UUID. No new family may hide an existing unresolved operation or allocator exposure.

## Physical evidence still required before apply

1. Verify installed native 10.1.8/wheel in the exact operator venv, host/OS identity and unchanged service physical targets. Read `db identity inventory --help` and `db schema migrate --help` from that installed CLI if desired; source registration confirms **`db identity`**, not top-level `identity`. Global `--json` is not supported by schema migrate.
2. Verify the existing `postgres` control database on the same authenticated writer, same exact host/port/TLS and `dayhoff` operator. Do not create a database. Prepare a private per-service `control-operator.yaml` with the matching service client/logical metadata and operator authority, but physical target database `postgres`; retain the exact reviewed schema/domain metadata and registry inputs. The CLI loads this within the service context, so unrelated control client/logical metadata would fail config validation. The public operator session does not initialize or apply schema in the control database.
3. Obtain current authenticated RDS metadata for `dayhoff-lsmcok1-tapdb`: engine/version, cluster ARN, cluster resource ID and writer endpoint. Verify the named AWS profile is present and usable as ubuntu on EC2 (historical Bloom used `lsmc`; that is precedent, not current proof). Native fence requires engine `aurora-postgresql`, version **16.13**, matching server_version_num **160013**, exact ARN/resource ID/endpoint, verify-full TLS/CA, and authenticated provider-role topology. If any differ, stop; do not fabricate a provider exception or substitute a reader endpoint.
4. Verify complete service admission/writer shutdown evidence: owning Compose container IDs/services, workers, timers/cron, other clients and active transactions/queues. Stop only the approved targets, preserving unrelated services. Keep restart controllers disabled until the cutover gate is satisfied. Require no stale target application sessions, prepared transactions or enabled subscriptions. The native gate blocks new database connections but **does not kill existing writers**; a stale session can cause refusal after the database gate closes.
5. Record exact service family descriptor, journal roots/history and absence of unresolved recovery/fence intents. Record the exact source mappings, frozen source receipt and preflight hashes. Source receipts captured while writers ran remain historical; final frozen source/preflight must represent the stopped state. Never overwrite earlier failures/successful receipts.

A native bounded read-only `db census --database <target> --database postgres --receipt <new absolute file>` captures principals, database ACLs/activity and control existence. It requires complete activity visibility for the writer conclusion. A catalog census is not proof of successful control login: open the actual control through public `operator_connection(read_only=True)` or the owning existing operator helper and compare its physical server/database identity. Native apply independently verifies these again. Census SQL contains no application row scan. Root's OS/queue inspection remains separate evidence.

## Provider contract

Stage one private immutable JSON file from the current owning RDS response. Exact required keys, with no invented values:

```json
{
  "schema_version": "tapdb-fence-provider/v1",
  "engine": "aurora-postgresql",
  "engine_version": "16.13",
  "aws_profile": "<verified named profile on EC2>",
  "region": "us-west-2",
  "cluster_identifier": "dayhoff-lsmcok1-tapdb",
  "cluster_arn": "<exact DBClusterArn from authenticated RDS>",
  "cluster_resource_id": "<exact DbClusterResourceId from authenticated RDS>",
  "writer_endpoint": "<exact Endpoint matching operator target host>",
  "sslmode": "verify-full"
}
```

Angle-bracket fields are unresolved evidence requirements, not executable values. Native reads all required fields, re-queries AWS and rejects any extra/missing field. Reuse one contract across targets only after proving all three point to this same writer; each migration retains its own native provider/fence receipts.

## Per-service commands

Run one reviewed service at a time. Set `SERVICE` explicitly to `atlas`, `kahlo` or `zebra-day`; do not loop automatically. The five external variables below must name already reviewed inputs/selected new outputs. `WINDOW` is a root-selected new receipt directory component, `MAPPINGS`/`PROVIDER` are exact existing absolute files, and `FAMILY_ID` is a root-selected canonical UUID only when no family already exists. Use existing family input in the alternate source command when present.

```bash
set -euo pipefail
umask 077
ROOT=/home/ubuntu/atlas-globaldag-20260926-operator
TAPDB="$ROOT/venv/bin/tapdb"
: "${SERVICE:?select one exact service}"
: "${WINDOW:?select one new private receipt directory name}"
: "${MAPPINGS:?exact existing reviewed sequence mapping file}"
: "${PROVIDER:?exact existing verified provider contract file}"
case "$SERVICE" in
  atlas) CLIENT=atlas; LOGICAL=lsmc-atlas-day; SOURCE_VERSION=9.0.10; DATABASE=lsmc_atlas_prod ;;
  kahlo) CLIENT=kahlo; LOGICAL=kahlo-day; SOURCE_VERSION=9.0.9; DATABASE=kahlo_prod ;;
  zebra-day) CLIENT=zebra-day; LOGICAL=zebra-day-day; SOURCE_VERSION=9.0.9; DATABASE=zebra_day_prod ;;
  *) return 2 ;;
esac
OPERATOR="$ROOT/$SERVICE/operator.yaml"
CONTROL="$ROOT/$SERVICE/control-operator.yaml"
JOURNAL="$ROOT/$SERVICE/journal"
OUT="$ROOT/$SERVICE/receipts/$WINDOW"
test -d "$JOURNAL"
test -f "$OPERATOR" && test -f "$CONTROL"
test -f "$MAPPINGS" && test -f "$PROVIDER"
mkdir -m 700 "$OUT"
```

`mkdir` must fail if the output directory already exists. Review `WINDOW` as a simple basename; do not accept slashes or traversal. Do not use `mkdir -p` to reuse an uncertain operation. All subsequent stdout/stderr/receipts stay private and create-exclusive; record each exit code and file hash. Avoid printing full source/preflight JSON to the terminal.

### 1. Frozen principal/session evidence

After root has stopped/drained the exact target and closed admission:

```bash
"$TAPDB" --config "$OPERATOR" --client-id "$CLIENT" --database-name "$LOGICAL" \
  db census --database "$DATABASE" --database postgres \
  --statement-timeout-ms 10000 --max-catalog-rows 100000 \
  --receipt "$OUT/frozen-principals.json" \
  > "$OUT/frozen-principals.stdout" 2> "$OUT/frozen-principals.stderr"
```

Review activity completeness and target sessions before proceeding. This is evidence, not a writer fence or schema mutation.

### 2. Complete frozen source and family

Only for the first family, after proving there is no existing family/history to retain:

```bash
: "${FAMILY_ID:?explicit reviewed canonical family UUID}"
FAMILY="$OUT/recovery-family.json"
"$TAPDB" --config "$OPERATOR" --client-id "$CLIENT" --database-name "$LOGICAL" \
  db identity inventory --source-version "$SOURCE_VERSION" \
  --sequence-mappings "$MAPPINGS" \
  --new-recovery-family-id "$FAMILY_ID" --family-receipts-dir "$JOURNAL" \
  --family-receipt "$FAMILY" --receipt "$OUT/frozen-source-contract.json" \
  > "$OUT/frozen-source.stdout" 2> "$OUT/frozen-source.stderr"
```

If an existing family was found, preserve its exact absolute `FAMILY` path and use `--recovery-family "$FAMILY"` instead of all three new-family options. Do not run both commands. The native source contract includes complete physical identities, every table's row count/content evidence and complete sequence inventory; family creation writes files only. Kahlo audit is compact but exhaustive, never excluded. Verify expected mode/target/version, rowcounts, digest/transformation counters, sequence mappings/floors and family. This is **source capture only**, not migration completion.

### 3. Native migration preflight

```bash
"$TAPDB" --config "$OPERATOR" --client-id "$CLIENT" --database-name "$LOGICAL" \
  db schema migrate --dry-run --receipt "$OUT/schema-preflight.json" \
  --source-contract "$OUT/frozen-source-contract.json" \
  --sequence-mappings "$MAPPINGS" --recovery-family "$FAMILY" \
  --receipts-dir "$JOURNAL" \
  > "$OUT/schema-preflight.stdout" 2> "$OUT/schema-preflight.stderr"
```

Review exact target/physical identity, expanded pending asset hashes, source equality, family and journal/floor state. Kahlo compact mode admits exactly the fourteen reviewed pending assets or none; initial Kahlo must have fourteen. Atlas/Zebra pending lists require their actual native receipt review, not a copy of Kahlo's list. Freeze the plan hash; any changed source/sequence/config/asset requires a newly reviewed plan while retaining old evidence. Native preservation does not silently correct object tenant scopes; Atlas's separately reviewed 96-source correction stays after the terminal schema receipt.

### 4. Native fenced apply — root gate, once

Run only after the full scoped-credential set, frozen writers, source/family/plan, control connection and provider contract are accepted. This command deliberately closes the target database connection gate; keep target and control sessions/process alive. No container/app restart until terminal evidence is reviewed.

```bash
"$TAPDB" --config "$OPERATOR" --client-id "$CLIENT" --database-name "$LOGICAL" \
  db schema migrate --apply --preflight-receipt "$OUT/schema-preflight.json" \
  --receipt "$OUT/schema-result.json" \
  --source-contract "$OUT/frozen-source-contract.json" \
  --sequence-mappings "$MAPPINGS" --recovery-family "$FAMILY" \
  --receipts-dir "$JOURNAL" --establish-writer-fence \
  --control-config "$CONTROL" --provider-contract "$PROVIDER" \
  > "$OUT/schema-apply.stdout" 2> "$OUT/schema-apply.stderr"
```

There is no raw SQL apply, manually fabricated fence, new control database, no-fence path or automatic retry in this handoff. Native acquires session locks, verifies provider/control/ACL/background-worker state, writes durable fence intent, closes connections, proves exclusivity, compares the complete plan again, applies exact SQL in one transaction, verifies content/catalog and permitted allocator changes, commits, re-observes complete evidence, finalizes recovery and releases the gate. Native code may scan multiple times; this is required preservation evidence, not a test campaign.

### 5. Completion and failure boundaries

Successful command exit alone is insufficient. Require result `migration_result`, `recovery_completion.status=committed`, matching reviewed pending filenames/evidence hashes, native terminal journal receipt, `writer_fence_release.phase=released`, `principal_binding_required=true`, and exact allocator preservation/advance outcomes. Record receipt and journal hashes. A source or preflight receipt is never completion.

The public CLI finalizes verified aborts when possible but deliberately leaves the gate closed on failure; commit exceptions are ambiguous. Result-file write can fail after database commit/recovery/release. If any command fails, preserve private output and native journal, keep application writers stopped, inspect the exact last fence/recovery state and do **not** replay apply or open connections manually. The native `db sequences reconcile` supports explicit takeover/release-intent reconciliation; root must select the specific observed intent and review its native plan before any mutation. A fresh process cannot reuse an old backend identity or invent a fence. Native recovery sequence floors remain binding after rollback.

After terminal schema receipt only: separate native operator bootstrap/template/actor preparation; Atlas approved scope/XRF repair; exact runtime-config-identity binding and grants; restart only exact final tagged service image/fresh pool; bounded authenticated DAG/XRF acceptance. Use the already prepared per-service helpers and controlling ledger. Schema completion does not imply preparation, deployment or acceptance.

## Source and prior-success basis

- Native 10.1.8 `cli/db.py:1567–1905`: public flags and retained target/control session with commit, recovery and release ordering.
- `cli/identity.py:91–210`, registered at `cli/db.py:936`: source/family inventory options and public command placement.
- `sequence_fence.py:59–103,138–267,1150–1313`: explicit different existing control DB, authenticated Aurora16.13 provider, physical same-server comparison, durable gate lifecycle and no implicit session termination.
- Successful Bloom source: `bloom-tapdb10-performance-integrated/docs/plans/20260911T150553Z_bloom_tapdb_10_1_maintenance_sop.md`, §§1–2. Historical exact control DB `postgres`; terminal 10.1.3 delta result SHA256 `1cbaa863c0b4682587f091ad8eb5d9d0f328949bfd7567b8b726c531bf87878b`, recovery `000018-20260912T004804Z`, fence released. This confirms supported sequence, not current permission or current provider identity.
