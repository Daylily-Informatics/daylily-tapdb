# TapDB service readiness

This is the operator contract for preparing an existing TapDB-backed service
for inventory, recovery, schema migration, and service acceptance.

> **Release status:** TapDB 10.1.0 is an unreleased candidate. This release's
> qualification scope is exact community PostgreSQL 16.13 and isolated Aurora
> PostgreSQL 16.13. PostgreSQL 17 qualification is deferred to
> [GitHub issue #107](https://github.com/Daylily-Informatics/daylily-tapdb/issues/107)
> and has not passed; that deferral is not a claim that the runtime is
> unsupported. Independent
> 16.13/Aurora qualification, reviewed merge, immutable tag, published
> artifacts, and fresh-install verification are not complete. Do not pin a
> consumer to 10.1.0 until the release handoff contains those receipts.

TapDB supplies substrate evidence and guarded lifecycle operations. The service
still owns its data conversion, dependency pin, deployment, policy, runtime
configuration, maintenance window, acceptance tests, and cleanup.

## Recovery set: database plus external artifacts

A logical dump or provider snapshot is not a complete service recovery set.
Before changing a service, preserve these artifacts under their owning systems
and record their exact versions, checksums, or immutable references:

| Required artifact | Why the database backup does not contain it |
|---|---|
| Exact TapDB config for each source, control, and target | Config selects physical database, schema, runtime/operator principals, domain, owner, tenant scope, and safety policy. |
| Domain-code and prefix-ownership registries | Registry files are external identity authority; schema rows and sequence names are not substitutes. |
| TLS trust files and certificate deployment | `pg_dump --no-owner --no-acl` and RDS snapshots do not preserve client trust material or its deployment. |
| IAM policies, trust relationships, and explicit AWS profile selection | A database artifact cannot prove the caller's cloud identity or authorization. |
| Operator and runtime secret references plus a separately managed recovery method | TapDB receipts reject secrets. Never copy a secret value into a receipt, manifest, handoff, or repository. |
| PostgreSQL principal-state inventory | The owning system must capture login/role attributes, memberships, database/schema/object ownership and ACLs, immutable domain/owner/tenant scope bindings, and IAM-to-database-role mapping. Do not record secret values. A `--no-owner --no-acl` dump and a later bootstrap/bind result do not preserve or prove the original state. |
| Service runtime files | Container/image digests, process definitions, environment-file references, mounted files, and application config are service-owned. |
| External receipt journals and their head anchors | Allocator intents, writer-fence state, recovery floors, and ambiguous outcomes live outside the restored database. |
| Deployment and acceptance receipts | A restored schema does not prove that a consumer was pinned, deployed, rebound, or accepted. |

Missing external artifacts are a hard recovery blocker. Do not infer a database
name, schema, principal, registry, TLS path, profile, receipt root, or service
deployment identity from another environment.

## Public interfaces

Use one existing absolute config path on every CLI invocation:

```bash
tapdb --config /abs/path/to/tapdb-config.yaml --json <command>
```

The service-readiness CLI groups are:

| Purpose | Public command |
|---|---|
| Catalog-complete identity/source evidence | `tapdb db identity inventory` |
| Preservation comparison | `tapdb db identity verify` |
| Allocator plan/apply | `tapdb db sequences advance` |
| Final allocator floor check | `tapdb db sequences verify` |
| Stalled fence or lost-acknowledgement reconciliation | `tapdb db sequences reconcile` |
| Offline runtime-principal plan and CONNECT-only preparation | `tapdb db runtime-principal bootstrap` |
| Receipt-bound runtime database/schema binding | `tapdb db runtime-principal bind` |
| Backup, verification, staged restore, and rehearsal | `tapdb backup ...` |

The public Python entry points, for callers that already own the transaction
and explicit target construction, are:

```python
from daylily_tapdb.backup import (
    capture_source_contract,
    create_backup,
    plan_backup,
    plan_restore,
    restore_backup,
    verify_backup,
)
from daylily_tapdb.identity_inventory import (
    capture_identity_inventory,
    verify_identity_inventory,
)
from daylily_tapdb.runtime_principal import (
    bind_runtime_principal,
    bootstrap_runtime_principal,
)
from daylily_tapdb.sequences import (
    apply_sequence_advance_plan,
    build_sequence_advance_plan,
    capture_sequence_inventory,
    verify_sequence_floors,
)
```

Do not replace these interfaces with raw SQL, private module imports, direct
`pg_dump`/`pg_restore` commands, or a second allocator implementation.

## Versioned evidence

Receipts are JSON-compatible and checksum-bound. Treat an unknown version,
digest mismatch, missing physical identity, missing journal head, or changed
reviewed plan as a refusal, not a warning.

| Evidence | Version marker |
|---|---|
| Historical source contract | `tapdb-source-contract/v1` |
| Identity inventory / verification | `tapdb-identity-inventory/v1` / `tapdb-identity-verification/v1` |
| Sequence inventory / plan / apply / verification | `tapdb-sequence-inventory/v1` / `tapdb-sequence-advance/v1` / `tapdb-sequence-apply/v1` / `tapdb-sequence-verification/v1` |
| Writer fence / takeover / quarantine verification | `tapdb-writer-fence/v1` / `tapdb-writer-fence-takeover/v1` / `tapdb-writer-quarantine-verification/v1` |
| Recovery floor / recovery family | `tapdb-recovery-floor/v1` / `tapdb-recovery-family/v1` |
| Runtime-principal plan/result | `tapdb-runtime-principal/v1` |
| Migration preflight | `tapdb-migration-receipt/v1` |
| Backup manifest | manifest schema version `2` |

The following are the minimum consumer-facing shapes. They are sealed by the
named public interface; operators supply inputs and paths, but do not handwrite
or edit a sealed receipt.

| Artifact | Required keys and types | Generation authority |
|---|---|---|
| Recovery family | `schema_version: str`, canonical `family_id: UUID str`, `origin: {target, physical_target}`, sorted unique `receipts_dirs: list[str]` of existing canonical absolute roots, `sha256: str` | `tapdb db identity inventory --new-recovery-family-id ... --family-receipts-dir ... --family-receipt <new-file>`; library callers use `build_recovery_family(...)`. |
| Historical source contract | `schema_version: str`, exact `source_version: str`, `source_version_evidence: "operator_declared"`, complete `identity_inventory: object`, complete `sequence_inventory: object`, optional unchanged `recovery_family: object`, `sha256: str` | `tapdb db identity inventory --source-version ... --receipt <new-file>`; library callers use `capture_source_contract(...)`. |
| Sequence advance plan | `schema_version: str`, `schema_name: str`, exact `target: object`, sealed `inventory: object`, `floors: list[{name: str, value: int, source: str}]`, `advances: list[{name: str, floor: int, next_value: int}]`, `sha256: str`; a family plan also carries `recovery_family`, `input_floors`, and `family_state_sha256` | Dry-run `tapdb db sequences advance --receipt <new-file> ...`; library callers use `build_sequence_advance_plan(...)`. Apply requires that unchanged preflight receipt. |
| Writer fence | `schema_version: str`, `phase: "acquired"`, exact `target` and `physical_target`, `fence: object`, allocator `inventory_sha256`, connection/control/provider evidence, original database ACL, intent IDs, and `sha256`; family operations also carry the unchanged `recovery_family` | `tapdb db sequences advance --apply --establish-writer-fence --control-config ... --receipts-dir ...`; the library authority is `acquire_database_writer_fence(...)`. It must be validated and later released or reconciled through the public sequence lifecycle. |
| Restore `recovery_source` payload | JSON object with exact `purpose`. `isolated_rehearsal` requires `{"purpose": "isolated_rehearsal"}` and, when present in the backup contract, its unchanged `recovery_family`. `fenced_source_recovery` requires `purpose`, the freshly captured sealed `source_contract`, its verified `writer_fence` (or the separately supplied quarantine receipt), canonical absolute `source_receipts_dir`, and unchanged `recovery_family`. | The operator composes this input only from the public source-contract, family, fence/quarantine, and journal outputs. `tapdb backup restore-plan` validates it against live/source history and binds it into the reviewed plan fingerprint; `tapdb backup restore` accepts only the unchanged validated payload. |

`fenced_migration` is the exact purpose recorded internally by the public
receipt-bound schema-migration path. It is not a substitute restore purpose and
must not be placed in `recovery-source.json`.

The physical target binding includes the authenticated database, database OID,
server address, and server port in addition to the explicit config identity.
Receipt IDs are unique only inside their journal root; identify a receipt by
both journal root and receipt ID.

## Required lifecycle

Complete the stages in order. A later stage does not waive an earlier receipt.

### 1. Inventory the exact source

Record the service source commit, installed TapDB distribution/version, exact
config checksum, database/schema identity, external artifact inventory, and a
read-only historical source contract.

```bash
tapdb --config /abs/path/to/source-config.yaml --json \
  db identity inventory \
  --source-version <exact-source-tapdb-version> \
  --sequence-mappings /abs/path/to/verified-sequence-mappings.json \
  --receipt /abs/path/to/evidence/source-contract.json
```

`--source-version` is an operator declaration tied to the physical inventory;
it is never inferred from an application version. Unknown allocator mappings,
incomplete physical tables, unreadable identities, or a source that changed
during capture fail the stage.

The preceding command is the non-family form. For recovery that can span the
source and replacements, use the following alternative with distinct new
output files. Create every intended journal root first. The operator supplies
the canonical UUID and repeats `--family-receipts-dir` for the source and every
planned replacement:

```bash
tapdb --config /abs/path/to/source-config.yaml --json \
  db identity inventory \
  --source-version <exact-source-tapdb-version> \
  --sequence-mappings /abs/path/to/verified-sequence-mappings.json \
  --new-recovery-family-id <operator-chosen-canonical-uuid> \
  --family-receipts-dir /abs/path/to/source-journal \
  --family-receipts-dir /abs/path/to/replacement-a-journal \
  --family-receipts-dir /abs/path/to/replacement-b-journal \
  --family-receipt /abs/path/to/evidence/recovery-family.json \
  --receipt /abs/path/to/evidence/source-contract-with-family.json
```

The family descriptor is immutable. It must retain the origin's configured and
physical identity and the complete sorted set of source/replacement journal
roots. TapDB does not discover related roots from names or directories. Add a
replacement only through the reviewed recovery lifecycle; never edit a sealed
descriptor or omit an older root.

### 2. Prepare the target principal

Create an offline configuration-only plan first. It performs no connection or
credential lookup:

```bash
tapdb --config /abs/path/to/target-config.yaml \
  db runtime-principal bootstrap \
  > /abs/path/to/evidence/runtime-principal-bootstrap.plan.json
```

Review the configured target identity and principal attributes. The offline
plan cannot claim an observed physical identity. Applying bootstrap
authenticates and verifies the configured physical target, creates or validates
only the configured constrained runtime login, and grants only `CONNECT` on the
configured existing database (plus explicit non-admin `rds_iam` membership
when configured):

```bash
tapdb --config /abs/path/to/target-config.yaml \
  db runtime-principal bootstrap --apply \
  > /abs/path/to/evidence/runtime-principal-bootstrap.result.json
```

Bootstrap does not create the database, apply schema, seed data, grant schema
access, or bind runtime scope. Role collision, elevation, unexpected
membership/settings, effective database `CREATE`, or an incomplete operator
authority check is a hard failure.

### 3. Back up and restore with explicit recovery authority

Plan and capture from the unchanged source contract and recovery family:

```bash
tapdb --config /abs/path/to/source-config.yaml --json backup plan \
  --class full \
  --source-contract /abs/path/to/evidence/source-contract-with-family.json \
  --recovery-family /abs/path/to/evidence/recovery-family.json

tapdb --config /abs/path/to/source-config.yaml --json backup create \
  --class full \
  --source-contract /abs/path/to/evidence/source-contract-with-family.json \
  --recovery-family /abs/path/to/evidence/recovery-family.json

tapdb --config /abs/path/to/source-config.yaml --json backup verify \
  --backup-id <backup-id> --level deep
```

An ordinary recovery apply must name the destination database and unchanged
schema explicitly. Stage with the same source/fence/control/provider evidence
that apply will use, retain the returned `plan_fingerprint`, and then apply the
unchanged plan:

```bash
tapdb --config /abs/path/to/target-config.yaml --json backup restore-plan \
  --backup-id <backup-id> \
  --mode isolated \
  --target-database <exact-new-database> \
  --target-schema <exact-source-schema> \
  --recovery-source /abs/path/to/evidence/recovery-source.json \
  --control-config /abs/path/to/control-config.yaml \
  --provider-contract /abs/path/to/evidence/provider-contract.json \
  --quarantine-receipt /abs/path/to/evidence/quarantine.json

tapdb --config /abs/path/to/target-config.yaml --json backup restore \
  --backup-id <backup-id> \
  --mode isolated \
  --target-database <exact-new-database> \
  --target-schema <exact-source-schema> \
  --plan-fingerprint <reviewed-fingerprint> \
  --recovery-source /abs/path/to/evidence/recovery-source.json \
  --control-config /abs/path/to/control-config.yaml \
  --provider-contract /abs/path/to/evidence/provider-contract.json \
  --quarantine-receipt /abs/path/to/evidence/quarantine.json
```

Only `tapdb backup rehearse` owns its UUID-qualified disposable rehearsal name.
That does not authorize TapDB or an operator to infer the name of an actual
recovery database. In-place recovery is destructive and requires its separate
policy and typed-target approval; it is not an ordinary service-conversion
path.

Every successful restore still reports `principal_binding_required: true`.
Database restoration does not reactivate runtime access.

### 4. Apply the reviewed schema migration

Use `tapdb db schema migrate` through the explicit target config and the
receipt-bound migration flow. Migration requires the verified writer fence and
external receipts directory recorded during preflight. Do not run migration
SQL directly, infer historical table coverage, or treat a warning-only identity
comparison as acceptance.

The migration must preserve every original UID/EUID, creation identity, audit
row, lineage edge, application table, prefix binding, and allocator definition
unless an explicit conversion manifest names the permitted non-identity
transformation. Existing stored prefixes remain immutable even if a service's
historical template binding differs.

### 5. Perform the service-owned conversion

At this point the owning service updates its exact dependency pin and lock,
applies its domain conversion, binds its external registries/runtime files, and
runs application tests. The named plan rows consume the shared TapDB
prerequisites above, while their remaining pin, conversion, deployment,
cleanup, and acceptance work is service-owned. The relevant cross-repo plan
rows are:

- Dewey L02, L04, and L05.
- Bloom T10-00, T10-01, and the TapDB part of MG01.
- Ursa DB01 and the TapDB parts of DB04 through DB07.

Do not deploy or clean up the old service/database until final verification and
service acceptance are complete.

### 6. Verify identities, floors, and runtime binding

Capture the final target inventory and compare it with the original contract:

```bash
tapdb --config /abs/path/to/target-config.yaml --json \
  db identity verify \
  --before /abs/path/to/evidence/source-contract-with-family.json \
  --conversion-manifest /abs/path/to/evidence/conversion-manifest.json \
  --sequence-mappings /abs/path/to/verified-sequence-mappings.json

tapdb --config /abs/path/to/target-config.yaml --json \
  db sequences verify \
  --floors /abs/path/to/evidence/retained-floors.json \
  --sequence-mappings /abs/path/to/verified-sequence-mappings.json \
  --recovery-family /abs/path/to/evidence/recovery-family.json
```

Every supported generator must be positive, noncycling, mapped by verified
catalog evidence, and strictly beyond all source, target, reserved, aborted,
probe, migration, and recovery floors. Cached generators require a real writer
fence and closure of other allocator sessions; table locks or an empty queue
are not equivalent.

Then plan and apply runtime binding against the unchanged target:

```bash
tapdb --config /abs/path/to/target-config.yaml \
  db runtime-principal bind \
  --receipt /abs/path/to/evidence/runtime-bind.plan.json

tapdb --config /abs/path/to/target-config.yaml \
  db runtime-principal bind \
  --receipt /abs/path/to/evidence/runtime-bind.plan.json --apply
```

Apply writes a separate result next to the reviewed plan. Binding grants exact
database `CONNECT`, managed-schema privileges, proven managed sequence access,
and immutable runtime scope. It must deny managed-schema and startup DDL,
elevation, broad default privileges, and access to preserved service objects
that TapDB does not own. Database `TEMP` may remain available; TapDB's managed
allocator functions use qualified object resolution so temporary objects
cannot redirect them.

### 7. Run service acceptance

The service owner must prove the exact source and package pin, deployment
identity, runtime principal, config/registry/TLS/IAM rebinding, application data
conversion, API/GUI behavior, rollback posture, and acceptance tests. A passing
TapDB migration or restore is prerequisite evidence, not service acceptance.

## Writer fence and ambiguous outcomes

`db sequences advance` creates a dry-run plan receipt by default. Apply requires
the unchanged preflight, an external receipt directory, and either a verified
fence receipt or `--establish-writer-fence` with a different explicit control
database. The gate must remain closed through commit, reconciliation, and
verified release.

If a process dies or the commit acknowledgement is lost:

1. Keep the durable intent and all reserved floors.
2. Leave the target operator-only until the outcome is established.
3. Use `db sequences reconcile` with the exact journal root and exact stalled
   intent receipt ID.
4. Record `committed` or `rolled_back` only from authoritative evidence.
5. When the transaction outcome cannot be established, record an independently
   validated observed-only reconciliation and retain its conservative floors.
6. Create a new reviewed plan only after the prior intent has a recognized
   terminal reconciliation.
7. Verify gate release and session closure separately.

Observed-only means “the fenced physical state was measured.” It does not mean
the old operation committed. Never retry past an ambiguous intent, overwrite a
journal, drop a family root, or relabel an observation as success.

## Service source crosswalk

These are source-contract inputs, not claims about deployed binaries:

| Service plan rows | Exact source basis | Shared TapDB prerequisites | Remaining service-owned work |
|---|---|---|---|
| Dewey L02/L04/L05 | Historical TapDB `9.0.9`; service inventory must be captured by its owner | Published exact release, identity/allocator inventory, principal lifecycle, backup/restore, migration preservation, and recovery floors | Pin and lock, replacement database, domain/data/config conversion, rehearsal, deployment, cutover, cleanup, and acceptance |
| Bloom T10-00/T10-01 and TapDB part of MG01 | Source commit `948bc487415899eb50f024f2d421066a061f0a48`; `pyproject.toml` pins `9.0.10`; lock resolves TapDB commit `12351b832276d35228527c3c9596e5d60e7c7824`; `9.0.10` schema assets are byte-equivalent to `9.0.9` | Published exact release, allocator/recovery/principal/backup/migration substrate | Pin adoption, assertion/template conversion, in-place service migration, deployment, cleanup, and acceptance |
| Ursa DB01 and TapDB parts of DB04–DB07 | Historical TapDB `9.0.9`; service inventory must be captured by its owner | Published exact release, principal lifecycle, backup/migration, and allocator/recovery substrate | Source-plan pin amendment, typed-reference/policy conversion, deployment, cutover, cleanup, and acceptance |

Each service must still capture its actual installed package, source commit,
config, physical database, and registry evidence before conversion.
