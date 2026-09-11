# Dewey unblock: 10.1.1rc1

This is a tagged, pip-installable prerelease, not a merge to main or a stable
10.1.1 promotion. Publication evidence is recorded in the controlling
[ledger](20260911_tapdb_dewey_prerelease_ledger.md).

## Install and resume

Publication and fresh public installation are verified. Dewey can now install
the exact pin in the service's intended environment:

```bash
python -m pip install --index-url https://pypi.org/simple 'daylily-tapdb==10.1.1rc1'
```

Python >=3.12 and the existing exact `meridian-euid==0.4.8` dependency apply.
Select capacity through the existing public config command; these values are
an example 512 MiB evidence budget, not a measured Dewey capacity requirement:

```bash
tapdb --config /abs/path/to/source.yaml db-config update \
  --inventory-max-rows 1000000 \
  --inventory-max-row-bytes 8388608 \
  --inventory-max-receipt-bytes 536870912
tapdb --config /abs/path/to/source.yaml --json db identity inventory \
  --receipt /abs/path/to/new-source-identity.json
tapdb --config /abs/path/to/source.yaml --json db census \
  --database EXACT_DESTINATION_DATABASE --database EXACT_CONTROL_DATABASE \
  --receipt /abs/path/to/new-principal-census.json
tapdb --config /abs/path/to/source.yaml --json db schema drift-check
```

Use the existing source-contract capture options (`--source-version` and any
verified `--sequence-mappings`) when the migration plan requires a historical
source contract, instead of substituting a standalone identity receipt.
Keep private receipts outside Git. No source schema is initialized or upgraded
by inventory or census. Schema drift-check reports drift; it does not apply it.

## T01: complete configurable inventory

Optional root config `inventory_limits` contains `max_rows`, `max_row_bytes`,
and `max_receipt_bytes`. Defaults remain 250000 rows, 8388608 serialized bytes
per source row, and 134217728 cumulative row-evidence bytes. Values must be
positive 64-bit integers; unknown fields, booleans and strings are rejected.
The byte budget is evidence size, not database size, final indented JSON file
size, or a process-memory limit. Complete evidence remains in memory; hashing
and receipt output now stream to avoid additional full JSON copies.

Public Python `InventoryLimits`, `capture_identity_inventory(..., limits=...)`
and explicit-target `inventory_limits` use the same implementation. Selected
limits and usage are sealed in receipts. Historical source-contract, backup,
restore verification, migration preflight/apply/postcommit, and fence-recovery
captures retain the reviewed policy. Conflicting explicit policies fail rather
than silently raising or lowering the reviewed budget. Unconfigured existing
v1 receipts retain their original shape and limits. Recapture after deliberately
changing a source policy; do not edit signed/hash-bound receipt content.

Limit failure raises `InventoryLimitExceededError` with sanitized `diagnostics`:
phase, table, processed_rows, accumulated_bytes, attempted_bytes, limit_name,
configured_value and attempted_value. The identity CLI emits these in JSON
error details. No partial successful inventory/source-contract receipt results.
Existing preservation rules, sequence floors and historical schema contracts
are not relaxed. Historical source-version validation also accepts the exact
PEP440 prerelease version, so this package can capture its own source contract.

## T02: read-only historical census

Python: `daylily_tapdb.principal_census.capture_principal_census(cfg,
database_names=(...), statement_timeout_ms=10000, max_catalog_rows=100000)`.
CLI: `tapdb --config ABS db census`, with repeatable `--database`, optional
new absolute `--receipt`, `--statement-timeout-ms`, and `--max-catalog-rows`.

The sealed `tapdb-principal-census/v1` receipt records authenticated operator,
server/database identity, observation time, roles/memberships/IAM membership,
database/schema/object owners and ACLs, column/default ACLs, effective database
CONNECT/CREATE/TEMP and schema privileges, available runtime bindings and named
database activity. It runs in a repeatable-read, read-only transaction. Catalog
queries are bounded and time-limited; overflow is an error, not truncation.
Passwords and query text are never selected. Missing historical binding
structures or unfiltered access are explicitly unavailable, not fabricated.

Named database existence comes from the configured server's pg_database, with
database OID and physical-server identity for existing databases. It does not
certify login to another database. Failed source authentication is an error,
never evidence that a destination is absent. Activity visibility is explicitly
marked; database sessions are not a complete service/HTTP/worker/scheduler
census or a writer fence. No roles, grants, bindings, objects, sessions or
identifiers are mutated. Exit code is 0 for capture and 2 for failure.

## T03: canonical schema drift JSON

Root `--json` and command-local `--json` both emit canonical machine-readable
JSON. Native CLI exit codes are 0 for clean, 1 for drift, and 2 for missing
database or inspection failure. This includes real entrypoint exit propagation,
not only an in-process command test.

## Validation and service boundary

Focused inventory/CLI/source-contract/migration-contract tests include complete
evidence above the old 128 MiB ceiling and receipt serialization/roundtrip.
An existing local PostgreSQL 16.13 server supplied a read-only census with
source presence, explicit destination/control absence, 16 roles and 173 objects.
No database was deployed, restored or migrated for this prerelease. No Aurora,
production, service dependency pin, service config or main branch was changed.
Full CI, full-suite coverage and a new database migration campaign were not
run, as requested. Dewey owns its actual inventory rerun, capacity selection,
recovery-set acceptance and subsequent migration acceptance after resuming.
