# Scoped identity-prefix discovery

`tapdb --config /absolute/operator-config.yaml db identity inventory --prefix BG`
reads a single prefix in the explicitly configured database and schema. It uses
the native operator read-only transaction; it does not initialize a schema,
allocate an EUID, change a sequence, or grant namespace ownership.

The `tapdb-prefix-usage/v1` result reports:

- Stored identities, including soft-deleted rows, grouped by native owner.
- Template instance-prefix and native identity-prefix bindings.
- Historical audit references to the requested domain/prefix.
- Matching native sequence state, including its cached reservation position.
- Target and authenticated physical database identity.

Known external allocation journals can be included with repeated
`--allocation-receipts-dir /absolute/native-journal-directory` arguments. The
native journal reader retains matching reservation floors regardless of a
receipt's success/failure status. No object properties or unrelated table rows
are exported.

An optional `--receipt /absolute/new-result.json` stores the result using the
existing receipt writer. This scoped result is deliberately not a complete
identity-preservation inventory and cannot be used as one. Source-version,
sequence-mapping, and recovery-family capture flags are mutually exclusive with
`--prefix`.

Availability across a Meridian domain requires the explicit relevant database
targets and known allocation journals, together with its owning registry. A
database with no matching rows does not establish global availability, and an
unregistered prefix does not erase historical allocations. Existing assignments,
generators, bindings, or reservation floors require disposition before a new
independent database begins issuing that domain/prefix.

## Existing evidence, without a database connection

`tapdb db identity prefix-from-evidence` consumes existing successful native
evidence. It does not call the database configuration resolver or open a database
connection. Its `tapdb-prefix-usage-from-evidence/v1` result is distinct from live
`tapdb-prefix-usage/v1` inventory. `--evidence-kind` is required; an unavailable
live target never silently selects one of these modes.

### Archived source contract

Use `--evidence-kind archived_source_contract` with all three explicit files:

```bash
tapdb --config /absolute/operator-config.yaml db identity prefix-from-evidence \
  --evidence-kind archived_source_contract \
  --evidence /absolute/existing-source-contract.json \
  --archive-result /absolute/existing-copy-sop-result.json \
  --recovery-family /absolute/existing-recovery-family.json \
  --prefix BG \
  --receipt /absolute/new-archived-prefix-projection.json
```

The accepted source is `tapdb-source-contract/v1` containing full
`tapdb-identity-inventory/v1` rows and `tapdb-sequence-inventory/v1`. The projection
queries retained identities, template/identity-prefix bindings, audit references
and native generators. It includes soft-deleted identities and missing-generator
binding evidence. A compact audit-digest inventory cannot supply per-prefix audit
coverage and is rejected for this operation.

Native per-column value hashes are queryable evidence: the operation computes
the lookup key for the explicitly requested prefix when reading the retained
identity-prefix configuration. It does not recalculate receipt seals or verify
whole inventories. Original source/identity/sequence seals are retained only as
provenance identifiers.

The supported archive evidence is the existing copy-SOP result with
`status = copy_created_source_remains_fenced`. Its `fingerprint.source`,
`source_oid`, `source_physical` and `source_contract_sha256` must identify that
exact source. Its matching `final_database_gates` entry records `datallowconn =
false`, database name and OID. The result's existing `completed_at` and
`fingerprint.acceptance_reference` are preserved. This consumes the recorded
successful lifecycle outcome; it does not rerun the copy or reopen/recheck its
fence. This result is not a backup certificate.

The explicit `tapdb-recovery-family/v1` descriptor supplies the native journal
roots. Its origin must identify the copy recorded by this same copy-SOP result:
`fingerprint.target` and `copied_database.datname`/`oid` bind the copy name and
database OID; the source's recorded server identity and target scope bind the
host, port, schema, domain and owner. The family's explicit replacement config
path is preserved. An unrelated family UUID or journal directory is insufficient.
The output retains the family origin and the source-to-recorded-copy linkage.
Existing native `read_receipts` reads the declared roots once, and the projection retains
M/prefix reservations and generator evidence for the source or its explicitly
tagged family. Failed, intent and committed receipt statuses remain visible;
reservations are not discarded because an operation failed. The output lists
consumed journal receipt IDs, sequences, operations, statuses and recorded times.
There is no chain/checksum revalidation, recovery-family recapture, or second
journal store.

The output records `observation_mode = archived_source_contract` and
`scope = explicit_archived_database_schema`. Its time basis is archive-operation
completion, not a newly invented snapshot time: `snapshot_capture_timestamp` is
null because the historical contract did not record one. Journal coverage is
the explicitly supplied family roots and consumed existing receipt records.

### A schema reported absent by native census

Use the exact successful `tapdb-principal-census/v1` result:

```bash
tapdb --config /absolute/operator-config.yaml db identity prefix-from-evidence \
  --evidence-kind catalog_schema_absent \
  --evidence /absolute/existing-principal-census.json \
  --prefix BG \
  --receipt /absolute/new-schema-absence-projection.json
```

This mode requires the census's `schemas` and `objects` lists both to be empty.
It preserves the exact target/schema, physical database identity, authenticated
operator and original native `observed_at`. It records
`scope = explicit_database_schema_at_census`; it does not manufacture empty
identity rows, assert the entire database is empty, or establish absence of past
allocations. External journals are explicitly `not_supplied` in this mode.
Archive/family arguments are not accepted.

Consumers must retain the evidence mode, target, time basis and historical
coverage limitation when deciding on a new assignment. Neither mode establishes
domain-global absence or calculates a sequence starting range.
