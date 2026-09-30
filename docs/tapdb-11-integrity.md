# TapDB 11 integrity and consumer adoption

Status: published in TapDB **11.0.0**, with isolated PostgreSQL 16.13 qualification.
See the [controlling ledger](plans/20260929_tapdb_integrity_11_ledger.md) for
publication receipts. This document is not production-adoption evidence.

## Guarantees and boundaries

TapDB is a typed object, lineage, audit and external-reference substrate. Native
object/reference operations require no event broker, outbox delivery, decision
engine, remote object owner, or workflow engine. Atomic audit capture remains
mandatory. Optional messaging facilities remain separate.

Constrained runtime roles can read authorized audit rows but cannot directly
insert, update, delete, truncate, or hide them. Native domain triggers append
one versioned before/after record per actual row mutation. A failed mandatory
audit append rolls back the domain mutation. Domain deletes remain soft deletes.

A database owner or superuser can disable these controls. No independent
immutable storage, administrator-proof history, operating backup schedule,
RPO/RTO, fleet-wide EUID uniqueness, or distributed rollback is claimed.
Privileged-administrator preservation is tracked separately in
[issue #117](https://github.com/Daylily-Informatics/daylily-tapdb/issues/117).

EUID formatting, prefix ownership, allocators, sequences and recovery-floor
semantics are unchanged. Administrators must provision nonoverlapping issuance
namespaces. A restored copy does not gain independent issuance authority merely
because restoration succeeded; existing runtime admission and recovery gates
still apply.

## Identifier meanings

| Value | Meaning and accepted interface |
|---|---|
| UID | Numeric row identity, local to one table/database; use a record type with UID selectors. |
| EUID | Owning TapDB persisted object identity. Syntax is not proof of persistence; cross-service targets also identify their owner. |
| Opaque UUID/barcode | External namespace + kind + exact value, with explicit tenant/global scope; never substitute it for an EUID. |
| Tenant UUID | Authorization/ownership context, not an object's UID or EUID. |
| Actor issuer + subject | Stable authenticated or service identity asserted by the trusted application. Subjects need not be UUIDs. |
| Request / operation ID | Correlation and operation identity; not a domain object identity. Correction operation IDs are durable retry keys. |
| Record revision | Database-owned monotonic integer for a particular record; not an identity or a commit timestamp. |
| History epoch / boundary | Historical interpretation scope and PostgreSQL visibility snapshot; not an EUID or a lock/permission token. |

Audit query results separate `audit_uid`/`audit_euid` from `record_uid`,
`record_table`, and the changed object's `euid`, domain and issuer. Joins use all
these identity coordinates, so equal numeric UIDs in different tables do not
connect unrelated history. Descriptions come from retained historical state;
`current_context` is explicitly today's record. Legacy audit evidence is labeled
`legacy_incomplete`, never upgraded by guessing an actor or an old value.

## Attribution v1 and revisions

Every writing transaction supplies an `Attribution` envelope:

```python
from uuid import uuid4
from daylily_tapdb.security_context import Attribution

attribution = Attribution(
    actor_kind="human",
    actor_issuer=verified_identity_provider_issuer,
    actor_subject=verified_identity_provider_subject,
    service_identity="atlas",
    request_id=str(uuid4()),
    operation_id=str(uuid4()),
)
# Pass attribution=attribution to TAPDBConnection, or set it on the request's
# RuntimeDBConnection before opening its transaction.
```

Automation uses `actor_kind="service"` and its real service subject/issuer. An
email/display username is not implicitly converted to an identity. The database
derives timestamps, actual authenticated `session_user`, database, scope and
transaction identity. Actor and executing-service assertions remain assertions
from the trusted service; a compromised authorized service can misstate them.

Context is installed transaction-locally, including an explicit empty envelope
on reads. Pooled connections do not inherit the preceding request's actor. A
write without valid context fails with a TapDB 11 contract mismatch. There is no
old-actor conversion or fabricated UUID fallback.

Templates, instances and lineages have `record_revision`. Fresh records begin
at 1; legacy records observed at adoption begin at 0. Actual updates increment
once. No-op updates produce no new revision. `tapdb.revision/v1` audit JSON
contains full persisted before/after state, attribution, revision, top-level
transaction ID, epoch and governing template identity. Hashed/encrypted values
are retained as stored, not decrypted. Lineages have no invented template;
their full endpoint/type/scope/deletion state is captured directly.

## History, graph boundaries and correction

`HistoryService.capture_boundary()` returns a read-only visibility token.
Capture it in a transaction with no preceding writes, then close the transaction.
Pass the unchanged token to later reads; no connection remains open for human
review, and no search snapshot, lease or result object is persisted.

`object_at(euid, record_type=..., boundary=...)` reads retained state at that
boundary. Alternatively use an exact `revision`; an explicit `epoch` permits
inspection of retained evidence from an older epoch. Old boundary tokens are
rejected against the new active epoch after restore/adoption. Tokens are query
parameters, not cryptographically signed evidence or access grants. RLS and
scope checks remain mandatory for every read.

`graph_at(seeds, boundary=..., max_depth=8, max_nodes=1000)` returns a bounded
local graph, including cycles. It reports missing seeds/endpoints, truncation,
external-reference boundaries and history completeness. It never expands into
Bloom or another owner. Historical template identity/revision is returned with
retained object evidence; applications remain responsible for interpreting
application-specific template semantics.

PostgreSQL visibility snapshots and stored top-level transaction IDs determine
visibility. Timestamp order and sequence order are not commit order. Time
filters identify candidate evidence, not exact commit boundaries. See
[PostgreSQL 16 snapshot functions](https://www.postgresql.org/docs/16/functions-info.html).

`plan_correction(changes, operation_id=..., reason=...)` is read-only. Callers
may select fields from an inspected historical revision; the proposal records
only selected fields, the diff, exact current revisions and scope. Lineage
corrections also capture both endpoint revisions and reject intervening endpoint
changes, even if the lineage itself has not changed. It does not
silently replace the whole current object with an old snapshot.

`apply_correction(plan)` locks affected records/endpoints in deterministic order,
checks revisions and recomputes the diff, runs registered owner validators, and
uses the same guarded field mutation primitive as ordinary object updates.
The transaction preserves unrelated fields and appends new history plus an
immutable native correction receipt. Reusing the same operation/plan returns
that receipt; a different plan cannot reuse the operation identity. Identity,
template bindings and canonical external target coordinates cannot be corrected
through arbitrary field writes. Missing owner validation refuses apply.

Owner validators are deployment code, never user-supplied module names:

- Python/embedded API: explicitly pass `owner_validators={owner: validator}`.
- Installed CLI/API extensions: register entry points in
  `daylily_tapdb.correction_validators`, keyed by the exact owner repository name.
- Signature: `validator(session, record, proposed_fields)`; raise to reject.
- Conflicting registrations fail. There is no permissive default validator.

Correction does not undo laboratory activity or remotely owned specimen state.
A read-only proposal may be available when application semantics prevent apply.

## External references and annotations

`ExternalReferenceService` is the single native writer:

| Operation | Contract |
|---|---|
| `register(target)` | Independent create-or-resolve in explicit current scope; no dummy source or network call. |
| `resolve(target)` | Exact local lookup; no registration, enrichment or write. |
| `annotate(reference, authority, key, data, expected_revision=...)` | Versioned, linked annotation owned by the executing service; updates require its current revision. |
| `attach(source, spec, expected_source_revision, expected_lineage_revision=None, owner_validator=None)` | Protect endpoints and exact association. `None` expects no prior edge. Optional owner callback enforces consumer cardinality under those locks. |
| `detach(..., expected_source_revision, expected_lineage_revision)` | Soft-deactivate the exact authority-owned association; retain identities and history. |
| `reconcile(..., expected_source_revision, expected_lineage_revisions)` | Compare the whole current authority-owned active edge set before applying its desired set. |

Reference identity/target coordinates remain immutable; supported optional
non-null enrichment conflicts fail explicitly. An opaque tube reference may
later point through a typed, provenance-bearing assertion to a service-owned
reference. Its original EUID and upstream participation links remain intact.
Registration alone does not verify remote existence. Verification belongs to
the owner/consumer and is recorded separately.

Annotations use `reference/annotation/generic/1.0`, an existing reserved XRF
prefix, and an `annotates_reference` lineage. They are metadata about a
reference, not another physical tube identity. Authority/key are immutable;
annotation writes require their executing authority. Canonical reference
properties are not arbitrary application metadata storage.

TapDB supplies transaction locks and optimistic revisions. Atlas must supply
its own binding cardinalities, edit leases, UI session rules and clinical
constraints. These controls do not lock direct Bloom operations.

## CLI, API and GUI contract

New CLI groups are `tapdb history` and `tapdb references`; operations use JSON
`--payload` files. Writes require `--apply` and an explicit `--attribution` file
(either at the root or on the integrity command). `history status` reports
scoped completeness and epoch diagnostics. Existing object update/delete also
require `--expected-revision`.

Authenticated routes use the same dispatcher and native services:

- `POST /api/integrity/read/{operation}`: bounded JSON query body, no writes.
- `POST /api/integrity/write/{operation}`: authenticated administrator required.
- Stale revision conflicts: HTTP 409. Invalid inputs: 422. Unauthorized
  operations: 403. Missing objects: 404.

An embedding host must provide stable `actor_issuer` and `actor_subject` from
its authentication layer, plus actor kind when the initiator is a service.
A host's numeric user UID is never relabeled as a native TapDB user identity.
Native GUI users carry their persisted actor EUID. Auth-disabled placeholders
cannot make attributable writes. Shared-host provisioning without stable host
identity fails explicitly.

Current standalone Cognito adapter inspection found a preexisting dependency
mismatch: `admin/cognito.py` imports `daylily_cognito.CognitoAuth`, whereas the
pinned `daylily-auth-cognito==2.1.5` exposes `daylily_auth_cognito`. No live Cognito
verification or provider configuration was exercised in this qualification.
Embedded authenticated API tests establish adapter behavior, not successful
standalone Cognito login. This remains an explicit integration gap; do not
interpret a passing API fixture as proof of that login path.

GUI object forms carry current revisions. Existing generic lineage creation
protects both endpoints and requires both observed revisions. New history and
reference business operations live in native services and shared adapters;
there is no GUI-only implementation of them.

## Fresh installation, adoption and restore

Fresh provisioning uses the packaged schema, audited-role setup and exact
bundled core templates. The two additions are:

- `reference/annotation/generic/1.0`
- `governance/correction_receipt/generic/1.0`

Existing installations require explicit, reviewed adoption. Installing the
Python package or invoking ordinary schema apply does not silently adopt them.

1. Back up the selected installation through its approved native lifecycle.
   Inventory consumer writers and prepare explicit attribution/revision changes.
2. Run `tapdb --config /absolute/target.yaml integrity-adopt plan --output
   /absolute/adoption-plan.json`. This fingerprints existing rows/audit, schema
   and security catalogs, allocators, exact SQL assets and the two new templates.
3. Review the target, preserved evidence, operator authority and required runtime
   rebind. Apply with `integrity-adopt apply --plan ... --control-config ...
   --receipts-dir ... --receipt ... --apply`. The independent control database is
   mandatory; provider-backed fences additionally take `--provider-contract`.
4. The operation uses native writer fencing. It verifies old records/audit and
   allocator state before installing the two exact new templates through the
   native loader. Those new template/audit identities are issued normally. No
   unrelated template pack or governance object set is reseeded.
5. A baseline records current state, observer attribution and governing template
   identity. It is explicitly state observed at adoption, not recreated history.
   Repeated adoption is rejected.
6. Use native `db runtime-principal bind` planning/application with its receipts
   before admitting updated consumers. Old consumers fail rather than silently
   losing attribution. A failed adoption retains the writer fence for explicit
   native reconciliation; never reopen it with operator SQL.

Runtime audit grants are reconciled for every bound principal. Stale column
grants are removed; inherited/PUBLIC mutation authority or membership in the
audit-writer role blocks completion rather than silently editing unrelated role
memberships. Existing principal/catalog diagnostics still validate triggers,
routine bodies, pinned paths, ownership, RLS and effective privileges.

Native restore of a current-contract backup retains prior audit/history and
observes current states in a new epoch before admitting writes. Historical
backups retain their verified source contract: restoring a 10.1.11 backup does
not silently adopt it. Its receipt reports `history.adoption_required`; explicit
adoption is required before new-contract writes. Recovery floors and principal
binding remain separate gates. Restore verification includes soft-deleted
records. The focused qualification covers current-contract and exact 10.1.11
isolated restores plus separate adoption from exact 10.1.11 assets. It does not
establish every historical-release restore or an Aurora production cutover.

## Consumer handoff and qualification limits

| Owner | Required action outside this package work |
|---|---|
| Atlas | Supply verified initiating identity and service context; use native external references/annotations; pass expected revisions; implement its own sessions/search/editor/leases and owner validators. |
| Bloom | Supply attributable writes and owner verification/existing-tube association contracts. No operations inside Bloom are performed here. |
| Other consumers | Inventory every supported writer, including automation/bulk/import paths; provide explicit context and adopt changed native signatures. |
| Operator / Dayhoff | Review backups, adoption, runtime rebind and coordinated dependency pins; retain recovery receipts; maintain nonoverlapping issuance authority. |

The qualified runtime uses SQLAlchemy **2.0** (`>=2.0,<2.1`) and psycopg2.
SQLAlchemy 2.1 changes the implicit PostgreSQL driver and exposed existing
parameterized-SET incompatibility during qualification; that unqualified range
is excluded rather than silently switching drivers.

The focused suite uses disposable native PostgreSQL **16.13**, synthetic records
minted by native factories/triggers, and no production/AWS resources or sibling
service tests. It covers runtime audit denials, mandatory audit atomicity,
identity-safe lookup, attribution, visibility/rollback/savepoints, revision
conflicts, correction receipts, offline references, competing binding rules,
tenant rejection, API/CLI parity, exact 10.1.11 adoption and native restore.
See the ledger/evidence for the exact latest pass count and unresolved items.

No performance benchmark, PG17, Aurora cutover, browser walkthrough, live
identity-provider login, privileged-administrator tamper-proof store, Atlas
case replay, or fleet acceptance is claimed. Full-row audit increases storage
and retention cost; measure that against consumer workloads before capacity
commitments. Publication, consumer rollout and production acceptance are
separate states.
