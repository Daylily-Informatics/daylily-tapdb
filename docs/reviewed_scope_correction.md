# Reviewed native scope correction

This narrowly bounded operator API assigns an explicitly reviewed tenant to
existing, active, unclaimed native-global domain objects. It is separate from
physical schema migration. It does not infer tenancy from metadata, migrate
identity claims, move adjacent shared objects, or authorize arbitrary operator
cross-tenant writes. Canonical XRF/DAG wire contracts do not change.

## Preconditions and exact input

Use an authenticated operator connection with the exact configured database,
schema, domain, owner, operator role and config path. Physical migration must
already have completed with its identity-preservation receipt. Provision the
packaged `evidence/repair/scope_assignment/1.0/` template using native template
tooling before preview. Preview verifies the installed lineage trigger against
the package's canonical source, ownership, search path and firing shape.

The request is a JSON object with exactly these fields:

| Field | Required meaning |
| --- | --- |
| `target` | Object containing exact `database`, `schema_name`, `domain_code`, `owner_repo_name`, `operator_user`, absolute `config_path` |
| `operation_id` | Unique ordinary lowercase idempotency key; never an invented EUID |
| `actor`, `reason`, `approval_ref` | Attributable operator and retained authorization |
| `migration_receipt_sha256` | SHA256 of the successful native physical-migration receipt |
| `tenant_authority_sha256` | SHA256 of the reviewed owning-service tenant authority receipt |
| `tenant_id` | Explicit canonical UUID; null is forbidden |
| `source_euids` | Exact owner-issued source identities; 1–96 unique entries |
| `allowed_templates` | Exact reviewed `category/type/subtype/version/` codes |
| `retire_lineage_euids` | Exact old `external_relation:identifies` assertions to retire; empty for scope-only CLI apply |

The operator verifies the two authority receipts; the library binds their
digests into the preview and final evidence but does not independently interpret
external receipt formats or obtain approval. A digest is not approval by itself.

Preview records sanitized structural fields and protected-row hashes, not raw
source payloads. It includes all incident active and inactive lineages and their
outside endpoints, with a maximum of 1,024 incident rows. Every active source and
incident endpoint must initially be native-global, active and in the same
owner/domain. Source identity keys/claims are rejected. Source native template,
taxonomy and prefix must match. An unexpected boundary, missing row or exhausted
bound fails rather than broadening the cohort.

## Native CLI and library composition

Use explicit absolute input/output paths:

```text
tapdb --config <operator-config> objects scope-correct preview --request <request-json> --receipt <plan-json>
tapdb --config <operator-config> objects scope-correct apply --plan <reviewed-plan-json> --expected-plan-sha256 <approved-plan-hash> --receipt <result-json>
tapdb --config <operator-config> objects scope-correct status --plan <reviewed-plan-json> --expected-plan-sha256 <approved-plan-hash> --receipt <status-json>
```

Preview is read-only. Apply refuses global dry-run mode and refuses planned
legacy retirements: those require atomic composition with canonical XRF writes.
The public functions are in `daylily_tapdb.services.scope_corrections`.

For the Atlas conversion, the owning operator command uses its existing native
`session_scope(commit=True)`, then `scope_correction_transaction(session, plan,
approved_hash)`. Inside that context it attaches each exact reviewed canonical
reference using `ExternalReferenceService.attach`, preserving the original
assertion authority/time/provenance. After those successful attaches it retires
the exact planned old assertions using `soft_delete_object(session,
ObjectSelector(euid=actual_persisted_lineage_euid, record_type="lineage"),
actor=reviewed_actor, dry_run=False)`. This existing public operation changes
only `is_deleted`. No caller-side ORM field assignments or SQL updates are
part of the supported application contract.

The context owns a savepoint, not the outer transaction. It yields a provisional
`pending_commit` outcome, creates truthful evidence after successful native
composition, and leaves commit ownership to the caller. The caller must not
report success until the outer transaction commits. No independent auto-commit,
runtime-role impersonation or connection substitution occurs.

## Exact mutation effects

The context locks instance and lineage tables against concurrent writes with a
10-second lock-acquisition timeout, rechecks the exact preview, then changes:

1. Selected existing instance `tenant_id`: null to the reviewed UUID.
2. Every active incident lineage `tenant_id`: null to that UUID. Outside objects
   remain global, including shared product-catalog and fulfillment instances.
3. Only explicitly reviewed legacy assertion lineages: `is_deleted` false to
   true, after successful native canonical attachment in the caller's body.
4. Native audit/timestamp changes, canonical XRF objects/lineages from the owning
   native API, and one new scope-assignment receipt with receipt-to-source links.

No existing EUID, uid, machine UUID, template, prefix assignment, allocator
configuration, source identity key/claim, relationship endpoint/type, source
payload or unrelated row is rewritten. Native allocation of new XRF/evidence
objects can advance existing sequences normally. A rollback does not promise
gapless allocation. Existing inactive lineages remain unchanged.

The lineage trigger's additional path only handles UPDATE by the authenticated
operator with an exact transaction-local manifest. It binds database/schema,
owner/domain, operator, actor, config path, backend PID, full transaction ID,
operation and plan hash. Each admitted old/new lineage transition binds row
identity, endpoint EUIDs/scopes, old protected hash, new tenant and retirement
flag; it is consumed once. It never grants INSERT permission or a blanket
operator scope exception. A session GUC alone cannot authorize a runtime role.

## Prospective shared-catalog runtime links

The same candidate corrects a separate native gap exposed by prospective Atlas
OrderTest creation: a global ProductVersion parent links to an explicitly scoped
OrderTest child. The native factory previously copied the global parent's tenant
onto the lineage, and the single-tenant guard rejected the mixed endpoints.

For newly created generic lineage, the factory now uses the parent's tenant
when present, otherwise the child's tenant. This preserves direction and places
a global-to-private relationship in its private scope. Two global endpoints
remain global; two scoped endpoints retain the parent's tenant and continue to
require the existing database authorization. No tenant is inferred from metadata.

A separate trigger branch permits INSERT and UPDATE of ordinary domain lineage
between exactly one global endpoint and one authorized tenant endpoint, in
either direction. It requires a non-operator runtime role, native bound
`allow_global_rows=true`, the scoped tenant in its immutable allowed-tenant set,
and the lineage tenant equal to that scoped endpoint. The prior active-endpoint
RLS and owner/domain checks and lineage RLS WITH CHECK still run. This branch
excludes canonical external-reference endpoint types, preserving their existing
typed-global assertion rules. A client GUC cannot turn on the bound capability.

This is the newly supported single-tenant-plus-global case. The pre-existing
explicit multi-tenant branch is unchanged: it already permits edges among its
authorized visible scopes. The new branch does not admit two unequal scoped
tenants, unbound tenants, operators or hidden endpoints. It does not turn a
global-read-only single-tenant runtime into a global-domain linker. Generic
factory-created mixed edges now use the scoped tenant even for existing
multi-tenant callers; historical rows are not rewritten by installation.

The reviewed operator correction runs before this runtime branch and remains
UPDATE-only with exact manifest consumption. New receipt links have matching
tenants and native XRF writes retain their existing dedicated implementation.

Postvalidation preserves every historical row and neighbor. Added incident
lineages must be active native XRF assertions projected successfully by
`ExternalReferenceService`, or exact links from this operation's receipt to its
sources. Arbitrary extra callback edges fail. The owning conversion still owns
the precise reviewed assertion payload/count: native structural validation does
not prove business-level completeness.

## Failure and deployed validation

Any stale preview, identity claim, contract mismatch, timeout, unconsumed
transition, malformed native assertion or changed protected row aborts the
correction savepoint. The caller should propagate failure from its outer
transaction. There is no best-effort partial correction. After an uncertain
commit result or failed local receipt export, run status with the same exact
plan/hash. Matching durable evidence plus exact old-cohort/XRF readback reports
`committed`; exact original state without evidence reports `not_applied`;
missing evidence with changed state or later protected-row drift fails for
reconciliation. Do not replay a composed mutation body after a committed status.

There is no automatic inverse operation after commit. Reassignment or undo is a
separately reviewed operation. Physical migration preservation checks remain
unchanged: neither native tenant column is made mutable by schema migration.

The single critical deployed check is the reviewed Atlas transaction and its
immediate native readback: 96 corrected sources, all 345 measured incident
lineages scoped exactly, shared catalog/fulfillment objects still global,
reviewed canonical assertion count/provenance and legacy retirement, unchanged
historical identities/payload hashes, audit and durable receipt, then a native
single-tenant-plus-global DAG read. Live preview must confirm those retained
counts before applying. The explicit graph helper includes global only when
`allow_global_claims` is true; no dummy extra tenant is configured.

The deployed gate must additionally prove one controlled prospective
ProductVersion→OrderTest native lineage INSERT and a subsequent ordinary UPDATE
under Atlas's exact bound single-tenant/global runtime. Its lineage must have the
scoped child's tenant and its catalog parent must remain global. Focused deployed
guard checks must retain denial for an unbound tenant, a global-denied
single-tenant role and a wrongly global lineage; pure source checks do not prove
PostgreSQL execution or those negative cases.

Minimal pure contract checks precede release. PostgreSQL execution, lock,
rollback and native audit behavior remain deployed validation, not claimed
locally. At most one corrective rollout cycle is planned. Broad PostgreSQL
rehearsals and extensive suites are deferred to separately authorized later work.
