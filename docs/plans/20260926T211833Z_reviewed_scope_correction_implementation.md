# Native reviewed scope correction — source candidate

## Current continuation state

**Objective:** prepare the smallest native administrative operation for the
reviewed Atlas cohort while preserving shared global neighbors, native identity,
allocator authority and physical-migration preservation. Source preparation is
complete for root review; production execution is not claimed.

**Identity:** worktree `tapdb-reviewed-scope-correction-20260926`, branch
`codex/tapdb-reviewed-scope-correction-20260926`; The release base and exact annotated tag
`10.1.6` resolve to `c799679e4201d36e92b7aef097764c2510f0fabe`.
Root is freezing this reviewed candidate as annotated10.1.7 after the user's explicit proceed instruction. The release tag will identify its exact commit; service pins follow that commit.

**Authority:** root delegated native source preparation following the user's
explicit TapDB-revision instruction. The user subsequently instructed the coordinator to proceed with the scoped release/deployment. Root owns tag/publication/operator execution. This artifact records evidence rather than granting authority. PRs, merges, full suites, replacement databases and destructive tests remain outside scope.

**Evidence and bounds:** retained Atlas `x0-atlas-scope-closure.json` identifies
96 active, native-global, unclaimed sources and 345 active incident lineages.
There are 32 global shared-catalog `ordered_as` parents, 64 links to outside
fulfillment instances and 185 legacy external-object assertions; outside objects
stay global. Root verified the owning external-client configuration explicitly
maps the observed client to zero UUID; no metadata-derived scope inference.
Fresh native preview must reconfirm the complete inventory and authority receipts.

**Completed:** native preview/apply/status and caller-owned transactional
composition; function-only SQL upgrade; exact consumed operator transition
manifest; truthful native scope-assignment evidence; explicit single-tenant plus
global graph helper; operator documentation. Root's missing `child_euid` INTO
finding is fixed in both SQL copies. Added edges are now restricted by native
XRF projection or the operation's exact receipt links. Existing public
`soft_delete_object` changes only `is_deleted` and is the documented retirement API.

Subsequent bounded review confirmed a prospective native defect: Atlas's new
scoped OrderTests cannot link from global ProductVersions. The candidate now
also assigns new generic mixed-scope lineage to its scoped endpoint and permits
ordinary domain INSERT/UPDATE under an explicitly bound runtime tenant/global
grant, symmetrically. Canonical XRF endpoint types retain their dedicated rules.
The existing multi-tenant branch and operator manifest stay separate. This
is included in the presented **10.1.7 amendment scope**. The coordinator's extra numeric approval gate was withdrawn; existing implementation authority and the later explicit proceed cover this release.

**Validation:** latest 16 pure focused checks passed in 0.21 seconds, with
conftest and default pytest options disabled (the prior 10 cases plus five
factory-scope cases and one static guard-contract check). No database, SQL
execution, container, installation or broad suite ran.
PostgreSQL trigger/locking/savepoint/audit behavior is therefore not validated.

**Next:** root publishes the reviewed exact10.1.7 tag and verifies package hashes. On authorized deployment, require successful physical migration and
template provisioning before preview. The critical deployed check is exact
correction plus canonical-XRF conversion, immediate durable status/readback and
single-tenant-plus-global DAG. The owning conversion command still supplies and
verifies the reviewed assertion payload/count/provenance. One corrective rollout
cycle; extensive suites deferred to separately authorized future work.

## Exact changed files

| File | Effect |
| --- | --- |
| `daylily_tapdb/services/scope_corrections.py` | Public bounded native preview, status, scope-only apply, composable savepoint transaction and evidence export |
| `daylily_tapdb/factory/instance.py` | New global-parent/scoped-child generic lineage takes the child's scope; all other parent-scoped choices preserved |
| `daylily_tapdb/cli/objects.py` | `objects scope-correct preview/apply/status`; operator connection; commit before success report |
| `daylily_tapdb/core_config/governance/governance.json` | New native `evidence/repair/scope_assignment/1.0/`, existing GVR prefix |
| `daylily_tapdb/security_context.py` | Explicit single-tenant `allow_global_claims=True` graph scope includes global |
| `schema/rls.sql` | Exact operator UPDATE transition path and distinct bound-runtime shared-domain INSERT/UPDATE branch inside existing lineage scope function |
| `schema/migrations/20260926_220000_reviewed_scope_correction.sql` | Same function body and pinned search path; no data/table/trigger/allocator migration |
| `tests/test_reviewed_scope_correction_contract.py` | Sixteen pure cases: original correction checks plus five factory scope cases and static shared-domain guard contract |
| `docs/reviewed_scope_correction.md` | Request, preconditions, native composition, exact effects, recovery and deployed validation |
| `docs/architecture/evidence_vs_governance.md` | Explicit distinction from validator repair and physical migration |
| This report | Source-state handoff; not execution permission |

## SQL and identity effect declaration

Installation replaces only `tapdb_validate_lineage_endpoint_scope()` and pins
its native search path. It adds no persistent authorization table, policy,
trigger, allocator, relation or data-update statement. Migration immutable-column
sets remain unchanged, including both existing native `tenant_id` columns.

The separate native operator operation changes the selected instance tenants and
active incident-lineage tenants, then optionally retires only approved legacy
assertions through the existing native API. The trigger consumes exact OLD/NEW
row instructions bound to the current operator, configured target, actor,
backend and transaction. Other operator traffic and all INSERTs retain native
scope rules. Runtime sessions cannot gain this operator exception by setting a GUC.

Separately, the runtime shared-domain branch requires exactly one global
endpoint, explicitly allowed global rows, an authorized scoped endpoint, and
NEW lineage tenant equal to that endpoint. It applies symmetrically to INSERT
and UPDATE after active-endpoint RLS/owner/domain checks; canonical XRF types
are excluded. Existing same-tenant and explicit multi-tenant rules remain. This
resolves a demonstrated new-OrderTest defect without a synthetic second tenant
or application SQL. The factory scope change affects new lineages only.

Existing EUIDs, uid, machine UUIDs, identity claims, templates, endpoint identity,
relationship types, payloads and allocator configuration are preserved. New
native XRF/evidence objects and links use ordinary allocation. Audit and modified
timestamps record the correction; sequences may advance even on rollback.

## Limits that root must retain

- This is a feasible source design for the measured 96-source/global-catalog
  boundary, not successful deployed correction evidence.
- The receipt and authority digests are explicit reviewed inputs; the library
  does not authenticate arbitrary external receipt formats or grant approval.
- Exact native evidence template must already be provisioned. Preview fails if
  the installed trigger differs from packaged authority or the template is absent.
- The composable native context owns a savepoint; the caller owns the outer
  transaction and business-level XRF conversion. Scope-only CLI apply refuses a
  nonempty retirement list. No unsupported caller field assignment is needed.
- Status compares retained cohort and converted-XRF state exactly. Subsequent
  legitimate protected-row changes can require reconciliation; status is an
  outcome-resolution receipt, not an evergreen application health endpoint.
- Shared global adjacency needs the changed graph helper in Atlas's runtime
  package as well as the operator package. Operator-only installation cannot
  supply the runtime helper fix. Canonical wire formats remain unchanged.
- Runtime shared-catalog INSERT and UPDATE must be validated on deployed
  PostgreSQL with the exact bound Atlas role. Pure factory/static guard checks
  cannot establish SQL execution, hidden-endpoint denial, global-denial or
  cross-tenant isolation. This is a distinct acceptance requirement from the
  historical 96-source operator correction.
- Scope correction preserves existing graph topology and does not promise new
  universal domain/storage acyclicity. Existing native DAG limitations remain.

## Bounded checks run

Latest after the shared-domain amendment: the same single-file command below
reported **16 passed in 0.21s**. The static guard check inspects the native SQL
branch; it does not simulate or claim a PostgreSQL test. The prior run is retained
below as historical evidence.

```text
/Users/jmajor/miniconda3/envs/URSA-tap101/bin/python -m pytest -q --noconftest -o addopts='' tests/test_reviewed_scope_correction_contract.py
10 passed in 0.19s

/Users/jmajor/miniconda3/envs/URSA-tap101/bin/python -m py_compile daylily_tapdb/services/scope_corrections.py daylily_tapdb/cli/objects.py daylily_tapdb/security_context.py tests/test_reviewed_scope_correction_contract.py
exit 0

git diff --check
exit 0
```
