# TapDB 11 integrity implementation ledger

## Current continuation state

Updated: 2026-09-29. Objective: the approved TapDB 11 audit, attribution,
history/correction and external-reference release. Checkout
`/Users/jmajor/projects/mega_dayhoff/repos_work/tapdb-scoped-template-import-20260927`,
branch `codex/tapdb-integrity-11`, exact baseline 10.1.11
`8fb344ecac341dcf29fc84faf9d1e0bd0af2bd4c`. Baseline was clean and no concurrent
TapDB writer was found. The commit containing this ledger records the complete
source candidate; qualified source hashes are retained in evidence file 04.

Authority: the user's “PLEASE IMPLEMENT THIS PLAN” approves G1 source work and
the deferred issue. G2 is approved by the explicit reply “Authorize the isolated
qualification suite”: disposable PostgreSQL 16.13 including audit-denial and
adoption/restore; no production/AWS/Atlas replay/sibling services. G3 publication
remains a separate gate under that plan. No PR/merge or consumer rollout authority
is inferred. No container was built locally; PostgreSQL 16.13 was compiled as a
native qualification tool after verifying the upstream source checksum.

Completed: native implementation, consumer contract and candidate release notes;
all 47 requirements dispositioned; deferred privileged-administrator preservation
issue #117 filed. **38 focused checks passed** (4.37 s final run), covering audit
mutation denial and atomicity, identity overlap, context/pool isolation, MVCC
history, correction retries/stale endpoints, references/concurrency/scope,
Python/CLI/authenticated HTTP parity, exact 10.1.11 adoption, old-source restore,
and current-contract restore into a new history epoch. Three dependency
deprecation warnings remain. Source AST review and `git diff --check` passed.
No broad suite, CI campaign or production test was run.

Known gap: pre-existing standalone Cognito adapter imports do not match its
pinned SDK. No live identity-provider login was attempted. Qualified authenticated
HTTP uses the host-session integration; it is not proof of standalone login.
Consumer identity propagation, validators and rollout remain separately owned.
Full-row audit storage cost and Aurora/PG17 behavior are not qualified here.

Fixed boundaries: no EUID issuance/prefix/sequence redesign or new global
uniqueness guarantee; administrator namespace responsibility retained. No
independent immutable store, history repair, consumer code changes, deployment,
production mutation or intermediate image build. Old evidence remains intact.

Next executable action: obtain G3 publication approval, recheck `11.0.0` is
unused, then publish one immutable annotated numeric package release from this
branch. `11.0.0` was absent from remote tags at the final source review. No
package/tag has been published. Consumer adoption requires separate approval.

Evidence: `20260929_tapdb_integrity_11_evidence/03_final_focused_qualification.txt`
and `04_qualified_source_manifest.json`; 47-row requirements matrix;
`../tapdb-11-integrity.md` and `../tapdb-11-release-notes.md`.
Ledger counts: **12 SUCCESS, 1 NO_LONGER_NEEDED, 2 OPEN**.
All rows terminal: **no**. Source implementation and focused qualification:
**complete within the documented scope**. Release objective complete: **no**.
Production changed: **no**.

## Approved implementation plan

The controlling request is the full user-provided "TapDB 11: audit integrity, attributable history, and external-reference support" plan. The handoff requirements are `/Users/jmajor/projects/mega_dayhoff/repos_work/dayhoff-service-pins-20260924/docs/plans/20260929_tapdb_integrity_external_reference_requirements.md`. Apply the amendments in this ledger over the original requirements: no issuance redesign; privileged-administrator immutable preservation deferred to an issue. Initial 47-requirement mapping below is authoritative for scope.

### A. Identity-safe audit queries

Retain existing selectors, isolation, natural identity claims, locks, reference validation and recovery fences. Correct audit joins with record table, UID, EUID, domain and issuer; distinguish audit identity from changed-object identity. Historical descriptions use historical evidence; current descriptions are labeled current context.

### B. Append-only audit

Remove audit_log from ordinary writable grants. Runtime scoped reads only; deny direct INSERT/UPDATE/DELETE/TRUNCATE and hiding. Trigger append uses narrowly privileged non-login audit writer, pinned search path, explicit schema and caller-scope validation. Runtime cannot assume that role. Reject audit mutation in depth. Domain mutation and audit append are atomic. Preserve domain soft deletion and prior audit data. Update fresh schema, binding, effective grants, catalog validation and restores together. Do not repeat initialization that rewrites historical audit actors.

### C. Structured attribution and revisions

Version transaction-local context with human/service actor kind, issuer and opaque subject, executing service, actual database principal, scope, request and operation IDs, optional correction reason. No inferred old-actor conversion. Capture full before/after states, per-record monotonic revision, top-level transaction identity and governing template identity for future template/instance/lineage mutations. Preserve older formats as incomplete historical evidence. Extend paginated authorized audit queries. Do not decrypt stored secrets.

### D. History and correction

Native HistoryService exposes capture_boundary, object_at, graph_at, plan_correction and apply_correction. Use PostgreSQL snapshots and top-level transaction IDs scoped to database/history epoch; no timestamp/sequence-as-commit-order approximation, no durable search record, no transaction held while a human reviews. Corrections are new audited writes with deterministic locking, expected revisions, recomputed diff, owner validators and durable idempotency receipts. Missing owner validator prevents apply. Immutable identities/templates/reference targets stay unchanged. Existing data gets explicit fenced adoption baseline, never fabricated old history. Restore starts a new history epoch; never imply remote/physical rollback.

### E. External references

Extend the existing ExternalReferenceService with public independent register/resolve and supported annotations. Do not fabricate a source object or mutate canonical target coordinates. Typed annotation record is metadata, not another physical-object identity. Permit authorized typed reference endpoints and later opaque-to-owner association; preserve original reference EUID and upstream associations. Local operations never require a network owner call. Serialize competing associations without imposing Atlas cardinality on all reference use.

### F. Interfaces and adoption

Python domain services own behavior; thin CLI and authenticated API adapters expose it. No GUI-only business operations. Existing update/delete gain expected-revision support. Reads remain nonmutating; CRUD has no broker dependency. A reviewed native adoption command handles exact schema/roles/context/revisions/baseline through existing lifecycle/fencing. Old consumers fail with explicit contract mismatch, not degraded attribution. Consumer changes and deployment stay outside this task.

## Gates and ledger

G0 baseline/scope; G1 source implementation (approved); G2 isolated qualification (approved); G3 publication (later gate); live consumer adoption/deployment separately scoped.

| ID | Area / requirement | Status | Category | Gate | Owner | Evidence | Root cause | Terminal note |
|---|---|---|---|---|---|---|---|---|
| P00 | Freeze source; OPS-01 | SUCCESS | plan_amendment | G0 | Lead | clean 10.1.11; remote refs; thread inventory | | Source baseline only; no production assertions. |
| P01 | EUID issuance; ID-02–03 | NO_LONGER_NEEDED | plan_amendment | G0 | Lead | explicit user decision in planning | | Administrator responsibility; no issuing changes. |
| P02 | Typed audit lookup; ID-01, preservation ID-04–06 | SUCCESS | feature_implementation | G1 | Lead | audit.py; focused UID-overlap, exact-source adoption and restore | UID-only joins corrected | Existing issuance retained; no fleet collision audit. |
| P03 | Append-only audit; AUD-01–05,07 | SUCCESS | config_or_startup_contract | G1 | Lead | audit_storage.py; RLS/catalog/grants; focused privilege/atomicity checks | audit_log was runtime writable | Runtime append-only contract qualified; privileged operators excluded. |
| P04 | ATTR-01–06 | SUCCESS | active_product_contract | G1 | Lead | security_context.py; pool/host/API actor qualification | single legacy actor string | Structured prospective context mandatory; consumer adoption separate. |
| P05 | Revisions; AUD-06, HIST-01,07 | SUCCESS | feature_implementation | G1 | Lead | schema revision/audit triggers; exact-source baseline qualification | incomplete initial state | Full prospective states; older evidence remains explicitly incomplete. |
| P06 | HIST-02–06 | SUCCESS | feature_implementation | G1 | Lead | history.py; MVCC/correction/cycles/stale-endpoint/retry checks | no native selective correction | Read-only boundaries and guarded forward correction; owner validators required. |
| P07 | XRF-01–05,08 | SUCCESS | feature_implementation | G1 | Lead | external_references.py; concurrent independent registration/lookup checks | source-bound public API | Independent canonical references; no owner network call. |
| P08 | XRF-06–07, CORE-02 | SUCCESS | feature_implementation | G1 | Lead | annotation template/DB guards; CAS/binding/concurrency checks | canonical target is not annotation storage | Typed annotations and associations; consumer cardinality remains consumer owned. |
| P09 | XRF-09, CORE-01,03–05 | SUCCESS | active_product_contract | G1 | Lead | shared dispatcher; actual CLI and host-authenticated HTTP qualification | new contracts required thin adapters | Native parity qualified; pre-existing standalone Cognito gap disclosed. |
| P10 | OPS-02–05 | SUCCESS | config_or_startup_contract | G1 | Lead | native fenced adoption; exact 10.1.11/current restore checks | new contract required explicit adoption | No silent old-schema upgrade; restore epochs/floors/admission preserved. |
| P11 | Focused acceptance | SUCCESS | contract_test | G2 | Lead | 03_final_focused_qualification.txt; 04 source hashes | G2 approved by user | 38 passed; 3 dependency deprecation warnings; isolated PG16.13 only. |
| P12 | Major release; OPS-06 | OPEN | plan_amendment | G3 | Lead | tapdb-11-release-notes.md; remote 11.0.0 absent at review | G3 publication pending | No tag/package publication or consumer deployment. |
| P13 | Deferred AUD-08 issue | SUCCESS | plan_amendment | G1 | Lead | https://github.com/Daylily-Informatics/daylily-tapdb/issues/117 | excluded privileged-admin infrastructure | Issue filed; no immutable infrastructure implemented. |
| P14 | Closure matrix/status | OPEN | plan_amendment | G3 | Lead | 47-row matrix and this continuation state | Release gate still pending | Source and qualification complete; final release closure remains. |

The original 47 IDs map to P01–P10/P12/P13. ID-04–06 mean preservation and qualification of existing behavior only, not allocator redesign. AUD-08 means documented trust boundary and separate issue only. Consumer propagation/operations require owning-service evidence; cannot be claimed complete from TapDB fixtures.

## Focused acceptance to prepare

Isolated PostgreSQL 16.13 only: negative audit privileges including inheritance/routine misuse; overlapping numeric UIDs; pooled/delegated attribution; full revisions and snapshot visibility with abort/savepoints/concurrent commits; read-only history and correction preview; stale/duplicate correction and owner-validator rejection; offline references and tenant/global boundaries; adoption/restore equivalence; Python/CLI/API parity. No production resources, AWS mutations, broad Atlas replay or sibling-suite campaign. G2 authorization received; run only this focused scope.

## Release and completion

One immutable annotated numeric major release, 11.0.0 only if still unused. Preserve main; no PR/merge automatically. Stop at consumer handoff, not deployment. Report local source, qualification, publication, consumer adoption and production separately; all-rows-terminal and objective-complete are distinct. Existing source tests are not production evidence.

## Initial qualification evidence

`20260929_tapdb_integrity_11_evidence/01_audit_revision_qualification.txt`: 6 passed with PostgreSQL 16.13 (native `/tmp/tapdb11-pg1613/bin`) on disposable fixture. Full row revisions and attribution, direct runtime INSERT/UPDATE/DELETE/TRUNCATE denial, missing-attribution domain-write denial. Additional negative role/catalog checks, history, reference/adoption/restore and interfaces remain unverified. Isolated environment initially installed SQLAlchemy 2.1, which changed the implicit driver and exposed existing parameterized SET incompatibility during seed; restored SQLAlchemy 2.0 in this venv only (no package dependency change).

## Final local qualification and source disposition

The initial six- and eighteen-check receipts above are historical checkpoints,
not current remaining-work lists. Final evidence is file 03 (38 passes). The
initial SQLAlchemy 2.0 venv-only choice was superseded by the source dependency
constraint `>=2.0,<2.1`; 2.1's implicit driver change is outside the qualified
runtime. The two added templates use existing reserved prefixes; no allocator
redesign or existing template overwrite was introduced.

Final review added endpoint-revision preconditions to lineage correction:
changing an endpoint after preview now rejects apply even when the edge revision
is unchanged. Exact-source restore was also qualified: old-schema restore keeps
its original contract and reports explicit adoption required; it does not try
to install new audit-writer routines against missing history tables.

Native writers reviewed: CLI root context, object mutation adapters, generic
GUI/API mutations, native user provisioning/login metadata, bootstrap seed,
reference operations and lifecycle operations. All rely on mandatory database
audit context. Direct owner/superuser intervention remains outside runtime
assurance. The standalone Cognito import mismatch is a pre-existing login
integration gap, explicitly excluded from the authenticated host-session proof.
No broad identity-provider refactor or live provider action was undertaken.

Qualification resources were disposable native PostgreSQL fixture databases;
the fixture stopped its server at teardown. Prior records, services and AWS
resources were not touched. No test-generated object identities are claimed as
production objects. The final source manifest records content hashes for the
changed/new implementation and focused fixtures, including exact 10.1.11 assets.
