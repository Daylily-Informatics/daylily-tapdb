# TapDB 10.0.0 Canonical External References and Federation Execution Ledger

Created: 2026-09-04T06:18:19Z
Controlling plan: user-approved **TapDB 10.0.0 Canonical External References and Federated DAG Plan** in the implementation thread
Ledger path: `docs/plans/20260904T061819Z_tapdb_10_external_references_federation_ledger.md`

## Scope and authority

This ledger controls the TapDB-only implementation, validation, pull request,
release, and publication work for 10.0.0. Atlas, Bloom, Ursa, Dewey, Kahlo,
deployed databases, and production services are read-only reference surfaces and
must not be changed by this work.

The release is an intentional hard contract cut: DAG v1, URL-bearing reference
routing, metadata-derived pseudo-edges, admin proxy endpoints, and the embedded
GUI external-link writer are removed without compatibility aliases. Existing
database identities are immutable: no existing UID, EUID, identity key, sequence
value, template identity, source instance, XRF, or lineage may be regenerated or
rewritten by the migration.

The user additionally required one web implementation, with no loss of useful
admin behavior. `daylily_tapdb.gui` is therefore the canonical stack. Before
`admin.main` is removed, route and browser contracts must prove parity for
authentication, search/browse, templates, instances, lineage, validation,
runtime/readiness/inventory, metrics, backups, and DAG-v2 visualization.

## Gate 0 inventory freeze

- Repository: `/Users/jmajor/projects/mega_dayhoff/repos_work/daylily-tapdb-9.2.0`
- Branch: `codex/tapdb-10-external-references`, created directly from immutable
  annotated tag `9.2.2`.
- Branch point: `be0e4b063f78f63f43d21c37070ac61a8450c619`.
- Initial status: clean (`git status --short --branch` reported only the branch
  header before ledger creation).
- GitHub baseline: `main` is the default branch; no open pull requests existed at
  inventory time; `origin/main` equalled the peeled `9.2.2` commit.
- Baseline inventory: 359 tracked paths; 9 shipped migration files; 6 core-config
  paths. CI already runs the identical full suite on `postgres:16.13` and
  `postgres:17`, with branch coverage and changed-module coverage gates at 90%.
- Current public/debt surfaces found by the inventory sweep:
  `daylily_tapdb.services.external_refs`, `daylily_tapdb.web.dag`, v1 exports in
  `daylily_tapdb.services` and `daylily_tapdb.web`, automatic v1 router mounting,
  admin v1 proxy routes, legacy graph payload builders, and embedded-GUI external
  link GET/HTML POST/JSON POST routes.
- Current identity surface: one global-only natural-identity check and one partial
  unique index; `InstanceFactory.claim_instance_by_identity` always persists a
  null tenant and records only claimant tenancy in evidence.
- Current XRF surface: one URL-bearing TapDB-object template and a mixed module
  containing typed helpers plus DAG-v1 parsing/network proxy behavior.
- Consumer references (read-only): Atlas `7.0.2` / `52e258e5c60f`, Bloom `8.0.0`
  / `856f92186a04`, Ursa `11.0.50` / `2d04a0a79bfe`, Dewey `7.0.2` /
  `695de64f2ca5`, and Kahlo `7.0.7` / `63429c1f67a0`.
- Inventory sweeps: `rg --files`; targeted `rg` for DAG-v1, XRF, GUI writer,
  natural identity, CI images/version, and public exports; `gh repo view`; `gh pr
  list`; `git ls-tree`; direct inspection of the annotated tag and branch point.
- Baseline tests: `source ./activate && python -m pytest tests -q` -> 2,215
  passed, 14 skipped in 113.17 seconds on the immutable branch point. The skips
  are the suite's pre-existing environment-gated integration cases.
- Live limits: no consumer deployment, database conversion, Aurora operation, or
  production inspection is authorized. Federation tests use fake transports.

## Control ledger

| ID | Area | Requirement / acceptance surface | Status | Category | Approval gate | Owner | Evidence | Root cause | Terminal note |
|---|---|---|---|---|---|---|---|---|---|
| `GATE-0` | Baseline | Verify immutable branch point, consumer references, exports, schemas, migrations, CI, clean state, and baseline tests before deletion | SUCCESS | contract_test | Gate 0 | primary agent | Annotated `9.2.2` peels to `be0e4b063f78`; `main` is default and equal; zero open PRs; all five consumer tags were verified locally and against their remotes; baseline `pytest tests -q` -> 2,215 passed, 14 pre-existing environment skips |  | Inventory frozen before runtime deletion; consumer repositories remain read-only |
| `ID-001` | Identity | Add tenant/global natural-identity migration and explicit scope API; prove immutable before/after state and tenant concurrency | SUCCESS | feature_implementation | Gate 1 | primary agent | Append-only migration, base schema, ORM partial indexes, explicit `IdentityScope`, schema/migration contract tests, migration snapshot tests, and PostgreSQL concurrency tests are green in the 2,161-test local acceptance run | Prior claimant tenancy was evidence only and the uniqueness index was global-only | Migration contains no data rewrite; no UID, EUID, identity key, sequence, template, object, XRF, or lineage is regenerated |
| `XRF-001` | XRF model | Define exact TapDB-object and opaque XRF templates, strict validation, natural keys, and guarded sole write path | SUCCESS | feature_implementation | Gate 1 | primary agent | Exact core template JSON, strict typed targets, canonical validation, SHA-256 opaque identity coordinates, guarded factory/update/delete paths, and focused tests are present; changed-module coverage is 90.85% for the lifecycle module | The old XRF shape mixed target identity with URL/auth routing and permitted generic writes | Two exact XRF templates and the canonical service are now the only supported model |
| `LIFE-001` | Lifecycle | Implement attach/reuse/detach/reactivation/reverse lookup/authority-scoped reconcile without transaction ownership | SUCCESS | feature_implementation | Gate 1 | primary agent | Attach/replay/shared-target/detach/reactivation/reverse lookup/reconcile/conflict/rollback/RLS/concurrency cases pass in `tests/test_external_references_v10.py` and the complete suite | Applications previously implemented incompatible replacement and ownership behavior | Lifecycle preserves caller transaction ownership and reuses persisted XRF and lineage identities |
| `DAG-001` | DAG v2 | Add typed projections, exact external-reference search, manifest features, and opaque non-expansion | SUCCESS | feature_implementation | Gate 3 | primary agent | DAG-v2 contract, graph payload, exact-filter, RLS, manifest, rejection, and GUI projection tests pass; `web/dag_v2.py` coverage is 96.16% | Discovery previously depended on copied metadata and DAG-v1 URL routing | DAG v2 now projects canonical references and exact reverse-search matches; opaque identifiers remain non-expandable |
| `FED-001` | Federation | Add reusable exact-service federated search, owner resolution, and bounded global graph composition | SUCCESS | feature_implementation | Gate 1 | primary agent | Fake-transport tests cover parallel search, exact owner lookup, bridging, cycles, collision safety, bounds, deadline/failure receipts, and opaque non-expansion; module coverage is 95.87% | Kahlo had to infer ownership and merge graphs independently | Core client composes admitted exact DAG-v2 targets without owning credentials, discovery, retries, aliases, or UI |
| `CUT-001` | Breaking cut | Remove DAG v1, proxy/network helpers, legacy payloads, metadata pseudo-edges, and GUI writer; converge standalone and embedded web operation on feature-complete `daylily_tapdb.gui`; add negative and parity contracts | SUCCESS | removable_compatibility_debt | Gate 3 | primary agent | `admin.main`, its templates/static app, DAG-v1 modules, proxy routes, legacy factory, legacy payload code, and duplicate GUI writer are absent; explicit route/behavior parity tests, 19 GUI parity tests, embedded tests, and real Chromium tests pass; GUI router coverage is 93.30% | Two web stacks duplicated features and allowed divergent graph/reference writes | `daylily_tapdb.gui` is the sole complete stack; only deliberately retired DAG-v1/proxy and duplicate writer behavior is excluded |
| `AUDIT-001` | Validation | Add read-only JSON `tapdb validation external-references` audit with redacted samples and nonzero violation exit | SUCCESS | feature_implementation | Gate 1 | primary agent | Canonical/malformed/legacy/mixed/redaction/no-mutation/exit-code tests pass; installed-wheel CLI help smoke passes; module coverage is 94.04% | No bounded way existed to inventory incompatible application-owned shapes before migration | Audit is read-only, emits bounded EUID-only samples, redacts identifier values, and fails nonzero on violations |
| `TEST-001` | Acceptance | Pass PostgreSQL 16.13/17, migration, RLS, concurrency, browser, security, coverage, build, wheel, and docs gates | IN_PROGRESS | contract_test | Gate 5 | primary agent | Local complete suite: 2,161 passed, 12 environment-only skips, 95.30% branch coverage; all 31 changed production modules are at least 90%; Ruff, format, mypy, Bandit, verified-secret scan, lock check, build, wheel-asset check, `twine check`, and isolated installed-wheel API/GUI/CLI smoke pass |  | PostgreSQL 16.13 and 17 CI receipts remain required before merge |
| `DOC-001` | Documentation | Overhaul README and active docs; add comprehensive API/federation and tagged-consumer migration guidance | SUCCESS | feature_implementation | Gate 5 | primary agent | README and active architecture/DAG/runtime/GUI/consumer docs were rewritten; new comprehensive external-reference/federation guide and tagged Atlas/Bloom/Ursa/Dewey/Kahlo migration mapping are tested; executable README examples pass | Active docs described multiple generations of graph and admin behavior | Docs now present one canonical GUI, DAG v2, exact discoverability, federation, operator audit, and application ownership boundaries |
| `REL-001` | Release | Green PR/main CI, merge, exact artifacts, annotated `10.0.0`, GitHub Release, PyPI, hashes, and clean synchronized main | IN_PROGRESS | feature_implementation | Gate 5 | primary agent | Candidate `10.0.0` wheel/sdist build, exact Meridian pin check, `twine check`, and clean external-venv smoke pass; CI now enforces these artifact gates |  | PR, dual-PostgreSQL CI, merge, fresh main CI, immutable tag, GitHub Release, PyPI publication, and final hashes remain |

## Execution receipts

Receipts are appended as work proceeds. A row is terminal only when its evidence
and terminal note are recorded in the table.

- 2026-09-04 Gate 0 consumer receipts: Atlas `7.0.2` peels to
  `52e258e5c60f57ae1e90b895364166cd3b358faa`; Bloom `8.0.0` to
  `856f92186a04ba91cf1ba3d825d0522d9ca97c32`; Ursa `11.0.50` to
  `2d04a0a79bfec58d6d949ac669687b83eeb8c50a`; Dewey `7.0.2` to
  `695de64f2ca5fe4f4a5416d1a731e3989ee4ba56`; Kahlo `7.0.7` to
  `63429c1f67a0d27604ef87b97c46cc8527e99ffd`.
- 2026-09-04 Gate 0 dependency receipts: tagged Atlas and Bloom pin TapDB
  `9.0.10`; tagged Ursa, Dewey, and Kahlo pin TapDB `9.0.9`.
- 2026-09-04 web-convergence inventory: `daylily_tapdb.gui` already covers
  search/browse, template authoring and repository packs, object creation and
  governed mutation, lineage, validation/repair, readiness, Meridian, metrics,
  runtime information, backup/recovery, and graph rendering. The parity gate
  identified standalone Cognito/session routes, operator inventory, audit
  browsing, and help as the capabilities to carry forward before deleting the
  duplicate `admin.main` application.
- 2026-09-04 implementation receipt: tenant/global identity scoping, both exact
  XRF templates, the canonical lifecycle API, DAG-v2 discovery/search,
  reusable federation, the read-only audit, and the hard legacy cut are
  implemented without modifying existing migration files.
- 2026-09-04 GUI parity receipt: canonical `daylily_tapdb.gui` carries forward
  standalone and embedded auth, overview, independent name/EUID plus combined
  search, templates/repository packs, instances, lineages, audit, object
  mutation/repair/validation, inventory, readiness, Meridian, metrics, runtime,
  backups/restores, rich graph exploration, download, and administrator graph
  mutations. Forward cursor pagination was restored during the parity audit.
- 2026-09-04 local acceptance receipt: `TAPDB_RUN_DOCS_LOCAL=1 python -m
  pytest tests -q --cov=daylily_tapdb --cov=admin --cov-branch
  --cov-report=json:coverage.json --cov-fail-under=90` with the activated
  environment returned 2,161 passed, 12 environment-only skips, and 95.30%
  branch coverage in 149.74 seconds. The changed-module verifier passed all 31
  changed production modules at 90% or higher.
- 2026-09-04 quality/artifact receipt: Ruff check and format, mypy (14 strict
  source files), Bandit, verified detect-secrets, `uv lock --check`, and `git
  diff --check` passed. Candidate wheel/sdist build, wheel asset and exact
  `meridian-euid==0.4.8` verification, `twine check`, and a fresh external-venv
  API/GUI/CLI smoke all passed.

## Final report

All rows terminal: no
Objective complete: no

Status counts:

- SUCCESS: 9
- DUPLICATE: 0
- NO_LONGER_NEEDED: 0
- FAIL: 0
- BLOCKED: 0
- IN_PROGRESS: 2
- OPEN: 0
