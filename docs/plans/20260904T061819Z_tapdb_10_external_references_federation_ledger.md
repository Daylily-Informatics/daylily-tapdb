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
| `ID-001` | Identity | Add tenant/global natural-identity migration and explicit scope API; prove immutable before/after state and tenant concurrency | SUCCESS | feature_implementation | Gate 1 | primary agent | Append-only migration, base schema, ORM partial indexes, explicit `IdentityScope`, schema/migration contract tests, migration snapshot tests, and PostgreSQL concurrency tests are green in the 2,163-test local acceptance run | Prior claimant tenancy was evidence only and the uniqueness index was global-only | Migration contains no data rewrite; no UID, EUID, identity key, sequence, template, object, XRF, or lineage is regenerated |
| `XRF-001` | XRF model | Define exact TapDB-object and opaque XRF templates, strict validation, natural keys, and guarded sole write path | SUCCESS | feature_implementation | Gate 1 | primary agent | Exact core template JSON, strict typed targets, canonical validation, SHA-256 opaque identity coordinates, guarded factory/update/delete paths, and focused tests are present; changed-module coverage is 90.85% for the lifecycle module | The old XRF shape mixed target identity with URL/auth routing and permitted generic writes | Two exact XRF templates and the canonical service are now the only supported model |
| `LIFE-001` | Lifecycle | Implement attach/reuse/detach/reactivation/reverse lookup/authority-scoped reconcile without transaction ownership | SUCCESS | feature_implementation | Gate 1 | primary agent | Attach/replay/shared-target/detach/reactivation/reverse lookup/reconcile/conflict/rollback/RLS/concurrency cases pass in `tests/test_external_references_v10.py` and the complete suite | Applications previously implemented incompatible replacement and ownership behavior | Lifecycle preserves caller transaction ownership and reuses persisted XRF and lineage identities |
| `DAG-001` | DAG v2 | Add typed projections, exact external-reference search, manifest features, and opaque non-expansion | SUCCESS | feature_implementation | Gate 3 | primary agent | DAG-v2 contract, graph payload, exact-filter, RLS, manifest, rejection, and GUI projection tests pass; `web/dag_v2.py` coverage is 96.16% | Discovery previously depended on copied metadata and DAG-v1 URL routing | DAG v2 now projects canonical references and exact reverse-search matches; opaque identifiers remain non-expandable |
| `FED-001` | Federation | Add reusable exact-service federated search, owner resolution, and bounded global graph composition | SUCCESS | feature_implementation | Gate 1 | primary agent | Fake-transport tests cover parallel search, exact owner lookup, bridging, cycles, collision safety, reserved-field rejection, bounds, deadline/failure receipts, and opaque non-expansion; module coverage is 95.93% | Kahlo had to infer ownership and merge graphs independently | Core client composes admitted exact DAG-v2 targets without owning credentials, discovery, retries, aliases, or UI |
| `CUT-001` | Breaking cut | Remove DAG v1, proxy/network helpers, legacy payloads, metadata pseudo-edges, and GUI writer; converge standalone and embedded web operation on feature-complete `daylily_tapdb.gui`; add negative and parity contracts | SUCCESS | removable_compatibility_debt | Gate 3 | primary agent | `admin.main`, its templates/static app, DAG-v1 modules, proxy routes, legacy factory, legacy payload code, and duplicate GUI writer are absent; explicit route/behavior parity tests, 19 GUI parity tests, embedded tests, and real Chromium tests pass; GUI router coverage is 93.30% | Two web stacks duplicated features and allowed divergent graph/reference writes | `daylily_tapdb.gui` is the sole complete stack; only deliberately retired DAG-v1/proxy and duplicate writer behavior is excluded |
| `AUDIT-001` | Validation | Add read-only JSON `tapdb validation external-references` audit with redacted samples and nonzero violation exit | SUCCESS | feature_implementation | Gate 1 | primary agent | Canonical/malformed/legacy/mixed/redaction/no-mutation/exit-code tests pass; installed-wheel CLI help smoke passes; module coverage is 94.04% | No bounded way existed to inventory incompatible application-owned shapes before migration | Audit is read-only, emits bounded EUID-only samples, redacts identifier values, and fails nonzero on violations |
| `TEST-001` | Acceptance | Pass PostgreSQL 16.13/17, migration, RLS, concurrency, browser, security, coverage, build, wheel, and docs gates | SUCCESS | contract_test | Gate 5 | primary agent | Review-remediated local suite: 2,163 passed with 95.32% branch coverage; remediated PR run `33858301129`: 2,177 passed with zero skips and 95.37% on both PostgreSQL 16.13 and 17.11; all 31 changed modules >=90%; quality, security, docs, build, assets, and installed-wheel smoke passed | Automated review found three bounded contract defects after the first green run | Exact remediated code head passed every dual-PostgreSQL and global gate without deselection |
| `DOC-001` | Documentation | Overhaul README and active docs; add comprehensive API/federation and tagged-consumer migration guidance | SUCCESS | feature_implementation | Gate 5 | primary agent | README and active architecture/DAG/runtime/GUI/consumer docs were rewritten; new comprehensive external-reference/federation guide and tagged Atlas/Bloom/Ursa/Dewey/Kahlo migration mapping are tested; executable README examples pass | Active docs described multiple generations of graph and admin behavior | Docs now present one canonical GUI, DAG v2, exact discoverability, federation, operator audit, and application ownership boundaries |
| `REL-001` | Release | Green PR/main CI, merge, exact artifacts, annotated `10.0.0`, GitHub Release, PyPI, hashes, and clean synchronized main | SUCCESS | feature_implementation | Gate 5 | primary agent | PR #104 merged to `main` as `eadef9e8426ee968fb850328c7a6e2f90353fe29`; fresh main run `33864847727` passed both database lanes and every global/artifact gate; annotated tag `10.0.0`, latest GitHub Release, PyPI wheel/sdist, matching SHA-256 digests, and a no-cache Python 3.12 published-wheel smoke are verified |  | The immutable release is complete; PyPI is the canonical artifact source, remote `main` contains the tagged commit, and no consumer service or deployed database was changed |

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
- 2026-09-04 local acceptance receipt: `python -m pytest tests -q
  --cov=daylily_tapdb --cov=admin --cov-branch
  --cov-report=json:coverage.json --cov-fail-under=90` with the activated
  environment returned 2,163 passed, 14 environment/platform skips, and 95.32%
  branch coverage in 141.79 seconds. `TAPDB_RUN_DOCS_LOCAL=1 python -m pytest
  -q tests/test_docs_examples.py` separately passed all three executable
  examples. The changed-module verifier passed all 31 changed production
  modules at 90% or higher.
- 2026-09-04 automated-review remediation receipt: three review findings were
  accepted and fixed. Tenant sources may now attach only validated
  `public_global` opaque XRF endpoints; rooted GUI graph requests enforce their
  accepted `max_edges`; and federation rejects noncanonical/reserved opaque
  projection fields before node construction. Five focused regressions,
  including real PostgreSQL and migration-from-`9.2.2` proofs, passed.
- 2026-09-04 remediated PR qualification receipt: [run
  `33858301129`](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33858301129)
  passed on exact code head `ebc6a506c6025eb1634551a7a9da3af6162f990b`.
  The [PostgreSQL 16.13 job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33858301129/job/100976771351)
  reported `PostgreSQL 16.13 (Debian 16.13-1.pgdg13+1)` and passed all 2,177
  tests with zero skips, 95.37% branch coverage, and all 31 changed modules at
  90% or higher in 7m01s. The [PostgreSQL 17 job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33858301129/job/100976771259)
  reported `PostgreSQL 17.11 (Debian 17.11-1.pgdg13+2)` and passed the same
  2,177 tests and coverage gates in 6m42s. The [artifact
  job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33858301129/job/100978634595)
  passed the `10.0.0` build, `twine check`, packaged-asset inspection, and
  isolated installed-wheel external-reference/federation/GUI/CLI smoke.
- 2026-09-04 quality/artifact receipt: Ruff check and format, mypy (14 strict
  source files), Bandit, verified detect-secrets, `uv lock --check`, and `git
  diff --check` passed. Candidate wheel/sdist build, wheel asset and exact
  `meridian-euid==0.4.8` verification, `twine check`, and a fresh external-venv
  API/GUI/CLI smoke all passed.
- 2026-09-04 PR qualification receipt: [PR #104](https://github.com/Daylily-Informatics/daylily-tapdb/pull/104)
  run `33856027045` passed. The [PostgreSQL 16.13 job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33856027045/job/100969592123)
  reported `PostgreSQL 16.13 (Debian 16.13-1.pgdg13+1)` and completed in
  6m32s. The [PostgreSQL 17 job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33856027045/job/100969592064)
  reported `PostgreSQL 17.11 (Debian 17.11-1.pgdg13+2)` and completed in
  5m37s. The [artifact job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33856027045/job/100971335833)
  passed build, `twine check`, packaged-asset validation, and isolated installed
  wheel API/GUI/CLI smoke.
- 2026-09-04 merge and fresh-main receipt: PR
  [#104](https://github.com/Daylily-Informatics/daylily-tapdb/pull/104) was merged
  by `iamh2o` at `2026-09-04T10:46:53Z` as merge commit
  `eadef9e8426ee968fb850328c7a6e2f90353fe29`. Fresh post-merge
  [main run `33864847727`](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33864847727)
  passed. Its [PostgreSQL 16.13
  job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33864847727/job/100997416056)
  reported `PostgreSQL 16.13 (Debian 16.13-1.pgdg13+1)` and passed 2,177 tests
  with zero skips and 95% rounded branch coverage. Its [PostgreSQL 17
  job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33864847727/job/100997416011)
  reported `PostgreSQL 17.11 (Debian 17.11-1.pgdg13+2)` and passed the same
  2,177 tests with zero skips and 95% rounded branch coverage. Ruff, format,
  mypy, Bandit, and verified-secret scanning passed. The [artifact
  job](https://github.com/Daylily-Informatics/daylily-tapdb/actions/runs/33864847727/job/100999154747)
  passed the source/wheel build, `twine check`, packaged schema and migration
  inspection, and installed-wheel API/GUI/CLI smoke.
- 2026-09-04 immutable-release receipt: annotated bare-semver tag `10.0.0`
  has tag object `fade52ec6a9d78f782b49823a020d77b77c7b564` and peels exactly to
  `eadef9e8426ee968fb850328c7a6e2f90353fe29`. The latest, non-prerelease
  [GitHub Release](https://github.com/Daylily-Informatics/daylily-tapdb/releases/tag/10.0.0)
  was published at `2026-09-04T11:00:00Z` with extensive breaking-change,
  migration, compatibility, test, and artifact notes and no duplicate binary
  assets.
- 2026-09-04 PyPI receipt: `twup` published the tag-built wheel and sdist once
  to [daylily-tapdb 10.0.0](https://pypi.org/project/daylily-tapdb/10.0.0/).
  Local and PyPI SHA-256 digests match: wheel
  `10ebb9a5559be3df403f17331147b8bb353cd11cb5b70dc334e7e3e8982a9b06`;
  sdist `d8cd441b59d945b108319fd5aaf4ada41970611959676db1e572bbe6e295db79`.
  A fresh Python 3.12 `--no-cache-dir` install from the public PyPI index
  verified `daylily-tapdb==10.0.0`, exact `meridian-euid==0.4.8`, canonical
  external-reference and federation imports, the canonical GUI factory, and
  CLI help including `validation external-references`.

## Final report

All rows terminal: yes
Objective complete: yes

Status counts:

- SUCCESS: 11
- DUPLICATE: 0
- NO_LONGER_NEEDED: 0
- FAIL: 0
- BLOCKED: 0
- IN_PROGRESS: 0
- OPEN: 0
