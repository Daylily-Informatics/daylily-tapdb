# TapDB 9.2.0 Issue-Convergence Execution Ledger

Date: 2026-09-02 UTC

## Objective

Implement the approved TapDB 9.2.0 issue-convergence plan, including issues
`#93`, `#92`, `#90`, `#88`, `#87`, `#72`, `#48`, `#43`, `#42`, `#41`, and
`#40`; the Kahlo DAG v2 eligibility contract; exact `meridian-euid==0.4.8`;
identity-preserving migrations; comprehensive tests and documentation; one
reviewed PR to `main`; annotated tag `9.2.0`; and PyPI publication through the
existing `twup` function.

## Gate 0: Inventory Freeze

- Controlling plan: `/Users/jmajor/.codex/attachments/19b9e8d8-72e7-4aac-a322-0a815b93a1b7/pasted-text.txt`.
- DAG specification: `docs/plans/20260901T100631Z_kahlo_global_dag_tapdb_eligibility_spec.md`, required SHA-256 `d5e8593b6bc85256924db76630d3014882b4a65389a5154782ef7a2d087ca8eb`.
- Ledger: `docs/plans/20260902T031019Z_tapdb_9_2_0_issue_convergence_ledger.md`.
- Clean implementation worktree: `/Users/jmajor/projects/mega_dayhoff/repos_work/daylily-tapdb-9.2.0`.
- Branch: `codex/tapdb-9.2.0-issue-convergence`.
- Baseline: current `origin/main` commit `d285dc6021b77d01bcb17347f0099fd90bd419e1`, which supersedes the original plan baseline because it contains the already-merged Meridian 0.4.8 pin PRs #99 and #100.
- Preserved checkout: `/Users/jmajor/projects/mega_dayhoff/repos_work/daylily-tapdb` remains on dirty `jem-dev`; its seven modified and two untracked files are not edited by this execution.
- Live GitHub inventory: all eleven target issues are open; no 9.2 branch or tag existed before Gate 0.
- PyPI inventory: `daylily-tapdb` latest published version was `9.0.10` at Gate 0.
- Baseline checks: `source ./activate && python -m pytest tests/ -q` -> `1620 passed, 14 skipped, 5 warnings in 97.99s`; the 14 skips are an explicit acceptance gap to eliminate or classify before release.
- No live service deployment, downstream adoption, DNS/registry mutation, or OWY mutation is in scope.
- Release publication is authorized by the controlling plan and the user's instruction to complete it.

## Ownership And Write Scopes

- `schema_identity` (`gpt-5.6-sol`, high): schema identity, migration receipts,
  natural identity, advisory locks, owner uniqueness, and focused tests.
- `ops_templates` (`gpt-5.6-sol`, high): template repository operations,
  object CLI/API operations, runtime information, GUI/API integration, and
  focused tests.
- `dag_security_docs` (`gpt-5.6-sol`, high): DAG v2, typed federation,
  RLS/audit/auth hardening, active documentation, consumer guide, CI, and tests.
- `orchestrator`: ledger, integration, cross-workstream review, full acceptance,
  GitHub/PyPI release, and all final evidence.

## Control Ledger

| ID | Area | Requirement | Status | Category | Gate | Owner | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| PLAN-001 | plan | Preserve exact DAG specification in clean branch | SUCCESS | plan_amendment | Gate 0 | orchestrator | `shasum -a 256` -> `d5e8593b6bc85256924db76630d3014882b4a65389a5154782ef7a2d087ca8eb` |  | Exact supplied specification preserved byte-for-byte. |
| PLAN-002 | plan | Record clean baseline, scopes, checks, and release authority | SUCCESS | plan_amendment | Gate 0 | orchestrator | this ledger; baseline `1620 passed, 14 skipped` |  | Gate 0 inventory is frozen before runtime implementation. |
| MIG-ID-001 | migration | Existing identity values remain byte-for-byte unchanged | SUCCESS | feature_implementation | Gate 2 | schema_identity | `migration_identity.py`; immutable tuple/hash receipts; populated PostgreSQL preservation tests in the 2,223-test gate |  | Every pre-existing UID/EUID/EUID component, UUID, scope, template, lineage, linkage, and creation timestamp is fingerprinted and compared before commit. |
| MIG-SEQ-001 | migration | Existing identity/EUID sequence assignments and state remain unchanged except declared new allocations | SUCCESS | feature_implementation | Gate 2 | schema_identity | pre/post sequence definitions and state in migration receipts; failure/no-op/sparse/high-watermark PostgreSQL tests |  | Undeclared changes, cached/behind/cycling generators, and gap reuse fail closed; only declared new-message allocation can advance exact generators. |
| MIG-DIFF-001 | migration | Only explicitly declared non-identity columns may change | SUCCESS | contract_test | Gate 2 | schema_identity | per-row/per-column hashes plus exact transformation markers; failure injection in `test_postgres_identity_concurrency.py` |  | Unknown allowlists or transformations fail preflight and every undeclared postflight delta rolls back. |
| MIG-CLI-001 | migration | Dry-run/apply preflight-receipt migration interface and exclusive guard | SUCCESS | feature_implementation | Gate 2 | schema_identity | `tapdb db schema migrate --dry-run/--apply`; immutable receipt tests; advisory plus ACCESS EXCLUSIVE guards |  | Apply is receipt-bound and transactional and refuses target, asset, schema, row, FK, or sequence drift. |
| MIG-OUTBOX-001 | migration | Legacy outbox conversion preserves existing IDs/UUIDs and records deterministic mapping receipts | SUCCESS | feature_implementation | Gate 2 | schema_identity | `20260902_010100_legacy_outbox_message_conversion.sql`; mixed-owner success and fail-closed PostgreSQL tests |  | Existing outbox ID/event UUID/scope/payload/state/timestamps remain authoritative; only genuinely new canonical MSG identities are allocated and mapped. |
| MIG-TEST-001 | migration | Historical, sparse, failure, no-op, and restore-then-migrate identity fixtures | SUCCESS | contract_test | Gate 5 | schema_identity | `2,223 passed`, including migration contract, real-PostgreSQL concurrency, post-restore, no-op, and injected-failure cases |  | No migration acceptance test skipped. |
| DEP-001 | dependency | Every install and artifact resolves exactly `meridian-euid==0.4.8` | IN_PROGRESS | contract_test | Gate 5 | orchestrator | exact `pyproject.toml`/`uv.lock` pin and approved PyPI hashes; isolated 9.2.0 wheel metadata/install resolved 0.4.8 |  | Final post-merge artifact and no-cache PyPI proofs remain. |
| ISS-093 | issue #93 | Database-enforced typed-instance natural identity claim API | SUCCESS | feature_implementation | Gate 2 | schema_identity | partial unique index; `InstanceIdentityClaim`; `INSERT ... ON CONFLICT`; separate-session replay/divergence/rollback tests |  | Global exact keys retain ownership through soft delete; caller transaction boundaries remain untouched. |
| ISS-092 | issue #92 | Transaction-scoped advisory locks with bounded waits and redacted diagnostics | SUCCESS | feature_implementation | Gate 2 | schema_identity | framed SHA-256 signed-int64 helper; same-key/different-key/commit/rollback/timeout real-PostgreSQL tests |  | No session-level lock is used and receipts expose only a fingerprint. |
| ISS-090 | issue #90 | Canonical repository template export/import/inventory with receipts | SUCCESS | feature_implementation | Gate 3 | ops_templates | deterministic pack round trips; absolute-path, no-overwrite, conflict, prefix provenance, receipt, CLI/API/GUI tests |  | Repository pack and runtime snapshot backup kinds remain distinct. |
| ISS-088 | issue #88 | Enforce lineage as the only durable object relationship model | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | lineage-only graph projection, untyped-edge rejection, docs/static contracts, and DAG tests |  | Copied EUID metadata remains lookup/display data and cannot become a v2 edge. |
| ISS-087 | issue #87 | Correct owner-scoped template uniqueness in schema, ORM, migration, and tests | SUCCESS | feature_implementation | Gate 2 | schema_identity | owner included in base schema, ORM, migration, manager/loader/user-store and real-PostgreSQL tests |  | Same coordinates can coexist only across distinct owners; same-owner duplicates are rejected. |
| ISS-072 | issue #72 | Governed object search/get/update/repair/delete CLI and API | SUCCESS | feature_implementation | Gate 3 | ops_templates | exact selector, SQL filter, keyset cursor, allowlist, repair evidence, soft-delete, CLI/API/GUI tests |  | Templates are read-only; mutations are allowlisted, audited, exact-target, and dry-run by default. |
| ISS-048 | issue #48 | One sanitized runtime-info payload shared by CLI/API/GUI/legacy admin | SUCCESS | feature_implementation | Gate 3 | ops_templates | `tapdb.runtime-info/v1` parity/sanitization tests across all four surfaces |  | Payload covers package, Meridian, Git, config, database, scope, storage, UI, and DAG without credentials. |
| ISS-043 | issue #43 | Block client inheritance/reuse of TapDB-owned category prefixes | SUCCESS | contract_test | Gate 4 | dag_security_docs | exact installed-core proof, operator materialization, copied/modified path rejection, prefix tests |  | Clients may consume canonical core types but cannot claim TapDB-reserved prefixes or unlock them by copying files. |
| ISS-042 | issue #42 | Document and implement typed external-reference plus lineage primitive | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | typed XRF factory/projector, natural identity, authoritative lineage, DAG projections, consumer docs/tests |  | Target identity, tenant/kind, assertion time, provenance, reference EUID, and lineage EUID are explicit. |
| ISS-041 | issue #41 | Document shipped core object types and optional system-user role | SUCCESS | historical_docs_only | Gate 5 | dag_security_docs | README/core inventory/template guide contracts for exactly nine shipped templates |  | System User is described as optional auth/UI substrate rather than universal business identity. |
| ISS-040 | issue #40 | Runtime config ignores ambient TapDB/PG target overrides | SUCCESS | contract_test | Gate 2 | dag_security_docs | explicit-context/config tests and fail-closed CLI/API/runtime connection paths |  | Managed runtime identity comes only from one explicit config path. |
| SEC-RLS-001 | security | Forced fail-closed owner/tenant RLS with one transaction context | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | forced-RLS schema asset and cross-owner/cross-tenant/global/operator PostgreSQL tests |  | Runtime roles are immutable NOSUPERUSER/NOBYPASSRLS principals bound to exact config and scope. |
| SEC-AUDIT-001 | security | Non-null actor attribution and explicit historical marker migration | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | actor constraint/trigger tests and exact `migration:pre-9.2-unattributed` transformation receipt |  | No generic fallback attribution remains. |
| SEC-AUTH-001 | security | Authenticate legacy read APIs; disabled auth only for loopback development | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | admin/embedded/DAG route authorization tests and construction-time dependency checks |  | Anonymous reads fail; disabled mode is constrained to loopback development. |
| DAG-MOUNT-001 | DAG v2 | Atomic explicit-config mount and eligibility manifest | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | mount rollback/collision/config/auth/identity/limits tests |  | Failed mounts advertise nothing and register no partial routes. |
| DAG-ROUTE-001 | DAG v2 | Manifest, exact object, bounded data, and cursor search routes | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | authenticated manifest/exact/data/search tests in full gate |  | Search is discovery only; exact lookup is the ownership proof. |
| DAG-XRF-001 | DAG v2 | Typed XRF factory/projector with exact target service/object identity | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | canonical installed-XRF, global claim, lineage, projection, and client-owner PostgreSQL tests |  | EUIDs are persisted/validated values, never synthesized placeholders. |
| DAG-LINEAGE-001 | DAG v2 | Scope, cycle, self-loop, revision, snapshot, presentation, and truncation contracts | SUCCESS | feature_implementation | Gate 3 | dag_security_docs | graph contract tests and supplied ten-scenario DAG acceptance matrix |  | Every bounded response states effective limits and truncation. |
| DAG-FED-001 | DAG v2 | No outbound v2 fetch; hardened allowlisted v1 proxy only when explicit | SUCCESS | legitimate_safety_handling | Gate 4 | dag_security_docs | v2 no-fetch tests; v1 HTTPS/DNS/IP/redirect/content/size/deadline/credential tests |  | V1 remains separately mounted and cannot act as a v2 fallback. |
| DOC-README-001 | docs | Rewrite root README using approved RGBW structure and current code truth | SUCCESS | historical_docs_only | Gate 5 | dag_security_docs | rewritten `README.md`; docs/release contract tests |  | Purpose, model, install, core types, embedding, DAG, security, testing, and release are current and public-safe. |
| DOC-ACTIVE-001 | docs | Reconcile every active guide with schema, CLI, API, GUI, migration, security, and packaging | SUCCESS | historical_docs_only | Gate 5 | dag_security_docs | active-doc audit plus 31 focused docs/release contract tests and full suite |  | Historical ledgers retain their historical pins/paths; active guidance reflects 9.2. |
| DOC-CONS-001 | docs | Consumer discoverability guide is complete, linked, and contract-tested | SUCCESS | historical_docs_only | Gate 5 | dag_security_docs | `docs/consumer-discoverability-guide.md`, README/docs index links, snippet/link/anti-pattern tests |  | Includes service mount, manifest, discovery, claims/locks, typed XRF, migration expectations, adopter checklist, and troubleshooting. |
| TEST-PG-001 | tests | PostgreSQL 17 critical integration suite cannot skip | SUCCESS | contract_test | Gate 5 | orchestrator | GitHub run `33609517506`, PostgreSQL 17: `2,223 passed`, zero skipped, eight warnings in 286.52 seconds |  | Full PostgreSQL 17 acceptance passed before merge. |
| TEST-CONC-001 | tests | Separate-session natural-identity and lock concurrency coverage | SUCCESS | contract_test | Gate 5 | schema_identity | real PostgreSQL separate-session same/different key, winner, timeout, commit, rollback, and connection-reuse tests |  | Transaction and lock lifecycle acceptance is complete locally. |
| TEST-DAG-001 | tests | All ten DAG v2 acceptance scenarios pass | SUCCESS | contract_test | Gate 5 | dag_security_docs | DAG/XRF/RLS/mount acceptance included in `2,223 passed`; focused DAG/docs runs `99 passed` plus follow-up |  | No v2 acceptance skip. |
| TEST-OPS-001 | tests | Template, object, runtime, GUI/API, and repository round-trip coverage | SUCCESS | contract_test | Gate 5 | ops_templates | focused CLI/unit repair `283 passed`; complete operations/browser/database suite in full gate |  | Browser acceptance ran with no skip. |
| TEST-COV-001 | tests | Branch coverage enabled; overall and changed/new modules at least 90% | SUCCESS | contract_test | Gate 5 | orchestrator | overall 95.30%; 44 changed production modules verified at >=90%, lowest 90.21% |  | CLI exclusions removed and branch coverage enforced. |
| CI-001 | CI | Ruff, mypy, Bandit, secret scan, full pytest, coverage, docs, build, schema, and installed-wheel smoke | SUCCESS | contract_test | Gate 5 | dag_security_docs | GitHub push/PR runs `33609511959` and `33609517506`: all eight jobs green |  | PostgreSQL 17, installed-wheel, static, security, coverage, docs, and build gates passed. |
| REL-PR-001 | release | One green PR closes all eleven issues | IN_PROGRESS | contract_test | Gate 5 | orchestrator | PR `#101`, head `99b9cfef8bd4e6492730451351d1bce46778763e`, all checks green |  | Reviewed merge remains. |
| REL-TAG-001 | release | Annotated immutable bare tag `9.2.0` points to exact merge commit | OPEN | contract_test | Gate 5 | orchestrator | pending |  |  |
| REL-BUILD-001 | release | Clean artifacts validate version, dependency, assets, checksums, and fresh install | IN_PROGRESS | contract_test | Gate 5 | orchestrator | candidate and GitHub installed-wheel smoke passed |  | Final exact-merge artifact build and checksums remain. |
| REL-PYPI-001 | release | Invoke `twup` once and verify no-cache PyPI install of 9.2.0 | OPEN | contract_test | Gate 5 | orchestrator | pending |  |  |

## Gate 5: Local Verification Receipt

- Full release suite, using explicit config
  `/tmp/tapdb-release-pg.8FLOUT/tapdb-ci-v2/tapdb-config.yaml` against local
  PostgreSQL 16.14: `2,223 passed`, zero skipped, seven warnings, 117.90
  seconds.
- Branch coverage: 95.30% overall. The changed-module verifier accepted all 44
  changed production modules at or above 90%; the lowest result was 90.21%.
- Static and supply-chain gates: Ruff lint and formatting, strict mypy, Bandit,
  detect-secrets, compileall, `uv lock --check`, and `git diff --check` all
  passed.
- Candidate build: an isolated build with
  `SETUPTOOLS_SCM_PRETEND_VERSION=9.2.0` passed wheel-asset verification, fresh
  virtual-environment installation, CLI smoke tests, exact
  `meridian-euid==0.4.8` resolution, and source-distribution asset inspection.
- Candidate-only SHA-256 values were
  `a22fd08a67071278234c072cf509ed3ebf45677c4b2ab69afad4ae72fb27710f`
  for the wheel and
  `2801d800d93ae3e9467a045052c2d01dba27f9119ded3269b08d79cf85cd1223`
  for the source distribution. These are not release checksums; final
  artifacts will be rebuilt from the exact annotated merge commit.
- Remaining gates are reviewed PR merge, annotated tag, final clean artifact
  verification, the single authorized `twup` invocation, and fresh no-cache
  PyPI verification.

## Gate 5: Remote Verification Receipt

- Pull-request run `33609517506` completed successfully at head
  `99b9cfef8bd4e6492730451351d1bce46778763e`; its PostgreSQL 17 job ran all
  2,223 tests with zero skips in 286.52 seconds.
- Push run `33609511959` independently completed the same quality, security,
  PostgreSQL 17, coverage, and installed-wheel sequence.
- Each run passed Ruff and mypy, Bandit and verified-secret scanning,
  PostgreSQL 17 full-suite and changed-module coverage enforcement, and the
  9.2.0 wheel/source build plus installed-wheel CLI smoke test.
- The first remote attempts exposed CI-only assumptions: an invalid job-level
  runner context, zero-findings handling in the secret gate, local-trust-masked
  test-role passwords, operator-only inventory DDL through a runtime role,
  ANSI-styled help assertions, a macOS-only browser shortcut, and a real Linux
  PostgreSQL log path. These were corrected without weakening production
  behavior; the combined affected local suite passed 114 tests before the
  final remote runs.

## Final Report

All rows terminal: no.

Objective complete: no.

Status counts: 36 `SUCCESS`, three `IN_PROGRESS`, two `OPEN` (41 control rows
total).
