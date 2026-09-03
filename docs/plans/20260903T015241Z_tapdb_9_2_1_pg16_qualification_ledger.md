# TapDB 9.2.1 PostgreSQL 16/17 Qualification Ledger

## Objective

Make `main` the sole forward-development branch and release a qualification-only
TapDB 9.2.1 package whose unchanged runtime, schema, migration, identity, and
dependency surfaces are tested on PostgreSQL 16.13 and PostgreSQL 17.

The shared Aurora cluster, all Dayhoff service deployments, and independent
Aurora compatibility qualification are out of scope. The existing
`meridian-euid==0.4.8` pin and every published 9.2 migration asset are frozen.

## Control Ledger

Controlling plan: user-approved plan in the implementing Codex task

Ledger path:
`docs/plans/20260903T015241Z_tapdb_9_2_1_pg16_qualification_ledger.md`

### Gate 0 baseline

- Repository: `Daylily-Informatics/daylily-tapdb`.
- Initial checkout: clean
  `codex/tapdb-9.2.0-issue-convergence`; implementation branch created as
  `codex/tapdb-9.2.1-pg16-qualification` from annotated tag `9.2.0`.
- `9.2.0` tag object: `50c90a41e35bceb9dc561f74eee621ec0aa435f0`;
  peeled commit: `8155d5743464c8f46413a5c2c1b9a444f57d0625`.
- Before cutover, GitHub default and remote `HEAD` were `jemdev10`; `main`
  equaled the peeled 9.2.0 commit, `jemdev10` was its ancestor with
  `0 13` left/right counts, and no open pull requests existed.
- Default-branch cutover completed: GitHub default and remote `HEAD` now resolve
  to `main`; unchanged ruleset `core` (`12120471`) follows
  `~DEFAULT_BRANCH`; `main` reports protected and `jemdev10` reports
  unprotected.
- `docker manifest inspect postgres:16.13` succeeded. No remote `9.2.1` tag or
  GitHub Release existed at baseline.
- Baseline focused check:
  `source ./activate && python -m pytest tests/test_release_920_contract.py -q`
  -> `7 passed`; installed TapDB 9.2.0 resolves `meridian-euid==0.4.8`.
- Existing release evidence: the 9.2.0 suite passed all 2,225 tests with zero
  skips on local PostgreSQL 16.14 and on PostgreSQL 17 CI. New release evidence
  must come from the two-lane 9.2.1 CI matrix.
- Live limits: do not inspect, create, change, migrate, or deploy any Aurora or
  Dayhoff resource. Do not delete `jemdev10` until the release is complete and
  a second explicit destructive-action confirmation is received.

| ID | Area/Repo | Requirement/Surface | Status | Category | Approval Gate | Owner | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| BRANCH-001 | GitHub | Change default branch and remote HEAD from `jemdev10` to `main` without altering branch policy | SUCCESS | config_or_startup_contract | Gate 0 | orchestrator | `gh repo view`; `git ls-remote --symref`; ruleset `12120471`; `main` protected |  | Default and remote HEAD are `main`; the unchanged default-branch ruleset transferred to it. |
| BRANCH-002 | Git | Create qualification branch from exact annotated 9.2.0 release commit | SUCCESS | contract_test | Gate 0 | orchestrator | branch `codex/tapdb-9.2.1-pg16-qualification`; HEAD/tree equal `9.2.0^{}` |  | New work is based only on the immutable release tag and will target `main`. |
| PLAN-001 | docs | Preserve this plan, baseline, receipts, and terminal state in a durable ledger | IN_PROGRESS | plan_amendment | Gate 0 | orchestrator | this file |  |  |
| CI-001 | CI | Run the identical complete database suite on `postgres:16.13` and `postgres:17`, with `fail-fast: false` and parameterized tools | IN_PROGRESS | contract_test | Gate 5 | orchestrator | two-entry workflow matrix; local YAML parse confirms exact image/major fields |  | Remote PostgreSQL 16.13 and 17 receipts remain required. |
| CONTRACT-001 | tests | Require both database lanes, unchanged complete-suite shape, no deselection, and 9.2.1 artifact expectations | SUCCESS | contract_test | Gate 5 | orchestrator | `python -m pytest tests/test_release_920_contract.py -q` -> `7 passed`; Ruff check/format passed |  | Existing contract now requires both explicit matrix entries, parameterized tooling, 9.2.1 build expectations, support wording, and no deselection. |
| DOC-001 | docs | State PostgreSQL 16/17 support and exact qualification boundary without claiming Aurora validation | SUCCESS | historical_docs_only | Gate 5 | orchestrator | updated `README.md` and `AI_DIRECTIVE.md`; documentation contract passed |  | Active guidance names PostgreSQL 16.13/17 qualification and explicitly disclaims independent Aurora qualification. |
| FREEZE-001 | source | Keep production Python, schema, migrations, dependencies, identity behavior, and Meridian 0.4.8 byte-for-byte unchanged | IN_PROGRESS | active_product_contract | Gate 5 | orchestrator | `git diff --quiet 9.2.0 -- daylily_tapdb admin schema pyproject.toml uv.lock` -> 0; base schema, RLS, three 9.2 migrations, metadata, and lock blobs match 9.2.0 |  | Repeat against the merged release commit before tagging. |
| TEST-016 | CI | PostgreSQL 16.13 lane passes schema apply/seed, all tests, migrations, concurrency, browser, docs, backup, and coverage with zero skips | OPEN | contract_test | Gate 5 | orchestrator | pending |  |  |
| TEST-017 | CI | PostgreSQL 17 lane retains the same complete acceptance with zero skips | OPEN | contract_test | Gate 5 | orchestrator | pending |  |  |
| TEST-GLOBAL | CI | Quality, security, build, packaged assets, installed wheel, and post-merge main gates pass | IN_PROGRESS | contract_test | Gate 5 | orchestrator | local Ruff, format, mypy, Bandit, detect-secrets, compileall, `uv lock --check`, YAML parse, and diff check passed |  | GitHub and post-merge main receipts remain. |
| PR-001 | GitHub | Reviewed, green PR merges normally to `main` without admin or force bypass | OPEN | contract_test | Gate 5 | orchestrator | pending |  |  |
| REL-TAG | release | Annotated bare `9.2.1` tag points to the exact clean main merge commit while 9.2.0 remains unchanged | OPEN | contract_test | Gate 5 | orchestrator | pending |  |  |
| REL-BUILD | release | Exact-tag wheel/sdist pass metadata, asset, checksum, fresh-install, and Meridian 0.4.8 checks | IN_PROGRESS | contract_test | Gate 5 | orchestrator | preserved old build outputs in `/tmp/tapdb-pre921-dist.SMZDOm`; clean candidate build, wheel-asset check, Twine check, fresh install, supported CLI smoke, and exact Meridian pin passed |  | Candidate repair succeeded; exact-tag rebuild remains required. |
| REL-PYPI | release | Publish once with `twup` and verify no-cache PyPI 9.2.1 artifacts and install | OPEN | contract_test | Gate 5 | orchestrator | pending |  |  |
| REL-GH | release | Publish a latest non-prerelease GitHub Release with qualification notes, CI receipts, comparison, and PyPI hashes | OPEN | contract_test | Gate 5 | orchestrator | pending |  |  |
| RETIRE-001 | GitHub | After verified release, reconfirm ancestry/open-PR safety and delete remote `jemdev10` only after second explicit confirmation | OPEN | removable_compatibility_debt | Gate 5 | orchestrator | pending |  |  |

## Final Report

All rows terminal: no.

Objective complete: no.

Current status counts: four `SUCCESS`, five `IN_PROGRESS`, and seven `OPEN`.
