# TapDB Meridian EUID 0.4.8 Pin Ledger

Date: 2026-09-01

## Scope

Move TapDB from the released `meridian-euid==0.4.7` dependency to the verified
`meridian-euid==0.4.8` release. The upstream release clarifies the canonical
uppercase and accepted-input documentation policy; it does not require a TapDB
runtime, schema, API, or migration change. Keep the update limited to the
dependency pin, resolved lock, and version-specific documentation notes.

## Gate 0: Inventory Freeze

- Repository: `/Users/jmajor/projects/cli_refactor/daylily-tapdb-meridian-euid-0-4-8`.
- Branch: `codex/tapdb-meridian-euid-0-4-8-pin-20260901`, created from clean
  `origin/main` commit `9a60412` (tag `9.1.0`).
- The separate local checkout is dirty only in `AGENTS.md` and remains
  untouched.
- Current exact pin: `meridian-euid==0.4.7` in `pyproject.toml` and `uv.lock`.
- Current version-specific notes: `config/tapdb-config-example.yaml`,
  `docs/identity-and-scoping.md`, and `docs/runtime-and-cli.md` name
  `meridian-euid 0.4.7`.
- Release evidence: PyPI `meridian-euid==0.4.8` was built, checked, published,
  and installed fresh before this downstream work began.
- No database, schema, runtime configuration, CLI, public API, or live TapDB
  action is in scope.
- Validation is limited to lock resolution, an exact-version import check, diff
  review, and the existing GitHub PR CI run; no duplicate full local test suite.

## Control Ledger

| ID | Area | Requirement | Status | Category | Gate | Owner | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| LEDGER-001 | plan | Record source baseline, scope, and release evidence | SUCCESS | plan_amendment | Gate 0 | Codex | This ledger; clean worktree at `9a60412` |  | Baseline recorded before edits. |
| PIN-001 | dependencies | Pin `meridian-euid` exactly to `0.4.8` | SUCCESS | feature_implementation | Gate 1 | Codex | `pyproject.toml` |  | Exact downstream dependency is now `meridian-euid==0.4.8`. |
| LOCK-001 | dependencies | Resolve `uv.lock` to the published `0.4.8` artifacts | SUCCESS | contract_test | Gate 1 | Codex | `uv lock --upgrade-package meridian-euid`; `uv.lock` records `0.4.8` sdist and wheel hashes |  | Lock resolution uses the PyPI artifacts published for this release. |
| DOC-001 | config documentation | Update all version-specific registry notes | SUCCESS | historical_docs_only | Gate 1 | Codex | `config/tapdb-config-example.yaml`; `docs/identity-and-scoping.md`; `docs/runtime-and-cli.md` | Current-source inventory found three references, not one. | All three notes now cite `0.4.8` and retain the registry `0.1.1` relationship. |
| VERIFY-001 | packaging | Confirm exact `0.4.8` resolution and clean scoped diff | SUCCESS | contract_test | Gate 5 | Codex | `uv sync --locked`; `importlib.metadata.version` -> `0.4.8`; `meridian_euid.__version__` -> `0.4.8`; `uv lock --check`; `git diff --check` |  | Exact dependency resolution and scoped diff verified without a duplicate full local suite. |
| PR-001 | integration | Commit, push, open a PR, and use its CI as the regression run | OPEN | contract_test | Gate 5 | Codex | Pending |  |  |
| FINAL-001 | handoff | Terminalize every ledger row and report the downstream pin | OPEN | plan_amendment | Gate 5 | Codex | Pending |  |  |

## Terminal-State Report

- `SUCCESS`: 5
- Working rows: 2
- All rows terminal: no.
- Objective complete: no.
