# TapDB Dirty Theme 9.0.1 Release Ledger

Controlling plan: chat request, 2026-06-23, "TapDB Dirty Theme Release Plan"
Ledger path: `/Users/jmajor/projects/daylily/daylily-tapdb/docs/plans/20260623T124600Z_tapdb_dirty_theme_901_release_ledger.md`
Target release: `9.0.10`

## Gate 0 Baseline

- Repo: `/Users/jmajor/projects/daylily/daylily-tapdb`
- Branch: `jem-dev`
- Baseline refs: `HEAD`, `origin/main`, and `origin/jem-dev` all at `739bee15dd05b231244b0e8a63f3fd777fd2d699`
- Baseline tag: `9.0.0`
- Baseline version resolution: `python -m setuptools_scm -> 9.0.1.dev0` with dirty theme edits present
- Dirty files before implementation:
  - `daylily_tapdb/gui/static/css/tapdb-gui.css`
  - `daylily_tapdb/gui/static/js/lsmc-ui.js`
- `analysis_results`: `find analysis_results -maxdepth 3 -type f -> No such file or directory`
- Incorporation check: dirty theme blobs were not present on `origin/main`, `origin/jem-dev`, or normal remote branches; they existed only under an internal `refs/codex/turn-diffs/.../base` ref.
- Scope correction: preserve existing `cbf` theme while adding S.SF, Viridis, and Viridis Dark.
- Release target amendment: after the first push attempt, `git fetch origin --prune --tags` showed `origin/jem-dev` had advanced to `19f92ee176d708a42c48865fab44fc2ca59febf4` with pushed tags `9.0.1` through `9.0.9`; this work was rebased onto `origin/jem-dev` and retargeted to the next patch release, `9.0.10`.
- Live limits: no destructive DB/AWS actions are in scope.

## Control Ledger

| ID | Area/Repo | Requirement/Surface | Status | Category | Approval Gate | Owner | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| G0-001 | TapDB | Record Gate 0 baseline, dirty files, branch refs, and release target. | SUCCESS | plan_amendment | Gate 0 | orchestrator | Baseline section above. |  | Gate 0 recorded before implementation edits beyond preserving dirty theme work. |
| G0-002 | TapDB | Amend release target after discovering remote `jem-dev` and tags advanced. | SUCCESS | plan_amendment | Gate 0 | orchestrator | Fetch after rejected push showed `origin/jem-dev` at `19f92ee` and pushed tags `9.0.1` through `9.0.9`; rebase onto `origin/jem-dev` completed with a resolved `lsmc-ui.js` conflict. |  | Release target amended from `9.0.1` to `9.0.10`; pushed tags will not be moved or overwritten. |
| THEM-001 | GUI assets | Preserve dirty S.SF, Viridis, and Viridis Dark skin changes as release work. | SUCCESS | feature_implementation | Gate 1 | orchestrator | `tapdb-gui.css` adds `html[data-theme="ssf"]`, `html[data-theme="viridis"]`, and `html[data-theme="viridis-dark"]`; `lsmc-ui.js` exposes those themes. |  | New theme skins preserved and covered by `tests/test_gui_theme_assets.py`. |
| THEM-002 | GUI assets | Preserve existing `original`, `light`, `dark`, and `cbf` themes in selector behavior. | SUCCESS | contract_test | Gate 1 | orchestrator | `lsmc-ui.js` theme list includes `original`, `light`, `dark`, `cbf`, `ssf`, `viridis`, `viridis-dark`; `cbf: "CBF"` restored. |  | Existing CBF option preserved and regression-covered. |
| TEST-001 | Static checks | Run whitespace/static diff checks. | SUCCESS | contract_test | Gate 5 | orchestrator | `git diff --check -> pass`; `source ./activate && python -m ruff check daylily_tapdb/ admin/ tests/ && python -m ruff format --check daylily_tapdb/ admin/ tests/ -> pass`. |  | Static checks passed. A broader ruff attempt against JS was invalid because Ruff parses Python only; corrected command was used. CI Ruff format initially identified four files; local `ruff format` corrected them. |
| TEST-002 | Focused GUI tests | Run focused embedded and Playwright GUI tests. | SUCCESS | contract_test | Gate 5 | orchestrator | `source ./activate && python -m pytest tests/test_gui_theme_assets.py tests/test_gui_embedded.py tests/test_gui_playwright.py -q -> 51 passed`. |  | Focused GUI tests passed in the TapDB venv. Ambient conda Python failed collection due stale `meridian_euid`; rerun used repo activation contract. |
| TEST-003 | Full test suite | Run full TapDB pytest suite before merge/release. | SUCCESS | contract_test | Gate 5 | orchestrator | Initial pre-rebase run: `750 passed, 14 skipped`; after rebasing onto advanced `origin/jem-dev` and fixing upstream failures: `source ./activate && python -m pytest tests/ -q -> 758 passed, 14 skipped, 2 warnings`. |  | Full suite passed on final branch shape. |
| TEST-004 | Advanced branch green build | Repair advanced `jem-dev` test failures surfaced after rebase. | SUCCESS | contract_test | Gate 5 | orchestrator | `README.md` restored links to `examples/readme/*` and Meridian CLI cues; `tests/test_validation_governance.py` fake `_Session` now exposes `no_autoflush`; formerly failing focused tests -> `2 passed`; ruff on touched Python/doc files -> pass. |  | Advanced branch is green before release. |
| TEST-005 | CI formatting | Match GitHub Actions Ruff format gate on advanced branch files. | SUCCESS | contract_test | Gate 5 | orchestrator | GitHub Actions Ruff format check on PR #86 reported `daylily_tapdb/services/graph_payloads.py`, `daylily_tapdb/user_store.py`, `tests/test_gui_theme_assets.py`, and `tests/test_user_store_core.py`; `source ./activate && python -m ruff format ... -> 4 files reformatted`; local Ruff check/format and full pytest passed after formatting. |  | PR branch matches CI formatter expectations. |
| REL-001 | GitHub | Commit, push `jem-dev`, open PR to `main`, wait for green, and merge. | OPEN | feature_implementation | Gate 5 | orchestrator | Pending. |  |  |
| REL-002 | Release | Sync merged release commit locally, create annotated `9.0.1` tag, and push tags. | OPEN | feature_implementation | Gate 5 | orchestrator | Pending. |  |  |
| REL-003 | Package | Clear `dist/*`, build package, publish with existing Twine flow, and verify package-index visibility. | OPEN | feature_implementation | Gate 5 | orchestrator | Pending. |  |  |
| BROW-001 | Browser acceptance | Verify theme selector and legibility for new themes in local TapDB GUI. | SUCCESS | contract_test | Gate 5 | orchestrator | In-app browser local HTTP harness served final edited `tapdb-gui.css` and `lsmc-ui.js`; selector options were `original`, `light`, `dark`, `CBF`, `S.SF`, `Viridis`, `Viridis Dark`; global `viridis-dark` and service `ssf` persisted across reload; computed S.SF card/button/link colors were non-default. |  | Browser acceptance passed for the edited theme assets. DB-backed local GUI start was avoided because TapDB CLI could not clear a stale local Postgres PID file. |

## Final Report

All rows terminal: no
Objective complete: no

Status counts:
- SUCCESS: 10
- OPEN: 3
- DUPLICATE: 0
- NO_LONGER_NEEDED: 0
- FAIL: 0
- BLOCKED: 0
