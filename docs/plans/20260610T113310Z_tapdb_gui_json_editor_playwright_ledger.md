# TapDB GUI JSON Editor And Playwright Ledger

## Objective
Fix the focus-losing JSON editor on the embeddable TapDB GUI, use a public clean interactive JSON editor, and add pytest-driven Playwright coverage for the main mounted GUI interactions and submits.

## Gate 0 Inventory
- Repo: `/Users/jmajor/projects/daylily/daylily-tapdb`
- Branch: `jem-dev`
- Running local GUI: `http://127.0.0.1:8921/tapdb/`
- Running local Postgres: `localhost:5545`
- Affected GUI files:
  - `daylily_tapdb/gui/static/js/tapdb-json-editor.js`
  - `daylily_tapdb/gui/templates/base.html`
  - `daylily_tapdb/gui/templates/template_editor.html`
  - `daylily_tapdb/gui/templates/create.html`
- Existing unit coverage: `tests/test_gui_embedded.py`
- Browser coverage gap: no pytest Playwright tests currently present.

## Ledger
| Row | Area | Work | Status | Evidence |
|---|---|---|---|---|
| GUI-JSON-001 | JSON editor | Replace rerender-on-input custom editor with public JSONEditor integration while preserving hidden textarea form posts | SUCCESS | `daylily_tapdb/gui/static/js/tapdb-json-editor.js`; `base.html` pins `jsoneditor@10.4.3`; hidden textarea remains form source |
| GUI-JSON-002 | UX | Confirm typing keeps focus and invalid JSON surfaces inline | SUCCESS | Live Playwright probe on `http://127.0.0.1:8921/tapdb/templates/new`: `focused_after_type: True`, hidden textarea updated to typed JSON |
| GUI-PW-001 | Tests | Add pytest Playwright fixture for mounted TapDB GUI using in-process app/server | SUCCESS | `tests/test_gui_playwright.py`; starts mounted `/tapdb` FastAPI host on random port and launches Chromium |
| GUI-PW-002 | Tests | Cover search, templates/new, validate/save, create-from-template, object detail edit/status/lineage, external link, graph, Meridian, metrics, admin gates | SUCCESS | `tests/test_gui_playwright.py` covers JSON editor focus, validate/save, search, templates, create, detail edits, JSON save, status, lineage, external link, graph, readiness, Meridian, metrics, admin 403 gates |
| GUI-PW-003 | Verification | Run GUI unit tests and Playwright tests | SUCCESS | `python -m pytest tests/test_gui_embedded.py tests/test_gui_playwright.py tests/test_template_loader.py -q` -> `45 passed`; `python -m pytest tests/ -q` -> `705 passed, 14 skipped` |
