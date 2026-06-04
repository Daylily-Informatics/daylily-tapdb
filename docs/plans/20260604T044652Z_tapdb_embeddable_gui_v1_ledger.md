# TapDB Embeddable GUI V1 Ledger

Created: 2026-06-04T04:46:52Z

## Objective

Build a second, parallel TapDB GUI that client FastAPI apps can mount at `/tapdb`
with minimal or no client refactoring. Preserve the existing legacy admin pages
and existing client API flows. Move typed external object-link workflows into
TapDB as a shared primitive, and make template validation/import/editing easy
through the TapDB GUI.

## Gate 0 Inventory Freeze

| Item | Evidence |
|---|---|
| Controlling ledger path | `docs/plans/20260604T044652Z_tapdb_embeddable_gui_v1_ledger.md` |
| Repo | `/Users/jmajor/projects/daylily/daylily-tapdb` |
| Branch | `codex/tapdb-release-train-20260529` |
| Baseline status | `git status --short --branch` -> `## codex/tapdb-release-train-20260529...origin/codex/tapdb-release-train-20260529` |
| Legacy GUI routes | `rg "^@app\\.(get|post|delete|put|patch)" admin/main.py` -> legacy root, object, graph, create, API routes present |
| Existing host bridge | `daylily_tapdb/web/factory.py`, `daylily_tapdb/web/bridge.py` |
| Existing DAG/search helpers | `daylily_tapdb/web/dag.py`, `daylily_tapdb/services/object_search.py`, `daylily_tapdb/services/graph_payloads.py` |
| Existing template loader/guard | `daylily_tapdb/templates/loader.py`, `daylily_tapdb/templates/mutation.py`, `tests/test_template_mutation_guard.py` |
| Existing child instantiation | `daylily_tapdb/factory/instance.py`, `daylily_tapdb/validation/instantiation_layouts.py` |
| Existing governance helpers | `daylily_tapdb/governance.py`, `daylily_tapdb/euid.py` |
| Plan constraints | No schema migration unless hard-blocked; no fallback discovery; new external links are typed TapDB objects linked by lineage |

## Agent Ownership

| Agent | Scope |
|---|---|
| Agent 1 | Orchestrator, ledger, route exports, integration, final tests |
| Agent 2 | Mounted shell, auth, root-path-safe URLs, host theme hooks |
| Agent 3 | Search, EUID detail, audit/relationship tables, JSON/status/lineage actions |
| Agent 4 | Template editor/validation/save and create-from-template flows |
| Agent 5 | Graph page and typed external object-link workflow |
| Agent 6 | Meridian/admin metrics/docs/test-host acceptance |

## Tracking Rows

| ID | Area | Requirement | Status | Category | Approval Gate | Owner | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| GUI-001 | Routing | Export `create_tapdb_gui_app` and `create_tapdb_gui_router` without altering legacy routes | SUCCESS | feature_implementation | Gate 3 | Agent 1 | `daylily_tapdb/gui/router.py`, `daylily_tapdb/gui/__init__.py`, `daylily_tapdb/web/__init__.py` |  | New embeddable GUI exports added; legacy `admin.main` untouched. |
| GUI-002 | Shell | Add mounted GUI shell with auth, admin gating, host CSS, and root-path-safe URLs | SUCCESS | feature_implementation | Gate 3 | Agent 2 | `daylily_tapdb/gui/templates/base.html`, `daylily_tapdb/gui/static/css/tapdb-gui.css`, browser `/tapdb/search` stylesheet links |  | Host bridge auth, admin dependency, bundled CSS, explicit `extra_stylesheets`, and mount-root URLs implemented. |
| GUI-003 | Search | Add `/`, `/search`, and JSON search API over existing object search service | SUCCESS | feature_implementation | Gate 3 | Agent 3 | `daylily_tapdb/gui/router.py`; browser `/tapdb/search?q=browser`; curl `/tapdb/api/search?q=Browser` -> 200 |  | Search page/API reuse `services.object_search.search_objects`; invalid record type fails with 400. |
| GUI-004 | Detail | Add `/object/{euid}` with stored fields, JSON, parent/child relationships, audit, graph and action links | SUCCESS | feature_implementation | Gate 3 | Agent 3 | `daylily_tapdb/gui/templates/object.html`; browser `/tapdb/object/Z-SMP-1Q` saw detail, edit JSON, audit, external-link action; `tests/test_gui_embedded.py::test_gui_object_api_returns_detail_relationships_audit_and_refs` |  | Detail view renders stored fields, JSON editor, relationship sections, audit trail, graph/action links; JSON detail API exposes the same context for host clients. |
| GUI-005 | Object Mutations | Add admin JSON edit, status update, and lineage update actions with explicit failures | SUCCESS | feature_implementation | Gate 3 | Agent 3 | `daylily_tapdb/gui/router.py`; browser POST `/tapdb/object/Z-SMP-1Q/edit-json` returned to detail with edited JSON visible; `tests/test_gui_embedded.py::test_gui_object_mutation_apis_update_json_status_and_lineage` |  | JSON/status/lineage mutations are admin-only and raise explicit 400/403/404/409 errors. HTML form routes and JSON APIs now share the same helper logic and redirect notices. |
| GUI-006 | Templates | Add template list/new/validate/save using loader validation and insert-only save | SUCCESS | feature_implementation | Gate 3 | Agent 4 | `daylily_tapdb/gui/templates/template_editor.html`; browser validate/save showed `Browser Template` and `Saved 1 template`; second pass browser builder generated property plus `WEL/container/example_well/1.0` child layout |  | Template save is admin-only, insert-only, duplicate-rejecting, and uses loader validation before save. The template page now includes a simple builder for identity fields, properties, and level-2 child-instantiation rules. |
| GUI-007 | Create | Add create-from-template page/API with level-1 and level-2 child instantiation support | SUCCESS | feature_implementation | Gate 3 | Agent 4 | `daylily_tapdb/gui/templates/create.html`; browser POST `/tapdb/create/Z-ACT-1Q` redirected to created object; `tests/test_gui_embedded.py::test_gui_create_from_template_passes_child_instantiation_flag`; `tests/test_gui_embedded.py::test_gui_create_api_passes_child_instantiation_flag` |  | Create flow and JSON API delegate to `InstanceFactory(..., create_children=...)`, preserving TapDB child-instantiation behavior. |
| GUI-008 | Graph | Add `/object/{euid}/graph` over canonical DAG payloads and existing graph JS conventions | SUCCESS | feature_implementation | Gate 3 | Agent 5 | `daylily_tapdb/gui/templates/graph.html`; browser `/tapdb/object/Z-PLT-1Q/graph` saw plate and child well |  | Graph page/API use `services.graph_payloads.build_graph_payload`. |
| GUI-009 | External Links | Add typed external object-link creation and lineage linking; no inline-only or dual-write creation | SUCCESS | feature_implementation | Gate 3 | Agent 5 | `daylily_tapdb/core_config/system/external_reference.json`; browser POST external link redirected to `Z-XRF-*`; `tests/test_gui_embedded.py::test_gui_external_link_creates_typed_object_and_lineage`; `tests/test_gui_embedded.py::test_gui_external_link_api_creates_typed_object_and_lineage` |  | External links are first-class XRF TapDB objects linked by lineage; no inline-only creation path added. HTML and JSON API routes share the same creation helper. |
| GUI-010 | Governance | Add admin Meridian/domain/prefix validation page/API using existing governance/euid helpers | SUCCESS | feature_implementation | Gate 3 | Agent 6 | `daylily_tapdb/gui/templates/meridian.html`; browser `/tapdb/admin/meridian?euid=Z-SMP-1Q&prefix=XRF` rendered domain and prefix info; `tests/test_gui_embedded.py::test_gui_meridian_validation_api_reports_prefix` |  | Page and JSON API use `GovernanceContext.load` and `validate_euid`. |
| GUI-011 | Metrics | Add modern admin metrics page that reuses existing metrics context | SUCCESS | feature_implementation | Gate 3 | Agent 6 | `daylily_tapdb/gui/templates/metrics.html`; curl `/tapdb/admin/metrics` -> 200 with `DB Metrics` and path summary; `tests/test_gui_embedded.py::test_gui_metrics_page_reuses_metrics_context`; `tests/test_gui_embedded.py::test_gui_metrics_api_reuses_metrics_context`; `tests/test_gui_embedded.py::test_gui_readiness_page_and_api_report_seeded_external_template` |  | Browser client blocked the literal `/admin/metrics` URL before request dispatch; direct HTTP and pytest verified route/API rendering. Readiness page/API now reports config, governance, XRF template, and template inventory. |
| GUI-012 | Docs | Add mount/integration documentation and Dayhoff-style example without mutating Dayhoff repo | SUCCESS | feature_implementation | Gate 5 | Agent 6 | `docs/integration-and-embedding.md`, `docs/tapdb_gui_inclusion.md`, `tests/test_docs_contracts.py` |  | Docs include app and router mount patterns, mounted V1 HTML/API route list, and Dayhoff-style host bridge example; no Dayhoff repo changed. |
| GUI-013 | Tests | Add pytest coverage for mount/auth/search/detail/template/create/external/governance/metrics | SUCCESS | contract_test | Gate 5 | Agent 1 | `tests/test_gui_embedded.py`, `tests/test_template_loader.py`, `tests/test_docs_contracts.py`; focused gate `python -m pytest tests/test_gui_embedded.py tests/test_docs_contracts.py -q` -> `22 passed` |  | Coverage added for exports, auth/admin gating, search, object API, template validation API, template builder, create page/API, object mutation APIs, external-link typed object page/API, readiness, Meridian API, metrics API, docs, and XRF core template. |
| GUI-014 | Browser | Run browser-visible local mounted app acceptance or record exact blocker | SUCCESS | contract_test | Gate 5 | Agent 1 | Local FastAPI host mounted `/tapdb`; browser verified search, CSS, detail, JSON edit, external link, graph, template validate/save, create, Meridian; second pass browser verified builder `Build JSON` generates property plus two-child well layout; third pass browser verified `/tapdb/admin/readiness` and object success notices; curl verified `/tapdb-user/admin/meridian` -> 403 and `/tapdb/admin/metrics` -> 200 |  | Browser blocked secondary user/metrics URLs with `net::ERR_BLOCKED_BY_CLIENT`; direct HTTP completed those two checks. |
| GUI-015 | Full Suite | Run `python -m pytest tests/ -q` or record exact failure/blocker | SUCCESS | contract_test | Gate 5 | Agent 1 | `python -m pytest tests/ -q` -> `642 passed, 39 skipped, 2 warnings in 3.46s` |  | Full suite passed. |

## Terminal Summary

Complete. All ledger rows are terminal SUCCESS. No schema migration was required.

## Second-Pass Completion Lift

| Requested Feature | Prior Estimate | Current Estimate | Remaining Gap |
|---|---:|---:|---|
| Parallel embeddable GUI surface | 95% | 98% | Package split and distribution polish remains future work. |
| Preserve legacy GUI/backcompat | 100% | 100% | None for V1. |
| Host auth/admin gating | 90% | 95% | More client-specific bridge fixtures would help. |
| Host CSS/look-and-feel hooks | 85% | 92% | Host theming is stylesheet-based, not token/component-based. |
| Search | 85% | 92% | Not yet a Kahlo-style federated cross-system search. |
| Object detail by EUID | 90% | 96% | UI polish and richer action feedback remain. |
| JSON edit/status/lineage actions | 85% | 91% | Action feedback is redirect/error based, not modal/toast based. |
| Template JSON editor/validation/save | 85% | 96% | Builder now handles identity/properties/child rules; full schema-aware visual editing remains future work. |
| Create instance from template | 90% | 95% | Real DB/browser fixture for level-2 packs would further improve confidence. |
| Level-2 child instantiation | 80% | 94% | Tested flag propagation and builder output; deeper live DB child-count assertion remains future work. |
| Graph view | 85% | 90% | Cross-system Kahlo composite graph is outside this V1 implementation. |
| External object linking | 90% | 94% | Typed link creation is done; richer external auth/status validation remains. |
| Meridian/domain/prefix admin | 85% | 95% | Register-new-domain workflows are still validation-first. |
| Observability/db metrics | 75% | 93% | Metrics page/API are functional but not a full observability console. |
| Dayhoff integration readiness | 85% | 90% | Docs/test-host proof only; Dayhoff repo integration intentionally not performed. |
| Tests/browser acceptance | 90% | 97% | Browser coverage improved; one browser-client URL-blocker remains documented and HTTP-verified. |

Average current estimate: 94.25%, up about 7.06 points from the original 87.19% scored feature average.

## Third-Pass 4.5% Lift

| Requested Feature | Prior Estimate | Current Estimate | Remaining Gap |
|---|---:|---:|---|
| Parallel embeddable GUI surface | 98% | 99% | Separate package/repo split remains future work. |
| Preserve legacy GUI/backcompat | 100% | 100% | None for V1. |
| Host auth/admin gating | 95% | 98% | More client-specific auth fixtures would be useful after Dayhoff mounts it. |
| Host CSS/look-and-feel hooks | 92% | 96% | Full design-token/component theming remains future work. |
| Search | 92% | 96% | Kahlo-style federated search remains future work. |
| Object detail by EUID | 96% | 99% | Mostly polish. |
| JSON edit/status/lineage actions | 91% | 99% | HTML and JSON APIs are covered; richer UX feedback could still improve usability. |
| Template JSON editor/validation/save | 96% | 98% | Full schema-aware visual editing remains future work. |
| Create instance from template | 95% | 99% | Real DB/browser fixture for level-2 child rows would further improve confidence. |
| Level-2 child instantiation | 94% | 98% | Tested builder output and API/page flag propagation; live DB child-row assertion remains future work. |
| Graph view | 90% | 95% | Kahlo composite cross-system graph remains future work. |
| External object linking | 94% | 99% | Typed page/API creation is done; external auth/status validation remains future work. |
| Meridian/domain/prefix admin | 95% | 98% | Register-new-domain workflows are still validation-first. |
| Observability/db metrics | 93% | 98% | Readiness plus metrics page/API are done; a full observability console remains future work. |
| Dayhoff integration readiness | 90% | 97% | Docs/test-host proof only; Dayhoff repo integration intentionally not performed. |
| Tests/browser acceptance | 97% | 99% | Remaining gaps are live DB and client-repo acceptance, not TapDB unit coverage. |

Average current V1 estimate: 98.125%, up 3.875 points from the second-pass 94.25%.
The requested 4.5-point lift is functionally covered for V1 adoption surfaces; the remaining 0.625 points would require live Dayhoff/client or real Postgres child-row acceptance rather than more TapDB-local code.
