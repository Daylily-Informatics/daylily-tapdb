# TapDB Core Refactor Spec for Codex IDE

## Goal

Refactor `daylily-tapdb` into a cleaner internal architecture without splitting it into multiple repos or fully separate products.

The desired outcome is:

- keep TapDB as the substrate plus canonical reusable API surface
- separate reusable API logic from admin GUI implementation details
- keep the admin GUI as a higher layer that depends on shared services
- rename the GitHub repo to `tapdb-core`
- **do not** widen scope into schema redesign, workflow redesign, or downstream application refactors unless needed for compatibility tests

This is an internal architecture cleanup and packaging cleanup, not a product rewrite.

---

## Decision Summary

### What we are doing

1. **Keep the API bundled with TapDB**
   - The canonical DAG/object API belongs with the substrate.
   - We are **not** divorcing the API from the database into a separate repo or separate product.

2. **Separate GUI concerns from reusable API concerns**
   - The admin HTML/Jinja/session app is not the same thing as the reusable DAG API.
   - Reusable DAG/object routes must not import `admin.main` or depend on the admin app module.

3. **Modularize more internally, not organizationally**
   - Add a shared service layer inside the repo.
   - Add a new optional dependency extra for the reusable API layer.
   - Keep one repo and one main Python package for now.

4. **Rename the repo to `tapdb-core`**
   - This rename is at the repository/docs identity level in this refactor.
   - **Do not rename the Python import package** `daylily_tapdb` in this PR.
   - **Do not rename the published distribution package** unless explicitly required as part of release work. That can be a later coordinated change.

### Why this is the right cut

The current code already presents TapDB as a reusable substrate with embedding support, but the reusable DAG router currently imports and reloads `admin.main`. That is the wrong dependency direction. The fix is to move shared query/graph/external-ref logic into library modules and have both the admin app and the DAG router depend on those modules.

---

## Current Problems To Fix

These are the important current seams:

1. **Reusable DAG API depends on admin app**
   - `daylily_tapdb/web/dag.py` imports `admin.main` dynamically via `_load_admin_main(...)`.
   - That makes the supposed reusable API depend on the full admin module.

2. **Shared query helpers live in admin.main**
   - `_find_object_by_euid(...)`
   - `_external_ref_payloads(...)`
   - related object/detail helpers
   - These should live in reusable service modules, not in the UI app module.

3. **External graph logic is stranded in the admin layer**
   - `admin/external_graph.py` is not really UI logic.
   - It is reusable reference-resolution / proxy / namespacing logic and should be moved under the TapDB library surface.

4. **Packaging conflates API and GUI**
   - `fastapi` is only available via the current `admin` extra.
   - But the DAG router is part of the reusable substrate contract, not just the admin GUI.
   - We need a new optional extra for the reusable API layer.

5. **Docs overstate decoupling**
   - Docs say TapDB exposes a reusable web/API surface.
   - That is directionally true, but not yet true in dependency structure.
   - Code and docs need to match.

---

## Non-Goals

Do **not** do any of the following in this refactor:

- no database schema redesign
- no EUID / Meridian redesign
- no domain template redesign
- no outbox contract redesign
- no client app behavior changes in Atlas, Bloom, Dewey, or Ursa beyond import/path compatibility if tests require it
- no full rename of the Python import package from `daylily_tapdb` to `tapdb_core`
- no mandatory move to a separate `tapdb-gui` repo or separate distribution

---

## Required Architectural End State

After this refactor, the dependency direction must look like this:

```text
admin UI / HTML app
    -> shared services
    -> core models / connection / templates / outbox / lineage

reusable DAG API router
    -> shared services
    -> core models / connection / templates / outbox / lineage

core substrate
    -> no dependency on admin UI module
```

### Hard rule

`daylily_tapdb/web/dag.py` must no longer import `admin.main`, directly or indirectly through a runtime reload hook.

---

## Packaging Target

Keep a single repo.

Keep a single main Python import package: `daylily_tapdb`.

Add a new optional dependency extra:

- `api`
  - contains dependencies needed for the reusable DAG/API surface
  - at minimum `fastapi`
  - add anything else strictly required for the reusable API layer

Keep `admin` as a separate optional extra, but make it conceptually sit on top of `api`:

- `admin`
  - includes `fastapi` and admin-specific dependencies
  - includes `uvicorn`, `jinja2`, `python-multipart`, `itsdangerous`, `daylily-auth-cognito`, password/auth deps, etc.

### Packaging rules

- core install should continue to support non-web substrate usage
- `pip install -e .[api]` should support importing and mounting the canonical DAG router
- `pip install -e .[admin]` should support the full admin UI / mounted HTML surface
- CLI command `tapdb` must continue to work

---

## Repo Rename Rules

We are renaming the repo identity to `tapdb-core`.

In this PR:

- update README title and descriptive text to use `tapdb-core` as the repo name
- update repository/homepage/documentation URLs that clearly refer to the GitHub repo name
- keep Python import paths as `daylily_tapdb`
- keep internal module names unchanged unless needed for the service split
- keep CLI command name `tapdb`

Do **not** mass-rename `daylily_tapdb` imports in this PR.

If there are places where repo identity and Python package identity are both mentioned, make the distinction explicit.

---

## Concrete Refactor Plan

### 1. Introduce shared reusable service modules

Create a new package subtree for shared non-UI logic. Suggested structure:

```text
daylily_tapdb/services/
    __init__.py
    object_lookup.py
    graph_payloads.py
    external_refs.py
```

#### `object_lookup.py`
Move reusable lookup logic here, including:

- exact object lookup by EUID across:
  - `generic_template`
  - `generic_instance`
  - `generic_instance_lineage`

This module should expose a clean function such as:

- `find_object_by_euid(session, euid) -> tuple[obj | None, record_type | None]`

This should become the canonical implementation used by both:
- the admin app
- the DAG router

#### `external_refs.py`
Move or copy the reusable parts of `admin/external_graph.py` here:

- `ExternalGraphRef`
- `resolve_external_graph_refs(...)`
- `get_external_ref_by_index(...)`
- `fetch_remote_graph(...)`
- `fetch_remote_object_detail(...)`
- `namespace_external_graph(...)`

This module is library logic, not admin-page logic.

If there is code in `admin/external_graph.py` that is truly admin-page-specific, keep only that thin layer there.

#### `graph_payloads.py`
Move reusable graph/object payload shaping here:

- object detail payload building
- node payload building
- lineage edge payload building
- graph traversal payload building

This module should not know about FastAPI routes or HTML rendering.
It should accept model objects and return serializable dict payloads.

### 2. Refactor `daylily_tapdb/web/dag.py`

Refactor the canonical DAG router so it depends only on:

- TapDB runtime/context helpers
- shared service modules
- FastAPI
- DB connection/session helpers

It must **not** depend on `admin.main`.

#### Requirements

- remove `_load_admin_main(...)`
- stop using `importlib.reload(admin.main)`
- stop calling helpers via `admin_main._find_object_by_euid`
- stop calling helpers via `admin_main._external_ref_payloads`
- use shared service modules directly
- preserve the existing public route contract:
  - `GET /api/dag/object/{euid}`
  - `GET /api/dag/data`
  - `GET /api/dag/external`
  - `GET /api/dag/external/object`

#### Runtime access

Choose one of these low-risk patterns:

**Preferred**
- use the existing TapDB config/env context and get a connection via the canonical DB access helper directly

or

**Acceptable**
- add a small internal runtime adapter module under `daylily_tapdb/web` or `daylily_tapdb/runtime` that resolves the configured connection/session provider

Do not pull in the full admin app just to get a DB session.

### 3. Keep the admin app as a higher layer

`admin/main.py` should remain the entry point for the HTML/admin app for now, but it must consume shared library functions instead of being the source of truth for them.

#### Required changes

- replace local `_find_object_by_euid(...)` implementation with imports from `daylily_tapdb.services.object_lookup`
- replace local external-ref payload helpers with imports from `daylily_tapdb.services.external_refs`
- if helper names are part of internal tests, keep thin wrapper functions in `admin.main` temporarily, but the real implementation must live in the service modules

The admin app is allowed to keep:
- Jinja setup
- sessions
- CORS / trusted host setup
- admin auth
- route handlers
- page rendering
- admin form handling

### 4. Add `api` extra in `pyproject.toml`

Update packaging so reusable API support is not conceptually lumped into the GUI/admin extra.

#### Required outcome

- new `[project.optional-dependencies].api`
- `admin` should include or duplicate the dependencies from `api` plus admin-only deps
- package metadata and docs must explain:
  - core substrate install
  - API install
  - admin install

### 5. Update docs to match reality

Update docs so they describe the new layering accurately.

At minimum update:

- `README.md`
- `docs/integration-and-embedding.md`
- `docs/tapdb_gui_inclusion.md`
- `docs/dag_spec.md`
- any architecture or runtime docs that describe the current embedding pattern

#### Docs should make these distinctions explicit

- TapDB core substrate
- reusable API layer
- admin GUI layer
- repo name `tapdb-core`
- Python package/import name remains `daylily_tapdb` for now

### 6. Keep public import surfaces stable where reasonable

Preserve the public imports already documented, unless there is a compelling reason not to.

Examples to keep stable if possible:

```python
from daylily_tapdb.web import create_tapdb_dag_router
from daylily_tapdb.web import create_tapdb_web_app
from daylily_tapdb.web import TapdbHostBridge
```

Internal implementation can move, but the public surface should not churn unnecessarily.

### 7. Tests to update or add

#### Update existing tests

- `tests/test_web_dag.py`
  - stop monkeypatching `_load_admin_main(...)`
  - test the DAG router through the new service-layer or runtime dependency seam

- any tests that import `admin.main` only to reach shared helpers
  - update them to test the new shared service modules directly where appropriate

#### Add tests

Add focused unit tests for the new service modules:

- `tests/test_services_object_lookup.py`
- `tests/test_services_graph_payloads.py`
- `tests/test_services_external_refs.py`

#### Must preserve

- current route contract behavior for DAG endpoints
- current mounted admin UI behavior
- current embedding pattern for host apps
- current CLI start path for UI

---

## Expected File-Level Changes

This is the minimum expected touch set. Exact filenames can vary if the implementation is cleaner, but scope should remain comparable.

### Must change

- `pyproject.toml`
- `README.md`
- `docs/integration-and-embedding.md`
- `docs/tapdb_gui_inclusion.md`
- `docs/dag_spec.md`
- `daylily_tapdb/web/dag.py`
- `admin/main.py`

### Likely add

- `daylily_tapdb/services/__init__.py`
- `daylily_tapdb/services/object_lookup.py`
- `daylily_tapdb/services/graph_payloads.py`
- `daylily_tapdb/services/external_refs.py`

### Possibly change

- `admin/external_graph.py`
- `daylily_tapdb/web/__init__.py`
- `daylily_tapdb/web/factory.py`
- tests around web/admin embedding
- CLI/admin startup docs or helpers if packaging text changes

### Should not need to change

- database schema SQL
- template JSON packs
- lineage model semantics
- outbox core contracts
- client repos in this same overall tarball, except only if a test or import path explicitly breaks

---

## Acceptance Criteria

The refactor is complete only if all of the following are true.

### Architecture

- `daylily_tapdb/web/dag.py` does not import `admin.main`
- reusable DAG/object route behavior comes from shared library modules, not admin wrappers
- shared lookup/graph/external-ref logic is no longer owned by `admin.main`

### Packaging

- there is a distinct `api` extra
- `admin` remains installable and usable
- core installs still work for non-web users

### Compatibility

- the public `daylily_tapdb.web` imports still work unless there is a documented and necessary reason otherwise
- `tapdb ui start` still works
- mounted host-app inclusion still works

### Tests

- all existing relevant tests pass
- new service-layer tests exist and pass
- `tests/test_web_dag.py` no longer relies on monkeypatching `_load_admin_main`

### Docs

- docs accurately describe the new layering
- docs mention repo rename to `tapdb-core`
- docs distinguish repo identity from Python import package identity

---

## Implementation Notes

### On the repo rename

The repo rename to `tapdb-core` is fine.

For this refactor, treat it as a repository/documentation identity change, not an import-path migration.

A full Python package rename can be considered later, but it is a separate compatibility event with a much larger blast radius across Atlas, Bloom, Dewey, Ursa, tests, release packaging, and any external installs.

### On naming the new service modules

Use boring names. This is infrastructure, not branding.

Prefer names like:

- `object_lookup.py`
- `graph_payloads.py`
- `external_refs.py`

Avoid names that imply domain meaning.

### On keeping the API with TapDB

Do not create a separate repo like `tapdb-api`.

That would create version skew and turn a packaging problem into a distributed systems problem.

The correct move is:
- one repo
- one core import package
- internal service split
- optional extras for dependency layers

---

## Suggested Work Sequence

1. create the new `daylily_tapdb/services/` modules
2. move shared lookup / external-ref / graph-payload logic there
3. refactor `daylily_tapdb/web/dag.py` to use the new modules directly
4. refactor `admin/main.py` to consume the same shared modules
5. add the `api` extra in `pyproject.toml`
6. update tests
7. update docs and repo-name references
8. run targeted tests first, then full suite
9. ensure no accidental schema or behavior drift

---

## Commands To Run Before Finishing

Run the smallest useful set first, then the broader suite.

Suggested order:

```bash
pytest -q tests/test_web_dag.py
pytest -q tests/test_web_factory.py
pytest -q tests/test_admin_routes_smoke.py
pytest -q tests/test_external_graph_helpers.py
pytest -q tests/test_docs_contracts.py
pytest -q
```

Also run any local lint/format steps already standard in the repo.

---

## Final Deliverable Format

Open a single focused PR or a tightly-related PR stack with:

1. the service-layer extraction
2. DAG router decoupling
3. packaging extras cleanup
4. docs updates for `tapdb-core`

Do not hide this in unrelated cleanup.

In the PR description, include:

- before/after dependency direction
- list of moved helpers
- note that repo renamed to `tapdb-core` while Python import package stays `daylily_tapdb`
- note that no schema or domain semantics changed
