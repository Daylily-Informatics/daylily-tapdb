# Integration and Embedding

This guide lives in the `tapdb-core` repository. The Python import package remains `daylily_tapdb`.

TAPDB can run as a standalone substrate, or it can be embedded inside a larger application. The tapdb-core codebase supports both patterns, but the responsibilities stay sharply divided.

## What TAPDB Owns

TAPDB owns the substrate layer:

- polymorphic template and instance persistence
- lineage and traversal
- audit history
- transactional outbox and inbox handling
- admin UI mounting and auth plumbing
- CLI-driven database lifecycle

The current object model and relationships are implemented in [`daylily_tapdb/models`](../daylily_tapdb/models) and the outbox/inbox helpers in [`daylily_tapdb/outbox`](../daylily_tapdb/outbox).

## Reusable Web Surface

TapDB now exposes a reusable library surface in
[`daylily_tapdb.web`](../daylily_tapdb/web):

- `create_tapdb_web_app(...)` for the full mounted HTML/UI surface
- `create_tapdb_gui_app(...)` for the new embeddable GUI V1 surface
- `create_tapdb_gui_router(...)` when a host app wants to include only the GUI router
- `create_tapdb_dag_router(...)` for canonical root-level `/api/dag/*` routes
- `TapdbHostBridge` for host auth, shell links, template overrides, and host CSS
- `build_dag_capability_advertisement(...)` for `obs_services`-style discovery

Install the GUI extra before importing the V1 embeddable GUI in a host app:

```bash
pip install "daylily-tapdb[gui]"
```

Use `daylily-tapdb[admin]` instead when the host also needs the legacy TapDB
admin UI or TapDB-native browser auth.

The supported embedding pattern is:

```python
from fastapi import Depends, FastAPI

from daylily_tapdb.web import (
    TapdbHostBridge,
    TapdbHostNavLink,
    create_tapdb_dag_router,
    create_tapdb_gui_app,
    create_tapdb_web_app,
)

app = FastAPI()
bridge = TapdbHostBridge(
    auth_mode="host_session",
    service_name="dewey",
    app_name="Dewey",
    home_url="/ui",
    login_url="/login",
    logout_url="/auth/logout",
    nav_links=(TapdbHostNavLink(label="Dashboard", href="/ui"),),
    extra_stylesheets=("/static/console.css",),
    resolve_user=my_host_user_resolver,
)

app.mount(
    "/tapdb",
    create_tapdb_gui_app(
        config_path="/abs/path/to/tapdb-config.yaml",
        host_bridge=bridge,
    ),
)
app.include_router(
    create_tapdb_dag_router(
        config_path="/abs/path/to/tapdb-config.yaml",
        service_name="dewey",
    ),
    dependencies=[Depends(my_session_or_service_auth)],
)
```

`create_tapdb_web_app(...)` remains available for the legacy full admin surface.
Use `create_tapdb_gui_app(...)` for the V1 client-embeddable GUI pages.

The standalone `tapdb ui start` path also builds on this same factory.

## V1 Embedded GUI Routes

When mounted at `/tapdb`, the V1 GUI exposes both HTML pages and JSON APIs:

- `GET /tapdb/search` and `GET /tapdb/api/search`
- `GET /tapdb/object/{euid}` and `GET /tapdb/api/object/{euid}`
- `GET /tapdb/object/{euid}/graph` and `GET /tapdb/api/object/{euid}/graph`
- `POST /tapdb/api/object/{euid}/edit-json`
- `POST /tapdb/api/object/{euid}/status`
- `POST /tapdb/api/object/{euid}/lineage`
- `POST /tapdb/api/object/{euid}/external-links`
- `GET /tapdb/templates`, `GET /tapdb/templates/new`, and
  `POST /tapdb/api/templates/validate`
- `POST /tapdb/api/create/{template_euid}`
- `GET /tapdb/admin/readiness` and `GET /tapdb/api/admin/readiness`
- `GET /tapdb/admin/meridian` and
  `GET /tapdb/api/admin/meridian/validate`
- `GET /tapdb/admin/metrics` and `GET /tapdb/api/admin/metrics`

The external-link API creates the same first-class typed TapDB external
reference object as the HTML form, then links it to the source object by
lineage. It does not create inline-only external references.

## Auth Modes

TapDB now supports two embedding stories:

- `tapdb` auth in TapDB config when TapDB owns its own session/login flow
- `host_session` through `TapdbHostBridge` when a parent app owns browser auth

The older `shared_host` cookie-decoding mode still exists inside the admin app,
but it is no longer the preferred host integration pattern.

Practical guidance:

- Use `TapdbHostBridge(auth_mode="host_session", ...)` when the parent app
  already authenticates operators and wants `/tapdb` to inherit host auth and
  host chrome.
- Use TapDB-native auth only when TapDB should own its own login screens.

## Client Repository Responsibilities

Client repos should own:

- domain semantics
- domain template packs
- workflow semantics
- integration adapters
- business-specific tests and fixtures

TapDB should not become the place where domain meaning lives. It is the persistence and object model substrate, not the application authority.

That split is consistent with the current core config pack in [`daylily_tapdb/core_config`](../daylily_tapdb/core_config) and with the template loading policy in [`daylily_tapdb/templates/loader.py`](../daylily_tapdb/templates/loader.py).

## Extension Boundaries

Use TAPDB as the substrate, but keep domain behavior in the owning application repo.

TapDB is a good fit for:

- reusable object models
- lineage-based relationships
- versioned templates
- audit and outbox persistence
- shared admin/runtime management

Application repos should own:

- customer or lab workflow semantics
- domain-specific template packs
- external integrations
- access policy beyond TAPDB substrate rules
- user-facing behavior that is not generic enough to belong here

That line matters because TAPDB has been refactored into a reusable base layer. The docs should explain the base layer clearly, but not blur the boundary back into application logic.

## What Not To Outsource

Application repos should not outsource these responsibilities to TAPDB:

- the meaning of domain objects
- the business meaning of EUIDs
- the choice of workflow states
- the shape of domain-specific JSON payloads
- the orchestration of external APIs

TAPDB can store, validate, and expose those objects. It should not decide what they mean.

## Dewey Reference Pattern

Dewey is the reference adopter for this embedding model:

- mounted HTML surface at `/tapdb`
- root-level DAG contract at `/api/dag/*`
- Dewey session or bearer-token auth guarding the root DAG API
- host shell link and CSS integration through `TapdbHostBridge`

## Dayhoff-Style Host Example

This is a TapDB-side example only. It does not require mutating a Dayhoff repo.

```python
from fastapi import Depends, FastAPI, Request

from daylily_tapdb.web import (
    TapdbHostBridge,
    TapdbHostNavLink,
    create_tapdb_dag_router,
    create_tapdb_gui_app,
)


def resolve_operator(request: Request) -> dict | None:
    user = request.session.get("operator")
    if not user:
        return None
    return {
        "username": user["email"],
        "email": user["email"],
        "display_name": user.get("name") or user["email"],
        "role": user.get("role", "user"),
    }


def require_dayhoff_api_user():
    ...


app = FastAPI()
bridge = TapdbHostBridge(
    auth_mode="host_session",
    service_name="dayhoff",
    app_name="Dayhoff",
    home_url="/",
    login_url="/login",
    logout_url="/logout",
    nav_links=(TapdbHostNavLink(label="Dashboard", href="/"),),
    extra_stylesheets=("/static/dayhoff.css",),
    resolve_user=resolve_operator,
)

app.mount(
    "/tapdb",
    create_tapdb_gui_app(
        config_path="/abs/path/to/tapdb-config.yaml",
        host_bridge=bridge,
    ),
)
app.include_router(
    create_tapdb_dag_router(
        config_path="/abs/path/to/tapdb-config.yaml",
        service_name="dayhoff",
    ),
    dependencies=[Depends(require_dayhoff_api_user)],
)
```

## Related Materials

- [`docs/dag_spec.md`](./dag_spec.md)
- [`docs/runtime-and-cli.md`](./runtime-and-cli.md)
- [`docs/tapdb_gui_inclusion.md`](./tapdb_gui_inclusion.md)
