# Integration and Embedding

TapDB 10 has one web implementation, `daylily_tapdb.gui`. The same factory
powers the standalone `tapdb ui` process and a GUI mounted inside a FastAPI
service. There is no separate `admin.main` application.

## Ownership boundary

TapDB owns typed persistence, lineage, audit, canonical external references,
transactional inbox/outbox state, schema lifecycle, backup/recovery, DAG v2,
and reusable GUI/API surfaces.

The host owns domain semantics, business policy, its session or service
credentials, external-system validation, fleet admission, and its surrounding
UI.

## Install

~~~bash
python -m pip install "daylily-tapdb[gui]"
~~~

The `gui` extra includes standalone server, forms/session, TapDB-native auth,
and host-embedding dependencies. There is no `admin` extra.

## Canonical mount

~~~python
from fastapi import FastAPI, Request

from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.web import (
    DagV2Limits,
    TapdbHostBridge,
    TapdbHostNavLink,
    mount_tapdb_dag_surfaces,
)


def resolve_operator(request: Request) -> dict | None:
    operator = request.session.get("operator")
    if not operator:
        return None
    return {
        "username": operator["email"],
        "email": operator["email"],
        "display_name": operator.get("name") or operator["email"],
        "role": operator.get("role", "user"),
    }


app = FastAPI()
bridge = TapdbHostBridge(
    auth_mode="host_session",
    service_name="dewey",
    app_name="Dewey",
    home_url="/ui",
    login_url="/login",
    logout_url="/auth/logout",
    change_password_url="/account/password",
    nav_links=(TapdbHostNavLink(label="Dashboard", href="/ui"),),
    extra_stylesheets=("/static/console.css",),
    resolve_user=resolve_operator,
)

app.mount(
    "/tapdb",
    create_tapdb_gui_app(
        config_path="/abs/path/to/tapdb-config.yaml",
        host_bridge=bridge,
    ),
)

dag_mount = mount_tapdb_dag_surfaces(
    app,
    config_path="/abs/path/to/tapdb-config.yaml",
    service_id="dewey",
    display_name="Dewey",
    auth_dependency=require_session_or_service_user,
    limits=DagV2Limits(max_depth=6, max_nodes=500, max_search_page_size=100),
)
if not dag_mount.mounted:
    raise RuntimeError(f"DAG v2 unavailable: {dag_mount.reason}")
~~~

Mounting the GUI under `/tapdb` keeps HTML and management JSON namespaced.
Mount DAG v2 at the service root so a fleet client can use the exact paths in
the manifest.

A host may include `create_tapdb_gui_router(...)` directly only when it already
owns the surrounding FastAPI application and middleware. It is the router from
the same implementation, not a second feature set.

## GUI and JSON capability map

Full former-admin feature parity is a release contract, not a best-effort
migration target. The only exclusions are the explicitly removed DAG-v1 proxy
and duplicate external-link writer.

The canonical GUI includes all valid capabilities formerly served by
`admin.main`:

| Capability | Representative HTML | Representative JSON |
|---|---|---|
| Auth/account | `/login`, `/signup`, `/change-password` | session/auth flow |
| Overview | `/admin/overview` | `/api/admin/overview` |
| Search | `/search` | `/api/search` |
| Rich graph explorer | `/graph`, `/object/{euid}/graph` | `/api/graph`, `/api/object/{euid}/graph` |
| Object detail | `/object/{euid}` | `/api/object/{euid}` |
| Create from template | `/create/{template_euid}` | `/api/create/{template_euid}` |
| Governed update/delete | object forms | `PATCH`/`DELETE /api/objects/{euid}` |
| Lineage | object form | `POST /api/object/{euid}/lineage` |
| Assessment/repair | object form | assess, revalidate, recommendations, repair APIs |
| Template repository | `/templates`, `/templates/new` | validate/import/download APIs |
| Audit | `/audit` | `/api/audit` |
| Readiness | `/admin/readiness` | `/api/admin/readiness` |
| Inventory | `/admin/inventory` | `/api/admin/inventory` |
| Meridian | `/admin/meridian` | `/api/admin/meridian/validate` |
| Metrics/runtime | `/admin/metrics`, `/admin/runtime` | corresponding management JSON endpoints |
| Backup/recovery | `/admin/backups` | `/api/admin/backups/*` |

Search preserves the former advanced-query combinations: free text, independent
name/EUID contains filters, exact category/type/subtype filters, record kind,
and forward cursor pagination across templates, instances, and lineage.

The graph explorer retains fuzzy and exact find, degree transparency, distance
filtering, type visibility, subtype muting, multiple layouts, child/parent
waves, neighborhood highlighting, admin lineage creation and soft deletion,
object/edge detail, DAG JSON download, and Mermaid export. It uses canonical
DAG-v2 builders only. External references and opaque identifiers are displayed
read-only.

Every JSON request without a user fails with `401`. Every HTML request without
a user redirects to login. Mutations require an administrator. Governed object
APIs default to dry-run and require explicit apply where documented.

## Auth modes

### TapDB-native auth

Use `admin.auth.mode: tapdb` when TapDB should own login, signup, password
challenge, logout, and Cognito Hosted UI flows. Production-like targets require
a stable session secret and HTTPS-safe configuration.

### Host session bridge

Use `TapdbHostBridge(auth_mode="host_session", ...)` when the parent service
already owns browser auth. The bridge resolves and normalizes the current user,
gates every mounted route, redirects anonymous HTML, returns JSON `401` for
anonymous API requests, and injects only the normalized identity into TapDB.

The host can add navigation, stylesheets, template override directories, and
request-local display context. Those features do not grant storage or
authorization authority.

### Disabled auth

`admin.auth.mode: disabled` is local-development-only. The GUI refuses
production-like configurations that disable auth.

The old shared-cookie decoder is not an embedding fallback. Prefer a host
resolver that validates its own session and supplies a normalized user.

## DAG v2 auth

The GUI bridge and root DAG API can use different callables because browser and
service-to-service auth are often different. Every DAG endpoint uses the
explicit `auth_dependency` provided at mount. The dependency must return a
stable actor identity or reject the request with `401`/`403`.

The DAG mount has no anonymous path, outbound proxy, or credential-forwarding
behavior.

## External-reference integration

Consumer code creates cross-service relationships through
`daylily_tapdb.external_references.ExternalReferenceService` inside its own
transaction. Do not submit an external-link HTML form or write an XRF through a
generic endpoint.

Remote validation and synchronization remain in the host. TapDB stores exact
target identity and source-to-XRF lineage and projects it through DAG v2.

For global search and graph composition, the host or Kahlo supplies an exact
fleet plus an authenticated `DagV2Transport` to
`DagV2FederationClient`. TapDB does not discover services or credentials.

## Backup/recovery

The canonical HTTP prefix is `/api/admin/backups`. HTML and JSON call the same
backup service and staged restore review path, so they cannot implement
different recovery rules. See [backup and recovery](backup-and-recovery.md).

## Standalone GUI

`tapdb --config <path> ui start` launches the same
`create_tapdb_gui_app(...)` factory with explicit config context. Status, logs,
restart, and stop remain under `tapdb ui`.

## Dayhoff-Style Host Example

The canonical mount above is the Dayhoff-style pattern: host-owned session,
host shell integration, TapDB under `/tapdb`, and root authenticated DAG v2.
It does not require mutating a Dayhoff repo; each consumer migration is separate
work.

## Removed integration paths

TapDB 10 deliberately omits:

- `admin.main` and its templates/static application;
- `create_tapdb_web_app`;
- a separate `admin` package extra;
- DAG v1 and its manifest/router;
- outbound external-graph proxying;
- URL/auth routing metadata;
- metadata-derived graph edges;
- embedded GUI external-link creation;
- generic XRF writes;
- compatibility aliases and fallbacks.

Use the [GUI inclusion guide](tapdb_gui_inclusion.md),
[DAG contract](dag_spec.md), and
[external-reference and federation guide](external-references-and-federation.md)
for focused contracts.
