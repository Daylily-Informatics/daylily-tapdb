# TapDB GUI Inclusion Guide

TapDB 10 exposes one complete web stack from `daylily_tapdb.gui`. It can run
standalone or be mounted by a host; both forms contain the same features.

## Install

~~~bash
python -m pip install "daylily-tapdb[gui]"
~~~

The `gui` extra includes FastAPI, Uvicorn, Jinja, form/session support,
TapDB-native Cognito auth, and password hashing. There is no separate
`daylily-tapdb[admin]` extra.

## Mount under a host

~~~python
from fastapi import FastAPI

from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.web import TapdbHostBridge

app = FastAPI()
bridge = TapdbHostBridge(
    auth_mode="host_session",
    service_name="dewey",
    app_name="Dewey",
    home_url="/ui",
    login_url="/login",
    logout_url="/auth/logout",
    resolve_user=my_host_user_resolver,
)
app.mount(
    "/tapdb",
    create_tapdb_gui_app(
        config_path="/abs/path/to/tapdb-config.yaml",
        host_bridge=bridge,
    ),
)
~~~

Use `create_tapdb_gui_router(...)` only when the host must include the same
router directly rather than mount the app. It is not a reduced UI.

## Feature contract

The canonical GUI includes:

- TapDB-native login, signup, OAuth callback, logout, password change, and
  first-login password challenge;
- host-session integration;
- overview and SQL-bounded, cursor-paginated search with independent name/EUID
  contains and exact category/type/subtype filters;
- templates, repository import/download, and create-from-template;
- object detail, governed update/delete, assessment, revalidation, and repair;
- lineage creation and inspection;
- audit exploration;
- inventory, readiness, Meridian, metrics, and runtime views;
- backup create/list/verify, staged restore/apply, and rehearsal;
- rich lineage graph exploration;
- canonical read-only external-reference and opaque-identifier display.

The graph page retains the useful former admin experience: fuzzy search, exact
find, degree transparency, relative-distance filtering, type visibility,
subtype muting, Dagre/CoSE/breadth-first/circle/grid layouts, parent/child
waves, neighborhoods, administrator lineage creation and soft deletion,
details, JSON download, and Mermaid output.

DAG v1 proxy/merge and the duplicate external-link writer are not features of
TapDB 10.

## Authentication

### Host session

`TapdbHostBridge` calls the host's `resolve_user(request)`. Return at least
`username` or `email` and optionally display name, role, active state, and
password-change state. Roles normalize to `admin` or `user`.

Anonymous HTML redirects to the host login. Anonymous JSON returns `401`.
Administrative pages and mutations return `403` for a non-admin. Health and
readiness probes remain available as configured.

### TapDB-native

Configure `admin.auth.mode: tapdb` when TapDB owns browser auth. Hosted Cognito
and username/password login use the same canonical app. Production-like
targets require an explicit stable session secret and cannot disable auth.

### Disabled

`admin.auth.mode: disabled` is accepted only for local development and
diagnostics. Do not use it in a production-like target.

## Root DAG API

The GUI does not move the service discovery contract under its mount. Publish
root DAG v2 separately:

~~~python
from daylily_tapdb.web import DagV2Limits, mount_tapdb_dag_surfaces

result = mount_tapdb_dag_surfaces(
    app,
    config_path="/abs/path/to/tapdb-config.yaml",
    service_id="dewey",
    display_name="Dewey",
    auth_dependency=require_service_or_user,
    limits=DagV2Limits(max_depth=6, max_nodes=500, max_search_page_size=100),
)
if not result.mounted:
    raise RuntimeError(result.diagnostic)
~~~

The manifest, exact lookup, graph, and search endpoints all use that explicit
auth dependency.

## Canonical management routes

When mounted at `/tapdb`, representative routes are:

- `GET /tapdb/search` and `GET /tapdb/api/search`;
- `GET /tapdb/object/{euid}` and `GET /tapdb/api/object/{euid}`;
- `GET /tapdb/graph` and `GET /tapdb/api/graph`;
- `POST /tapdb/api/object/{euid}/lineage`;
- `POST /tapdb/api/object/{euid}/repairs`;
- `POST /tapdb/api/create/{template_euid}`;
- `GET /tapdb/templates`;
- `GET /tapdb/audit` and `GET /tapdb/api/audit`;
- `GET /tapdb/admin/readiness`;
- `GET /tapdb/api/admin/readiness`;
- `GET /tapdb/admin/inventory`;
- `GET /tapdb/admin/meridian`;
- `GET /tapdb/admin/metrics`;
- `GET /tapdb/admin/runtime`;
- `GET /tapdb/admin/backups` and `/tapdb/api/admin/backups/*`.

The exact route table is tested as a release contract. Do not add an alternate
prefix for compatibility.

## External references

The GUI displays canonical `external_refs` and `external_identifiers` but does
not create them. Applications use `ExternalReferenceService` inside an explicit
transaction. This prevents a second, duplicate-prone writer from diverging
from lifecycle and identity rules.

## Runtime checks

Verify:

- the root redirects or renders according to the chosen auth mode;
- anonymous JSON is `401`;
- user and admin navigation differ correctly;
- the Graph page renders a Cytoscape canvas and its non-JavaScript tables;
- every admin mutation is rejected for a user;
- `/api/admin/readiness` reports config, schema, templates, and governance;
- backup HTML and JSON produce the same service receipts;
- shutdown disposes GUI, metrics, auth, and runtime database pools.

## Removed names

Do not import or mount `admin.main`, `create_tapdb_web_app`, or a DAG-v1 router.
Do not install an `admin` extra. Missing imports are intentional breaking
changes, not signals to add a shim.

See [integration and embedding](integration-and-embedding.md) for the full host
pattern and [external references and federation](external-references-and-federation.md)
for discoverability guidance.
