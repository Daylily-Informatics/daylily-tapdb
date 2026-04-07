# TAPDB GUI Inclusion Guide

This guide lives in the `tapdb-core` repository. The Python import package remains `daylily_tapdb`.

This guide explains how to embed the TAPDB Admin GUI inside another app and
choose an auth strategy. For the broader FastAPI + Jinja2 host-app pattern, see
[../README.md](../README.md).

## 1) Basic Inclusion

Install TapDB admin support in the host environment, then mount the reusable
TapDB web app:

```python
from fastapi import FastAPI

from daylily_tapdb.web import TapdbHostBridge, create_tapdb_web_app

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
    create_tapdb_web_app(
        config_path="/abs/path/to/tapdb-config.yaml",
        env_name="dev",
        host_bridge=bridge,
    ),
)
```

The same TapDB package also exposes `create_tapdb_dag_router(...)` for
root-level `/api/dag/*` routes.

## 2) Auth Modes

TAPDB admin supports three practical modes when embedded inside a parent app.
All auth settings live in the TapDB config file.

### Mode A: TAPDB Native Auth

```yaml
admin:
  auth:
    mode: tapdb
```

Use this when:
- The host app does not already have an auth/session system.
- You want TAPDB login screens and Cognito flow to remain independent.

### Mode B: Host Bridge Session Auth

```python
from daylily_tapdb.web import TapdbHostBridge

bridge = TapdbHostBridge(
    auth_mode="host_session",
    resolve_user=my_host_user_resolver,
    login_url=my_host_login_url,
    logout_url=my_host_logout_url,
)
```

How it works:

- the host app authenticates the request first
- the bridge resolves the current operator and injects it into the mounted TapDB
  scope
- TapDB renders its own pages, but can inherit host nav links, stylesheet URLs,
  and template overrides

Use this when:

- the host app already owns browser session auth
- `/tapdb` should feel like part of the host UI
- the host app wants canonical `/api/dag/*` routes at root while TapDB HTML
  stays namespaced under `/tapdb`

### Mode C: Auth Disabled

```yaml
admin:
  auth:
    mode: disabled
    disabled_user:
      email: "tapdb-admin@localhost"
      role: "admin"
```

Use this only for local development or diagnostics.

## 3) Recommended Client Pattern

1. Mount TapDB HTML at `/tapdb` with `create_tapdb_web_app(...)`.
2. Publish canonical DAG routes at root `/api/dag/*` with `create_tapdb_dag_router(...)`.
3. Use `TapdbHostBridge(auth_mode="host_session", ...)` when the host owns auth.
4. Use TapDB-native auth only when TapDB should manage its own login flow.

## 4) Runtime Checks

- `GET /tapdb/` should render through host auth when the bridge resolves a user.
- `GET /tapdb/` should redirect to the host login when the bridge does not
  resolve a user.
- `GET /api/dag/object/{euid}` should be guarded by the host app's chosen API
  dependency, not by TapDB browser auth.

## 5) Security Notes

- Do not use `admin.auth.mode: disabled` in production.
- If using a host bridge, keep the host auth/session validation logic inside the
  parent app and inject only the normalized user payload into TapDB.
- Use HTTPS so secure cookies and callback flows behave correctly.

## 6) Troubleshooting

- `OAuth login is not configured ... cognito_user_pool_id`:
  Set `environments.<env>.cognito_user_pool_id` in tapdb config.
- `redirect_mismatch` from Cognito:
  Ensure callback/logout URLs in Cognito app client match actual TAPDB URL/port.
- Host bridge auth not taking effect:
  Verify the bridge `resolve_user(...)` callback returns a normalized user
  payload with at least `email` or `username`.
