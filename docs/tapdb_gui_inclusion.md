# TAPDB GUI Inclusion Guide

This guide explains how to embed the TAPDB Admin GUI inside another app and
choose an auth strategy. For the broader FastAPI + Jinja2 host-app pattern, see
[../README.md](../README.md).

## 1) Basic Inclusion

Install TAPDB admin support in the host environment, then mount the sub-app:

```python
from fastapi import FastAPI
from admin.main import app as tapdb_admin_app  # from daylily-tapdb[admin]

app = FastAPI()

# your own routers/middleware here
app.mount("/tapdb", tapdb_admin_app)
```

Run the host app over HTTPS. TAPDB admin login and callback routes assume HTTPS.

## 2) Auth Modes

TAPDB admin supports three practical modes when embedded inside a parent app.

### Mode A: TAPDB Native Auth (default)

Do nothing special. TAPDB uses its own Cognito-backed login flow.

Use this when:
- The host app does not already have an auth/session system.
- You want TAPDB login screens and Cognito flow to remain independent.

### Mode B: Shared Auth From Host App

Enable shared auth only when the host app can provide the signed session cookie
shape TAPDB currently expects:

```bash
export TAPDB_ADMIN_SHARED_AUTH=1
```

Optional overrides:

```bash
export TAPDB_ADMIN_BLOOM_SESSION_SECRET="<host-session-secret>"
export TAPDB_ADMIN_BLOOM_SESSION_COOKIE="session"
export TAPDB_ADMIN_BLOOM_SESSION_MAX_AGE=1209600
```

How it works:
- TAPDB reads the host cookie (default cookie name: `session`).
- It validates signature/max-age using the configured secret.
- It expects `user_data.email` and optional `user_data.role` (`admin`/`user`) in that cookie.
- It maps/provisions a TAPDB user row and opens TAPDB without a separate login.

Use this when:
- The host app already authenticates users and you want SSO-like behavior for `/tapdb`.
- The host session payload already carries the `user_data.*` fields required by TAPDB.

### Mode C: Auth Disabled (development only)

```bash
export TAPDB_ADMIN_DISABLE_AUTH=true
```

Optional identity overrides:

```bash
export TAPDB_ADMIN_DISABLED_USER_EMAIL="tapdb-admin@localhost"
export TAPDB_ADMIN_DISABLED_USER_ROLE="admin"
```

Use this only for local development or diagnostics.

## 3) Recommended Client Pattern

For production-like integrations, choose the mode that matches the host app:

1. If the host app owns auth and should guard `/tapdb` itself, use the host-gated mount pattern and set `TAPDB_ADMIN_DISABLE_AUTH=true`.
2. If the host app can supply the session payload TAPDB expects, use shared auth.
3. Keep `TAPDB_ADMIN_DISABLE_AUTH` unset only when you intend TapDB to manage its own login flow.

## 4) Runtime Checks

With shared auth enabled and a compatible host cookie:
- `GET /tapdb/login` should redirect to `/tapdb/` when a valid host session cookie is present.
- `GET /tapdb/login` should render login page (`200`) when no valid host session exists.

With auth disabled:
- `GET /tapdb/login` should redirect to `/tapdb/` unconditionally.

## 5) Security Notes

- Do not use `TAPDB_ADMIN_DISABLE_AUTH=true` in production.
- If using shared auth, keep the host session secret private and rotate as needed.
- Use HTTPS so secure cookies and callback flows behave correctly.

## 6) Troubleshooting

- `OAuth login is not configured ... cognito_user_pool_id`:
  Set `environments.<env>.cognito_user_pool_id` in tapdb config.
- `redirect_mismatch` from Cognito:
  Ensure callback/logout URLs in Cognito app client match actual TAPDB URL/port.
- Shared auth not taking effect:
  Verify `TAPDB_ADMIN_SHARED_AUTH=1`, cookie name, signing secret, and that the host session payload includes `user_data.email` and `user_data.role`.
