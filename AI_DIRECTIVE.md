# AI Directive: daylily-tapdb

## Purpose
This document instructs AI agents how to operate `daylily-tapdb` safely and predictably.
Use it when you are building, running, or integrating TAPDB as:
1. a Python library, and
2. a CLI-managed database + service runtime.

Primary goals:
- preserve data integrity,
- keep EUID behavior compliant with Meridian,
- prevent runtime collisions when multiple TAPDB clients run under one OS account.

## Operating Policy
1. Prefer TAPDB public APIs and TAPDB CLI commands for lifecycle operations.
2. Do not use ad-hoc SQL for standard create/schema/seed/bootstrap flows.
3. Do not assume global shared runtime state.
4. For local engine configs, host must be exactly `localhost`.
5. Treat destructive operations as opt-in only.

## Required Context (Strict Namespace)
TAPDB is namespace-isolated. For commands that touch config/runtime/db/ui/cognito/user/aurora/info, require both:
- `client-id`
- `database-name`

Provide either by CLI flags or environment variables:
- CLI: `--client-id <id> --database-name <name>`
- env: `TAPDB_CLIENT_ID`, `TAPDB_DATABASE_NAME`

Environment selector:
- `TAPDB_ENV` (`dev|test|prod`)

Resolution precedence:
1. CLI flags
2. env vars
3. no fallback

If either key is missing, commands should fail with actionable guidance.

## Namespace Path Model
Config root:
- `~/.config/tapdb/<client-id>/<database-name>/tapdb-config.yaml`

Runtime root per environment:
- `~/.config/tapdb/<client-id>/<database-name>/<env>/`

Runtime files:
- UI PID: `.../<env>/ui/ui.pid`
- UI logs: `.../<env>/ui/ui.log`
- UI certs: `.../<env>/ui/certs/localhost.crt` and `localhost.key`
- Postgres data: `.../<env>/postgres/data/`
- Postgres log: `.../<env>/postgres/postgresql.log`
- Lock metadata: `.../<env>/locks/instance.lock`

Never use shared global runtime files like `~/.tapdb/ui.pid` or `~/.tapdb/ui.log` for active flows.

## Canonical Command Groups (No Overlap)
Use these functional groups only:

1. `tapdb bootstrap`
- Orchestration command.
- Handles create/start/schema/seed and optionally GUI startup.
- Main entry point for local and aurora setup.

2. `tapdb pg`
- Runtime/service control only.
- Local Postgres init/start/stop/status/logs.

3. `tapdb db`
- Logical database operations only.
- DB create/delete, schema apply/status/reset/migrate, data seed/backup/restore.

4. `tapdb aurora`
- Cloud infrastructure lifecycle only.
- Provision/delete/status/list/connect for Aurora resources.

5. `tapdb ui`
- Admin UI process lifecycle only.
- start/stop/status/logs/restart, plus HTTPS cert helpers.

6. `tapdb cognito`
- Cognito lifecycle and validation flows.
- Integrates TAPDB config with `daylily-cognito` (`daycog`) config files.

## Bootstrap-First Workflow
Preferred setup path:

```sh
export TAPDB_CLIENT_ID=<client>
export TAPDB_DATABASE_NAME=<database>
export TAPDB_ENV=dev

# Local runtime + logical setup + optional GUI
 tapdb bootstrap local

# or Aurora infra + logical setup + optional GUI
 tapdb bootstrap aurora --cluster <cluster-id> --region <region>
```

Use `--no-gui` when you need headless setup.

## Core Template Policy
Bundled TAPDB core templates are intentionally minimal:
1. `generic/generic/generic/1.0`
2. `generic/generic/external_object_link/1.0`
3. `generic/actor/generic/1.0`
4. `generic/actor/system_user/1.0`

Operational rules:
1. Treat these as TAPDB-native baseline templates.
2. Do not add client-domain workflow/action/content packs to TAPDB core repo.
3. If domain packs are needed, provide them via client repos or external config
   directories and seed them explicitly.

## Config Schema Expectations
Namespace config (`tapdb-config.yaml`) should include:

```yaml
meta:
  config_version: 2
  client_id: <client-id>
  database_name: <database-name>
environments:
  dev:
    engine_type: local|aurora
    host: localhost|<aurora-endpoint>
    port: "<db-port>"
    ui_port: "<https-port>"
    user: <db-user>
    password: <db-password>
    database: <db-name>
    cognito_user_pool_id: <pool-id>
```

Rules:
- local engine must use `host: localhost`.
- `port` and `ui_port` must be explicit per environment.
- port conflicts are hard errors (no silent auto-reassignment).

## Python Library Usage
Prefer TAPDB APIs over low-level SQL.

```python
import os
from daylily_tapdb import TAPDBConnection, TemplateManager, InstanceFactory
from daylily_tapdb.cli.db_config import get_db_config_for_env

env = os.environ.get("TAPDB_ENV", "dev")
cfg = get_db_config_for_env(env)

conn = TAPDBConnection(
    db_hostname=f"{cfg['host']}:{cfg['port']}",
    db_user=cfg["user"],
    db_pass=cfg["password"],
    db_name=cfg["database"],
)

templates = TemplateManager()
factory = InstanceFactory(templates)
```

Guidance:
- Use transactional flows for multi-step writes.
- On relationship validation failure, fail atomically (no partial writes).
- Maintain current EUID behavior; do not introduce alternate ID formats.

## Auth User Storage Policy
TAPDB auth users are actor-backed objects, not a dedicated user table.

Required model:
1. Store auth users as `generic_instance` rows with:
   - `polymorphic_discriminator='actor_instance'`
   - `category='generic'`
   - `type='actor'`
   - `subtype='system_user'`
   - `version='1.0'`
2. Use template code: `generic/actor/system_user/1.0`.
3. Keep canonical login identity in `json_addl.login_identifier` (lowercased).
4. Keep role/active/password metadata in `json_addl` fields.

## EUID Requirements (Mandatory)
Normative spec:
- `../../lsmc/meridian-euid/SPEC.md`

Agent requirements:
1. Keep Meridian-compatible EUID generation/validation intact.
2. Use TAPDB EUID helpers where available.
3. Do not replace EUIDs with UUID-only external identifiers.
4. Preserve prefix + sequence behavior when creating new entities.

## Cognito Integration Policy
TAPDB config stores only the pool reference per environment:
- `environments.<env>.cognito_user_pool_id`

All Cognito app/client/domain auth context is managed in daycog files under:
- `~/.config/daycog/<pool>.<region>.env`
- `~/.config/daycog/<pool>.<region>.<app>.env`
- `~/.config/daycog/default.env`

Operational requirements:
1. Use app client name `tapdb` for TAPDB UI.
2. Validate callback/logout URLs against TAPDB configured HTTPS UI port.
3. Keep TAPDB and daycog in sync before testing login/signup.

## HTTPS Policy
TAPDB GUI should run on HTTPS with localhost certs.

Preferred:

```sh
tapdb ui mkcert
tapdb ui restart
```

Manual fallback:

```sh
mkcert -install
mkcert -cert-file ~/.config/tapdb/<client>/<database>/<env>/ui/certs/localhost.crt \
  -key-file ~/.config/tapdb/<client>/<database>/<env>/ui/certs/localhost.key \
  localhost
```

## Multi-Client Safety Checklist
Before running commands in shared OS accounts:
1. Verify `TAPDB_CLIENT_ID`, `TAPDB_DATABASE_NAME`, `TAPDB_ENV`.
2. Run `tapdb info` and confirm namespace paths.
3. Confirm DB/UI ports are from active namespace config.
4. Run `tapdb cognito status <env>` and verify pool/app/callback.

## Destructive Operation Guardrails
Only execute with explicit user intent:
- `tapdb db delete`
- `tapdb db schema reset`
- `tapdb aurora delete`
- Cognito delete operations
- Manual removal of runtime data directories

## Agent Delivery Expectations
When making TAPDB changes:
1. Explain what changed and why.
2. Reference exact files modified.
3. Run tests relevant to changed behavior (`pytest -q` preferred when feasible).
4. Report remaining risk and known gaps.
