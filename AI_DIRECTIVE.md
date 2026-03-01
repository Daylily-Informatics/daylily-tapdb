# AI Directive: daylily-tapdb

## Operational Policy
Use TAPDB through the `tapdb` CLI for runtime, database, schema, seeding, UI, and Cognito operations.

When this repository is designated as the operational path:
- Prefer `tapdb` commands over ad-hoc scripts.
- Do not use direct AWS Cognito commands (`aws cognito-idp ...`); use `tapdb cognito` (delegates to `daycog`).
- Do not use direct CloudFormation/Aurora mutation commands for normal lifecycle; use `tapdb aurora` or `tapdb bootstrap aurora`.

## Environment Bootstrap
From repo root, start with:

```sh
source ./tapdb_activate.sh
```

This activates the environment and exposes the current TAPDB CLI command surface.

## Required Runtime Context
TAPDB command routing depends on explicit context:

1. `TAPDB_ENV` (target environment): `dev | test | prod`
2. Optional database namespace (strongly recommended for multi-app use):
   - `--database-name <name>` on each command, or
   - `TAPDB_DATABASE_NAME=<name>` in the shell

For bootstrap commands, `TAPDB_ENV` is required.

## Multi-App / Same User Account Isolation (Critical)
If multiple apps use TAPDB under the same OS user account, you MUST isolate configuration and identity context per app.

### TAPDB config namespacing
Use database-scoped config filenames:
- `~/.config/tapdb/tapdb-config-<database-name>.yaml`
- `./config/tapdb-config-<database-name>.yaml` (repo-local override)

Set one of:

```sh
export TAPDB_DATABASE_NAME=myapp
```

or:

```sh
tapdb --database-name myapp info
```

Config search order (highest precedence first):
1. `TAPDB_CONFIG_PATH`
2. `~/.config/tapdb/tapdb-config-<database-name>.yaml` (if scoped)
3. `~/.config/tapdb/tapdb-config.yaml`
4. `./config/tapdb-config-<database-name>.yaml` (if scoped)
5. `./config/tapdb-config.yaml`

### Cognito isolation
Use unique pool names and app client names per TAPDB app namespace:

```sh
tapdb cognito setup dev --pool-name tapdb-myapp-dev-users --client-name tapdb-myapp-dev-client
```

`tapdb cognito` stores only `cognito_user_pool_id` in TAPDB config and uses daycog-managed env files:
- `~/.config/daycog/<pool>.<region>.env`
- `~/.config/daycog/<pool>.<region>.<app>.env`
- `~/.config/daycog/default.env`

Before making changes, verify active binding:

```sh
tapdb info
tapdb cognito status dev
```

### Example: Two TAPDB apps in one account
Use separate namespaces for each app:

```sh
# App A
export TAPDB_DATABASE_NAME=appa
export TAPDB_ENV=dev
tapdb bootstrap local
tapdb cognito setup dev --pool-name tapdb-appa-dev-users --client-name tapdb-appa-dev-client

# App B (same OS user, different namespace)
export TAPDB_DATABASE_NAME=appb
export TAPDB_ENV=dev
tapdb bootstrap local
tapdb cognito setup dev --pool-name tapdb-appb-dev-users --client-name tapdb-appb-dev-client
```

This produces isolated config scopes and avoids cross-app collisions in:
- TAPDB DB config files
- Cognito pool/client bindings
- daycog env context selection

## Required CLI Usage

### End-to-end bootstrap
```sh
export TAPDB_ENV=dev
tapdb bootstrap local
tapdb bootstrap local --no-gui

export TAPDB_ENV=dev
tapdb bootstrap aurora --cluster <cluster-id> --region <aws-region>
tapdb bootstrap aurora --cluster <cluster-id> --region <aws-region> --no-gui
```

### PostgreSQL runtime/service (`tapdb pg`)
```sh
tapdb pg init <env>
tapdb pg start-local <env>
tapdb pg stop-local <env>

tapdb pg start
tapdb pg stop
tapdb pg restart
tapdb pg status
tapdb pg logs
```

### Logical DB/schema/data (`tapdb db`)
```sh
tapdb db create <env>
tapdb db delete <env>
tapdb db setup <env>

tapdb db schema apply <env>
tapdb db schema status <env>
tapdb db schema reset <env>
tapdb db schema migrate <env>

tapdb db data seed <env>
tapdb db data backup <env>
tapdb db data restore <env>

tapdb db config validate
```

### Cognito (`tapdb cognito`)
```sh
tapdb cognito setup <env>
tapdb cognito setup-with-google <env>
tapdb cognito bind <env> --pool-id <pool-id>
tapdb cognito status <env>

tapdb cognito list-pools <env>
tapdb cognito list-apps <env>
tapdb cognito add-app <env> --app-name <name> --callback-url <url>
tapdb cognito edit-app <env> --app-name <name>
tapdb cognito remove-app <env> --app-name <name> --force
tapdb cognito add-google-idp <env> --app-name <name>
tapdb cognito fix-auth-flows <env>
tapdb cognito add-user <env> <email> --password <password>

tapdb cognito config print <env>
tapdb cognito config create <env>
tapdb cognito config update <env>
```

For Hosted UI domain control (daycog 0.1.22+), use:
- `--domain-prefix <prefix>`
- `--attach-domain` / `--no-attach-domain`

### Admin UI (`tapdb ui`)
```sh
tapdb ui start
tapdb ui mkcert
tapdb ui stop
tapdb ui status
tapdb ui logs
tapdb ui restart
```

Default UI port is `8911`.

UI is HTTPS-first. For browser-trusted local certs:

```sh
tapdb ui mkcert
tapdb ui restart --port 8911
```

Defaults:
- cert: `~/.tapdb/certs/localhost.crt`
- key: `~/.tapdb/certs/localhost.key`

Override paths (if needed):
- `TAPDB_UI_SSL_CERT`
- `TAPDB_UI_SSL_KEY`

### Aurora infra (`tapdb aurora`)
```sh
tapdb aurora create <env>
tapdb aurora status <env>
tapdb aurora list
tapdb aurora connect <env>
tapdb aurora delete <env>
```

## TAPDB as a Client Library
Use the public Python API for application code:

```python
import os
from daylily_tapdb import TAPDBConnection, TemplateManager, InstanceFactory
from daylily_tapdb.cli.db_config import get_db_config_for_env

env = os.environ.get("TAPDB_ENV", "dev")
cfg = get_db_config_for_env(env)

db = TAPDBConnection(
    db_hostname=f"{cfg['host']}:{cfg['port']}",
    db_user=cfg["user"],
    db_pass=cfg["password"],
    db_name=cfg["database"],
)

templates = TemplateManager()
factory = InstanceFactory(templates)

with db.session_scope(commit=True) as session:
    instance = factory.create_instance(
        session=session,
        template_code="container/plate/fixed-plate-96/1.0/",
        name="PLATE-001",
    )
    print(instance.euid)
```

Guidance:
- Prefer library methods (`TemplateManager`, `InstanceFactory`, `ActionDispatcher`) over writing raw SQL for core workflows.
- Keep `TAPDB_ENV` and config namespace explicit in process startup.

## Meridian EUID Requirements (Mandatory)
Users of TAPDB MUST follow Meridian EUID spec:

- Spec source: `../../lsmc/meridian-euid/SPEC.md`
- Normative identity statement:

> Meridian does not use UUIDs. Internal identity is sequence-based. External identity is EUID-based.

Conformance requirements:
- EUIDs MUST be Meridian-conformant (syntax + checksum).
- EUIDs MUST be uppercase ASCII with no whitespace.
- Forbidden characters: `I`, `L`, `O`, `U`.
- Production format: `CATEGORY-BODYCHECK`.
- Sandbox format: `SANDBOX:CATEGORY-BODYCHECK`.

TAPDB includes helper functions in `daylily_tapdb.euid`:
- `format_euid(...)`
- `validate_euid(...)`
- `meridian_checksum(...)`

If your app generates or validates EUIDs outside DB triggers, you MUST use Meridian rules from the spec above.

## Guardrails for Agents and Operators
- Default to non-destructive operations.
- Use destructive commands (`db delete`, `db schema reset`, `aurora delete`, `cognito remove-app`, user deletes) only when explicitly requested.
- In shared user accounts, do not operate without confirming active namespace/context (`TAPDB_ENV`, `TAPDB_DATABASE_NAME`, `tapdb info`, `tapdb cognito status`).
