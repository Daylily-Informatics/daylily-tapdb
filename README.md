# daylily-tapdb

**Templated Abstract Polymorphic Database** — A flexible object model library for building template-driven database applications with PostgreSQL and SQLAlchemy.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![SQLAlchemy 2.0+](https://img.shields.io/badge/sqlalchemy-2.0+-green.svg)](https://www.sqlalchemy.org/)
[![PostgreSQL 13+](https://img.shields.io/badge/postgresql-13+-336791.svg)](https://www.postgresql.org/)
[![Aurora PostgreSQL](https://img.shields.io/badge/Aurora-PostgreSQL-ff9900.svg)](https://aws.amazon.com/rds/aurora/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

TAPDB provides a reusable foundation for applications that need:

- **Template-driven object creation** — Define blueprints, instantiate objects from them
- **Polymorphic type hierarchies** — Single-table inheritance with typed subclasses
- **Flexible relationships** — DAG-based lineage tracking between instances
- **Enterprise identifiers** — Auto-generated EUIDs with configurable prefixes
- **Full audit trails** — Automatic change tracking via database triggers
- **Soft deletes** — Records are never hard-deleted

**Target use cases:** LIMS, workflow management, inventory tracking, any system needing flexible template-driven objects with complex relationships.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         TAPDB Core                                  │
├─────────────────────────────────────────────────────────────────────┤
│  Templates           Instances              Lineages                │
│  ┌──────────┐        ┌──────────┐          ┌──────────┐            │
│  │ GT1234   │───────▶│ CX5678   │◀────────▶│ GN9012   │            │
│  │ (blueprint)│       │ (object) │          │ (edge)   │            │
│  └──────────┘        └──────────┘          └──────────┘            │
│       │                   │                      │                  │
│       ▼                   ▼                      ▼                  │
│  generic_template   generic_instance    generic_instance_lineage   │
│  (single table)     (single table)      (single table)             │
│  polymorphic        polymorphic         polymorphic                │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Tables

| Table | Purpose | EUID Prefix |
|-------|---------|-------------|
| `generic_template` | Blueprints defining how instances should be created | `GT` |
| `generic_instance` | Concrete objects created from templates | Configurable per template |
| `generic_instance_lineage` | Directed edges between instances (DAG) | `GN` |
| `audit_log` | Automatic change tracking | — |

### Type Hierarchy

TAPDB uses SQLAlchemy single-table inheritance. Each table has typed subclasses:

**Templates:** `workflow_template`, `container_template`, `content_template`, `equipment_template`, `data_template`, `test_requisition_template`, `actor_template`, `action_template`, `health_event_template`, `file_template`, `subject_template`

**Instances:** Corresponding `*_instance` classes for each template type

**Lineages:** Corresponding `*_instance_lineage` classes for relationship tracking

## Installation

- **Library + CLI (default)**: `pip install daylily-tapdb`
- **Admin UI (optional)**: `pip install "daylily-tapdb[admin]"`
- **Developer tooling (optional)**: `pip install "daylily-tapdb[dev]"`
- **Aurora (AWS) support**: `pip install "daylily-tapdb[aurora]"` (adds boto3 for Aurora cluster management)
- **CLI YAML config support (optional)**: `pip install "daylily-tapdb[cli]"` (adds PyYAML for TAPDB CLI config files like `tapdb-config*.yaml`; template config files remain JSON)

### Quick Start (recommended)

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\\Scripts\\activate on Windows
pip install -U pip
pip install -e ".[admin,dev]"
```

If you want the optional CLI YAML config support during development, install:

- `pip install -e ".[admin,dev,cli]"`

This workflow:
- creates and activates a virtual environment
- installs this repo in editable mode (with admin + dev extras)

Optional convenience wrapper (macOS/Linux):

```bash
source tapdb_activate.sh
```

### Notes

- To enable completion persistently: `tapdb --install-completion`

## Quick Start

### 1. Initialize the database

```bash
# Using the CLI (recommended):
export TAPDB_ENV=dev
tapdb bootstrap local

# Or apply schema manually to an existing database:
# psql -d your_database -f schema/tapdb_schema.sql
```

### 2. Connect and create objects

```python
import os
from daylily_tapdb import TAPDBConnection, TemplateManager, InstanceFactory
from daylily_tapdb.cli.db_config import get_db_config_for_env

# Connect to database using canonical config loader (recommended)
env = os.environ.get("TAPDB_ENV", "dev")
cfg = get_db_config_for_env(env)
db = TAPDBConnection(
    db_hostname=f"{cfg['host']}:{cfg['port']}",
    db_user=cfg["user"],
    db_pass=cfg["password"],
    db_name=cfg["database"],
)

# Initialize managers
templates = TemplateManager()
factory = InstanceFactory(templates)

# Create an instance from a template
with db.session_scope(commit=True) as session:
    plate = factory.create_instance(
        session=session,
        template_code='container/plate/fixed-plate-96/1.0/',
        name='PLATE-001'
    )
    print(f"Created: {plate.euid}")  # e.g., CX1234
```

## Core Concepts

### Templates

Templates are blueprints stored in `generic_template`. They define:

- **Type hierarchy:** `category`, `type`, `subtype`, `version`
- **Instance prefix:** EUID prefix for created instances (e.g., `CX` for containers)
- **JSON schema:** Optional validation for instance `json_addl`
- **Default properties:** Merged into instance `json_addl` at creation
- **Action imports:** Actions available on instances of this template
- **Instantiation layouts:** Child objects to create automatically

```python
# Template code format: {category}/{type}/{subtype}/{version} (optional trailing `/`)
with db.session_scope(commit=False) as session:
    template = templates.get_template(session, 'container/plate/fixed-plate-96/1.0/')
    # Or by EUID
    template = templates.get_template_by_euid(session, 'GT123')
```

## Template Configuration Schema

TAPDB templates are typically seeded from JSON files under `./config/`. The canonical v2 schema metadata is:

- `config/_metadata.json`

Each JSON file contains a top-level `templates` array:

- `config/generic/generic.json`
- `config/action/core.json`
- `config/workflow/assay.json`
- `config/workflow_step/queue.json`

### Canonical fields (v2)

Each element in `templates` is a template definition with:

- `name` (string)
- `polymorphic_discriminator` (string; e.g. `generic_template`, `workflow_template`, `action_template`)
- `category`, `type`, `subtype`, `version` (strings) — used to build the template code:
  - `{category}/{type}/{subtype}/{version}` (optional trailing `/`)
- `instance_prefix` (string; EUID prefix for created instances)
- `is_singleton` (bool)
- `bstatus` (string; lifecycle status)
- `json_addl` (object; template-specific data). Common subkeys seen in the repo examples:
  - `properties` (object)
  - `action_imports` (object mapping action keys → action template codes)
  - `expected_inputs` / `expected_outputs` (arrays)
  - `instantiation_layouts` (array)
  - `cogs` (object)

```json
{
  "templates": [
    {
      "name": "Generic Object",
      "polymorphic_discriminator": "generic_template",
      "category": "generic",
      "type": "generic",
      "subtype": "generic",
      "version": "1.0",
      "instance_prefix": "GX",
      "json_addl": {"properties": {"name": "Generic Object"}}
    }
  ]
}
```

### `instantiation_layouts` schema

If a template’s `json_addl.instantiation_layouts` is present, TAPDB validates it (Phase 2) and uses it to create child instances automatically.

Canonical shape:

- `instantiation_layouts`: **list** of layout objects
  - `relationship_type` (string, optional; default `"contains"`)
  - `name_pattern` (string, optional)
  - `child_templates`: **list** of child entries
    - string form: a template code (e.g. `"content/sample/dna/1.0/"`)
    - object form: `{ "template_code": "...", "count": 1, "name_pattern": "..." }` (`count` must be >= 1)

Example:

```json
{
  "json_addl": {
    "instantiation_layouts": [
      {
        "relationship_type": "contains",
        "child_templates": [
          "content/sample/dna/1.0/",
          {"template_code": "content/sample/dna/1.0/", "count": 2, "name_pattern": "Sample-{i}"}
        ]
      }
    ]
  }
}
```

### Validating config files (structural vs semantic)

You can run a **structure/format** validation pass over your `config/` JSON without connecting to a database:

- `tapdb db config validate` (auto-discovers `./config/` when run from the repo root)

What it checks (structural):
- JSON parses and top-level shape (`{"templates": [...]}`)
- required template keys + basic types
- duplicate `(category, type, subtype, version)` keys
- template-code formatting in reference fields (accepts optional trailing `/`)
- `json_addl.instantiation_layouts` shape via Pydantic (e.g., `count >= 1`, `relationship_type` non-empty)

What it does **not** check (semantic):
- whether `relationship_type` values are from an allowed vocabulary for your application
- cross-template business rules (capacity constraints, compatibility, naming conventions)
- database state (foreign keys, existing rows) or runtime action wiring correctness

Useful flags:
- `--strict/--no-strict`: in strict mode, missing referenced templates are errors
- `--json`: emit a machine-readable report

### Instances

Instances are concrete objects created from templates:

```python
with db.session_scope(commit=True) as session:
    # Create with default properties
    instance = factory.create_instance(
        session=session,
        template_code='container/plate/fixed-plate-96/1.0/',
        name='My Plate'
    )

    # Create with custom properties
    instance = factory.create_instance(
        session=session,
        template_code='content/sample/dna/1.0/',
        name='Sample-001',
        properties={
            'concentration': 25.5,
            'volume_ul': 100
        }
    )
```

### Lineages (Relationships)

Lineages connect instances in a directed acyclic graph:

```python
with db.session_scope(commit=True) as session:
    # Link two existing instances (plate/sample assumed already loaded)
    lineage = factory.link_instances(
        session=session,
        parent=plate,
        child=sample,
        relationship_type='contains'
    )

    # Traverse relationships (read-only)
    for lineage in plate.parent_of_lineages:
        child = lineage.child_instance
        print(f"{plate.euid} -> {child.euid}")
```

### Enterprise Unique IDs (EUIDs)

EUIDs are human-readable identifiers generated by database triggers:

| Prefix | Type | Example |
|--------|------|---------|
| `GT` | Template | `GT123` |
| `GX` | Generic instance | `GX456` |
| `GN` | Lineage | `GN789` |
| `WX` | Workflow instance | `WX101` |
| `WSX` | Workflow step instance | `WSX102` |
| `XX` | Action instance | `XX103` |
| Custom | Application-defined | `CX104`, `MX105` |

EUIDs are generated by database triggers using **per-prefix sequences** (e.g. `gx_instance_seq`, `wx_instance_seq`).

### Actions

Actions are operations executed on instances with automatic audit tracking:

```python
from daylily_tapdb import ActionDispatcher

class MyActionHandler(ActionDispatcher):
    def do_action_set_status(self, instance, action_ds, captured_data):
        """Handler for 'set_status' action."""
        new_status = captured_data.get('status')
        instance.bstatus = new_status
        return {'status': 'success', 'message': f'Status set to {new_status}'}

    def do_action_transfer(self, instance, action_ds, captured_data):
        """Handler for 'transfer' action."""
        # Implementation here
        return {'status': 'success', 'message': 'Transfer complete'}

# Execute an action
handler = MyActionHandler()
with db.session_scope(commit=True) as session:
    result = handler.execute_action(
        session=session,
        instance=plate,
        action_group='core_actions',
        action_key='set_status',
        action_ds=action_definition,
        captured_data={'status': 'in_progress'},
        user='john.doe'
    )
```

## Connection Management

TAPDB provides two transaction patterns:

### Manual transaction control (recommended for complex operations)

```python
session = db.get_session()
try:
    # Your operations
    session.add(instance)
    session.commit()  # You control when to commit
except Exception:
    session.rollback()
    raise
finally:
    session.close()
```

### Scoped sessions (auto-commit)

```python
with db.session_scope(commit=True) as session:
    session.add(instance)
```

## Database Schema

The schema includes:

- **Tables:** `generic_template`, `generic_instance`, `generic_instance_lineage`, `audit_log`
- **Sequences:** Per-prefix sequences for EUID generation
- **Triggers:**
  - EUID auto-generation on insert
  - Soft delete (prevents hard deletes, sets `is_deleted=TRUE`)
  - Audit logging (INSERT, UPDATE, DELETE tracking)
  - Auto-update `modified_dt` timestamp

Initialize the schema:

```bash
# Using the CLI (recommended):
export TAPDB_ENV=dev
tapdb bootstrap local

# Or apply schema manually to an existing database:
# psql -d your_database -f schema/tapdb_schema.sql
```

## Configuration

### Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Full database URL (overrides component/env-style config) | — |
| `TAPDB_ENV` | Active environment name used by the CLI (e.g. `dev`, `test`, `prod`) | — |
| `TAPDB_DATABASE_NAME` | Optional config namespace key; when set, CLI prefers `tapdb-config-<name>.yaml` | — |
| `TAPDB_<ENV>_COGNITO_USER_POOL_ID` | Optional override for env-bound Cognito pool ID | — |
| `TAPDB_CONFIG_PATH` | Explicit path to TAPDB CLI config (`tapdb-config*.yaml`/`.json`) | — |
| `TAPDB_TEST_DSN` | Postgres DSN enabling integration tests (`tests/test_integration.py`) | — |
| `TAPDB_SESSION_SECRET` | **Prod admin UI required**: session secret | — |
| `TAPDB_ADMIN_ALLOWED_ORIGINS` | **Prod admin UI required**: comma-separated CORS origins | — |
| `PGPASSWORD` | PostgreSQL password | — |
| `PGPORT` | PostgreSQL port | `5432` |
| `USER` | Database user / audit username | System user |
| `ECHO_SQL` | Log SQL statements (`true`/`1`/`yes`) | `false` |

### CLI config file (recommended)

The CLI can read per-environment DB settings from:

- `~/.config/tapdb/tapdb-config-<database-name>.yaml` (when `--database-name` or `TAPDB_DATABASE_NAME` is set)
- `~/.config/tapdb/tapdb-config.yaml` (default)
- `./config/tapdb-config-<database-name>.yaml` (repo-local scoped config)
- `./config/tapdb-config.yaml` (repo-local default)
- or `TAPDB_CONFIG_PATH=/path/to/tapdb-config.yaml`

Resolution order (highest precedence first):

1. `TAPDB_<ENV>_*` environment variables (e.g. `TAPDB_DEV_HOST`)
2. `PG*` environment variables
3. TAPDB config files in search order:
   - `tapdb-config-<database-name>.yaml` (user then repo, when scoped name is set)
   - `tapdb-config.yaml` (user then repo)

To select a scoped config namespace:

```bash
tapdb --database-name atlas info
# equivalent:
TAPDB_DATABASE_NAME=atlas tapdb info
```

An example config is included at: `./config/tapdb-config-example.yaml`

Example config:

```yaml
environments:
  dev:
    host: localhost
    port: 5432
    user: daylily
    database: tapdb_dev
    cognito_user_pool_id: us-east-1_xxxxxxxxx
```

Supported file shapes:

- `{"dev": {...}, "test": {...}, "prod": {...}}`
- or `{"environments": {"dev": {...}}}`

### Admin Auth (Cognito via daycog)

TAPDB Admin login uses `daylily-cognito` and resolves Cognito runtime settings from
daycog-managed env files:
- `~/.config/daycog/<pool-name>.<region>.env` (pool-selected app context)
- `~/.config/daycog/<pool-name>.<region>.<app-name>.env` (app-specific context)
- `~/.config/daycog/default.env` (global default context)

TAPDB config stores only:

- `environments.<env>.cognito_user_pool_id`

Typical setup:

```bash
# Create/reuse pool + app client and bind pool ID into tapdb config
# (AWS profile must be provided via --profile or AWS_PROFILE)
tapdb cognito setup dev \
  --region us-east-1 \
  --profile my-aws-profile \
  --domain-prefix tapdb-dev-users \
  --autoprovision \
  --client-name tapdb-dev-users-client

# Create user with permanent password
tapdb cognito add-user dev user@example.com --password 'SecurePass123' --no-verify
```

`tapdb cognito` delegates lifecycle operations to `daycog` (0.1.22 patterns),
so multiple apps on the same user account can coexist without config collisions.
`tapdb cognito status` also reports daycog metadata including client name and
domain/callback/logout URLs when available.

GUI behavior:
- `/login` authenticates against Cognito.
- `/signup` creates a Cognito user and automatically provisions `tapdb_user`.
- First successful Cognito login auto-provisions `tapdb_user` when missing.

### Admin UI (prod hardening)

If you run the admin UI in production mode (`TAPDB_ENV=prod`), startup will refuse to proceed unless:

- `TAPDB_SESSION_SECRET` is set
- `TAPDB_ADMIN_ALLOWED_ORIGINS` is set (comma-separated origins)

### Connection parameters

```python
db = TAPDBConnection(
    db_url='postgresql://user:pass@host:5432/dbname',  # Full URL (overrides others)
    # OR component-based:
    db_url_prefix='postgresql://',
    db_hostname='localhost:5432',
    db_user='myuser',
    db_pass='mypass',
    db_name='tapdb',
    # Connection pool settings:
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    # Debugging:
    echo_sql=False,
    app_username='my_app'  # For audit logging
)
```

## API Reference

### TAPDBConnection

| Method | Description |
|--------|-------------|
| `get_session()` | Get a new session (caller manages transaction) |
| `session_scope(commit=False)` | Context manager with optional auto-commit |
| `reflect_tables()` | Reflect database tables into AutomapBase |
| `close()` | Close session and dispose engine |

### TemplateManager

| Method | Description |
|--------|-------------|
| `get_template(session, template_code)` | Get template by code string |
| `get_template_by_euid(session, euid)` | Get template by EUID |
| `list_templates(session, category=None, type_=None, include_deleted=False)` | List templates with filters |
| `template_code_from_template(template)` | Generate code string from template |
| `clear_cache()` | Clear template cache |

### InstanceFactory

| Method | Description |
|--------|-------------|
| `create_instance(session, template_code, name, properties=None, create_children=True)` | Create instance from template |
| `link_instances(session, parent, child, relationship_type="generic")` | Create lineage between instances |

### ActionDispatcher

| Method | Description |
|--------|-------------|
| `execute_action(session, instance, action_group, action_key, ...)` | Execute and (optionally) create an audit action record |

Implement handlers as `do_action_{action_key}(self, instance, action_ds, captured_data)` methods.

## Integration Testing

TAPDB’s integration tests require a reachable PostgreSQL DSN.

For a quick local Postgres (recommended):

```bash
tapdb pg init test
tapdb pg start-local test
tapdb db create test

export TAPDB_TEST_DSN="postgresql://$USER@localhost:5433/tapdb_test"
```

Or, point at any reachable Postgres:

```bash
export TAPDB_TEST_DSN='postgresql://user@localhost:5432/postgres'
```

Run the integration tests:

```bash
pytest -q tests/test_integration.py
```

### GitHub Actions

CI uses a PostgreSQL service container and sets `TAPDB_TEST_DSN` automatically. See:

- `.github/workflows/ci.yml`

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run Postgres integration tests (always-on when TAPDB_TEST_DSN is set)
TAPDB_TEST_DSN='postgresql://user:pass@localhost:5432/dbname' pytest -q

# With coverage
pytest tests/ --cov=daylily_tapdb --cov-report=term-missing

# Type checking
mypy daylily_tapdb/

# Linting
ruff check daylily_tapdb/
```

## Admin Web Interface

TAPDB includes a FastAPI-based admin interface for browsing and managing objects.

### Running the Admin App

```bash
# Activate environment
source .venv/bin/activate

# Start the server (background)
tapdb ui start

# Check status
tapdb ui status

# View logs
tapdb ui logs -f

# Stop the server
tapdb ui stop
```

### Trusted Local HTTPS with mkcert (recommended)

`tapdb ui start` always serves HTTPS. For browser-trusted local certs, use `mkcert`:

```bash
# One command: install local CA and generate certs TAPDB uses by default
tapdb ui mkcert

# Restart UI to use the generated certs
tapdb ui restart --port 8911
```

Manual fallback (if you prefer to run mkcert yourself):

```bash
mkcert -install
mkcert -cert-file ~/.tapdb/certs/localhost.crt \
  -key-file ~/.tapdb/certs/localhost.key \
  localhost 127.0.0.1 ::1
```

Then open [https://localhost:8911](https://localhost:8911).

Or run in foreground with auto-reload for development:

```bash
tapdb ui start --reload --foreground
# Or directly:
uvicorn admin.main:app --reload --port 8911
```

### CLI Quickstart

The fastest way to get a local TAPDB instance running:

```bash
# 1. Activate environment
source tapdb_activate.sh

# 2. Set active target and bootstrap in one command
export TAPDB_ENV=dev
tapdb bootstrap local

# 3. Verify everything is working
tapdb db schema status dev
# Open https://localhost:8911 in your browser
```

To stop everything:

```bash
source tapdb_deactivate.sh  # Stops PostgreSQL and deactivates .venv
```

### Reset / "Nuke" (Local vs Full)

- **Local reset (default):** removes local repo/user artifacts only (does **not** delete remote DBs or AWS resources)
  - `bash bin/nuke_all.sh`
  - `bash bin/nuke_all.sh --dry-run`
- **Full deletion (dangerous):** local reset + optional remote DB deletion + optional AWS deletion
  - `bash bin/nuke_all.sh --full`
  - Full deletion is gated behind double confirmations and requires explicit AWS resource IDs via env vars.

### CLI Command Reference

```bash
# General
tapdb --help                 # Show all commands
tapdb version                # Show version
tapdb info                   # Show config and status

# End-to-end bootstrap (TAPDB_ENV must be set)
tapdb bootstrap local        # Local runtime + db + schema + seed + UI
tapdb bootstrap local --no-gui
tapdb bootstrap aurora --cluster tapdb-dev --region us-west-2

# Local PostgreSQL (data-dir based; requires initdb/pg_ctl on PATH; conda recommended)
tapdb pg init <env>          # Initialize data directory (dev/test only)
tapdb pg start-local <env>   # Start local PostgreSQL instance
tapdb pg stop-local <env>    # Stop local PostgreSQL instance

# System PostgreSQL (system service; production only)
tapdb pg start               # Start system PostgreSQL service
tapdb pg stop                # Stop system PostgreSQL service
tapdb pg status              # Check if PostgreSQL is running
tapdb pg logs                # View logs (--follow/-f to tail)
tapdb pg restart             # Restart system PostgreSQL

# Database management (env: dev | test | prod)
tapdb db create <env>        # Create empty database (e.g., tapdb_dev)
tapdb db delete <env>        # Delete database (⚠️ destructive)
tapdb db setup <env>         # Full setup: create db + schema + seed (recommended)
tapdb db schema apply <env>  # Apply TAPDB schema to existing database
tapdb db schema status <env> # Check schema status and row counts
tapdb db schema reset <env>  # Drop TAPDB schema objects (⚠️ destructive)
tapdb db schema migrate <env># Apply schema migrations
tapdb db data seed <env>     # Seed templates from config/ directory
tapdb db data backup <env>   # Backup database (--output/-o FILE)
tapdb db data restore <env>  # Restore from backup (--input/-i FILE)
tapdb db config validate     # Validate template JSON config files (no DB required)

# User management
tapdb user list              # List users
tapdb user add               # Add a user
tapdb user set-role          # Promote/demote (admin|user)
tapdb user deactivate        # Disable login
tapdb user activate          # Re-enable login
tapdb user set-password      # Set password
tapdb user delete            # Permanently delete (⚠️ destructive)
# Note: Admin UI authentication is Cognito-based; tapdb users are app roles/metadata.

# Cognito integration (delegates to daycog; stores only pool-id in tapdb config)
tapdb cognito setup <env>    # daycog 0.1.22 setup wrapper (pool/client/callback policy)
tapdb cognito setup-with-google <env> [google flags]
tapdb cognito bind <env>     # Bind existing pool ID to env
tapdb cognito status <env>   # Show pool binding and mapped daycog env file
tapdb cognito list-pools <env>
tapdb cognito add-user <env> <email> --password <pw> [--role user|admin] [--no-verify]
tapdb cognito list-apps <env>
tapdb cognito add-app <env> --app-name <name> --callback-url <url> [--set-default]
tapdb cognito edit-app <env> (--app-name <name>|--client-id <id>) [--new-app-name <name>]
tapdb cognito remove-app <env> (--app-name <name>|--client-id <id>) [--force]
tapdb cognito add-google-idp <env> (--app-name <name>|--client-id <id>) [google creds flags]
tapdb cognito fix-auth-flows <env>
tapdb cognito config print <env>
tapdb cognito config create <env>
tapdb cognito config update <env>

# Admin UI
tapdb ui start               # Start admin UI (background)
tapdb ui mkcert             # Install mkcert CA + generate trusted localhost cert/key
tapdb ui stop                # Stop admin UI
tapdb ui status              # Check if running
tapdb ui logs                # View logs (--follow/-f to tail)
tapdb ui restart             # Restart server

# Aurora PostgreSQL (AWS) — requires pip install daylily-tapdb[aurora]
tapdb aurora create <env>    # Provision Aurora cluster via CloudFormation
tapdb aurora status <env>    # Show cluster status, endpoint, outputs
tapdb aurora connect <env>   # Print connection info / psql command
tapdb aurora list            # List all tapdb Aurora stacks in a region
tapdb aurora delete <env>    # Tear down Aurora CloudFormation stack
```

### CLI Migration (Duplicate Commands Removed)

| Old command | New canonical command |
|-------------|-----------------------|
| `tapdb pg create <env>` | `tapdb db create <env>` |
| `tapdb pg delete <env>` | `tapdb db delete <env>` |
| `tapdb db create <env>` (schema apply) | `tapdb db schema apply <env>` |
| `tapdb db status <env>` | `tapdb db schema status <env>` |
| `tapdb db nuke <env>` | `tapdb db schema reset <env>` |
| `tapdb db migrate <env>` | `tapdb db schema migrate <env>` |
| `tapdb db seed <env>` | `tapdb db data seed <env>` |
| `tapdb db backup <env>` | `tapdb db data backup <env>` |
| `tapdb db restore <env>` | `tapdb db data restore <env>` |
| `tapdb db validate-config` | `tapdb db config validate` |

### Backup & Recovery

TAPDB includes thin wrappers around PostgreSQL client tools:

- `tapdb db data backup <env>` uses `pg_dump` and writes an **SQL** file
- `tapdb db data restore <env>` replays that file using `psql` (`ON_ERROR_STOP=1`)

What is included by default (table-limited logical backup):
- `generic_template`
- `generic_instance`
- `generic_instance_lineage`
- `audit_log`

Notes / operational guidance:
- These commands require `pg_dump` / `psql` on your `PATH` (PostgreSQL client install).
- For most workflows, prefer **data-only** backups and restore into an existing TAPDB schema:
  - Backup: `tapdb db data backup <env> --data-only -o tapdb_<env>.sql`
  - Ensure schema exists: `tapdb db schema apply <env>` (or `tapdb db setup <env>`)
  - Restore: `tapdb db data restore <env> -i tapdb_<env>.sql --force`
- `restore` does not automatically drop tables; for a clean restore, run `tapdb db schema reset <env>` (or recreate the database) before restoring.

### Environments

TAPDB supports three environments: `dev`, `test`, and `prod`.

| Environment | Default Port | Database Name | Data Directory |
|-------------|--------------|---------------|----------------|
| `dev` | 5432 | `tapdb_dev` | `./postgres_data/dev/` |
| `test` | 5433 | `tapdb_test` | `./postgres_data/test/` |
| `prod` | 5432 | `tapdb_prod` | System PostgreSQL |

**Local vs Remote:**
- `dev` and `test` can use local PostgreSQL (`tapdb pg init/start-local`)
- `prod` requires an external PostgreSQL instance (AWS RDS, system install, etc.)

For local/manual PostgreSQL, bring your own Postgres instance, database, credentials, and network access. For automated AWS provisioning, see [Aurora PostgreSQL (AWS)](#aurora-postgresql-aws).

For remote/AWS databases, configure via environment variables:

```bash
export TAPDB_PROD_HOST=my-rds.us-west-2.rds.amazonaws.com
export TAPDB_PROD_PORT=5432
export TAPDB_PROD_USER=tapdb_admin
export TAPDB_PROD_PASSWORD=your-password
export TAPDB_PROD_DATABASE=tapdb_prod

tapdb db setup prod
```

### Environment Configuration

Database connections are configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `TAPDB_DEV_HOST` | Dev database host | `localhost` |
| `TAPDB_DEV_PORT` | Dev database port | `5432` |
| `TAPDB_DEV_USER` | Dev database user | `$USER` |
| `TAPDB_DEV_PASSWORD` | Dev database password | — |
| `TAPDB_DEV_DATABASE` | Dev database name | `tapdb_dev` |

Replace `DEV` with `TEST` or `PROD` for other environments. Falls back to `PGHOST`, `PGPORT`, etc. if environment-specific vars are not set.

### Admin Features

- **Dashboard** — Overview of templates, instances, and lineages
- **Browse** — List views with filtering and pagination
- **Object Details** — View any object by EUID with relationships
- **Graph Visualization** — Interactive Cytoscape.js DAG explorer
  - Dagre hierarchical layout
  - Click nodes to see details
  - Double-click to navigate
  - Multiple layout options

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/templates` | GET | List templates with pagination |
| `/api/instances` | GET | List instances with pagination |
| `/api/object/{euid}` | GET | Get object details by EUID |
| `/api/graph/data` | GET | Get Cytoscape-compatible graph data |
| `/api/lineage` | POST | Create a new lineage relationship |
| `/api/object/{euid}` | DELETE | Soft-delete an object |


## Aurora PostgreSQL (AWS)

TAPDB can provision and manage Aurora PostgreSQL clusters on AWS via CloudFormation. This is the recommended path for staging and production deployments.

### Prerequisites

- **AWS CLI** configured with credentials (`aws configure` or `AWS_PROFILE`)
- **IAM permissions**: CloudFormation, RDS, EC2 (VPC/SG/subnets), Secrets Manager, IAM (for IAM auth)
- **Install Aurora extras**: `pip install "daylily-tapdb[aurora]"`

### Aurora Quick Start

```bash
# 1. Set active TAPDB target
export TAPDB_ENV=dev

# 2. Bootstrap Aurora end-to-end (provision + schema + seed + UI)
tapdb bootstrap aurora --cluster tapdb-dev --region us-east-1

# 3. Verify schema and row counts
tapdb db schema status dev
```

The `create` command provisions a full CloudFormation stack, waits for completion, and writes connection details to the active TAPDB config path (for example `~/.config/tapdb/tapdb-config.yaml`, or `tapdb-config-<database-name>.yaml` when scoped).

### Aurora CLI Command Reference

#### `tapdb aurora create <env>`

Provision a new Aurora PostgreSQL cluster via CloudFormation.

| Flag | Default | Description |
|------|---------|-------------|
| `--region`, `-r` | `us-west-2` | AWS region |
| `--instance-class` | `db.r6g.large` | RDS instance class |
| `--engine-version` | `16.6` | Aurora PostgreSQL engine version |
| `--vpc-id` | *(auto-discover)* | VPC to deploy into (uses default VPC if omitted) |
| `--cidr` | `10.0.0.0/8` | Ingress CIDR for the security group |
| `--cost-center` | `global` | Value for `lsmc-cost-center` tag |
| `--project` | `tapdb-<region>` | Value for `lsmc-project` tag |
| `--publicly-accessible` / `--no-publicly-accessible` | `False` | Whether the instance has a public endpoint |
| `--no-iam-auth` | `False` | Disable IAM database authentication |
| `--no-deletion-protection` | `False` | Disable deletion protection (enabled by default) |
| `--background` | `False` | Initiate creation and exit immediately |

#### `tapdb aurora status <env>`

Show stack status, endpoint, and CloudFormation outputs.

| Flag | Description |
|------|-------------|
| `--region`, `-r` | AWS region (default: `us-west-2`) |
| `--json` | Output as JSON |

#### `tapdb aurora connect <env>`

Print connection info for an Aurora environment.

| Flag | Description |
|------|-------------|
| `--region`, `-r` | AWS region (default: `us-west-2`) |
| `--user`, `-u` | Database user (default: `tapdb_admin`) |
| `--database`, `-d` | Database name (default: `tapdb_<env>`) |
| `--export`, `-e` | Print shell export statements (`PGHOST`, `PGPORT`, etc.) |

#### `tapdb aurora list`

List all tapdb Aurora stacks in a region.

| Flag | Description |
|------|-------------|
| `--region`, `-r` | AWS region to scan (default: `us-west-2`) |
| `--json` | Output as JSON |

#### `tapdb aurora delete <env>`

Delete an Aurora CloudFormation stack.

| Flag | Description |
|------|-------------|
| `--region`, `-r` | AWS region (default: `us-west-2`) |
| `--retain-networking` / `--no-retain-networking` | Retain VPC security group and subnet group (default: retain) |
| `--force`, `-f` | Skip confirmation prompt |

> `--force` bypasses all confirmation prompts — use with caution in scripts.

### Architecture

The `tapdb aurora create` command deploys a CloudFormation stack containing:

| Resource | Type | Purpose |
|----------|------|---------|
| `DBSubnetGroup` | `AWS::RDS::DBSubnetGroup` | Subnet group for cluster placement |
| `ClusterSecurityGroup` | `AWS::EC2::SecurityGroup` | Ingress rule allowing port 5432 from configured CIDR |
| `MasterSecret` | `AWS::SecretsManager::Secret` | Auto-generated 32-char master password |
| `AuroraCluster` | `AWS::RDS::DBCluster` | Aurora PostgreSQL Serverless-compatible cluster |
| `AuroraInstance` | `AWS::RDS::DBInstance` | Writer instance in the cluster |

Module structure:

```
daylily_tapdb/aurora/
├── __init__.py           # Public API exports
├── cfn_template.py       # CloudFormation template generator
├── config.py             # AuroraConfig dataclass
├── connection.py         # AuroraConnectionBuilder (IAM + Secrets Manager auth)
├── schema_deployer.py    # Deploy TAPDB schema to Aurora
├── stack_manager.py      # CloudFormation stack CRUD operations
└── templates/
    └── aurora-postgres.json  # Base CFN template
```

### Security Model

**Authentication:**
- **IAM auth** (default): Short-lived tokens via `rds.generate_db_auth_token()`. No long-lived passwords in config files.
- **Secrets Manager**: Master password auto-generated and stored in Secrets Manager. Retrieved at connection time.

**SSL/TLS:**
- All connections use `sslmode=verify-full` with the AWS RDS global CA bundle
- CA bundle auto-downloaded and cached at `~/.config/tapdb/rds-ca-bundle.pem`

**Network security:**
- Security group restricts port 5432 ingress to the configured `--cidr`
- Default CIDR is `0.0.0.0/0` — restrict to your IP for dev: `--cidr $(curl -s ifconfig.me)/32`
- Use `--no-publicly-accessible` for VPC-only access

**Credential storage:**
- Connection config written to the active TAPDB config file (default: `~/.config/tapdb/tapdb-config.yaml`, or scoped `tapdb-config-<database-name>.yaml`)
- Secrets Manager ARN stored in CloudFormation outputs (no plaintext passwords on disk)

### Cost Considerations

Aurora pricing varies by region. Representative us-east-1 costs (as of 2026):

| Instance Class | vCPU | RAM | Hourly | Daily | Monthly (730h) |
|----------------|------|-----|--------|-------|----------------|
| `db.t4g.medium` | 2 | 4 GB | ~$0.073 | ~$1.75 | ~$53 |
| `db.t4g.large` | 2 | 8 GB | ~$0.146 | ~$3.50 | ~$107 |
| `db.r6g.large` | 2 | 16 GB | ~$0.26 | ~$6.24 | ~$190 |
| `db.r6g.xlarge` | 4 | 32 GB | ~$0.52 | ~$12.48 | ~$380 |

Additional costs:
- **Storage**: $0.10/GB-month (Aurora automatically scales)
- **I/O**: $0.20 per million requests (Aurora Standard) or included (Aurora I/O-Optimized)
- **Secrets Manager**: $0.40/secret/month + $0.05 per 10K API calls

> **Tip**: Use `db.t4g.medium` for dev/test. Use `--retain-networking` on delete to speed up re-creation.

### Connectivity Options

| Method | Best For | Setup |
|--------|----------|-------|
| **IP-restricted public access** | Dev/test, solo developers | `--publicly-accessible --cidr $(curl -s ifconfig.me)/32` |
| **VPN / Tailscale subnet router** | Teams, multi-user | Route VPC subnet through Tailscale exit node |
| **VPC peering** | Production, cross-account | Peer application VPC with database VPC |
| **Private (no public endpoint)** | Production, compliance | `--no-publicly-accessible` + VPC-internal access only |

For development, **IP-restricted public access** is the simplest approach. The security group limits port 5432 to your IP.

### Teardown

```bash
# Delete the Aurora stack (retains networking resources by default)
tapdb aurora delete dev --region us-east-1

# Delete everything including VPC security group and subnet group
tapdb aurora delete dev --region us-east-1 --no-retain-networking

# Skip confirmation prompt
tapdb aurora delete dev --region us-east-1 --force
```

### Security Considerations

#### Credential Passing (`PGPASSWORD`)

TAPDB passes database credentials to `psql` via the `PGPASSWORD` environment variable — the standard PostgreSQL approach. On Linux, environment variables may be visible to other processes on the same system via `/proc/<pid>/environ`.

**Mitigations:**
- IAM auth tokens (the default for Aurora) are short-lived (~15 minutes)
- For password-based auth, consider using a [`.pgpass` file](https://www.postgresql.org/docs/current/libpq-pgpass.html) as an alternative
- The TAPDB config file is written with `0600` permissions

#### Network Security

The Aurora security group created by CloudFormation:
- **Ingress:** TCP port 5432 from the configured CIDR (default: `10.0.0.0/8`, VPC-only)
- **Egress:** All outbound allowed (required for Aurora — S3 backups, CloudWatch metrics, RDS API)
- **Public access:** Disabled by default (`--publicly-accessible` flag to override)

For compliance environments requiring explicit egress rules, modify the security group manually after provisioning.

#### Deletion Protection

Deletion protection is **enabled by default**. Use `--no-deletion-protection` for dev/test clusters. The `tapdb aurora delete` command automatically disables deletion protection before stack deletion.

> **Caution:** The `--force` flag bypasses all confirmation prompts. Use with care in automated scripts.

#### Logging

Debug-level logging may include infrastructure metadata (hostname, port, username). Ensure production deployments use logging level `INFO` or above:

```bash
export TAPDB_LOG_LEVEL=INFO
```

No credentials or IAM tokens are ever logged at any level.


## Project Structure

```
daylily-tapdb/
├── daylily_tapdb/
│   ├── __init__.py          # Public API exports
│   ├── _version.py          # Version info
│   ├── connection.py        # TAPDBConnection
│   ├── euid.py              # EUIDConfig
│   ├── models/
│   │   ├── base.py          # tapdb_core abstract base
│   │   ├── template.py      # generic_template + typed subclasses
│   │   ├── instance.py      # generic_instance + typed subclasses
│   │   └── lineage.py       # generic_instance_lineage + typed subclasses
│   ├── templates/
│   │   └── manager.py       # TemplateManager
│   ├── factory/
│   │   └── instance.py      # InstanceFactory
│   ├── actions/
│   │   └── dispatcher.py    # ActionDispatcher
│   ├── validation/
│   │   └── instantiation_layouts.py  # Pydantic schema for json_addl.instantiation_layouts
│   ├── aurora/              # Aurora PostgreSQL (AWS) support
│   │   ├── cfn_template.py  # CloudFormation template generator
│   │   ├── config.py        # AuroraConfig dataclass
│   │   ├── connection.py    # IAM + Secrets Manager auth
│   │   ├── schema_deployer.py # Schema deployment to Aurora
│   │   └── stack_manager.py # CloudFormation stack operations
│   └── cli/
│       └── __init__.py      # CLI entry point
├── admin/                   # FastAPI admin interface
│   ├── main.py              # App entry point
│   ├── api/                 # API endpoints
│   ├── templates/           # Jinja2 HTML templates
│   └── static/              # CSS and JavaScript
├── schema/
│   └── tapdb_schema.sql     # PostgreSQL schema
├── tests/
│   ├── conftest.py
│   ├── test_euid.py
│   └── test_models.py
├── tapdb_activate.sh        # Dev helper: create/activate .venv
├── tapdb_deactivate.sh      # Dev helper: deactivate .venv
├── pyproject.toml
└── README.md
```

## Requirements

- **Python:** 3.10+
- **PostgreSQL:** 13+ (for built-in `gen_random_uuid()`)
- **SQLAlchemy:** 2.0+
- **psycopg2-binary:** 2.9+

## License

MIT License — see [LICENSE](LICENSE) for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes with tests
4. Run the test suite (`pytest tests/ -v`)
5. Submit a pull request

---

**Daylily Informatics** — [daylilyinformatics.com](https://daylilyinformatics.com)
