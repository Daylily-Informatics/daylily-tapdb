# daylily-tapdb

**Templated Abstract Polymorphic Database** — A flexible object model library for building template-driven database applications with PostgreSQL and SQLAlchemy.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![SQLAlchemy 2.0+](https://img.shields.io/badge/sqlalchemy-2.0+-green.svg)](https://www.sqlalchemy.org/)
[![PostgreSQL 14+](https://img.shields.io/badge/postgresql-14+-336791.svg)](https://www.postgresql.org/)
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
│  │ GT1234   │───────▶│ CX5678   │◀────────▶│ GL9012   │            │
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
| `generic_instance_lineage` | Directed edges between instances (DAG) | `GL` |
| `audit_log` | Automatic change tracking | — |

### Type Hierarchy

TAPDB uses SQLAlchemy single-table inheritance. Each table has typed subclasses:

**Templates:** `workflow_template`, `container_template`, `content_template`, `equipment_template`, `data_template`, `test_requisition_template`, `actor_template`, `action_template`, `health_event_template`, `file_template`, `subject_template`

**Instances:** Corresponding `*_instance` classes for each template type

**Lineages:** Corresponding `*_instance_lineage` classes for relationship tracking

## Installation

### Installation

- **Library + CLI (default)**: `pip install daylily-tapdb`
- **Admin UI (optional)**: `pip install "daylily-tapdb[admin]"`
- **Developer tooling (optional)**: `pip install "daylily-tapdb[dev]"`
- **CLI YAML config support (optional)**: `pip install "daylily-tapdb[cli]"` (otherwise JSON config works without PyYAML)

### Quick Start (recommended)

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\\Scripts\\activate on Windows
pip install -U pip
pip install -e ".[admin,dev]"
```

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
psql -d your_database -f schema/tapdb_schema.sql
```

### 2. Connect and create objects

```python
import os
from daylily_tapdb import TAPDBConnection, TemplateManager, InstanceFactory

# Connect to database
db = TAPDBConnection(
    db_url=os.environ.get('DATABASE_URL'),
    # Or specify components:
    # db_hostname='localhost:5432',
    # db_user='myuser',
    # db_pass='mypass',
    # db_name='tapdb'
)

# Initialize managers
templates = TemplateManager()
factory = InstanceFactory(templates)

# Create an instance from a template
with db.session_scope(commit=False) as session:
    plate = factory.create_instance(
        session=session,
        template_code='container/plate/fixed-plate-96/1.0/',
        name='PLATE-001'
    )
    session.commit()
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
# Template code format: {category}/{type}/{subtype}/{version}/
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
  - `{category}/{type}/{subtype}/{version}/`
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

### Instances

Instances are concrete objects created from templates:

```python
with db.session_scope(commit=False) as session:
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
    session.commit()
```

### Lineages (Relationships)

Lineages connect instances in a directed acyclic graph:

```python
with db.session_scope(commit=False) as session:
    # Link two existing instances (plate/sample assumed already loaded)
    lineage = factory.link_instances(
        session=session,
        parent=plate,
        child=sample,
        relationship_type='contains'
    )
    session.commit()

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
| `GL` | Lineage | `GL789` |
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
handler = MyActionHandler(db)
result = handler.execute_action(
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

### Scoped sessions (explicit commit)

```python
with db.session_scope(commit=False) as session:
    session.add(instance)
    session.commit()
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
psql -d your_database -f schema/tapdb_schema.sql
```

## Configuration

### Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Full database URL | — |
| `PGPASSWORD` | PostgreSQL password | — |
| `PGPORT` | PostgreSQL port | `5432` |
| `USER` | Database user / audit username | System user |
| `ECHO_SQL` | Log SQL statements (`true`/`1`/`yes`) | `false` |

### CLI config file (recommended)

The CLI can read per-environment DB settings from:

- `~/.config/tapdb/tapdb-config.yaml` (default)
- `./config/tapdb-config.yaml` (repo-local)
- or `TAPDB_CONFIG_PATH=/path/to/tapdb-config.yaml`

Resolution order (highest precedence first):

1. `TAPDB_<ENV>_*` environment variables (e.g. `TAPDB_DEV_HOST`)
2. `PG*` environment variables
3. `tapdb-config.yaml` (searched in: `~/.config/tapdb/`, then `./config/`)

An example config is included at: `./config/tapdb-config-example.yaml`

Example config:

```yaml
environments:
  dev:
    host: localhost
    port: 5432
    user: daylily
    database: tapdb_dev
```

Supported file shapes:

- `{"dev": {...}, "test": {...}, "prod": {...}}`
- or `{"environments": {"dev": {...}}}`

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
| `session` | Property returning the primary session |
| `reflect_tables()` | Reflect database tables into AutomapBase |
| `close()` | Close session and dispose engine |

### TemplateManager

| Method | Description |
|--------|-------------|
| `get_template(template_code)` | Get template by code string |
| `get_template_by_euid(euid)` | Get template by EUID |
| `list_templates(category, type_, include_deleted)` | List templates with filters |
| `template_code_from_template(template)` | Generate code string from template |
| `clear_cache()` | Clear template cache |

### InstanceFactory

| Method | Description |
|--------|-------------|
| `create_instance(template_code, name, properties, create_children)` | Create instance from template |
| `link_instances(parent, child, relationship_type)` | Create lineage between instances |

### ActionDispatcher

| Method | Description |
|--------|-------------|
| `execute_action(instance, action_group, action_key, ...)` | Execute and track an action |

Implement handlers as `do_action_{action_key}(self, instance, action_ds, captured_data)` methods.

## Integration Testing

TAPDB’s integration tests require a reachable PostgreSQL DSN.

1) Set a DSN:

```bash
export TAPDB_TEST_DSN='postgresql://user@localhost:5432/postgres'
```

2) Run the integration tests:

```bash
pytest tests/test_integration.py -v
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

Or run in foreground with auto-reload for development:

```bash
tapdb ui start --reload --foreground
# Or directly:
uvicorn admin.main:app --reload --port 8000
```

### CLI Quickstart

The fastest way to get a local TAPDB instance running:

```bash
# 1. Activate environment
source tapdb_activate.sh

# 2. Initialize and start local PostgreSQL
# If initdb/pg_ctl are missing, install PostgreSQL tools via conda:
#   conda install -c conda-forge postgresql
tapdb pg init dev           # Creates ./postgres_data/dev/
tapdb pg start-local dev    # Starts PostgreSQL on port 5432

# 3. Create database, apply schema, and seed templates
tapdb db setup dev

# 4. Verify everything is working
tapdb db status dev

# 5. Start the admin UI (optional)
tapdb ui start
# Open http://localhost:8000 in your browser
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
tapdb --help              # Show all commands
tapdb version             # Show version
tapdb info                # Show config and status

# Local PostgreSQL (data-dir based; requires initdb/pg_ctl on PATH; conda recommended)
tapdb pg init <env>       # Initialize data directory (dev/test only)
tapdb pg start-local <env> # Start local PostgreSQL instance
tapdb pg stop-local <env>  # Stop local PostgreSQL instance

# System PostgreSQL (system service; production only)
tapdb pg start            # Start system PostgreSQL service
tapdb pg stop             # Stop system PostgreSQL service
tapdb pg status           # Check if PostgreSQL is running
tapdb pg logs             # View logs (--follow/-f to tail)
tapdb pg restart          # Restart system PostgreSQL

# Database management (env: dev | test | prod)
tapdb pg create <env>     # Create empty database (e.g., tapdb_dev)
tapdb pg delete <env>     # Delete database (⚠️ destructive)
tapdb db create <env>     # Apply TAPDB schema to existing database
tapdb db seed <env>       # Seed templates from config/ directory
tapdb db setup <env>      # Full setup: create db + schema + seed (recommended)
tapdb db status <env>     # Check schema status and row counts
tapdb db nuke <env>       # Drop all tables (⚠️ destructive)
tapdb db backup <env>     # Backup database (--output/-o FILE)
tapdb db restore <env>    # Restore from backup (--input/-i FILE)
tapdb db migrate <env>    # Apply schema migrations

# Admin UI
tapdb ui start            # Start admin UI (background)
tapdb ui stop             # Stop admin UI
tapdb ui status           # Check if running
tapdb ui logs             # View logs (--follow/-f to tail)
tapdb ui restart          # Restart server
```

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
│   └── cli/
│       └── __init__.py      # CLI entry point (placeholder)
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