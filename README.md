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

### Using conda (recommended)

```bash
conda env create -n TAPDB -f tapdb_env.yaml
conda activate TAPDB
```

### Using pip

```bash
pip install daylily-tapdb
```

### Manual setup (without environment file)

```bash
conda create -n TAPDB python=3.10
conda activate TAPDB
conda install sqlalchemy psycopg2 pytest pytest-cov pytest-asyncio black ruff mypy
pip install -e .
```

## Quick Start

### 1. Initialize the database

```bash
psql -d your_database -f schema/tapdb_schema.sql
```

### 2. Connect and create objects

```python
import os
from pathlib import Path
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
templates = TemplateManager(db, Path('./config'))
factory = InstanceFactory(db, templates)

# Create an instance from a template
with db.session_scope(commit=True) as session:
    plate = factory.create_instance(
        template_code='container/plate/fixed-plate-96/1.0/',
        name='PLATE-001'
    )
    print(f"Created: {plate.euid}")  # e.g., CX1234
```

## Core Concepts

### Templates

Templates are blueprints stored in `generic_template`. They define:

- **Type hierarchy:** `super_type`, `btype`, `b_sub_type`, `version`
- **Instance prefix:** EUID prefix for created instances (e.g., `CX` for containers)
- **JSON schema:** Optional validation for instance `json_addl`
- **Default properties:** Merged into instance `json_addl` at creation
- **Action imports:** Actions available on instances of this template
- **Instantiation layouts:** Child objects to create automatically

```python
# Template code format: {super_type}/{btype}/{b_sub_type}/{version}/
template = templates.get_template('container/plate/fixed-plate-96/1.0/')

# Or by EUID
template = templates.get_template_by_euid('GT123')
```

### Instances

Instances are concrete objects created from templates:

```python
# Create with default properties
instance = factory.create_instance(
    template_code='container/plate/fixed-plate-96/1.0/',
    name='My Plate'
)

# Create with custom properties
instance = factory.create_instance(
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
# Link two existing instances
lineage = factory.link_instances(
    parent=plate,
    child=sample,
    relationship_type='contains'
)

# Traverse relationships
for lineage in plate.parent_of_lineages:
    child = lineage.child_instance
    print(f"{plate.euid} -> {child.euid}")

# Filter lineage members
samples = plate.filter_lineage_members(
    of_lineage_type='parent_of_lineages',
    lineage_member_type='child_instance',
    filter_criteria={'btype': 'sample'}
)
```

### Enterprise Unique IDs (EUIDs)

EUIDs are human-readable identifiers generated by database triggers:

| Prefix | Type | Example |
|--------|------|---------|
| `GT` | Template | `GT123` |
| `GX` | Generic instance (fallback) | `GX456` |
| `GL` | Lineage | `GL789` |
| `WX` | Workflow instance | `WX101` |
| `WSX` | Workflow step instance | `WSX102` |
| `XX` | Action instance | `XX103` |
| Custom | Application-defined | `CX104`, `MX105` |

Configure custom prefixes:

```python
from daylily_tapdb import EUIDConfig

config = EUIDConfig()
config.register_prefix('CX', 'container_instance')
config.register_prefix('MX', 'content_instance')
config.register_prefix('EX', 'equipment_instance')

# Generate SQL for triggers
print(config.to_sql_case_statement())
```

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

### Scoped sessions with auto-commit

```python
with db.session_scope(commit=True) as session:
    session.add(instance)
    # Auto-commits on success, rolls back on exception
```

### Context manager

```python
with TAPDBConnection() as db:
    # Use db.session
    pass
# Session automatically closed
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
| `list_templates(super_type, btype, include_deleted)` | List templates with filters |
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

## Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=daylily_tapdb --cov-report=term-missing

# Type checking
mypy daylily_tapdb/

# Linting
ruff check daylily_tapdb/
```

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
├── schema/
│   └── tapdb_schema.sql     # PostgreSQL schema
├── tests/
│   ├── conftest.py
│   ├── test_euid.py
│   └── test_models.py
├── pyproject.toml
└── README.md
```

## Requirements

- **Python:** 3.10+
- **PostgreSQL:** 14+ (for `gen_random_uuid()`)
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