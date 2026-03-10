# daylily-tapdb

`daylily-tapdb` is the shared persistence layer used by Bloom, Atlas, Dewey, and Ursa.

It provides:

- a template-driven polymorphic object model
- TapDB-managed database/bootstrap workflows
- lineage and audit primitives
- Meridian EUID-backed public identifiers
- a Typer-based operational CLI
- an optional FastAPI admin UI package

## Repository Role

Use this repo when you need to:

- bootstrap or reset a TapDB-backed environment
- work with core template and instance models
- embed or run the TapDB admin UI
- validate namespace config, schema, data seeding, or Aurora support

Primary Python package: `daylily_tapdb`

## Main CLI Surface

Entry command: `tapdb`

Primary groups:

- `tapdb bootstrap`: one-command local or Aurora bootstrap
- `tapdb ui`: admin UI start, stop, status, logs, mkcert
- `tapdb config`: namespace config initialization and migration helpers
- `tapdb db`: database, schema, data, and config validation commands
- `tapdb pg`: local PostgreSQL service commands
- `tapdb user`: TapDB admin/auth user management
- `tapdb cognito`: Cognito/daycog integration helpers
- `tapdb aurora`: Aurora cluster management when installed with `.[aurora]`

## Data Model

The core operational tables are:

- `generic_template`
- `generic_instance`
- `generic_instance_lineage`
- `audit_log`
- `outbox_event`

TapDB uses `uid` for internal joins and EUIDs for public-facing identity.

`tenant_id` is the logical isolation key on templates, instances, lineage, audit, and outbox rows.

## Installation

Default install:

```bash
pip install daylily-tapdb
```

Common dev install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[admin,dev,cli]"
```

Optional convenience wrapper:

```bash
source tapdb_activate.sh
```

## Quick Start

Initialize a strict namespace and bootstrap a local stack:

```bash
export TAPDB_CLIENT_ID=tapdb
export TAPDB_DATABASE_NAME=tapdb
export TAPDB_ENV=dev

tapdb config init --client-id "$TAPDB_CLIENT_ID" --database-name "$TAPDB_DATABASE_NAME" --env dev --db-port dev=5533 --ui-port dev=8911
tapdb bootstrap local
```

## Admin UI

The admin UI is optional and ships with the `admin` extra.

Key modes:

- native TapDB auth
- shared host-app auth
- auth-disabled local development

See the embedding guide for the current supported patterns.

## Timezone Policy

- persisted timestamps are UTC
- user display timezone preference lives in TapDB-backed `system_user` preferences
- canonical preference key: `display_timezone`

## Current Docs

- [Docs index](docs/README.md)
- [GUI inclusion guide](docs/tapdb_gui_inclusion.md)

Historical execution plans and breaking-change notes remain in `docs/` for background only.

<!-- release-sweep: 2026-03-10 -->
