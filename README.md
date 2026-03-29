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

## Ownership Boundary

TapDB owns all canonical template-pack behavior:

- the core template-pack schema
- JSON validation
- duplicate/reference checks
- template writes into `generic_template`
- runtime guards that block direct client-side template mutation

Client repos own their JSON packs under `config/tapdb_templates/`, but they do
not own template mutation logic.

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
source ./activate
```

## Release Versioning

Package versions are derived from Git tags via `setuptools_scm`.

- An exact tagged commit builds the exact release version.
- Commits after the latest tag build the next inferred version with a `.devN`
  suffix, where `N` is the commit count since that tag.
- Local version metadata is disabled, so builds do not append Git hashes to the
  published version string.

Examples:

- `git describe` = `0.2.5` -> build version `0.2.5`
- `git describe` = `0.2.5-2-g1774465` -> build version `0.2.6.dev2`

For release artifacts, run `git describe --tags --dirty --always` before
`python -m build` and build from the exact release tag. If `HEAD` includes
additional commits that should be released, create a new tag on that commit
first.

## Quick Start

Initialize a strict namespace and bootstrap a local stack:

```bash
export TAPDB_CLIENT_ID=tapdb
export TAPDB_DATABASE_NAME=tapdb
export TAPDB_ENV=dev

tapdb config init --client-id "$TAPDB_CLIENT_ID" --database-name "$TAPDB_DATABASE_NAME" --env dev --db-port dev=5533 --ui-port dev=8911
tapdb bootstrap local
```

## JSON-Only Template Packs

TapDB core templates ship from the packaged `daylily_tapdb/core_config`
directory. That packaged path is the only canonical TapDB core pack.

Service/client repos should place their app-owned packs under
`config/tapdb_templates/` and load them through TapDB:

```bash
tapdb db config validate --config ./config/tapdb_templates --strict
tapdb --client-id atlas --database-name lsmc-atlas db data seed dev --config ./config/tapdb_templates
```

Direct `generic_template` creation or mutation from client code is not a
supported path. If runtime code needs a template, it should require an already
seeded template rather than defining one in Python.

## Integrating TapDB Into a Parent FastAPI + Jinja2 App

If your app already owns its own FastAPI and Jinja2 stack, treat TapDB as a
library dependency rather than a standalone web shell. The Atlas workspace in
`../../lsmc/lsmc-atlas` is the best current example of that pattern:

- the parent app creates and configures `FastAPI()`
- the parent app owns session middleware, auth gates, and trusted-host/origin checks
- the parent app owns Jinja templates, static assets, and its route surface
- TapDB is used through its persistence/runtime helpers, not by mutating core
  templates directly
- TapDB template packs are seeded from JSON packs, not authored dynamically at
  runtime

That same shape works well for any FastAPI + Jinja2 host app that wants to use
TapDB-backed repositories while keeping the host application in control of UI
composition and authorization.

## Admin UI Integration

The optional TAPDB admin UI can still be embedded in a parent app, but the
mount strategy should be explicit:

- `Host-gated mount`: the parent app protects `/tapdb` itself and sets
  `TAPDB_ADMIN_DISABLE_AUTH=true` so the mounted admin app defers to host
  authentication.
- `Shared-auth mount`: use this only when the host can provide the signed
  session cookie shape TAPDB currently expects. The session payload must expose
  `user_data.email` and `user_data.role` for the shared-auth path to work.

For the current supported admin-mount guidance, see
[docs/tapdb_gui_inclusion.md](docs/tapdb_gui_inclusion.md).

For local PostgreSQL, TAPDB now uses a namespaced user-writable Unix socket
directory under `~/.config/tapdb/<client>/<database>/<env>/postgres/run` by
default, so `tapdb pg start-local <env>` does not depend on system paths such
as `/var/run/postgresql`. You can override that path with
`environments.<env>.unix_socket_dir` or `TAPDB_<ENV>_UNIX_SOCKET_DIR` when you
need a custom local runtime location.

## Admin UI

The admin UI is optional and ships with the `admin` extra.

Key modes:

- native TapDB auth
- shared auth for compatible host cookies
- auth-disabled local development

See the integration section above for parent-app patterns and the focused
mount guide for the supported admin embedding details.

## Timezone Policy

- persisted timestamps are UTC
- user display timezone preference lives in TapDB-backed `system_user` preferences
- canonical preference key: `display_timezone`

## Current Docs

- [Docs index](docs/README.md)
- [GUI inclusion guide](docs/tapdb_gui_inclusion.md): admin mounting and auth modes

Historical execution plans and breaking-change notes remain in `docs/` for background only.

<!-- release-sweep: 2026-03-10 -->
 
 
