# Runtime and CLI

This guide lives in the `tapdb-core` repository. The Python import package remains `daylily_tapdb`.

This document describes how TAPDB is operated today: config-first, namespace-scoped, and CLI-driven. It is intentionally about the substrate layer, not about any one application domain.

## Operating Model

TAPDB treats a deployment as a namespace made from `client_id` + `database_name` + `env`.

The CLI resolves that namespace from config metadata and explicit runtime flags:

```bash
tapdb --config /abs/path/to/tapdb-config.yaml --env dev ...
```

The config file must carry `meta.client_id` and `meta.database_name`. The runtime context helpers live in [`daylily_tapdb/cli/context.py`](../daylily_tapdb/cli/context.py), and the public command groups are defined in [`daylily_tapdb/cli/__init__.py`](../daylily_tapdb/cli/__init__.py).

Config initialization is the main exception: it requires `--config` but creates the metadata that later runtime commands resolve through `--env`.

### Namespace Layout

The current runtime layout is rooted under:

```text
~/.config/tapdb/<client-id>/<database-name>/
```

The environment-specific runtime tree is then nested beneath the active `env`:

```text
~/.config/tapdb/<client-id>/<database-name>/<env>/
```

Important subpaths used by the CLI:

- `ui/` for UI PID, logs, and certs
- `postgres/` for local Postgres state
- `locks/` for local lock metadata

For most namespaces the local Postgres socket directory lives under
`<env>/postgres/run/`. When the full path would exceed PostgreSQL's Unix socket
path limit, TAPDB automatically falls back to a short deterministic temp
directory so the local bootstrap path remains runnable from deep worktrees or
temporary test paths.

The namespace model is tested indirectly through the CLI and integration suite, and the canonical config example lives in [`config/tapdb-config-example.yaml`](../config/tapdb-config-example.yaml).

## Command Groups

The root command is `tapdb`. The current help surface shows these top-level groups:

- `version`
- `info`
- `config`
- `bootstrap`
- `ui`
- `db-config`
- `db`
- `pg`
- `user`
- `cognito`
- `aurora`

That split is deliberate:

- `pg` manages local or system PostgreSQL processes.
- `db` manages database lifecycle, schema, migrations, backup, and data seeding.
- `ui` manages the admin server process.
- `bootstrap` is the one-command orchestration path.
- `cognito` is the TAPDB-side bridge to `daylily-cognito`.
- `aurora` is optional cloud infrastructure support.

## Local Lifecycle

The basic local flow is:

1. Create or update the namespace config.
2. Initialize a local Postgres data directory.
3. Start the local Postgres instance.
4. Apply schema.
5. Seed built-in templates and any client packs.
6. Optionally start the admin UI.

The CLI surfaces for that flow are already present and exercised in tests:

```bash
tapdb --config <path> db-config init \
  --client-id <client-id> \
  --database-name <database-name> \
  --euid-client-code <client-code> \
  --env dev \
  --db-port dev=5533 \
  --ui-port dev=8911

tapdb --config <path> --env dev pg init dev
tapdb --config <path> --env dev pg start-local dev
tapdb --config <path> --env dev db config validate
tapdb --config <path> --env dev db schema apply dev
tapdb --config <path> --env dev db data seed dev
tapdb --config <path> --env dev bootstrap local --no-gui
```

`tapdb bootstrap local` is the preferred orchestration entrypoint for local developer setup. It includes optional flags for `--no-gui`, `--include-workflow`, and `--insecure-dev-defaults` for dev-only bootstrap flows. The command is documented by the CLI help and covered by the CLI test suite in [`tests/test_cli.py`](../tests/test_cli.py) and [`tests/test_cli_coverage.py`](../tests/test_cli_coverage.py).

### Schema and Seed Flow

Schema work is separated from data seeding:

- `tapdb db schema apply <env>` applies the SQL schema.
- `tapdb db config validate` checks namespace config and template-pack structure without touching the database.
- `tapdb db schema migrate <env>` runs tracked SQL migrations.
- `tapdb db data seed <env>` loads templates into the database.

That separation matters because TAPDB uses the database as the runtime source of truth for templates, while JSON packs are just input material for seeding and validation. The schema and seed commands are part of the CLI contract and are exercised against a real ephemeral PostgreSQL instance in [`tests/test_pg_integration.py`](../tests/test_pg_integration.py).

## Status And Info

`tapdb info` is the operator-facing status surface. It reports:

- resolved namespace context
- effective config path
- UI PID and log locations
- runtime root
- Postgres probe status
- optional JSON output via `--json`

It also performs best-effort `psql` probes when available. The implementation lives in [`daylily_tapdb/cli/__init__.py`](../daylily_tapdb/cli/__init__.py), and the runtime context resolution lives in [`daylily_tapdb/cli/context.py`](../daylily_tapdb/cli/context.py).

## UI Runtime

The admin UI is managed separately from the database, but still inside the TAPDB namespace model.

Relevant commands:

```bash
tapdb --config <path> --env <name> ui start
tapdb --config <path> --env <name> ui stop
tapdb --config <path> --env <name> ui status
tapdb --config <path> --env <name> ui restart
tapdb --config <path> --env <name> ui logs
```

The UI server supports foreground/background operation, explicit port and host overrides, and explicit TLS certificate overrides. The admin app itself is loaded through [`daylily_tapdb.cli.admin_server`](../daylily_tapdb/cli/admin_server.py).

## Aurora

Aurora support is available, but it is optional infrastructure rather than the default developer path.

- `tapdb aurora ...` manages cloud cluster lifecycle.
- `tapdb db ...` still owns the logical database operations.
- The documentation should treat Aurora as an advanced path, not the baseline.

That keeps the README and local docs aligned with the current CLI contract and avoids mixing cloud rollout mechanics into the core mental model.
