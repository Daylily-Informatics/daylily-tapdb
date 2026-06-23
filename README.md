<p align="center">
  <strong>TapDB</strong><br>
  Templated Abstract Polymorphic Database for Dayhoff objects, EUIDs, lineage, audit, and embeddable GUI/API surfaces.
</p>

<p align="center">
  <a href="docs/runtime-and-cli.md">Runtime</a> ·
  <a href="docs/integration-and-embedding.md">Embedding</a> ·
  <a href="docs/template-authoring.md">Templates</a> ·
  <a href="docs/dag_spec.md">DAG</a>
</p>

## Overview

TapDB is the shared substrate library for typed templates, generic instances, EUIDs, lineage, audit, external references, and embeddable object GUI/API surfaces. It is not a workflow engine, clinical decision engine, or service-specific application.

Current Dayhoff pin: `9.0.5`.

Active clients mount TapDB with `TapdbHostBridge` and `create_tapdb_gui_app` at `/tapdb` when they need generic object/template/lineage UI.

## What It Does

| Capability | Current surface |
|---|---|
| Templates and generic instances | CLI, Python APIs, embedded GUI, embedded JSON/action APIs |
| EUID object detail | `/tapdb/object/{euid}` plus object APIs |
| Lineage and external references | Graph/detail pages and lineage/external-link APIs |
| Template creation and validation | GUI/API routes and template tests |
| Audit and metrics | Object audit/admin readiness/metrics pages where configured |

## How It Works

TapDB provides substrate primitives only. Host services supply auth, styling, config path, and app identity through `TapdbHostBridge`; TapDB supplies mount-aware pages and actions over templates, generic instances, lineage, audit, and external refs. Business workflows, clinical policy, and service-specific validation stay in the owning service.

## Quickstart

```bash
cd /Users/jmajor/projects/mega_dayhoff/repos_work/daylily-tapdb
source ./activate
tapdb --help
tapdb --config /abs/path/to/tapdb-config.yaml db schema apply
tapdb --config /abs/path/to/tapdb-config.yaml db data seed
```

Always pass an explicit `--config`. Do not rely on ambient `TAPDB_*`, `PG*`, default database names, or implicit localhost/public-schema behavior.

Runnable examples:

- [`examples/readme/00_smoke.sh`](examples/readme/00_smoke.sh): activation and CLI smoke check.
- [`examples/readme/10_bootstrap_local.sh`](examples/readme/10_bootstrap_local.sh): explicit local config and `bootstrap local --no-gui`.
- [`examples/readme/20_python_api.py`](examples/readme/20_python_api.py): minimal `TAPDBConnection`, `TemplateManager`, and `InstanceFactory` usage.

The canonical command shape is `tapdb --config <path> ...`; machine-readable inspection surfaces should expose `--json info` where supported. Meridian governance should remain aligned with `lsmc-bio/meridian-registry`, for example `meridian-euid domain-check Q`.

## CLI Interface

The primary CLI is `tapdb`. It covers schema apply/migrate, template seed/load/validate, generic object operations, admin server helpers, and runtime diagnostics.

Common command families:

| Family | Purpose |
|---|---|
| `tapdb db schema ...` | Apply or migrate schema through supported migration files. |
| `tapdb db data ...` | Seed template/data packs through explicit config. |
| `tapdb templates ...` | Validate and load template packs. |
| `tapdb objects/lineage ...` | Work with generic instances and lineage where exposed by CLI. |
| `tapdb admin ...` | Start or inspect admin/embedded GUI support where configured. |

For TapDB, the CLI, embeddable JSON/action APIs, and embeddable GUI are alternate surfaces over the same object/template/lineage/audit substrate. Client services decide which surfaces they expose.

## GUI

TapDB provides an embeddable FastAPI GUI. The client-safe pattern is:

```python
from daylily_tapdb.web import TapdbHostBridge, create_tapdb_gui_app

app.mount("/tapdb", create_tapdb_gui_app(config_path="/abs/path/to/tapdb-config.yaml", host_bridge=bridge))
```

Current GUI routes include `/tapdb/`, `/tapdb/search`, `/tapdb/templates`, `/tapdb/templates/new`, `/tapdb/create/{template_euid}`, `/tapdb/object/{euid}`, `/tapdb/object/{euid}/graph`, `/tapdb/object/{euid}/external-links/new`, and admin readiness/metrics pages.

## API

TapDB exposes matching embedded APIs for search, object detail, graph, edit JSON, status, lineage, external links, template validate, create instance, readiness, Meridian validation, and metrics.

TapDB stores v0 graph metadata on existing lineage records using `json_addl.properties.v0_edge` when canonical v0 edge metadata is present. It does not add v0-specific tables.

## Testing Info

Focused checks:

```bash
python -m pytest tests -q
python -m pytest tests/test_gui_json_editor.py -q
```

Client integration tests should prove mounted `/tapdb` routes through the host service, host-session auth, admin role mapping, template creation, object detail/edit, audit, graph, and external-link behavior.

## Technical Details, History, And Linkouts

- [`docs/runtime-and-cli.md`](docs/runtime-and-cli.md): runtime and CLI details.
- [`docs/integration-and-embedding.md`](docs/integration-and-embedding.md): service embedding guide.
- [`docs/template-authoring.md`](docs/template-authoring.md): template authoring.
- [`docs/dag_spec.md`](docs/dag_spec.md): DAG/lineage model.
- [`docs/identity-and-scoping.md`](docs/identity-and-scoping.md): identity, EUID, tenant, and scope model.
- [`docs/plans/`](docs/plans/): TapDB ledgers.

TapDB remains substrate-first. Service-specific workflow rules belong in the owning service.
