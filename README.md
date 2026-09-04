<p align="center">
  <strong>TapDB</strong><br>
  Typed objects, immutable Meridian EUIDs, lineage, audit, and discoverable DAG surfaces for Python services.
</p>

<p align="center">
  <a href="https://github.com/Daylily-Informatics/daylily-tapdb/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/Daylily-Informatics/daylily-tapdb/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pypi.org/project/daylily-tapdb/"><img alt="PyPI" src="https://img.shields.io/pypi/v/daylily-tapdb.svg"></a>
  <a href="https://pypi.org/project/daylily-tapdb/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/daylily-tapdb.svg"></a>
</p>

<p align="center">
  <a href="docs/runtime-and-cli.md">Operate</a> ·
  <a href="docs/integration-and-embedding.md">Embed</a> ·
  <a href="docs/consumer-discoverability-guide.md">Discover</a> ·
  <a href="docs/external-references-and-federation.md">Federate</a> ·
  <a href="docs/template-authoring.md">Model</a> ·
  <a href="docs/backup-and-recovery.md">Recover</a>
</p>

## Why TapDB

TapDB is a reusable persistence substrate for services that need typed,
versioned objects with stable identifiers and authoritative relationships. It
provides templates, generic instances, immutable EUIDs, lineage, audit history,
transactional messaging records, and authenticated embeddable web surfaces.

TapDB is not an untyped graph database, workflow engine, or domain application.
The owning service defines business meaning and access policy. Relationships
belong in `generic_instance_lineage`; metadata may support display and search,
but never becomes the relationship authority.

## Install

TapDB 10.0.0 requires Python 3.12 or newer and supports PostgreSQL 16 and 17.
Release qualification runs against community PostgreSQL 16.13 and the
PostgreSQL 17 minor reported by CI. Aurora PostgreSQL has not been independently
qualified by this release.

```bash
python -m pip install "daylily-tapdb[cli,gui]"
```

TapDB pins `meridian-euid==0.4.8`. Consumers must not replace that pin with an
unverified range or synthesize strings that resemble Meridian EUIDs.

## Quick start

From a source checkout:

```bash
source ./activate
tapdb --help
tapdb --config <path> ...
tapdb --config <path> bootstrap local --no-gui
tapdb --config <path> --json info
```

Every stateful command takes one explicit config path. There is no environment
selector, ambient database discovery, or implicit fallback target. Config
initialization records the client, logical database, physical database,
schema, Meridian domain, prefix registry, and owner repository in one file.

Runnable examples live in the repository:

- [`examples/readme/00_smoke.sh`](examples/readme/00_smoke.sh) activates the
  checkout and verifies the CLI.
- [`examples/readme/10_bootstrap_local.sh`](examples/readme/10_bootstrap_local.sh)
  creates an isolated PostgreSQL target from an explicit config.
- [`examples/readme/20_python_api.py`](examples/readme/20_python_api.py) creates
  an object through the public Python API and prints the EUID that TapDB
  actually persisted.

The public Meridian registry is maintained by
[`lsmc-bio/meridian-registry`](https://github.com/lsmc-bio/meridian-registry).
For example, validate domain `Q` with:

```bash
meridian-euid domain-check Q \
  --registry-index /abs/path/to/meridian-registry/registry/generated/domains.json
```

Domain registration does not grant a prefix claim. The explicit TapDB prefix
ownership registry remains authoritative for prefixes.

## Object model

TapDB stores four primary kinds of durable facts:

| Fact | Authority |
|---|---|
| Object shape and version | `generic_template` |
| Persisted typed object | `generic_instance` |
| Object-to-object relationship | `generic_instance_lineage` |
| Actor-attributed change evidence | `audit_log` |

All runtime PostgreSQL access installs schema, config identity, domain, owner,
tenant, actor, and global-row policy together inside the transaction. Row-level
security is forced on protected tables. Runtime roles with `SUPERUSER` or
`BYPASSRLS` are rejected. Schema apply and receipt-bound migration also revoke
`CREATE` on the managed TapDB schema from `PUBLIC` and its runtime role, while
leaving runtime DML grants unchanged. Schema operators must use the distinct
migration connection role; application startup, reads, and writes must never
create schema objects, including through `CREATE TABLE IF NOT EXISTS`.

### Bundled templates

The core pack contains exactly ten substrate templates:

| Category / type / subtype | Purpose |
|---|---|
| `actor/user/system` | Optional bundled GUI/auth user actor; not a universal business primitive |
| `set/generic/generic` | Generic set |
| `governance/validator/definition` | Validator definition |
| `governance/terminology/set` | Terminology set |
| `governance/relationship/constraint` | Lineage constraint |
| `governance/position/scheme` | Position scheme |
| `evidence/repair/record` | Explicit repair evidence |
| `reference/external_identifier/tapdb_object` | Typed external object reference |
| `reference/external_identifier/opaque` | Scoped non-federated external identifier |
| `message/webhook/event` | Transactional webhook event |

Application-specific templates belong in the consuming repository and are
loaded explicitly. Core and consumer packs cannot silently override one
another.

The database operator materializes the exact installed core definitions inside
each configured owner scope, allowing that owner's constrained runtime to use
typed XRF/SYS/MSG objects without owning TapDB's reserved prefixes. A copied
path or modified client-authored template cannot unlock reserved-prefix
seeding.

## Python API

Use the factory inside a caller-owned transaction. Natural identity claims are
atomic and distinguish a new object from an idempotent replay:

```python
from daylily_tapdb import (
    IdentityScope,
    InstanceFactory,
    TAPDBConnection,
    TemplateManager,
)

manager = TemplateManager()
factory = InstanceFactory(manager, domain_code=domain_code)

with connection.session_scope(commit=True) as session:
    claim = factory.claim_instance_by_identity(
        session,
        template_code="message/webhook/event/1.0/",
        identity_key=event_identity_key,
        name="Webhook event",
        scope=IdentityScope.GLOBAL,
        properties=event_properties,
        command_evidence={"source": "consumer"},
    )
    persisted_euid = claim.instance.euid
```

Any replay of the same identity key returns `EXISTING` and the stored winner;
TapDB does not compare consumer payload fingerprints. A race-safe consumer such
as Dewey first claims or reads the committed stored winner, then compares its
client-owned fingerprint and returns its own divergent-payload `409` without
creating a second receipt. The claim API requires an already-active transaction
and never commits or rolls back its caller's transaction.

## Canonical external references

TapDB 10 has one external-reference model and one writer. A local source points
through persisted lineage to a shared, typed `XRF` object. A federated target
names an exact TapDB service and an EUID that service actually persisted; an
opaque target names a non-expandable external identifier such as a DOI or PMID.

```python
from datetime import UTC, datetime

from daylily_tapdb.external_references import (
    ExternalLinkSpec,
    ExternalReferenceService,
    TapDBObjectTarget,
)

target = TapDBObjectTarget(
    target_service_id="atlas",
    target_object_euid=remote_object_euid,
    target_object_kind="analysis",
)
spec = ExternalLinkSpec(
    target=target,
    relationship_type="references",
    assertion_authority="catalog-sync",
    asserted_at=datetime.now(UTC),
    assertion_provenance=sync_receipt,
)

with connection.session_scope(commit=True) as session:
    outcome = ExternalReferenceService(session).attach(source, spec)
```

`attach`, `detach`, authority-scoped `reconcile`, `list_for_source`, and
`find_sources` all participate in the caller's transaction. Replays reuse the
same reference and lineage; reactivation keeps the lineage UID and EUID.
Applications still own remote validation, credentials, synchronization,
business status, and fleet configuration.

There is no URL-bearing reference shape, metadata-derived edge, generic XRF
writer, or legacy GUI writer. Use the
[`external-reference and federation guide`](docs/external-references-and-federation.md)
for scopes, failure modes, reverse lookup, migration guidance, and complete API
examples.

## Discoverable DAG v2

Hosts mount the authenticated v2 contract atomically. A failed mount publishes
no advertisement and registers no partial routes:

```python
from fastapi import FastAPI

from daylily_tapdb.web import DagV2Limits, mount_tapdb_dag_surfaces

app = FastAPI()
result = mount_tapdb_dag_surfaces(
    app,
    config_path="/abs/path/to/tapdb-config.yaml",
    service_id="catalog-api",
    display_name="Catalog API",
    auth_dependency=require_service_or_user,
    limits=DagV2Limits(
        max_depth=6,
        max_nodes=500,
        max_search_page_size=100,
    ),
)
if not result.mounted:
    raise RuntimeError(f"DAG v2 unavailable: {result.reason}: {result.diagnostic}")
```

The mount exposes:

- `GET /api/dag/manifest`
- `GET /api/dag/v2/object/{euid}` for exact ownership lookup
- `GET /api/dag/v2/data` for bounded native traversal
- `GET /api/dag/v2/search` for bounded opaque-cursor discovery

Every route requires auth. The immutable `service_id` must exactly match fleet
registration. Search results are discovery candidates, not ownership proof;
consumers confirm ownership with exact lookup. Graph responses include a
revision, snapshot time, presentation metadata, effective limits, and explicit
truncation. DAG v2 projects only outbound typed references backed by a
persisted external-reference object plus lineage, and it never fetches a remote
v2 service on the caller's behalf.

Exact external-reference search is part of the same endpoint. Supply either
`external_service_id` plus `external_object_euid`, or
`external_namespace`, `external_kind`, and `external_value`; an optional
`external_relationship_type` narrows either group. Incomplete or mixed groups
fail with `422`.

`daylily_tapdb.federation.DagV2FederationClient` composes authenticated DAG-v2
services for Kahlo-style global search and visualization. The application
provides an exact service inventory and its authenticated transport; TapDB
validates manifests, namespaces IDs as `service_id::euid`, follows canonical
references under hard limits, and returns per-service receipts and unresolved
boundaries. TapDB does not discover endpoints, own credentials, forward auth,
retry aliases, or fall back to another protocol.

See the runnable request flow, eligibility reasons, adoption checklist, and
anti-patterns in the
[`consumer discoverability guide`](docs/consumer-discoverability-guide.md).

## Web and GUI embedding

`daylily_tapdb.gui` is the only TapDB web implementation. Its standalone and
embedded forms share the same auth/account, overview, search, template,
object, lineage, repair, audit, inventory, readiness, Meridian, metrics,
runtime, backup/recovery, and graph features. The graph explorer retains
search, filters, layouts, neighborhood and lineage gestures, detail inspection,
JSON export, and Mermaid export; it consumes canonical DAG v2 only.

`TapdbHostBridge` supplies host identity, navigation, and styling without
giving TapDB authority over application policy:

```python
from fastapi import FastAPI

from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.web import TapdbHostBridge

app = FastAPI()
tapdb_gui = create_tapdb_gui_app(
    config_path="/abs/path/to/tapdb-config.yaml",
    host_bridge=TapdbHostBridge(
        auth_mode="host_session",
        service_name="catalog-api",
        app_name="Catalog API",
        resolve_user=resolve_host_user,
        login_url="/login",
    ),
)
app.mount("/tapdb", tapdb_gui)
```

The former `admin.main` ASGI app, DAG v1 router, outbound proxy, legacy graph
payload adapter, URL-bearing external graph merge, and embedded external-link
writer do not exist in 10.0. There is no compatibility alias or fallback.

## Development and release checks

```bash
python -m pytest tests/ -q
ruff check daylily_tapdb admin tests
ruff format --check daylily_tapdb admin tests
mypy
bandit -c pyproject.toml -r daylily_tapdb admin
python -m build
```

Release CI runs the same complete suite independently against community
PostgreSQL 16.13 and PostgreSQL 17, including local-doc examples and branch
coverage. The shared release gates also run Ruff, mypy, Bandit, detect-secrets,
wheel build, and installed-wheel smoke checks. CI does not hide integration
tests with deselects. The mypy file list in `pyproject.toml` covers every new
10.0 implementation module; older dynamically mapped ORM and Typer modules are
not yet globally strict-clean.

## Documentation

- [`docs/architecture.md`](docs/architecture.md): structural model and write path
- [`docs/identity-and-scoping.md`](docs/identity-and-scoping.md): EUID, tenant,
  domain, owner, and runtime scope
- [`docs/template-authoring.md`](docs/template-authoring.md): consumer template packs
- [`docs/runtime-and-cli.md`](docs/runtime-and-cli.md): explicit-target operation
- [`docs/integration-and-embedding.md`](docs/integration-and-embedding.md): GUI and API embedding
- [`docs/consumer-discoverability-guide.md`](docs/consumer-discoverability-guide.md): DAG v2 federation contract
- [`docs/external-references-and-federation.md`](docs/external-references-and-federation.md): canonical XRF lifecycle, federated search/graph composition, and tagged-consumer migration map
- [`docs/backup-and-recovery.md`](docs/backup-and-recovery.md): backup and staged recovery
- [`docs/plans/`](docs/plans/): specifications and execution records
