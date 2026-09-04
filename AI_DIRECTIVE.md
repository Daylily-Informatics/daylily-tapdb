# AI Directive: daylily-tapdb

Use this document when changing, operating, or integrating TapDB. The public
API, CLI help, schema assets, and active docs are authoritative when this file
and code disagree.

## Purpose and boundary

TapDB is a typed object, lineage, audit, external-reference, and transactional
messaging substrate. It is not an untyped graph database, workflow engine, or
application-domain authority. Keep business meaning, workflow policy, and
service-specific access decisions in the consuming repository.

Prefer public TapDB APIs and CLI commands. Do not use ad hoc SQL for normal
object creation, schema lifecycle, seeding, or repair.

## Explicit target rule

Every stateful CLI operation uses one explicit config:

```bash
tapdb --config <path> ...
tapdb --config <path> --json info
```

There is no `--env` runtime selector and no ambient config, database, schema,
localhost, or public-schema fallback. `meta.client_id`, `meta.database_name`,
`meta.owner_repo_name`, registry paths, and the complete `target` live in the
same config. `--client-id` and `--database-name` are config-initialization or
migration metadata, not alternate runtime selectors.

The normal local setup is:

```bash
tapdb --config <path> db-config init \
  --client-id <client> \
  --database-name <database> \
  --owner-repo-name <owner-repository> \
  --domain-code <domain-code> \
  --domain-registry-path /abs/path/to/domain_code_registry.json \
  --prefix-ownership-registry-path /abs/path/to/prefix_ownership_registry.json \
  --engine-type local \
  --host localhost \
  --port 5533 \
  --ui-port 8911 \
  --user <database-role> \
  --database <physical-database> \
  --schema-name <schema>

tapdb --config <path> bootstrap local --no-gui
```

Use `tapdb --help` and subcommand help for the current option set. Do not copy
old command shapes from historical plans.

## Command ownership

- `tapdb db-config init` creates the explicit namespaced target config;
  `tapdb config` provides generic config-file operations.
- `tapdb bootstrap` orchestrates local or Aurora setup.
- `tapdb pg` controls the configured local PostgreSQL runtime.
- `tapdb db` owns database, schema, migration, seed, and config validation.
- `tapdb backup` owns plan, create, verify, list, restore, and rehearsal.
- `tapdb validation` assesses evidence without persisting an assessment.
- `tapdb repair` creates explicit repair evidence without rewriting the subject.
- `tapdb templates` manages repository-owned template packs.
- `tapdb objects` performs exact-selector governed object operations.
- `tapdb ui` controls the standalone canonical TapDB GUI server.
- `tapdb users` manages actor-backed TapDB users.
- `tapdb cognito` manages the TapDB side of Cognito integration.
- `tapdb aurora` manages optional cloud infrastructure.

Destructive operations remain explicit. Never turn a dry run, plan, review, or
preflight into an apply without the exact required confirmation or receipt.

## Core templates

The bundled core inventory is exactly ten substrate templates:

1. `actor/user/system/1.0/`
2. `set/generic/generic/1.0/`
3. `governance/validator/definition/1.0/`
4. `governance/terminology/set/1.0/`
5. `governance/relationship/constraint/1.0/`
6. `governance/position/scheme/1.0/`
7. `evidence/repair/record/1.0/`
8. `reference/external_identifier/tapdb_object/1.0/`
9. `reference/external_identifier/opaque/1.0/`
10. `message/webhook/event/1.0/`

Do not add application-domain packs to TapDB core. Core loads before explicit
consumer packs, and duplicate coordinates hard-fail rather than override.

## EUID and identity rules

TapDB uses `meridian-euid==0.4.8` with explicit domain and prefix ownership
registries. Preserve minted EUIDs forever. Never generate, hash, format, or
document a string that merely looks like a Meridian EUID. A link to
`/tapdb/object/...` is valid only after the owning TapDB service confirms that
the object exists.

Natural identity claims use
`InstanceFactory.claim_instance_by_identity(...)` with an explicit
`IdentityScope.GLOBAL` or `IdentityScope.TENANT` inside an already-active
transaction. The result is `CREATED` or `EXISTING`; every replay returns the
stored winner. TapDB does not compare consumer payload fingerprints; clients
such as Dewey own any divergent-payload `409`. The factory never controls the
outer transaction.

## Relationship and external-reference rules

Durable relationships exist only in `generic_instance_lineage`. Do not model an
edge with raw `object_euid`, `target_object_euid`, another `*_object_euid`
property, or an ad hoc graph blob.

Cross-service relationships use only `daylily_tapdb.external_references` and
require all of the following:

1. a foreign EUID returned by the target owning service;
2. a persisted `reference/external_identifier/tapdb_object/1.0/` instance;
3. authoritative lineage from the local source to that reference;
4. assertion time and provenance.

Use `TapDBObjectTarget` for graph-expandable TapDB targets and
`ExternalIdentifierTarget` for exact opaque identifiers. Opaque identifiers
must be explicitly tenant-scoped or `public_global`. Only
`ExternalReferenceService` may create, reuse, detach, reactivate, reconcile, or
reverse-query core XRFs. It never commits or rolls back the outer transaction.
Do not create, update, or delete XRFs through generic object APIs.

Metadata may improve display or search but cannot become relationship,
ownership, tenant, or authorization evidence.

## Transaction security

Every PostgreSQL runtime transaction installs these values atomically:

- exact config identity and schema;
- Meridian domain and owner repository;
- tenant scope, including explicit empty global scope;
- authenticated actor;
- explicit global-row write policy.

Missing or malformed context fails closed. Do not catch and continue, install
checkout-level ambient state, or attribute a write to `unknown`.

Protected tables use enabled and forced RLS with both `USING` and `WITH CHECK`
policies. Runtime roles must be `NOSUPERUSER NOBYPASSRLS` and must not have
`CREATE` on the managed TapDB schema. Schema apply and migration revoke that
privilege from `PUBLIC` and the configured runtime role without changing its
DML grants. Runtime code must never create or alter schema objects—even behind
`IF NOT EXISTS`. Provision consumer-owned objects explicitly with a migration
role.

Schema/bootstrap and migration code must opt into
`connection_role="operator"`; never expose that credential to an application
process or use it to bypass runtime checks.

`schema/rls.sql` is canonical for fresh apply, migration, packaged schema
inventory, backup drift verification, and ad hoc integration schemas. Do not
duplicate its functions or policies in a fixture.

## Web auth and embedding

`daylily_tapdb.gui.create_tapdb_gui_app(...)` is the only standalone and
embeddable GUI/JSON implementation. Supply
`TapdbHostBridge(auth_mode="host_session", ...)` when the host owns browser
auth. TapDB-native auth is for a standalone TapDB login flow. The former
`admin.main` application has been removed; internal `admin.*` modules are
support libraries, not another web stack. All reads and writes require
authentication, and mutation routes enforce administrator role plus explicit
apply/dry-run contracts.

## DAG v2

New consumers use `mount_tapdb_dag_surfaces(...)` with:

- one existing absolute config path;
- one immutable, exact `service_id` matching fleet registration;
- an explicit auth dependency;
- positive bounded depth, node, and page-size limits.

Mounting is atomic. Publish `app.state.tapdb_dag_v2_advertisement` only from a
successful mount; a failure leaves no partial route or advertisement. Stable
eligibility reasons must remain machine-readable.

The authenticated routes are:

- `GET /api/dag/manifest`
- `GET /api/dag/v2/object/{euid}`
- `GET /api/dag/v2/data`
- `GET /api/dag/v2/search`

Search is bounded opaque-cursor discovery, never ownership proof. Exact lookup
returns `404 object_not_owned` for a non-owned EUID. Graph output includes
revision, snapshot, presentation, limits, and truncation. Project only outbound
typed references backed by persisted EUIDs and lineage. DAG v2 performs no
outbound fetch. DAG v1, URL/auth routing metadata, metadata-derived edges,
external-graph proxying, and protocol fallback do not exist.

For global search or traversal, applications may use
`daylily_tapdb.federation.DagV2FederationClient` with an exact fleet inventory
and their own authenticated `DagV2Transport`. TapDB validates manifests and
payload identities, namespaces node IDs, exposes per-service receipts and
unresolved boundaries, and enforces hard service, concurrency, jump, node,
search, and deadline limits. TapDB never owns credentials, discovers services,
retries aliases, or forwards one service's auth to another.

See `docs/consumer-discoverability-guide.md` for the tested adoption flow.

## Testing and release floor

TapDB 10.0.0 supports PostgreSQL 16 and 17. Release qualification runs the same
complete suite against community PostgreSQL 16.13 and the PostgreSQL 17 minor
reported by CI without deselecting integration tests. Aurora PostgreSQL has not
been independently qualified by this release. The matrix enables local
documentation examples and requires no unexpected skips. Shared CI gates also
run Ruff check and format, mypy, Bandit, detect-secrets, branch coverage for
`daylily_tapdb` and `admin`, wheel build, schema-asset inspection, and
installed-wheel smoke checks.

Do not weaken RLS, auth, exact identity, no-fallback, or evidence checks to make
a test pass. Fix the fixture to supply the same explicit contract as runtime.

## Public-safety rule

README, active docs, examples, fixtures, and agent guidance must not include
private machine paths, credentials, internal hostnames, organization-only
deployment details, or invented EUIDs. Historical plans may describe older
states; never promote them over current code and active documentation.
