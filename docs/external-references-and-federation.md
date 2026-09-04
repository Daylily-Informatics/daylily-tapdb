# External References, Discoverability, and Federation

This guide is the TapDB 10.0 contract for linking a local object to something
owned elsewhere and for composing discoverable graphs across a known fleet.
It is written for application developers, visualizer authors, and database
operators.

## The boundary

TapDB owns:

- exact external-target identity and deduplication;
- global or tenant natural-identity scope;
- authoritative local-source-to-XRF lineage;
- assertion ownership, detach, and reactivation lifecycle;
- exact reverse lookup;
- safe DAG-v2 projections and external-target search;
- bounded, transport-neutral federation composition.

Applications own:

- what the relationship means to the business;
- whether a remote target is valid or current;
- synchronization schedules and state;
- credentials and authorization policy;
- exact fleet configuration and service origins;
- remote status, version, and operational metadata;
- visualization and user interaction.

Metadata is never an authoritative edge. A copied `*_object_euid` field, a URL,
or a graph-shaped JSON document may help a display, but it cannot establish
lineage, ownership, or authorization.

## Two target types

### Federated TapDB object

`TapDBObjectTarget` represents an exact object in another TapDB service:

| Field | Contract |
|---|---|
| `target_service_id` | Exact registered DAG-v2 service ID |
| `target_object_euid` | Meridian EUID actually persisted by the owning service |
| `target_tenant_id` | Optional remote tenant UUID descriptor |
| `target_object_kind` | Optional exact application-owned kind descriptor |

The identity is global and is keyed by exact service ID plus exact EUID. A
service rename is therefore an identity change, not an alias.

### Opaque external identifier

`ExternalIdentifierTarget` represents something that cannot be expanded through
TapDB DAG v2:

| Field | Contract |
|---|---|
| `namespace` | Lowercase canonical token, for example `doi` or `pmid` |
| `kind` | Lowercase canonical token |
| `value` | Exact case-preserving identifier, at most 2,048 characters |
| `scope` | Exactly `tenant` or `public_global` |
| `tenant_id` | Required for `tenant`; forbidden for `public_global` |
| `canonical_uri` | Optional absolute, credential-free, fragment-free URI |

TapDB does not normalize identifier values and never fetches a canonical URI.
The built-in GUI hyperlinks only credential-free HTTPS values. Use
`public_global` only for genuinely public identifiers; patient, customer,
account, and organization identifiers should normally be tenant-scoped.

The natural identity key is a SHA-256 digest of canonical JSON containing the
namespace, kind, and exact value. Scope is enforced by separate global and
tenant partial unique indexes. TapDB rechecks persisted fields after every
natural-identity winner is selected.

## Authoritative persisted shape

Both target types use the reserved `XRF` prefix and exact core templates:

- `reference/external_identifier/tapdb_object/1.0/`
- `reference/external_identifier/opaque/1.0/`

The direction is always local source to XRF:

~~~text
local source instance -- persisted generic_instance_lineage --> shared XRF
~~~

The source-to-XRF lineage stores exactly:

- `assertion_authority`
- `asserted_at`
- `assertion_provenance`
- `approved_global_link`
- `deactivated_at`
- `deactivation_provenance`

Do not create or mutate either XRF template through `InstanceFactory`, generic
object HTTP endpoints, or raw SQL. `ExternalReferenceService` is the only
supported lifecycle writer.

## Public lifecycle API

Import the five public lifecycle types only from
`daylily_tapdb.external_references`:

- `TapDBObjectTarget`
- `ExternalIdentifierTarget`
- `ExternalLinkSpec`
- `ExternalLinkOutcome`
- `ExternalReferenceService`

### Attach or replay

~~~python
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
    source = load_source_in_this_session(session)
    result = ExternalReferenceService(session).attach(source, spec)
    assert result.status in {"created", "existing", "reactivated"}
~~~

`attach` locks the source, atomically claims or reuses the XRF, and creates,
replays, or reactivates one exact lineage. An active replay must match immutable
assertion data. A reactivation reuses the same lineage UID and EUID. Missing
optional target descriptors may be enriched once; conflicting non-null values
fail.

### Opaque identifiers

~~~python
from daylily_tapdb.external_references import ExternalIdentifierTarget

public_article = ExternalIdentifierTarget(
    namespace="doi",
    kind="article",
    value=doi_value_from_source,
    scope="public_global",
    canonical_uri=canonical_doi_uri,
)

tenant_identifier = ExternalIdentifierTarget(
    namespace="customer",
    kind="accession",
    value=customer_accession,
    scope="tenant",
    tenant_id=source.tenant_id,
)
~~~

The source tenant must match a tenant-scoped target. Public-global identifiers
have no tenant.

### Detach

`detach(source, target, ...)` soft-deletes only the exact lineage owned by the
specified assertion authority. It records the deactivation time and
provenance, never deletes the shared XRF, and returns `deactivated` or
`already_inactive`. There is no automatic XRF garbage collection.

### Reconcile

`reconcile(source, assertion_authority, desired, ...)` applies one authority's
exact desired set. It locks the source, accepts at most 500 link specs, rejects
duplicates, and leaves every other authority's links untouched. A conflicting
authority for the same source, target, and relationship fails instead of
silently taking ownership.

### List and reverse lookup

`list_for_source` returns active links by default and supports exact
relationship and authority filters, inactive history, a limit, and an opaque
cursor.

`find_sources` performs exact active reverse lookup by target with optional
relationship and authority filters. PostgreSQL RLS still determines which
source rows the caller may see.

Both methods are bounded and use keyset pagination. Do not decode or invent a
cursor; return it unchanged on the next call.

### Transaction ownership

No lifecycle method commits or rolls back. The application owns the outer
transaction. If that transaction rolls back, a newly claimed XRF and its
lineage disappear together. Callers must pass a persisted active source loaded
inside the correctly scoped PostgreSQL transaction.

## Lifecycle failures that should stop the operation

TapDB fails closed for:

- malformed target identity or a non-persisted-looking EUID;
- missing, extra, or conflicting canonical fields;
- a target whose persisted identity key does not match its fields;
- a divergent active replay;
- authority conflict;
- more than one active or historical candidate;
- cross-tenant mismatch;
- a generic attempt to create, update, or delete a core XRF;
- absent transaction or missing source visibility.

Do not catch these and create an alternate representation. Run the read-only
audit, preserve evidence, and perform an explicit repair.

## DAG-v2 producer contract

Mount DAG v2 with an exact config, immutable service ID, explicit auth
dependency, and limits. The authenticated endpoints are:

- `GET /api/dag/manifest`
- `GET /api/dag/v2/object/{euid}`
- `GET /api/dag/v2/data`
- `GET /api/dag/v2/search`

The manifest advertises `tapdb.dag_v2` / `dag:v2` and features
`typed_external_references`, `typed_external_identifiers`, and
`external_reference_search`. `outbound_fetch` is false.

A source node projects:

- `external_refs` for graph-expandable TapDB targets;
- `external_identifiers` for visible, non-expandable opaque targets.

Each item carries relationship, authority, assertion time/provenance, XRF EUID,
and lineage EUID. Federated items additionally carry exact target service and
EUID. Opaque items carry namespace, kind, value, scope, and optional canonical
URI. No public property contains a service base URL, route, auth mode, or
credential.

Graph envelopes include `graph_revision`, `snapshot_at`, `truncated`,
`truncation_reason`, and effective limits. Only persisted lineage becomes an
edge.

## Exact external-reference search

Search one bounded page for a federated target:

~~~bash
curl -fsS -G "https://service.example/api/dag/v2/search" \
  -H "Authorization: Bearer <service-token>" \
  --data-urlencode "record_type=instance" \
  --data-urlencode "external_service_id=atlas" \
  --data-urlencode "external_object_euid=<persisted-euid>" \
  --data-urlencode "limit=100"
~~~

Or search for an opaque identifier with all three fields:

~~~bash
curl -fsS -G "https://service.example/api/dag/v2/search" \
  -H "Authorization: Bearer <service-token>" \
  --data-urlencode "record_type=instance" \
  --data-urlencode "external_namespace=doi" \
  --data-urlencode "external_kind=article" \
  --data-urlencode "external_value=<exact-value>"
~~~

The two filter groups are mutually exclusive. An incomplete group or a
non-instance `record_type` returns `422`. An optional
`external_relationship_type` narrows either group.

Search is discovery, not ownership proof. Confirm any candidate with exact
`GET /api/dag/v2/object/{euid}`. Never replace exact lookup with fuzzy search,
service aliases, or copied metadata.

## Federated search and graph composition

Import federation types from `daylily_tapdb.federation`:

- `DagServiceTarget`
- `FederatedObjectKey`
- `FederationLimits`
- `DagV2Transport`
- `DagV2FederationClient`

The application supplies exact credential-free HTTPS service origins and an
authenticated transport. Credentials remain inside that transport.

~~~python
from daylily_tapdb.federation import (
    DagServiceTarget,
    DagV2FederationClient,
    FederatedObjectKey,
    FederationLimits,
)

targets = [
    DagServiceTarget("atlas", "https://atlas.day.lsmc.bio"),
    DagServiceTarget("bloom", "https://bloom.day.lsmc.bio"),
    DagServiceTarget("ursa", "https://ursa.day.lsmc.bio"),
    DagServiceTarget("dewey", "https://dewey.day.lsmc.bio"),
]
client = DagV2FederationClient(
    targets,
    authenticated_transport,
    limits=FederationLimits(
        max_services=4,
        concurrency=4,
        max_external_jumps=16,
        max_nodes=2000,
        search_limit=100,
        deadline_seconds=20,
    ),
)

search_result = await client.search(filters={"q": query_text}, limit=50)
owner = await client.resolve_owner(candidate_euid)
global_graph = await client.graph(
    FederatedObjectKey(owner.service_id, owner.euid),
    local_depth=6,
)
~~~

Default ceilings are 32 services, concurrency 8, 32 external jumps, 5,000
nodes, search limit 100, and a 30-second deadline. Callers may lower, never
raise, those bounds.

The client:

- validates every manifest against its registered service ID;
- performs parallel search with deterministic `service_id::euid` deduplication;
- resolves ownership with parallel exact lookup, never search inference;
- validates the target manifest again before every external jump;
- namespaces every graph node and edge;
- emits synthetic bridge edges carrying assertion evidence;
- renders opaque IDs as non-expandable nodes;
- reports unresolved boundaries and per-service receipts;
- rejects cycles, collisions, malformed payloads, and ambiguous ownership.

A starting-service graph failure is fatal. Search can be partial only when at
least one service succeeds and every failure is reported. Owner resolution
requires every admitted exact lookup to complete successfully. The client has
no aliases, retries, alternate endpoints, service discovery, credential
forwarding, or older-protocol fallback.

## Designing for discoverability

A TapDB consumer is easiest to find and visualize when it does all of the
following:

1. Register one stable DAG-v2 `service_id` and keep it exact across deployment,
   manifest, XRF targets, and fleet configuration.
2. Put durable local relationships in `generic_instance_lineage`.
3. Attach cross-service targets with `ExternalReferenceService`.
4. Use accurate template coordinates, names, category/type/subtype, and bounded
   public properties for search and display.
5. Use a stable `assertion_authority` per synchronizer so reconciliation cannot
   affect another producer.
6. Preserve exact timestamps and provenance receipts.
7. Use opaque IDs for DOI, PMID, PMCID, cloud IDs, or vendor identifiers that
   cannot participate in DAG-v2 traversal.
8. Use exact external filters to find local owners, then exact object lookup to
   verify them.
9. Surface `truncated` and every federation warning; never present a partial
   graph as complete.
10. Treat unresolved boundary nodes as actionable evidence, not permission to
    guess a URL or alias.

For Kahlo, the neutral federation payload is the input to visualization. Kahlo
owns layout, interaction, fleet credentials, and presentation; it should not
reimplement graph merge rules.

## Canonical GUI

`daylily_tapdb.gui` is the sole web stack. It includes the former
`admin.main` capabilities: auth/account flows, overview, object and template
operations, lineage and repair, audit, inventory, readiness, Meridian checks,
metrics, runtime inspection, backups/recovery, search, and the rich graph
explorer.

The graph explorer provides fuzzy and exact find, degree transparency, distance
filtering, type visibility, subtype muting, Dagre/CoSE/breadth-first/circle/grid
layouts, waves and neighborhoods, admin lineage creation and soft deletion,
details, DAG JSON download, and Mermaid export. External projections are
read-only. DAG v1 proxying and URL-bearing graph merge are absent.

## Read-only operator audit

Run:

~~~bash
tapdb --config <path> --json validation external-references --sample-limit 25
~~~

The command reports:

- canonical template seed state;
- canonical reference counts by scope;
- malformed XRFs;
- raw graph metadata;
- copied pseudo-edge fields;
- duplicate historical links;
- bounded samples containing TapDB EUIDs and field names only.

It never emits external identifier values or canonical URIs, never fetches a
remote service, and never mutates or flushes the database. A violation produces
a nonzero exit. Audit first; convert or delete only through a separately
approved application migration.

## Upgrade invariants

The TapDB 10 schema migration replaces the old global-only natural-identity
index with global and tenant-scoped partial unique indexes. It does not update
data rows.

Before and after migration, verify that all existing:

- UIDs;
- EUIDs;
- EUID sequence values;
- identity keys;
- template identities;
- source objects;
- XRFs;
- lineage rows

are byte-for-byte or value-for-value unchanged. The release performs no
automatic conversion of application-owned external objects or legacy metadata.
After tenant-scoped identities have been created, downgrade is restore-only
from a verified pre-upgrade backup.

## Tagged-consumer migration map

These immutable released tags were the design references. Updating their
repositories is separate future work.

| Consumer reference | Existing pattern | TapDB 10 target | Remains application-owned |
|---|---|---|---|
| Atlas `7.0.2` (`52e258e5c60f`) | Mutable AGX mirrors, direct lineage, copied graph metadata | Federated `TapDBObjectTarget` plus canonical source-to-XRF lineage | Mirror status, remote validation, copied domain projections |
| Bloom `8.0.0` (`856f92186a04`) | BGX references reminted during replacement | Stable XRF identity with detach/reactivate or authority reconciliation | Business replacement semantics and sync scheduling |
| Ursa `11.0.50` (`2d04a0a79bfe`) | RGX objects mix TapDB and AWS/DYEC/service IDs | TapDB targets for graph objects; scoped opaque targets for cloud/service IDs | AWS validation, credentials, execution and cluster status |
| Dewey `7.0.2` (`695de64f2ca5`) | DGX objects and relation objects include DOI/PMID/PMCID | TapDB targets for owned graph objects; public-global opaque IDs for public literature IDs | Citation semantics, domain relation objects, validation |
| Kahlo `7.0.7` (`63429c1f67a0`) | DAG-v1 merge, aliases, search ownership inference, fallback reads | `DagV2FederationClient` exact search/owner resolution/global graph payload | UI, fleet admission, auth transport, presentation |

Migration should be a deliberate read-map-validate-write process in each
consumer. Do not edit already published TapDB migrations, mint replacement
identities, rewrite source objects, or interpret copied EUID fields as proof.
Use a verified backup and the audit output as preconditions.

## Removed concepts

TapDB 10 has no:

- `tapdb.dag_v1` router or manifest;
- outbound graph proxy;
- URL/auth route fields in public reference data;
- metadata-to-edge parser;
- service alias or owner inference;
- v2 compatibility adapter;
- admin external-graph endpoint;
- embedded GUI external-link writer;
- generic XRF mutation route;
- fallback endpoint or protocol.

A missing configuration, target, manifest, auth context, or canonical
relationship fails clearly. Do not rebuild a removed path in an application.
