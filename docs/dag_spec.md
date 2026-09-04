# TapDB DAG v2 Contract

TapDB 10 exposes one graph protocol: `tapdb.dag_v2` with contract `dag:v2`.
It is an authenticated, bounded projection of persisted typed objects and
authoritative `generic_instance_lineage`. The original eligibility
specification remains preserved at
[the Kahlo global DAG plan](plans/20260901T100631Z_kahlo_global_dag_tapdb_eligibility_spec.md).

## Eligibility and atomic mount

Use `mount_tapdb_dag_surfaces(...)` with:

- one existing absolute TapDB config path;
- one immutable exact fleet `service_id`;
- one exact display name;
- one explicit authentication dependency;
- one `DagV2Limits` value with positive bounds.

A failed mount registers no route and publishes no advertisement. A successful
manifest carries the exact endpoint paths, service identity, limits, features,
and deterministic revision.

Consumers reject an absent manifest, any unknown contract, a service-ID
mismatch, a missing endpoint, invalid bounds, or `eligible` other than true.
There are no aliases or fallback versions.

## Authenticated routes

### `GET /api/dag/manifest`

Returns the exact mounted capability contract. All other graph operations first
validate it.

### `GET /api/dag/v2/object/{euid}`

Performs exact local lookup and is the ownership-proof endpoint. The requested
value must be an EUID returned by TapDB or another owning service; never invent
one. A non-owned value returns `404 object_not_owned`.

The response contains typed identity, safe public properties, presentation
hints, `external_refs`, and `external_identifiers`. It contains no base URL,
auth mode, credential, or route template.

### `GET /api/dag/v2/search`

Performs SQL-bounded discovery with an opaque keyset cursor. Search results are
candidates and do not prove ownership.

External target filtering accepts exactly one complete group:

- `external_service_id` plus `external_object_euid`; or
- `external_namespace`, `external_kind`, plus `external_value`.

`external_relationship_type` is optional. External filters require
`record_type=instance`. Mixed or incomplete groups return `422`.

### `GET /api/dag/v2/data`

Traverses local persisted lineage from `start_euid`. A caller may request lower
depth or node bounds but cannot exceed the manifest.

Every response contains:

- exact contract and service identity;
- `graph_revision` and `snapshot_at`;
- effective limits;
- `truncated` and `truncation_reason`;
- typed nodes;
- persisted lineage edges;
- canonical external projections.

Traversal rejects self-loops, cycles, cross-domain/owner edges, and unapproved
cross-tenant lineage.

## External projections

A graph-expandable reference is authoritative only when a local source has
persisted lineage to a canonical
`reference/external_identifier/tapdb_object/1.0/` XRF. It projects the exact
target service/EUID and assertion evidence.

A non-expandable identifier uses
`reference/external_identifier/opaque/1.0/` with explicit tenant or
`public_global` scope.

Copied `object_euid`, `target_object_euid`, other `*_object_euid` properties,
`external_payload.tapdb_graph`, and URL-bearing graph blobs are rejected as
relationship evidence. TapDB DAG routes never fetch another service.

## Federation

`DagV2FederationClient` accepts exact `DagServiceTarget` values and an
application-owned authenticated `DagV2Transport`. It validates manifests,
searches in parallel, resolves owners with exact lookup, namespaces global IDs,
follows canonical references under hard limits, and returns receipts,
warnings, and unresolved boundaries.

The transport owns credentials and HTTP. TapDB owns no fleet discovery,
credential forwarding, URL fallback, alias, retry, or visualization UI.

## Presentation

`properties.graph_presentation` is validated display guidance only. It can
describe role, collapse preference, and bounded expected fan-out. It cannot
create a relationship, establish ownership, change scope, or grant access.

## Removed protocols and writers

TapDB 10 does not include DAG v1, its manifest/router, the external graph
proxy, URL/auth routing fields, the metadata parser, the v2 compatibility
adapter, admin proxy endpoints, or the embedded GUI external-link writer.
Missing canonical data fails clearly.
See the [consumer discoverability guide](consumer-discoverability-guide.md) and
[external-reference and federation guide](external-references-and-federation.md)
for integration examples and migration guidance.
