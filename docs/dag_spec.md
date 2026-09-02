# TapDB DAG Contract

The current federation surface is DAG v2 (`tapdb.dag_v2`, contract `dag:v2`).
It is an authenticated, bounded projection of TapDB objects and authoritative
lineage. The controlling acceptance specification is preserved at
[`docs/plans/20260901T100631Z_kahlo_global_dag_tapdb_eligibility_spec.md`](plans/20260901T100631Z_kahlo_global_dag_tapdb_eligibility_spec.md).

## Eligibility and mount

Use `mount_tapdb_dag_surfaces(...)` with one existing absolute config path, one
immutable exact fleet `service_id`, an explicit auth dependency, and a
`DagV2Limits` instance containing positive bounds. Mounting is atomic: a
failure registers no partial routes and publishes no advertisement.

The successful advertisement declares:

- extension `tapdb.dag_v2`;
- contract `dag:v2`;
- exact service identity and display name;
- exact endpoint kinds and paths;
- positive hard limits;
- typed-reference, presentation, and snapshot features;
- `outbound_fetch: false`;
- a deterministic manifest revision.

Consumers reject absent manifests, v1/unknown versions, aliases or identity
mismatches, non-positive limits, missing endpoint kinds, or `eligible` values
other than `true`.

## Authenticated routes

### `GET /api/dag/manifest`

Returns the mounted manifest. It requires the same host session or service
credential as every other v2 route.

### `GET /api/dag/v2/object/{euid}`

Performs exact local lookup. It is the ownership-proof endpoint. A value such
as `<persisted-euid>` must come from TapDB or another owning service; clients
must not construct one. A non-owned EUID returns `404 object_not_owned`.

The payload contains the local typed object, safe properties, validated graph
presentation, and outbound typed-reference projections. It does not include
legacy remote routing metadata.

### `GET /api/dag/v2/search`

Performs bounded local discovery with an opaque keyset cursor. Filters execute
in SQL. `page` contains `limit`, `returned`, and `next_cursor`; it does not
promise a total count. A search hit is a candidate, never proof of ownership.
The consumer must call exact lookup before assigning ownership.

### `GET /api/dag/v2/data`

Traverses local persisted lineage from `start_euid` within service hard bounds.
The caller may request a smaller `depth` or `max_nodes` but cannot exceed the
manifest. The response carries:

- service and contract identity;
- `graph_revision` and `snapshot_at`;
- effective depth/node limits;
- explicit truncation state and reason;
- typed nodes with validated presentation metadata;
- typed lineage edges with relationship semantics, assertion time,
  provenance, and evidence references;
- projected outbound typed references.

Traversal rejects self-loops, cycles, cross-domain/owner edges, and unapproved
cross-tenant edges. A typed global reference must be explicitly approved and
cannot relax ownership or authorization.

## External references

A v2 cross-service reference is authoritative only when a local source object
is connected through `generic_instance_lineage` to a persisted
`reference/external_identifier/tapdb_object/1.0/` instance. The target EUID
comes from the target owning service. TapDB itself mints the reference and
lineage EUIDs.

Raw `object_euid`, `target_object_euid`, other `*_object_euid` properties, or
`external_payload.tapdb_graph` blobs do not create v2 edges and are rejected on
ordinary nodes.

DAG v2 never fetches the target service. The consumer reads the outbound
projection, resolves the exact registered target service, authenticates to it,
validates its manifest, and performs target exact lookup directly.

## Presentation

`properties.graph_presentation` is validated display guidance. Supported data
describes node role, collapse behavior, expected fan-out relationship types,
maximum degree, and a reason. It cannot create relationships, establish
ownership, change tenant scope, or grant traversal access.

## DAG v1 boundary

`create_tapdb_dag_router(...)` is a separate legacy v1 surface. It is not a
fallback for v2 and should not be mounted by a new adopter. Its outbound proxy
is disabled by default. An existing deployment may enable it only with an
explicit `V1ProxyPolicy`: exact HTTPS DNS allowlist, public DNS resolution,
timeout at most ten seconds, response limit at most five MiB, no redirects,
JSON content type, and no forwarded credentials.

Every v1 route also requires the host's explicit authentication dependency.
Missing or non-callable auth fails router construction; the dependency must
reject anonymous callers with `401` or `403` and return an authenticated
identity for authorized callers:

```python
from daylily_tapdb.web import create_tapdb_dag_router

legacy_router = create_tapdb_dag_router(
    config_path="/abs/path/to/tapdb-config.yaml",
    auth_dependency=require_service_or_user,
)
app.include_router(legacy_router)
```

See the [consumer discoverability guide](consumer-discoverability-guide.md) for
runnable request flows, typed-reference creation, troubleshooting, and the
adopter checklist.
