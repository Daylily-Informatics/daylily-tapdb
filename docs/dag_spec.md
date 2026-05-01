# TapDB DAG Contract

This contract lives in the `tapdb-core` repository. The Python import package remains `daylily_tapdb`.

This document defines `dag:v1`, the canonical cross-service DAG contract for
TapDB-backed services and graph aggregators such as Kahlo.

## Ownership Rule

- Ownership is determined by exact EUID hit, not search.
- A contributing service must return `200` only for EUIDs it owns in that
  service's current perspective.
- A contributing service must return `404` for non-owned EUIDs.
- If more than one service returns `200` for the same exact EUID probe, that is
  a contract violation. Aggregators must not guess.

## Canonical Endpoints

All TapDB-backed contributors should expose these routes:

- `GET /api/dag/object/{euid}`
- `GET /api/dag/data?start_euid=<euid>&depth=<n>`
- `GET /api/dag/search?...filters`
- `GET /api/dag/external?source_euid=<euid>&ref_index=<i>&depth=<n>`
- `GET /api/dag/external/object?source_euid=<euid>&ref_index=<i>&euid=<remote_euid>`

These routes are implemented in `daylily_tapdb.web.create_tapdb_dag_router(...)`.

## Endpoint Semantics

### `GET /api/dag/object/{euid}`

- Returns exact local object detail for an owned EUID.
- Returns `404` for a non-owned EUID.
- Search behavior, fuzzy matching, aliases, or partial matches are out of
  contract.

Minimum payload shape:

```json
{
  "euid": "Z:BCN-33",
  "system": "bloom",
  "record_type": "instance",
  "type": "container",
  "subtype": "tube",
  "json_addl": {},
  "external_refs": [
    {
      "ref_index": 0,
      "system": "atlas",
      "root_euid": "Z:AGX-A43V",
      "graph_expandable": true,
      "href": "https://atlas.local/api/dag/object/Z:AGX-A43V",
      "reason": null
    }
  ]
}
```

### `GET /api/dag/data`

- Requires `start_euid`.
- Returns the native DAG rooted at that exact owned object.
- If the object exists but has no traversable relationships yet, return a
  one-node graph instead of an error.

Minimum payload shape:

```json
{
  "elements": {
    "nodes": [{ "data": {} }],
    "edges": [{ "data": {} }]
  },
  "meta": {
    "start_euid": "Z:BCN-33",
    "depth": 3,
    "owner_service": "bloom",
    "contract_version": "dag:v1"
  }
}
```

Node `data` must include at least:

```json
{
  "id": "Z:BCN-33",
  "euid": "Z:BCN-33",
  "display_label": "Z:BCN-33",
  "system": "bloom",
  "type": "container",
  "subtype": "tube",
  "href": "/object/Z:BCN-33"
}
```

Edge `data` must include at least:

```json
{
  "id": "edge-1",
  "source": "Z:BCN-33",
  "target": "Z:BCT-3Y",
  "relationship_type": "contains"
}
```

Graph node `data` also includes `external_refs` when the object carries explicit
TapDB graph refs or is a typed external identifier object.

### `GET /api/dag/search`

- Searches local TapDB objects for UI and aggregator entrypoints.
- Supports filters for `q`, exact `euid`, `record_type`, `category`, `type`,
  `subtype`, `tenant_id`, `relationship_type`, and `limit`.
- Search results are candidates. Exact ownership still comes from
  `/api/dag/object/{euid}` or an explicit `service_id + euid` request.

Minimum payload shape:

```json
{
  "items": [
    {
      "system": "bloom",
      "service": "bloom",
      "record_type": "instance",
      "euid": "Z:BCN-33",
      "display_label": "Specimen tube",
      "category": "container",
      "type": "tube",
      "tenant_id": null,
      "relationship_type": null,
      "graph_href": "/api/dag/data?start_euid=Z:BCN-33"
    }
  ],
  "page": {
    "limit": 25,
    "total": 1,
    "next_cursor": null
  },
  "meta": {
    "owner_service": "bloom",
    "contract_version": "dag:v1"
  }
}
```

### `GET /api/dag/external`

- Expands one explicit external reference from a local object.
- The service follows the indexed `external_refs` entry and returns a namespaced
  remote graph payload that can be merged safely into the local DAG view.

### `GET /api/dag/external/object`

- Returns object detail for a node inside a previously expanded external graph.

## Federation Rules

- `external_refs` are the federation contract. Aggregators follow refs; they do
  not infer cross-system joins from arbitrary fields.
- Externally merged nodes and edges must be namespaced to avoid collisions with
  local IDs.
- A service can be perspective-local only. It does not need to be globally
  canonical to contribute.
- Search endpoints are for UI convenience and cross-service entrypoints. They
  are not a substitute for exact ownership.
- Typed external identifier objects are valid federation refs when their
  metadata provides a `system`/`target_system` and `root_euid`/`target_euid`.

## Capability Advertising

Services should advertise the DAG contract through their discovery surface. For
`obs_services`-style payloads, TapDB provides
`daylily_tapdb.web.build_dag_capability_advertisement(...)`.

Current canonical capability labels are:

- `exact_lookup`
- `native_graph`
- `object_search`
- `external_graph_expansion`

## Host Integration

TapDB owns this contract in the shared library so host applications do not need
to reimplement it. A host app should:

1. mount the reusable TapDB HTML surface under a namespaced path such as
   `/tapdb`
2. expose the canonical DAG endpoints at root `/api/dag/*`
3. advertise those endpoints through the host discovery surface

The Dewey implementation is the current reference adopter.
