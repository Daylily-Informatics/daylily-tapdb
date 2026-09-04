# Consumer Discoverability Guide

This guide explains how a TapDB 10 service becomes discoverable and how a
fleet consumer performs trustworthy search and DAG traversal. The companion
[external-reference and federation guide](external-references-and-federation.md)
covers the full XRF lifecycle and tagged-service migration map.

## Contract in one sentence

A producer publishes authenticated `tapdb.dag_v2` / `dag:v2` routes from
persisted objects and `generic_instance_lineage`; a consumer searches under
bounds, verifies ownership by exact lookup, and follows only typed external
references through an exact fleet registry.

There is no DAG v1 fallback, URL proxy, alias resolution, metadata-derived
edge, or service-side discovery.

## Producer adoption

Mount the surface atomically:

~~~python
from daylily_tapdb.web import DagV2Limits, mount_tapdb_dag_surfaces

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
    raise RuntimeError(f"{result.reason}: {result.diagnostic}")
~~~

Required inputs are one existing absolute config, one exact immutable service
ID, one callable auth dependency, and positive limits. Failed mounting leaves
no partial routes and no advertisement.

The authenticated routes are:

| Purpose | Route |
|---|---|
| Capability and identity | `GET /api/dag/manifest` |
| Exact ownership proof | `GET /api/dag/v2/object/{euid}` |
| Bounded lineage graph | `GET /api/dag/v2/data` |
| Bounded discovery | `GET /api/dag/v2/search` |

The manifest must advertise exact endpoint paths, limits, a stable
`manifest_revision`, and these features:

- `typed_external_references`
- `typed_external_identifiers`
- `external_reference_search`
- `typed_graph_presentation`
- `snapshot_metadata`
- `outbound_fetch: false`

The service ID in the manifest must equal its fleet registration. Treat a
mismatch as a different service, not an alias.

## Request flow

1. Fetch and validate `/api/dag/manifest`.
2. Search one bounded page.
3. Preserve the returned opaque cursor unchanged when requesting the next page.
4. Treat results as candidates.
5. Call exact `/api/dag/v2/object/{euid}` to prove ownership.
6. Fetch `/api/dag/v2/data?start_euid=<persisted-euid>`.
7. Inspect `graph_revision`, `snapshot_at`, `truncated`,
   `truncation_reason`, and effective limits.
8. Follow only items in `external_refs` whose exact target service is admitted.
9. Display `external_identifiers` as non-expandable identifiers.
10. Preserve every failure and unresolved boundary in the user-visible result.

Exact lookup returns `404 object_not_owned` when that service does not own the
EUID. It is normal evidence, not permission to try an alias or older endpoint.

## Search

Ordinary search accepts exact record filters and bounded free text. External
search adds one of two mutually exclusive groups:

- `external_service_id` and `external_object_euid`; or
- `external_namespace`, `external_kind`, and `external_value`.

`external_relationship_type` may narrow either group. External filters require
`record_type=instance`. Incomplete groups, mixed groups, and other record types
return `422`.

Example:

~~~bash
curl -fsS -G "https://service.example/api/dag/v2/search" \
  -H "Authorization: Bearer <service-token>" \
  --data-urlencode "record_type=instance" \
  --data-urlencode "external_service_id=atlas" \
  --data-urlencode "external_object_euid=<persisted-euid>" \
  --data-urlencode "limit=100"
~~~

Free-text search is SQL-bounded and does not scan arbitrary JSON. Search never
proves ownership.

## Graph output

Every local graph node has an exact persisted EUID and service ID. Every local
edge is a persisted lineage row. Public properties omit base URLs, route
templates, auth modes, credentials, and URL-bearing routing metadata.

Source nodes make external discovery prominent:

- `external_refs` carries exact service/EUID targets plus relationship,
  authority, assertion timestamp/provenance, XRF EUID, and lineage EUID.
- `external_identifiers` carries namespace/kind/value/scope plus the same
  assertion evidence and an optional canonical URI.

Opaque identifiers are visible but never graph-expandable. A canonical URI is
display metadata, not an endpoint-discovery mechanism.

## Fleet composition

TapDB supplies a transport-neutral client; the application supplies exact
origins and authenticated I/O:

~~~python
from daylily_tapdb.federation import (
    DagServiceTarget,
    DagV2FederationClient,
    FederatedObjectKey,
    FederationLimits,
)

async def compose(authenticated_transport, query_text, candidate_euid):
    client = DagV2FederationClient(
        [
            DagServiceTarget("atlas", "https://atlas.day.lsmc.bio"),
            DagServiceTarget("bloom", "https://bloom.day.lsmc.bio"),
            DagServiceTarget("ursa", "https://ursa.day.lsmc.bio"),
            DagServiceTarget("dewey", "https://dewey.day.lsmc.bio"),
        ],
        authenticated_transport,
        limits=FederationLimits(max_services=4, concurrency=4),
    )

    results = await client.search(filters={"q": query_text})
    owner = await client.resolve_owner(candidate_euid)
    graph = await client.graph(
        FederatedObjectKey(owner.service_id, owner.euid),
        local_depth=4,
    )
    return results, graph
~~~

The client validates manifests before operations and before every external
jump. Global IDs use `service_id::euid`. Synthetic bridge edges retain source
and target identity, relationship, XRF EUID, lineage EUID, assertion authority,
time, and provenance.

Default global ceilings are 32 admitted services, concurrency 8, 32 external
jumps, 5,000 nodes, search limit 100, and a 30-second deadline. Callers may
lower them.

### Partial results

- Starting-service graph failure is fatal.
- All-service search failure is fatal.
- Search may be partial only when at least one service succeeds and every
  failure appears in receipts and warnings.
- Owner resolution requires all exact lookups to succeed and exactly one owner.
- A failed remote graph branch becomes an explicit unresolved boundary node.
- Bounds set `truncated` and `truncation_reason`.

Never hide these conditions in a visualization.

## Canonical external links

Create graph-expandable `TapDBObjectTarget` values and non-expandable
`ExternalIdentifierTarget` values through `ExternalReferenceService`. The
service handles identity, replay, detach, reactivation, authority-scoped
reconciliation, list, and exact reverse lookup. It never owns the outer
transaction.

Do not use removed legacy writer types, generic factory creation, raw XRF
mutation, copied EUID fields, or URL-bearing graph payloads. Those paths are not
TapDB 10 contracts.

## Canonical GUI and visualizers

`daylily_tapdb.gui` uses the same DAG-v2 builders as the API and is the only
TapDB web stack. Its graph explorer keeps the useful former admin features:
fuzzy and exact find, type/subtype and distance/degree filtering, multiple
layouts, neighborhood and direction waves, administrator lineage mutation,
detail inspection, JSON export, and Mermaid export.

Kahlo remains the global visualization layer. Its later migration should feed
the neutral `DagV2FederationClient` payload into its UI and keep credentials,
fleet admission, and visualization policy in Kahlo. Atlas `7.0.2`, Bloom
`8.0.0`, Ursa `11.0.50`, Dewey `7.0.2`, and Kahlo `7.0.7` were immutable design
references; this TapDB release does not modify those repositories.

## Identity and scoping

Runtime database roles must be `NOSUPERUSER NOBYPASSRLS`. Every transaction
must install exact schema, config identity, domain, owner, tenant, actor, and
global-row policy.

Use `IdentityScope.GLOBAL` or `IdentityScope.TENANT` explicitly with
`claim_instance_by_identity`. Tenant scope requires a tenant UUID. Never replace
a persisted UID, EUID, identity key, or sequence value during migration.

## Operational prerequisites

Before advertising a service:

~~~bash
tapdb --config <path> --json info
tapdb --config <path> --json templates inventory
tapdb --config <path> db migration preflight
tapdb --config <path> --json validation external-references --sample-limit 25
~~~

Use the exact migration preflight receipt with apply via `--preflight-receipt`.
Take and verify a backup before applying a production migration. See
[identity and scoping](identity-and-scoping.md) and
[backup and recovery](backup-and-recovery.md).

## Adoption checklist

A producer is ready when:

- its stable service ID is registered exactly;
- the manifest is authenticated and validates;
- exact lookup returns owned objects and `object_not_owned` otherwise;
- graph traversal is lineage-only and bounded;
- external targets use canonical XRF lineage;
- external identifiers have an explicit safe scope;
- runtime role, RLS, actor attribution, and schema `CREATE` revocation pass;
- the external-reference audit returns `ok: true`;
- tests cover search, exact lookup, truncation, RLS, and external projections.

A fleet consumer is ready when:

- targets are an explicit allowlisted inventory;
- credentials stay inside its transport;
- owner resolution uses exact lookup;
- global IDs are namespaced;
- unresolved and partial states remain visible;
- no alias, retry, alternate endpoint, or v1 fallback exists;
- bounds are equal to or lower than TapDB ceilings.

## Anti-patterns

Do not:

- infer an owner from search rank;
- infer an edge from metadata;
- fetch a URL stored in an XRF;
- copy an EUID into metadata and call it lineage;
- mint a Meridian-looking placeholder;
- silently omit a failed service;
- treat `truncated` as complete;
- forward one service's credential to another;
- revive DAG v1 or the old proxy;
- use compatibility fallbacks.

Missing or malformed state fails clearly. Fix the producer or fleet
configuration instead of guessing.
