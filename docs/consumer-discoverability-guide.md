# Consumer Discoverability Guide

This guide is for a service that publishes or consumes TapDB DAG v2. It uses
only EUIDs returned by an owning service or persisted by TapDB. Never replace
`<persisted-euid>` with a made-up Meridian-looking value.

## Contract at a glance

A participating service mounts `tapdb.dag_v2`, authenticates every route, and
publishes an advertisement only after all v2 routes are registered. Its
immutable `service_id` must exactly match the ID in the fleet registry.

| Route | Meaning |
|---|---|
| `GET /api/dag/manifest` | Authenticated capability and bound declaration |
| `GET /api/dag/v2/search` | Bounded discovery; never ownership proof |
| `GET /api/dag/v2/object/{euid}` | Exact local ownership proof and detail |
| `GET /api/dag/v2/data` | Bounded native lineage graph |

There is no DAG v1 fallback. A v2 endpoint never performs outbound v2 fetches.
The consumer follows a projected typed external reference and authenticates to
the target service directly.

For an existing legacy integration only, `create_tapdb_dag_router(...)`
requires a callable `auth_dependency`; omission or a non-callable value fails
router construction. That host dependency must reject anonymous requests with
`401` or `403`. Its outbound proxy remains separately disabled unless an exact
`V1ProxyPolicy` is supplied.

## Choose durable identity or transient coordination

Use `InstanceFactory.claim_instance_by_identity(...)` when repeated delivery of
the same stable business fact must resolve to one persisted TapDB object. The
identity key is exact caller-supplied data: every replay returns `EXISTING` with
the stored winner, even if the caller supplies different content. TapDB does
not compare consumer payload fingerprints. A race-safe Dewey or other consumer
first claims or reads the committed stored winner, then compares its own
fingerprint and returns its own divergent-payload `409` without creating a
second receipt. Do not derive or backfill identity keys for historical rows,
and do not use a lock key as a durable identifier.

Use `acquire_transaction_advisory_lock(...)` only when several writers need to
serialize a short operation that cannot be expressed as a unique database
claim. Pass a stable namespace and framed key parts inside an already-active
PostgreSQL transaction, choose a bounded timeout for request paths, and let the
transaction release the lock. Receipts expose only a SHA-256 fingerprint, never
the raw lock inputs. Natural identity and advisory locking solve different
problems; adding one is not a reason to add the other.

## Publish a service

Mount once during application startup and treat any failed result as a startup
or readiness failure:

```python
from fastapi import FastAPI, Request

from daylily_tapdb.web import DagV2Limits, mount_tapdb_dag_surfaces


async def require_service_or_user(request: Request) -> dict[str, str]:
    # Replace this body with the host's real session or bearer-token validator.
    # It must raise 401/403 on failure and return an authenticated identity.
    return await host_authenticator(request)


app = FastAPI()
mount = mount_tapdb_dag_surfaces(
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
if not mount.mounted:
    raise RuntimeError(f"DAG v2 mount failed: {mount.reason}: {mount.diagnostic}")
```

The advertisement is available as
`app.state.tapdb_dag_v2_advertisement` only after a successful atomic mount.
Do not construct a second advertisement by hand.

Stable ineligibility reasons are `missing_config`, `invalid_config`,
`service_identity_mismatch`, `auth_required`, `invalid_limits`,
`mount_unavailable`, `version_mismatch`, and `missing_manifest`.

## Discover, then prove ownership

Set values from the actual service registry, auth system, and an owning TapDB
response. `PERSISTED_EUID` can also be copied from the output of
`examples/readme/20_python_api.py` after that example commits its object.

```bash
export TAPDB_DAG_BASE_URL="https://catalog.example"
export TAPDB_DAG_TOKEN="<service-or-user-token>"
export TAPDB_DAG_SERVICE_ID="catalog-api"
export PERSISTED_EUID="<persisted-euid>"
```

Fetch and validate the manifest before admission:

```bash
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer ${TAPDB_DAG_TOKEN}" \
  "${TAPDB_DAG_BASE_URL}/api/dag/manifest"
```

The response must declare `extension: tapdb.dag_v2`, `contract: dag:v2`, the
exact registered `service_id`, positive limits, all three endpoint kinds, and
`eligible: true`. Aliases, normalization, and a v1 manifest are mismatches.

Search one bounded page:

```bash
curl --fail-with-body --silent --show-error --get \
  -H "Authorization: Bearer ${TAPDB_DAG_TOKEN}" \
  --data-urlencode "q=webhook" \
  --data-urlencode "limit=25" \
  "${TAPDB_DAG_BASE_URL}/api/dag/v2/search"
```

Use the returned opaque `page.next_cursor` unchanged for the next page. Do not
derive offsets, scan beyond the service limit, or infer ownership from a search
hit. Prove ownership with exact lookup:

```bash
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer ${TAPDB_DAG_TOKEN}" \
  "${TAPDB_DAG_BASE_URL}/api/dag/v2/object/${PERSISTED_EUID}"
```

`404 object_not_owned` is a terminal negative ownership answer from that
service. A registry or aggregator may query another eligible service, but it
must not reinterpret the 404 or route through DAG v1.

Fetch bounded local lineage:

```bash
curl --fail-with-body --silent --show-error --get \
  -H "Authorization: Bearer ${TAPDB_DAG_TOKEN}" \
  --data-urlencode "start_euid=${PERSISTED_EUID}" \
  --data-urlencode "depth=2" \
  --data-urlencode "max_nodes=100" \
  "${TAPDB_DAG_BASE_URL}/api/dag/v2/data"
```

Cache or render a graph only with its `graph_revision` and `snapshot_at`.
Respect `truncated`, `truncation_reason`, `effective_limits`, node presentation
hints, and typed edge semantics. A later revision is a new snapshot, not an
in-place extension of the earlier payload.

## Create an outbound typed reference

The source EUID below must resolve to a locally persisted object. The foreign
EUID must come from an exact lookup response by the target owning service.
TapDB persists a typed reference object and connects it to the source with
`generic_instance_lineage`; the reference object's EUID and lineage EUID are
minted by TapDB.

```python
from datetime import datetime, timezone

from daylily_tapdb.services import (
    TypedExternalReferenceSpec,
    create_or_reuse_typed_external_reference,
    find_object_by_euid,
)


with connection.session_scope(commit=True) as session:
    source, record_type = find_object_by_euid(session, LOCAL_PERSISTED_EUID)
    if source is None or record_type != "instance":
        raise LookupError("source object is not locally persisted")

    result = create_or_reuse_typed_external_reference(
        session,
        source=source,
        instance_factory=factory,
        spec=TypedExternalReferenceSpec(
            target_service_id=TARGET_SERVICE_ID,
            target_object_euid=FOREIGN_PERSISTED_EUID,
            relationship_type="references",
            asserted_at=datetime.now(timezone.utc),
            assertion_provenance="authenticated exact ownership lookup",
        ),
    )
    external_reference_euid = result.reference.euid
    lineage_euid = result.lineage.euid
```

Because the reserved reference identity is global, configure the runtime
principal with explicit `target.allow_global_claims: true` and construct the
connection with the matching `allow_global_rows=True` assertion. The database
accepts that assertion only when it matches the operator-owned immutable
principal binding; the helper cannot widen transaction scope or switch owner or
tenant on its caller's behalf. An operator must first seed the exact installed
TapDB core definitions into the configured client-owner scope, so the runtime
creates the XRF and lineage with the same owner as its source object.

The v2 projection includes the target service and target object identifiers,
the persisted external-reference EUID, the lineage EUID, assertion time, and
provenance. It does not convert copied JSON fields or an
`external_payload.tapdb_graph` blob into an edge.

For ordinary local lineage, an explicit stored `assertion_provenance` is
projected unchanged when present. Otherwise the authoritative provenance is the
persisted lineage record itself, rendered as `tapdb.lineage:<persisted-euid>`;
this never synthesizes a relationship from object metadata.

## Inspect operational readiness

Use the same explicit config that the host uses at runtime. The shared info
payload is sanitized and is also used by the API, embedded GUI, and legacy
admin surface:

```bash
tapdb --config /abs/path/to/tapdb-config.yaml --json info
tapdb --config /abs/path/to/tapdb-config.yaml templates inventory \
  --repository-pack /abs/path/to/repository-template-pack.json
```

The first command reports package/Python/Meridian versions, exact config
identity, database reachability, scope, storage, UI, Git, and DAG status without
passwords or tokens. Template inventory compares the database with one explicit
canonical pack and reports each item as `pending`, `backed-up`, or `failed`.
Treat `failed`, a config mismatch, an unreachable database, a dirty unexpected
checkout, or an ineligible DAG mount as a readiness failure; do not search for a
different config or template pack.

Use `tapdb objects search` only for bounded discovery, preserve its opaque
cursor unchanged, then confirm a chosen object with `tapdb objects get` or the
authenticated exact DAG endpoint. Updates, repairs, imports, and deletion are
dry-run or review operations unless their explicit apply switch is supplied.

## Upgrade without changing identity

Before applying migrations, take the appropriate verified backup and capture a
preflight receipt from the exact target:

```bash
tapdb --config /abs/path/to/tapdb-config.yaml db schema migrate \
  --dry-run --receipt /abs/path/to/preflight.json
tapdb --config /abs/path/to/tapdb-config.yaml db schema migrate \
  --apply --preflight-receipt /abs/path/to/preflight.json \
  --receipt /abs/path/to/result.json
```

An upgrade never regenerates or normalizes an existing UID, EUID, EUID sequence
assignment, machine/message/event/receipt UUID, identity key, scope, template
reference, lineage endpoint, or creation timestamp. Existing rows receive no
inferred natural identity. The apply step refuses a changed target, schema,
pending-migration set, row fingerprint, or sequence state; a failed or no-op
migration leaves both rows and generator state unchanged. Preserve both
receipts with the release evidence.

## Presentation metadata

An object may provide `properties.graph_presentation` for display only. TapDB
validates its role, collapse behavior, expected fan-out relationship types,
maximum degree, and reason. Presentation metadata cannot establish lineage,
change ownership, relax tenant scope, or authorize traversal.

## Explicit DAG v1 proxy boundary

Do not mount DAG v1 for a new adopter. If an existing deployment must retain
the legacy outbound proxy, it must supply `V1ProxyPolicy` explicitly with exact
DNS host names, a timeout no greater than ten seconds, and a response limit no
greater than five MiB. The proxy accepts HTTPS only, resolves every target to
public addresses, rejects redirects, requires JSON, and forwards no cookie or
authorization header. DAG v2 never invokes it.

## Anti-patterns

- Do not invent, hash, or format a placeholder as a Meridian EUID.
- Do not link `/tapdb/object/...` unless that object was confirmed to exist in
  that TapDB service.
- Do not store durable relationships in `object_euid`,
  `target_object_euid`, another `*_object_euid` property, or an ad hoc graph
  blob.
- Do not treat search as ownership proof or paginate by offset.
- Do not normalize, alias, or guess a `service_id`.
- Do not advertise eligibility before the atomic mount succeeds.
- Do not add v1 fallback, compatibility discovery, or service-side routing.
- Do not use a PostgreSQL `SUPERUSER` or `BYPASSRLS` role for runtime access.

## Troubleshooting

| Symptom | Meaning and response |
|---|---|
| Manifest 401/403 | Supply valid host auth; never make the route public. |
| `missing_manifest` | The service is not mounted or is not advertising v2; fix the host rather than probing alternate paths. |
| `service_identity_mismatch` | Fix registry or mount configuration; do not alias either ID. |
| `version_mismatch` | Upgrade the service to the exact v2 contract; do not fall back to v1. |
| `mount_unavailable` | Read the host's local mount diagnostic and repair its explicit config/readiness failure. |
| `invalid_limits` | Configure positive limits inside the published hard bounds. |
| Two exact lookups return 200 | Report `ambiguous_owner`; do not choose by search rank or display name. |
| Search returns an item but exact lookup is 404 | The search candidate is not ownership proof; discard it for this service. |
| `invalid_local_graph_contract` | Replace untyped metadata with a persisted typed reference plus lineage, or repair invalid graph/presentation data. |
| Cursor is rejected | Restart bounded discovery from the first page; never decode, alter, or convert the opaque cursor to an offset. |
| Response is truncated | Continue from a service-defined discovery cursor where applicable, or request a smaller bounded graph; never infer omitted nodes. |
| Runtime role rejected | Use a dedicated `NOSUPERUSER NOBYPASSRLS` role. Use the explicit operator connection role only for schema/bootstrap work. |

## Adopter checklist

Use this same checklist for Atlas, Bloom, Ursa, Dewey, Zebra Day, or another
prospective contributor; the names do not change the contract. The initial
plan keeps Kahlo hub-only, explicitly excludes OWY as an eligible TapDB web
contributor, and requires the Zebra Day registry authority to select one exact
service ID before any manifest or typed reference is wired. TapDB supplies no
`zebra-day`/`zebra_day` alias.

- Register one immutable `service_id` and use it byte-for-byte at mount time.
- Supply one existing absolute TapDB config with exact schema/domain/owner
  identity.
- Require host session or service-token auth on every manifest and v2 route.
- Choose positive depth, node, and page-size limits.
- Fail startup/readiness if the atomic mount is not eligible.
- Advertise only `tapdb.dag_v2`; do not imply outbound fetch.
- Prove keyset discovery, exact ownership lookup, 404 behavior, graph bounds,
  revision/snapshot/truncation, and typed outbound refs in integration tests.
- Persist external references through the reserved template and lineage.
- Run runtime database access under a forced-RLS-safe role with complete
  transaction context and actor attribution.

## Related guides

- [DAG contract](dag_spec.md)
- [Identity and scoping](identity-and-scoping.md)
- [Template authoring and repository packs](template-authoring.md)
- [Runtime, CLI, and migration commands](runtime-and-cli.md)
- [Embedding the API and GUI](integration-and-embedding.md)
- [Backup and recovery](backup-and-recovery.md)
