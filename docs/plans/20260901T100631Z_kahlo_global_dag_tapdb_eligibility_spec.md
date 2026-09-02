# TapDB DAG v2 Eligibility and Federation Spec

**Status:** proposal for the TapDB updating agent
**Date:** 2026-09-01
**Scope:** a read-only review of Kahlo's global DAG hub and the Atlas, Bloom,
Ursa, Dewey, OWY, and Zebra Day integrations. This document proposes a
coordinated library and adopter cutover; it does not authorize a deployment,
data migration, or production mutation.

## Outcome

TapDB already has a useful `dag:v1` foundation: a common router, exact object
lookup, native graph payloads, and explicit external-reference expansion. The
next improvement should be a **strict, explicitly configured
`tapdb.dag_v2` eligibility contract**.

Kahlo should discover eligible contributors only by asking the services already
registered in its Dayhoff fleet configuration for an authenticated, self
advertised manifest. It must not scan DNS, enumerate hosts, probe arbitrary
URLs, infer aliases, or turn copied EUID-like strings or S3 artifacts into
graph edges. A service is eligible only when its mounted TapDB router and its
advertisement prove the same contract, identity, and access path.

The initial view should keep Kahlo **hub-only**. Kahlo itself should not become
an eligible graph node merely because it embeds TapDB; that requires a separate
explicit domain-object and ownership decision.

## Gate 0: investigation ledger

| ID | Work item | Terminal state | Evidence / result |
| --- | --- | --- | --- |
| DAG-001 | Inspect the live Kahlo global DAG viewer | BLOCKED | `kahlo.day.lsmc.com` returned `ERR_NAME_NOT_RESOLVED` in both available browsers; local DNS lookup and curl also could not resolve it. This is an investigation-environment reachability limitation, not a claim about production service health. |
| DAG-002 | Inspect Kahlo's current hub implementation | COMPLETE | `kahlo/app/domain/global_graph.py` and configuration template reviewed. The checkout is dirty, so these are source-review findings rather than deployed-state claims. |
| DAG-003 | Inspect TapDB DAG contract and shared library behavior | COMPLETE | `docs/dag_spec.md`, `docs/integration-and-embedding.md`, `daylily_tapdb/web/dag.py`, `services/graph_payloads.py`, `services/object_search.py`, and `services/external_refs.py` reviewed. |
| DAG-004 | Scan named candidate services | COMPLETE | Atlas, Bloom, Ursa, Dewey, OWY, and Zebra Day source/configuration surfaces reviewed. |
| DAG-005 | Produce updating-agent handoff | COMPLETE | This durable spec and acceptance checklist are the handoff. |
| DAG-006 | Run focused existing DAG tests | BLOCKED | TapDB test collection is blocked in this shell by missing `meridian_euid`; Kahlo test collection is blocked by missing `daylily_tapdb`. No dependency installation or environment mutation was performed for this read-only review. |

All ledger rows are terminal. The source and contract objective is complete;
the visual/live-runtime portion remains unverified until the Kahlo hostname is
reachable from an authorized environment.

The new specification passed `git diff --check`. The two focused Python test
commands did not collect, so they are not evidence of a passing or failing DAG
implementation:

- TapDB: `python -m pytest -q tests/test_web_dag.py tests/test_external_graph_helpers.py tests/test_services_external_refs.py tests/test_services_graph_payloads.py tests/test_lsmc_v0_graph_contract.py`
- Kahlo: `python -m pytest -q tests/test_global_graph.py`

## Evidence that drives the proposal

| Area | Observed behavior | Why it matters |
| --- | --- | --- |
| TapDB v1 | The shared router advertises `tapdb.dag_v1` with exact lookup, native graph, and search endpoints. Its documented federation mechanism is typed external references plus lineage. | Preserve this ownership and relationship model; do not replace it with an untyped service graph. |
| Kahlo ownership resolution | Kahlo's `_resolve_euid_owner()` uses cross-service search results to decide who owns a root. | Search is discovery, not proof of ownership. The core contract already defines exact lookup as the ownership endpoint. |
| Kahlo node semantics | The hub contains a large role-to-relationship map and parses fanout/collapse data from generic JSON properties. | Presentation semantics need a shared, validated TapDB payload rather than each hub embedding domain-specific role knowledge. |
| Kahlo federation | It follows explicit `external_refs`, which is the right boundary, but currently matches service identifiers by lower-cased service ID or display name. | A federation edge needs a single exact service identity, not display-name or punctuation matching. |
| Atlas | The application mounts the DAG router in a catch-all `try` block, while its observability snapshot independently advertises the DAG capability. | A router mount failure can be advertised as an eligible DAG contributor. Mount success and advertisement must be atomic. |
| Bloom and Ursa | Both adopt the router, but maintain local helpers that write `external_payload.tapdb_graph` and graph-presentation metadata. | Repeated custom projection code is evidence that the library needs a supported typed external-reference and presentation builder. |
| Dewey | It mounts the shared router but also reimplements a search route and derives graph hints from explicit external targets. | Shared search/pagination and safe external-reference projection should live in TapDB, while the host remains the owner of explicitly configured target credentials. |
| Zebra Day | Its router identifies as `zebra-day`, while Kahlo's checked-in template uses `zebra_day`. | This is a real source-level identity mismatch risk. The runtime registry must supply one exact canonical ID; no hyphen/underscore alias fallback. |
| OWY | OWY is a CLI/S3 artifact producer, not a TapDB web service or DAG-router adopter. Its own documentation says Kahlo should consume declared downstream state rather than infer it from markers or prefixes. | Do not make OWY a discoverable DAG node. Model the relevant transfer/run entities in the owning service (for example Dewey, Bloom, or Ursa) using typed objects and lineage. |

## Required TapDB library contract

### 1. Atomic DAG surface and eligibility manifest

Add one supported helper, conceptually named
`mount_tapdb_dag_surfaces(...)`. A host uses this single helper rather than
mounting a router in one place and assembling an observability advertisement in
another.

The helper must:

1. Require an explicit absolute TapDB configuration path and an explicit
   `service_id` supplied by the host's Dayhoff registration.
2. Mount the v2 router and manifest endpoint successfully before returning an
   advertised capability fragment.
3. Fail closed: if configuration, identity, route mounting, or conformance
   setup fails, it returns no DAG eligibility advertisement and a local,
   actionable diagnostic.
4. Never perform outbound service discovery and never accept a base URL or
   credential embedded in object metadata as routing authority.

Expose an authenticated manifest at a stable path such as
`GET /api/dag/manifest`. The exact path is less important than making it part
of the core contract. A representative response is:

```json
{
  "extension": "tapdb.dag_v2",
  "contract": "dag:v2",
  "service_id": "<exact Dayhoff registered service ID>",
  "display_name": "<operator-facing name>",
  "eligible": true,
  "endpoints": [
    {"kind": "dag_exact_lookup", "path": "/api/dag/object/{euid}"},
    {"kind": "dag_native_graph", "path": "/api/dag/data"},
    {"kind": "dag_object_search", "path": "/api/dag/search"}
  ],
  "features": {
    "typed_external_references": true,
    "typed_graph_presentation": true,
    "snapshot_metadata": true
  },
  "limits": {"max_depth": 0, "max_nodes": 0},
  "manifest_revision": "<opaque revision>"
}
```

The manifest contains no credential, token, arbitrary remote base URL, or
authorization bypass. Kahlo obtains the service URL and request credentials
only from its existing explicit fleet registration.

### 2. Exact canonical service identity

Make `service_id` a required, immutable contract field for a DAG surface and a
typed external reference. Its value must exactly equal the Dayhoff fleet
registration used by Kahlo. Display name is presentation only.

There is deliberately no automatic normalization or compatibility alias for
`zebra-day` versus `zebra_day`. The owning service registration must choose one
canonical value, then Kahlo configuration and all writers use that precise
value. A mismatch makes the contributor ineligible with an explicit
`service_identity_mismatch` reason.

### 3. Typed external graph references, backed by lineage

Provide a core factory and projector for a federable external reference. It
must create or validate:

- a typed external-reference object;
- the `generic_instance_lineage` relation from the local object to that typed
  external-reference object; and
- the external target descriptor: `target_service_id`, target object EUID,
  relationship type, optional tenant, target object kind, assertion time, and
  assertion provenance.

The public graph payload projects that persisted relationship into a stable
`external_refs` entry. It may expose the local external-reference and lineage
identifiers for auditability, but it does not expose a remote base URL,
auth-mode selection, or caller-controlled endpoint path. `system` can remain a
human label only; `target_service_id` is the routing key.

`tapdb.dag_v2` does not federate raw `external_payload.tapdb_graph` blobs,
copied target EUID fields, or S3/marker-derived hints. Existing writers must be
deliberately changed to use the core factory before they participate in the v2
view; do not add a silent legacy projection or automatic migration path.

### 4. Typed graph presentation and snapshot metadata

Promote the currently ad-hoc graph presentation values into a validated core
payload. Node presentation should support a role, collapse preference, and an
optional expected-fanout declaration with relationship types, maximum degree,
and human reason. Edge presentation should provide relationship semantics,
assertion/evidence provenance, and normalized timestamps from the existing
lineage contract.

The graph response metadata must include:

- `graph_revision` and `snapshot_at`;
- `truncated` and a precise `truncation_reason` when limits cut the graph; and
- effective depth/node limits.

Kahlo then renders these generic semantics without maintaining a service-role
map. TapDB validates shape and provenance, while each domain service remains
responsible for choosing its own semantics.

### 5. Ownership, search, and scale guarantees

The v2 exact object endpoint is the only owner proof:

- an owning service returns `200` with a locally owned object;
- a non-owner returns `404`;
- an unauthorized response is not interpreted as absence; and
- two `200` responses are an explicit `ambiguous_owner` error.

Kahlo resolves a root by parallel exact lookup across the already eligible,
explicitly configured contributors. It must not use search results to establish
ownership.

Keep search as a bounded discovery tool. The shared implementation needs
database-filtered queries and an opaque, deterministic cursor; it must not load
all non-deleted rows of every model into memory before filtering. Graph data
must include each included node's outbound typed external references so a hub
does not need one object-detail request per node merely to discover federation
edges.

## Hub behavior required in Kahlo

1. Enumerate only existing fleet registrations; no DNS, subnet, URL, or
   application-name scanning.
2. Fetch each registration's manifest with that registration's configured
   endpoint and credentials.
3. Admit a contributor only if the registration ID equals manifest
   `service_id`, the v2 contract is present, all required endpoints are
   advertised, and the core conformance/readiness checks pass.
4. Surface ineligible registrations with a diagnostic state (for example
   `missing_manifest`, `identity_mismatch`, `mount_unavailable`,
   `version_mismatch`, or `auth_failed`) rather than silently hiding or
   guessing.
5. Resolve a root through parallel exact lookup and stop on no unique owner.
6. Follow only typed, lineage-backed external references whose
   `target_service_id` names another eligible registration.
7. Render generic presentation and snapshot metadata from TapDB; remove
   Kahlo's hard-coded role-to-relationship expectation map.

No retry, legacy v1 inference, display-name routing, or inferred-default path
is part of the v2 global graph. This is a coordinated cutover for the v2 view;
v1 can remain independently available only where explicitly configured outside
that view.

## Service adoption matrix

| Service | Required change | Eligibility after cutover |
| --- | --- | --- |
| Kahlo | Consume manifests from its registered fleet targets; exact-owner probing; generic presentation rendering; retain hub-only role initially. | Hub, not an automatic contributor. |
| Atlas | Replace independent observability advertisement plus catch-all router mount with atomic core helper; replace custom graph-reference projection with typed core factory. | Eligible only after atomic manifest reports ready. |
| Bloom | Use the core external-reference factory for Atlas/Dewey links and the typed presentation payload; retain domain graph endpoints only for domain-specific UI. | Eligible after writer conversion and manifest conformance. |
| Ursa | Replace local explicit-reference/fanout payload builders with core typed builders; preserve Ursa's domain choices as data values. | Eligible after writer conversion and manifest conformance. |
| Dewey | Use shared search unchanged rather than overriding it; pass its explicitly configured target descriptors to the core factory without giving TapDB discovery authority. | Eligible after manifest conformance. |
| Zebra Day | Set one registry-approved `service_id` consistently in service configuration, manifest, external-reference writers, and Kahlo fleet configuration. | Eligible only on exact identity match. |
| OWY | No TapDB DAG router or manifest. Publish only its normal declared artifacts/events; owning services create typed TapDB objects and lineage for any graph-visible transfer/run relationship. | Not an eligible contributor. |

## Acceptance tests for the updating agent

1. A host with an invalid or missing absolute TapDB config cannot emit a DAG
   eligibility manifest or capability advertisement.
2. A successfully mounted host reports its exact configured `service_id`; a
   different fleet ID is rejected by Kahlo with a visible diagnostic.
3. The router's exact-owner endpoint returns `200` only for local ownership and
   `404` for a non-owned object. A hub test proves that search cannot select an
   owner.
4. The external-reference factory creates a typed external-reference object and
   `generic_instance_lineage`; the graph payload exposes its routing key and
   provenance.
5. A raw metadata blob or copied target EUID without that typed lineage is
   non-federable in v2 and produces a clear writer-side validation failure.
6. Graph payload node presentation, edge semantics, snapshot revision, and
   truncation metadata validate against the shared schema.
7. Search pagination is deterministic, bounded, and database-filtered; a test
   fails if the generic service scans all rows before applying filters.
8. A graph payload with multiple nodes needs no per-node detail calls to obtain
   outbound typed external references.
9. The core and Kahlo tests prove that no object-supplied URL, auth mode, or
   arbitrary host is ever contacted as part of federation.
10. Integration fixtures cover a valid multi-service path across the named
    adopters and verify no unresolved alias remains for the selected Zebra Day
    identity.

## Suggested delivery order

1. Implement the v2 manifest, atomic mounting helper, typed external-reference
   factory/projector, presentation schema, and cursor/snapshot changes in
   TapDB with the tests above.
2. Convert Atlas, Bloom, Ursa, Dewey, and Zebra Day in isolated branches,
   choosing the registry-authoritative Zebra Day identity before wiring refs.
3. Update Kahlo to admit only v2 manifests and replace search-based owner
   resolution and hard-coded fanout expectations.
4. In an authorized environment that can reach Kahlo and the registered
   services, exercise exact-owner, cross-service expansion, visibility of
   ineligible diagnostics, and graph truncation behavior.

The stop point for this handoff is the written contract and tests. It does not
include a TapDB release, a Kahlo deployment, a registry rewrite, or changes to
OWY's live transfer behavior.
