from __future__ import annotations

from copy import deepcopy

import pytest

import daylily_tapdb.federation as federation
from daylily_tapdb.federation import (
    DagServiceTarget,
    DagV2FederationClient,
    FederatedObjectKey,
    FederationContractError,
    FederationLimits,
    FederationOperationError,
)
from daylily_tapdb.web.dag_v2 import DagV2Limits, _manifest_for


def _manifest(service_id: str) -> dict:
    return _manifest_for(
        service_id=service_id,
        display_name=service_id,
        limits=DagV2Limits(max_depth=8, max_nodes=1_000, max_search_page_size=100),
    ).to_dict()


def _node(service_id: str, euid: str, *, refs=(), identifiers=()):
    return {
        "data": {
            "id": euid,
            "euid": euid,
            "service_id": service_id,
            "record_type": "instance",
            "display_label": euid,
            "external_refs": list(refs),
            "external_identifiers": list(identifiers),
        }
    }


def _graph(service_id: str, nodes, edges=()):
    return {
        "elements": {"nodes": list(nodes), "edges": list(edges)},
        "meta": {"contract": "dag:v2", "service_id": service_id},
    }


def _ref(service_id: str, euid: str, lineage: str):
    return {
        "target_service_id": service_id,
        "target_object_euid": euid,
        "relationship_type": "references",
        "external_reference_euid": f"reference-{lineage}",
        "lineage_euid": lineage,
        "assertion_authority": "unit-test",
        "asserted_at": "2026-09-04T00:00:00+00:00",
        "assertion_provenance": "unit-test-fixture",
    }


class FakeTransport:
    def __init__(self):
        self.manifests = {}
        self.searches = {}
        self.objects = {}
        self.graphs = {}
        self.fail = set()
        self.calls = []

    async def manifest(self, target):
        self.calls.append(("manifest", target.service_id))
        if ("manifest", target.service_id) in self.fail:
            raise ConnectionError("manifest unavailable")
        return deepcopy(self.manifests[target.service_id])

    async def search(self, target, *, params):
        self.calls.append(("search", target.service_id, dict(params)))
        if ("search", target.service_id) in self.fail:
            raise ConnectionError("search unavailable")
        return deepcopy(self.searches[target.service_id])

    async def object(self, target, *, euid):
        self.calls.append(("object", target.service_id, euid))
        if ("object", target.service_id) in self.fail:
            raise ConnectionError("lookup unavailable")
        return deepcopy(self.objects.get((target.service_id, euid)))

    async def graph(self, target, *, euid, depth, max_nodes):
        self.calls.append(("graph", target.service_id, euid, depth, max_nodes))
        if ("graph", target.service_id) in self.fail:
            raise ConnectionError("graph unavailable")
        return deepcopy(self.graphs[(target.service_id, euid)])


@pytest.fixture(autouse=True)
def _persisted_euids(monkeypatch):
    # These are labels in an in-memory transport fixture, not minted identities.
    monkeypatch.setattr(federation, "validate_euid", lambda value: bool(value))


def _targets(*service_ids: str):
    return [
        DagServiceTarget(service_id, f"https://{service_id}.example")
        for service_id in service_ids
    ]


def _client(*service_ids: str, limits=None):
    transport = FakeTransport()
    for service_id in service_ids:
        transport.manifests[service_id] = _manifest(service_id)
    return DagV2FederationClient(
        _targets(*service_ids), transport, limits=limits
    ), transport


def test_targets_limits_and_transport_are_exact_and_bounded():
    assert DagServiceTarget("atlas", "https://atlas.example").service_id == "atlas"
    for bad in (
        "http://atlas.example",
        "https://user@atlas.example",
        "https://atlas.example/path",
        "https://atlas.example?x=1",
    ):
        with pytest.raises(ValueError, match="credential-free HTTPS"):
            DagServiceTarget("atlas", bad)
    with pytest.raises(ValueError, match="max_services"):
        FederationLimits(max_services=33)
    with pytest.raises(ValueError, match="duplicate service_id"):
        DagV2FederationClient(_targets("atlas", "atlas"), FakeTransport())


@pytest.mark.anyio
async def test_search_is_parallel_deterministic_deduplicated_and_receipted():
    client, transport = _client("atlas", "bloom")
    transport.searches = {
        "atlas": {
            "items": [
                {
                    "service_id": "atlas",
                    "euid": "persisted-atlas-object",
                    "record_type": "instance",
                }
            ],
            "meta": {"contract": "dag:v2", "service_id": "atlas"},
        },
        "bloom": {
            "items": [
                {
                    "service_id": "bloom",
                    "euid": "persisted-bloom-object",
                    "record_type": "instance",
                }
            ],
            "meta": {"contract": "dag:v2", "service_id": "bloom"},
        },
    }

    result = await client.search(filters={"q": "sample"}, limit=25)

    assert [item["global_id"] for item in result["items"]] == [
        "atlas::persisted-atlas-object",
        "bloom::persisted-bloom-object",
    ]
    assert result["meta"]["partial"] is False
    assert {item["status"] for item in result["receipts"]} == {"success"}
    assert all(
        call[2]["limit"] == 25 for call in transport.calls if call[0] == "search"
    )


@pytest.mark.anyio
async def test_search_returns_honest_partial_receipts_and_all_failure_is_fatal():
    client, transport = _client("atlas", "bloom")
    transport.searches["atlas"] = {
        "items": [],
        "meta": {"contract": "dag:v2", "service_id": "atlas"},
    }
    transport.fail.add(("search", "bloom"))
    result = await client.search()
    assert result["meta"]["partial"] is True
    assert result["warnings"][0]["service_id"] == "bloom"

    transport.fail.add(("search", "atlas"))
    with pytest.raises(FederationOperationError, match="every admitted service"):
        await client.search()


@pytest.mark.anyio
async def test_owner_resolution_uses_exact_lookup_and_fails_on_uncertainty():
    client, transport = _client("atlas", "bloom")
    euid = "persisted-owned-object"
    transport.objects[("atlas", euid)] = {
        "service_id": "atlas",
        "euid": euid,
        "external_refs": [],
        "external_identifiers": [],
    }
    assert await client.resolve_owner(euid) == FederatedObjectKey("atlas", euid)
    assert not any(call[0] == "search" for call in transport.calls)

    transport.fail.add(("object", "bloom"))
    with pytest.raises(FederationOperationError, match="every exact lookup"):
        await client.resolve_owner(euid)

    transport.fail.clear()
    transport.objects[("bloom", euid)] = {
        "service_id": "bloom",
        "euid": euid,
        "external_refs": [],
        "external_identifiers": [],
    }
    with pytest.raises(FederationContractError, match="multiple services"):
        await client.resolve_owner(euid)


@pytest.mark.anyio
async def test_graph_namespaces_nodes_bridges_services_and_never_expands_opaque_ids():
    client, transport = _client("atlas", "bloom")
    atlas_euid = "persisted-atlas-source"
    bloom_euid = "persisted-bloom-target"
    transport.graphs[("atlas", atlas_euid)] = _graph(
        "atlas",
        [
            _node(
                "atlas",
                atlas_euid,
                refs=[_ref("bloom", bloom_euid, "persisted-external-lineage")],
                identifiers=[
                    {
                        "namespace": "doi",
                        "kind": "publication",
                        "value": "10.1000/example",
                        "scope": "public_global",
                        "relationship_type": "identifies",
                        "external_reference_euid": "persisted-opaque-reference",
                        "lineage_euid": "persisted-opaque-lineage",
                        "assertion_authority": "unit-test",
                        "asserted_at": "2026-09-04T00:00:00+00:00",
                        "assertion_provenance": "unit-test-fixture",
                    }
                ],
            )
        ],
    )
    transport.graphs[("bloom", bloom_euid)] = _graph(
        "bloom", [_node("bloom", bloom_euid)]
    )

    result = await client.graph(FederatedObjectKey("atlas", atlas_euid))
    ids = {item["data"]["id"] for item in result["elements"]["nodes"]}
    assert f"atlas::{atlas_euid}" in ids
    assert f"bloom::{bloom_euid}" in ids
    opaque = next(item for item in ids if item.startswith("opaque::"))
    assert opaque
    edges = [item["data"] for item in result["elements"]["edges"]]
    bridge = next(item for item in edges if item["edge_kind"] == "external_reference")
    assert bridge["source"] == f"atlas::{atlas_euid}"
    assert bridge["target"] == f"bloom::{bloom_euid}"
    assert len([call for call in transport.calls if call[0] == "graph"]) == 2


@pytest.mark.anyio
async def test_graph_rejects_reserved_fields_in_remote_opaque_projection():
    client, transport = _client("atlas")
    root = "persisted-root"
    transport.graphs[("atlas", root)] = _graph(
        "atlas",
        [
            _node(
                "atlas",
                root,
                identifiers=[
                    {
                        "namespace": "doi",
                        "kind": "publication",
                        "value": "10.1000/example",
                        "scope": "public_global",
                        "relationship_type": "identifies",
                        "external_reference_euid": "persisted-opaque-reference",
                        "lineage_euid": "persisted-opaque-lineage",
                        "assertion_authority": "unit-test",
                        "asserted_at": "2026-09-04T00:00:00+00:00",
                        "assertion_provenance": "unit-test-fixture",
                        "id": "attacker-controlled-node-id",
                    }
                ],
            )
        ],
    )

    with pytest.raises(FederationContractError, match="unsupported field.*id"):
        await client.graph(FederatedObjectKey("atlas", root))


@pytest.mark.anyio
async def test_graph_surfaces_failed_and_unadmitted_branches_as_boundaries():
    client, transport = _client("atlas", "bloom")
    root = "persisted-root"
    transport.graphs[("atlas", root)] = _graph(
        "atlas",
        [
            _node(
                "atlas",
                root,
                refs=[
                    _ref("bloom", "persisted-bloom", "persisted-bloom-lineage"),
                    _ref("ursa", "persisted-ursa", "persisted-ursa-lineage"),
                ],
            )
        ],
    )
    transport.fail.add(("graph", "bloom"))

    result = await client.graph(FederatedObjectKey("atlas", root))

    boundaries = [
        item["data"]
        for item in result["elements"]["nodes"]
        if item["data"]["record_type"] == "unresolved_external_boundary"
    ]
    assert {item["target_service_id"] for item in boundaries} == {"bloom", "ursa"}
    assert {item["service_id"] for item in result["warnings"]} == {"bloom", "ursa"}


@pytest.mark.anyio
async def test_graph_bounds_external_cycles_and_payload_collisions_fail_safely():
    client, transport = _client("atlas", "bloom")
    atlas_euid = "persisted-atlas"
    bloom_euid = "persisted-bloom"
    transport.graphs[("atlas", atlas_euid)] = _graph(
        "atlas",
        [
            _node(
                "atlas",
                atlas_euid,
                refs=[_ref("bloom", bloom_euid, "persisted-a-b")],
            )
        ],
    )
    transport.graphs[("bloom", bloom_euid)] = _graph(
        "bloom",
        [
            _node(
                "bloom",
                bloom_euid,
                refs=[_ref("atlas", atlas_euid, "persisted-b-a")],
            )
        ],
    )
    with pytest.raises(FederationContractError, match="cycle detected"):
        await client.graph(FederatedObjectKey("atlas", atlas_euid))

    bounded, bounded_transport = _client(
        "atlas",
        "bloom",
        limits=FederationLimits(max_external_jumps=1, max_nodes=2),
    )
    bounded_transport.graphs = deepcopy(transport.graphs)
    # Remove the back edge so the outcome tests bounds rather than cycle rejection.
    bounded_transport.graphs[("bloom", bloom_euid)] = _graph(
        "bloom", [_node("bloom", bloom_euid)]
    )
    result = await bounded.graph(FederatedObjectKey("atlas", atlas_euid))
    assert len(result["elements"]["nodes"]) <= 2


def test_manifest_mismatch_and_unknown_filters_fail_closed():
    client, transport = _client("atlas")
    bad = deepcopy(transport.manifests["atlas"])
    bad["service_id"] = "wrong"
    transport.manifests["atlas"] = bad

    with pytest.raises(ValueError, match="unsupported"):
        # Filter validation occurs before any transport call.
        import asyncio

        asyncio.run(client.search(filters={"alias": "atlas"}))


def test_value_limits_fleet_and_transport_validation_are_complete():
    for invalid in (None, "", " atlas"):
        with pytest.raises(ValueError, match="non-empty exact"):
            federation._service_id(invalid)
    with pytest.raises(ValueError, match="alphanumerics"):
        federation._service_id("bad service")
    for invalid in (None, "", " bad", "bad value"):
        with pytest.raises(ValueError, match="persisted Meridian"):
            federation._persisted_euid(invalid)

    for kwargs, field in (
        ({"concurrency": 0}, "concurrency"),
        ({"max_external_jumps": True}, "max_external_jumps"),
        ({"max_nodes": 5_001}, "max_nodes"),
        ({"search_limit": "10"}, "search_limit"),
        ({"deadline_seconds": 0}, "deadline_seconds"),
        ({"deadline_seconds": 31}, "deadline_seconds"),
    ):
        with pytest.raises(ValueError, match=field):
            FederationLimits(**kwargs)

    transport = FakeTransport()
    with pytest.raises(ValueError, match="sequence"):
        DagV2FederationClient("atlas", transport)
    with pytest.raises(ValueError, match="at least one"):
        DagV2FederationClient([], transport)
    with pytest.raises(ValueError, match="target count"):
        DagV2FederationClient(
            _targets("atlas", "bloom"),
            transport,
            limits=FederationLimits(max_services=1),
        )
    with pytest.raises(ValueError, match="only DagServiceTarget"):
        DagV2FederationClient([object()], transport)
    with pytest.raises(ValueError, match="transport"):
        DagV2FederationClient(_targets("atlas"), object())

    client, _ = _client("atlas")
    with pytest.raises(FederationContractError, match="not admitted"):
        client._target("bloom")


@pytest.mark.anyio
async def test_manifest_search_and_owner_failure_contracts():
    client, transport = _client("atlas")
    invalid_manifest = deepcopy(transport.manifests["atlas"])
    invalid_manifest["service_id"] = "wrong"
    transport.manifests["atlas"] = invalid_manifest
    with pytest.raises(FederationContractError, match="manifest rejected"):
        await client._validated_manifest(client.targets["atlas"])

    for payload, message in (
        ({}, "metadata"),
        ({"meta": {"contract": "dag:v2", "service_id": "atlas"}}, "items"),
        (
            {
                "meta": {"contract": "dag:v2", "service_id": "atlas"},
                "items": ["not-an-object"],
            },
            "item must be an object",
        ),
        (
            {
                "meta": {"contract": "dag:v2", "service_id": "atlas"},
                "items": [{"service_id": "wrong", "euid": "persisted-object"}],
            },
            "service_id mismatch",
        ),
    ):
        with pytest.raises(FederationContractError, match=message):
            client._validate_search_payload(payload, service_id="atlas")

    for limit in (0, True, 101):
        with pytest.raises(ValueError, match="search limit"):
            await client.search(limit=limit)
    with pytest.raises(ValueError, match="must be a string"):
        await client.search(filters={"q": 1})

    valid_manifest = _manifest("atlas")
    transport.manifests["atlas"] = valid_manifest
    transport.searches["atlas"] = {
        "meta": {"contract": "dag:v2", "service_id": "atlas"},
        "items": [
            {"service_id": "atlas", "euid": "persisted-object", "name": "one"},
            {"service_id": "atlas", "euid": "persisted-object", "name": "two"},
        ],
    }
    with pytest.raises(FederationContractError, match="conflicting duplicate"):
        await client.search()

    transport.objects.clear()
    with pytest.raises(LookupError, match="no admitted service"):
        await client.resolve_owner("persisted-unowned-object")


def test_object_graph_and_cycle_payload_validation_is_strict():
    client, _ = _client("atlas")
    with pytest.raises(FederationContractError, match="identity"):
        client._validate_object(
            {"service_id": "atlas", "euid": "other"},
            "atlas",
            "persisted-object",
        )
    with pytest.raises(FederationContractError, match="must be lists"):
        client._validate_object(
            {
                "service_id": "atlas",
                "euid": "persisted-object",
                "external_refs": {},
            },
            "atlas",
            "persisted-object",
        )

    valid_meta = {"contract": "dag:v2", "service_id": "atlas"}
    for payload, message in (
        ({}, "envelope"),
        (
            {"meta": valid_meta, "elements": {"nodes": ["bad"], "edges": []}},
            "node must contain data",
        ),
        (
            {
                "meta": valid_meta,
                "elements": {
                    "nodes": [_node("wrong", "persisted-object")],
                    "edges": [],
                },
            },
            "node service_id mismatch",
        ),
        (
            {
                "meta": valid_meta,
                "elements": {
                    "nodes": [
                        {
                            "data": {
                                **_node("atlas", "persisted-object")["data"],
                                "id": "other",
                            }
                        }
                    ],
                    "edges": [],
                },
            },
            "id must equal",
        ),
        (
            {
                "meta": valid_meta,
                "elements": {
                    "nodes": [
                        {
                            "data": {
                                **_node("atlas", "persisted-object")["data"],
                                "external_refs": {},
                            }
                        }
                    ],
                    "edges": [],
                },
            },
            "projections must be lists",
        ),
        (
            {
                "meta": valid_meta,
                "elements": {
                    "nodes": [_node("atlas", "persisted-object")],
                    "edges": ["bad"],
                },
            },
            "edge must contain data",
        ),
        (
            {
                "meta": valid_meta,
                "elements": {
                    "nodes": [_node("atlas", "persisted-object")],
                    "edges": [
                        {
                            "data": {
                                "service_id": "atlas",
                                "euid": "persisted-edge",
                                "source": "missing",
                                "target": "persisted-object",
                            }
                        }
                    ],
                },
            },
            "invalid service or endpoint",
        ),
    ):
        with pytest.raises(FederationContractError, match=message):
            client._validate_graph(payload, service_id="atlas")

    with pytest.raises(FederationContractError, match="edge data"):
        federation._assert_global_acyclic(
            {"a": {"data": {"id": "a"}}}, {"edge": {"not_data": {}}}
        )


@pytest.mark.anyio
async def test_graph_input_start_failure_local_edges_and_hard_bounds():
    client, transport = _client("atlas")
    with pytest.raises(ValueError, match="FederatedObjectKey"):
        await client.graph("not-a-key")
    for depth in (-1, True, "2"):
        with pytest.raises(ValueError, match="non-negative integer"):
            await client.graph(
                FederatedObjectKey("atlas", "persisted-root"), local_depth=depth
            )
    with pytest.raises(FederationContractError, match="not admitted"):
        await client.graph(FederatedObjectKey("bloom", "persisted-root"))
    with pytest.raises(FederationOperationError, match="starting service failed"):
        await client.graph(FederatedObjectKey("atlas", "persisted-missing"))

    first = "persisted-first"
    second = "persisted-second"
    edge = {
        "data": {
            "id": "persisted-local-edge",
            "euid": "persisted-local-edge",
            "service_id": "atlas",
            "source": first,
            "target": second,
            "relationship_type": "contains",
        }
    }
    transport.graphs[("atlas", first)] = _graph(
        "atlas", [_node("atlas", first), _node("atlas", second)], [edge]
    )
    result = await client.graph(FederatedObjectKey("atlas", first))
    local_edge = result["elements"]["edges"][0]["data"]
    assert local_edge["source"] == f"atlas::{first}"
    assert local_edge["target"] == f"atlas::{second}"

    bounded, bounded_transport = _client(
        "atlas",
        "bloom",
        "ursa",
        limits=FederationLimits(max_external_jumps=1, max_nodes=4),
    )
    root = "persisted-bounded-root"
    bounded_transport.graphs[("atlas", root)] = _graph(
        "atlas",
        [
            _node(
                "atlas",
                root,
                refs=[
                    _ref("bloom", "persisted-bloom", "persisted-lineage-bloom"),
                    _ref("ursa", "persisted-ursa", "persisted-lineage-ursa"),
                ],
                identifiers=["malformed"],
            )
        ],
    )
    with pytest.raises(FederationContractError, match="opaque identifier"):
        await bounded.graph(FederatedObjectKey("atlas", root))

    bounded_transport.graphs[("atlas", root)]["elements"]["nodes"][0]["data"][
        "external_identifiers"
    ] = []
    bounded_transport.graphs[("bloom", "persisted-bloom")] = _graph(
        "bloom", [_node("bloom", "persisted-bloom")]
    )
    result = await bounded.graph(FederatedObjectKey("atlas", root))
    assert "max_external_jumps" in result["meta"]["truncation_reason"]

    opaque_client, opaque_transport = _client(
        "atlas", limits=FederationLimits(max_nodes=1)
    )
    opaque_transport.graphs[("atlas", root)] = _graph(
        "atlas",
        [
            _node(
                "atlas",
                root,
                identifiers=[
                    {
                        "namespace": "doi",
                        "kind": "article",
                        "value": "10.1000/example",
                        "lineage_euid": "persisted-opaque-lineage",
                    }
                ],
            )
        ],
    )
    result = await opaque_client.graph(FederatedObjectKey("atlas", root))
    assert result["meta"]["truncation_reason"] == "max_nodes"

    malformed_ref_client, malformed_ref_transport = _client("atlas")
    malformed_ref_transport.graphs[("atlas", root)] = _graph(
        "atlas", [_node("atlas", root, refs=["malformed"])]
    )
    with pytest.raises(FederationContractError, match="projection must be an object"):
        await malformed_ref_client.graph(FederatedObjectKey("atlas", root))
