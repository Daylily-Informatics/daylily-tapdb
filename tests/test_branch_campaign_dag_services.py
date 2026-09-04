from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import daylily_tapdb.external_references as refs
from daylily_tapdb.services import graph_payloads as graphs

REMOTE_EUID = "<persisted-remote-object-euid>"
XRF_EUID = "<persisted-external-reference-euid>"
LINEAGE_EUID = "<persisted-lineage-euid>"
TENANT_ID = "00000000-0000-4000-8000-000000000203"


@pytest.fixture(autouse=True)
def _accept_unit_persisted_euid_placeholders(monkeypatch):
    real_validator = refs.validate_euid
    monkeypatch.setattr(
        refs,
        "validate_euid",
        lambda value: (
            (
                isinstance(value, str)
                and value.startswith("<persisted-")
                and value.endswith("-euid>")
            )
            or real_validator(value)
        ),
    )


class _Related:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def filter_by(self, **values):
        return _Related(
            row
            for row in self.rows
            if all(getattr(row, key, None) == value for key, value in values.items())
        )

    def all(self):
        return list(self.rows)


def _xrf_template(*, domain: str = "A", owner: str = "owner-a"):
    return SimpleNamespace(
        **refs._canonical_template_definition("tapdb_object"),
        domain_code=domain,
        issuer_app_code=owner,
    )


def _object(
    uid: int,
    euid: str,
    *,
    tenant: str | None = "tenant-a",
    owner: str = "owner-a",
    domain: str = "A",
    typed: bool = False,
    properties: object | None = None,
):
    coords = (
        ("reference", "external_identifier", "tapdb_object", "1.0")
        if typed
        else ("content", "specimen", "sample", "1.0")
    )
    payload = properties or {}
    identity_key = None
    if typed and isinstance(payload, dict):
        identity_key = (
            f"{refs.TAPDB_OBJECT_IDENTITY_NAMESPACE}:"
            f"{payload.get('target_service_id')}:{payload.get('target_object_euid')}"
        )
    return SimpleNamespace(
        uid=uid,
        euid=euid,
        name=f"Object {uid}",
        category=coords[0],
        type=coords[1],
        subtype=coords[2],
        version=coords[3],
        polymorphic_discriminator="generic_instance",
        parent_template=(
            _xrf_template(domain=domain, owner=owner)
            if typed
            else SimpleNamespace(instance_prefix="SMP")
        ),
        identity_key=identity_key,
        tenant_id=tenant,
        domain_code=domain,
        issuer_app_code=owner,
        created_dt=datetime(2026, 1, 1, tzinfo=timezone.utc),
        modified_dt=None,
        json_addl={"properties": payload},
        parent_of_lineages=_Related(),
        child_of_lineages=_Related(),
    )


def _lineage(
    uid: int,
    euid: str,
    parent,
    child,
    *,
    tenant: str | None = "tenant-a",
    owner: str = "owner-a",
    domain: str = "A",
    properties: dict | None = None,
):
    lineage_properties = {
        "assertion_authority": "unit-test-authority",
        "asserted_at": "2026-01-02T00:00:00+00:00",
        "assertion_provenance": "unit-test persisted lineage fixture",
        "approved_global_link": False,
        "deactivated_at": None,
        "deactivation_provenance": None,
    }
    if properties:
        lineage_properties.update(properties)
    lineage = SimpleNamespace(
        uid=uid,
        euid=euid,
        parent_instance=parent,
        child_instance=child,
        relationship_type="references",
        tenant_id=tenant,
        domain_code=domain,
        issuer_app_code=owner,
        is_deleted=False,
        created_dt=datetime(2026, 1, 2, tzinfo=timezone.utc),
        json_addl={"properties": lineage_properties},
    )
    parent.parent_of_lineages.rows.append(lineage)
    child.child_of_lineages.rows.append(lineage)
    return lineage


def test_v2_public_properties_strip_all_routing_case_variants():
    obj = _object(
        23,
        "<persisted-object-euid>",
        properties={
            "display_value": "kept",
            "BASE_URL": "https://forbidden.example",
            "Callback_URL": "https://forbidden.example/callback",
            "AUTH_MODE": "same_origin",
            "auth_token": "forbidden",
            "Graph_Data_Path": "/api/legacy",
            7: "non-json-key",
        },
    )
    assert graphs._clean_public_properties(obj) == {"display_value": "kept"}


def test_v2_projects_only_canonical_xrf_lineage_with_optional_target_fields():
    root = _object(1, "<persisted-local-root-euid>")
    ordinary = _object(2, "<persisted-ordinary-child-euid>")
    _lineage(3, "<persisted-ordinary-lineage-euid>", root, ordinary)
    xrf = _object(
        4,
        XRF_EUID,
        tenant=None,
        typed=True,
        properties={
            "target_service_id": "z-remote",
            "target_object_euid": REMOTE_EUID,
            "target_tenant_id": TENANT_ID,
            "target_object_kind": "specimen",
        },
    )
    _lineage(
        5,
        LINEAGE_EUID,
        root,
        xrf,
        properties={
            "approved_global_link": True,
            "asserted_at": "2026-01-01T00:00:00+00:00",
            "assertion_provenance": "branch-campaign",
        },
    )
    root.parent_of_lineages.rows.insert(
        0, SimpleNamespace(child_instance=None, is_deleted=False)
    )

    projected = refs._project_outbound_external_references(root)
    assert projected["external_identifiers"] == []
    assert projected["external_refs"] == [
        {
            "target_service_id": "z-remote",
            "target_object_euid": REMOTE_EUID,
            "relationship_type": "references",
            "assertion_authority": "unit-test-authority",
            "asserted_at": "2026-01-01T00:00:00+00:00",
            "assertion_provenance": "branch-campaign",
            "external_reference_euid": XRF_EUID,
            "lineage_euid": LINEAGE_EUID,
            "target_tenant_id": TENANT_ID,
            "target_object_kind": "specimen",
        }
    ]


@pytest.mark.parametrize(
    ("properties", "message"),
    [
        ({"graph_presentation": "bad"}, "must be an object"),
        ({"graph_presentation": {"role": " bad"}}, "role must be exact"),
        ({"graph_presentation": {"collapse_by_default": 1}}, "must be boolean"),
        ({"graph_presentation": {"expected_fanout": []}}, "must be an object"),
        (
            {
                "graph_presentation": {
                    "expected_fanout": {
                        "relationship_types": [],
                        "max_degree": 1,
                        "reason": "why",
                    }
                }
            },
            "non-empty string list",
        ),
        (
            {
                "graph_presentation": {
                    "expected_fanout": {
                        "relationship_types": ["rel"],
                        "max_degree": True,
                        "reason": "why",
                    }
                }
            },
            "positive integer",
        ),
        (
            {
                "graph_presentation": {
                    "expected_fanout": {
                        "relationship_types": ["rel"],
                        "max_degree": 1,
                        "reason": " bad",
                    }
                }
            },
            "reason must be exact",
        ),
        ({"graph_presentation": {"unsupported": True}}, "Unsupported"),
    ],
)
def test_graph_presentation_rejects_malformed_values(properties, message):
    with pytest.raises(graphs.DagV2GraphContractError, match=message):
        graphs.build_object_detail_v2_payload(
            _object(1, "<persisted-object-euid>", properties=properties),
            record_type="instance",
            service_id="local-service",
        )


def test_graph_presentation_accepts_complete_contract():
    payload = graphs.build_object_detail_v2_payload(
        _object(
            1,
            "<persisted-object-euid>",
            properties={
                "graph_presentation": {
                    "role": "source",
                    "collapse_by_default": False,
                    "expected_fanout": {
                        "relationship_types": ["z", "a", "z"],
                        "max_degree": 2,
                        "reason": "bounded",
                    },
                },
                "base_url": "https://must-not-leak.example",
                "callback_url": "https://must-not-leak.example/callback",
                "public": "visible",
            },
        ),
        record_type="instance",
        service_id="local-service",
    )
    assert payload["presentation"]["expected_fanout"]["relationship_types"] == [
        "a",
        "z",
    ]
    assert payload["properties"] == {"public": "visible"}


@pytest.mark.parametrize(
    ("depth", "max_nodes", "snapshot", "message"),
    [
        (True, 1, None, "depth"),
        (0, False, None, "max_nodes"),
        (0, 1, datetime(2026, 1, 1), "timezone"),
    ],
)
def test_graph_v2_validates_bounds(depth, max_nodes, snapshot, message):
    with pytest.raises(ValueError, match=message):
        graphs.build_graph_v2_payload(
            _object(1, "<persisted-object-euid>"),
            record_type="instance",
            service_id="local-service",
            depth=depth,
            max_nodes=max_nodes,
            snapshot_at=snapshot,
        )


def test_graph_v2_non_instance_and_max_nodes_truncation():
    singleton = graphs.build_graph_v2_payload(
        _object(1, "<persisted-template-euid>"),
        record_type="template",
        service_id="local-service",
        depth=0,
        max_nodes=1,
    )
    assert singleton["meta"]["truncated"] is False

    root = _object(1, "<persisted-root-euid>")
    child = _object(2, "<persisted-child-euid>")
    _lineage(3, "<persisted-edge-euid>", root, child)
    graph = graphs.build_graph_v2_payload(
        root,
        record_type="instance",
        service_id="local-service",
        depth=2,
        max_nodes=1,
    )
    assert graph["meta"]["truncation_reason"] == "max_nodes"


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda root, child, edge: setattr(root, "domain_code", ""), "root is missing"),
        (
            lambda root, child, edge: setattr(child, "issuer_app_code", "owner-b"),
            "Cross-domain",
        ),
        (
            lambda root, child, edge: setattr(edge, "domain_code", "B"),
            "Lineage row scope",
        ),
        (
            lambda root, child, edge: setattr(edge, "child_instance", None),
            "endpoint could not",
        ),
    ],
)
def test_graph_v2_rejects_scope_and_endpoint_faults(mutate, message):
    root = _object(1, "<persisted-root-euid>")
    child = _object(2, "<persisted-child-euid>")
    edge = _lineage(3, "<persisted-edge-euid>", root, child)
    mutate(root, child, edge)
    with pytest.raises(graphs.DagV2GraphContractError, match=message):
        graphs.build_graph_v2_payload(
            root,
            record_type="instance",
            service_id="local-service",
            depth=2,
            max_nodes=5,
        )


def test_graph_v2_allows_approved_tenant_to_global_xrf():
    root = _object(1, "<persisted-root-euid>")
    xrf = _object(
        2,
        XRF_EUID,
        tenant=None,
        typed=True,
        properties={
            "target_service_id": "remote-service",
            "target_object_euid": REMOTE_EUID,
            "target_tenant_id": None,
            "target_object_kind": None,
        },
    )
    _lineage(
        3,
        LINEAGE_EUID,
        root,
        xrf,
        properties={"approved_global_link": True},
    )
    graph = graphs.build_graph_v2_payload(
        root,
        record_type="instance",
        service_id="local-service",
        depth=2,
        max_nodes=5,
    )
    assert len(graph["elements"]["edges"]) == 1


def test_v2_edge_requires_persisted_endpoint_euids():
    parent = _object(1, "<persisted-parent-euid>")
    child = _object(2, "")
    edge = _lineage(3, "<persisted-edge-euid>", parent, child)
    with pytest.raises(graphs.DagV2GraphContractError, match="persisted EUIDs"):
        graphs._v2_edge(edge, service_id="local-service")


def test_graph_cycle_is_rejected():
    root = _object(1, "<persisted-root-euid>")
    child = _object(2, "<persisted-child-euid>")
    _lineage(3, "<persisted-forward-edge-euid>", root, child)
    _lineage(4, "<persisted-back-edge-euid>", child, root)
    with pytest.raises(graphs.DagV2GraphContractError, match="Cycle"):
        graphs.build_graph_v2_payload(
            root,
            record_type="instance",
            service_id="local-service",
            depth=3,
            max_nodes=5,
        )
