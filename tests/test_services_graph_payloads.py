from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import daylily_tapdb.services as services
import daylily_tapdb.services.graph_payloads as graph_payloads


class _Related:
    def filter_by(self, **_values):
        return self

    def all(self):
        return []


def test_only_dag_v2_payload_builders_are_exported() -> None:
    assert not hasattr(graph_payloads, "build_graph_payload")
    assert not hasattr(graph_payloads, "build_object_detail_payload")
    assert not hasattr(services, "build_graph_payload")
    assert not hasattr(services, "build_object_detail_payload")
    assert services.__all__ == [
        "DagV2GraphContractError",
        "build_graph_v2_payload",
        "build_object_detail_v2_payload",
        "build_visible_graph_v2_payload",
        "find_object_by_euid",
        "search_external_reference_sources",
        "search_objects",
    ]


def test_v2_object_payload_has_both_canonical_discovery_collections() -> None:
    obj = SimpleNamespace(
        uid=1,
        euid="persisted-object",
        name="Persisted object",
        category="content",
        type="specimen",
        subtype="sample",
        version="1.0",
        tenant_id=None,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        json_addl={"properties": {}},
        created_dt=datetime(2026, 9, 4, tzinfo=UTC),
        modified_dt=None,
        parent_of_lineages=_Related(),
        child_of_lineages=_Related(),
    )
    payload = graph_payloads.build_object_detail_v2_payload(
        obj, record_type="instance", service_id="catalog"
    )
    assert payload["service_id"] == "catalog"
    assert payload["external_refs"] == []
    assert payload["external_identifiers"] == []
    assert "href" not in payload
    assert "graph_href" not in payload


def test_visible_graph_v2_is_lineage_only_bounded_and_deterministic() -> None:
    parent = SimpleNamespace(
        uid=2,
        euid="persisted-parent",
        name="Parent",
        category="content",
        type="specimen",
        subtype="sample",
        version="1.0",
        tenant_id=None,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        json_addl={"properties": {}},
        created_dt=datetime(2026, 9, 4, tzinfo=UTC),
        modified_dt=None,
        parent_of_lineages=_Related(),
        child_of_lineages=_Related(),
        is_deleted=False,
    )
    child = SimpleNamespace(
        **{
            **parent.__dict__,
            "uid": 1,
            "euid": "persisted-child",
            "name": "Child",
        }
    )
    lineage = SimpleNamespace(
        uid=3,
        euid="persisted-lineage",
        parent_instance=parent,
        child_instance=child,
        relationship_type="contains",
        tenant_id=None,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        json_addl={"properties": {}},
        created_dt=datetime(2026, 9, 4, tzinfo=UTC),
        is_deleted=False,
    )

    payload = graph_payloads.build_visible_graph_v2_payload(
        [parent, child],
        [lineage],
        service_id="catalog",
        max_nodes=10,
        max_edges=10,
        snapshot_at=datetime(2026, 9, 4, tzinfo=UTC),
    )

    assert payload["meta"] == {
        "contract": "dag:v2",
        "service_id": "catalog",
        "query_mode": "visible_scope",
        "graph_revision": payload["meta"]["graph_revision"],
        "snapshot_at": "2026-09-04T00:00:00+00:00",
        "truncated": False,
        "truncation_reason": None,
        "effective_limits": {
            "max_depth": None,
            "max_nodes": 10,
            "max_edges": 10,
        },
    }
    assert [node["data"]["euid"] for node in payload["elements"]["nodes"]] == [
        "persisted-child",
        "persisted-parent",
    ]
    edge = payload["elements"]["edges"][0]["data"]
    assert edge["euid"] == "persisted-lineage"
    assert edge["relationship_type"] == "contains"
    assert edge["source"] == "persisted-parent"
    assert edge["target"] == "persisted-child"
    assert edge["presentation"]["assertion_provenance"] == (
        "tapdb.lineage:persisted-lineage"
    )


def test_visible_graph_v2_reports_bounds() -> None:
    rows = [
        SimpleNamespace(
            uid=index,
            euid=f"persisted-{index}",
            name=f"Object {index}",
            category="content",
            type="specimen",
            subtype="sample",
            version="1.0",
            tenant_id=None,
            domain_code="Z",
            issuer_app_code="daylily-tapdb",
            json_addl={"properties": {}},
            created_dt=None,
            modified_dt=None,
            parent_of_lineages=_Related(),
            child_of_lineages=_Related(),
            is_deleted=False,
        )
        for index in (1, 2)
    ]
    payload = graph_payloads.build_visible_graph_v2_payload(
        rows,
        [],
        service_id="catalog",
        max_nodes=1,
        max_edges=1,
    )
    assert payload["meta"]["truncated"] is True
    assert payload["meta"]["truncation_reason"] == "max_nodes"
