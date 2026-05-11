from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from daylily_tapdb.services.graph_payloads import (
    build_graph_payload,
    build_object_detail_payload,
)


class _FakeRelatedQuery:
    def __init__(self, items):
        self._items = list(items)

    def filter_by(self, **kwargs):
        rows = [
            item
            for item in self._items
            if all(getattr(item, key, None) == value for key, value in kwargs.items())
        ]
        return _FakeRelatedQuery(rows)

    def __iter__(self):
        return iter(self._items)


def _build_instance_graph():
    root = SimpleNamespace(
        uid=1,
        euid="GX1",
        name="Root Tube",
        category="container",
        type="tube",
        subtype="sample",
        version="1.0",
        bstatus="active",
        json_addl={
            "properties": {
                "external_payload": {"tapdb_graph": []},
                "graph": {
                    "role": "root",
                    "expected_fanout_max": 12,
                    "collapse_by_default": True,
                    "fanout_reason": "one source object can have many outputs",
                    "unsafe_layout": "left",
                },
            }
        },
        created_dt=datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        modified_dt=datetime(2024, 1, 3, 4, 5, 6, tzinfo=timezone.utc),
        is_deleted=False,
    )
    child = SimpleNamespace(
        uid=2,
        euid="GX2",
        name="Child Specimen",
        category="content",
        type="specimen",
        subtype="aliquot",
        version="1.0",
        bstatus="active",
        json_addl={},
        created_dt=None,
        modified_dt=None,
        is_deleted=False,
    )
    lineage = SimpleNamespace(
        uid=3,
        euid="LG1",
        relationship_type="contains",
        parent_instance=root,
        child_instance=child,
        is_deleted=False,
    )
    root.parent_of_lineages = _FakeRelatedQuery([lineage])
    root.child_of_lineages = _FakeRelatedQuery([])
    child.parent_of_lineages = _FakeRelatedQuery([])
    child.child_of_lineages = _FakeRelatedQuery([lineage])
    return root, child


def test_build_object_detail_payload_includes_external_refs_and_iso_dates() -> None:
    obj = SimpleNamespace(
        uid=1,
        euid="GX1",
        name="Root Tube",
        category="container",
        type="tube",
        subtype="sample",
        version="1.0",
        bstatus="active",
        json_addl={
            "properties": {
                "external_payload": {
                    "tapdb_graph": {
                        "system": "atlas",
                        "root_euid": "AT-1",
                        "base_url": "https://atlas.local",
                        "graph_data_path": "/api/graph/data",
                        "object_detail_path_template": "/api/object/{euid}",
                        "auth_mode": "none",
                        "label": "Atlas patient record",
                        "relationship_type": "represents",
                        "source_field": "properties.patient_id",
                    }
                }
            }
        },
        created_dt=datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        modified_dt=datetime(2024, 1, 3, 4, 5, 6, tzinfo=timezone.utc),
    )

    payload = build_object_detail_payload(
        obj,
        record_type="instance",
        service_name="dewey",
    )

    assert payload["uid"] == 1
    assert payload["system"] == "dewey"
    assert payload["display_label"] == "Root Tube"
    assert payload["created_dt"] == "2024-01-02T03:04:05+00:00"
    assert payload["modified_dt"] == "2024-01-03T04:05:06+00:00"
    assert payload["external_refs"] == [
        {
            "label": "Atlas patient record",
            "system": "atlas",
            "root_euid": "AT-1",
            "tenant_id": None,
            "href": "https://atlas.local/api/object/AT-1",
            "graph_expandable": True,
            "ref_index": 0,
            "relationship_type": "represents",
            "source_field": "properties.patient_id",
        }
    ]


def test_build_graph_payload_returns_instance_graph_and_singletons() -> None:
    root, child = _build_instance_graph()

    payload = build_graph_payload(
        root,
        record_type="instance",
        service_name="dewey",
        depth=2,
    )
    node_ids = {item["data"]["id"] for item in payload["elements"]["nodes"]}

    assert node_ids == {"GX1", "GX2"}
    assert payload["elements"]["edges"][0]["data"]["source"] == "GX2"
    assert payload["elements"]["edges"][0]["data"]["target"] == "GX1"
    root_node = next(
        node["data"]
        for node in payload["elements"]["nodes"]
        if node["data"]["id"] == "GX1"
    )
    child_node = next(
        node["data"]
        for node in payload["elements"]["nodes"]
        if node["data"]["id"] == "GX2"
    )
    assert root_node["color"] == "#8B00FF"
    assert root_node["created_dt"] == "2024-01-02T03:04:05+00:00"
    assert root_node["modified_dt"] == "2024-01-03T04:05:06+00:00"
    assert child_node["created_dt"] is None
    assert child_node["modified_dt"] is None
    assert root_node["role"] == "root"
    assert root_node["expected_fanout_max"] == 12
    assert root_node["collapse_by_default"] is True
    assert root_node["fanout_reason"] == "one source object can have many outputs"
    assert "unsafe_layout" not in root_node

    lineage_only = build_graph_payload(
        child.child_of_lineages._items[0],
        record_type="lineage",
        service_name="dewey",
        depth=1,
    )
    assert lineage_only["elements"]["nodes"][0]["data"]["record_type"] == "lineage"
    assert lineage_only["elements"]["edges"] == []
