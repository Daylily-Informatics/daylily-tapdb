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
        json_addl={"properties": {"external_payload": {"tapdb_graph": []}}},
        created_dt=datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
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
                    }
                }
            }
        },
        created_dt=datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
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
    assert payload["external_refs"] == [
        {
            "label": "atlas:AT-1",
            "system": "atlas",
            "root_euid": "AT-1",
            "tenant_id": None,
            "href": "https://atlas.local/api/object/AT-1",
            "graph_expandable": True,
            "ref_index": 0,
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
    assert payload["elements"]["nodes"][0]["data"]["color"] == "#8B00FF"

    lineage_only = build_graph_payload(
        child.child_of_lineages._items[0],
        record_type="lineage",
        service_name="dewey",
        depth=1,
    )
    assert lineage_only["elements"]["nodes"][0]["data"]["record_type"] == "lineage"
    assert lineage_only["elements"]["edges"] == []
