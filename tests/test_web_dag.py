from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from daylily_tapdb.web.dag import (
    CONTRACT_VERSION,
    build_dag_capability_advertisement,
    create_tapdb_dag_router,
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


class _FakeConn:
    def __init__(self) -> None:
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    @contextmanager
    def session_scope(self, commit: bool = False):
        _ = commit
        yield object()


def _build_fake_admin_main():
    root = SimpleNamespace(
        uid=1,
        euid="GX1",
        name="Root Tube",
        category="container",
        type="tube",
        subtype="sample",
        version="1.0",
        bstatus="active",
        json_addl={"properties": {"color": "blue"}},
        created_dt=None,
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

    objects = {
        "GX1": (root, "instance"),
        "GX2": (child, "instance"),
        "LG1": (lineage, "lineage"),
    }

    refs = [
        {
            "label": "atlas:AT-1",
            "system": "atlas",
            "root_euid": "AT-1",
            "tenant_id": "tenant-1",
            "href": "https://atlas.local/object/AT-1",
            "graph_expandable": True,
            "ref_index": 0,
        }
    ]
    root.external_refs = refs
    root.refs = [SimpleNamespace(system="atlas", root_euid="AT-1")]

    return SimpleNamespace(
        get_db=lambda: _FakeConn(),
        _find_object_by_euid=lambda _session, euid: objects.get(euid, (None, None)),
        _external_ref_payloads=lambda obj: list(getattr(obj, "external_refs", [])),
        get_external_ref_by_index=lambda obj, idx: obj.refs[idx],
        fetch_remote_graph=lambda _request, _ref, *, depth: {
            "elements": {
                "nodes": [{"data": {"id": "AT-1", "euid": "AT-1"}}],
                "edges": [],
            },
            "meta": {"remote_depth": depth},
        },
        namespace_external_graph=lambda payload, *, ref, ref_index, source_euid: {
            "elements": payload["elements"],
            "meta": {
                "source_euid": source_euid,
                "ref_index": ref_index,
                "system": ref.system,
            },
        },
        fetch_remote_object_detail=lambda _request, ref, *, euid: {
            "euid": euid,
            "system": ref.system,
        },
    )


def test_build_dag_capability_advertisement_has_canonical_paths() -> None:
    payload = build_dag_capability_advertisement(auth="session_or_bearer")

    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["extensions"] == ["tapdb.dag_v1"]
    assert payload["capabilities"] == [
        "exact_lookup",
        "native_graph",
        "external_graph_expansion",
    ]
    assert payload["endpoints"][0]["path"] == "/api/dag/object/{euid}"
    assert payload["endpoints"][1]["path"] == "/api/dag/data"


def test_create_tapdb_dag_router_serves_exact_lookup_graph_and_external(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "daylily_tapdb.web.dag._load_admin_main",
        lambda _config_path, _env_name: _build_fake_admin_main(),
    )

    app = FastAPI()
    app.include_router(
        create_tapdb_dag_router(
            config_path="/tmp/tapdb-config.yaml",
            env_name="dev",
            service_name="dewey",
        )
    )
    client = TestClient(app)

    object_response = client.get("/api/dag/object/GX1")
    assert object_response.status_code == 200
    object_body = object_response.json()
    assert object_body["euid"] == "GX1"
    assert object_body["system"] == "dewey"
    assert object_body["record_type"] == "instance"
    assert object_body["external_refs"][0]["root_euid"] == "AT-1"

    graph_response = client.get(
        "/api/dag/data", params={"start_euid": "GX1", "depth": 2}
    )
    assert graph_response.status_code == 200
    graph_body = graph_response.json()
    assert graph_body["meta"] == {
        "start_euid": "GX1",
        "depth": 2,
        "owner_service": "dewey",
        "root_record_type": "instance",
        "contract_version": CONTRACT_VERSION,
    }
    node_ids = {item["data"]["id"] for item in graph_body["elements"]["nodes"]}
    assert node_ids == {"GX1", "GX2"}
    assert graph_body["elements"]["edges"][0]["data"]["source"] == "GX2"
    assert graph_body["elements"]["edges"][0]["data"]["target"] == "GX1"

    external_graph_response = client.get(
        "/api/dag/external",
        params={"source_euid": "GX1", "ref_index": 0, "depth": 3},
    )
    assert external_graph_response.status_code == 200
    assert external_graph_response.json()["meta"]["system"] == "atlas"

    external_object_response = client.get(
        "/api/dag/external/object",
        params={"source_euid": "GX1", "ref_index": 0, "euid": "AT-9"},
    )
    assert external_object_response.status_code == 200
    assert external_object_response.json()["euid"] == "AT-9"


def test_create_tapdb_dag_router_returns_404_for_non_owned_euid(monkeypatch) -> None:
    monkeypatch.setattr(
        "daylily_tapdb.web.dag._load_admin_main",
        lambda _config_path, _env_name: _build_fake_admin_main(),
    )
    app = FastAPI()
    app.include_router(
        create_tapdb_dag_router(
            config_path="/tmp/tapdb-config.yaml",
            env_name="dev",
            service_name="dewey",
        )
    )
    client = TestClient(app)

    response = client.get("/api/dag/object/NOPE-1")
    assert response.status_code == 404
