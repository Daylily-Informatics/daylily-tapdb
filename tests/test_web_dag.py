from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
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


class _FakeQuery:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter_by(self, **kwargs):
        rows = [
            item
            for item in self._rows
            if all(getattr(item, key, None) == value for key, value in kwargs.items())
        ]
        return _FakeQuery(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, rows_by_model):
        self._rows_by_model = {
            model: list(rows) for model, rows in rows_by_model.items()
        }

    def query(self, model):
        return _FakeQuery(self._rows_by_model.get(model, []))


class _FakeConn:
    def __init__(self, session) -> None:
        self._session = session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    @contextmanager
    def session_scope(self, commit: bool = False):
        _ = commit
        yield self._session


def _build_fake_runtime_connection():
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
                "color": "blue",
                "graph": {
                    "role": "source",
                    "expected_fanout_max": 8,
                    "collapse_by_default": False,
                    "fanout_reason": "root specimen source",
                    "debug": "do not expose",
                },
                "external_payload": {
                    "tapdb_graph": {
                        "system": "atlas",
                        "base_url": "https://atlas.local",
                        "root_euid": "AT-1",
                        "tenant_id": "tenant-1",
                        "graph_data_path": "/api/graph/data",
                        "object_detail_path_template": "/api/object/{euid}",
                        "auth_mode": "none",
                    }
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
    template = SimpleNamespace(
        uid=9,
        euid="GT1",
        name="Template Node",
        category="generic",
        type="template",
        subtype="demo",
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
    template.parent_of_lineages = _FakeRelatedQuery([])
    template.child_of_lineages = _FakeRelatedQuery([])

    objects = {
        generic_template: [template],
        "GX1": (root, "instance"),
        "GX2": (child, "instance"),
        "LG1": (lineage, "lineage"),
    }
    rows_by_model = {
        generic_template: [template],
        generic_instance: [root, child],
        generic_instance_lineage: [lineage],
    }

    return SimpleNamespace(
        conn=_FakeConn(_FakeSession(rows_by_model)),
        objects=objects,
    )


def test_build_dag_capability_advertisement_has_canonical_paths() -> None:
    payload = build_dag_capability_advertisement(auth="session_or_bearer")

    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["extensions"] == ["tapdb.dag_v1"]
    assert payload["capabilities"] == [
        "exact_lookup",
        "native_graph",
        "object_search",
        "external_graph_expansion",
    ]
    assert payload["endpoints"][0]["path"] == "/api/dag/object/{euid}"
    assert payload["endpoints"][1]["path"] == "/api/dag/data"
    assert payload["endpoints"][2]["path"] == "/api/dag/search"
    assert payload["endpoints"][2]["kind"] == "dag_object_search"
    assert payload["external_ref_models"] == [
        "external_payload.tapdb_graph",
        "typed_external_identifier",
    ]


def test_create_tapdb_dag_router_serves_exact_lookup_graph_and_external(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "daylily_tapdb.web.runtime.get_db",
        lambda _config_path: _build_fake_runtime_connection().conn,
    )
    monkeypatch.setattr(
        "daylily_tapdb.web.dag.fetch_remote_graph",
        lambda _request, _ref, *, depth: {
            "elements": {
                "nodes": [{"data": {"id": "AT-1", "euid": "AT-1"}}],
                "edges": [],
            },
            "meta": {"remote_depth": depth},
        },
    )
    monkeypatch.setattr(
        "daylily_tapdb.web.dag.namespace_external_graph",
        lambda payload, *, ref, ref_index, source_euid: {
            "elements": payload["elements"],
            "meta": {
                "source_euid": source_euid,
                "ref_index": ref_index,
                "system": ref.system,
            },
        },
    )
    monkeypatch.setattr(
        "daylily_tapdb.web.dag.fetch_remote_object_detail",
        lambda _request, ref, *, euid: {"euid": euid, "system": ref.system},
    )

    app = FastAPI()
    app.include_router(
        create_tapdb_dag_router(
            config_path="/tmp/tapdb-config.yaml",
            service_name="dewey",
        )
    )
    client = TestClient(app)

    runtime = _build_fake_runtime_connection()
    session = runtime.conn._session
    assert (
        session.query(generic_instance).filter_by(euid="GX1", is_deleted=False).first()
    )

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
    root_node = next(
        node["data"]
        for node in graph_body["elements"]["nodes"]
        if node["data"]["id"] == "GX1"
    )
    assert root_node["created_dt"] == "2024-01-02T03:04:05+00:00"
    assert root_node["modified_dt"] == "2024-01-03T04:05:06+00:00"
    assert root_node["role"] == "source"
    assert root_node["expected_fanout_max"] == 8
    assert root_node["collapse_by_default"] is False
    assert root_node["fanout_reason"] == "root specimen source"
    assert "debug" not in root_node
    assert (
        graph_body["elements"]["nodes"][0]["data"]["external_refs"][0]["root_euid"]
        == "AT-1"
    )

    search_response = client.get(
        "/api/dag/search",
        params={"q": "root", "record_type": "instance", "category": "container"},
    )
    assert search_response.status_code == 200
    search_body = search_response.json()
    assert search_body["meta"]["owner_service"] == "dewey"
    assert search_body["items"][0]["euid"] == "GX1"
    assert search_body["items"][0]["record_type"] == "instance"

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
        "daylily_tapdb.web.runtime.get_db",
        lambda _config_path: _build_fake_runtime_connection().conn,
    )
    app = FastAPI()
    app.include_router(
        create_tapdb_dag_router(
            config_path="/tmp/tapdb-config.yaml",
            service_name="dewey",
        )
    )
    client = TestClient(app)

    response = client.get("/api/dag/object/NOPE-1")
    assert response.status_code == 404
