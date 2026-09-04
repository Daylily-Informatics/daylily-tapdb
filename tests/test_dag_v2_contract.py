from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.services.graph_payloads import (
    DagV2GraphContractError,
    build_graph_v2_payload,
)
from daylily_tapdb.web.dag_v2 import (
    DAG_V2_CONTRACT,
    DagV2EligibilityReason,
    DagV2Limits,
    mount_tapdb_dag_surfaces,
    validate_dag_v2_manifest,
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


def _instance(
    uid: int,
    euid: str,
    *,
    tenant_id: str | None = "00000000-0000-0000-0000-000000000001",
    properties: dict | None = None,
):
    return SimpleNamespace(
        uid=uid,
        euid=euid,
        name=f"Object {uid}",
        category="content",
        type="specimen",
        subtype="sample",
        version="1.0",
        bstatus="active",
        tenant_id=tenant_id,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        polymorphic_discriminator="generic_instance",
        json_addl={"properties": properties or {}},
        created_dt=datetime(2026, 1, 1, tzinfo=UTC),
        modified_dt=datetime(2026, 1, 2, tzinfo=UTC),
        is_deleted=False,
        parent_of_lineages=_Related(),
        child_of_lineages=_Related(),
    )


def _lineage(uid: int, euid: str, parent, child):
    row = SimpleNamespace(
        uid=uid,
        euid=euid,
        name=euid,
        parent_instance=parent,
        child_instance=child,
        parent_instance_uid=parent.uid,
        child_instance_uid=child.uid,
        relationship_type="contains",
        tenant_id=parent.tenant_id,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        json_addl={"properties": {}},
        created_dt=datetime(2026, 1, 1, tzinfo=UTC),
        is_deleted=False,
    )
    parent.parent_of_lineages.rows.append(row)
    child.child_of_lineages.rows.append(row)
    return row


def _config(tmp_path):
    domain_registry = tmp_path / "domain_code_registry.json"
    prefix_registry = tmp_path / "prefix_ownership_registry.json"
    domain_registry.write_text(
        json.dumps({"version": "0.4.0", "domains": {"Z": {"name": "unit-test"}}}),
        encoding="utf-8",
    )
    prefix_registry.write_text(
        json.dumps(
            {
                "version": "0.4.0",
                "ownership": {
                    "Z": {
                        prefix: {"issuer_app_code": "daylily-tapdb"}
                        for prefix in (
                            "ADT",
                            "EDG",
                            "GSE",
                            "GVR",
                            "MSG",
                            "SYS",
                            "TPX",
                            "XRF",
                        )
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    path = tmp_path / "tapdb-config.yaml"
    path.write_text(
        json.dumps(
            {
                "meta": {
                    "config_version": 4,
                    "client_id": "example",
                    "database_name": "graph",
                    "owner_repo_name": "daylily-tapdb",
                    "domain_registry_path": str(domain_registry),
                    "prefix_ownership_registry_path": str(prefix_registry),
                },
                "target": {
                    "engine_type": "local",
                    "host": "localhost",
                    "port": 5432,
                    "ui_port": 8000,
                    "user": "tapdb_runtime",
                    "password": "",
                    "database": "tapdb",
                    "schema_name": "tapdb_unit",
                    "domain_code": "Z",
                },
                "safety": {
                    "safety_tier": "local",
                    "destructive_operations": "confirm_required",
                },
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def test_mount_is_atomic_explicit_authenticated_and_exact(tmp_path) -> None:
    app = FastAPI()
    limits = DagV2Limits(max_depth=4, max_nodes=100, max_search_page_size=25)

    missing = mount_tapdb_dag_surfaces(
        app,
        config_path=str(tmp_path / "missing.yaml"),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=lambda _request: {"username": "operator"},
        limits=limits,
    )
    assert missing.mounted is False
    assert missing.reason is DagV2EligibilityReason.MISSING_CONFIG
    assert missing.advertisement is None
    assert not any(route.path == "/api/dag/manifest" for route in app.routes)

    no_auth = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=None,
        limits=limits,
    )
    assert no_auth.reason is DagV2EligibilityReason.AUTH_REQUIRED

    async def authenticated(_request: Request):
        return {"username": "operator"}

    mounted = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=authenticated,
        limits=limits,
    )
    assert mounted.mounted is True
    assert mounted.manifest is not None
    assert mounted.manifest.service_id == "example-service"
    assert mounted.manifest.contract == DAG_V2_CONTRACT
    assert mounted.manifest.features == {
        "typed_external_references": True,
        "typed_external_identifiers": True,
        "external_reference_search": True,
        "typed_graph_presentation": True,
        "snapshot_metadata": True,
        "outbound_fetch": False,
    }
    mismatch = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="example_service",
        display_name="Example Service",
        auth_dependency=authenticated,
        limits=limits,
    )
    assert mismatch.reason is DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH


def test_manifest_validation_has_no_alias_or_v1_fallback(tmp_path) -> None:
    app = FastAPI()

    async def authenticated(_request: Request):
        return {"username": "operator"}

    result = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="zebra-day",
        display_name="Zebra Day",
        auth_dependency=authenticated,
        limits=DagV2Limits(max_depth=3, max_nodes=50, max_search_page_size=20),
    )
    payload = result.manifest.to_dict()
    assert validate_dag_v2_manifest(payload, expected_service_id="zebra-day") is None
    assert (
        validate_dag_v2_manifest(payload, expected_service_id="zebra_day")
        is DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH
    )
    old_contract = dict(payload, extension="tapdb.dag_v1", contract="dag:v1")
    assert (
        validate_dag_v2_manifest(old_contract, expected_service_id="zebra-day")
        is DagV2EligibilityReason.VERSION_MISMATCH
    )


def test_graph_v2_snapshot_presentation_and_truncation() -> None:
    root = _instance(
        1,
        "persisted-root",
        properties={
            "graph_presentation": {
                "role": "source",
                "collapse_by_default": False,
                "expected_fanout": {
                    "relationship_types": ["contains"],
                    "max_degree": 4,
                    "reason": "bounded source fanout",
                },
            }
        },
    )
    child = _instance(2, "persisted-child")
    _lineage(3, "persisted-edge", root, child)

    payload = build_graph_v2_payload(
        root,
        record_type="instance",
        service_id="example-service",
        depth=0,
        max_nodes=10,
        snapshot_at=datetime(2026, 1, 3, tzinfo=UTC),
    )
    assert payload["meta"]["contract"] == "dag:v2"
    assert payload["meta"]["snapshot_at"] == "2026-01-03T00:00:00+00:00"
    assert len(payload["meta"]["graph_revision"]) == 64
    assert payload["meta"]["truncated"] is True
    assert payload["meta"]["truncation_reason"] == "max_depth"
    node = payload["elements"]["nodes"][0]["data"]
    assert node["presentation"]["role"] == "source"
    assert node["external_refs"] == []
    assert node["external_identifiers"] == []


def test_graph_v2_rejects_self_loop_cycle_cross_tenant_and_metadata_edges() -> None:
    root = _instance(1, "persisted-root")
    self_loop = _lineage(2, "persisted-self-loop", root, root)
    with pytest.raises(DagV2GraphContractError, match="Self-loop"):
        build_graph_v2_payload(
            root,
            record_type="instance",
            service_id="example-service",
            depth=2,
            max_nodes=10,
        )
    root.parent_of_lineages.rows.remove(self_loop)
    root.child_of_lineages.rows.remove(self_loop)

    other = _instance(
        3,
        "persisted-other-tenant",
        tenant_id="00000000-0000-0000-0000-000000000002",
    )
    _lineage(4, "persisted-cross-tenant", root, other)
    with pytest.raises(DagV2GraphContractError, match="Cross-tenant"):
        build_graph_v2_payload(
            root,
            record_type="instance",
            service_id="example-service",
            depth=2,
            max_nodes=10,
        )

    isolated = _instance(
        5,
        "persisted-metadata-source",
        properties={"external_payload": {"tapdb_graph": {"copied": True}}},
    )
    with pytest.raises(DagV2GraphContractError, match="canonical XRF lineage"):
        build_graph_v2_payload(
            isolated,
            record_type="instance",
            service_id="example-service",
            depth=0,
            max_nodes=10,
        )


class _ObjectQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter_by(self, **values):
        return _ObjectQuery(
            row
            for row in self.rows
            if all(getattr(row, key, None) == value for key, value in values.items())
        )

    def first(self):
        return self.rows[0] if self.rows else None


class _ObjectSession:
    def __init__(self, root):
        self.root = root

    def query(self, model):
        return _ObjectQuery([self.root] if model is generic_instance else [])


class _Connection:
    def __init__(self, root):
        self.session = _ObjectSession(root)
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self):
        yield self.session


def test_v2_routes_are_authenticated_and_dispatch_exact_external_search(
    monkeypatch, tmp_path
) -> None:
    root = _instance(1, "persisted-root")
    app = FastAPI()

    async def authenticated(request: Request):
        if request.headers.get("authorization") != "Bearer test-token":
            raise HTTPException(status_code=401, detail="auth_required")
        return {"username": "operator"}

    monkeypatch.setattr(
        "daylily_tapdb.web.runtime.get_db", lambda _path: _Connection(root)
    )
    generic_calls = []
    external_calls = []

    def fake_search(_session, **kwargs):
        generic_calls.append(kwargs)
        return {
            "items": [],
            "page": {"limit": kwargs["limit"], "returned": 0, "next_cursor": None},
            "filters": {},
        }

    def fake_external_search(_session, **kwargs):
        external_calls.append(kwargs)
        if not kwargs["external_object_euid"]:
            raise ValueError(
                "external_service_id and external_object_euid are required together"
            )
        return {
            "items": [],
            "page": {"limit": kwargs["limit"], "returned": 0, "next_cursor": None},
            "filters": {},
        }

    monkeypatch.setattr("daylily_tapdb.web.dag_v2.search_objects", fake_search)
    monkeypatch.setattr(
        "daylily_tapdb.web.dag_v2.search_external_reference_sources",
        fake_external_search,
    )
    result = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=authenticated,
        limits=DagV2Limits(max_depth=2, max_nodes=20, max_search_page_size=5),
    )
    assert result.mounted is True
    client = TestClient(app)
    headers = {"authorization": "Bearer test-token"}

    assert client.get("/api/dag/manifest").status_code == 401
    assert client.get("/api/dag/manifest", headers=headers).status_code == 200
    detail = client.get("/api/dag/v2/object/persisted-root", headers=headers)
    assert detail.status_code == 200
    assert detail.json()["service_id"] == "example-service"
    graph = client.get(
        "/api/dag/v2/data?start_euid=persisted-root&depth=0", headers=headers
    )
    assert graph.status_code == 200

    generic = client.get("/api/dag/v2/search?record_type=instance", headers=headers)
    assert generic.status_code == 200, generic.text
    assert generic_calls[-1]["record_type"] == "instance"
    assert client.get("/api/dag/v2/search?limit=6", headers=headers).status_code == 422

    non_instance = client.get(
        "/api/dag/v2/search?external_service_id=remote&record_type=all",
        headers=headers,
    )
    assert non_instance.status_code == 422
    incomplete = client.get(
        "/api/dag/v2/search?external_service_id=remote&record_type=instance",
        headers=headers,
    )
    assert incomplete.status_code == 422
    exact = client.get(
        "/api/dag/v2/search?external_service_id=remote&"
        "external_object_euid=persisted-by-owner&record_type=instance",
        headers=headers,
    )
    assert exact.status_code == 200
    assert external_calls[-1]["external_service_id"] == "remote"
    assert external_calls[-1]["external_object_euid"] == "persisted-by-owner"
