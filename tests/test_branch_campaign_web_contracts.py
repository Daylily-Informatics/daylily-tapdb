from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from daylily_tapdb.services.graph_payloads import DagV2GraphContractError
from daylily_tapdb.web import dag_v2, runtime


class _Conn:
    def __init__(self):
        self.session = object()
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self, commit=False):
        assert commit is False
        yield self.session


def _manifest():
    return dag_v2._manifest_for(
        service_id="local-service",
        display_name="Local Service",
        limits=dag_v2.DagV2Limits(
            max_depth=2,
            max_nodes=4,
            max_search_page_size=3,
        ),
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_depth": True, "max_nodes": 1, "max_search_page_size": 1}, "max_depth"),
        ({"max_depth": 33, "max_nodes": 1, "max_search_page_size": 1}, "<= 32"),
        ({"max_depth": 1, "max_nodes": 10_001, "max_search_page_size": 1}, "<= 10000"),
        ({"max_depth": 1, "max_nodes": 1, "max_search_page_size": 101}, "<= 100"),
    ],
)
def test_branch_campaign_v2_limits_enforce_type_and_caps(kwargs, message):
    with pytest.raises(ValueError, match=message):
        dag_v2.DagV2Limits(**kwargs)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (" bad", "exact"),
        ("bad/service", "only alphanumerics"),
        ("service-é", "only alphanumerics"),
    ],
)
def test_branch_campaign_v2_service_id_validation(value, message):
    with pytest.raises(ValueError, match=message):
        dag_v2._require_exact_service_id(value)


@pytest.mark.parametrize("value", ["", " bad", "bad\nname"])
def test_branch_campaign_v2_display_name_validation(value):
    with pytest.raises(ValueError):
        dag_v2._require_display_name(value)


def test_branch_campaign_v2_config_path_must_be_explicit_absolute(tmp_path):
    with pytest.raises(FileNotFoundError, match="explicit absolute"):
        dag_v2._require_absolute_config("")
    with pytest.raises(FileNotFoundError, match="must be absolute"):
        dag_v2._require_absolute_config("relative.yaml")
    with pytest.raises(FileNotFoundError, match="must be absolute"):
        dag_v2._require_absolute_config("~/tapdb-config.yaml")
    with pytest.raises(FileNotFoundError, match="does not exist"):
        dag_v2._require_absolute_config(str(tmp_path / "missing.yaml"))


def test_branch_campaign_v2_manifest_validation_reasons():
    manifest = _manifest().to_dict()
    assert (
        dag_v2.validate_dag_v2_manifest(None, expected_service_id="local-service")
        is dag_v2.DagV2EligibilityReason.MISSING_MANIFEST
    )
    assert (
        dag_v2.validate_dag_v2_manifest(
            {**manifest, "eligible": False}, expected_service_id="local-service"
        )
        is dag_v2.DagV2EligibilityReason.MOUNT_UNAVAILABLE
    )
    assert (
        dag_v2.validate_dag_v2_manifest(
            {**manifest, "endpoints": []}, expected_service_id="local-service"
        )
        is dag_v2.DagV2EligibilityReason.VERSION_MISMATCH
    )
    assert (
        dag_v2.validate_dag_v2_manifest(
            {**manifest, "limits": []}, expected_service_id="local-service"
        )
        is dag_v2.DagV2EligibilityReason.INVALID_LIMITS
    )
    assert (
        dag_v2.validate_dag_v2_manifest(
            {**manifest, "limits": {"max_depth": 0}},
            expected_service_id="local-service",
        )
        is dag_v2.DagV2EligibilityReason.INVALID_LIMITS
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(display_name=""),
        lambda payload: payload.update(manifest_revision="0" * 64),
        lambda payload: payload.update(features={"outbound_fetch": True}),
        lambda payload: payload["endpoints"].append(dict(payload["endpoints"][0])),
        lambda payload: payload["endpoints"][0].update(path="/wrong"),
    ],
)
def test_branch_campaign_v2_manifest_rejects_malformed_contract(mutate):
    payload = _manifest().to_dict()
    mutate(payload)

    assert (
        dag_v2.validate_dag_v2_manifest(payload, expected_service_id="local-service")
        is dag_v2.DagV2EligibilityReason.VERSION_MISMATCH
    )


def test_branch_campaign_v2_actor_resolution_shapes():
    assert (
        dag_v2._actor_from_auth({"email": "operator@example.test"})
        == "operator@example.test"
    )
    assert dag_v2._actor_from_auth(SimpleNamespace(username="operator")) == "operator"
    with pytest.raises(HTTPException) as exc_info:
        dag_v2._actor_from_auth({})
    assert exc_info.value.status_code == 401


def _v2_client(monkeypatch, *, manifest=None):
    conn = _Conn()
    monkeypatch.setattr(dag_v2.dag_runtime, "get_db", lambda _path: conn)

    async def authenticated(_request: Request):
        return {"sub": "operator"}

    app = FastAPI()
    app.include_router(
        dag_v2._build_router(
            config_path="/explicit/config.yaml",
            manifest=manifest or _manifest(),
            auth_dependency=authenticated,
        )
    )
    return TestClient(app), conn


def test_branch_campaign_v2_routes_cover_success_and_service_limits(monkeypatch):
    client, conn = _v2_client(monkeypatch)
    owned = SimpleNamespace(euid="persisted-object")
    monkeypatch.setattr(
        dag_v2, "find_object_by_euid", lambda _session, _euid: (owned, "instance")
    )
    monkeypatch.setattr(
        dag_v2,
        "build_object_detail_v2_payload",
        lambda *_a, **_k: {"euid": "persisted-object"},
    )
    monkeypatch.setattr(
        dag_v2,
        "build_graph_v2_payload",
        lambda *_a, **kwargs: {"meta": {"max_nodes": kwargs["max_nodes"]}},
    )
    monkeypatch.setattr(
        dag_v2,
        "search_objects",
        lambda *_a, **kwargs: {"items": [], "page": {"limit": kwargs["limit"]}},
    )

    assert client.get("/api/dag/manifest").status_code == 200
    assert client.get("/api/dag/v2/object/persisted-object").status_code == 200
    graph = client.get("/api/dag/v2/data", params={"start_euid": "persisted-object"})
    assert graph.json()["meta"]["max_nodes"] == 4
    search = client.get("/api/dag/v2/search", params={"limit": 2, "cursor": "next"})
    assert search.status_code == 200
    assert search.json()["meta"]["ownership_proof"] is False
    assert conn.app_username == "operator"

    assert (
        client.get(
            "/api/dag/v2/data", params={"start_euid": "x", "depth": 3}
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/dag/v2/data", params={"start_euid": "x", "max_nodes": 5}
        ).status_code
        == 422
    )
    assert client.get("/api/dag/v2/search", params={"limit": 4}).status_code == 422


def test_branch_campaign_v2_search_honors_advertised_boundary_and_canonicalizes(
    monkeypatch,
):
    manifest = dag_v2._manifest_for(
        service_id="local-service",
        display_name="Local Service",
        limits=dag_v2.DagV2Limits(
            max_depth=2,
            max_nodes=4,
            max_search_page_size=100,
        ),
    )
    client, _conn = _v2_client(monkeypatch, manifest=manifest)
    calls = []

    def _search(_session, **kwargs):
        calls.append(kwargs)
        return {
            "items": [
                {
                    "euid": "<persisted-search-result-euid>",
                    "record_type": "instance",
                    "system": "legacy-system",
                    "service": "legacy-service",
                    "kind": "legacy-kind",
                    "href": "/object/legacy",
                    "graph_href": "/api/dag/data?start_euid=legacy",
                    "display_label": "Result",
                }
            ],
            "page": {"limit": kwargs["limit"], "returned": 1, "next_cursor": None},
            "filters": {},
        }

    monkeypatch.setattr(dag_v2, "search_objects", _search)
    response = client.get("/api/dag/v2/search", params={"limit": 100})
    assert response.status_code == 200
    assert calls[0]["limit"] == 100
    item = response.json()["items"][0]
    assert item["service_id"] == "local-service"
    assert item["record_type"] == "instance"
    assert item["href"] == "/api/dag/v2/object/<persisted-search-result-euid>"
    assert item["graph_href"] == (
        "/api/dag/v2/data?start_euid=<persisted-search-result-euid>"
    )
    assert not ({"system", "service", "kind"} & set(item))
    assert client.get("/api/dag/v2/search", params={"limit": 101}).status_code == 422


def test_branch_campaign_v2_routes_translate_not_owned_and_contract_errors(monkeypatch):
    client, _conn = _v2_client(monkeypatch)
    state = {"result": (None, None)}
    monkeypatch.setattr(dag_v2, "find_object_by_euid", lambda *_a: state["result"])

    assert client.get("/api/dag/v2/object/missing").status_code == 404
    assert (
        client.get("/api/dag/v2/data", params={"start_euid": "missing"}).status_code
        == 404
    )

    state["result"] = (SimpleNamespace(euid="persisted-object"), "instance")
    monkeypatch.setattr(
        dag_v2,
        "build_object_detail_v2_payload",
        lambda *_a, **_k: (_ for _ in ()).throw(ValueError("bad detail")),
    )
    assert client.get("/api/dag/v2/object/persisted-object").status_code == 409
    monkeypatch.setattr(
        dag_v2,
        "build_graph_v2_payload",
        lambda *_a, **_k: (_ for _ in ()).throw(DagV2GraphContractError("bad graph")),
    )
    assert (
        client.get(
            "/api/dag/v2/data", params={"start_euid": "persisted-object"}
        ).status_code
        == 409
    )
    monkeypatch.setattr(
        dag_v2,
        "search_objects",
        lambda *_a, **_k: (_ for _ in ()).throw(ValueError("bad cursor")),
    )
    response = client.get("/api/dag/v2/search", params={"limit": 1})
    assert response.status_code == 422
    assert response.json()["detail"] == "bad cursor"


def test_branch_campaign_v2_mount_failure_paths_are_atomic(monkeypatch, tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("placeholder", encoding="utf-8")
    monkeypatch.setattr(dag_v2, "resolve_context", lambda **_kwargs: object())
    monkeypatch.setattr(
        dag_v2, "get_db_config", lambda **_kwargs: {"schema_name": "unit"}
    )
    app = FastAPI()

    invalid_identity = dag_v2.mount_tapdb_dag_surfaces(
        app,
        config_path=str(config),
        service_id="bad/service",
        display_name="Service",
        auth_dependency=lambda _request: {"sub": "operator"},
        limits=_manifest().limits,
    )
    assert (
        invalid_identity.reason
        is dag_v2.DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH
    )

    invalid_limits = dag_v2.mount_tapdb_dag_surfaces(
        app,
        config_path=str(config),
        service_id="local-service",
        display_name="Service",
        auth_dependency=lambda _request: {"sub": "operator"},
        limits=object(),
    )
    assert invalid_limits.reason is dag_v2.DagV2EligibilityReason.INVALID_LIMITS

    monkeypatch.setattr(
        dag_v2,
        "_build_router",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("router failed")),
    )
    unavailable = dag_v2.mount_tapdb_dag_surfaces(
        app,
        config_path=str(config),
        service_id="local-service",
        display_name="Service",
        auth_dependency=lambda _request: {"sub": "operator"},
        limits=_manifest().limits,
    )
    assert unavailable.reason is dag_v2.DagV2EligibilityReason.MOUNT_UNAVAILABLE


def test_branch_campaign_v2_mount_rejects_existing_route_atomically(
    monkeypatch, tmp_path
):
    config = tmp_path / "config.yaml"
    config.write_text("placeholder", encoding="utf-8")
    monkeypatch.setattr(dag_v2, "resolve_context", lambda **_kwargs: object())
    monkeypatch.setattr(
        dag_v2, "get_db_config", lambda **_kwargs: {"schema_name": "unit"}
    )
    app = FastAPI()

    @app.get("/api/dag/manifest")
    async def stale_manifest():
        return {"stale": True}

    route_count = len(app.router.routes)
    result = dag_v2.mount_tapdb_dag_surfaces(
        app,
        config_path=str(config),
        service_id="local-service",
        display_name="Local Service",
        auth_dependency=lambda: {"username": "tester"},
        limits=_manifest().limits,
    )

    assert result.reason is dag_v2.DagV2EligibilityReason.MOUNT_UNAVAILABLE
    assert "route collision" in str(result.diagnostic)
    assert len(app.router.routes) == route_count
    assert not hasattr(app.state, "tapdb_dag_v2_advertisement")


def test_branch_campaign_v2_include_failure_restores_routes(monkeypatch, tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("placeholder", encoding="utf-8")
    monkeypatch.setattr(dag_v2, "resolve_context", lambda **_kwargs: object())
    monkeypatch.setattr(
        dag_v2, "get_db_config", lambda **_kwargs: {"schema_name": "unit"}
    )
    app = FastAPI()
    app.state.tapdb_dag_v2_advertisement = {"stale": True}
    prior = list(app.router.routes)

    def _fail_include(_router):
        app.router.routes.append(object())
        raise RuntimeError("mount failed")

    monkeypatch.setattr(app, "include_router", _fail_include)
    result = dag_v2.mount_tapdb_dag_surfaces(
        app,
        config_path=str(config),
        service_id="local-service",
        display_name="Service",
        auth_dependency=lambda _request: {"sub": "operator"},
        limits=_manifest().limits,
    )
    assert result.reason is dag_v2.DagV2EligibilityReason.MOUNT_UNAVAILABLE
    assert app.router.routes == prior
    assert app.state.tapdb_dag_v2_advertisement == {"stale": True}
    assert not hasattr(app.state, "tapdb_dag_v2_mount")
    assert not hasattr(app.state, "tapdb_dag_v2_mount_fingerprint")


def test_branch_campaign_v2_state_assignment_failure_rolls_back_all_mount_state(
    monkeypatch, tmp_path
):
    config = tmp_path / "config.yaml"
    config.write_text("placeholder", encoding="utf-8")
    monkeypatch.setattr(dag_v2, "resolve_context", lambda **_kwargs: object())
    monkeypatch.setattr(
        dag_v2, "get_db_config", lambda **_kwargs: {"schema_name": "unit"}
    )
    app = FastAPI()
    prior = list(app.router.routes)

    class _FailingState:
        def __setattr__(self, name, value):
            if name == "tapdb_dag_v2_advertisement":
                raise RuntimeError("state assignment failed")
            object.__setattr__(self, name, value)

    app.state = _FailingState()
    result = dag_v2.mount_tapdb_dag_surfaces(
        app,
        config_path=str(config),
        service_id="local-service",
        display_name="Service",
        auth_dependency=lambda _request: {"sub": "operator"},
        limits=_manifest().limits,
    )
    assert result.reason is dag_v2.DagV2EligibilityReason.MOUNT_UNAVAILABLE
    assert app.router.routes == prior
    assert not hasattr(app.state, "tapdb_dag_v2_mount")
    assert not hasattr(app.state, "tapdb_dag_v2_advertisement")
    assert not hasattr(app.state, "tapdb_dag_v2_mount_fingerprint")


class _Session:
    def __init__(self, dialect_name):
        self.bind = SimpleNamespace(dialect=SimpleNamespace(name=dialect_name))
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))


def test_branch_campaign_runtime_context_helpers_are_postgres_specific():
    sqlite = _Session("sqlite")
    runtime._set_search_path(sqlite, "unit")
    runtime._set_identity_scope(sqlite, {})
    assert sqlite.calls == []

    postgres = _Session("postgresql")
    runtime._set_search_path(postgres, "unit")
    runtime._set_identity_scope(
        postgres, {"domain_code": "A", "owner_repo_name": "owner"}
    )
    assert len(postgres.calls) == 3
    with pytest.raises(RuntimeError, match="owner_repo_name"):
        runtime._set_identity_scope(postgres, {"domain_code": "A"})


def test_branch_campaign_runtime_connection_context_manager_returns_self():
    bundle = SimpleNamespace()
    conn = runtime.RuntimeDBConnection(bundle)
    with conn as entered:
        assert entered is conn
    assert conn.__exit__(None, None, None) is False


def test_branch_campaign_web_lazy_exports_and_unknown_attribute():
    import daylily_tapdb.web as web

    assert callable(web.__getattr__("create_tapdb_gui_app"))
    with pytest.raises(AttributeError):
        web.__getattr__("unknown_export")
