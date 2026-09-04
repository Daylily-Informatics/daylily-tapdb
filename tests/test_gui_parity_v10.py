"""Feature-parity gate for the sole TapDB web implementation."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from starlette.middleware.sessions import SessionMiddleware

import daylily_tapdb.gui.auth_routes as auth_routes
import daylily_tapdb.gui.router as gui_router
from daylily_tapdb.audit import AuditEntry
from daylily_tapdb.gui.auth_routes import create_tapdb_gui_auth_router
from daylily_tapdb.gui.router import _build_templates, create_tapdb_gui_router
from daylily_tapdb.schema_inventory import TapdbSchemaInventory
from daylily_tapdb.web.dag_v2 import DagV2Limits, _build_router, _manifest_for

ROOT = Path(__file__).resolve().parents[1]


def _routes(router):
    return {
        (method, route.path)
        for route in router.routes
        for method in getattr(route, "methods", set()) - {"HEAD", "OPTIONS"}
    }


def test_canonical_gui_covers_every_still_valid_admin_capability():
    gui = create_tapdb_gui_router(config_path="/tmp/explicit-config.yaml")
    auth = create_tapdb_gui_auth_router(templates=_build_templates(None))
    dag = _build_router(
        config_path="/tmp/explicit-config.yaml",
        manifest=_manifest_for(
            service_id="tapdb",
            display_name="TapDB",
            limits=DagV2Limits(max_depth=8, max_nodes=1_000, max_search_page_size=100),
        ),
        auth_dependency=lambda _request: {"username": "operator"},
    )
    mounted = _routes(gui) | _routes(auth) | _routes(dag)
    capabilities = {
        "authentication": {
            ("GET", "/login"),
            ("POST", "/login"),
            ("GET", "/auth/login"),
            ("GET", "/auth/callback"),
            ("GET", "/signup"),
            ("POST", "/signup"),
            ("GET", "/logout"),
            ("GET", "/change-password"),
            ("POST", "/change-password"),
        },
        "overview_search_and_audit": {
            ("GET", "/"),
            ("GET", "/search"),
            ("GET", "/api/search"),
            ("GET", "/graph"),
            ("GET", "/api/graph"),
            ("GET", "/audit"),
            ("GET", "/api/audit"),
        },
        "templates": {
            ("GET", "/templates"),
            ("GET", "/templates/new"),
            ("POST", "/templates/save"),
            ("POST", "/api/templates/repository/import"),
            ("GET", "/api/templates/repository/download"),
        },
        "objects_lineage_and_repair": {
            ("GET", "/object/{euid}"),
            ("POST", "/api/create/{template_euid}"),
            ("PATCH", "/api/objects/{euid}"),
            ("DELETE", "/api/objects/{euid}"),
            ("POST", "/api/object/{euid}/lineage"),
            ("POST", "/api/object/{euid}/repairs"),
            ("POST", "/api/object/{euid}/revalidate"),
        },
        "operations": {
            ("GET", "/admin/readiness"),
            ("GET", "/admin/inventory"),
            ("GET", "/admin/meridian"),
            ("GET", "/admin/metrics"),
            ("GET", "/admin/runtime"),
            ("GET", "/admin/backups"),
            ("GET", "/api/admin/backups/health"),
            ("GET", "/api/admin/backups/plan"),
            ("POST", "/api/admin/backups"),
            ("POST", "/api/admin/backups/{ref}/verify"),
            ("POST", "/api/admin/backups/{ref}/restore/stage"),
            ("POST", "/api/admin/backups/{ref}/restore/apply"),
            ("POST", "/api/admin/backups/{ref}/rehearse"),
        },
        "dag_v2": {
            ("GET", "/api/dag/manifest"),
            ("GET", "/api/dag/v2/object/{euid}"),
            ("GET", "/api/dag/v2/data"),
            ("GET", "/api/dag/v2/search"),
        },
    }
    for name, expected in capabilities.items():
        assert expected <= mounted, f"canonical GUI lost {name}: {expected - mounted}"

    removed = {
        ("GET", "/api/graph/manifest"),
        ("GET", "/api/graph/data"),
        ("GET", "/api/external-graph"),
        ("GET", "/external-link/{euid}"),
        ("POST", "/external-link/{euid}"),
        ("POST", "/api/external-link/{euid}"),
        ("GET", "/api/backups"),
        ("POST", "/api/backups"),
    }
    assert removed.isdisjoint(mounted)


def test_canonical_graph_ui_retains_valid_admin_features_without_v1_proxying():
    template = (ROOT / "daylily_tapdb/gui/templates/graph.html").read_text()
    script = (ROOT / "daylily_tapdb/gui/static/js/tapdb-graph.js").read_text()
    combined = template + script

    for retained_feature in (
        'id="start-euid"',
        'id="depth"',
        'id="max-nodes"',
        'id="max-edges"',
        'id="search-query"',
        'id="find-euid"',
        'id="transparency-slider"',
        'id="distance-slider"',
        'id="type-checkboxes"',
        'id="subtype-buttons"',
        'value="dagre"',
        'value="cose"',
        'value="breadthfirst"',
        'value="circle"',
        'value="grid"',
        "runWaveFromNode",
        "runNeighborhoodFromNode",
        "createLineageEdge",
        "deleteGraphObject",
        'id="graph-save"',
        'id="graph-mermaid-source"',
        'id="node-info-content"',
        'id="tapdb-graph-payload"',
        "renderExternalRefs",
        "renderExternalIdentifiers",
    ):
        assert retained_feature in combined

    assert "window.TAPDB_GRAPH_CAN_MUTATE" in template
    assert "/api/graph" in script
    assert "/api/dag/v2/object/" in script
    assert "/api/object/${encodeURIComponent(childId)}/lineage" in script
    assert "/api/objects/${encodeURIComponent(objectId)}?apply=true" in script
    assert "window.confirm" in script
    assert not (ROOT / "admin/main.py").exists()
    assert not (ROOT / "daylily_tapdb/web/dag.py").exists()

    for removed_capability in (
        "TAPDB_GRAPH_BOOTSTRAP",
        "mergeExternalRef",
        "buildExternalGraphUrl",
        "fetchExternalObjectData",
        "/api/dag/data",
        "/api/dag/object/",
        "/api/dag/external",
        "graph_expandable",
        "root_euid",
        "is_external_bridge",
    ):
        assert removed_capability not in combined


class _GuiResult:
    def __init__(self, *, scalar=None, rows=()):
        self.scalar_value = scalar
        self.rows = list(rows)

    def scalar(self):
        return self.scalar_value

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)


class _GuiSession:
    def __init__(self, *, active_schema="tapdb_app"):
        self.active_schema = active_schema

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def query(self, _model):
        return _GuiQuery()

    def execute(self, statement, _params=None):
        sql = str(statement)
        if "current_database" in sql:
            return _GuiResult(scalar="tapdb_database")
        if "current_schema" in sql:
            return _GuiResult(scalar=self.active_schema)
        if "current_schemas" in sql:
            return _GuiResult(scalar=[self.active_schema, "public"])
        if "FROM pg_namespace" in sql:
            return _GuiResult(
                rows=[{"schema_name": "public"}, {"schema_name": self.active_schema}]
            )
        if "FROM pg_views" in sql:
            return _GuiResult(rows=[{"view_name": "active_objects"}])
        if "FROM pg_matviews" in sql:
            return _GuiResult(rows=[{"view_name": "object_rollup"}])
        raise AssertionError(f"Unexpected inventory query: {sql}")


class _GuiQuery:
    def filter_by(self, **_kwargs):
        return self

    def filter(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def limit(self, _value):
        return self

    def all(self):
        return []


class _GuiConnection:
    def __init__(self, session):
        self.session = session
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def session_scope(self, **_kwargs):
        return self.session


@pytest.fixture
def canonical_gui_client():
    app = FastAPI()
    app.include_router(create_tapdb_gui_router(config_path="/tmp/explicit-config.yaml"))

    async def admin_user(request: Request):
        user = {
            "username": "operator@example.test",
            "email": "operator@example.test",
            "role": "admin",
        }
        request.state.user = user
        return user

    app.dependency_overrides[gui_router.require_tapdb_gui_user] = admin_user
    app.dependency_overrides[gui_router.require_tapdb_gui_admin] = admin_user
    return TestClient(app, base_url="https://localhost")


def test_canonical_gui_operator_inventory_retains_admin_database_inspection(
    monkeypatch,
):
    live = TapdbSchemaInventory(schema_name="tapdb_app")
    live.add_column("generic_instance", "euid")
    live.add_sequence("euid_seq")
    live.add_function("set_euid()")
    live.add_trigger("generic_instance", "set_instance_euid")
    live.add_index("generic_instance", "generic_instance_euid_key")
    connection = _GuiConnection(_GuiSession())
    monkeypatch.setattr(gui_router, "get_db", lambda _path: connection)
    monkeypatch.setattr(
        "daylily_tapdb.schema_inventory.load_live_schema_inventory",
        lambda _session, *, schema_name: live,
    )

    payload = gui_router._inventory_payload(
        config_path="/tmp/explicit-config.yaml", username="operator@example.test"
    )

    assert connection.app_username == "operator@example.test"
    assert payload["database_name"] == "tapdb_database"
    assert payload["active_schema"] == "tapdb_app"
    assert payload["tables"] == ["generic_instance"]
    assert payload["columns"] == {"generic_instance": ["euid"]}
    assert payload["views"] == ["active_objects"]
    assert payload["materialized_views"] == ["object_rollup"]
    assert payload["sequences"] == ["euid_seq"]
    assert payload["functions"] == ["set_euid()"]
    assert payload["triggers"] == [
        {"table": "generic_instance", "name": "set_instance_euid"}
    ]
    assert payload["indexes"] == [
        {"table": "generic_instance", "name": "generic_instance_euid_key"}
    ]

    monkeypatch.setattr(
        gui_router,
        "get_db",
        lambda _path: _GuiConnection(_GuiSession(active_schema="")),
    )
    with pytest.raises(RuntimeError, match="schema is not configured"):
        gui_router._inventory_payload(
            config_path="/tmp/explicit-config.yaml", username="operator@example.test"
        )


def test_canonical_gui_audit_browser_preserves_role_scoping_and_filters(monkeypatch):
    connection = _GuiConnection(_GuiSession())
    monkeypatch.setattr(gui_router, "get_db", lambda _path: connection)
    captured = []
    entry = AuditEntry(
        euid="<persisted-object-euid>",
        changed_by="person@example.test",
        operation_type="UPDATE",
        changed_at=datetime(2026, 9, 4, 6, tzinfo=UTC),
        name="Persisted object",
        polymorphic_discriminator="generic_instance",
        category="message",
        type="webhook",
        subtype="event",
        bstatus="active",
        old_value=None,
        new_value='{"name":"Persisted object"}',
    )

    def query_audit(_session, **kwargs):
        captured.append(kwargs)
        return [entry]

    monkeypatch.setattr("daylily_tapdb.audit.query_audit_trail", query_audit)
    scoped = gui_router._audit_payload(
        config_path="/tmp/explicit-config.yaml",
        user={"username": "person@example.test", "role": "user"},
        euid=" <persisted-object-euid> ",
        changed_by="someone-else@example.test",
        operation_type=" update ",
        limit=25,
    )
    assert (
        scoped["warning"] == "Non-admin users can view only their own audit activity."
    )
    assert scoped["can_query_any_actor"] is False
    assert captured[-1] == {
        "changed_by": "person@example.test",
        "euid": "<persisted-object-euid>",
        "operation_type": "UPDATE",
        "limit": 25,
    }
    assert scoped["items"][0]["euid"] == "<persisted-object-euid>"

    unrestricted = gui_router._audit_payload(
        config_path="/tmp/explicit-config.yaml",
        user={"email": "admin@example.test", "role": "admin"},
        euid="",
        changed_by="person@example.test",
        operation_type="ALL",
        limit=50,
    )
    assert unrestricted["warning"] is None
    assert unrestricted["can_query_any_actor"] is True
    assert captured[-1]["changed_by"] == "person@example.test"
    assert captured[-1]["operation_type"] is None

    with pytest.raises(ValueError, match="operation_type must be"):
        gui_router._audit_payload(
            config_path="/tmp/explicit-config.yaml",
            user={"username": "admin@example.test", "role": "admin"},
            euid="",
            changed_by="",
            operation_type="UPSERT",
            limit=50,
        )


def test_canonical_overview_audit_help_and_inventory_routes_are_self_contained(
    canonical_gui_client, monkeypatch
):
    overview = {
        "counts": {"templates": 2, "instances": 3, "lineages": 4},
        "total": 9,
    }
    audit = {
        "items": [],
        "filters": {
            "euid": "",
            "changed_by": "",
            "operation_type": "ALL",
            "limit": 50,
        },
        "can_query_any_actor": True,
        "warning": None,
    }
    inventory = {
        "database_name": "tapdb_database",
        "active_schema": "tapdb_app",
        "search_path": ["tapdb_app"],
        "counts": {},
        "schemas": ["tapdb_app"],
        "tables": [],
        "columns": {},
        "views": [],
        "materialized_views": [],
        "sequences": [],
        "functions": [],
        "triggers": [],
        "indexes": [],
    }
    monkeypatch.setattr(gui_router, "_overview_payload", lambda **_kwargs: overview)
    monkeypatch.setattr(gui_router, "_audit_payload", lambda **_kwargs: audit)
    monkeypatch.setattr(gui_router, "_inventory_payload", lambda **_kwargs: inventory)

    assert "2" in canonical_gui_client.get("/").text
    assert canonical_gui_client.get("/admin/overview").status_code == 200
    assert canonical_gui_client.get("/api/admin/overview").json() == overview
    assert "Audit explorer" in canonical_gui_client.get("/audit").text
    assert canonical_gui_client.get("/api/audit").json() == audit
    assert "TapDB GUI guide" in canonical_gui_client.get("/help").text
    assert "tapdb_database" in canonical_gui_client.get("/admin/inventory").text
    assert canonical_gui_client.get("/api/admin/inventory").json() == inventory

    monkeypatch.setattr(
        gui_router,
        "_audit_payload",
        lambda **_kwargs: (_ for _ in ()).throw(ValueError("invalid operation")),
    )
    assert canonical_gui_client.get("/audit").status_code == 422
    assert canonical_gui_client.get("/api/audit").status_code == 422
    monkeypatch.setattr(
        gui_router,
        "_inventory_payload",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("inventory offline")),
    )
    assert "inventory offline" in canonical_gui_client.get("/admin/inventory").text
    unavailable = canonical_gui_client.get("/api/admin/inventory")
    assert unavailable.status_code == 503
    assert unavailable.json()["detail"] == "inventory offline"


def test_canonical_readiness_graph_and_repository_download_routes(
    canonical_gui_client, monkeypatch
):
    readiness = {
        "ready": True,
        "config_path": "/tmp/explicit-config.yaml",
        "client_id": "tapdb",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "public_domain_registry": {
            "repository": "example/registry",
            "version": "1.0",
            "index_url": "https://example.test/index.json",
        },
        "checks": [{"name": "config", "ok": True, "detail": "loaded"}],
    }
    monkeypatch.setattr(gui_router, "_readiness_payload", lambda **_kwargs: readiness)
    monkeypatch.setattr(
        gui_router,
        "get_db_config",
        lambda **_kwargs: {
            "client_id": "tapdb",
            "domain_code": "Z",
            "owner_repo_name": "daylily-tapdb",
        },
    )
    monkeypatch.setattr(
        gui_router,
        "get_db",
        lambda _path: _GuiConnection(_GuiSession()),
    )

    assert canonical_gui_client.get("/admin/readiness").status_code == 200
    assert canonical_gui_client.get("/api/admin/readiness").json() == readiness
    graph_page = canonical_gui_client.get("/graph")
    assert graph_page.status_code == 200
    assert 'data-testid="tapdb-graph"' in graph_page.text
    graph_payload = canonical_gui_client.get("/api/graph").json()
    assert graph_payload["elements"] == {"nodes": [], "edges": []}

    monkeypatch.setattr(
        gui_router,
        "repository_pack_bytes",
        lambda *_args, **_kwargs: b'{"templates":[]}',
    )
    download = canonical_gui_client.get("/api/templates/repository/download")
    assert download.status_code == 200
    assert download.content == b'{"templates":[]}'
    assert "attachment" in download.headers["content-disposition"]

    monkeypatch.setattr(
        gui_router,
        "repository_pack_bytes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(LookupError("missing")),
    )
    assert (
        canonical_gui_client.get("/api/templates/repository/download").status_code
        == 404
    )
    monkeypatch.setattr(
        gui_router,
        "repository_pack_bytes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("invalid")),
    )
    assert (
        canonical_gui_client.get("/api/templates/repository/download").status_code
        == 400
    )


def test_canonical_governed_update_and_delete_api_retain_admin_operations(
    canonical_gui_client, monkeypatch
):
    connection = _GuiConnection(_GuiSession())
    monkeypatch.setattr(gui_router, "get_db", lambda _path: connection)
    calls = []

    def update(_session, selector, changes, *, actor, dry_run):
        calls.append(("update", selector.euid, changes, actor, dry_run))
        return {"status": "updated", "euid": selector.euid, "dry_run": dry_run}

    def delete(_session, selector, *, actor, dry_run):
        calls.append(("delete", selector.euid, actor, dry_run))
        return {"status": "deleted", "euid": selector.euid, "dry_run": dry_run}

    monkeypatch.setattr(gui_router, "update_object", update)
    monkeypatch.setattr(gui_router, "soft_delete_object", delete)
    preview = canonical_gui_client.patch(
        "/api/objects/<persisted-object-euid>",
        json={"changes": {"name": "Revised"}},
    )
    assert preview.json()["dry_run"] is True
    applied = canonical_gui_client.patch(
        "/api/objects/<persisted-object-euid>",
        json={"changes": {"name": "Revised"}, "apply": True},
    )
    assert applied.json()["dry_run"] is False
    deleted = canonical_gui_client.delete(
        "/api/objects/<persisted-object-euid>?apply=true"
    )
    assert deleted.json()["status"] == "deleted"
    assert calls[-1] == (
        "delete",
        "<persisted-object-euid>",
        "operator@example.test",
        False,
    )
    malformed = canonical_gui_client.patch(
        "/api/objects/<persisted-object-euid>", json={"changes": []}
    )
    assert malformed.status_code == 400

    for error, status in (
        (LookupError("missing"), 404),
        (PermissionError("denied"), 403),
        (ValueError("invalid"), 400),
    ):
        monkeypatch.setattr(
            gui_router,
            "update_object",
            lambda *_args, _error=error, **_kwargs: (_ for _ in ()).throw(_error),
        )
        assert (
            canonical_gui_client.patch(
                "/api/objects/<persisted-object-euid>",
                json={"changes": {"name": "Revised"}},
            ).status_code
            == status
        )
        monkeypatch.setattr(
            gui_router,
            "soft_delete_object",
            lambda *_args, _error=error, **_kwargs: (_ for _ in ()).throw(_error),
        )
        assert (
            canonical_gui_client.delete(
                "/api/objects/<persisted-object-euid>"
            ).status_code
            == status
        )


def test_canonical_gui_rejects_unknown_mutation_and_invalid_search_template_inputs(
    canonical_gui_client,
):
    with pytest.raises(HTTPException) as raised:
        gui_router._reject_unknown_payload_fields({"unexpected": True}, allowed=set())
    assert getattr(raised.value, "status_code", None) == 400
    assert "unexpected" in str(getattr(raised.value, "detail", ""))

    invalid_search = canonical_gui_client.get("/search?record_type=unknown")
    assert invalid_search.status_code == 400
    invalid_template = canonical_gui_client.post("/api/templates/validate", json={})
    assert invalid_template.status_code == 200
    assert invalid_template.json()["valid"] is False
    assert "non-empty array" in invalid_template.json()["issues"][0]["message"]


def test_canonical_search_retains_combined_advanced_filters_and_forward_pagination(
    canonical_gui_client, monkeypatch
):
    monkeypatch.setattr(
        gui_router,
        "get_db_config",
        lambda **_kwargs: {"client_id": "tapdb"},
    )
    monkeypatch.setattr(
        gui_router,
        "get_db",
        lambda _path: _GuiConnection(_GuiSession()),
    )
    captured = []

    def search(_session, **kwargs):
        captured.append(kwargs)
        return {
            "items": [],
            "page": {"limit": 25, "returned": 0, "next_cursor": "next-cursor"},
            "filters": kwargs,
        }

    monkeypatch.setattr(gui_router, "search_objects", search)
    response = canonical_gui_client.get(
        "/search?record_type=instance&name_like=Sample&euid_like=ABC&category=data"
    )
    assert response.status_code == 200
    assert 'name="name_like" value="Sample"' in response.text
    assert 'name="euid_like" value="ABC"' in response.text
    assert "Next page" in response.text
    assert "cursor=next-cursor" in response.text
    assert captured[-1]["name_like"] == "Sample"
    assert captured[-1]["euid_like"] == "ABC"

    api = canonical_gui_client.get(
        "/api/search?record_type=instance&name_like=Sample&euid_like=ABC"
    )
    assert api.status_code == 200
    assert captured[-1]["name_like"] == "Sample"
    assert captured[-1]["euid_like"] == "ABC"


@pytest.fixture
def client(monkeypatch):
    async def no_user(_request):
        return None

    monkeypatch.setattr(auth_routes, "get_current_user", no_user)
    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-session-secret")
    app.include_router(create_tapdb_gui_auth_router(templates=_build_templates(None)))
    return TestClient(app, base_url="https://localhost")


def test_login_provisions_a_user_and_preserves_cognito_session(client, monkeypatch):
    monkeypatch.setattr(auth_routes, "get_user_by_username", lambda _value: None)
    monkeypatch.setattr(
        auth_routes,
        "authenticate_with_cognito",
        lambda username, password: {
            "access_token": f"token-for-{username}-{len(password)}"
        },
    )
    monkeypatch.setattr(
        auth_routes,
        "get_or_create_user_from_email",
        lambda email, **_kwargs: {"uid": 17, "email": email, "role": "user"},
    )
    updated = []
    monkeypatch.setattr(auth_routes, "update_last_login", updated.append)

    response = client.post(
        "/login",
        data={"username": "person@example.com", "password": "correct-horse"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["location"] == "/"
    assert updated == [17]


def test_new_password_challenge_and_signup_validation_are_retained(client, monkeypatch):
    monkeypatch.setattr(
        auth_routes,
        "get_user_by_username",
        lambda _value: {
            "uid": 18,
            "email": "person@example.com",
            "role": "user",
        },
    )
    monkeypatch.setattr(
        auth_routes,
        "authenticate_with_cognito",
        lambda *_args: {"challenge": "NEW_PASSWORD_REQUIRED", "session": "opaque"},
    )
    monkeypatch.setattr(auth_routes, "update_last_login", lambda _uid: None)
    response = client.post(
        "/login",
        data={"username": "person@example.com", "password": "temporary-pass"},
        follow_redirects=False,
    )
    assert response.headers["location"] == "/change-password"

    invalid = client.post(
        "/signup",
        data={
            "email": "invalid",
            "password": "long-enough",
            "confirm_password": "long-enough",
        },
    )
    assert invalid.status_code == 200
    assert "Valid email is required" in invalid.text


def test_oauth_state_is_exact_and_post_auth_redirect_cannot_leave_tapdb(
    client, monkeypatch
):
    runtime = {
        "domain": "pool.auth.example",
        "callback_url": "https://tapdb.example/auth/callback",
        "client_id": "client-id",
        "client_secret": "",
        "scope": "openid email profile",
    }
    monkeypatch.setattr(auth_routes, "_resolve_cognito_oauth_runtime", lambda: runtime)
    begin = client.get(
        "/auth/login", params={"next": "https://evil.example"}, follow_redirects=False
    )
    state = parse_qs(urlsplit(begin.headers["location"]).query)["state"][0]
    invalid = client.get(
        "/auth/callback", params={"state": f"{state}x", "code": "code"}
    )
    assert "invalid state" in invalid.text

    begin = client.get(
        "/auth/login", params={"next": "//evil.example"}, follow_redirects=False
    )
    state = parse_qs(urlsplit(begin.headers["location"]).query)["state"][0]
    monkeypatch.setattr(
        auth_routes,
        "_exchange_oauth_authorization_code",
        lambda *_args: {"access_token": "opaque-token"},
    )
    monkeypatch.setattr(
        auth_routes,
        "_resolve_oauth_user_profile",
        lambda *_args: {"email": "person@example.com", "display_name": "Person"},
    )
    monkeypatch.setattr(
        auth_routes,
        "get_or_create_user_from_email",
        lambda *_args, **_kwargs: {"uid": 19, "email": "person@example.com"},
    )
    monkeypatch.setattr(auth_routes, "update_last_login", lambda _uid: None)
    callback = client.get(
        "/auth/callback",
        params={"state": state, "code": "code"},
        follow_redirects=False,
    )
    assert callback.status_code == 302
    assert callback.headers["location"] == "/"


def test_logout_clears_the_session(client):
    response = client.get("/logout", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == "/login"


def test_safe_next_path_respects_an_embedded_root_path():
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/auth/login",
            "root_path": "/tapdb",
            "headers": [],
            "query_string": b"",
            "scheme": "https",
            "server": ("localhost", 443),
        }
    )
    assert auth_routes._safe_next_path(request, "/tapdb/search") == "/tapdb/search"
    assert auth_routes._safe_next_path(request, "https://evil.example") == "/tapdb/"
    assert auth_routes._safe_next_path(request, "/search") == "/tapdb/search"


class _UrlResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


def test_canonical_auth_helpers_cover_success_and_fail_closed_paths(monkeypatch):
    assert (
        auth_routes._require_https_url("https://auth.example/path", label="endpoint")
        == "https://auth.example/path"
    )
    with pytest.raises(RuntimeError, match="https URL"):
        auth_routes._require_https_url("http://auth.example", label="endpoint")
    with pytest.raises(RuntimeError, match="not configured"):
        auth_routes._normalize_cognito_domain("")
    for invalid in ("https://pool.example", "pool.example/path", "pool example"):
        with pytest.raises(RuntimeError, match="Invalid cognito_domain"):
            auth_routes._normalize_cognito_domain(invalid)
    assert auth_routes._normalize_cognito_domain(" pool.example ") == "pool.example"

    pool = SimpleNamespace(
        domain="pool.example",
        callback_url="",
        app_client_id="client-id",
        app_client_secret="secret",
    )
    monkeypatch.setattr(auth_routes, "resolve_tapdb_pool_config", lambda: pool)
    with pytest.raises(RuntimeError, match="callback_url"):
        auth_routes._resolve_cognito_oauth_runtime()
    pool.callback_url = "https://service.example/auth/callback"
    pool.app_client_id = ""
    with pytest.raises(RuntimeError, match="app_client_id"):
        auth_routes._resolve_cognito_oauth_runtime()
    pool.app_client_id = "client-id"
    runtime = auth_routes._resolve_cognito_oauth_runtime()
    assert runtime["client_secret"] == "secret"
    authorize = auth_routes._build_cognito_authorize_url(runtime, "state-token")
    assert parse_qs(urlsplit(authorize).query)["state"] == ["state-token"]

    seen = []

    def success(request, timeout):
        seen.append((request, timeout))
        return _UrlResponse(b'{"access_token":"opaque-access"}')

    monkeypatch.setattr(auth_routes, "urlopen", success)
    assert auth_routes._exchange_oauth_authorization_code(runtime, "code") == {
        "access_token": "opaque-access"
    }
    assert seen[0][0].get_header("Authorization").startswith("Basic ")

    public_runtime = {**runtime, "client_secret": ""}
    seen.clear()
    assert auth_routes._exchange_oauth_authorization_code(public_runtime, "code")
    assert seen[0][0].get_header("Authorization") is None
    assert b"client_id=client-id" in seen[0][0].data

    monkeypatch.setattr(
        auth_routes,
        "urlopen",
        lambda *_args, **_kwargs: _UrlResponse(b"not-json"),
    )
    with pytest.raises(RuntimeError, match="not valid JSON"):
        auth_routes._exchange_oauth_authorization_code(runtime, "code")
    monkeypatch.setattr(
        auth_routes,
        "urlopen",
        lambda *_args, **_kwargs: _UrlResponse(b"[]"),
    )
    with pytest.raises(RuntimeError, match="not an object"):
        auth_routes._exchange_oauth_authorization_code(runtime, "code")
    monkeypatch.setattr(
        auth_routes,
        "urlopen",
        lambda *_args, **_kwargs: _UrlResponse(
            b'{"error":"invalid_grant","error_description":"expired"}'
        ),
    )
    with pytest.raises(RuntimeError, match="expired"):
        auth_routes._exchange_oauth_authorization_code(runtime, "code")

    http_error = HTTPError(
        "https://pool.example/oauth2/token",
        400,
        "bad request",
        hdrs=None,
        fp=io.BytesIO(b"provider detail"),
    )
    monkeypatch.setattr(
        auth_routes,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(http_error),
    )
    with pytest.raises(RuntimeError, match="provider detail"):
        auth_routes._exchange_oauth_authorization_code(runtime, "code")
    unreadable_error = HTTPError(
        "https://pool.example/oauth2/token",
        400,
        "bad request",
        hdrs=None,
        fp=None,
    )
    unreadable_error.read = lambda: (_ for _ in ()).throw(OSError("unreadable"))
    monkeypatch.setattr(
        auth_routes,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(unreadable_error),
    )
    with pytest.raises(RuntimeError, match="bad request"):
        auth_routes._exchange_oauth_authorization_code(runtime, "code")
    monkeypatch.setattr(
        auth_routes,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("offline")),
    )
    with pytest.raises(RuntimeError, match="unreachable"):
        auth_routes._exchange_oauth_authorization_code(runtime, "code")


def test_canonical_oauth_claim_resolution_is_exact(monkeypatch, caplog):
    runtime = {
        "domain": "pool.example",
        "callback_url": "https://service.example/auth/callback",
        "client_id": "client-id",
        "client_secret": "",
        "scope": "openid email profile",
    }
    original_fetch = auth_routes._fetch_oauth_userinfo
    monkeypatch.setattr(
        auth_routes,
        "urlopen",
        lambda *_args, **_kwargs: _UrlResponse(
            json.dumps({"email": "PERSON@EXAMPLE.COM", "name": "Person"}).encode()
        ),
    )
    assert auth_routes._fetch_oauth_userinfo(runtime, "opaque-token")["email"] == (
        "PERSON@EXAMPLE.COM"
    )
    profile = auth_routes._resolve_oauth_user_profile(
        {"access_token": "opaque-token"}, runtime
    )
    assert profile == {"email": "person@example.com", "display_name": "Person"}

    monkeypatch.setattr(
        auth_routes,
        "_fetch_oauth_userinfo",
        lambda *_args: (_ for _ in ()).throw(OSError("offline")),
    )
    verifier = SimpleNamespace(
        verify_token=lambda _token: {
            "cognito:username": "FALLBACK@EXAMPLE.COM",
            "preferred_username": "Fallback",
        }
    )
    monkeypatch.setattr(auth_routes, "get_cognito_auth", lambda: verifier)
    assert auth_routes._resolve_oauth_user_profile(
        {"access_token": "bad", "id_token": "opaque-id"}, runtime
    ) == {"email": "fallback@example.com", "display_name": "Fallback"}

    verifier.verify_token = lambda _token: (_ for _ in ()).throw(ValueError("bad"))
    with pytest.raises(RuntimeError, match="no email"):
        auth_routes._resolve_oauth_user_profile({"id_token": "opaque-id"}, runtime)
    assert "Failed to verify Cognito" in caplog.text

    monkeypatch.setattr(
        auth_routes,
        "urlopen",
        lambda *_args, **_kwargs: _UrlResponse(b"[]"),
    )
    monkeypatch.setattr(auth_routes, "_fetch_oauth_userinfo", original_fetch)
    with pytest.raises(RuntimeError, match="not an object"):
        auth_routes._fetch_oauth_userinfo(runtime, "opaque-token")


def test_auth_pages_and_oauth_failure_decisions(client, monkeypatch):
    user = {
        "uid": 21,
        "email": "person@example.com",
        "role": "user",
        "require_password_change": False,
    }

    async def current(_request):
        return user

    monkeypatch.setattr(auth_routes, "get_current_user", current)
    assert client.get("/auth/login", follow_redirects=False).headers["location"] == "/"
    assert client.get("/login", follow_redirects=False).headers["location"] == "/"
    assert client.get("/signup", follow_redirects=False).headers["location"] == "/"
    user["require_password_change"] = True
    assert client.get("/login", follow_redirects=False).headers["location"] == (
        "/change-password"
    )

    async def anonymous(_request):
        return None

    monkeypatch.setattr(auth_routes, "get_current_user", anonymous)
    assert client.get("/login").status_code == 200
    assert client.get("/signup").status_code == 200
    monkeypatch.setattr(
        auth_routes,
        "_resolve_cognito_oauth_runtime",
        lambda: (_ for _ in ()).throw(RuntimeError("missing config")),
    )
    assert "not configured" in client.get("/auth/login").text
    assert (
        "provider denied"
        in client.get(
            "/auth/callback",
            params={"error": "access_denied", "error_description": "provider denied"},
        ).text
    )

    runtime = {
        "domain": "pool.example",
        "callback_url": "https://service.example/auth/callback",
        "client_id": "client-id",
        "client_secret": "",
        "scope": "openid email profile",
    }
    monkeypatch.setattr(auth_routes, "_resolve_cognito_oauth_runtime", lambda: runtime)
    begin = client.get("/auth/login", follow_redirects=False)
    state = parse_qs(urlsplit(begin.headers["location"]).query)["state"][0]
    assert (
        "missing authorization code"
        in client.get("/auth/callback", params={"state": state}).text
    )

    begin = client.get("/auth/login", follow_redirects=False)
    state = parse_qs(urlsplit(begin.headers["location"]).query)["state"][0]
    monkeypatch.setattr(
        auth_routes,
        "_exchange_oauth_authorization_code",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("exchange refused")),
    )
    assert (
        "exchange refused"
        in client.get(
            "/auth/callback", params={"state": state, "code": "opaque-code"}
        ).text
    )


def test_login_signup_and_password_failure_surfaces(client, monkeypatch):
    monkeypatch.setattr(auth_routes, "get_user_by_username", lambda _value: None)
    monkeypatch.setattr(
        auth_routes,
        "authenticate_with_cognito",
        lambda *_args: (_ for _ in ()).throw(ValueError("bad credentials")),
    )
    assert (
        "Invalid username or password"
        in client.post(
            "/login", data={"username": "person@example.com", "password": "bad"}
        ).text
    )
    monkeypatch.setattr(
        auth_routes,
        "authenticate_with_cognito",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("provider offline")),
    )
    assert (
        "provider offline"
        in client.post(
            "/login", data={"username": "person@example.com", "password": "bad"}
        ).text
    )

    monkeypatch.setattr(
        auth_routes,
        "authenticate_with_cognito",
        lambda *_args: {"access_token": "opaque-access"},
    )
    monkeypatch.setattr(
        auth_routes,
        "get_or_create_user_from_email",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("store offline")),
    )
    assert (
        "failed to provision"
        in client.post(
            "/login", data={"username": "person@example.com", "password": "valid-pass"}
        ).text
    )

    user = {
        "uid": 22,
        "email": "person@example.com",
        "role": "user",
        "require_password_change": False,
    }
    monkeypatch.setattr(auth_routes, "get_user_by_username", lambda _value: user)
    monkeypatch.setattr(auth_routes, "update_last_login", lambda _uid: None)
    monkeypatch.setattr(auth_routes, "authenticate_with_cognito", lambda *_args: {})
    assert (
        "no access token"
        in client.post(
            "/login", data={"username": "person@example.com", "password": "valid-pass"}
        ).text
    )

    base_signup = {
        "email": "person@example.com",
        "display_name": "Person",
        "password": "long-enough",
        "confirm_password": "long-enough",
    }
    assert (
        "at least 8"
        in client.post(
            "/signup",
            data={**base_signup, "password": "short", "confirm_password": "short"},
        ).text
    )
    assert (
        "do not match"
        in client.post(
            "/signup", data={**base_signup, "confirm_password": "different-pass"}
        ).text
    )
    monkeypatch.setattr(
        auth_routes,
        "create_cognito_user_account",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("signup offline")),
    )
    assert "signup offline" in client.post("/signup", data=base_signup).text


def test_signup_and_password_challenge_and_regular_success_paths(client, monkeypatch):
    user = {
        "uid": 23,
        "email": "person@example.com",
        "role": "user",
        "require_password_change": False,
    }
    monkeypatch.setattr(
        auth_routes, "create_cognito_user_account", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        auth_routes, "get_or_create_user_from_email", lambda *_a, **_k: user
    )
    monkeypatch.setattr(auth_routes, "update_last_login", lambda _uid: None)
    monkeypatch.setattr(
        auth_routes,
        "authenticate_with_cognito",
        lambda *_args: {"challenge": "NEW_PASSWORD_REQUIRED", "session": "opaque"},
    )
    response = client.post(
        "/signup",
        data={
            "email": "PERSON@example.com",
            "display_name": "Person",
            "password": "long-enough",
            "confirm_password": "long-enough",
        },
        follow_redirects=False,
    )
    assert response.headers["location"] == "/change-password"

    async def current(_request):
        return user

    monkeypatch.setattr(auth_routes, "get_current_user", current)
    assert "change password" in client.get("/change-password").text.lower()
    assert (
        "at least 8"
        in client.post(
            "/change-password",
            data={"new_password": "short", "confirm_password": "short"},
        ).text
    )
    assert (
        "do not match"
        in client.post(
            "/change-password",
            data={"new_password": "long-enough", "confirm_password": "different-pass"},
        ).text
    )
    monkeypatch.setattr(
        auth_routes,
        "respond_to_new_password_challenge",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("challenge refused")),
    )
    assert (
        "challenge refused"
        in client.post(
            "/change-password",
            data={"new_password": "long-enough", "confirm_password": "long-enough"},
        ).text
    )
    monkeypatch.setattr(
        auth_routes,
        "respond_to_new_password_challenge",
        lambda *_args: {"access_token": "new-access"},
    )
    assert (
        client.post(
            "/change-password",
            data={"new_password": "long-enough", "confirm_password": "long-enough"},
            follow_redirects=False,
        ).headers["location"]
        == "/"
    )

    client.get("/logout")
    monkeypatch.setattr(
        auth_routes,
        "authenticate_with_cognito",
        lambda *_args: {"access_token": "signup-access"},
    )
    assert (
        client.post(
            "/signup",
            data={
                "email": "person@example.com",
                "display_name": "Person",
                "password": "long-enough",
                "confirm_password": "long-enough",
            },
            follow_redirects=False,
        ).headers["location"]
        == "/"
    )
    client.get("/logout")
    monkeypatch.setattr(auth_routes, "get_user_by_username", lambda _value: user)
    monkeypatch.setattr(
        auth_routes,
        "authenticate_with_cognito",
        lambda *_args: {"access_token": "regular-access"},
    )
    client.post(
        "/login",
        data={"username": "person@example.com", "password": "long-enough"},
    )
    assert (
        "Current password is required"
        in client.post(
            "/change-password",
            data={"new_password": "long-enough", "confirm_password": "long-enough"},
        ).text
    )
    monkeypatch.setattr(auth_routes, "change_cognito_password", lambda *_args: None)
    response = client.post(
        "/change-password",
        data={
            "current_password": "old-password",
            "new_password": "long-enough",
            "confirm_password": "long-enough",
        },
    )
    assert "Password changed successfully" in response.text
