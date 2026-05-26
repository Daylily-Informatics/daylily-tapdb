"""Smoke coverage for admin FastAPI routes.

These tests intentionally use lightweight fakes to execute each route handler
at least once without requiring a real database or Cognito runtime.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient

import admin.auth as auth_mod
import admin.main as admin_main


class _FakeTemplateRender:
    def __init__(self, name: str, state: dict):
        self.name = name
        self._state = state

    def render(self, **_kwargs):
        self._state.setdefault("render_calls", []).append(
            {"template": self.name, "context": dict(_kwargs)}
        )
        return f"TEMPLATE:{self.name}"


class _FakeRelatedQuery:
    def __init__(self, items):
        self._items = list(items)

    def filter_by(self, **kwargs):
        items = [
            it
            for it in self._items
            if all(getattr(it, k, None) == v for k, v in kwargs.items())
        ]
        return _FakeRelatedQuery(items)

    def all(self):
        return list(self._items)

    def __iter__(self):
        return iter(self._items)


class _FakeQuery:
    def __init__(self, items):
        self._base = list(items)
        self._filtered = list(items)
        self._offset = 0
        self._limit = None

    def filter_by(self, **kwargs):
        self._filtered = [
            it
            for it in self._filtered
            if all(getattr(it, k, None) == v for k, v in kwargs.items())
        ]
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def distinct(self):
        seen = []
        uniq = []
        for row in self._filtered:
            if row not in seen:
                seen.append(row)
                uniq.append(row)
        self._filtered = uniq
        return self

    def count(self):
        return len(self._filtered)

    def offset(self, value: int):
        self._offset = value
        return self

    def limit(self, value: int):
        self._limit = value
        return self

    def _slice(self):
        rows = self._filtered[self._offset :]
        if self._limit is not None:
            rows = rows[: self._limit]
        return rows

    def all(self):
        return list(self._slice())

    def first(self):
        rows = self._slice()
        return rows[0] if rows else None


class _FakeSession:
    def __init__(self, state):
        self._state = state

    def query(self, model):
        if model is admin_main.generic_template:
            return _FakeQuery(self._state["templates"])
        if model is admin_main.generic_instance:
            return _FakeQuery(self._state["instances"])
        if model is admin_main.generic_instance_lineage:
            return _FakeQuery(self._state["lineages"])
        if model is admin_main.audit_log:
            return _FakeQuery(self._state["audit_rows"])
        if model is admin_main.generic_template.category:
            rows = [(t.category,) for t in self._state["templates"]]
            return _FakeQuery(rows)
        if model is admin_main.generic_instance.category:
            rows = [(i.category,) for i in self._state["instances"]]
            return _FakeQuery(rows)
        raise AssertionError(f"Unexpected query model: {model!r}")

    def add(self, obj):
        if getattr(obj, "euid", None) is None:
            obj.euid = f"TGX{len(self._state['lineages']) + 200}"
        if getattr(obj, "uid", None) is None:
            obj.uid = len(self._state["lineages"]) + 200
        obj.is_deleted = False
        obj.created_dt = datetime.now(timezone.utc)
        parent = next(
            (i for i in self._state["instances"] if i.uid == obj.parent_instance_uid),
            None,
        )
        child = next(
            (i for i in self._state["instances"] if i.uid == obj.child_instance_uid),
            None,
        )
        obj.parent_instance = parent
        obj.child_instance = child
        if parent:
            parent.parent_of_lineages = _FakeRelatedQuery(
                list(parent.parent_of_lineages.all()) + [obj]
            )
        if child:
            child.child_of_lineages = _FakeRelatedQuery(
                list(child.child_of_lineages.all()) + [obj]
            )
        self._state["lineages"].append(obj)

    def flush(self):
        return None


class _FakeConn:
    def __init__(self, state):
        self._state = state
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    @contextmanager
    def session_scope(self, commit=False):
        _ = commit
        yield _FakeSession(self._state)


@pytest.fixture
def route_client(monkeypatch: pytest.MonkeyPatch):
    now = datetime.now(timezone.utc)
    parent = SimpleNamespace(
        uid=11,
        euid="GX11",
        name="Parent",
        category="generic",
        type="generic",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={},
        created_dt=now,
        is_deleted=False,
        polymorphic_discriminator="generic_instance",
        parent_of_lineages=_FakeRelatedQuery([]),
        child_of_lineages=_FakeRelatedQuery([]),
    )
    child = SimpleNamespace(
        uid=12,
        euid="GX12",
        name="Child",
        category="generic",
        type="generic",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={},
        created_dt=now,
        is_deleted=False,
        polymorphic_discriminator="generic_instance",
        parent_of_lineages=_FakeRelatedQuery([]),
        child_of_lineages=_FakeRelatedQuery([]),
    )
    lineage = SimpleNamespace(
        uid=21,
        euid="GN21",
        name="Parent->Child:contains",
        category="lineage",
        type="lineage",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={},
        created_dt=now,
        is_deleted=False,
        relationship_type="contains",
        parent_instance_uid=parent.uid,
        child_instance_uid=child.uid,
        parent_type="generic_instance",
        child_type="generic_instance",
        parent_instance=parent,
        child_instance=child,
    )
    parent.parent_of_lineages = _FakeRelatedQuery([lineage])
    child.child_of_lineages = _FakeRelatedQuery([lineage])

    template = SimpleNamespace(
        uid=1,
        euid="GT1",
        name="Generic Template",
        category="generic",
        type="generic",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={"properties": {"x": 1}},
        created_dt=now,
        is_deleted=False,
    )
    system_user_template = SimpleNamespace(
        uid=2,
        euid="GT2",
        name="System User Actor",
        category="generic",
        type="actor",
        subtype="system_user",
        version="1.0",
        bstatus="active",
        json_addl={"properties": {}},
        created_dt=now,
        is_deleted=False,
    )

    state = {
        "templates": [template, system_user_template],
        "instances": [parent, child],
        "lineages": [lineage],
        "audit_rows": [
            SimpleNamespace(
                uid=501,
                euid="AD501",
                rel_table_name="generic_instance",
                column_name="name",
                rel_table_uid_fk=child.uid,
                rel_table_euid_fk=child.euid,
                old_value="Child",
                new_value="Child Updated",
                changed_by="admin",
                changed_at=now,
                operation_type="UPDATE",
                is_deleted=False,
            ),
            SimpleNamespace(
                uid=502,
                euid="AD502",
                rel_table_name="generic_instance",
                column_name="bstatus",
                rel_table_uid_fk=child.uid,
                rel_table_euid_fk=child.euid,
                old_value="active",
                new_value="archived",
                changed_by="other_user",
                changed_at=now,
                operation_type="UPDATE",
                is_deleted=False,
            ),
            SimpleNamespace(
                uid=503,
                euid="AD503",
                rel_table_name="generic_template",
                column_name="name",
                rel_table_uid_fk=template.uid,
                rel_table_euid_fk=template.euid,
                old_value="Generic Template",
                new_value="Generic Template v2",
                changed_by="john@example.com",
                changed_at=now,
                operation_type="INSERT",
                is_deleted=False,
            ),
        ],
        "render_calls": [],
    }

    monkeypatch.setattr(
        admin_main.templates,
        "get_template",
        lambda name: _FakeTemplateRender(name, state),
    )
    monkeypatch.setattr(
        admin_main, "get_style", lambda *_args, **_kwargs: {"skin_css": "x.css"}
    )
    monkeypatch.setattr(admin_main, "get_db", lambda: _FakeConn(state))
    monkeypatch.setattr(admin_main, "get_user_permissions", lambda _u: {"ok": True})
    monkeypatch.setattr(admin_main, "get_user_by_username", lambda _u: None)
    monkeypatch.setattr(admin_main, "update_last_login", lambda _u: None)
    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_: {"access_token": "tok"},
    )
    monkeypatch.setattr(
        admin_main,
        "create_cognito_user_account",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        admin_main,
        "get_or_create_user_from_email",
        lambda email, **_k: {
            "uid": 777,
            "username": email,
            "email": email,
            "role": "user",
            "require_password_change": False,
        },
    )
    monkeypatch.setattr(
        admin_main,
        "respond_to_new_password_challenge",
        lambda *_: {"access_token": "tok"},
    )
    monkeypatch.setattr(admin_main, "change_cognito_password", lambda *_: None)
    monkeypatch.setattr(
        admin_main,
        "load_db_metrics_context",
        lambda **_k: {
            "metrics_enabled": True,
            "metrics_message": "",
            "metrics_file": "/tmp/db_metrics.tsv",
            "period_start_utc": "2026-01-01T00:00:00+00:00",
            "limit": 5000,
            "dropped_count": 0,
            "summary": {
                "count": 2,
                "p50_ms": 1.0,
                "p95_ms": 2.0,
                "p99_ms": 2.5,
                "max_ms": 3.0,
                "last_seen": "2026-01-01T00:00:00+00:00",
                "slowest": [
                    {
                        "ts_utc": "2026-01-01T00:00:00+00:00",
                        "duration_ms": 3.0,
                        "ok": True,
                        "op": "SELECT",
                        "table_hint": "generic_instance_lineage",
                        "path": "/",
                        "method": "GET",
                        "username": "admin",
                        "rowcount": "1",
                        "error_type": "",
                    }
                ],
                "by_path": [{"path": "/", "count": 2, "p95_ms": 2.0, "max_ms": 3.0}],
                "by_table": [
                    {
                        "table_hint": "generic_instance_lineage",
                        "count": 2,
                        "p95_ms": 2.0,
                        "max_ms": 3.0,
                    }
                ],
            },
        },
    )
    monkeypatch.setattr(
        admin_main,
        "load_db_inventory_context",
        lambda: {
            "db_inventory_error": None,
            "db_inventory_db_name": "tapdb_dev_runtime",
            "db_inventory_schema_names": ["public", "app_ns"],
            "db_inventory_counts": {
                "schemas": 2,
                "tables": 3,
                "views": 1,
                "materialized_views": 1,
                "sequences": 2,
                "triggers": 4,
                "functions": 5,
                "indexes": 6,
            },
            "db_inventory_tables": [
                {"schema_name": "public", "table_name": "generic_template"},
                {"schema_name": "public", "table_name": "generic_instance"},
                {"schema_name": "app_ns", "table_name": "domain_table"},
            ],
            "db_inventory_views": [
                {"schema_name": "public", "view_name": "v_active_instances"}
            ],
            "db_inventory_materialized_views": [
                {
                    "schema_name": "public",
                    "materialized_view_name": "mv_instance_counts",
                }
            ],
            "db_inventory_sequences": [
                {"schema_name": "public", "sequence_name": "generic_template_seq"},
                {"schema_name": "public", "sequence_name": "gx_instance_seq"},
            ],
            "db_inventory_triggers": [
                {
                    "schema_name": "public",
                    "table_name": "generic_instance",
                    "trigger_name": "trigger_update_modified_dt_generic_instance",
                }
            ],
            "db_inventory_functions": [
                {
                    "schema_name": "public",
                    "function_signature": "set_generic_instance_euid()",
                }
            ],
            "db_inventory_indexes": [
                {
                    "schema_name": "public",
                    "table_name": "generic_instance",
                    "index_name": "idx_generic_instance_euid",
                }
            ],
        },
    )
    monkeypatch.setattr(
        admin_main,
        "get_db_config",
        lambda: {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "tapdb_dev_runtime",
            "username": "tapdb",
            "password": "secret",
        },
    )
    monkeypatch.setattr(admin_main, "get_config_path", lambda: "/tmp/tapdb-config.yaml")
    monkeypatch.setattr(
        admin_main,
        "resolve_tapdb_pool_config",
        lambda _env: SimpleNamespace(
            pool_id="pool-123",
            app_client_id="client-123",
            region="us-west-2",
            aws_profile="default",
            source_file="/tmp/tapdb-config.yaml",
            client_name="tapdb-admin",
            domain="example.auth.us-west-2.amazoncognito.com",
            callback_url="https://localhost:8916/auth/callback",
            logout_url="https://localhost:8916/login",
        ),
    )
    return TestClient(admin_main.app), state


def _admin_user():
    return {
        "uid": 1,
        "username": "admin",
        "email": "admin@example.com",
        "role": "admin",
        "require_password_change": False,
    }


def _last_render_context(state: dict, template_name: str) -> dict:
    for entry in reversed(state.get("render_calls", [])):
        if entry.get("template") == template_name:
            return entry.get("context") or {}
    raise AssertionError(f"No render call found for {template_name}")


def test_auth_and_public_routes(route_client, monkeypatch: pytest.MonkeyPatch):
    client, _state = route_client

    # Login page (not logged in)
    async def _anon_main(_request):
        return None

    monkeypatch.setattr(admin_main, "get_current_user", _anon_main)
    monkeypatch.setattr(auth_mod, "get_current_user", _anon_main)
    resp = client.get("/login")
    assert resp.status_code == 200
    resp = client.get("/help")
    assert resp.status_code == 200
    resp = client.get("/info", follow_redirects=False)
    assert resp.status_code == 302

    # Login submit error path
    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_: (_ for _ in ()).throw(ValueError("bad")),
    )
    resp = client.post("/login", data={"username": "u", "password": "p"})
    assert resp.status_code == 200
    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_: {"access_token": "tok"},
    )

    # Signup page + early validation path
    resp = client.get("/signup")
    assert resp.status_code == 200
    resp = client.post(
        "/signup",
        data={
            "email": "bad-email",
            "display_name": "X",
            "password": "Password1!",
            "confirm_password": "Password1!",
        },
    )
    assert resp.status_code == 200

    # Logout and change-password unauthenticated path
    resp = client.get("/logout", follow_redirects=False)
    assert resp.status_code == 302
    resp = client.get("/change-password", follow_redirects=False)
    assert resp.status_code == 302
    resp = client.post(
        "/change-password",
        data={
            "current_password": "x",
            "new_password": "Password1!",
            "confirm_password": "Password1!",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302


def test_protected_html_and_api_routes(route_client, monkeypatch: pytest.MonkeyPatch):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)

    # Protected HTML routes
    assert client.get("/").status_code == 200
    assert client.get("/templates").status_code == 200
    assert client.get("/instances").status_code == 200
    assert client.get("/lineages").status_code == 200
    assert client.get("/object/GX12").status_code == 200
    assert client.get("/graph").status_code == 200
    assert client.get("/query").status_code == 200
    assert client.get("/info").status_code == 200
    assert client.get("/admin/metrics").status_code == 200
    assert client.get("/create-instance/GT1").status_code == 200
    assert client.get("/create-instance/GT2").status_code == 403
    create_instance_resp = client.post(
        "/create-instance/GT1",
        data={
            "instance_name": "",
            "create_children": "false",
            "parent_euids": "",
            "child_euids": "",
            "relationship_type": "contains",
        },
    )
    assert create_instance_resp.status_code == 200
    assert "Instance name is required." not in create_instance_resp.text
    blocked_create_resp = client.post(
        "/create-instance/GT2",
        data={
            "instance_name": "blocked",
            "create_children": "false",
            "parent_euids": "",
            "child_euids": "",
            "relationship_type": "contains",
        },
    )
    assert blocked_create_resp.status_code == 403

    # API routes (public)
    assert client.get("/api/graph/data").status_code == 200
    assert client.get("/api/templates").status_code == 200
    assert client.get("/api/instances").status_code == 200
    assert client.get("/api/object/GT1").status_code == 200

    # API routes (admin protected)
    # Cover handler logic with missing required keys.
    resp = client.post("/api/lineage", json={})
    assert resp.status_code == 400

    # Soft-delete an existing object.
    resp = client.delete("/api/object/GT1")
    assert resp.status_code == 200
    assert state["templates"][0].is_deleted is True


def test_api_object_detail_includes_external_refs(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)
    state["instances"][0].json_addl = {
        "properties": {
            "external_payload": {
                "tapdb_graph": {
                    "system": "atlas",
                    "base_url": "https://atlas.local",
                    "root_euid": "AT-PAT-1",
                    "tenant_id": "atlas-tenant-1",
                    "graph_data_path": "/api/graph/data",
                    "object_detail_path_template": "/api/graph/object/{euid}",
                    "auth_mode": "none",
                }
            }
        }
    }

    resp = client.get("/api/object/GX11")
    assert resp.status_code == 200
    body = resp.json()
    assert body["external_refs"] == [
        {
            "label": "atlas:AT-PAT-1",
            "system": "atlas",
            "root_euid": "AT-PAT-1",
            "tenant_id": "atlas-tenant-1",
            "href": "https://atlas.local/api/graph/object/AT-PAT-1",
            "graph_expandable": True,
            "ref_index": 0,
        }
    ]


def test_api_external_graph_proxy_route(route_client, monkeypatch: pytest.MonkeyPatch):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)
    state["instances"][0].json_addl = {
        "properties": {
            "external_payload": {
                "tapdb_graph": {
                    "system": "atlas",
                    "base_url": "https://atlas.local",
                    "root_euid": "AT-PAT-1",
                    "tenant_id": "atlas-tenant-1",
                    "graph_data_path": "/api/graph/data",
                    "object_detail_path_template": "/api/graph/object/{euid}",
                    "auth_mode": "none",
                }
            }
        }
    }

    observed = {}

    def _fake_fetch_remote_graph(_request, ref, *, depth):
        observed["depth"] = depth
        observed["ref_system"] = ref.system
        return {"elements": {"nodes": [{"data": {"id": "AT-PAT-1"}}], "edges": []}}

    def _fake_namespace_external_graph(payload, *, ref, ref_index, source_euid):
        observed["source_euid"] = source_euid
        observed["ref_index"] = ref_index
        assert payload["elements"]["nodes"][0]["data"]["id"] == "AT-PAT-1"
        assert ref.system == "atlas"
        return {"elements": {"nodes": [], "edges": []}, "ok": True}

    monkeypatch.setattr(admin_main, "fetch_remote_graph", _fake_fetch_remote_graph)
    monkeypatch.setattr(
        admin_main, "namespace_external_graph", _fake_namespace_external_graph
    )

    resp = client.get(
        "/api/graph/external",
        params={"source_euid": "GX11", "ref_index": 0, "depth": 3},
    )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert observed == {
        "depth": 3,
        "ref_system": "atlas",
        "source_euid": "GX11",
        "ref_index": 0,
    }


def test_api_external_graph_object_proxy_route(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)
    state["instances"][0].json_addl = {
        "properties": {
            "external_payload": {
                "tapdb_graph": {
                    "system": "atlas",
                    "base_url": "https://atlas.local",
                    "root_euid": "AT-PAT-1",
                    "tenant_id": "atlas-tenant-1",
                    "graph_data_path": "/api/graph/data",
                    "object_detail_path_template": "/api/graph/object/{euid}",
                    "auth_mode": "none",
                }
            }
        }
    }

    observed = {}

    def _fake_fetch_remote_object_detail(_request, ref, *, euid):
        observed["ref_system"] = ref.system
        observed["euid"] = euid
        return {"euid": euid, "name": "External Node", "source": ref.system}

    monkeypatch.setattr(
        admin_main, "fetch_remote_object_detail", _fake_fetch_remote_object_detail
    )

    resp = client.get(
        "/api/graph/external/object",
        params={"source_euid": "GX11", "ref_index": 0, "euid": "AT-PAT-2"},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "euid": "AT-PAT-2",
        "name": "External Node",
        "source": "atlas",
    }
    assert observed == {"ref_system": "atlas", "euid": "AT-PAT-2"}


def test_home_query_and_audit_panels_admin(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)

    # Default load includes current admin user's audit activity.
    resp = client.get("/")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "index.html")
    assert ctx["audit_user_effective"] == "admin"
    assert ctx["user_audit_rows"]

    # Simple object query by text.
    resp = client.get("/?q=GX&scope=all")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "index.html")
    assert any(row["euid"] == "GX11" for row in ctx["object_results"])
    assert any(row["euid"] == "GX12" for row in ctx["object_results"])

    # Per-object audit trail by object EUID.
    resp = client.get("/?object_euid=GX12")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "index.html")
    assert ctx["object_audit_rows"]
    assert all(row.rel_table_euid_fk == "GX12" for row in ctx["object_audit_rows"])

    # Admin can inspect another user's trail.
    resp = client.get("/?audit_user=other_user")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "index.html")
    assert ctx["audit_user_effective"] == "other_user"
    assert ctx["audit_warning"] is None
    assert all(
        (row.changed_by or "").lower() == "other_user" for row in ctx["user_audit_rows"]
    )

    # Operation filter applies to user audit.
    resp = client.get("/?op=UPDATE")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "index.html")
    assert ctx["query_params"]["op"] == "UPDATE"
    assert all(
        (row.operation_type or "").upper() == "UPDATE" for row in ctx["user_audit_rows"]
    )

    # Limit is clamped server-side.
    resp = client.get("/?limit=9999")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "index.html")
    assert ctx["query_params"]["limit"] == 100
    assert len(ctx["user_audit_rows"]) <= 100


def test_complex_query_page_filters(route_client, monkeypatch: pytest.MonkeyPatch):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)

    resp = client.get("/query?kind=instance&euid_like=GX")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "complex_query.html")
    assert ctx["query_params"]["kind"] == "instance"
    assert ctx["should_run"] is True
    assert ctx["results"]
    assert all(row["kind"] == "instance" for row in ctx["results"])


def test_home_user_audit_forces_non_admin_to_self(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, state = route_client

    async def _user_auth(_request):
        return {
            "uid": 44,
            "username": "john@example.com",
            "email": "john@example.com",
            "role": "user",
            "require_password_change": False,
        }

    monkeypatch.setattr(auth_mod, "get_current_user", _user_auth)

    resp = client.get("/?audit_user=other_user")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "index.html")
    assert ctx["audit_user_effective"] == "john@example.com"
    assert isinstance(ctx["audit_warning"], str)
    assert "own user audit trail" in ctx["audit_warning"]
    assert all(
        (row.changed_by or "").lower() == "john@example.com"
        for row in ctx["user_audit_rows"]
    )


def test_info_page_includes_inventory_for_admin(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)

    resp = client.get("/info")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "info.html")
    assert ctx["db_inventory_visible"] is True
    assert ctx["db_inventory_db_name"] == "tapdb_dev_runtime"
    assert ctx["db_inventory_schema_names"] == ["public", "app_ns"]
    assert ctx["db_inventory_counts"]["tables"] == 3
    assert ctx["db_inventory_tables"]
    assert ctx["db_inventory_functions"]
    db_rows = dict(ctx["db_rows"])
    assert db_rows["runtime_database_name"] == "tapdb_dev_runtime"


def test_info_page_hides_inventory_for_non_admin(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, state = route_client

    async def _user_auth(_request):
        return {
            "uid": 44,
            "username": "john@example.com",
            "email": "john@example.com",
            "role": "user",
            "require_password_change": False,
        }

    monkeypatch.setattr(auth_mod, "get_current_user", _user_auth)

    resp = client.get("/info")
    assert resp.status_code == 200
    ctx = _last_render_context(state, "info.html")
    assert ctx["db_inventory_visible"] is False
    assert ctx["db_inventory_schema_names"] == []
    assert ctx["db_inventory_tables"] == []


def test_graph_page_includes_shared_viewer_controls(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)

    resp = client.get("/graph?start_euid=GX11&depth=3")
    assert resp.status_code == 200
    assert resp.text == "TEMPLATE:graph.html"
    ctx = _last_render_context(state, "graph.html")
    assert ctx["start_euid"] == "GX11"
    assert ctx["depth"] == 3

    text = (admin_main.TEMPLATES_DIR / "graph.html").read_text(encoding="utf-8")
    assert 'id="search-query"' in text
    assert 'id="find-euid"' in text
    assert 'id="transparency-slider"' in text
    assert 'id="distance-slider"' in text
    assert 'id="type-checkboxes"' in text
    assert 'id="subtype-buttons"' in text
    assert 'id="graph-save"' in text
    assert 'id="graph-mermaid-source"' in text
    assert "initGraphPage()" in text


def test_graph_javascript_includes_focus_filter_and_export_helpers(route_client):
    client, _state = route_client

    resp = client.get("/static/js/graph.js")
    assert resp.status_code == 200
    text = resp.text
    assert "function chooseFocusNode" in text
    assert "Math.floor(Math.random() * visibleNodes.length)" in text
    assert "function applyFiltersAndStyles" in text
    assert "function buildMermaidSource" in text
    assert "window.applySearch = applySearch;" in text


def test_static_assets_and_openapi_are_served(route_client):
    client, _state = route_client

    # Static mount should serve core UI assets.
    assert client.get("/static/css/style.css").status_code == 200
    assert client.get("/static/js/graph.js").status_code == 200

    # OpenAPI should be available for introspection/debugging.
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    data = resp.json()
    assert "/api/templates" in data.get("paths", {})


def test_oauth_login_redirect_and_callback_success(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, _state = route_client

    async def _anon(_request):
        return None

    monkeypatch.setattr(admin_main, "get_current_user", _anon)
    monkeypatch.setattr(
        admin_main,
        "_resolve_cognito_oauth_runtime",
        lambda _env: {
            "domain": "example.auth.us-east-1.amazoncognito.com",
            "callback_url": "https://localhost:8911/auth/callback",
            "client_id": "client123",
            "client_secret": "",
            "scope": "openid email profile",
        },
    )
    monkeypatch.setattr(
        admin_main,
        "_exchange_oauth_authorization_code",
        lambda _runtime, _code: {"access_token": "at123", "id_token": "id123"},
    )
    monkeypatch.setattr(
        admin_main,
        "_resolve_oauth_user_profile",
        lambda _env, _tokens, _runtime: {
            "email": "google.user@example.com",
            "display_name": "Google User",
        },
    )

    login_resp = client.get("/auth/login", follow_redirects=False)
    assert login_resp.status_code == 302
    location = login_resp.headers["location"]
    assert location.startswith(
        "https://example.auth.us-east-1.amazoncognito.com/oauth2/authorize"
    )
    query = parse_qs(urlsplit(location).query)
    assert query.get("identity_provider") == ["Google"]
    assert "state" in query
    state = query["state"][0]

    cb_resp = client.get(
        f"/auth/callback?code=abc123&state={state}",
        follow_redirects=False,
    )
    assert cb_resp.status_code == 302
    assert cb_resp.headers["location"] == "/"


def test_oauth_callback_invalid_state_shows_error(
    route_client, monkeypatch: pytest.MonkeyPatch
):
    client, _state = route_client

    async def _anon(_request):
        return None

    monkeypatch.setattr(admin_main, "get_current_user", _anon)
    monkeypatch.setattr(
        admin_main,
        "_resolve_cognito_oauth_runtime",
        lambda _env: {
            "domain": "example.auth.us-east-1.amazoncognito.com",
            "callback_url": "https://localhost:8911/auth/callback",
            "client_id": "client123",
            "client_secret": "",
            "scope": "openid email profile",
        },
    )

    _ = client.get("/auth/login", follow_redirects=False)
    bad_cb = client.get("/auth/callback?code=abc123&state=wrong")
    assert bad_cb.status_code == 200
    assert "TEMPLATE:login.html" in bad_cb.text
