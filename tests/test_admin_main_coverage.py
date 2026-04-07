from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient

import admin.auth as auth_mod
import admin.main as admin_main


class _FakeTemplateRender:
    def __init__(self, name: str, state: dict):
        self.name = name
        self._state = state

    def render(self, **kwargs):
        self._state.setdefault("render_calls", []).append(
            {"template": self.name, "context": dict(kwargs)}
        )
        return f"TEMPLATE:{self.name}"


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
            item
            for item in self._filtered
            if all(getattr(item, key, None) == value for key, value in kwargs.items())
        ]
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def distinct(self):
        seen = []
        rows = []
        for item in self._filtered:
            if item not in seen:
                seen.append(item)
                rows.append(item)
        self._filtered = rows
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


class _InventoryMappings:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self._rows)


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
            return _FakeQuery([(t.category,) for t in self._state["templates"]])
        if model is admin_main.generic_instance.category:
            return _FakeQuery([(i.category,) for i in self._state["instances"]])
        raise AssertionError(f"Unexpected query model: {model!r}")

    def add(self, obj):
        if getattr(obj, "euid", None) is None:
            obj.euid = f"LG{len(self._state['lineages']) + 200}"
        if getattr(obj, "uid", None) is None:
            obj.uid = len(self._state["lineages"]) + 200
        obj.is_deleted = False
        obj.created_dt = datetime.now(timezone.utc)
        parent = next(
            (
                item
                for item in self._state["instances"]
                if item.uid == obj.parent_instance_uid
            ),
            None,
        )
        child = next(
            (
                item
                for item in self._state["instances"]
                if item.uid == obj.child_instance_uid
            ),
            None,
        )
        if not hasattr(obj, "_sa_instance_state"):
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


def _admin_user():
    return {
        "uid": 1,
        "username": "admin@example.com",
        "email": "admin@example.com",
        "role": "admin",
        "require_password_change": False,
    }


def _last_render_context(state: dict, template_name: str) -> dict:
    for item in reversed(state.get("render_calls", [])):
        if item.get("template") == template_name:
            return item["context"]
    raise AssertionError(f"Template {template_name!r} was not rendered")


def _request(*, path: str = "/", root_path: str = "", session=None, user=None):
    return SimpleNamespace(
        session={} if session is None else session,
        scope={"root_path": root_path, "path": path},
        state=SimpleNamespace(user=user),
        app=SimpleNamespace(state=SimpleNamespace(tapdb_host_bridge=None)),
    )


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
        euid="LG21",
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
    reserved_template = SimpleNamespace(
        uid=2,
        euid="GT2",
        name="System User Template",
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
        "templates": [template, reserved_template],
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
                changed_by="admin@example.com",
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
    monkeypatch.setattr(
        admin_main, "get_user_permissions", lambda _user: {"can_manage_users": True}
    )
    monkeypatch.setattr(admin_main, "get_user_by_username", lambda _user: None)
    monkeypatch.setattr(admin_main, "update_last_login", lambda _uid: None)
    monkeypatch.setattr(
        admin_main, "authenticate_with_cognito", lambda *_args: {"access_token": "tok"}
    )
    monkeypatch.setattr(
        admin_main, "create_cognito_user_account", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        admin_main,
        "get_or_create_user_from_email",
        lambda email, **_kwargs: {
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
        lambda *_args: {"access_token": "tok"},
    )
    monkeypatch.setattr(admin_main, "change_cognito_password", lambda *_args: None)
    monkeypatch.setattr(
        admin_main,
        "load_db_metrics_context",
        lambda **_kwargs: {
            "metrics_enabled": True,
            "metrics_message": "",
            "metrics_file": "/tmp/db_metrics.tsv",
            "period_start_utc": "2026-01-01T00:00:00+00:00",
            "limit": 5000,
            "dropped_count": 0,
            "summary": {
                "count": 2,
                "p95_ms": 2.0,
                "max_ms": 3.0,
                "slowest": [],
                "by_path": [],
                "by_table": [],
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
                {"schema_name": "public", "table_name": "generic_template"}
            ],
            "db_inventory_views": [
                {"schema_name": "public", "view_name": "v_instances"}
            ],
            "db_inventory_materialized_views": [
                {"schema_name": "public", "materialized_view_name": "mv_instances"}
            ],
            "db_inventory_sequences": [
                {"schema_name": "public", "sequence_name": "gx_instance_seq"}
            ],
            "db_inventory_triggers": [
                {
                    "schema_name": "public",
                    "table_name": "generic_instance",
                    "trigger_name": "trigger_update",
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
        "get_db_config_for_env",
        lambda _env: {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "tapdb_dev_runtime",
            "username": "tapdb",
            "password": "secret",
        },
    )
    monkeypatch.setattr(admin_main, "get_config_path", lambda: "/tmp/tapdb-config.yaml")
    monkeypatch.setattr(admin_main, "_active_tapdb_env", lambda: "dev")
    monkeypatch.setattr(
        admin_main,
        "resolve_tapdb_pool_config",
        lambda _env: SimpleNamespace(
            pool_id="pool-123",
            app_client_id="client-123",
            app_client_secret="",
            region="us-west-2",
            aws_profile="default",
            source_file="/tmp/tapdb-config.yaml",
            client_name="tapdb-admin",
            domain="example.auth.us-west-2.amazoncognito.com",
            callback_url="https://localhost:8911/auth/callback",
            logout_url="https://localhost:8911/login",
        ),
    )
    monkeypatch.setattr(
        auth_mod,
        "_admin_settings",
        lambda: {
            "auth_mode": "tapdb",
            "shared_host_session_secret": "secret-a",
            "shared_host_session_cookie": "session",
            "shared_host_session_max_age_seconds": 1209600,
        },
    )
    monkeypatch.setattr(auth_mod, "get_db", lambda: _FakeConn(state))
    return TestClient(admin_main.app), state


def test_main_query_helpers_and_footer_metadata(route_client, monkeypatch):
    _client, state = route_client
    session = _FakeSession(state)

    assert admin_main._normalize_home_limit("9999") == 100
    assert admin_main._normalize_home_limit("bad") == 20
    assert admin_main._normalize_home_scope("bogus") == "all"
    assert admin_main._normalize_home_op("noop") == "ALL"
    assert admin_main._normalize_complex_kind("instance") == "instance"
    assert admin_main._normalize_complex_kind("bad") == "all"
    assert admin_main._match_object_query(state["instances"][0], "gx11") is True
    assert admin_main._match_object_query(state["instances"][0], "") is False
    assert (
        admin_main._to_object_result("instance", state["instances"][0])["euid"]
        == "GX11"
    )
    assert admin_main._timestamp_rank(None) == 0.0

    results = admin_main._run_simple_object_query(session, "GX", "all", 10)
    assert {item["euid"] for item in results} == {"GX11", "GX12"}
    complex_results = admin_main._run_complex_query(
        session,
        "instance",
        "generic",
        "generic",
        "generic",
        "",
        "GX",
        10,
    )
    assert {item["euid"] for item in complex_results} == {"GX11", "GX12"}

    object_rows = admin_main._load_object_audit(session, "GX12", "ALL", 10)
    assert object_rows and all(row.rel_table_euid_fk == "GX12" for row in object_rows)
    user_rows = admin_main._load_user_audit(session, "other_user", "UPDATE", 10)
    assert user_rows and all(row.changed_by == "other_user" for row in user_rows)
    effective, warning = admin_main._resolve_effective_audit_user(
        {"username": "john@example.com", "role": "user"},
        "other_user",
        {"can_manage_users": False},
    )
    assert effective == "john@example.com"
    assert "own user audit trail" in warning

    assert admin_main._mask_sensitive_value("db_password", "secret") == "(redacted)"
    assert admin_main._mask_sensitive_value("host", "") == "(empty)"
    assert admin_main._parse_allowed_origins(
        " https://a.example, https://b.example "
    ) == [
        "https://a.example",
        "https://b.example",
    ]
    assert (
        admin_main._normalize_cognito_domain("example.auth.us-west-2.amazoncognito.com")
        == "example.auth.us-west-2.amazoncognito.com"
    )
    with pytest.raises(RuntimeError, match="COGNITO_DOMAIN"):
        admin_main._normalize_cognito_domain("")
    with pytest.raises(RuntimeError, match="https URL"):
        admin_main._require_https_url("http://not-secure", label="endpoint")

    monkeypatch.setattr(
        admin_main,
        "_git_output",
        lambda *args: {"rev-parse": "abc123", "describe": "5.0.0"}.get(args[0], "main"),
    )
    monkeypatch.setattr(
        admin_main,
        "ADMIN_SETTINGS",
        {
            "support_email": "support@example.com",
            "repo_url": "https://example.com/repo",
        },
    )
    footer = admin_main._build_footer_metadata()
    assert footer["support_email"] == "support@example.com"
    assert footer["repo_url"] == "https://example.com/repo"


def test_main_oauth_helpers_cover_success_and_error_branches(monkeypatch):
    monkeypatch.setattr(
        admin_main,
        "resolve_tapdb_pool_config",
        lambda _env: SimpleNamespace(
            domain="example.auth.us-west-2.amazoncognito.com",
            callback_url="https://localhost/auth/callback",
            app_client_id="client-123",
            app_client_secret="secret",
        ),
    )
    runtime = admin_main._resolve_cognito_oauth_runtime("dev")
    assert runtime["client_id"] == "client-123"
    authorize_url = admin_main._build_cognito_authorize_url(runtime, "state-1")
    query = parse_qs(urlsplit(authorize_url).query)
    assert query["identity_provider"] == ["Google"]
    assert query["state"] == ["state-1"]

    class _Response:
        def __init__(self, payload):
            self._payload = payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return self._payload

    monkeypatch.setattr(
        admin_main,
        "urlopen",
        lambda req, timeout=15: _Response(
            json.dumps({"access_token": "at-1", "id_token": "id-1"}).encode("utf-8")
        ),
    )
    tokens = admin_main._exchange_oauth_authorization_code(runtime, "code-1")
    assert tokens["access_token"] == "at-1"

    monkeypatch.setattr(
        admin_main,
        "urlopen",
        lambda req, timeout=15: (_ for _ in ()).throw(URLError("offline")),
    )
    with pytest.raises(RuntimeError, match="unreachable"):
        admin_main._exchange_oauth_authorization_code(runtime, "code-2")

    error = HTTPError("https://example.com", 400, "bad", hdrs=None, fp=None)
    error.read = lambda: b'{"error":"invalid_grant"}'
    monkeypatch.setattr(
        admin_main,
        "urlopen",
        lambda req, timeout=15: (_ for _ in ()).throw(error),
    )
    with pytest.raises(RuntimeError, match="invalid_grant"):
        admin_main._exchange_oauth_authorization_code(runtime, "code-3")

    monkeypatch.setattr(
        admin_main,
        "_fetch_oauth_userinfo",
        lambda runtime_cfg, access_token: {
            "email": "oauth@example.com",
            "name": "OAuth User",
        },
    )
    profile = admin_main._resolve_oauth_user_profile(
        "dev",
        {"access_token": "at-1"},
        runtime,
    )
    assert profile == {"email": "oauth@example.com", "display_name": "OAuth User"}

    monkeypatch.setattr(
        admin_main,
        "_fetch_oauth_userinfo",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no userinfo")),
    )
    monkeypatch.setattr(
        admin_main,
        "get_cognito_auth",
        lambda env_name: SimpleNamespace(
            verify_token=lambda token: {"cognito:username": "fallback@example.com"}
        ),
    )
    profile = admin_main._resolve_oauth_user_profile(
        "dev",
        {"access_token": "at-1", "id_token": "id-1"},
        runtime,
    )
    assert profile["email"] == "fallback@example.com"

    monkeypatch.setattr(
        admin_main,
        "get_cognito_auth",
        lambda env_name: SimpleNamespace(verify_token=lambda token: {}),
    )
    with pytest.raises(RuntimeError, match="no email"):
        admin_main._resolve_oauth_user_profile("dev", {"id_token": "id-1"}, runtime)


def test_main_load_db_inventory_context_covers_success_and_error(monkeypatch):
    rows = iter(
        [
            SimpleNamespace(scalar=lambda: "tapdb_dev"),
            _InventoryMappings([{"schema_name": "public"}, {"schema_name": "app_ns"}]),
            _InventoryMappings(
                [{"schema_name": "public", "table_name": "generic_instance"}]
            ),
            _InventoryMappings([{"schema_name": "public", "view_name": "v_active"}]),
            _InventoryMappings(
                [{"schema_name": "public", "materialized_view_name": "mv_active"}]
            ),
            _InventoryMappings(
                [{"schema_name": "public", "sequence_name": "gx_instance_seq"}]
            ),
            _InventoryMappings(
                [
                    {
                        "schema_name": "public",
                        "table_name": "generic_instance",
                        "trigger_name": "trigger_update",
                    }
                ]
            ),
            _InventoryMappings(
                [
                    {
                        "schema_name": "public",
                        "function_signature": "set_generic_instance_euid()",
                    }
                ]
            ),
            _InventoryMappings(
                [
                    {
                        "schema_name": "public",
                        "table_name": "generic_instance",
                        "index_name": "idx_generic_instance_euid",
                    }
                ]
            ),
        ]
    )

    class _InventorySession:
        def execute(self, _stmt):
            return next(rows)

    class _InventoryConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        @contextmanager
        def session_scope(self):
            yield _InventorySession()

    monkeypatch.setattr(admin_main, "get_db", lambda: _InventoryConn())
    ctx = admin_main.load_db_inventory_context()
    assert ctx["db_inventory_db_name"] == "tapdb_dev"
    assert ctx["db_inventory_counts"]["schemas"] == 2
    assert ctx["db_inventory_tables"]

    class _BrokenConn:
        def __enter__(self):
            raise RuntimeError("db unavailable")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(admin_main, "get_db", lambda: _BrokenConn())
    error_ctx = admin_main.load_db_inventory_context()
    assert error_ctx["db_inventory_error"] == "db unavailable"


def test_main_public_routes_and_protected_pages(route_client, monkeypatch):
    client, state = route_client

    async def _anon(_request):
        return None

    monkeypatch.setattr(admin_main, "get_current_user", _anon)
    monkeypatch.setattr(auth_mod, "get_current_user", _anon)
    assert client.get("/login").status_code == 200
    assert client.get("/help").status_code == 200
    assert client.get("/info", follow_redirects=False).status_code == 302
    assert client.get("/logout", follow_redirects=False).status_code == 302
    assert client.get("/change-password", follow_redirects=False).status_code == 302

    async def _admin(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin)
    assert client.get("/").status_code == 200
    assert client.get("/templates").status_code == 200
    assert client.get("/instances").status_code == 200
    assert client.get("/lineages").status_code == 200
    assert client.get("/object/GX12").status_code == 200
    assert client.get("/graph?start_euid=GX11&depth=2").status_code == 200
    assert client.get("/query?kind=instance&euid_like=GX").status_code == 200
    assert client.get("/info").status_code == 200
    assert client.get("/admin/metrics").status_code == 200
    assert client.get("/create-instance/GT1").status_code == 200
    assert client.get("/create-instance/GT2").status_code == 403

    home_ctx = _last_render_context(state, "index.html")
    assert home_ctx["template_count"] == 2
    info_ctx = _last_render_context(state, "info.html")
    assert info_ctx["db_inventory_visible"] is True
    query_ctx = _last_render_context(state, "complex_query.html")
    assert query_ctx["results"]


@pytest.mark.anyio
async def test_main_form_handlers_cover_login_signup_and_password_flows(
    route_client, monkeypatch
):
    _client, state = route_client
    request = _request(path="/login", session={})

    monkeypatch.setattr(
        admin_main, "get_style", lambda *_args, **_kwargs: {"skin_css": "x.css"}
    )
    monkeypatch.setattr(
        admin_main.templates,
        "get_template",
        lambda name: _FakeTemplateRender(name, state),
    )
    monkeypatch.setattr(admin_main, "get_user_by_username", lambda username: None)
    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_args: (_ for _ in ()).throw(ValueError("bad credentials")),
    )
    response = await admin_main.login_submit(
        request, username="alice@example.com", password="pw"
    )
    assert response.status_code == 200
    assert (
        _last_render_context(state, "login.html")["error"]
        == "Invalid username or password"
    )

    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_args: {
            "challenge": "NEW_PASSWORD_REQUIRED",
            "session": "challenge-token",
        },
    )
    response = await admin_main.login_submit(
        request, username="alice@example.com", password="pw"
    )
    assert response.status_code == 302
    assert response.headers["location"] == "/change-password"
    assert request.session["cognito_challenge"] == "NEW_PASSWORD_REQUIRED"

    request = _request(path="/login", session={})
    monkeypatch.setattr(
        admin_main,
        "get_user_by_username",
        lambda username: {"uid": 9, "email": username, "require_password_change": True},
    )
    monkeypatch.setattr(
        admin_main, "authenticate_with_cognito", lambda *_args: {"access_token": "tok"}
    )
    response = await admin_main.login_submit(
        request, username="alice@example.com", password="pw"
    )
    assert response.status_code == 302
    assert response.headers["location"] == "/change-password"

    request = _request(path="/signup", session={})
    response = await admin_main.signup_submit(
        request,
        email="bad-email",
        display_name="Alice",
        password="Password1!",
        confirm_password="Password1!",
    )
    assert response.status_code == 200
    response = await admin_main.signup_submit(
        request,
        email="alice@example.com",
        display_name="Alice",
        password="short",
        confirm_password="short",
    )
    assert response.status_code == 200
    response = await admin_main.signup_submit(
        request,
        email="alice@example.com",
        display_name="Alice",
        password="Password1!",
        confirm_password="Mismatch1!",
    )
    assert response.status_code == 200

    monkeypatch.setattr(
        admin_main, "create_cognito_user_account", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        admin_main,
        "get_or_create_user_from_email",
        lambda email, **_kwargs: {
            "uid": 77,
            "username": email,
            "email": email,
            "role": "user",
            "require_password_change": False,
        },
    )
    monkeypatch.setattr(
        admin_main, "authenticate_with_cognito", lambda *_args: {"access_token": "tok"}
    )
    response = await admin_main.signup_submit(
        request,
        email="alice@example.com",
        display_name="Alice",
        password="Password1!",
        confirm_password="Password1!",
    )
    assert response.status_code == 302
    assert response.headers["location"] == "/"

    async def _current_user(_request):
        return {
            "uid": 77,
            "username": "alice@example.com",
            "email": "alice@example.com",
            "require_password_change": False,
        }

    monkeypatch.setattr(admin_main, "get_current_user", _current_user)
    request = _request(path="/change-password", session={})
    response = await admin_main.change_password_submit(
        request,
        current_password=None,
        new_password="short",
        confirm_password="short",
    )
    assert response.status_code == 200
    response = await admin_main.change_password_submit(
        request,
        current_password=None,
        new_password="Password1!",
        confirm_password="Mismatch1!",
    )
    assert response.status_code == 200
    response = await admin_main.change_password_submit(
        request,
        current_password=None,
        new_password="Password1!",
        confirm_password="Password1!",
    )
    assert response.status_code == 200

    request = _request(
        path="/change-password",
        session={"cognito_access_token": "tok"},
    )
    monkeypatch.setattr(admin_main, "change_cognito_password", lambda *_args: None)
    response = await admin_main.change_password_submit(
        request,
        current_password="old-password",
        new_password="Password1!",
        confirm_password="Password1!",
    )
    assert response.status_code == 200
    assert (
        _last_render_context(state, "change_password.html")["success"]
        == "Password changed successfully"
    )


def test_main_oauth_routes_and_api_routes(route_client, monkeypatch):
    client, state = route_client

    async def _anon(_request):
        return None

    async def _admin(_request):
        return _admin_user()

    monkeypatch.setattr(admin_main, "get_current_user", _anon)
    monkeypatch.setattr(
        admin_main,
        "_exchange_oauth_authorization_code",
        lambda runtime_cfg, code: {"access_token": "tok", "id_token": "id-1"},
    )
    monkeypatch.setattr(
        admin_main,
        "_resolve_oauth_user_profile",
        lambda env_name, tokens, runtime_cfg: {
            "email": "oauth@example.com",
            "display_name": "OAuth User",
        },
    )
    login_response = client.get("/auth/login", follow_redirects=False)
    assert login_response.status_code == 302
    location = login_response.headers["location"]
    query = parse_qs(urlsplit(location).query)
    callback_response = client.get(
        f"/auth/callback?code=code-1&state={query['state'][0]}",
        follow_redirects=False,
    )
    assert callback_response.status_code == 302
    assert callback_response.headers["location"] == "/"

    monkeypatch.setattr(admin_main, "get_current_user", _anon)
    bad_state = client.get("/auth/callback?code=code-2&state=wrong")
    assert bad_state.status_code == 200
    assert "TEMPLATE:login.html" in bad_state.text

    monkeypatch.setattr(auth_mod, "get_current_user", _admin)
    monkeypatch.setattr(
        admin_main,
        "fetch_remote_graph",
        lambda request, ref, *, depth: {
            "elements": {"nodes": [{"data": {"id": ref.root_euid}}], "edges": []}
        },
    )
    monkeypatch.setattr(
        admin_main,
        "namespace_external_graph",
        lambda payload, *, ref, ref_index, source_euid: {
            "elements": payload["elements"],
            "source_euid": source_euid,
            "ref_index": ref_index,
        },
    )
    monkeypatch.setattr(
        admin_main,
        "fetch_remote_object_detail",
        lambda request, ref, *, euid: {"euid": euid, "system": ref.system},
    )
    assert client.get("/api/graph/data").status_code == 200
    assert client.get("/api/graph/data?start_euid=GX11&depth=2").status_code == 200
    assert client.get("/api/templates").status_code == 200
    assert client.get("/api/instances").status_code == 200
    assert client.get("/api/object/GT1").status_code == 200
    assert client.get("/api/object/MISSING").status_code == 404

    state["instances"][0].json_addl = {
        "properties": {
            "external_payload": {
                "tapdb_graph": {
                    "system": "atlas",
                    "base_url": "https://atlas.local",
                    "root_euid": "AT-1",
                    "tenant_id": "atlas-tenant",
                    "graph_data_path": "/api/graph/data",
                    "object_detail_path_template": "/api/object/{euid}",
                    "auth_mode": "none",
                }
            }
        }
    }
    assert (
        client.get(
            "/api/graph/external",
            params={"source_euid": "GX11", "ref_index": 0, "depth": 2},
        ).status_code
        == 200
    )
    assert (
        client.get(
            "/api/graph/external/object",
            params={"source_euid": "GX11", "ref_index": 0, "euid": "AT-2"},
        ).status_code
        == 200
    )

    duplicate = client.post(
        "/api/lineage",
        json={
            "parent_euid": "GX11",
            "child_euid": "GX12",
            "relationship_type": "contains",
        },
    )
    assert duplicate.status_code == 409

    created = client.post(
        "/api/lineage",
        json={
            "parent_euid": "GX11",
            "child_euid": "GX12",
            "relationship_type": "depends_on",
        },
    )
    assert created.status_code == 200
    assert state["lineages"][-1].relationship_type == "depends_on"

    deleted = client.delete("/api/object/GT1")
    assert deleted.status_code == 200
    assert state["templates"][0].is_deleted is True


@pytest.mark.anyio
async def test_main_create_instance_submit_success_and_errors(
    route_client, monkeypatch
):
    _client, state = route_client
    request = _request(
        path="/create-instance/GT1",
        session={},
        user=_admin_user(),
    )

    class _Factory:
        def __init__(self, manager):
            self.manager = manager

        def create_instance(
            self, session, template_code, name, properties=None, create_children=False
        ):
            return SimpleNamespace(euid="GX99")

        def link_instances(self, session, parent, child, relationship_type):
            return None

    monkeypatch.setattr(admin_main, "TemplateManager", lambda: object())
    monkeypatch.setattr(admin_main, "InstanceFactory", _Factory)

    class _Form(dict):
        def get(self, key, default=None):
            return super().get(key, default)

        def items(self):
            return super().items()

    async def _success_form():
        return _Form(
            instance_name="Created Item",
            create_children="false",
            parent_euids="GX11",
            child_euids="GX12",
            relationship_type="contains",
            prop_color='"blue"',
        )

    request.form = _success_form
    response = await admin_main.create_instance_submit.__wrapped__(request, "GT1")
    assert response.status_code == 302
    assert response.headers["location"] == "/object/GX99"

    monkeypatch.setattr(
        admin_main,
        "_resolve_lineage_targets_or_raise",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ValueError("missing parent EUID(s): GX404")
        ),
    )

    async def _validation_form():
        return _Form(
            instance_name="Broken Item",
            create_children="false",
            parent_euids="GX404",
            child_euids="",
            relationship_type="contains",
        )

    request.form = _validation_form
    response = await admin_main.create_instance_submit.__wrapped__(request, "GT1")
    assert response.status_code == 200
    assert (
        "Validation error"
        in _last_render_context(state, "create_instance.html")["error"]
    )

    monkeypatch.setattr(
        admin_main,
        "InstanceFactory",
        lambda manager: SimpleNamespace(
            create_instance=lambda **kwargs: (_ for _ in ()).throw(
                RuntimeError("boom")
            ),
            link_instances=lambda **kwargs: None,
        ),
    )
    monkeypatch.setattr(
        admin_main,
        "_resolve_lineage_targets_or_raise",
        lambda session, **kwargs: ([], []),
    )
    response = await admin_main.create_instance_submit.__wrapped__(request, "GT1")
    assert response.status_code == 200
    assert (
        "Error creating instance"
        in _last_render_context(state, "create_instance.html")["error"]
    )
