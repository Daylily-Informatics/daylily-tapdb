from __future__ import annotations

import base64
import json
from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import itsdangerous
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import admin.auth as auth_mod
import admin.backups as backups_api
import admin.main as admin_main
import daylily_tapdb.runtime_info as runtime_info


def _admin_user(**overrides):
    user = {
        "uid": 1,
        "username": "admin@example.com",
        "email": "admin@example.com",
        "role": "admin",
        "require_password_change": False,
    }
    user.update(overrides)
    return user


class _Template:
    def __init__(self, name, renders):
        self.name = name
        self.renders = renders

    def render(self, **context):
        self.renders.append((self.name, context))
        return f"TEMPLATE:{self.name}:{context.get('error', '')}"


class _Templates:
    def __init__(self):
        self.renders = []

    def get_template(self, name):
        return _Template(name, self.renders)


class _Session:
    pass


class _Connection:
    def __init__(self, session=None):
        self.app_username = None
        self.session = session or _Session()
        self.commits = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self, commit=False):
        self.commits.append(commit)
        yield self.session


@pytest.fixture
def admin_api(monkeypatch):
    async def _admin(_request):
        return _admin_user()

    templates = _Templates()
    connection = _Connection()
    monkeypatch.setattr(admin_main, "_ADMIN_SETTINGS_LOAD_ERROR", None)
    monkeypatch.setattr(auth_mod, "get_current_user", _admin)
    monkeypatch.setattr(admin_main, "get_current_user", _admin)
    monkeypatch.setattr(admin_main, "templates", templates)
    monkeypatch.setattr(admin_main, "get_db", lambda: connection)
    monkeypatch.setattr(admin_main, "update_last_login", lambda _uid: None)
    monkeypatch.setattr(
        admin_main,
        "get_db_config",
        lambda: {
            "client_id": "tapdb-local",
            "domain_code": "LAB",
            "owner_repo_name": "owner-repo",
            "prefix_ownership_registry_path": "/tmp/prefix.json",
            "domain_registry_path": "/tmp/domain.json",
        },
    )
    return TestClient(admin_main.app), connection, templates


def test_admin_main_helpers_cover_fail_closed_and_normalized_branches(monkeypatch):
    assert admin_main._is_production_like("blue-prod", {}) is True
    assert admin_main._require_https_url("https://example.com/path", label="endpoint")
    with pytest.raises(RuntimeError, match="https URL"):
        admin_main._require_https_url("http://example.com", label="endpoint")

    monkeypatch.setattr(
        admin_main.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("no git")),
    )
    assert admin_main._git_output("status") == ""
    monkeypatch.setattr(admin_main, "_git_output", lambda *_args: "")
    assert admin_main._build_footer_metadata()["tag"] == "n/a"

    request = SimpleNamespace(
        scope={"root_path": object()},
        app=SimpleNamespace(state=SimpleNamespace(tapdb_host_bridge="bridge")),
    )
    monkeypatch.setattr(
        admin_main,
        "resolve_host_shell",
        lambda bridge, req: {"bridge": bridge, "request": req},
    )
    monkeypatch.setattr(
        admin_main,
        "resolve_host_context",
        lambda bridge, req: {"bridge": bridge, "request": req},
    )
    assert admin_main.tapdb_base_path(request) == ""
    assert admin_main.tapdb_url(request, "") == ""
    assert admin_main.tapdb_url(request, "relative") == "/relative"
    assert admin_main.tapdb_host_shell(request)["bridge"] == "bridge"
    assert admin_main.tapdb_host_context(request)["bridge"] == "bridge"
    assert admin_main.get_style() == {"skin_css": "/static/css/style.css"}
    assert admin_main._is_reserved_template(None) is False

    monkeypatch.setattr(admin_main, "_active_tapdb_target", lambda: "target-a")
    monkeypatch.setattr(
        admin_main,
        "build_metrics_page_context",
        lambda env, *, limit: {"env": env, "limit": limit},
    )
    assert admin_main.load_db_metrics_context(limit=8) == {
        "env": "target-a",
        "limit": 8,
    }


def test_admin_main_oauth_helper_failure_and_claim_paths(monkeypatch):
    with pytest.raises(RuntimeError, match="not configured"):
        admin_main._normalize_cognito_domain("")
    with pytest.raises(RuntimeError, match="Invalid"):
        admin_main._normalize_cognito_domain("https://pool.example.com/path")

    base = SimpleNamespace(
        domain="pool.example.com",
        callback_url="",
        app_client_id="client",
        app_client_secret="",
    )
    monkeypatch.setattr(admin_main, "resolve_tapdb_pool_config", lambda _env: base)
    with pytest.raises(RuntimeError, match="callback_url"):
        admin_main._resolve_cognito_oauth_runtime("target")
    base.callback_url = "https://app.example.com/callback"
    base.app_client_id = ""
    with pytest.raises(RuntimeError, match="app_client_id"):
        admin_main._resolve_cognito_oauth_runtime("target")

    class _Response:
        def __init__(self, body):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return self.body

    runtime = {
        "domain": "pool.example.com",
        "callback_url": "https://app.example.com/callback",
        "client_id": "client",
        "client_secret": "",
    }
    monkeypatch.setattr(
        admin_main, "urlopen", lambda *_args, **_kwargs: _Response(b"{}")
    )
    assert admin_main._exchange_oauth_authorization_code(runtime, "code") == {}
    monkeypatch.setattr(
        admin_main, "urlopen", lambda *_args, **_kwargs: _Response(b"not-json")
    )
    with pytest.raises(RuntimeError, match="not valid JSON"):
        admin_main._exchange_oauth_authorization_code(runtime, "code")
    monkeypatch.setattr(
        admin_main,
        "urlopen",
        lambda *_args, **_kwargs: _Response(
            b'{"error":"invalid_grant","error_description":"expired code"}'
        ),
    )
    with pytest.raises(RuntimeError, match="expired code"):
        admin_main._exchange_oauth_authorization_code(runtime, "code")
    monkeypatch.setattr(
        admin_main,
        "urlopen",
        lambda *_args, **_kwargs: _Response(
            b'{"email":"USER@EXAMPLE.COM","name":"User"}'
        ),
    )
    assert (
        admin_main._fetch_oauth_userinfo(runtime, "token")["email"]
        == "USER@EXAMPLE.COM"
    )

    monkeypatch.setattr(
        admin_main,
        "_fetch_oauth_userinfo",
        lambda *_args: (_ for _ in ()).throw(OSError("offline")),
    )
    verifier = SimpleNamespace(
        verify_token=lambda _token: {
            "cognito:username": "CLAIM@EXAMPLE.COM",
            "preferred_username": "Claim User",
        }
    )
    monkeypatch.setattr(admin_main, "get_cognito_auth", lambda _env: verifier)
    profile = admin_main._resolve_oauth_user_profile(
        "target", {"access_token": "bad", "id_token": "id"}, runtime
    )
    assert profile == {"email": "claim@example.com", "display_name": "Claim User"}
    verifier.verify_token = lambda _token: (_ for _ in ()).throw(
        ValueError("bad token")
    )
    with pytest.raises(RuntimeError, match="no email"):
        admin_main._resolve_oauth_user_profile("target", {"id_token": "id"}, runtime)


def test_admin_main_oauth_and_password_route_decisions(admin_api, monkeypatch):
    client, _connection, templates = admin_api

    async def _existing(_request):
        return _admin_user()

    monkeypatch.setattr(admin_main, "get_current_user", _existing)
    assert client.get("/auth/login", follow_redirects=False).status_code == 302
    assert client.get("/login", follow_redirects=False).status_code == 302
    assert client.get("/signup", follow_redirects=False).status_code == 302

    async def _challenge(_request):
        return _admin_user(require_password_change=True)

    monkeypatch.setattr(admin_main, "get_current_user", _challenge)
    assert (
        client.get("/login", follow_redirects=False).headers["location"]
        == "/change-password"
    )

    async def _anonymous(_request):
        return None

    monkeypatch.setattr(admin_main, "get_current_user", _anonymous)
    monkeypatch.setattr(
        admin_main,
        "_resolve_cognito_oauth_runtime",
        lambda _env: (_ for _ in ()).throw(RuntimeError("missing config")),
    )
    assert "not configured" in client.get("/auth/login").text
    assert (
        "provider denied"
        in client.get(
            "/auth/callback?error=access_denied&error_description=provider%20denied"
        ).text
    )

    with client as session_client:
        session_client.cookies.clear()
        response = session_client.get("/auth/login")
        assert response.status_code == 200

    monkeypatch.setattr(admin_main, "get_user_by_username", lambda _name: None)
    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("provider offline")),
    )
    assert (
        "provider offline"
        in client.post(
            "/login", data={"username": "user@example.com", "password": "secret"}
        ).text
    )

    assert (
        "Valid email"
        in client.post(
            "/signup",
            data={
                "email": "bad",
                "display_name": "",
                "password": "longpassword",
                "confirm_password": "longpassword",
            },
        ).text
    )
    assert (
        "at least 8"
        in client.post(
            "/signup",
            data={
                "email": "user@example.com",
                "display_name": "",
                "password": "short",
                "confirm_password": "short",
            },
        ).text
    )
    assert (
        "do not match"
        in client.post(
            "/signup",
            data={
                "email": "user@example.com",
                "display_name": "",
                "password": "longpassword",
                "confirm_password": "differentpass",
            },
        ).text
    )

    monkeypatch.setattr(admin_main, "get_current_user", _anonymous)
    assert client.get("/change-password", follow_redirects=False).status_code == 302
    assert (
        client.post(
            "/change-password",
            data={
                "current_password": "old",
                "new_password": "newpassword",
                "confirm_password": "newpassword",
            },
            follow_redirects=False,
        ).status_code
        == 302
    )
    assert templates.renders


def test_admin_main_operator_routes_exercise_success_and_error_contracts(
    admin_api, monkeypatch
):
    client, connection, _templates = admin_api
    monkeypatch.setattr(admin_main, "get_config_path", lambda: "/tmp/tapdb.toml")
    monkeypatch.setattr(
        runtime_info,
        "build_runtime_info",
        lambda **kwargs: {"runtime": "ok", "config_path": str(kwargs["config_path"])},
    )
    assert client.get("/api/runtime-info").json()["runtime"] == "ok"

    monkeypatch.setattr(
        admin_main,
        "search_objects",
        lambda *_args, **kwargs: {"items": [], "page": {"limit": kwargs["limit"]}},
    )
    assert (
        client.get("/api/objects/search?limit=7&cursor=next").json()["page"]["limit"]
        == 7
    )
    monkeypatch.setattr(
        admin_main,
        "search_objects",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad cursor")),
    )
    assert client.get("/api/objects/search").status_code == 400

    assert client.patch("/api/objects/persisted-object", json=[]).status_code == 400
    assert client.patch("/api/objects/persisted-object", json={}).status_code == 400
    monkeypatch.setattr(
        admin_main,
        "update_object",
        lambda *_args, **kwargs: {"updated": True, "dry_run": kwargs["dry_run"]},
    )
    assert (
        client.patch(
            "/api/objects/persisted-object", json={"changes": {"name": "New"}}
        ).json()["dry_run"]
        is True
    )
    monkeypatch.setattr(
        admin_main,
        "update_object",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(LookupError("missing")),
    )
    assert (
        client.patch(
            "/api/objects/persisted-object", json={"changes": {}, "apply": True}
        ).status_code
        == 404
    )

    assert (
        client.post("/api/objects/persisted-object/repair", json=[]).status_code == 400
    )
    assert (
        client.post("/api/objects/persisted-object/repair", json={}).status_code == 400
    )
    monkeypatch.setattr(
        admin_main,
        "repair_object",
        lambda *_args, **kwargs: {"repaired": True, "dry_run": kwargs["dry_run"]},
    )
    assert (
        client.post(
            "/api/objects/persisted-object/repair",
            json={
                "repair_payload": {"field": "value"},
                "reason": "repair",
                "apply": True,
            },
        ).json()["dry_run"]
        is False
    )
    monkeypatch.setattr(
        admin_main,
        "repair_object",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError("denied")),
    )
    assert (
        client.post(
            "/api/objects/persisted-object/repair", json={"repair_payload": {}}
        ).status_code
        == 403
    )
    assert connection.commits.count(True) >= 2


@dataclass
class _ImportResult:
    created: int
    updated: int


def test_admin_main_repository_and_proxy_routes_cover_security_boundaries(
    admin_api, monkeypatch
):
    client, _connection, _templates = admin_api
    monkeypatch.setattr(
        admin_main, "repository_inventory", lambda *_args, **_kwargs: {"clean": True}
    )
    assert client.get(
        "/api/templates/repository/status?repository_pack=/tmp/pack.json"
    ).json() == {"clean": True}
    monkeypatch.setattr(
        admin_main,
        "repository_inventory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(FileExistsError("collision")),
    )
    assert (
        client.get(
            "/api/templates/repository/status?repository_pack=/tmp/pack.json"
        ).status_code
        == 409
    )

    assert client.post("/api/templates/repository/export", json=[]).status_code == 400
    monkeypatch.setattr(
        admin_main,
        "export_repository_pack",
        lambda *_args, **_kwargs: {"exported": True},
    )
    assert (
        client.post(
            "/api/templates/repository/export",
            json={"repository_pack": "/tmp/pack.json"},
        ).json()["exported"]
        is True
    )
    monkeypatch.setattr(
        admin_main,
        "export_repository_pack",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("ambiguous owner")
        ),
    )
    assert client.post("/api/templates/repository/export", json={}).status_code == 409

    assert client.post("/api/templates/repository/import", json=[]).status_code == 400
    monkeypatch.setattr(
        admin_main,
        "import_repository_pack",
        lambda *_args, **_kwargs: _ImportResult(created=2, updated=0),
    )
    assert (
        client.post(
            "/api/templates/repository/import",
            json={"repository_pack": "/tmp/pack.json", "apply": True},
        ).json()["created"]
        == 2
    )
    monkeypatch.setattr(
        admin_main,
        "import_repository_pack",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("invalid pack")),
    )
    assert client.post("/api/templates/repository/import", json={}).status_code == 400

    monkeypatch.delattr(admin_main.app.state, "tapdb_v1_proxy_policy", raising=False)
    params = {"source_euid": "persisted-source", "ref_index": 0}
    assert client.get("/api/graph/external", params=params).status_code == 404
    assert (
        client.get(
            "/api/graph/external/object", params={**params, "euid": "persisted-remote"}
        ).status_code
        == 404
    )


def test_admin_main_backup_routes_delegate_complete_typed_requests(
    admin_api, monkeypatch
):
    client, _connection, _templates = admin_api
    monkeypatch.setattr(
        admin_main, "_backup_context", lambda: ({"target": "a"}, {"root": "/tmp"})
    )
    monkeypatch.setattr(
        backups_api, "api_actor", lambda _request: {"username": "admin@example.com"}
    )
    calls = []

    def _payload(name):
        def _call(*args, **kwargs):
            calls.append((name, args, kwargs))
            return {"operation": name}

        return _call

    for name in (
        "list_payload",
        "status_payload",
        "health_payload",
        "plan_payload",
        "create_payload",
        "verify_payload",
        "stage_payload",
        "apply_payload",
        "rehearse_payload",
    ):
        monkeypatch.setattr(backups_api, name, _payload(name))

    assert (
        client.get("/api/backups?class=full&limit=2").json()["operation"]
        == "list_payload"
    )
    assert client.get("/api/backups/status").json()["operation"] == "status_payload"
    assert client.get("/api/backups/health").json()["operation"] == "health_payload"
    assert (
        client.get("/api/backups/plan?class=full&strict=true").json()["operation"]
        == "plan_payload"
    )
    assert client.post("/api/backups", json={"class": "full"}).status_code == 201
    assert (
        client.post("/api/backups/backup-ref/verify", json={"level": "quick"}).json()[
            "operation"
        ]
        == "verify_payload"
    )
    assert (
        client.post(
            "/api/backups/backup-ref/restore/stage", json={"database": "isolated"}
        ).json()["operation"]
        == "stage_payload"
    )
    assert (
        client.post(
            "/api/backups/backup-ref/restore/apply", json={"confirmation": "typed"}
        ).json()["operation"]
        == "apply_payload"
    )
    assert (
        client.post("/api/backups/backup-ref/rehearse", json={}).json()["operation"]
        == "rehearse_payload"
    )
    assert {name for name, _args, _kwargs in calls} == {
        "list_payload",
        "status_payload",
        "health_payload",
        "plan_payload",
        "create_payload",
        "verify_payload",
        "stage_payload",
        "apply_payload",
        "rehearse_payload",
    }


@pytest.mark.anyio
async def test_admin_main_backup_body_and_configuration_errors(monkeypatch):
    request = SimpleNamespace(json=lambda: None)

    async def _bad_json():
        raise ValueError("empty")

    request.json = _bad_json
    assert await admin_main._json_body(request) == {}

    async def _list_json():
        return []

    request.json = _list_json
    assert await admin_main._json_body(request) == {}

    monkeypatch.setattr(
        admin_main,
        "get_db_config",
        lambda: (_ for _ in ()).throw(RuntimeError("no config")),
    )
    with pytest.raises(HTTPException) as caught:
        admin_main._backup_context()
    assert caught.value.status_code == 503


def test_admin_main_operator_error_mapping_is_stable():
    assert admin_main._operator_http_error(LookupError("missing")).status_code == 404
    assert admin_main._operator_http_error(PermissionError("denied")).status_code == 403
    assert admin_main._operator_http_error(FileExistsError("exists")).status_code == 409
    assert (
        admin_main._operator_http_error(RuntimeError("ambiguous target")).status_code
        == 409
    )
    assert admin_main._operator_http_error(ValueError("bad input")).status_code == 400


def test_admin_main_oauth_callback_login_and_signup_failure_routes(
    admin_api, monkeypatch
):
    client, _connection, _templates = admin_api

    async def _anonymous(_request):
        return None

    monkeypatch.setattr(admin_main, "get_current_user", _anonymous)
    runtime = {
        "domain": "pool.example.com",
        "callback_url": "https://app.example.com/callback",
        "client_id": "client",
        "client_secret": "",
        "scope": "openid email profile",
    }
    monkeypatch.setattr(
        admin_main, "_resolve_cognito_oauth_runtime", lambda _env: runtime
    )

    login = client.get("/auth/login?next=not-absolute", follow_redirects=False)
    state = parse_qs(urlsplit(login.headers["location"]).query)["state"][0]
    missing_code = client.get(f"/auth/callback?state={state}")
    assert "missing authorization code" in missing_code.text

    login = client.get("/auth/login", follow_redirects=False)
    state = parse_qs(urlsplit(login.headers["location"]).query)["state"][0]
    monkeypatch.setattr(
        admin_main,
        "_exchange_oauth_authorization_code",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("exchange rejected")),
    )
    failed = client.get(f"/auth/callback?code=code&state={state}")
    assert "exchange rejected" in failed.text

    existing = _admin_user()
    monkeypatch.setattr(admin_main, "get_user_by_username", lambda _name: existing)
    monkeypatch.setattr(admin_main, "authenticate_with_cognito", lambda *_args: {})
    no_token = client.post(
        "/login", data={"username": "admin@example.com", "password": "secret"}
    )
    assert "no access token" in no_token.text

    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_args: {"access_token": "access"},
    )
    assert (
        client.post(
            "/login",
            data={"username": "admin@example.com", "password": "secret"},
            follow_redirects=False,
        ).headers["location"]
        == "/"
    )

    monkeypatch.setattr(admin_main, "get_user_by_username", lambda _name: None)
    monkeypatch.setattr(
        admin_main,
        "get_or_create_user_from_email",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("provision refused")
        ),
    )
    assert (
        "provision refused"
        in client.post(
            "/login", data={"username": "new@example.com", "password": "secret"}
        ).text
    )

    monkeypatch.setattr(
        admin_main,
        "create_cognito_user_account",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("account exists")),
    )
    signup_data = {
        "email": "new@example.com",
        "display_name": "New",
        "password": "longpassword",
        "confirm_password": "longpassword",
    }
    assert "account exists" in client.post("/signup", data=signup_data).text
    monkeypatch.setattr(
        admin_main,
        "create_cognito_user_account",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("Cognito offline")
        ),
    )
    assert "Cognito offline" in client.post("/signup", data=signup_data).text


def test_admin_main_signup_challenge_and_auto_login_routes(admin_api, monkeypatch):
    client, _connection, _templates = admin_api
    signup_data = {
        "email": "new@example.com",
        "display_name": "New",
        "password": "longpassword",
        "confirm_password": "longpassword",
    }
    monkeypatch.setattr(
        admin_main, "create_cognito_user_account", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        admin_main,
        "get_or_create_user_from_email",
        lambda *_args, **_kwargs: {"uid": 5, "username": "new@example.com"},
    )
    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("auto login failed")),
    )
    assert "auto login failed" in client.post("/signup", data=signup_data).text

    monkeypatch.setattr(
        admin_main,
        "authenticate_with_cognito",
        lambda *_args: {"challenge": "NEW_PASSWORD_REQUIRED", "session": "challenge"},
    )
    response = client.post("/signup", data=signup_data, follow_redirects=False)
    assert response.headers["location"] == "/change-password"

    monkeypatch.setattr(admin_main, "authenticate_with_cognito", lambda *_args: {})
    response = client.post("/signup", data=signup_data, follow_redirects=False)
    assert response.headers["location"] == "/"


class _RowsQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter_by(self, **_kwargs):
        return self

    def all(self):
        return list(self.rows)


class _RowsSession:
    def __init__(self, rows):
        self.rows = rows

    def query(self, _model):
        return _RowsQuery(self.rows)


def test_admin_main_query_filters_and_audit_branch_decisions():
    matching = SimpleNamespace(
        euid="persisted-object",
        name="Wanted",
        category="data",
        type="file",
        subtype="result",
        version="1",
        created_dt=object(),
    )
    other = SimpleNamespace(
        euid="another-object",
        name="Other",
        category="actor",
        type="user",
        subtype="person",
        version="1",
        created_dt=None,
    )
    session = _RowsSession([matching, other])
    result = admin_main._run_complex_query(
        session,
        "instance",
        "data",
        "file",
        "result",
        "want",
        "persisted",
        10,
    )
    assert [item["euid"] for item in result] == ["persisted-object"]
    for filters in (
        ("", "not-file", "", "", ""),
        ("", "", "not-result", "", ""),
        ("", "", "", "absent-name", ""),
        ("", "", "", "", "absent-euid"),
    ):
        assert (
            admin_main._run_complex_query(session, "instance", *filters, limit=10) == []
        )
    assert admin_main._timestamp_rank(object()) == 0.0

    audit_rows = [
        SimpleNamespace(
            uid=1,
            euid="audit-1",
            rel_table_euid_fk="persisted-object",
            operation_type="UPDATE",
            changed_by="user@example.com",
            changed_at=None,
        ),
        SimpleNamespace(
            uid=2,
            euid="audit-2",
            rel_table_euid_fk="persisted-object",
            operation_type="DELETE",
            changed_by="other@example.com",
            changed_at=None,
        ),
    ]
    audit_session = _RowsSession(audit_rows)
    assert (
        admin_main._load_object_audit(audit_session, "persisted-object", "UPDATE", 4)[
            0
        ].uid
        == 1
    )
    assert (
        admin_main._load_user_audit(audit_session, "user@example.com", "UPDATE", 4)[
            0
        ].uid
        == 1
    )
    audit_rows[1].changed_by = "user@example.com"
    assert (
        len(admin_main._load_user_audit(audit_session, "user@example.com", "UPDATE", 4))
        == 1
    )
    assert admin_main._load_user_audit(audit_session, "", "ALL", 4) == []


def test_admin_main_v1_proxy_enabled_error_routes(admin_api, monkeypatch):
    client, _connection, _templates = admin_api
    monkeypatch.setattr(
        admin_main.app.state,
        "tapdb_v1_proxy_policy",
        admin_main.V1ProxyPolicy(allowed_hosts=frozenset({"remote.example.com"})),
        raising=False,
    )
    graph_params = {"source_euid": "persisted-source", "ref_index": 0}
    object_params = {**graph_params, "euid": "persisted-remote"}

    monkeypatch.setattr(admin_main, "_find_object_by_euid", lambda *_args: (None, None))
    assert client.get("/api/graph/external", params=graph_params).status_code == 404
    assert (
        client.get("/api/graph/external/object", params=object_params).status_code
        == 404
    )

    source = SimpleNamespace(euid="persisted-source")
    monkeypatch.setattr(
        admin_main, "_find_object_by_euid", lambda *_args: (source, "instance")
    )
    monkeypatch.setattr(
        admin_main,
        "get_external_ref_by_index",
        lambda *_args: (_ for _ in ()).throw(IndexError("missing reference")),
    )
    assert client.get("/api/graph/external", params=graph_params).status_code == 404
    assert (
        client.get("/api/graph/external/object", params=object_params).status_code
        == 404
    )

    ref = SimpleNamespace(
        base_url="https://remote.example.com", root_euid="persisted-remote"
    )
    monkeypatch.setattr(admin_main, "get_external_ref_by_index", lambda *_args: ref)
    monkeypatch.setattr(
        admin_main,
        "fetch_remote_graph",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("remote failed")),
    )
    monkeypatch.setattr(
        admin_main,
        "fetch_remote_object_detail",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("remote failed")),
    )
    assert client.get("/api/graph/external", params=graph_params).status_code == 502
    assert (
        client.get("/api/graph/external/object", params=object_params).status_code
        == 502
    )
    assert (
        client.delete("/api/object/persisted-object?hard_delete=true").status_code
        == 400
    )


def _set_session_cookie(client, values):
    payload = base64.b64encode(json.dumps(values).encode())
    signed = (
        itsdangerous.TimestampSigner(admin_main.SESSION_SECRET).sign(payload).decode()
    )
    client.cookies.clear()
    client.cookies.set(admin_main.SESSION_COOKIE_NAME, signed)


def test_admin_main_change_password_challenge_and_token_routes(admin_api, monkeypatch):
    client, _connection, _templates = admin_api

    async def _user(_request):
        return _admin_user()

    monkeypatch.setattr(admin_main, "get_current_user", _user)
    assert "TEMPLATE:change_password.html" in client.get("/change-password").text

    form = {
        "current_password": "oldpassword",
        "new_password": "newpassword",
        "confirm_password": "newpassword",
    }
    _set_session_cookie(client, {"cognito_challenge": "NEW_PASSWORD_REQUIRED"})
    assert (
        "Missing Cognito challenge session"
        in client.post("/change-password", data=form).text
    )

    _set_session_cookie(
        client,
        {
            "cognito_challenge": "NEW_PASSWORD_REQUIRED",
            "cognito_challenge_session": "challenge-session",
            "cognito_username": "admin@example.com",
        },
    )
    monkeypatch.setattr(
        admin_main,
        "respond_to_new_password_challenge",
        lambda *_args: {"access_token": "new-access"},
    )
    assert (
        client.post("/change-password", data=form, follow_redirects=False).status_code
        == 302
    )

    _set_session_cookie(
        client,
        {
            "cognito_challenge": "NEW_PASSWORD_REQUIRED",
            "cognito_challenge_session": "challenge-session",
        },
    )
    monkeypatch.setattr(
        admin_main,
        "respond_to_new_password_challenge",
        lambda *_args: (_ for _ in ()).throw(ValueError("weak password")),
    )
    assert "weak password" in client.post("/change-password", data=form).text

    _set_session_cookie(client, {})
    assert (
        "missing Cognito access token"
        in client.post("/change-password", data=form).text
    )

    async def _required(_request):
        return _admin_user(require_password_change=True)

    monkeypatch.setattr(admin_main, "get_current_user", _required)
    _set_session_cookie(client, {"cognito_access_token": "access"})
    monkeypatch.setattr(admin_main, "change_cognito_password", lambda *_args: None)
    assert (
        client.post("/change-password", data=form, follow_redirects=False).headers[
            "location"
        ]
        == "/"
    )


class _ListQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter_by(self, **kwargs):
        self.rows = [
            row
            for row in self.rows
            if all(getattr(row, key, None) == value for key, value in kwargs.items())
        ]
        return self

    def order_by(self, *_args):
        return self

    def distinct(self):
        return self

    def count(self):
        return len(self.rows)

    def offset(self, _value):
        return self

    def limit(self, value):
        self.rows = self.rows[:value]
        return self

    def all(self):
        return list(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None


class _ModelSession:
    def __init__(self, model_rows):
        self.model_rows = model_rows

    def query(self, model):
        if model is admin_main.generic_template.category:
            return _ListQuery([("data",)])
        if model is admin_main.generic_instance.category:
            return _ListQuery([("data",)])
        return _ListQuery(self.model_rows.get(model, []))


def test_admin_main_filtered_lists_and_missing_resource_routes(admin_api, monkeypatch):
    client, _connection, _templates = admin_api
    now = SimpleNamespace(isoformat=lambda: "2026-09-02T00:00:00Z")
    template = SimpleNamespace(
        uid=1,
        euid="persisted-template",
        name="Template",
        category="data",
        type="file",
        subtype="result",
        version="1",
        is_deleted=False,
    )
    instance = SimpleNamespace(
        uid=2,
        euid="persisted-instance",
        name="Instance",
        category="data",
        type="file",
        subtype="result",
        bstatus="active",
        version="1",
        json_addl={},
        created_dt=now,
        is_deleted=False,
    )
    session = _ModelSession(
        {
            admin_main.generic_template: [template],
            admin_main.generic_instance: [instance],
        }
    )
    monkeypatch.setattr(admin_main, "get_db", lambda: _Connection(session))
    assert client.get("/templates?category=data").status_code == 200
    assert client.get("/instances?category=data&type_=file").status_code == 200
    assert client.get("/api/templates?category=data").json()["total"] == 1
    assert client.get("/api/instances?category=data").json()["total"] == 1

    empty = _ModelSession(
        {admin_main.generic_template: [], admin_main.generic_instance: []}
    )
    monkeypatch.setattr(admin_main, "get_db", lambda: _Connection(empty))
    assert client.get("/object/missing-object").status_code == 404
    assert client.get("/create-instance/missing-template").status_code == 404
    assert client.get("/api/graph/data?start_euid=missing-object").json() == {
        "elements": {"nodes": [], "edges": []}
    }
    assert (
        client.post(
            "/api/lineage",
            json={"parent_euid": "missing-parent", "child_euid": "missing-child"},
        ).status_code
        == 404
    )
