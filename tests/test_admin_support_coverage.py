from __future__ import annotations

import base64
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import itsdangerous
import pytest
from sqlalchemy import create_engine as sa_create_engine

import admin.auth as auth_mod
import admin.db_metrics as metrics_mod
import admin.db_pool as pool_mod
import admin.domain_access as domain_mod


def _signed_cookie(secret: str, payload: dict) -> str:
    signer = itsdangerous.TimestampSigner(secret)
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8"))
    return signer.sign(encoded).decode("utf-8")


def test_domain_access_helpers_cover_allowed_hosts_and_origins():
    assert (
        domain_mod._normalize_host("https://user@example.lsmc.bio:8443/path")
        == "example.lsmc.bio"
    )
    assert domain_mod._normalize_host(" [::1]:8911 ") == "::1"
    assert domain_mod._normalize_host("") == ""

    assert domain_mod.is_approved_domain("portal.lsmc.bio") is True
    assert domain_mod.is_approved_domain("evil.example.com") is False
    assert domain_mod.is_local_host("http://localhost:8911") is True
    assert domain_mod.is_allowed_host("127.0.0.1", allow_local=True) is True
    assert domain_mod.is_allowed_host("127.0.0.1", allow_local=False) is False

    assert (
        domain_mod.is_allowed_origin("https://portal.lsmc.bio", allow_local=False)
        is True
    )
    assert (
        domain_mod.is_allowed_origin("http://localhost:8911", allow_local=True) is True
    )
    assert (
        domain_mod.is_allowed_origin("http://portal.lsmc.bio", allow_local=False)
        is False
    )
    assert domain_mod.is_allowed_origin("notaurl", allow_local=True) is False

    assert domain_mod.validate_allowed_origins(
        ["https://portal.lsmc.bio/", "", "http://localhost:8911"],
        allow_local=True,
    ) == ["https://portal.lsmc.bio", "http://localhost:8911"]
    with pytest.raises(ValueError, match="outside the approved allowlist"):
        domain_mod.validate_allowed_origins(
            ["https://evil.example.com"], allow_local=False
        )

    trusted_hosts = domain_mod.build_trusted_hosts(allow_local=True)
    assert "localhost" in trusted_hosts
    assert "*.lsmc.bio" in trusted_hosts

    regex = re.compile(domain_mod.build_allowed_origin_regex(allow_local=True))
    assert regex.match("https://portal.lsmc.bio")
    assert regex.match("http://localhost:8911")
    assert not regex.match("https://evil.example.com")


def test_db_metrics_helpers_cover_parse_extract_tail_and_summary(tmp_path, monkeypatch):
    assert metrics_mod._parse_bool("yes", default=False) is True
    assert metrics_mod._parse_bool("off", default=True) is False
    assert metrics_mod._parse_bool("maybe", default=True) is True
    assert metrics_mod._sanitize_tsv("a\tb\nc") == "a b c"
    assert metrics_mod._extract_op("UPDATE foo SET name='x'") == "UPDATE"
    assert metrics_mod._extract_op("VACUUM") == "OTHER"
    assert (
        metrics_mod._extract_table_hint('SELECT * FROM "generic_instance"', "SELECT")
        == "generic_instance"
    )
    assert (
        metrics_mod._extract_table_hint(
            "INSERT INTO public.generic_template VALUES (1)", "INSERT"
        )
        == "public.generic_template"
    )
    assert (
        metrics_mod._extract_table_hint(
            "DELETE FROM generic_instance WHERE uid=1", "DELETE"
        )
        == "generic_instance"
    )
    assert metrics_mod._extract_table_hint("bad sql", "UPDATE") == ""

    monkeypatch.setattr(metrics_mod, "active_env_name", lambda default="dev": "DEV")
    monkeypatch.setattr(
        metrics_mod,
        "get_admin_settings_for_env",
        lambda env_name: {"metrics_enabled": False, "metrics_queue_max": 7},
    )
    assert metrics_mod._admin_settings()["metrics_queue_max"] == 7
    assert metrics_mod.metrics_enabled() is False

    modules = dict(sys.modules)
    modules.pop("pytest", None)
    monkeypatch.setattr(metrics_mod.sys, "modules", modules)
    monkeypatch.setattr(
        metrics_mod,
        "_admin_settings",
        lambda: {"metrics_enabled": "true"},
    )
    assert metrics_mod.metrics_enabled() is True

    monkeypatch.setattr(
        metrics_mod,
        "resolve_context",
        lambda **_kwargs: SimpleNamespace(runtime_dir=lambda env: tmp_path / env),
    )
    assert metrics_mod._metrics_root_dir("dev") == tmp_path / "dev" / "metrics"

    metrics_file = tmp_path / "metrics.tsv"
    metrics_file.write_text(
        "ts_utc\tduration_ms\tok\top\ttable_hint\tpath\tmethod\tusername\trowcount\terror_type\n"
        "2026-04-07T10:00:00+00:00\t1.0\t1\tSELECT\tgeneric_instance\t/\tGET\tadmin\t1\t\n"
        "2026-04-07T10:01:00+00:00\t9.5\t0\tUPDATE\tgeneric_template\t/info\tGET\tadmin\t\tValueError\n",
        encoding="utf-8",
    )
    assert metrics_mod._tail_lines(metrics_file, max_lines=2)[0].startswith(
        "2026-04-07T10:00:00"
    )
    assert metrics_mod._tail_lines(metrics_file, max_lines=0) == []
    assert metrics_mod._tail_lines(tmp_path / "missing.tsv", max_lines=5) == []

    monkeypatch.setattr(
        metrics_mod, "current_metrics_path", lambda env_name, now_utc=None: metrics_file
    )
    rows = metrics_mod.read_recent_metrics("dev", max_lines=10)
    assert rows[0]["ok"] is True
    assert rows[1]["error_type"] == "ValueError"
    assert metrics_mod._percentile([], 95.0) == 0.0
    assert metrics_mod._percentile([1.0, 5.0, 9.0], 50.0) == 5.0

    summary = metrics_mod.summarize_metrics(rows)
    assert summary["count"] == 2
    assert summary["max_ms"] == 9.5
    assert summary["by_table"][0]["table_hint"] == "generic_template"

    monkeypatch.setattr(metrics_mod, "metrics_enabled", lambda: True)
    monkeypatch.setattr(metrics_mod, "get_dropped_count", lambda env_name: 3)
    page = metrics_mod.build_metrics_page_context("dev", limit=99999)
    assert page["limit"] == 20000
    assert page["dropped_count"] == 3
    assert page["summary"]["count"] == 2

    monkeypatch.setattr(metrics_mod, "metrics_enabled", lambda: False)
    disabled = metrics_mod.build_metrics_page_context("dev", limit=5)
    assert disabled["metrics_enabled"] is False
    assert "disabled" in disabled["metrics_message"]


def test_db_metrics_writer_cache_and_engine_metrics_callbacks(monkeypatch):
    metrics_mod.stop_all_writers()

    created = []

    class _FakeWriter:
        def __init__(self, env_name):
            self.env_name = env_name
            self.rows = []
            self.stopped = 0

        def enqueue(self, row):
            self.rows.append(row)

        def dropped_count(self):
            return 11

        def stop(self):
            self.stopped += 1

    monkeypatch.setattr(metrics_mod, "metrics_enabled", lambda: True)
    monkeypatch.setattr(
        metrics_mod,
        "TSVMetricsWriter",
        lambda env_name: created.append(_FakeWriter(env_name)) or created[-1],
    )
    writer1 = metrics_mod._get_writer("DEV")
    writer2 = metrics_mod._get_writer("dev")
    assert writer1 is writer2
    assert metrics_mod.get_dropped_count("dev") == 11

    listeners = {}
    monkeypatch.setattr(
        metrics_mod.event,
        "listen",
        lambda _engine, name, fn: listeners.setdefault(name, fn),
    )
    writer = _FakeWriter("dev")
    monkeypatch.setattr(metrics_mod, "_get_writer", lambda env_name: writer)
    metrics_mod._installed_engine_ids.clear()
    engine = object()
    metrics_mod.maybe_install_engine_metrics(engine, env_name="dev")
    metrics_mod.maybe_install_engine_metrics(engine, env_name="dev")
    assert set(listeners) == {
        "before_cursor_execute",
        "after_cursor_execute",
        "handle_error",
    }

    conn = SimpleNamespace(info={})
    cursor = SimpleNamespace(rowcount=4)
    token_path = metrics_mod.request_path_var.set("/info")
    token_method = metrics_mod.request_method_var.set("GET")
    token_user = metrics_mod.db_username_var.set("admin")
    try:
        listeners["before_cursor_execute"](
            conn, cursor, "SELECT * FROM generic_instance", None, None, False
        )
        listeners["after_cursor_execute"](conn, cursor, "", None, None, False)
        listeners["before_cursor_execute"](
            conn, cursor, "UPDATE generic_template SET name='x'", None, None, False
        )
        listeners["handle_error"](
            SimpleNamespace(
                connection=conn,
                cursor=cursor,
                original_exception=ValueError("boom"),
            )
        )
    finally:
        metrics_mod.request_path_var.reset(token_path)
        metrics_mod.request_method_var.reset(token_method)
        metrics_mod.db_username_var.reset(token_user)

    assert writer.rows[0].ok == "1"
    assert writer.rows[0].table_hint == "generic_instance"
    assert writer.rows[1].ok == "0"
    assert writer.rows[1].error_type == "ValueError"

    metrics_mod.stop_all_writers()
    assert writer1.stopped == 1


def test_auth_helpers_cover_disabled_and_shared_auth(monkeypatch):
    monkeypatch.setattr(
        auth_mod,
        "_admin_settings",
        lambda: {
            "auth_mode": "disabled",
            "disabled_user_email": "",
            "disabled_user_role": "bad-role",
            "shared_host_session_secret": "secret-a",
            "shared_host_session_cookie": "session",
            "shared_host_session_max_age_seconds": "77",
        },
    )
    assert auth_mod._auth_disabled() is True
    assert auth_mod._shared_auth_enabled() is False
    assert auth_mod._disabled_auth_user()["username"] == "tapdb-admin@localhost"
    assert auth_mod._disabled_auth_user()["role"] == "admin"
    assert auth_mod._bloom_session_secret() == "secret-a"
    assert auth_mod._bloom_session_cookie_name() == "session"
    assert auth_mod._bloom_session_max_age() == 77

    request = SimpleNamespace(cookies={})
    assert auth_mod._extract_bloom_user(request) is None

    cookie = _signed_cookie(
        "secret-a",
        {"user_data": {"email": "user@example.com", "role": "super-admin"}},
    )
    request = SimpleNamespace(cookies={"session": cookie})
    assert auth_mod._extract_bloom_user(request) == {
        "email": "user@example.com",
        "role": "user",
    }

    monkeypatch.setattr(auth_mod, "_shared_auth_enabled", lambda: True)
    monkeypatch.setattr(
        auth_mod,
        "_extract_bloom_user",
        lambda _request: {"email": "shared@example.com", "role": "admin"},
    )
    monkeypatch.setattr(auth_mod, "get_user_by_username", lambda _username: None)
    monkeypatch.setattr(
        auth_mod,
        "get_or_create_user_from_email",
        lambda email, *, role: {"uid": 42, "username": email, "role": role},
    )
    request = SimpleNamespace(session={"cognito_challenge": "NEW_PASSWORD_REQUIRED"})
    user = auth_mod._resolve_shared_auth_user(request)
    assert user["uid"] == 42
    assert request.session["user_uid"] == 42
    assert "cognito_challenge" not in request.session


def test_auth_database_and_cognito_helpers_cover_error_paths(monkeypatch):
    class _Conn:
        def __init__(self, user_obj=None, uid_obj=None):
            self.app_username = None
            self.user_obj = user_obj
            self.uid_obj = uid_obj

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def session_scope(self, commit=False):
            class _Scope:
                def __enter__(self):
                    return SimpleNamespace()

                def __exit__(self, exc_type, exc, tb):
                    return False

            return _Scope()

    session_user = SimpleNamespace(
        to_session_user=lambda: {"uid": 7, "username": "alice@example.com"}
    )
    monkeypatch.setattr(auth_mod, "get_db", lambda: _Conn())
    monkeypatch.setattr(
        auth_mod,
        "get_by_login_or_email",
        lambda _session, _username, include_inactive=False: session_user,
    )
    monkeypatch.setattr(
        auth_mod,
        "get_actor_user_by_uid",
        lambda _session, _uid, include_inactive=False: session_user,
    )
    assert auth_mod.get_user_by_username("alice@example.com") == {
        "uid": 7,
        "username": "alice@example.com",
    }
    assert auth_mod.get_user_by_uid(7) == {"uid": 7, "username": "alice@example.com"}

    monkeypatch.setattr(
        auth_mod,
        "create_or_get",
        lambda _session, **kwargs: (
            SimpleNamespace(
                is_active=False,
                to_session_user=lambda: {
                    "uid": 8,
                    "username": kwargs["login_identifier"],
                },
            ),
            True,
        ),
    )
    with pytest.raises(RuntimeError, match="is inactive"):
        auth_mod.get_or_create_user_from_email("alice@example.com", role="user")
    with pytest.raises(ValueError, match="email is required"):
        auth_mod.get_or_create_user_from_email("", role="user")
    with pytest.raises(ValueError, match="invalid role"):
        auth_mod.get_or_create_user_from_email("alice@example.com", role="owner")

    calls = []

    class UsernameExistsError(Exception):
        pass

    class InvalidPasswordError(Exception):
        pass

    class _Cognito:
        exceptions = SimpleNamespace(
            UsernameExistsException=UsernameExistsError,
            InvalidPasswordException=InvalidPasswordError,
        )

        def __init__(self):
            self.calls = calls

        def admin_create_user(self, **kwargs):
            self.calls.append(("create", kwargs))

        def admin_set_user_password(self, **kwargs):
            self.calls.append(("set-password", kwargs))

    class _Auth:
        def __init__(self):
            self.cognito = _Cognito()
            self.user_pool_id = "pool-123"

        def authenticate(self, **kwargs):
            return kwargs

        def respond_to_new_password_challenge(self, **kwargs):
            return kwargs

        def change_password(self, **kwargs):
            calls.append(("change-password", kwargs))

    auth_client = _Auth()
    monkeypatch.setattr(auth_mod, "get_cognito_auth", lambda: auth_client)
    auth_mod.create_cognito_user_account(
        "Alice@example.com",
        "Password1!",
        display_name="Alice",
    )
    assert calls[0][0] == "create"
    assert auth_mod.authenticate_with_cognito("alice@example.com", "pw") == {
        "email": "alice@example.com",
        "password": "pw",
    }
    assert auth_mod.respond_to_new_password_challenge(
        "alice@example.com", "new", "sess"
    ) == {
        "email": "alice@example.com",
        "new_password": "new",
        "session": "sess",
    }
    auth_mod.change_cognito_password("token", "old", "new")
    assert calls[-1][0] == "change-password"

    monkeypatch.setattr(
        auth_mod,
        "set_last_login",
        lambda _session, user_uid: calls.append(("last-login", user_uid)),
    )
    auth_mod.update_last_login(11)
    assert calls[-1] == ("last-login", 11)
    assert (
        auth_mod._tapdb_base_path(SimpleNamespace(scope={"root_path": "/tapdb/"}))
        == "/tapdb"
    )
    assert (
        auth_mod._tapdb_url(SimpleNamespace(scope={"root_path": "/tapdb"}), "/login")
        == "/tapdb/login"
    )


def test_db_pool_helpers_cover_engine_build_session_scope_and_dispose(
    monkeypatch, caplog
):
    assert pool_mod._parse_bool("yes", default=False) is True
    assert pool_mod._parse_bool("wat", default=True) is True
    assert pool_mod._audit_username_for_session("") == "unknown"

    class _Trans:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    class _Session:
        def __init__(self, explode=False):
            self.trans = _Trans()
            self.closed = 0
            self.executed = []
            self.explode = explode

        def begin(self):
            return self.trans

        def execute(self, *args):
            if self.explode:
                raise RuntimeError("bad set local")
            self.executed.append(args)

        def close(self):
            self.closed += 1

    session = _Session()
    bundle = pool_mod.EngineBundle(
        env_name="dev",
        engine=sa_create_engine("sqlite://"),
        SessionFactory=lambda: session,
        cfg={},
    )
    conn = pool_mod.AdminDBConnection(bundle)
    conn.app_username = "admin"
    monkeypatch.setattr(
        "admin.db_metrics.db_username_var",
        SimpleNamespace(set=lambda value: value, reset=lambda token: None),
    )

    with conn.session_scope(commit=True) as active:
        assert active is session
    assert session.trans.commits == 1
    assert session.closed == 1

    caplog.clear()
    pool_mod._set_audit_username(_Session(explode=True), "admin")
    assert "Could not set session audit username" in caplog.text

    captured = {}
    monkeypatch.setattr(
        pool_mod,
        "_admin_settings",
        lambda _env: {
            "db_pool_size": 8,
            "db_max_overflow": 2,
            "db_pool_timeout": 11,
            "db_pool_recycle": 22,
        },
    )
    monkeypatch.setattr(
        pool_mod,
        "create_engine",
        lambda url, **kwargs: (
            captured.setdefault("calls", []).append((url, kwargs))
            or SimpleNamespace(dispose=lambda: None)
        ),
    )
    pool_mod._create_engine(
        pool_mod.URL.create("postgresql+psycopg2", username="tapdb", host="db"),
        echo_sql=True,
        env_name="dev",
    )
    assert captured["calls"][0][1]["pool_size"] == 8

    listeners = {}
    monkeypatch.setattr(
        pool_mod.event,
        "listen",
        lambda _engine, name, fn, **_kwargs: listeners.setdefault(name, fn),
    )
    monkeypatch.setattr(
        pool_mod.AuroraConnectionBuilder,
        "get_iam_auth_token",
        lambda **_kwargs: "iam-token",
    )
    pool_mod._attach_aurora_password_provider(
        SimpleNamespace(),
        region="us-west-2",
        host="db.local",
        port=5432,
        user="tapdb",
        aws_profile=None,
        iam_auth=True,
        password="",
    )
    cparams = {}
    listeners["do_connect"](None, None, None, cparams)
    assert cparams["password"] == "iam-token"

    monkeypatch.setattr(
        pool_mod,
        "_create_engine",
        lambda url, *, echo_sql, env_name: (
            captured.setdefault("built", []).append((url, echo_sql, env_name))
            or SimpleNamespace(dispose=lambda: None)
        ),
    )
    monkeypatch.setattr(
        pool_mod.AuroraConnectionBuilder,
        "ensure_ca_bundle",
        lambda: Path("/tmp/rds-ca.pem"),
    )
    attach_calls = []
    monkeypatch.setattr(
        pool_mod,
        "_attach_aurora_password_provider",
        lambda engine, **kwargs: attach_calls.append(kwargs),
    )
    aurora_engine = pool_mod._build_engine_for_cfg(
        {
            "engine_type": "aurora",
            "host": "db.local",
            "port": "5432",
            "database": "tapdb_dev",
            "user": "tapdb",
            "password": "secret",
            "region": "us-west-2",
            "iam_auth": "false",
        },
        env_name="dev",
    )
    assert aurora_engine is not None
    assert attach_calls[0]["iam_auth"] is False

    monkeypatch.setattr(
        pool_mod,
        "get_db_config_for_env",
        lambda _env: {
            "engine_type": "local",
            "host": "localhost",
            "port": "5432",
            "database": "tapdb_dev",
            "user": "tapdb",
            "password": "",
        },
    )
    monkeypatch.setattr(
        pool_mod,
        "_build_engine_for_cfg",
        lambda cfg, *, env_name: sa_create_engine("sqlite://"),
    )
    monkeypatch.setattr(
        "admin.db_metrics.maybe_install_engine_metrics",
        lambda engine, env_name: captured.setdefault("metrics", []).append(env_name),
    )
    pool_mod._clear_engine_cache_for_tests()
    bundle = pool_mod.get_engine_bundle("DEV")
    assert bundle.env_name == "dev"
    assert captured["metrics"] == ["dev"]
    assert isinstance(pool_mod.get_db_connection("dev"), pool_mod.AdminDBConnection)

    bad_engine = SimpleNamespace(
        dispose=lambda: (_ for _ in ()).throw(RuntimeError("dispose failed"))
    )
    pool_mod._bundles_by_env["bad"] = pool_mod.EngineBundle(
        env_name="bad",
        engine=bad_engine,
        SessionFactory=lambda: None,
        cfg={},
    )
    caplog.clear()
    pool_mod.dispose_all_engines()
    assert "Error disposing engine for env bad" in caplog.text
