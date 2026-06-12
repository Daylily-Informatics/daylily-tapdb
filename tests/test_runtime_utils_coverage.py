"""Runtime/admin utility coverage for explicit TapDB targets."""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from fastapi import FastAPI, HTTPException
from sqlalchemy.engine import URL
from starlette.requests import Request

import daylily_tapdb.cli.admin_server as admin_server_mod
import daylily_tapdb.web.factory as web_factory_mod
import daylily_tapdb.web.runtime as runtime_mod
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.web.bridge import TapdbHostBridge


def _write_config(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    domain_registry = path.parent / "domain_code_registry.json"
    prefix_registry = path.parent / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test"}}}\n',
        encoding="utf-8",
    )
    prefix_registry.write_text(
        (
            '{"version":"0.4.0","ownership":{"Z":{'
            '"TPX":{"issuer_app_code":"daylily-tapdb"},'
            '"EDG":{"issuer_app_code":"daylily-tapdb"},'
            '"ADT":{"issuer_app_code":"daylily-tapdb"},'
            '"SYS":{"issuer_app_code":"daylily-tapdb"},'
            '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
        ),
        encoding="utf-8",
    )
    path.write_text(
        "meta:\n"
        "  config_version: 4\n"
        "  client_id: testclient\n"
        "  database_name: testdb\n"
        "  owner_repo_name: daylily-tapdb\n"
        f"  domain_registry_path: {domain_registry}\n"
        f"  prefix_ownership_registry_path: {prefix_registry}\n"
        "admin:\n"
        "  session:\n"
        "    secret: secret123\n"
        "target:\n"
        "  engine_type: local\n"
        "  host: localhost\n"
        "  port: '5533'\n"
        "  ui_port: '8911'\n"
        "  domain_code: Z\n"
        "  user: tapdb\n"
        "  password: ''\n"
        "  database: tapdb_shared\n"
        "  schema_name: tapdb_testdb\n"
        "safety:\n"
        "  safety_tier: shared\n"
        "  destructive_operations: confirm_required\n",
        encoding="utf-8",
    )
    os.chmod(path, 0o600)
    return path


@pytest.fixture(autouse=True)
def _explicit_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    clear_cli_context()
    set_cli_context(config_path=cfg_path)
    yield cfg_path
    clear_cli_context()
    runtime_mod._clear_runtime_cache_for_tests()


def test_admin_server_parser_has_no_env_argument(tmp_path: Path):
    parser = admin_server_mod._build_parser()

    args = parser.parse_args(
        [
            "--config",
            str(tmp_path / "tapdb-config.yaml"),
            "--host",
            "localhost",
            "--port",
            "8911",
            "--ssl-keyfile",
            "key.pem",
            "--ssl-certfile",
            "cert.pem",
        ]
    )

    assert args.config.endswith("tapdb-config.yaml")
    assert not hasattr(args, "env")


def test_admin_server_context_file_helpers_round_trip(tmp_path: Path) -> None:
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    set_cli_context(config_path=cfg_path)

    written = admin_server_mod._write_context_file(
        config_path=str(cfg_path),
        host="localhost",
        port=8911,
        tls_mode="https",
    )

    assert written.parent == cfg_path.parent / "runtime" / "ui"
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["target"] == "explicit"
    assert payload["tls_mode"] == "https"
    assert payload["config_path"] == str(cfg_path.resolve())


def test_admin_server_load_admin_app_sets_explicit_context(monkeypatch, tmp_path: Path):
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    fake_app = FastAPI()
    fake_admin = SimpleNamespace(app=fake_app)

    monkeypatch.setattr(
        admin_server_mod.importlib, "import_module", lambda name: fake_admin
    )
    monkeypatch.setattr(admin_server_mod.importlib, "reload", lambda module: module)

    app = admin_server_mod.load_admin_app(config_path=str(cfg_path))

    assert app is fake_app
    assert app.state.tapdb_admin_module is fake_admin


def test_runtime_db_connection_session_scope_sets_search_path_and_audit_username():
    events: list[tuple[str, object]] = []

    class FakeTx:
        def commit(self):
            events.append(("commit", None))

        def rollback(self):
            events.append(("rollback", None))

    class FakeSession:
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def begin(self):
            return FakeTx()

        def execute(self, stmt, params=None):
            events.append(("execute", params))

        def close(self):
            events.append(("close", None))

    bundle = runtime_mod.RuntimeBundle(
        config_path="/tmp/tapdb-config.yaml",
        target_name="target",
        engine=SimpleNamespace(),
        SessionFactory=lambda: FakeSession(),
        cfg={"domain_code": "Z", "owner_repo_name": "lsmc-atlas"},
        schema_name="tapdb_testdb",
    )
    conn = runtime_mod.RuntimeDBConnection(bundle)
    conn.app_username = "alice@example.com"

    with conn.session_scope(commit=True):
        pass

    assert ("commit", None) in events
    assert ("execute", {"schema_name": "tapdb_testdb"}) in events
    assert ("execute", {"code": "Z"}) in events
    assert ("execute", {"owner": "lsmc-atlas"}) in events
    assert ("execute", {"username": "alice@example.com"}) in events


def test_runtime_db_connection_requires_identity_scope_for_postgres():
    class FakeTx:
        def commit(self):
            raise AssertionError("commit must not run")

        def rollback(self):
            pass

    class FakeSession:
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def begin(self):
            return FakeTx()

        def execute(self, stmt, params=None):
            del stmt, params

        def close(self):
            pass

    bundle = runtime_mod.RuntimeBundle(
        config_path="/tmp/tapdb-config.yaml",
        target_name="target",
        engine=SimpleNamespace(),
        SessionFactory=lambda: FakeSession(),
        cfg={"domain_code": "Z"},
        schema_name="tapdb_testdb",
    )
    conn = runtime_mod.RuntimeDBConnection(bundle)

    with pytest.raises(RuntimeError, match="owner_repo_name"):
        with conn.session_scope(commit=True):
            pass


def test_runtime_helpers_handle_non_postgres_and_rollback_paths(caplog):
    assert runtime_mod._parse_bool(None, default=True) is True
    assert runtime_mod._parse_bool("off", default=True) is False
    assert runtime_mod._parse_bool("wat", default=True) is True
    assert runtime_mod._audit_username_for_session("  ") == "unknown"
    assert runtime_mod._require_schema_name({"schema_name": " tapdb_x "}) == "tapdb_x"
    with pytest.raises(RuntimeError, match="schema_name"):
        runtime_mod._require_schema_name({})

    events: list[str] = []

    class FakeTx:
        def commit(self):
            events.append("commit")

        def rollback(self):
            events.append("rollback")

    class FakeSession:
        bind = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

        def begin(self):
            return FakeTx()

        def execute(self, stmt, params=None):
            del stmt, params
            raise AssertionError(
                "sqlite session should not receive search path or identity"
            )

        def close(self):
            events.append("close")

    bundle = runtime_mod.RuntimeBundle(
        config_path="/tmp/tapdb-config.yaml",
        target_name="target",
        engine=SimpleNamespace(),
        SessionFactory=lambda: FakeSession(),
        cfg={},
        schema_name="tapdb_testdb",
    )
    conn = runtime_mod.RuntimeDBConnection(bundle)
    with conn.session_scope(commit=False):
        pass
    assert events == ["rollback", "close"]

    class AuditFailSession(FakeSession):
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def execute(self, stmt, params=None):
            if params and "username" in params:
                raise RuntimeError("audit unavailable")
            events.append("execute")

    caplog.set_level("WARNING")
    runtime_mod._set_audit_username(AuditFailSession(), "user@example.com")
    assert "Could not set session audit username" in caplog.text


def test_runtime_engine_cache_key_includes_schema(monkeypatch):
    builds: list[str] = []

    def fake_get_db_config(config_path):
        schema = "schema_a" if config_path.endswith("a.yaml") else "schema_b"
        return {
            "config_path": config_path,
            "schema_name": schema,
            "engine_type": "local",
            "host": "localhost",
            "port": "5533",
            "database": "tapdb_shared",
            "user": "tapdb",
            "password": "",
        }

    monkeypatch.setattr(runtime_mod, "get_db_config", fake_get_db_config)
    monkeypatch.setattr(
        runtime_mod,
        "_build_engine_for_cfg",
        lambda cfg, *, config_path: (
            builds.append(cfg["schema_name"]) or SimpleNamespace()
        ),
    )
    monkeypatch.setattr(runtime_mod, "sessionmaker", lambda **kwargs: lambda: None)

    runtime_mod.get_db("/tmp/a.yaml")
    runtime_mod.get_db("/tmp/b.yaml")
    runtime_mod.get_db("/tmp/a.yaml")

    assert builds == ["schema_a", "schema_b"]


def test_runtime_create_engine_uses_admin_pool_settings(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        runtime_mod,
        "get_admin_settings",
        lambda config_path: {
            "db_pool_size": "7",
            "db_max_overflow": "8",
            "db_pool_timeout": "9",
            "db_pool_recycle": "10",
        },
    )
    monkeypatch.setattr(
        runtime_mod,
        "create_engine",
        lambda url, **kwargs: captured.update({"url": url, **kwargs}) or "engine",
    )

    engine = runtime_mod._create_engine(
        URL.create("postgresql+psycopg2", host="localhost", database="tapdb"),
        config_path="/tmp/tapdb-config.yaml",
        echo_sql=True,
    )

    assert engine == "engine"
    assert captured["pool_size"] == 7
    assert captured["max_overflow"] == 8
    assert captured["pool_timeout"] == 9
    assert captured["pool_recycle"] == 10
    assert captured["pool_pre_ping"] is True
    assert captured["echo"] is True


def test_runtime_aurora_password_provider_paths(monkeypatch):
    listeners = []
    monkeypatch.setattr(
        runtime_mod.event,
        "listen",
        lambda engine, name, callback: listeners.append((engine, name, callback)),
    )
    monkeypatch.setattr(
        runtime_mod.AuroraConnectionBuilder,
        "get_iam_auth_token",
        lambda **kwargs: f"iam:{kwargs['user']}@{kwargs['host']}",
    )
    monkeypatch.setattr(
        runtime_mod.AuroraConnectionBuilder,
        "get_secret_password",
        lambda **kwargs: f"secret:{kwargs['secret_arn']}",
    )

    engine = object()
    runtime_mod._attach_aurora_password_provider(
        engine,
        region="us-west-2",
        host="db.example",
        port=5432,
        user="tapdb",
        aws_profile="dev",
        iam_auth=True,
        secret_arn=None,
        password="",
    )
    params = {}
    listeners[-1][2](None, None, [], params)
    assert params["password"] == "iam:tapdb@db.example"

    runtime_mod._attach_aurora_password_provider(
        engine,
        region="us-west-2",
        host="db.example",
        port=5432,
        user="tapdb",
        aws_profile=None,
        iam_auth=False,
        secret_arn="arn:secret",
        password="",
    )
    params = {}
    listeners[-1][2](None, None, [], params)
    assert params["password"] == "secret:arn:secret"

    runtime_mod._attach_aurora_password_provider(
        engine,
        region="us-west-2",
        host="db.example",
        port=5432,
        user="tapdb",
        aws_profile=None,
        iam_auth=False,
        secret_arn=None,
        password="pw",
    )
    params = {}
    listeners[-1][2](None, None, [], params)
    assert params["password"] == "pw"

    runtime_mod._attach_aurora_password_provider(
        engine,
        region="us-west-2",
        host="db.example",
        port=5432,
        user="tapdb",
        aws_profile=None,
        iam_auth=False,
        secret_arn=None,
        password="",
    )
    with pytest.raises(ValueError, match="requires a password"):
        listeners[-1][2](None, None, [], {})


def test_runtime_build_engine_for_local_and_aurora(monkeypatch, tmp_path: Path):
    calls = []
    monkeypatch.setenv("ECHO_SQL", "true")
    monkeypatch.setattr(
        runtime_mod,
        "_create_engine",
        lambda url, *, config_path, echo_sql: (
            calls.append((url, config_path, echo_sql)) or SimpleNamespace(url=url)
        ),
    )
    monkeypatch.setattr(runtime_mod, "_attach_aurora_password_provider", mock.Mock())
    monkeypatch.setattr(
        runtime_mod.AuroraConnectionBuilder,
        "ensure_ca_bundle",
        lambda: tmp_path / "ca.pem",
    )

    local = runtime_mod._build_engine_for_cfg(
        {
            "schema_name": "tapdb_s",
            "engine_type": "local",
            "host": "localhost",
            "port": "5432",
            "database": "tapdb",
            "user": "tapdb",
            "password": "pw",
        },
        config_path="/tmp/local.yaml",
    )
    aurora = runtime_mod._build_engine_for_cfg(
        {
            "schema_name": "tapdb_s",
            "engine_type": "aurora",
            "host": "db.example",
            "hostaddr": "10.0.0.1",
            "port": "5432",
            "database": "tapdb",
            "user": "tapdb",
            "password": "",
            "region": "us-west-2",
            "iam_auth": "yes",
            "aws_profile": "dev",
        },
        config_path="/tmp/aurora.yaml",
    )

    assert local.url.get_backend_name() == "postgresql"
    assert aurora.url.query["sslmode"] == "verify-full"
    assert calls[0][2] is True
    assert runtime_mod._attach_aurora_password_provider.call_count == 1


def test_runtime_clear_cache_logs_dispose_errors(caplog):
    class BadEngine:
        def dispose(self):
            raise RuntimeError("dispose failed")

    runtime_mod._bundles[("/tmp/cfg.yaml", "tapdb_s")] = runtime_mod.RuntimeBundle(
        config_path="/tmp/cfg.yaml",
        target_name="target",
        engine=BadEngine(),
        SessionFactory=lambda: None,
        cfg={"schema_name": "tapdb_s"},
        schema_name="tapdb_s",
    )

    caplog.set_level("WARNING")
    runtime_mod._clear_runtime_cache_for_tests()

    assert "Error disposing DAG runtime engine" in caplog.text


@pytest.mark.anyio
async def test_require_tapdb_api_user_rejects_anonymous(monkeypatch):
    async def _no_user(request):
        return None

    monkeypatch.setattr("admin.auth.get_current_user", _no_user)
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})

    with pytest.raises(HTTPException) as exc_info:
        await web_factory_mod.require_tapdb_api_user(request)

    assert exc_info.value.status_code == 401


def test_web_factory_builds_app_from_explicit_config(monkeypatch, tmp_path: Path):
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    fake_app = FastAPI()
    fake_admin = SimpleNamespace(TEMPLATES_DIR=tmp_path)
    fake_app.state.tapdb_admin_module = fake_admin
    fake_admin.templates = SimpleNamespace(loader=None, globals={})

    monkeypatch.setattr(
        "daylily_tapdb.cli.admin_server.load_admin_app",
        lambda config_path: fake_app,
    )
    monkeypatch.setattr(
        web_factory_mod,
        "create_tapdb_dag_router",
        lambda **kwargs: FastAPI().router,
    )

    app = web_factory_mod.create_tapdb_web_app(config_path=str(cfg_path))

    assert app is fake_app
    assert app.state.tapdb_host_bridge is None
    assert app.state.tapdb_dag_router_attached is True


def test_web_factory_wraps_host_bridge(monkeypatch, tmp_path: Path):
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    fake_app = FastAPI()
    fake_admin = SimpleNamespace(TEMPLATES_DIR=tmp_path)
    fake_app.state.tapdb_admin_module = fake_admin
    fake_admin.templates = SimpleNamespace(loader=None, globals={})
    bridge = TapdbHostBridge(service_name="atlas", auth_mode="host_session")

    monkeypatch.setattr(
        "daylily_tapdb.cli.admin_server.load_admin_app",
        lambda config_path: fake_app,
    )
    monkeypatch.setattr(
        web_factory_mod,
        "create_tapdb_dag_router",
        lambda **kwargs: FastAPI().router,
    )

    wrapped = web_factory_mod.create_tapdb_web_app(
        config_path=str(cfg_path),
        host_bridge=bridge,
    )

    assert isinstance(wrapped, web_factory_mod.TapdbHostBridgeMount)
