"""Additional explicit-target CLI/runtime coverage."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import daylily_tapdb.cli.pg as pg_mod
import daylily_tapdb.cli.user as user_mod
import daylily_tapdb.web.runtime as runtime_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db import Environment

runner = CliRunner()


def _write_config(path: Path, *, socket_dir: str = "") -> Path:
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
    unix_socket_line = f"  unix_socket_dir: {socket_dir}\n" if socket_dir else ""
    path.write_text(
        "meta:\n"
        "  config_version: 4\n"
        "  client_id: testclient\n"
        "  database_name: testdb\n"
        "  owner_repo_name: daylily-tapdb\n"
        f"  domain_registry_path: {domain_registry}\n"
        f"  prefix_ownership_registry_path: {prefix_registry}\n"
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
        f"{unix_socket_line}"
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


def test_user_cli_rejects_invalid_role_before_db_access() -> None:
    result = runner.invoke(
        app, ["users", "add", "--username", "alice", "--role", "owner"]
    )

    assert result.exit_code == 1
    assert "Invalid role" in result.output


def test_user_open_connection_uses_explicit_target_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    class FakeConnection:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr(user_mod, "TAPDBConnection", FakeConnection)

    conn = user_mod._open_connection(Environment.target, app_username="tester")

    assert isinstance(conn, FakeConnection)
    assert seen["db_name"] == "tapdb_shared"
    assert seen["schema_name"] == "tapdb_testdb"
    assert seen["app_username"] == "tester"


def test_runtime_requires_schema_name() -> None:
    with pytest.raises(RuntimeError, match="schema_name"):
        runtime_mod._require_schema_name({})

    assert (
        runtime_mod._require_schema_name({"schema_name": "tapdb_testdb"})
        == "tapdb_testdb"
    )


def test_runtime_connection_sets_search_path_and_audit_username(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        cfg={},
        schema_name="tapdb_testdb",
    )
    conn = runtime_mod.RuntimeDBConnection(bundle)
    conn.app_username = "alice@example.com"

    with conn.session_scope(commit=True):
        pass

    assert ("commit", None) in events
    assert any(item == ("execute", {"schema_name": "tapdb_testdb"}) for item in events)
    assert any(
        item == ("execute", {"username": "alice@example.com"}) for item in events
    )


def test_runtime_get_db_caches_by_config_and_schema(monkeypatch: pytest.MonkeyPatch):
    builds: list[dict[str, str]] = []

    monkeypatch.setattr(
        runtime_mod,
        "get_db_config",
        lambda config_path: {
            "config_path": config_path,
            "schema_name": "tapdb_testdb",
            "engine_type": "local",
            "host": "localhost",
            "port": "5533",
            "database": "tapdb_shared",
            "user": "tapdb",
            "password": "",
        },
    )

    def _fake_build(cfg, *, config_path):
        builds.append(dict(cfg))
        return SimpleNamespace()

    monkeypatch.setattr(runtime_mod, "_build_engine_for_cfg", _fake_build)
    monkeypatch.setattr(runtime_mod, "sessionmaker", lambda **kwargs: lambda: None)
    runtime_mod._clear_runtime_cache_for_tests()

    first = runtime_mod.get_db("/tmp/tapdb-config.yaml")
    second = runtime_mod.get_db("/tmp/tapdb-config.yaml")

    assert isinstance(first, runtime_mod.RuntimeDBConnection)
    assert isinstance(second, runtime_mod.RuntimeDBConnection)
    assert len(builds) == 1


def test_pg_socket_dir_prefers_explicit_target_field(
    tmp_path: Path,
) -> None:
    socket_dir = tmp_path / "custom-socket"
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml", socket_dir=str(socket_dir))
    set_cli_context(config_path=cfg_path)

    assert pg_mod._get_postgres_socket_dir(Environment.target) == socket_dir


def test_pg_active_env_is_always_explicit_target() -> None:
    assert pg_mod._active_env() is Environment.target


def test_pg_stop_status_logs_and_restart_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pg_mod, "_is_pg_running", lambda: (False, ""))
    monkeypatch.setattr(
        pg_mod, "_get_pg_service_cmd", lambda: ("unknown", [], [], Path())
    )

    assert runner.invoke(app, ["pg", "status"]).exit_code == 0
    assert runner.invoke(app, ["pg", "stop"]).exit_code == 0


def test_pg_start_local_rejects_missing_postgres_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pg_mod.shutil, "which", lambda name: None)

    result = runner.invoke(app, ["pg", "start-local"])

    assert result.exit_code == 1
    assert (
        "Data directory not initialized" in result.output
        or "pg_ctl is required" in result.output
    )
