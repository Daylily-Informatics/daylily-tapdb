"""Focused CLI utility coverage for the explicit-target contract."""

from __future__ import annotations

import os
import re
import socket
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db import Environment

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip(s: str) -> str:
    return _ANSI_RE.sub("", s)


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
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "testclient" / "testdb" / "tapdb-config.yaml"
    )
    clear_cli_context()
    set_cli_context(config_path=cfg_path)
    monkeypatch.setattr(cli_mod, "PID_FILE", tmp_path / "ui.pid")
    monkeypatch.setattr(cli_mod, "LOG_FILE", tmp_path / "ui.log")
    yield cfg_path
    clear_cli_context()


def test_port_is_available_detects_free_and_bound_ports():
    from daylily_tapdb.cli import _port_is_available

    assert _port_is_available("localhost", 59999) is True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("localhost", 0))
        port = sock.getsockname()[1]
        sock.listen(1)
        assert _port_is_available("localhost", port) is False


def test_pid_file_handling(tmp_path: Path):
    from daylily_tapdb.cli import _get_pid

    missing = tmp_path / "missing.pid"
    assert _get_pid(missing) is None

    current = tmp_path / "current.pid"
    current.write_text(str(os.getpid()), encoding="utf-8")
    assert _get_pid(current) == os.getpid()

    stale = tmp_path / "stale.pid"
    stale.write_text("999999999", encoding="utf-8")
    assert _get_pid(stale) is None
    assert not stale.exists()


def test_admin_module_and_extras_are_discoverable(monkeypatch: pytest.MonkeyPatch):
    from daylily_tapdb.cli import _find_admin_module, _require_admin_extras

    assert _find_admin_module() == "admin.main:app"
    monkeypatch.setattr(cli_mod.importlib.util, "find_spec", lambda name: object())
    _require_admin_extras()


def test_admin_extras_missing_reports_required_install(monkeypatch: pytest.MonkeyPatch):
    from daylily_tapdb.cli import _require_admin_extras

    monkeypatch.setattr(cli_mod.importlib.util, "find_spec", lambda name: None)

    with pytest.raises(SystemExit):
        _require_admin_extras()


def test_pg_paths_are_derived_from_explicit_context():
    from daylily_tapdb.cli import active_context_overrides
    from daylily_tapdb.cli.pg import (
        _active_env,
        _get_instance_lock_file,
        _get_postgres_data_dir,
        _get_postgres_log_file,
        _get_postgres_socket_dir,
    )

    cfg_path = Path(active_context_overrides()["config_path"])
    assert _active_env() is Environment.target
    assert (
        _get_postgres_data_dir(Environment.target)
        == cfg_path.parent / "runtime" / "postgres" / "data"
    )
    assert (
        _get_postgres_log_file(Environment.target)
        == cfg_path.parent / "runtime" / "postgres" / "postgresql.log"
    )
    socket_dir = _get_postgres_socket_dir(Environment.target)
    assert socket_dir.name.startswith("tapdb-pg-")
    assert (
        _get_instance_lock_file(Environment.target)
        == cfg_path.parent / "runtime" / "locks" / "instance.lock"
    )


def test_ui_status_uses_explicit_runtime_paths():
    result = runner.invoke(app, ["ui", "status"])

    assert result.exit_code == 0
    out = _strip(result.output)
    assert "not running" in out


def test_ui_stop_not_running_is_successful():
    result = runner.invoke(app, ["ui", "stop"])

    assert result.exit_code == 0
    assert "no ui server running" in _strip(result.output).lower()


def test_db_and_pg_commands_require_config_when_context_cleared():
    clear_cli_context()

    db_result = runner.invoke(app, ["db", "create"])
    pg_result = runner.invoke(app, ["pg", "start-local"])

    assert db_result.exit_code == 1
    assert "TapDB config path is required" in _strip(db_result.output)
    assert pg_result.exit_code != 0
    assert "TapDB config path is required" in _strip(pg_result.output) or isinstance(
        pg_result.exception, RuntimeError
    )


def test_db_config_validate_remains_config_directory_only(tmp_path: Path):
    cfg_dir = tmp_path / "templates"
    cfg_dir.mkdir()

    with patch(
        "daylily_tapdb.cli.db._validate_template_configs",
        return_value=([], []),
    ):
        result = runner.invoke(
            app,
            ["db", "config", "validate", "--config", str(cfg_dir), "--json"],
        )

    assert result.exit_code == 0, result.output
    assert '"templates": 0' in result.output


def test_pg_help_has_no_env_selector():
    result = runner.invoke(app, ["pg", "--help"])

    assert result.exit_code == 0
    assert "--env" not in _strip(result.output)


def test_db_help_has_no_env_selector():
    result = runner.invoke(app, ["db", "--help"])

    assert result.exit_code == 0
    assert "--env" not in _strip(result.output)
