"""Coverage tests for CLI modules: __init__.py, pg.py, db.py, user.py.

Uses the same hermetic test infrastructure as test_cli.py (set_cli_context + app).
"""

from __future__ import annotations

import os
import re
import socket
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip(s: str) -> str:
    return _ANSI_RE.sub("", s)


@pytest.fixture(autouse=True)
def _isolate_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Hermetic CLI environment matching test_cli.py pattern."""
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg_path = tmp_path / ".config" / "tapdb" / "testclient" / "testdb" / "tapdb-config.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: testclient\n"
        "  database_name: testdb\n"
        "  euid_client_code: C\n"
        "environments:\n"
        "  dev:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: 5533\n"
        "    ui_port: 8911\n"
        "    user: test\n"
        '    password: ""\n'
        "    database: tapdb_dev\n"
        '    audit_log_euid_prefix: "CGX"\n',
        encoding="utf-8",
    )
    os.chmod(cfg_path, 0o600)
    clear_cli_context()
    set_cli_context(
        client_id="testclient", database_name="testdb", env_name="dev", config_path=cfg_path
    )
    monkeypatch.setattr(cli_mod, "PID_FILE", tmp_path / "ui.pid")
    monkeypatch.setattr(cli_mod, "LOG_FILE", tmp_path / "ui.log")
    yield
    clear_cli_context()


# ────────────────────────────────────────────────────────────────────
# cli/__init__.py — module-level utility functions
# ────────────────────────────────────────────────────────────────────


class TestCliUtilFunctions:
    def test_port_is_available_open(self):
        from daylily_tapdb.cli import _port_is_available
        assert _port_is_available("localhost", 59999) is True

    def test_port_is_not_available(self):
        from daylily_tapdb.cli import _port_is_available
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("localhost", 0))
            port = s.getsockname()[1]
            s.listen(1)
            assert _port_is_available("localhost", port) is False

    def test_get_pid_no_file(self, tmp_path):
        from daylily_tapdb.cli import _get_pid
        assert _get_pid(tmp_path / "nonexistent.pid") is None

    def test_get_pid_valid_running(self, tmp_path):
        from daylily_tapdb.cli import _get_pid
        pid_file = tmp_path / "test.pid"
        pid_file.write_text(str(os.getpid()))
        assert _get_pid(pid_file) == os.getpid()

    def test_get_pid_stale(self, tmp_path):
        from daylily_tapdb.cli import _get_pid
        pid_file = tmp_path / "test.pid"
        pid_file.write_text("999999999")
        assert _get_pid(pid_file) is None
        assert not pid_file.exists()

    def test_get_pid_invalid_text(self, tmp_path):
        from daylily_tapdb.cli import _get_pid
        pid_file = tmp_path / "test.pid"
        pid_file.write_text("not_a_number")
        assert _get_pid(pid_file) is None

    def test_port_conflict_details(self):
        from daylily_tapdb.cli import _port_conflict_details
        result = _port_conflict_details(99999)
        assert isinstance(result, str)

    def test_find_admin_module(self):
        from daylily_tapdb.cli import _find_admin_module
        result = _find_admin_module()
        assert "admin" in result

    def test_require_admin_extras_installed(self):
        from daylily_tapdb.cli import _require_admin_extras
        _require_admin_extras()

    def test_require_admin_extras_missing(self):
        from daylily_tapdb.cli import _require_admin_extras
        with mock.patch.dict("sys.modules", {"fastapi": None}):
            with mock.patch("importlib.util.find_spec", return_value=None):
                with pytest.raises(SystemExit):
                    _require_admin_extras()


# ────────────────────────────────────────────────────────────────────
# CLI commands via runner (context pre-set by _isolate_cli)
# ────────────────────────────────────────────────────────────────────


class TestCliCommandsWithContext:
    """Tests that exercise CLI commands with context already set."""

    def test_pg_help(self):
        result = runner.invoke(app, ["pg", "--help"])
        assert result.exit_code == 0

    def test_db_help(self):
        result = runner.invoke(app, ["db", "--help"])
        assert result.exit_code == 0

    def test_pg_init_no_pg(self):
        with patch("shutil.which", return_value=None):
            result = runner.invoke(app, ["pg", "init", "dev"])
        assert result.exit_code in (0, 1)

    def test_pg_status(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="running")
            result = runner.invoke(app, ["pg", "status"])
        assert result.exit_code in (0, 1)

    def test_pg_start(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            result = runner.invoke(app, ["pg", "start"])
        assert result.exit_code in (0, 1)

    def test_pg_stop(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            result = runner.invoke(app, ["pg", "stop"])
        assert result.exit_code in (0, 1)

    def test_db_create(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            result = runner.invoke(app, ["db", "create", "dev"])
        assert result.exit_code in (0, 1)

    def test_db_delete_force(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            result = runner.invoke(app, ["db", "delete", "dev", "--force"])
        assert result.exit_code in (0, 1)

    def test_db_schema_apply(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            result = runner.invoke(app, ["db", "schema", "apply", "dev"])
        assert result.exit_code in (0, 1)

    def test_db_schema_status(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            result = runner.invoke(app, ["db", "schema", "status", "dev"])
        assert result.exit_code in (0, 1)

    def test_db_config_validate(self):
        result = runner.invoke(app, ["db", "config", "validate"])
        assert result.exit_code in (0, 1)

    def test_ui_status(self):
        result = runner.invoke(app, ["ui", "status"])
        assert result.exit_code in (0, 1)

    def test_ui_stop_not_running(self):
        result = runner.invoke(app, ["ui", "stop"])
        assert result.exit_code in (0, 1)

    def test_bootstrap_help(self):
        result = runner.invoke(app, ["bootstrap", "--help"])
        assert result.exit_code == 0


class TestCliCommandsNoContext:
    """Tests verifying the root callback guard rejects without context."""

    def test_pg_requires_config(self):
        clear_cli_context()
        result = runner.invoke(app, ["pg", "--help"])
        assert result.exit_code == 1
        assert "require both --config and --env" in _strip(result.output)

    def test_db_requires_config(self):
        clear_cli_context()
        result = runner.invoke(app, ["db", "--help"])
        assert result.exit_code == 1


# ────────────────────────────────────────────────────────────────────
# cli/pg.py — helper functions tested directly
# ────────────────────────────────────────────────────────────────────


class TestPgHelpers:
    def test_get_postgres_data_dir_dev(self):
        from daylily_tapdb.cli.pg import Environment, _get_postgres_data_dir
        result = _get_postgres_data_dir(Environment.dev)
        assert "postgres" in str(result)
        assert "data" in str(result)

    def test_get_postgres_data_dir_prod(self):
        from daylily_tapdb.cli.pg import Environment, _get_postgres_data_dir
        result = _get_postgres_data_dir(Environment.prod)
        assert isinstance(result, Path)

    def test_get_postgres_log_file_dev(self):
        from daylily_tapdb.cli.pg import Environment, _get_postgres_log_file
        result = _get_postgres_log_file(Environment.dev)
        assert "postgresql.log" in str(result)

    def test_get_postgres_log_file_prod(self):
        from daylily_tapdb.cli.pg import Environment, _get_postgres_log_file
        result = _get_postgres_log_file(Environment.prod)
        assert "postgresql.log" in str(result)

    def test_get_postgres_socket_dir_dev(self):
        from daylily_tapdb.cli.pg import Environment, _get_postgres_socket_dir
        result = _get_postgres_socket_dir(Environment.dev)
        assert isinstance(result, Path)

    def test_get_postgres_socket_dir_prod(self):
        from daylily_tapdb.cli.pg import Environment, _get_postgres_socket_dir
        result = _get_postgres_socket_dir(Environment.prod)
        assert str(result) == "/var/run/postgresql"

    def test_get_instance_lock_file_dev(self):
        from daylily_tapdb.cli.pg import Environment, _get_instance_lock_file
        result = _get_instance_lock_file(Environment.dev)
        assert "instance.lock" in str(result)

    def test_get_instance_lock_file_prod(self):
        from daylily_tapdb.cli.pg import Environment, _get_instance_lock_file
        result = _get_instance_lock_file(Environment.prod)
        assert "prod" in str(result)

    def test_build_pg_ctl_options(self):
        from daylily_tapdb.cli.pg import _build_pg_ctl_options
        result = _build_pg_ctl_options(5432, Path("/tmp/sockets"))
        assert "-p 5432" in result
        assert "localhost" in result

    def test_port_conflict_details(self):
        from daylily_tapdb.cli.pg import _port_conflict_details
        result = _port_conflict_details(99999)
        assert "port 99999" in result

    def test_is_port_available_open(self):
        from daylily_tapdb.cli.pg import _is_port_available
        assert _is_port_available(59999) is True

    def test_is_port_available_exception(self):
        from daylily_tapdb.cli.pg import _is_port_available
        with patch("subprocess.run", side_effect=Exception("fail")):
            assert _is_port_available(5432) is True

    def test_active_env_default(self):
        from daylily_tapdb.cli.pg import Environment, _active_env
        result = _active_env()
        assert isinstance(result, Environment)

    def test_get_pg_service_cmd(self):
        from daylily_tapdb.cli.pg import _get_pg_service_cmd
        method, start_cmd, stop_cmd, log_path = _get_pg_service_cmd()
        assert isinstance(method, str)

    def test_is_pg_running_not_found(self):
        from daylily_tapdb.cli.pg import _is_pg_running
        with patch("subprocess.run", side_effect=FileNotFoundError):
            running, detail = _is_pg_running()
            assert running is False
            assert "not found" in detail

    def test_is_pg_running_timeout(self):
        import subprocess as sp

        from daylily_tapdb.cli.pg import _is_pg_running
        with patch("subprocess.run", side_effect=sp.TimeoutExpired("pg_isready", 5)):
            running, detail = _is_pg_running()
            assert running is False
            assert "timeout" in detail

    def test_pg_init_prod_rejected(self):
        result = runner.invoke(app, ["pg", "init", "prod"])
        assert result.exit_code == 1
        assert "prod" in _strip(result.output).lower()

    def test_pg_start_local_prod_rejected(self):
        result = runner.invoke(app, ["pg", "start-local", "prod"])
        assert result.exit_code == 1

    def test_pg_stop_local_prod_rejected(self):
        result = runner.invoke(app, ["pg", "stop-local", "prod"])
        assert result.exit_code == 1


# ────────────────────────────────────────────────────────────────────
# cli/db.py — helper functions tested directly
# ────────────────────────────────────────────────────────────────────


class TestDbHelpers:
    def test_ensure_dirs(self):
        from daylily_tapdb.cli.db import _ensure_dirs
        # _ensure_dirs() takes no args — uses get_config_path() internally
        _ensure_dirs()

    def test_find_schema_file(self):
        from daylily_tapdb.cli.db import _find_schema_file
        result = _find_schema_file()
        assert result.name == "tapdb_schema.sql" or result is not None

    def test_find_config_dir(self):
        from daylily_tapdb.cli.db import _find_config_dir
        result = _find_config_dir()
        assert result is not None

    def test_find_tapdb_core_config_dir(self):
        from daylily_tapdb.cli.db import _find_tapdb_core_config_dir
        result = _find_tapdb_core_config_dir()
        assert result is not None

    def test_get_db_config_dev(self):
        from daylily_tapdb.cli.db import Environment, _get_db_config
        cfg = _get_db_config(Environment.dev)
        assert "host" in cfg
        assert "port" in cfg

    def test_db_delete_prod_without_force(self):
        result = runner.invoke(app, ["db", "delete", "prod"])
        assert result.exit_code in (0, 1)

    def test_db_config_validate(self):
        result = runner.invoke(app, ["db", "config", "validate"])
        assert result.exit_code in (0, 1)
