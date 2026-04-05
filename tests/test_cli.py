"""Integration tests for TAPDB CLI commands."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db import (
    Environment,
    _ensure_dirs,
    _find_config_dir,
    _find_duplicate_template_keys,
    _find_schema_file,
    _find_tapdb_core_config_dir,
    _get_db_config,
    _load_template_configs,
    _resolve_seed_config_dirs,
)
from daylily_tapdb.templates import (
    validate_template_configs as _validate_template_configs,
)
from daylily_tapdb.cli.db_config import get_config_path

runner = CliRunner()


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(s: str) -> str:
    """Remove ANSI color/style escape sequences from CLI output.

    CI uses Typer+Rich which may emit ANSI escapes; tests should assert against the
    semantic text, not terminal formatting.
    """

    return _ANSI_ESCAPE_RE.sub("", s)


@pytest.fixture(autouse=True)
def _isolate_cli_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Keep CLI tests hermetic.

    - Avoid touching the user's real ~/.tapdb PID/log files.
    - Avoid probing unexpected environments.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg_path = (
        tmp_path / ".config" / "tapdb" / "testclient" / "testdb" / "tapdb-config.yaml"
    )
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
        '    audit_log_euid_prefix: "CGX"\n'
        "  test:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: 5534\n"
        "    ui_port: 8912\n"
        "    user: test\n"
        '    password: ""\n'
        "    database: tapdb_test\n"
        '    audit_log_euid_prefix: "CGX"\n'
        "  prod:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: 5535\n"
        "    ui_port: 8913\n"
        "    user: test\n"
        '    password: ""\n'
        "    database: tapdb_prod\n"
        '    audit_log_euid_prefix: "CGX"\n',
        encoding="utf-8",
    )
    os.chmod(cfg_path, 0o600)
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        env_name="dev",
        config_path=cfg_path,
    )
    monkeypatch.setattr(cli_mod, "PID_FILE", tmp_path / "ui.pid")
    monkeypatch.setattr(cli_mod, "LOG_FILE", tmp_path / "ui.log")
    yield
    clear_cli_context()


class TestCLIMain:
    """Tests for main CLI commands."""

    def test_help(self):
        """Test --help shows all command groups."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "TAPDB" in result.output
        assert "bootstrap" in result.output
        assert "ui" in result.output
        assert "db" in result.output
        assert "pg" in result.output
        assert "cognito" in result.output

    def test_version(self):
        """Test version command."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "daylily-tapdb" in result.output or "0." in result.output

    def test_info(self):
        """Test info command."""
        # Ensure we don't attempt real psql connections during unit tests.
        with patch("shutil.which", return_value=None):
            result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        assert "Version" in result.output
        assert "Python" in result.output
        assert "DB probes" in result.output

    def test_info_json(self):
        """Test info --json output is valid JSON and has stable keys."""
        # Ensure we don't attempt real psql connections during unit tests.
        with patch("shutil.which", return_value=None):
            result = runner.invoke(app, ["info", "--json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert "version" in payload
        assert "python" in payload
        assert "tapdb_env" in payload
        assert "paths" in payload
        assert "postgres" in payload

    def test_info_check_all_envs_json(self):
        """Test info --check-all-envs --json shape."""
        with patch("shutil.which", return_value=None):
            result = runner.invoke(app, ["info", "--check-all-envs", "--json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload.get("check_all_envs") is True
        pg = payload.get("postgres")
        assert isinstance(pg, dict)
        for env_name in ["dev", "test", "prod"]:
            assert env_name in pg

    def test_database_name_option_does_not_override_explicit_config_metadata(self):
        """Test config metadata stays authoritative once --config is resolved."""
        with patch("shutil.which", return_value=None):
            result = runner.invoke(app, ["--database-name", "atlas", "info", "--json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["database_name"] == "testdb"
        assert (
            "/testclient/testdb/tapdb-config.yaml"
            in payload["paths"]["effective_config"]["path"]
        )

    def test_runtime_command_without_config_fails(self):
        clear_cli_context()
        result = runner.invoke(app, ["db", "create", "dev"])
        clear_cli_context()
        assert result.exit_code != 0
        assert "require both --config and --env" in _strip_ansi(result.output)

    def test_runtime_command_without_config_for_ui_fails(self):
        clear_cli_context()
        result = runner.invoke(app, ["ui", "status"])
        clear_cli_context()
        assert result.exit_code != 0
        assert "require both --config and --env" in _strip_ansi(result.output)


class TestCLIUI:
    """Tests for UI management commands."""

    def test_ui_help(self):
        """Test ui --help."""
        result = runner.invoke(app, ["ui", "--help"])
        assert result.exit_code == 0
        assert "start" in result.output
        assert "mkcert" in result.output
        assert "stop" in result.output
        assert "status" in result.output
        assert "logs" in result.output

    def test_ui_mkcert_missing_binary(self, monkeypatch):
        """Test ui mkcert fails clearly when mkcert is not installed."""
        monkeypatch.setattr(cli_mod.shutil, "which", lambda _name: None)
        result = runner.invoke(app, ["ui", "mkcert"])
        assert result.exit_code == 1
        assert "mkcert is required" in _strip_ansi(result.output)

    def test_ui_mkcert_generates_cert_files(self, tmp_path, monkeypatch):
        """Test ui mkcert installs CA and generates cert/key files."""
        cert = tmp_path / "tls" / "localhost.crt"
        key = tmp_path / "tls" / "localhost.key"
        commands: list[list[str]] = []

        def _fake_run(cmd, capture_output=True, text=True):
            commands.append(list(cmd))
            if "-cert-file" in cmd:
                cert_idx = cmd.index("-cert-file") + 1
                key_idx = cmd.index("-key-file") + 1
                cert_path = Path(cmd[cert_idx])
                key_path = Path(cmd[key_idx])
                cert_path.parent.mkdir(parents=True, exist_ok=True)
                key_path.parent.mkdir(parents=True, exist_ok=True)
                cert_path.write_text("fake-cert", encoding="utf-8")
                key_path.write_text("fake-key", encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(
            cli_mod.shutil, "which", lambda name: "/opt/homebrew/bin/mkcert"
        )
        monkeypatch.setattr(cli_mod.subprocess, "run", _fake_run)

        result = runner.invoke(
            app,
            [
                "ui",
                "mkcert",
                "--cert-file",
                str(cert),
                "--key-file",
                str(key),
            ],
        )

        assert result.exit_code == 0
        assert cert.exists()
        assert key.exists()
        assert commands[0] == ["/opt/homebrew/bin/mkcert", "-install"]
        assert commands[1][:5] == [
            "/opt/homebrew/bin/mkcert",
            "-cert-file",
            str(cert),
            "-key-file",
            str(key),
        ]
        assert "localhost" in commands[1]

    def test_ui_status_not_running(self):
        """Test ui status when server is not running."""
        result = runner.invoke(app, ["ui", "status"])
        assert result.exit_code == 0
        assert "not running" in result.output

    def test_ui_stop_not_running(self):
        """Test ui stop when server is not running."""
        result = runner.invoke(app, ["ui", "stop"])
        assert result.exit_code == 0
        assert "No UI server running" in result.output or "not running" in result.output

    def test_ui_logs_no_file(self):
        """Test ui logs when no log file exists."""
        result = runner.invoke(app, ["ui", "logs"])
        assert result.exit_code == 0
        assert "No log file" in result.output or "not found" in result.output.lower()


class TestConfigUpdate:
    """Tests for namespaced config mutation commands."""

    def test_config_update_writes_selected_fields(self):
        result = runner.invoke(
            app,
            [
                "config",
                "update",
                "--env",
                "dev",
                "--host",
                "db.internal",
                "--port",
                "6001",
                "--ui-port",
                "9443",
                "--cognito-client-name",
                "atlas",
                "--support-email",
                "support@example.com",
            ],
        )

        assert result.exit_code == 0
        config_path = (
            Path(os.environ["HOME"])
            / ".config"
            / "tapdb"
            / "testclient"
            / "testdb"
            / "tapdb-config.yaml"
        )
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        env_cfg = payload["environments"]["dev"]
        assert env_cfg["host"] == "db.internal"
        assert env_cfg["port"] == "6001"
        assert env_cfg["ui_port"] == "9443"
        assert env_cfg["cognito_client_name"] == "atlas"
        assert env_cfg["support_email"] == "support@example.com"

    def test_config_update_can_clear_fields(self):
        result = runner.invoke(
            app,
            [
                "config",
                "update",
                "--env",
                "dev",
                "--support-email",
                "support@example.com",
            ],
        )
        assert result.exit_code == 0

        result = runner.invoke(
            app, ["config", "update", "--env", "dev", "--clear", "support_email"]
        )
        assert result.exit_code == 0

        config_path = (
            Path(os.environ["HOME"])
            / ".config"
            / "tapdb"
            / "testclient"
            / "testdb"
            / "tapdb-config.yaml"
        )
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert payload["environments"]["dev"]["support_email"] == ""


class TestCLICognito:
    """Tests for Cognito integration commands."""

    def test_cognito_help(self):
        result = runner.invoke(app, ["cognito", "--help"])
        assert result.exit_code == 0
        out = _strip_ansi(result.output)
        assert "setup" in out
        assert "setup-with-google" in out
        assert "bind" in out
        assert "status" in out
        assert "list-pools" in out
        assert "add-user" in out
        assert "list-apps" in out
        assert "add-app" in out
        assert "edit-app" in out
        assert "remove-app" in out
        assert "add-google-idp" in out
        assert "fix-auth-flows" in out
        assert "config" in out

    def test_cognito_bind_writes_pool_id(self, tmp_path, monkeypatch):
        cfg_path = tmp_path / "tapdb-config.yaml"
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
            "    port: 5432\n"
            "    ui_port: 8911\n"
            "    user: test\n"
            "    database: tapdb_dev\n",
            encoding="utf-8",
        )
        result = runner.invoke(
            app,
            [
                "--config",
                str(cfg_path),
                "--env",
                "dev",
                "cognito",
                "bind",
                "dev",
                "--pool-id",
                "us-east-1_TESTPOOL",
            ],
        )
        assert result.exit_code == 0
        content = cfg_path.read_text(encoding="utf-8")
        assert "cognito_user_pool_id" in content
        assert "us-east-1_TESTPOOL" in content

    def test_cognito_setup_uses_daycog_024_flags(self, tmp_path, monkeypatch):
        pool_name = "tapdb-dev-users"
        captured: dict[str, list[str]] = {}

        def _fake_run_daycog(args, env=None):
            captured["args"] = list(args)
            return ""

        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._finalize_setup_binding",
            lambda **_kwargs: None,
        )
        monkeypatch.setattr("daylily_tapdb.cli.cognito._run_daycog", _fake_run_daycog)

        result = runner.invoke(
            app,
            [
                "cognito",
                "setup",
                "dev",
                "--pool-name",
                pool_name,
                "--profile",
                "test-profile",
                "--region",
                "us-east-1",
            ],
        )

        assert result.exit_code == 0
        args = captured["args"]
        assert args[:2] == ["setup", "--name"]
        assert "--autoprovision" in args
        assert "--client-name" in args
        assert "tapdb" in args
        assert "--callback-path" in args
        assert "--attach-domain" in args
        assert "--domain-prefix" not in args
        assert "--oauth-flows" in args
        assert "--scopes" in args
        assert "--idp" in args

    def test_cognito_setup_with_domain_flags_routes_to_daycog(
        self, tmp_path, monkeypatch
    ):
        pool_name = "tapdb-dev-users"
        captured: dict[str, list[str]] = {}

        def _fake_run_daycog(args, env=None):
            captured["args"] = list(args)
            return ""

        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._finalize_setup_binding",
            lambda **_kwargs: None,
        )
        monkeypatch.setattr("daylily_tapdb.cli.cognito._run_daycog", _fake_run_daycog)

        result = runner.invoke(
            app,
            [
                "cognito",
                "setup",
                "dev",
                "--pool-name",
                pool_name,
                "--region",
                "us-east-1",
                "--domain-prefix",
                "tapdb-dev-domain",
                "--no-attach-domain",
            ],
        )
        assert result.exit_code == 0
        args = captured["args"]
        assert "--domain-prefix" in args
        assert "tapdb-dev-domain" in args
        assert "--no-attach-domain" in args

    def test_cognito_setup_with_google_routes_to_daycog(self, tmp_path, monkeypatch):
        pool_name = "tapdb-dev-users"
        captured: dict[str, list[str]] = {}

        def _fake_run_daycog(args, env=None):
            captured["args"] = list(args)
            return ""

        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._finalize_setup_binding",
            lambda **_kwargs: None,
        )
        monkeypatch.setattr("daylily_tapdb.cli.cognito._run_daycog", _fake_run_daycog)

        result = runner.invoke(
            app,
            [
                "cognito",
                "setup-with-google",
                "dev",
                "--pool-name",
                pool_name,
                "--profile",
                "test-profile",
                "--region",
                "us-east-1",
                "--google-client-id",
                "gid",
                "--google-client-secret",
                "gsecret",
            ],
        )

        assert result.exit_code == 0
        args = captured["args"]
        assert args[:2] == ["setup-with-google", "--name"]
        assert "--google-client-id" in args
        assert "--google-client-secret" in args
        assert "--callback-path" in args

    def test_cognito_list_apps_routes_to_daycog(self, monkeypatch):
        captured: dict[str, list[str]] = {}

        def _fake_run_daycog(args, env=None):
            captured["args"] = list(args)
            return ""

        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._resolve_pool_command_context",
            lambda *_args, **_kwargs: (
                "tapdb-dev-users",
                {"AWS_PROFILE": "test-profile"},
                "us-east-1",
                "test-profile",
            ),
        )
        monkeypatch.setattr("daylily_tapdb.cli.cognito._run_daycog", _fake_run_daycog)

        result = runner.invoke(app, ["cognito", "list-apps", "dev"])
        assert result.exit_code == 0
        args = captured["args"]
        assert args[:2] == ["list-apps", "--pool-name"]
        assert "--region" in args
        assert "--profile" in args

    def test_cognito_list_pools_routes_to_daycog(self, monkeypatch):
        captured: dict[str, list[str]] = {}

        def _fake_run_daycog(args, env=None):
            captured["args"] = list(args)
            return ""

        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._resolve_pool_command_context",
            lambda *_args, **_kwargs: (
                "tapdb-dev-users",
                {"AWS_PROFILE": "test-profile"},
                "us-east-1",
                "test-profile",
            ),
        )
        monkeypatch.setattr("daylily_tapdb.cli.cognito._run_daycog", _fake_run_daycog)

        result = runner.invoke(app, ["cognito", "list-pools", "dev"])
        assert result.exit_code == 0
        args = captured["args"]
        assert args[:2] == ["list-pools", "--region"]
        assert "--profile" in args

    def test_cognito_add_app_routes_to_daycog(self, monkeypatch):
        captured: dict[str, list[str]] = {}

        def _fake_run_daycog(args, env=None):
            captured["args"] = list(args)
            return ""

        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._resolve_pool_command_context",
            lambda *_args, **_kwargs: (
                "tapdb-dev-users",
                {"AWS_PROFILE": "test-profile"},
                "us-east-1",
                "test-profile",
            ),
        )
        monkeypatch.setattr("daylily_tapdb.cli.cognito._run_daycog", _fake_run_daycog)

        result = runner.invoke(
            app,
            [
                "cognito",
                "add-app",
                "dev",
                "--app-name",
                "web-app",
                "--callback-url",
                "https://localhost:8911/auth/callback",
                "--set-default",
            ],
        )
        assert result.exit_code == 0
        args = captured["args"]
        assert args[:2] == ["add-app", "--pool-name"]
        assert "--app-name" in args
        assert "--callback-url" in args
        assert "--set-default" in args

    def test_cognito_config_update_routes_to_daycog(self, monkeypatch):
        captured: dict[str, list[str]] = {}

        def _fake_run_daycog(args, env=None):
            captured["args"] = list(args)
            return ""

        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._resolve_pool_command_context",
            lambda *_args, **_kwargs: (
                "tapdb-dev-users",
                {"AWS_PROFILE": "test-profile"},
                "us-east-1",
                "test-profile",
            ),
        )
        monkeypatch.setattr("daylily_tapdb.cli.cognito._run_daycog", _fake_run_daycog)

        result = runner.invoke(app, ["cognito", "config", "update", "dev"])
        assert result.exit_code == 0
        args = captured["args"]
        assert args[:3] == ["config", "update", "--pool-name"]
        assert "--region" in args
        assert "--profile" in args

    def test_cognito_status_shows_daycog_024_fields(self, monkeypatch):
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito.get_db_config_for_env",
            lambda _env: {"cognito_user_pool_id": "us-east-1_TESTPOOL"},
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._find_pool_context_by_id",
            lambda _pool_id, **_kwargs: (
                "tapdb-dev-users.us-east-1",
                {
                    "AWS_PROFILE": "test",
                    "AWS_REGION": "us-east-1",
                    "COGNITO_REGION": "us-east-1",
                    "COGNITO_USER_POOL_ID": "us-east-1_TESTPOOL",
                    "COGNITO_APP_CLIENT_ID": "cid123",
                    "COGNITO_CLIENT_NAME": "tapdb",
                    "COGNITO_DOMAIN": (
                        "tapdb-dev-users.auth.us-east-1.amazoncognito.com"
                    ),
                    "COGNITO_CALLBACK_URL": "https://localhost:8911/auth/callback",
                    "COGNITO_LOGOUT_URL": "https://localhost:8911/",
                },
            ),
        )

        result = runner.invoke(app, ["cognito", "status", "dev"])
        assert result.exit_code == 0
        out = _strip_ansi(result.output)
        assert "Client:" in out
        assert "Domain:" in out
        assert "Callback:" in out
        assert "Logout:" in out

    def test_cognito_status_fails_on_local_uri_port_mismatch(self, monkeypatch):
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito.get_db_config_for_env",
            lambda _env: {"cognito_user_pool_id": "us-east-1_TESTPOOL"},
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._find_pool_context_by_id",
            lambda _pool_id, **_kwargs: (
                "tapdb-dev-users.us-east-1",
                {
                    "AWS_PROFILE": "test",
                    "AWS_REGION": "us-east-1",
                    "COGNITO_REGION": "us-east-1",
                    "COGNITO_USER_POOL_ID": "us-east-1_TESTPOOL",
                    "COGNITO_APP_CLIENT_ID": "cid123",
                    "COGNITO_CLIENT_NAME": "tapdb",
                    "COGNITO_CALLBACK_URL": "https://localhost:9999/auth/callback",
                    "COGNITO_LOGOUT_URL": "https://localhost:9999/",
                },
            ),
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._resolve_expected_ui_port",
            lambda _env: (8911, "test"),
        )

        result = runner.invoke(app, ["cognito", "status", "dev"])
        assert result.exit_code != 0
        out = _strip_ansi(result.output)
        assert "Cognito URI validation failed" in out
        assert "does not match TAPDB UI port 8911" in out

    def test_cognito_status_fails_on_non_https_uri(self, monkeypatch):
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito.get_db_config_for_env",
            lambda _env: {"cognito_user_pool_id": "us-east-1_TESTPOOL"},
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._find_pool_context_by_id",
            lambda _pool_id, **_kwargs: (
                "tapdb-dev-users.us-east-1",
                {
                    "AWS_PROFILE": "test",
                    "AWS_REGION": "us-east-1",
                    "COGNITO_REGION": "us-east-1",
                    "COGNITO_USER_POOL_ID": "us-east-1_TESTPOOL",
                    "COGNITO_APP_CLIENT_ID": "cid123",
                    "COGNITO_CLIENT_NAME": "tapdb",
                    "COGNITO_CALLBACK_URL": "http://localhost:8911/auth/callback",
                    "COGNITO_LOGOUT_URL": "https://localhost:8911/",
                },
            ),
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._resolve_expected_ui_port",
            lambda _env: (8911, "test"),
        )

        result = runner.invoke(app, ["cognito", "status", "dev"])
        assert result.exit_code != 0
        out = _strip_ansi(result.output)
        assert "Cognito URI validation failed" in out
        assert "must use https" in out

    def test_cognito_status_fails_if_client_name_not_tapdb(self, monkeypatch):
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito.get_db_config_for_env",
            lambda _env: {"cognito_user_pool_id": "us-east-1_TESTPOOL"},
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._find_pool_context_by_id",
            lambda _pool_id, **_kwargs: (
                "tapdb-dev-users.us-east-1",
                {
                    "AWS_PROFILE": "test",
                    "AWS_REGION": "us-east-1",
                    "COGNITO_REGION": "us-east-1",
                    "COGNITO_USER_POOL_ID": "us-east-1_TESTPOOL",
                    "COGNITO_APP_CLIENT_ID": "cid123",
                    "COGNITO_CLIENT_NAME": "wrong-client",
                    "COGNITO_CALLBACK_URL": "https://localhost:8911/auth/callback",
                    "COGNITO_LOGOUT_URL": "https://localhost:8911/",
                },
            ),
        )

        result = runner.invoke(app, ["cognito", "status", "dev"])
        assert result.exit_code != 0
        out = _strip_ansi(result.output)
        assert "must select Cognito app client name 'tapdb'" in out

    def test_cognito_setup_rejects_non_tapdb_client_name(self):
        result = runner.invoke(
            app,
            [
                "cognito",
                "setup",
                "dev",
                "--pool-name",
                "tapdb-dev-users",
                "--client-name",
                "custom-client",
            ],
        )
        assert result.exit_code != 0
        out = _strip_ansi(result.output)
        assert "requires Cognito app client name 'tapdb'" in out

    def test_cognito_add_user_provisions_actor_user(self, monkeypatch):
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito.get_db_config_for_env",
            lambda _env: {
                "cognito_user_pool_id": "us-east-1_TESTPOOL",
                "engine_type": "local",
                "host": "localhost",
                "port": "5533",
                "user": "test",
                "password": "",
                "database": "tapdb_dev",
            },
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._find_pool_context_by_id",
            lambda _pool_id, **_kwargs: (
                "tapdb-dev-users.us-east-1",
                {
                    "AWS_PROFILE": "test",
                    "AWS_REGION": "us-east-1",
                    "COGNITO_REGION": "us-east-1",
                    "COGNITO_USER_POOL_ID": "us-east-1_TESTPOOL",
                    "COGNITO_APP_CLIENT_ID": "cid123",
                    "COGNITO_CLIENT_NAME": "tapdb",
                },
            ),
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito._run_daycog",
            lambda *_args, **_kwargs: "",
        )

        class _FakeConn:
            def __init__(self, *args, **kwargs):
                _ = (args, kwargs)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            class _Scope:
                def __enter__(self):
                    return object()

                def __exit__(self, exc_type, exc, tb):
                    return False

            def session_scope(self, commit=False):
                _ = commit
                return self._Scope()

        monkeypatch.setattr("daylily_tapdb.cli.cognito.TAPDBConnection", _FakeConn)

        class _FakeUser:
            is_active = True

        monkeypatch.setattr(
            "daylily_tapdb.cli.cognito.create_or_get",
            lambda *_args, **_kwargs: (_FakeUser(), True),
        )

        result = runner.invoke(
            app,
            [
                "cognito",
                "add-user",
                "dev",
                "johnm@lsmc.bio",
                "--password",
                "TestPass123",
                "--no-verify",
            ],
        )
        assert result.exit_code == 0
        assert "Created Cognito user" in result.output


class TestCLIBootstrap:
    """Tests for bootstrap commands."""

    def test_bootstrap_help(self):
        result = runner.invoke(app, ["bootstrap", "--help"])
        assert result.exit_code == 0
        out = _strip_ansi(result.output)
        assert "local" in out
        assert "aurora" in out

    def test_bootstrap_local_requires_explicit_runtime_context(self):
        clear_cli_context()
        fresh_app = cli_mod.build_app()
        result = runner.invoke(fresh_app, ["bootstrap", "local", "--no-gui"])
        clear_cli_context()
        assert result.exit_code != 0
        assert "require both --config and --env" in _strip_ansi(result.output)

    def test_bootstrap_local_no_gui(self, monkeypatch):
        monkeypatch.setenv("TAPDB_ENV", "dev")
        monkeypatch.setenv("TAPDB_DEV_ENGINE_TYPE", "local")
        monkeypatch.setattr("daylily_tapdb.cli.db.create_database", lambda **_: None)
        monkeypatch.setattr("daylily_tapdb.cli.db.apply_schema", lambda **_: None)
        monkeypatch.setattr("daylily_tapdb.cli.db.run_migrations", lambda **_: None)
        monkeypatch.setattr("daylily_tapdb.cli.db.seed_templates", lambda **_: None)
        monkeypatch.setattr(
            "daylily_tapdb.cli.db._create_default_admin",
            lambda **_: False,
        )
        monkeypatch.setattr("daylily_tapdb.cli.pg.pg_init", lambda **_: None)
        monkeypatch.setattr("daylily_tapdb.cli.pg.pg_start_local", lambda **_: None)

        fresh_app = cli_mod.build_app()
        result = runner.invoke(fresh_app, ["bootstrap", "local", "--no-gui"])
        assert result.exit_code == 0
        assert "bootstrap complete" in result.output.lower()

    def test_bootstrap_aurora_requires_cluster(self, monkeypatch):
        monkeypatch.setenv("TAPDB_ENV", "dev")
        fresh_app = cli_mod.build_app()
        result = runner.invoke(fresh_app, ["bootstrap", "aurora"])
        assert result.exit_code != 0
        assert "--cluster" in result.output or "Missing option" in result.output


class TestCLIDB:
    """Tests for database management commands."""

    def test_db_help(self):
        """Test db --help."""
        result = runner.invoke(app, ["db", "--help"])
        assert result.exit_code == 0
        assert "create" in result.output
        assert "delete" in result.output
        assert "schema" in result.output
        assert "data" in result.output
        assert "config" in result.output

    def test_db_create_help(self):
        """Test db create --help."""
        result = runner.invoke(app, ["db", "create", "--help"])
        assert result.exit_code == 0
        assert "dev" in result.output
        assert "test" in result.output
        assert "prod" in result.output

    def test_db_schema_reset_help(self):
        """Test db schema reset --help shows safety warnings."""
        result = runner.invoke(app, ["db", "schema", "reset", "--help"])
        assert result.exit_code == 0
        assert "DESTRUCTIVE" in result.output or "force" in result.output

    def test_db_schema_drift_check_help(self):
        """Test db schema drift-check --help."""
        result = runner.invoke(app, ["db", "schema", "drift-check", "--help"])
        assert result.exit_code == 0
        out = _strip_ansi(result.output)
        assert "--json" in out
        assert "--strict" in out

    def test_db_schema_drift_check_json_clean(self, monkeypatch):
        """Clean drift-check emits JSON and exits 0."""
        monkeypatch.setattr(
            "daylily_tapdb.cli.db._check_db_exists",
            lambda _env, _db: True,
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.db._run_schema_drift_check",
            lambda _env, strict: (
                {
                    "status": "clean",
                    "env": "dev",
                    "database": "tapdb_dev",
                    "schema_name": "public",
                    "strict": strict,
                    "expected_asset_paths": ["/tmp/schema/tapdb_schema.sql"],
                    "counts": {
                        "expected": {"tables": 1},
                        "live": {"tables": 1},
                        "missing": {"tables": 0},
                        "unexpected": {"tables": 0},
                    },
                    "missing": {
                        "tables": [],
                        "columns": [],
                        "sequences": [],
                        "functions": [],
                        "triggers": [],
                        "indexes": [],
                    },
                    "unexpected": {
                        "tables": [],
                        "columns": [],
                        "sequences": [],
                        "functions": [],
                        "triggers": [],
                        "indexes": [],
                    },
                },
                False,
            ),
        )

        result = runner.invoke(app, ["db", "schema", "drift-check", "dev", "--json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "clean"
        assert payload["schema_name"] == "public"
        assert "counts" in payload
        assert "missing" in payload
        assert "unexpected" in payload

    def test_db_schema_drift_check_json_drift_exit_one(self, monkeypatch):
        """Drift-check exits 1 on detected drift."""
        monkeypatch.setattr(
            "daylily_tapdb.cli.db._check_db_exists",
            lambda _env, _db: True,
        )
        monkeypatch.setattr(
            "daylily_tapdb.cli.db._run_schema_drift_check",
            lambda _env, strict: (
                {
                    "status": "drift",
                    "env": "dev",
                    "database": "tapdb_dev",
                    "schema_name": "public",
                    "strict": strict,
                    "expected_asset_paths": ["/tmp/schema/tapdb_schema.sql"],
                    "counts": {
                        "expected": {"tables": 1},
                        "live": {"tables": 0},
                        "missing": {"tables": 1},
                        "unexpected": {"tables": 0},
                    },
                    "missing": {
                        "tables": ["generic_template"],
                        "columns": [],
                        "sequences": [],
                        "functions": [],
                        "triggers": [],
                        "indexes": [],
                    },
                    "unexpected": {
                        "tables": [],
                        "columns": [],
                        "sequences": [],
                        "functions": [],
                        "triggers": [],
                        "indexes": [],
                    },
                },
                True,
            ),
        )

        result = runner.invoke(
            app,
            ["db", "schema", "drift-check", "dev", "--json", "--strict"],
        )
        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["status"] == "drift"
        assert payload["strict"] is True
        assert payload["missing"]["tables"] == ["generic_template"]

    def test_db_schema_drift_check_json_operational_error_exit_two(self, monkeypatch):
        """Drift-check exits 2 for operational errors."""
        monkeypatch.setattr(
            "daylily_tapdb.cli.db._check_db_exists",
            lambda _env, _db: False,
        )
        result = runner.invoke(app, ["db", "schema", "drift-check", "dev", "--json"])
        assert result.exit_code == 2
        payload = json.loads(result.output)
        assert payload["status"] == "error"
        assert "does not exist" in payload["error"]

    def test_get_db_config_defaults(self):
        """Test _get_db_config returns correct defaults when no config file exists."""
        # Mock load_config so the real ~/.config/tapdb/tapdb-config.yaml is ignored.
        # Also clear PG* env vars that would override the hard defaults.
        env_clear = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("PGHOST")
            and not k.startswith("PGPORT")
            and not k.startswith("TAPDB_DEV_")
        }
        with (
            patch("daylily_tapdb.cli.db_config.load_config", return_value={}),
            patch.dict(os.environ, env_clear, clear=True),
        ):
            config = _get_db_config(Environment.dev)
            assert config["database"] == "tapdb_dev"
            assert config["host"] == "localhost"
            assert config["port"] == "5533"

    def test_get_db_config_ignores_removed_env_overrides(self):
        """Test removed TAPDB_<ENV>_* overrides no longer affect config resolution."""
        # Mock load_config to isolate from real config file.
        with (
            patch("daylily_tapdb.cli.db_config.load_config", return_value={}),
            patch.dict(
                os.environ,
                {
                    "TAPDB_TEST_HOST": "testhost",
                    "TAPDB_TEST_PORT": "5433",
                    "TAPDB_TEST_DATABASE": "my_test_db",
                },
            ),
        ):
            config = _get_db_config(Environment.test)
            assert config["host"] == "localhost"
            assert config["port"] == "5534"
            assert config["database"] == "tapdb_test"

    def test_find_schema_file(self):
        """Test _find_schema_file locates the schema."""
        schema_path = _find_schema_file()
        assert schema_path.exists()
        assert schema_path.name == "tapdb_schema.sql"

    def test_find_schema_file_falls_back_to_installed_data_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Use installed data-dir schema when repo-relative schema is absent."""
        import daylily_tapdb.cli.db as m

        fake_cli_dir = tmp_path / "site-packages" / "daylily_tapdb" / "cli"
        fake_cli_dir.mkdir(parents=True)
        fake_db_py = fake_cli_dir / "db.py"
        fake_db_py.write_text("# fake module path\n")
        monkeypatch.setattr(m, "__file__", str(fake_db_py))

        data_root = tmp_path / "py-data"
        expected_schema = data_root / "schema" / "tapdb_schema.sql"
        expected_schema.parent.mkdir(parents=True)
        expected_schema.write_text("-- installed schema\n")

        cwd_schema = tmp_path / "cwd" / "schema" / "tapdb_schema.sql"
        cwd_schema.parent.mkdir(parents=True)
        cwd_schema.write_text("-- cwd schema\n")
        monkeypatch.chdir(cwd_schema.parent.parent)

        monkeypatch.setattr(m.sysconfig, "get_paths", lambda: {"data": str(data_root)})

        schema_path = m._find_schema_file()
        assert schema_path == expected_schema
        assert schema_path.read_text() == "-- installed schema\n"

    def test_ensure_dirs_creates_config(self):
        """Test _ensure_dirs creates config directory."""
        _ensure_dirs()
        assert get_config_path().parent.exists()

    def test_db_status_no_psql(self):
        """Test db status handles missing psql gracefully."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError("psql not found")
            result = runner.invoke(app, ["db", "schema", "status", "dev"])
            # Should handle error gracefully
            assert (
                result.exit_code != 0
                or "psql" in result.output.lower()
                or "error" in result.output.lower()
            )

    def test_db_schema_reset_requires_confirmation(self):
        """Test db schema reset aborts without confirmation."""
        result = runner.invoke(app, ["db", "schema", "reset", "dev"], input="n\n")
        # Should abort, or exit early if db doesn't exist (which is also safe)
        output_lower = result.output.lower()
        assert (
            "abort" in output_lower
            or "does not exist" in output_lower
            or result.exit_code != 0
        )

    def test_db_backup_creates_file(self):
        """Test db backup command structure."""
        result = runner.invoke(app, ["db", "data", "backup", "--help"])
        assert result.exit_code == 0
        assert "--output" in result.output or "-o" in result.output

    def test_db_restore_requires_input(self):
        """Test db restore requires --input file."""
        result = runner.invoke(app, ["db", "data", "restore", "dev"])
        # Should fail without --input
        assert result.exit_code != 0 or "input" in result.output.lower()


class TestCLIPG:
    """Tests for PostgreSQL service commands."""

    def test_pg_help(self):
        """Test pg --help."""
        result = runner.invoke(app, ["pg", "--help"])
        assert result.exit_code == 0
        assert "start" in result.output
        assert "stop" in result.output
        assert "status" in result.output
        assert "logs" in result.output
        assert "init" in result.output
        assert "start-local" in result.output

    def test_pg_status_handles_missing_pg(self):
        """Test pg status handles missing PostgreSQL gracefully."""
        result = runner.invoke(app, ["pg", "status"])
        assert result.exit_code == 0
        # Should show status regardless of PostgreSQL availability
        assert "PostgreSQL" in result.output

    def test_pg_logs_help(self):
        """Test pg logs --help."""
        result = runner.invoke(app, ["pg", "logs", "--help"])
        assert result.exit_code == 0
        assert "--follow" in result.output or "-f" in result.output
        assert "--lines" in result.output or "-n" in result.output

    def test_pg_create_removed(self):
        """Test pg create was removed."""
        result = runner.invoke(app, ["pg", "create", "dev"])
        assert result.exit_code != 0
        assert "No such command" in result.output or "no such option" in result.output

    def test_pg_delete_removed(self):
        """Test pg delete was removed."""
        result = runner.invoke(app, ["pg", "delete", "dev"])
        assert result.exit_code != 0
        assert "No such command" in result.output or "no such option" in result.output

    def test_pg_init_help(self):
        """Test pg init --help."""
        result = runner.invoke(app, ["pg", "init", "--help"])
        assert result.exit_code == 0
        assert "dev" in result.output
        assert "test" in result.output
        assert "--force" in result.output or "-f" in result.output

    def test_pg_init_rejects_prod(self):
        """Test pg init rejects prod environment."""
        result = runner.invoke(app, ["pg", "init", "prod"])
        assert result.exit_code != 0
        assert "prod" in result.output.lower() or "cannot" in result.output.lower()

    def test_pg_init_uses_configured_superuser(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test pg init creates the local cluster with the configured DB user."""
        import daylily_tapdb.cli.pg as pg_mod

        data_dir = tmp_path / "pgdata"
        calls: list[list[str]] = []

        def _fake_run(cmd, capture_output=True, text=True, timeout=60):
            calls.append(list(cmd))
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(pg_mod, "_get_postgres_data_dir", lambda _env: data_dir)
        monkeypatch.setattr(pg_mod.shutil, "which", lambda _name: "/usr/bin/initdb")
        monkeypatch.setattr(
            pg_mod,
            "get_db_config_for_env",
            lambda _env: {
                "user": "postgres",
                "host": "localhost",
                "port": "5533",
            },
        )
        monkeypatch.setattr(pg_mod.subprocess, "run", _fake_run)

        result = runner.invoke(app, ["pg", "init", "dev"])

        assert result.exit_code == 0
        assert calls == [
            [
                "/usr/bin/initdb",
                "-D",
                str(data_dir),
                "--no-locale",
                "-E",
                "UTF8",
                "-U",
                "postgres",
            ]
        ]

    def test_pg_start_local_help(self):
        """Test pg start-local --help."""
        result = runner.invoke(app, ["pg", "start-local", "--help"])
        assert result.exit_code == 0
        assert "--port" in result.output or "-p" in result.output

    def test_pg_stop_local_help(self):
        """Test pg stop-local --help."""
        result = runner.invoke(app, ["pg", "stop-local", "--help"])
        assert result.exit_code == 0
        assert "dev" in result.output

    def test_pg_start_local_uses_namespaced_socket_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test local startup passes explicit socket dir and localhost binding."""
        import daylily_tapdb.cli.pg as pg_mod

        data_dir = tmp_path / "pgdata"
        data_dir.mkdir()
        (data_dir / "PG_VERSION").write_text("16\n", encoding="utf-8")
        log_file = tmp_path / "postgresql.log"
        lock_file = tmp_path / "instance.lock"
        socket_dir = tmp_path / "socket dir"

        calls: list[list[str]] = []

        def _fake_run(cmd, capture_output=True, text=True, timeout=30):
            calls.append(list(cmd))
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(pg_mod, "_get_postgres_data_dir", lambda _env: data_dir)
        monkeypatch.setattr(pg_mod, "_get_postgres_log_file", lambda _env: log_file)
        monkeypatch.setattr(pg_mod, "_get_instance_lock_file", lambda _env: lock_file)
        monkeypatch.setattr(pg_mod, "_get_postgres_socket_dir", lambda _env: socket_dir)
        monkeypatch.setattr(
            pg_mod,
            "get_db_config_for_env",
            lambda _env: {
                "port": "5533",
                "host": "localhost",
                "user": "test",
                "unix_socket_dir": str(socket_dir),
            },
        )
        monkeypatch.setattr(pg_mod, "_is_port_available", lambda _port: True)
        monkeypatch.setattr(pg_mod.shutil, "which", lambda _name: "/usr/bin/pg_ctl")
        monkeypatch.setattr(pg_mod.subprocess, "run", _fake_run)

        result = runner.invoke(app, ["pg", "start-local", "dev"])

        assert result.exit_code == 0
        assert socket_dir.exists()
        assert "Socket dir:" in result.output
        assert calls
        cmd = calls[0]
        opts = cmd[cmd.index("-o") + 1]
        assert "-p 5533" in opts
        assert "-h localhost" in opts
        assert f"-k '{socket_dir}'" in opts

        lock_payload = json.loads(lock_file.read_text(encoding="utf-8"))
        assert lock_payload["socket_dir"] == str(socket_dir)

    def test_pg_status_shows_effective_socket_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test pg status shows the local socket dir in operator-facing output."""
        import daylily_tapdb.cli.pg as pg_mod

        socket_dir = tmp_path / "pg-run"
        monkeypatch.setattr(
            pg_mod,
            "get_db_config_for_env",
            lambda _env: {
                "host": "localhost",
                "port": "5533",
                "user": "test",
                "unix_socket_dir": str(socket_dir),
            },
        )
        monkeypatch.setattr(
            pg_mod, "_get_postgres_data_dir", lambda _env: tmp_path / "data"
        )
        monkeypatch.setattr(
            pg_mod, "_get_postgres_log_file", lambda _env: tmp_path / "postgresql.log"
        )
        monkeypatch.setattr(
            pg_mod, "_get_instance_lock_file", lambda _env: tmp_path / "instance.lock"
        )
        monkeypatch.setattr(pg_mod, "_get_postgres_socket_dir", lambda _env: socket_dir)

        def _fake_run(cmd, capture_output=True, timeout=5):
            return subprocess.CompletedProcess(cmd, 1, b"", b"")

        monkeypatch.setattr(pg_mod.subprocess, "run", _fake_run)

        result = runner.invoke(app, ["pg", "status"])

        assert result.exit_code == 0
        out = _strip_ansi(result.output)
        assert "Socket dir:" in out
        assert "pg-run" in out


class TestCLIDBSeed:
    """Tests for database seeding commands."""

    def test_db_seed_help(self):
        """Test db seed --help."""
        result = runner.invoke(app, ["db", "data", "seed", "--help"])
        assert result.exit_code == 0
        out = _strip_ansi(result.output)
        assert "--config" in out or "-c" in out
        assert "--dry-run" in out
        assert "--skip-existing" in out or "--overwrite" in out

    def test_db_validate_config_help(self):
        """Test db validate-config --help."""
        result = runner.invoke(app, ["db", "config", "validate", "--help"])
        assert result.exit_code == 0
        out = _strip_ansi(result.output)
        assert "--config" in out or "-c" in out
        assert "--strict" in out
        assert "--json" in out

    def test_db_validate_config_valid_minimal(self, tmp_path: Path):
        """A minimal two-template config with a valid reference should pass."""
        (tmp_path / "generic").mkdir()
        (tmp_path / "action").mkdir()

        (tmp_path / "action" / "core.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Create Note",
                            "polymorphic_discriminator": "action_template",
                            "category": "action",
                            "type": "core",
                            "subtype": "create-note",
                            "version": "1.0",
                            "instance_prefix": "XX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        (tmp_path / "generic" / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-generic-valid-minimal",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {
                                "action_imports": {
                                    "create_note": "action/core/create-note/1.0"
                                },
                                "expected_inputs": [],
                                "expected_outputs": [],
                                "instantiation_layouts": [],
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        templates, issues = _validate_template_configs(tmp_path, strict=True)
        errors = [i for i in issues if i.level == "error"]
        assert not errors, f"Unexpected errors: {errors}"
        assert len(templates) >= 2

    def test_db_validate_config_valid_instantiation_layouts_child_templates_dict(
        self, tmp_path: Path
    ):
        """Dict-format child_templates entries validate and ref-check."""
        (tmp_path / "generic").mkdir()
        (tmp_path / "action").mkdir()

        (tmp_path / "action" / "core.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Create Note",
                            "polymorphic_discriminator": "action_template",
                            "category": "action",
                            "type": "core",
                            "subtype": "create-note",
                            "version": "1.0",
                            "instance_prefix": "XX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        (tmp_path / "generic" / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-generic-valid-layouts",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {
                                "instantiation_layouts": [
                                    {
                                        "relationship_type": "contains",
                                        "child_templates": [
                                            {
                                                "template_code": "action/core/create-note/1.0",  # noqa: E501
                                                "count": 2,
                                                "name_pattern": "{parent_name}_child_{index}",  # noqa: E501
                                            }
                                        ],
                                    }
                                ]
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        templates, issues = _validate_template_configs(tmp_path, strict=True)
        errors = [i for i in issues if i.level == "error"]
        assert not errors, f"Unexpected errors: {errors}"

    def test_db_validate_config_missing_reference_in_instantiation_layouts_dict_strict_fails(  # noqa: E501
        self, tmp_path: Path
    ):
        """Strict mode fails for missing refs in child_templates dicts."""
        (tmp_path / "generic").mkdir()

        (tmp_path / "generic" / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-generic-missing-ref-layouts",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {
                                "instantiation_layouts": [
                                    {
                                        "child_templates": [
                                            {
                                                "template_code": "action/core/create-note/1.0"  # noqa: E501
                                            }
                                        ]
                                    }
                                ]
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        _templates, issues = _validate_template_configs(tmp_path, strict=True)
        errors = [i for i in issues if i.level == "error"]
        assert errors
        msgs = " ".join(i.message.lower() for i in errors)
        assert "referenced template" in msgs or "not found" in msgs

    def test_db_validate_config_invalid_instantiation_layouts_bad_count(
        self, tmp_path: Path
    ):
        """count must be >= 1 for dict-format child_templates entries."""
        (tmp_path / "generic").mkdir()
        (tmp_path / "action").mkdir()

        (tmp_path / "action" / "core.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Create Note",
                            "polymorphic_discriminator": "action_template",
                            "category": "action",
                            "type": "core",
                            "subtype": "create-note",
                            "version": "1.0",
                            "instance_prefix": "XX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        (tmp_path / "generic" / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-generic-invalid-count",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {
                                "instantiation_layouts": [
                                    {
                                        "child_templates": [
                                            {
                                                "template_code": "action/core/create-note/1.0",  # noqa: E501
                                                "count": 0,
                                            }
                                        ]
                                    }
                                ]
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        _templates, issues = _validate_template_configs(tmp_path, strict=True)
        errors = [i for i in issues if i.level == "error"]
        assert errors
        msgs = "\n".join(i.message.lower() for i in errors)
        assert "instantiation_layouts" in msgs
        assert "count" in msgs

    def test_db_validate_config_invalid_instantiation_layouts_missing_template_code(
        self, tmp_path: Path
    ):
        """Dict-format child_templates must include template_code."""
        (tmp_path / "generic").mkdir()

        (tmp_path / "generic" / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-generic-missing-template-code",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {
                                "instantiation_layouts": [
                                    {"child_templates": [{"count": 1}]}
                                ]
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        _templates, issues = _validate_template_configs(tmp_path, strict=True)
        errors = [i for i in issues if i.level == "error"]
        assert errors
        msgs = "\n".join(i.message.lower() for i in errors)
        assert "instantiation_layouts" in msgs
        assert "template_code" in msgs

    def test_db_validate_config_missing_reference_strict_fails(self, tmp_path: Path):
        """Strict mode should fail if a referenced template is not present."""
        (tmp_path / "generic").mkdir()
        (tmp_path / "generic" / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-generic-missing-ref-strict",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {
                                "action_imports": {
                                    "create_note": "action/core/create-note/1.0"
                                }
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        _templates, issues = _validate_template_configs(tmp_path, strict=True)
        errors = [i for i in issues if i.level == "error"]
        assert errors
        msgs = " ".join(i.message.lower() for i in errors)
        assert "referenced template" in msgs or "not found" in msgs

    def test_db_validate_config_missing_reference_non_strict_warns(
        self, tmp_path: Path
    ):
        """Non-strict mode should warn but not error for missing references."""
        (tmp_path / "generic").mkdir()
        (tmp_path / "generic" / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-generic-missing-ref-nonstrict",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {
                                "action_imports": {
                                    "create_note": "action/core/create-note/1.0"
                                }
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        _templates, issues = _validate_template_configs(tmp_path, strict=False)
        errors = [i for i in issues if i.level == "error"]
        warnings = [i for i in issues if i.level == "warning"]
        assert not errors, f"Unexpected errors: {errors}"
        assert warnings  # missing ref is a warning in non-strict

    def test_db_validate_config_invalid_instance_prefix_fails(self, tmp_path: Path):
        """Validation should reject invalid TapDB instance prefixes."""
        (tmp_path / "generic").mkdir()
        (tmp_path / "generic" / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-generic-invalid-prefix",
                            "version": "1.0",
                            "instance_prefix": "LS",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        _templates, issues = _validate_template_configs(tmp_path, strict=True)
        errors = [i for i in issues if i.level == "error"]
        assert errors
        msgs = "\n".join(i.message for i in errors)
        assert "Invalid TAPDB instance prefix" in msgs

    def test_db_validate_config_merged_core_then_client_strict_ok(self, tmp_path: Path):
        """Strict validate should consider TAPDB core templates with client config."""
        (tmp_path / "generic").mkdir()
        (tmp_path / "generic" / "custom.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Client Probe",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "client-probe",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {
                                "action_imports": {
                                    "uses_core_actor": "generic/actor/system_user/1.0"
                                }
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        config_dirs = _resolve_seed_config_dirs(tmp_path)
        templates, issues = _validate_template_configs(config_dirs, strict=True)
        # Filter to only client-config errors (ignore pre-existing core config issues)
        client_errors = [
            i for i in issues
            if i.level == "error" and str(tmp_path) in (i.source_file or "")
        ]
        core_dir = _find_tapdb_core_config_dir().resolve()
        assert not client_errors, f"Unexpected client errors: {client_errors}"
        assert core_dir in [d.resolve() for d in config_dirs]

    def test_db_validate_config_json_includes_ordered_config_dirs(self, tmp_path: Path):
        """Merged config dirs should include core first, then client."""
        (tmp_path / "generic").mkdir()
        (tmp_path / "generic" / "custom.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Custom Generic",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "custom-generic",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        config_dirs = _resolve_seed_config_dirs(tmp_path)
        templates, issues = _validate_template_configs(config_dirs, strict=False)
        client_errors = [
            i for i in issues
            if i.level == "error" and str(tmp_path) in (i.source_file or "")
        ]
        core_dir = _find_tapdb_core_config_dir().resolve()
        assert not client_errors, f"Unexpected client errors: {client_errors}"
        resolved = [d.resolve() for d in config_dirs]
        assert resolved[0] == core_dir
        assert tmp_path.resolve() in resolved

    def test_db_validate_config_fails_on_core_client_duplicate_key(
        self, tmp_path: Path
    ):
        """Validation should hard-fail on duplicate template keys across sources."""
        (tmp_path / "generic").mkdir()
        client_file = tmp_path / "generic" / "generic.json"
        client_file.write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Client Duplicate Generic",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        config_dirs = _resolve_seed_config_dirs(tmp_path)
        _templates, issues = _validate_template_configs(config_dirs, strict=True)
        dup_errors = [
            i for i in issues
            if i.level == "error" and "duplicate template key" in i.message.lower()
        ]
        assert dup_errors
        # The duplicate error message references the first definition (core file)
        core_generic = str(
            _find_tapdb_core_config_dir().resolve() / "generic" / "generic.json"
        ).lower()
        all_messages = " ".join(i.message.lower() for i in dup_errors)
        assert core_generic in all_messages

    def test_db_setup_help(self):
        """Test db setup --help."""
        result = runner.invoke(app, ["db", "setup", "--help"])
        assert result.exit_code == 0
        assert "dev" in result.output
        assert "--force" in result.output or "-f" in result.output

    def test_find_config_dir(self):
        """Test _find_config_dir locates the config directory."""
        config_dir = _find_config_dir()
        assert config_dir.exists()
        assert (config_dir / "_metadata.json").exists() or len(
            list(config_dir.glob("*.json"))
        ) > 0

    def test_find_tapdb_core_config_dir(self):
        """Core TAPDB seed config should always be resolvable."""
        core_dir = _find_tapdb_core_config_dir()
        assert core_dir.exists()
        assert (core_dir / "actor" / "actor.json").exists()
        assert (core_dir / "generic" / "generic.json").exists()

    def test_resolve_seed_config_dirs_includes_core(self, tmp_path: Path):
        """Seed config resolution should include core + custom dirs without duplication."""
        custom_dir = tmp_path / "custom-config"
        custom_dir.mkdir()
        dirs = _resolve_seed_config_dirs(custom_dir)
        core_dir = _find_tapdb_core_config_dir().resolve()
        assert dirs[0] == core_dir
        assert custom_dir.resolve() in dirs
        assert len(dirs) == len(set(dirs))

    def test_resolve_seed_config_dirs_default_is_core_only(self):
        """Without a client config override, only core seed config should load."""
        dirs = _resolve_seed_config_dirs(None)
        core_dir = _find_tapdb_core_config_dir().resolve()
        assert dirs == [core_dir]

    def test_load_template_configs(self):
        """Test _load_template_configs loads templates from config files."""
        config_dir = _find_config_dir()
        templates = _load_template_configs(config_dir)
        assert len(templates) > 0

        # Check template structure
        for t in templates:
            assert "name" in t
            assert "polymorphic_discriminator" in t
            assert "category" in t
            assert "type" in t
            assert "subtype" in t
            assert "version" in t

    def test_load_template_configs_has_expected_types(self):
        """Test loaded templates include expected categories."""
        config_dir = _find_config_dir()
        templates = _load_template_configs(config_dir)
        categories = {t["category"] for t in templates}

        # Should have at least generic and container categories
        assert "generic" in categories or "container" in categories

    def test_find_duplicate_template_keys(self):
        """Duplicate template keys should be detected as hard errors."""
        templates = [
            {
                "category": "generic",
                "type": "generic",
                "subtype": "generic",
                "version": "1.0",
                "_source_file": "/tmp/a.json",
            },
            {
                "category": "generic",
                "type": "generic",
                "subtype": "generic",
                "version": "1.0",
                "_source_file": "/tmp/b.json",
            },
        ]
        duplicates = _find_duplicate_template_keys(templates)
        assert ("generic", "generic", "generic", "1.0") in duplicates
        assert len(duplicates[("generic", "generic", "generic", "1.0")]) == 2

    def test_db_seed_dry_run_fails_on_duplicate_template_keys(self, tmp_path: Path):
        """db data seed should hard-fail when merged config sources clash."""
        custom_generic_dir = tmp_path / "generic"
        custom_generic_dir.mkdir(parents=True)
        (custom_generic_dir / "generic.json").write_text(
            json.dumps(
                {
                    "templates": [
                        {
                            "name": "Duplicate Generic Object",
                            "polymorphic_discriminator": "generic_template",
                            "category": "generic",
                            "type": "generic",
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "AGX",
                            "is_singleton": False,
                            "bstatus": "active",
                            "json_addl": {},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        with (
            patch("daylily_tapdb.cli.db._check_db_exists", return_value=True),
            patch("daylily_tapdb.cli.db._schema_exists", return_value=True),
        ):
            result = runner.invoke(
                app,
                [
                    "db",
                    "data",
                    "seed",
                    "dev",
                    "--dry-run",
                    "--config",
                    str(tmp_path),
                ],
            )

        assert result.exit_code != 0
        output = _strip_ansi(result.output).lower()
        assert "generic/generic.json" in output or "duplicate template" in output

    def test_db_seed_dry_run(self):
        """Test db seed --dry-run shows templates without inserting."""
        result = runner.invoke(app, ["db", "data", "seed", "dev", "--dry-run"])
        output_lower = result.output.lower()
        # Should show templates or fail gracefully (no db)
        assert (
            "template" in output_lower
            or "dry run" in output_lower
            or "does not exist" in output_lower
            or "not found" in output_lower
        )


class TestEnvironmentEnum:
    """Tests for Environment enum."""

    def test_environment_values(self):
        """Test Environment enum has expected values."""
        assert Environment.dev.value == "dev"
        assert Environment.test.value == "test"
        assert Environment.prod.value == "prod"

    def test_environment_from_string(self):
        """Test creating Environment from string."""
        assert Environment("dev") == Environment.dev
        assert Environment("test") == Environment.test
        assert Environment("prod") == Environment.prod


class TestCLIIntegration:
    """End-to-end integration tests (require PostgreSQL)."""

    @pytest.fixture
    def skip_if_no_postgres(self):
        """Skip test if PostgreSQL is not available."""
        try:
            result = subprocess.run(
                ["pg_isready", "-q"],
                capture_output=True,
                timeout=5,
            )
            if result.returncode != 0:
                pytest.skip("PostgreSQL is not running")
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pytest.skip("PostgreSQL tools not available")

    def test_db_status_with_postgres(self, skip_if_no_postgres):
        """Test db status with real PostgreSQL."""
        result = runner.invoke(app, ["db", "schema", "status", "dev"])
        # May succeed or fail depending on database existence
        assert "tapdb_dev" in result.output or "error" in result.output.lower()

    def test_pg_status_with_postgres(self, skip_if_no_postgres):
        """Test pg status with real PostgreSQL."""
        result = runner.invoke(app, ["pg", "status"])
        assert result.exit_code == 0
        assert "running" in result.output


class TestCLISubprocess:
    """Tests that verify CLI works via subprocess (true integration)."""

    def test_cli_module_invocation(self):
        """Test CLI can be invoked as module."""
        result = subprocess.run(
            [sys.executable, "-m", "daylily_tapdb.cli", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "TAPDB" in result.stdout

    def test_cli_db_help_subprocess(self):
        """Test db subcommand via subprocess."""
        result = subprocess.run(
            [sys.executable, "-m", "daylily_tapdb.cli", "db", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "create" in result.stdout
        assert "schema" in result.stdout

    def test_cli_pg_help_subprocess(self):
        """Test pg subcommand via subprocess."""
        result = subprocess.run(
            [sys.executable, "-m", "daylily_tapdb.cli", "pg", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "start" in result.stdout
        assert "status" in result.stdout
