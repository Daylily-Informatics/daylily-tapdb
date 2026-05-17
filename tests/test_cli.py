"""CLI contract tests for the explicit-target TapDB model."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.cli import app, build_app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db import Environment, _get_db_config

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip(s: str) -> str:
    return _ANSI_RE.sub("", s)


def _write_registries(base_dir: Path) -> tuple[Path, Path]:
    domain_registry = base_dir / "domain_code_registry.json"
    prefix_registry = base_dir / "prefix_ownership_registry.json"
    domain_registry.write_text(
        json.dumps({"version": "0.4.0", "domains": {"Z": {"name": "test"}}}) + "\n",
        encoding="utf-8",
    )
    prefix_registry.write_text(
        json.dumps(
            {
                "version": "0.4.0",
                "ownership": {
                    "Z": {
                        "TPX": {"issuer_app_code": "daylily-tapdb"},
                        "EDG": {"issuer_app_code": "daylily-tapdb"},
                        "ADT": {"issuer_app_code": "daylily-tapdb"},
                        "SYS": {"issuer_app_code": "daylily-tapdb"},
                        "MSG": {"issuer_app_code": "daylily-tapdb"},
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return domain_registry, prefix_registry


def _write_config(
    path: Path,
    *,
    client_id: str = "testclient",
    database_name: str = "testdb",
    database: str = "tapdb_shared",
    schema_name: str = "tapdb_testdb",
    safety_tier: str = "shared",
    destructive_operations: str = "confirm_required",
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    domain_registry, prefix_registry = _write_registries(path.parent)
    path.write_text(
        "meta:\n"
        "  config_version: 4\n"
        f"  client_id: {client_id}\n"
        f"  database_name: {database_name}\n"
        "  owner_repo_name: daylily-tapdb\n"
        f"  domain_registry_path: {domain_registry}\n"
        f"  prefix_ownership_registry_path: {prefix_registry}\n"
        "admin:\n"
        "  footer:\n"
        "    repo_url: https://example.com/tapdb\n"
        "  session:\n"
        "    secret: secret123\n"
        "  auth:\n"
        "    mode: tapdb\n"
        "    disabled_user:\n"
        "      email: tapdb-admin@localhost\n"
        "      role: admin\n"
        "    shared_host:\n"
        "      session_secret: shared-secret\n"
        "      session_cookie: session\n"
        "      session_max_age_seconds: 1209600\n"
        "  cors:\n"
        "    allowed_origins: []\n"
        "  ui:\n"
        "    tls:\n"
        "      cert_path: ''\n"
        "      key_path: ''\n"
        "  metrics:\n"
        "    enabled: true\n"
        "    queue_max: 20000\n"
        "    flush_seconds: 1.0\n"
        "target:\n"
        "  engine_type: local\n"
        "  host: localhost\n"
        "  port: '5533'\n"
        "  ui_port: '8911'\n"
        "  domain_code: Z\n"
        "  user: tapdb\n"
        "  password: filepw\n"
        f"  database: {database}\n"
        f"  schema_name: {schema_name}\n"
        "safety:\n"
        f"  safety_tier: {safety_tier}\n"
        f"  destructive_operations: {destructive_operations}\n",
        encoding="utf-8",
    )
    os.chmod(path, 0o600)
    return path


@pytest.fixture(autouse=True)
def _isolate_cli_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "testclient" / "testdb" / "tapdb-config.yaml"
    )
    monkeypatch.setenv("HOME", str(tmp_path))
    clear_cli_context()
    set_cli_context(config_path=cfg_path)
    monkeypatch.setattr(cli_mod, "PID_FILE", tmp_path / "ui.pid")
    monkeypatch.setattr(cli_mod, "LOG_FILE", tmp_path / "ui.log")
    yield cfg_path
    clear_cli_context()


class TestRootCLI:
    def test_help_shows_command_groups_without_env_selector(self):
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        out = _strip(result.output)
        assert "TAPDB" in out
        assert "bootstrap" in out
        assert "ui" in out
        assert "db" in out
        assert "pg" in out
        assert "cognito" in out
        assert "--env" not in out

    def test_version(self):
        result = runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert "daylily-tapdb" in result.output

    def test_info_json_reports_single_explicit_target(self):
        with patch("shutil.which", return_value=None):
            result = runner.invoke(app, ["info", "--json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["target"] == "explicit"
        assert payload["client_id"] == "testclient"
        assert payload["database_name"] == "testdb"
        assert payload["schema_name"] == "tapdb_testdb"
        assert payload["postgres"]["target"] == "explicit"
        assert payload["postgres"]["schema_name"] == "tapdb_testdb"
        assert "tapdb_env" not in payload
        assert "check_all_envs" not in payload

    def test_info_human_reports_target_and_schema(self):
        with patch("shutil.which", return_value=None):
            result = runner.invoke(app, ["info"])

        assert result.exit_code == 0
        out = _strip(result.output)
        assert "Target" in out
        assert "explicit" in out
        assert "tapdb_testdb" in out
        assert "DB probes" not in out

    def test_root_env_option_is_removed(self, tmp_path: Path):
        cfg_path = _write_config(tmp_path / "tapdb-config.yaml")

        result = runner.invoke(app, ["--config", str(cfg_path), "--env", "dev", "info"])

        assert result.exit_code == 2
        assert "No such option" in result.output

    def test_runtime_command_requires_explicit_config(self):
        clear_cli_context()

        result = runner.invoke(app, ["info"])

        assert result.exit_code != 0
        assert isinstance(result.exception, RuntimeError)
        assert str(result.exception) == "TapDB config path is required. Set --config."


class TestConfigCLI:
    def test_config_init_requires_explicit_config_path(self):
        clear_cli_context()

        result = runner.invoke(
            app,
            [
                "config",
                "init",
                "--client-id",
                "atlas",
                "--database-name",
                "atlas-dayfly5",
                "--owner-repo-name",
                "lsmc-atlas",
                "--domain-code",
                "Z",
                "--engine-type",
                "local",
                "--host",
                "localhost",
                "--port",
                "5533",
                "--ui-port",
                "8911",
                "--user",
                "tapdb",
                "--database",
                "tapdb_dayfly5",
                "--schema-name",
                "tapdb_atlas_dayfly5",
            ],
        )

        assert result.exit_code != 0
        assert "TapDB config commands require --config" in str(result.exception)

    def test_config_init_writes_single_target_and_removes_legacy_environments(
        self, tmp_path: Path
    ):
        cfg_path = tmp_path / "tapdb-config.yaml"

        result = runner.invoke(
            app,
            [
                "--config",
                str(cfg_path),
                "config",
                "init",
                "--client-id",
                "atlas",
                "--database-name",
                "atlas-dayfly5",
                "--owner-repo-name",
                "lsmc-atlas",
                "--domain-code",
                "Z",
                "--engine-type",
                "local",
                "--host",
                "localhost",
                "--port",
                "5533",
                "--ui-port",
                "8911",
                "--user",
                "tapdb",
                "--database",
                "tapdb_dayfly5",
                "--schema-name",
                "tapdb_atlas_dayfly5",
                "--safety-tier",
                "shared",
                "--destructive-operations",
                "blocked",
            ],
        )

        assert result.exit_code == 0, result.output
        root = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        assert root["meta"]["config_version"] == 4
        assert root["meta"]["client_id"] == "atlas"
        assert root["target"]["database"] == "tapdb_dayfly5"
        assert root["target"]["schema_name"] == "tapdb_atlas_dayfly5"
        assert root["safety"] == {
            "safety_tier": "shared",
            "destructive_operations": "blocked",
        }
        assert "environments" not in root

    def test_config_update_targets_single_target_and_safety_fields(self):
        result = runner.invoke(
            app,
            [
                "config",
                "update",
                "--host",
                "localhost",
                "--port",
                "5544",
                "--schema-name",
                "tapdb_changed",
                "--support-email",
                "support@example.com",
                "--safety-tier",
                "production",
                "--destructive-operations",
                "blocked",
            ],
        )

        assert result.exit_code == 0, result.output
        active_cfg = yaml.safe_load(
            Path(cli_mod.active_context_overrides()["config_path"]).read_text(
                encoding="utf-8"
            )
        )
        assert active_cfg["target"]["port"] == "5544"
        assert active_cfg["target"]["schema_name"] == "tapdb_changed"
        assert active_cfg["target"]["support_email"] == "support@example.com"
        assert active_cfg["safety"]["safety_tier"] == "production"
        assert active_cfg["safety"]["destructive_operations"] == "blocked"
        assert "environments" not in active_cfg

    def test_config_update_clear_is_limited_to_target_fields(self):
        result = runner.invoke(app, ["config", "update", "--clear", "not_a_field"])

        assert result.exit_code != 0
        assert "Unknown target field" in str(result.exception)


class TestExplicitTargetRuntime:
    def test_environment_enum_has_only_explicit_target(self):
        assert [item.value for item in Environment] == ["target"]
        assert Environment("target") is Environment.target

    def test_db_config_loader_ignores_ambient_legacy_env(self, monkeypatch):
        monkeypatch.setenv("TAPDB_ENV", "prod")
        monkeypatch.setenv("TAPDB_DEV_HOST", "wrong.example.com")
        monkeypatch.setenv("PGPORT", "9999")

        cfg = _get_db_config(Environment.target)

        assert cfg["client_id"] == "testclient"
        assert cfg["database_name"] == "testdb"
        assert cfg["host"] == "localhost"
        assert cfg["port"] == "5533"
        assert cfg["schema_name"] == "tapdb_testdb"

    def test_db_create_rejects_old_positional_env(self):
        result = runner.invoke(app, ["db", "create", "dev"])

        assert result.exit_code == 2
        assert "unexpected extra argument" in result.output.lower()

    def test_db_help_has_no_dev_prod_target_selector(self):
        result = runner.invoke(app, ["db", "--help"])

        assert result.exit_code == 0
        out = _strip(result.output).lower()
        assert "--env" not in out
        assert "dev|test|prod" not in out

    def test_pg_help_has_no_env_target_selector(self):
        result = runner.invoke(app, ["pg", "--help"])

        assert result.exit_code == 0
        out = _strip(result.output).lower()
        assert "--env" not in out
        assert "dev|test|prod" not in out


class TestBootstrapCLI:
    def test_bootstrap_local_uses_explicit_target(self, tmp_path: Path):
        cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
        calls: list[tuple[str, object]] = []

        def _record(name: str):
            def inner(*args, **kwargs):
                calls.append((name, kwargs.get("env")))

            return inner

        with (
            patch("daylily_tapdb.cli.pg.pg_init", _record("pg_init")),
            patch("daylily_tapdb.cli.pg.pg_start_local", _record("pg_start_local")),
            patch("daylily_tapdb.cli.db.create_database", _record("create_database")),
            patch("daylily_tapdb.cli.db.apply_schema", _record("apply_schema")),
            patch("daylily_tapdb.cli.db.run_migrations", _record("run_migrations")),
            patch("daylily_tapdb.cli.db.seed_templates", _record("seed_templates")),
            patch(
                "daylily_tapdb.cli.db._create_default_admin",
                _record("create_default_admin"),
            ),
        ):
            fresh_app = build_app()
            result = runner.invoke(
                fresh_app,
                ["--config", str(cfg_path), "bootstrap", "local", "--no-gui"],
            )

        assert result.exit_code == 0, result.output
        env_calls = {name: env for name, env in calls if env is not None}
        assert env_calls == {
            "create_database": Environment.target,
            "apply_schema": Environment.target,
            "run_migrations": Environment.target,
            "seed_templates": Environment.target,
            "create_default_admin": Environment.target,
        }

    def test_bootstrap_local_rejects_old_env_option(self, tmp_path: Path):
        cfg_path = _write_config(tmp_path / "tapdb-config.yaml")

        result = runner.invoke(
            app,
            ["--config", str(cfg_path), "bootstrap", "local", "--env", "dev"],
        )

        assert result.exit_code == 2
        assert "No such option" in result.output
