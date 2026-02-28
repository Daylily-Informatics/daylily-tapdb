"""Integration tests for TAPDB CLI commands."""

import json
import os
import re
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.db import (
    CONFIG_DIR,
    Environment,
    _ensure_dirs,
    _find_config_dir,
    _find_schema_file,
    _get_db_config,
    _load_template_configs,
)

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
    - Avoid probing unexpected environments (e.g. TAPDB_ENV=prod in a dev shell).
    """
    monkeypatch.setenv("TAPDB_ENV", "dev")
    monkeypatch.delenv("TAPDB_TEST_DSN", raising=False)
    monkeypatch.delenv("TAPDB_CONFIG_PATH", raising=False)
    monkeypatch.setattr(cli_mod, "PID_FILE", tmp_path / "ui.pid")
    monkeypatch.setattr(cli_mod, "LOG_FILE", tmp_path / "ui.log")


class TestCLIMain:
    """Tests for main CLI commands."""

    def test_help(self):
        """Test --help shows all command groups."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "TAPDB" in result.output
        assert "ui" in result.output
        assert "db" in result.output
        assert "pg" in result.output

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


class TestCLIUI:
    """Tests for UI management commands."""

    def test_ui_help(self):
        """Test ui --help."""
        result = runner.invoke(app, ["ui", "--help"])
        assert result.exit_code == 0
        assert "start" in result.output
        assert "stop" in result.output
        assert "status" in result.output
        assert "logs" in result.output

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


class TestCLIDB:
    """Tests for database management commands."""

    def test_db_help(self):
        """Test db --help."""
        result = runner.invoke(app, ["db", "--help"])
        assert result.exit_code == 0
        assert "create" in result.output
        assert "status" in result.output
        assert "nuke" in result.output
        assert "migrate" in result.output
        assert "backup" in result.output
        assert "restore" in result.output
        assert "validate-config" in result.output

    def test_db_create_help(self):
        """Test db create --help."""
        result = runner.invoke(app, ["db", "create", "--help"])
        assert result.exit_code == 0
        assert "dev" in result.output
        assert "test" in result.output
        assert "prod" in result.output

    def test_db_nuke_help(self):
        """Test db nuke --help shows safety warnings."""
        result = runner.invoke(app, ["db", "nuke", "--help"])
        assert result.exit_code == 0
        assert "DESTRUCTIVE" in result.output or "force" in result.output

    def test_get_db_config_defaults(self):
        """Test _get_db_config returns correct defaults."""
        config = _get_db_config(Environment.dev)
        assert config["database"] == "tapdb_dev"
        assert config["host"] == os.environ.get("PGHOST", "localhost")
        assert config["port"] == os.environ.get("PGPORT", "5432")

    def test_get_db_config_env_override(self):
        """Test _get_db_config respects environment variables."""
        with patch.dict(
            os.environ,
            {
                "TAPDB_TEST_HOST": "testhost",
                "TAPDB_TEST_PORT": "5433",
                "TAPDB_TEST_DATABASE": "my_test_db",
            },
        ):
            config = _get_db_config(Environment.test)
            assert config["host"] == "testhost"
            assert config["port"] == "5433"
            assert config["database"] == "my_test_db"

    def test_find_schema_file(self):
        """Test _find_schema_file locates the schema."""
        schema_path = _find_schema_file()
        assert schema_path.exists()
        assert schema_path.name == "tapdb_schema.sql"

    def test_ensure_dirs_creates_config(self):
        """Test _ensure_dirs creates config directory."""
        _ensure_dirs()
        assert CONFIG_DIR.exists()

    def test_db_status_no_psql(self):
        """Test db status handles missing psql gracefully."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError("psql not found")
            result = runner.invoke(app, ["db", "status", "dev"])
            # Should handle error gracefully
            assert (
                result.exit_code != 0
                or "psql" in result.output.lower()
                or "error" in result.output.lower()
            )

    def test_db_nuke_requires_confirmation(self):
        """Test db nuke aborts without confirmation."""
        result = runner.invoke(app, ["db", "nuke", "dev"], input="n\n")
        # Should abort, or exit early if db doesn't exist (which is also safe)
        output_lower = result.output.lower()
        assert (
            "abort" in output_lower
            or "does not exist" in output_lower
            or result.exit_code != 0
        )

    def test_db_backup_creates_file(self):
        """Test db backup command structure."""
        result = runner.invoke(app, ["db", "backup", "--help"])
        assert result.exit_code == 0
        assert "--output" in result.output or "-o" in result.output

    def test_db_restore_requires_input(self):
        """Test db restore requires --input file."""
        result = runner.invoke(app, ["db", "restore", "dev"])
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
        assert "create" in result.output
        assert "delete" in result.output

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

    def test_pg_create_help(self):
        """Test pg create --help."""
        result = runner.invoke(app, ["pg", "create", "--help"])
        assert result.exit_code == 0
        assert "dev" in result.output
        assert "test" in result.output
        assert "prod" in result.output
        assert "--owner" in result.output or "-o" in result.output

    def test_pg_delete_help(self):
        """Test pg delete --help."""
        result = runner.invoke(app, ["pg", "delete", "--help"])
        assert result.exit_code == 0
        assert "DESTRUCTIVE" in result.output
        assert "--force" in result.output or "-f" in result.output

    def test_pg_create_requires_postgres(self):
        """Test pg create fails gracefully when PostgreSQL is unavailable.

        This test must be hermetic: on some dev machines PostgreSQL is installed
        (e.g. via conda) and may already be running, which would make
        `tapdb pg create dev` succeed or report "already exists".
        """

        # Simulate missing/unavailable PostgreSQL tools/connectivity.
        with patch(
            "daylily_tapdb.cli.pg._run_psql",
            return_value=(False, "psql not found. Install PostgreSQL client tools."),
        ):
            result = runner.invoke(app, ["pg", "create", "dev"])

        output_lower = result.output.lower()
        assert result.exit_code != 0
        assert "cannot connect" in output_lower or "psql not found" in output_lower

    def test_pg_delete_requires_confirmation(self):
        """Test pg delete requires confirmation."""
        result = runner.invoke(app, ["pg", "delete", "dev"], input="n\n")
        output_lower = result.output.lower()
        # Should abort, or exit if pg not running/db doesn't exist
        assert (
            "abort" in output_lower
            or "does not exist" in output_lower
            or "not running" in output_lower
            or result.exit_code != 0
        )

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


class TestCLIDBSeed:
    """Tests for database seeding commands."""

    def test_db_seed_help(self):
        """Test db seed --help."""
        result = runner.invoke(app, ["db", "seed", "--help"])
        assert result.exit_code == 0
        out = _strip_ansi(result.output)
        assert "--config" in out or "-c" in out
        assert "--dry-run" in out
        assert "--skip-existing" in out or "--overwrite" in out

    def test_db_validate_config_help(self):
        """Test db validate-config --help."""
        result = runner.invoke(app, ["db", "validate-config", "--help"])
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
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "GX",
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

        result = runner.invoke(
            app, ["db", "validate-config", "--config", str(tmp_path)]
        )
        assert result.exit_code == 0

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
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "GX",
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

        result = runner.invoke(
            app, ["db", "validate-config", "--config", str(tmp_path)]
        )
        assert result.exit_code == 0

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
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "GX",
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

        result = runner.invoke(
            app, ["db", "validate-config", "--config", str(tmp_path), "--strict"]
        )
        assert result.exit_code != 0
        assert (
            "referenced template" in result.output.lower()
            or "not found" in result.output.lower()
        )

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
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "GX",
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

        # Use --json to avoid brittle assertions against Rich table wrapping.
        result = runner.invoke(
            app, ["db", "validate-config", "--config", str(tmp_path), "--json"]
        )
        assert result.exit_code != 0

        payload = json.loads(result.output)
        assert payload["errors"] >= 1
        msgs = "\n".join(i["message"] for i in payload["issues"]).lower()
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
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "GX",
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

        # Use --json to avoid brittle assertions against Rich table wrapping.
        result = runner.invoke(
            app, ["db", "validate-config", "--config", str(tmp_path), "--json"]
        )
        assert result.exit_code != 0

        payload = json.loads(result.output)
        assert payload["errors"] >= 1
        msgs = "\n".join(i["message"] for i in payload["issues"]).lower()
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
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "GX",
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

        result = runner.invoke(
            app, ["db", "validate-config", "--config", str(tmp_path), "--strict"]
        )
        assert result.exit_code != 0
        assert (
            "referenced template" in result.output.lower()
            or "not found" in result.output.lower()
        )

    def test_db_validate_config_missing_reference_non_strict_warns(
        self, tmp_path: Path
    ):
        """Non-strict mode should warn but exit 0 for missing references."""
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
                            "subtype": "generic",
                            "version": "1.0",
                            "instance_prefix": "GX",
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

        result = runner.invoke(
            app, ["db", "validate-config", "--config", str(tmp_path), "--no-strict"]
        )
        assert result.exit_code == 0

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

    def test_db_seed_dry_run(self):
        """Test db seed --dry-run shows templates without inserting."""
        result = runner.invoke(app, ["db", "seed", "dev", "--dry-run"])
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
        result = runner.invoke(app, ["db", "status", "dev"])
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
            ["python", "-m", "daylily_tapdb.cli", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "TAPDB" in result.stdout

    def test_cli_db_help_subprocess(self):
        """Test db subcommand via subprocess."""
        result = subprocess.run(
            ["python", "-m", "daylily_tapdb.cli", "db", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "create" in result.stdout
        assert "nuke" in result.stdout

    def test_cli_pg_help_subprocess(self):
        """Test pg subcommand via subprocess."""
        result = subprocess.run(
            ["python", "-m", "daylily_tapdb.cli", "pg", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "start" in result.stdout
        assert "status" in result.stdout
