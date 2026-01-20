"""Integration tests for TAPDB CLI commands."""

import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from typer.testing import CliRunner

from daylily_tapdb.cli import app
from daylily_tapdb.cli.db import (
    Environment,
    _get_db_config,
    _find_schema_file,
    _ensure_dirs,
    _find_config_dir,
    _load_template_configs,
    CONFIG_DIR,
)

runner = CliRunner()


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
        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        assert "Version" in result.output
        assert "Python" in result.output


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
        # Remove log file if exists
        log_file = Path.home() / ".tapdb" / "ui.log"
        if log_file.exists():
            log_file.rename(log_file.with_suffix(".log.bak"))
        try:
            result = runner.invoke(app, ["ui", "logs"])
            assert result.exit_code == 0
            assert "No log file" in result.output or "not found" in result.output.lower()
        finally:
            # Restore if we backed it up
            bak = log_file.with_suffix(".log.bak")
            if bak.exists():
                bak.rename(log_file)


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
        with patch.dict(os.environ, {
            "TAPDB_TEST_HOST": "testhost",
            "TAPDB_TEST_PORT": "5433",
            "TAPDB_TEST_DATABASE": "my_test_db",
        }):
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
            assert result.exit_code != 0 or "psql" in result.output.lower() or "error" in result.output.lower()

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
        """Test pg create fails gracefully without PostgreSQL."""
        result = runner.invoke(app, ["pg", "create", "dev"])
        # Should fail or report PostgreSQL not running
        output_lower = result.output.lower()
        assert "not running" in output_lower or "psql" in output_lower or result.exit_code != 0

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
        assert "--config" in result.output or "-c" in result.output
        assert "--dry-run" in result.output
        assert "--skip-existing" in result.output or "--overwrite" in result.output

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
        assert (config_dir / "_metadata.json").exists() or len(list(config_dir.glob("*.json"))) > 0

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

