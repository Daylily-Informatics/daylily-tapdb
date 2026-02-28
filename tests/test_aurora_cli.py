"""Tests for tapdb aurora CLI commands."""

import json
import re
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from daylily_tapdb.cli import build_app

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip(s: str) -> str:
    return _ANSI_RE.sub("", s)


@pytest.fixture()
def app():
    """Build a fresh Typer app for each test."""
    return build_app()


# ── helpers ──────────────────────────────────────────────────────────


def _mock_stack_manager():
    """Return a MagicMock that quacks like AuroraStackManager."""
    mgr = MagicMock()
    mgr.create_stack.return_value = {
        "stack_name": "tapdb-dev",
        "stack_id": "arn:aws:cloudformation:us-west-2:123:stack/tapdb-dev/abc",
        "outputs": {
            "ClusterEndpoint": "tapdb-dev.cluster-xyz.us-west-2.rds.amazonaws.com",
            "ClusterPort": "5432",
            "SecretArn": "arn:aws:secretsmanager:us-west-2:123:secret:tapdb-dev",
        },
    }
    mgr.initiate_create_stack.return_value = {
        "stack_name": "tapdb-dev",
        "stack_id": "arn:aws:cloudformation:us-west-2:123:stack/tapdb-dev/abc",
        "vpc_id": "vpc-abc123",
    }
    mgr.delete_stack.return_value = {
        "stack_name": "tapdb-dev",
        "status": "DELETE_COMPLETE",
    }
    mgr.get_stack_status.return_value = {
        "stack_name": "tapdb-dev",
        "status": "CREATE_COMPLETE",
        "outputs": {
            "ClusterEndpoint": "tapdb-dev.cluster-xyz.us-west-2.rds.amazonaws.com",
            "ClusterPort": "5432",
        },
    }
    mgr.detect_existing_resources.return_value = {
        "tapdb-dev": {
            "status": "CREATE_COMPLETE",
            "outputs": {
                "ClusterEndpoint": "tapdb-dev.cluster-xyz.us-west-2.rds.amazonaws.com",
            },
            "tags": {"lsmc-cost-center": "global", "lsmc-project": "tapdb-us-west-2"},
        },
    }
    return mgr


# ── aurora help ──────────────────────────────────────────────────────


class TestAuroraHelp:
    def test_aurora_help(self, app):
        result = runner.invoke(app, ["aurora", "--help"])
        assert result.exit_code == 0
        out = _strip(result.output)
        assert "create" in out
        assert "delete" in out
        assert "status" in out
        assert "connect" in out
        assert "list" in out

    def test_tapdb_help_shows_aurora(self, app):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "aurora" in _strip(result.output)


# ── aurora create ────────────────────────────────────────────────────


class TestAuroraCreate:
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_create_success(self, mock_mgr_cls, app, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "create", "dev", "--vpc-id", "vpc-123"])
        assert result.exit_code == 0
        out = _strip(result.output)
        assert "created" in out.lower() or "✓" in result.output
        mock_mgr.create_stack.assert_called_once()

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_create_failure(self, mock_mgr_cls, app):
        mock_mgr_cls.return_value.create_stack.side_effect = RuntimeError("boom")
        result = runner.invoke(app, ["aurora", "create", "dev"])
        assert result.exit_code == 1


# ── aurora delete ────────────────────────────────────────────────────


class TestAuroraDelete:
    @patch("boto3.client")
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_delete_force(self, mock_mgr_cls, mock_boto3_client, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "delete", "dev", "--force"])
        assert result.exit_code == 0
        mock_mgr.delete_stack.assert_called_once_with(
            "tapdb-dev", retain_networking=True
        )

    @patch("boto3.client")
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_delete_no_retain(self, mock_mgr_cls, mock_boto3_client, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(
            app, ["aurora", "delete", "dev", "--force", "--no-retain-networking"]
        )
        assert result.exit_code == 0
        mock_mgr.delete_stack.assert_called_once_with(
            "tapdb-dev", retain_networking=False
        )

    @patch("boto3.client")
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_delete_failure(self, mock_mgr_cls, mock_boto3_client, app):
        mock_mgr_cls.return_value.delete_stack.side_effect = RuntimeError("boom")
        result = runner.invoke(app, ["aurora", "delete", "dev", "--force"])
        assert result.exit_code == 1


# ── aurora status ────────────────────────────────────────────────────


class TestAuroraStatus:
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_status_success(self, mock_mgr_cls, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "status", "dev"])
        assert result.exit_code == 0
        out = _strip(result.output)
        assert "CREATE_COMPLETE" in out
        assert "ClusterEndpoint" in out

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_status_json(self, mock_mgr_cls, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "status", "dev", "--json"])
        assert result.exit_code == 0
        data = json.loads(_strip(result.output))
        assert data["status"] == "CREATE_COMPLETE"

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_status_not_found(self, mock_mgr_cls, app):
        mock_mgr_cls.return_value.get_stack_status.side_effect = RuntimeError(
            "not found"
        )
        result = runner.invoke(app, ["aurora", "status", "dev"])
        assert result.exit_code == 1


# ── aurora connect ───────────────────────────────────────────────────


class TestAuroraConnect:
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_connect_info(self, mock_mgr_cls, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "connect", "dev"])
        assert result.exit_code == 0
        out = _strip(result.output)
        assert "tapdb-dev.cluster-xyz" in out
        assert "5432" in out

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_connect_export(self, mock_mgr_cls, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "connect", "dev", "--export"])
        assert result.exit_code == 0
        out = _strip(result.output)
        assert "export PGHOST=" in out
        assert "export PGPORT=" in out

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_connect_no_endpoint(self, mock_mgr_cls, app):
        mock_mgr = MagicMock()
        mock_mgr.get_stack_status.return_value = {
            "stack_name": "tapdb-dev",
            "status": "CREATE_IN_PROGRESS",
            "outputs": {},
        }
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "connect", "dev"])
        assert result.exit_code == 1


# ── aurora list ──────────────────────────────────────────────────────


class TestAuroraList:
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_list_table(self, mock_mgr_cls, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "list"])
        assert result.exit_code == 0
        out = _strip(result.output)
        assert "tapdb-dev" in out

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_list_json(self, mock_mgr_cls, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "list", "--json"])
        assert result.exit_code == 0
        data = json.loads(_strip(result.output))
        assert "tapdb-dev" in data

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_list_empty(self, mock_mgr_cls, app):
        mock_mgr = MagicMock()
        mock_mgr.detect_existing_resources.return_value = {}
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "list"])
        assert result.exit_code == 0
        assert "No tapdb Aurora stacks" in _strip(result.output)


# ── config update ────────────────────────────────────────────────────


class TestConfigUpdate:
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_create_updates_config(self, mock_mgr_cls, app, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(app, ["aurora", "create", "staging"])
        assert result.exit_code == 0

        config_path = tmp_path / ".config" / "tapdb" / "tapdb-config.yaml"
        assert config_path.exists()
        raw = config_path.read_text()
        # Should contain the environment entry
        assert "staging" in raw
        assert "aurora" in raw



# ── aurora create --background ──────────────────────────────────────


class TestAuroraCreateBackground:
    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_background_returns_immediately(self, mock_mgr_cls, app):
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(
            app, ["aurora", "create", "dev", "--background", "--vpc-id", "vpc-123"]
        )
        assert result.exit_code == 0
        out = _strip(result.output)
        assert "initiated" in out.lower()
        assert "tapdb aurora status dev" in out
        mock_mgr.initiate_create_stack.assert_called_once()
        mock_mgr.create_stack.assert_not_called()

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_no_background_calls_create_stack(
        self, mock_mgr_cls, app, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        mock_mgr = _mock_stack_manager()
        mock_mgr_cls.return_value = mock_mgr

        result = runner.invoke(
            app, ["aurora", "create", "dev", "--vpc-id", "vpc-123"]
        )
        assert result.exit_code == 0
        mock_mgr.create_stack.assert_called_once()
        mock_mgr.initiate_create_stack.assert_not_called()

    @patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
    def test_background_failure(self, mock_mgr_cls, app):
        mock_mgr_cls.return_value.initiate_create_stack.side_effect = RuntimeError(
            "No default VPC"
        )
        result = runner.invoke(
            app, ["aurora", "create", "dev", "--background"]
        )
        assert result.exit_code == 1
        assert "No default VPC" in _strip(result.output)
