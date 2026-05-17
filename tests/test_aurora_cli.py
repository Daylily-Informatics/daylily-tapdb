"""Tests for tapdb aurora CLI commands."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from typer.testing import CliRunner

from daylily_tapdb.cli import build_app
from daylily_tapdb.cli.aurora import (
    _DEFAULT_PRIVATE_INGRESS_CIDR,
    _resolve_ingress_cidr,
)

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip(s: str) -> str:
    return _ANSI_RE.sub("", s)


def _write_config(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    domain_registry = path.parent / "domain_code_registry.json"
    prefix_registry = path.parent / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test-localhost"}}}\n',
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
        "  port: '5432'\n"
        "  ui_port: '8911'\n"
        "  domain_code: Z\n"
        "  user: tapdb_admin\n"
        "  password: ''\n"
        "  database: tapdb_dev\n"
        "  schema_name: tapdb_testdb\n"
        "  cluster_identifier: dev\n"
        "safety:\n"
        "  safety_tier: shared\n"
        "  destructive_operations: blocked\n",
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _namespaced(tmp_path: Path, args: list[str]) -> list[str]:
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    return ["--config", str(cfg_path), *args]


def _mock_stack_manager():
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


def test_aurora_help(app=None):
    app = app or build_app()
    result = runner.invoke(app, ["aurora", "--help"])
    assert result.exit_code == 0
    out = _strip(result.output)
    assert "create" in out
    assert "delete" in out
    assert "status" in out
    assert "connect" in out
    assert "list" in out


def test_resolve_ingress_cidr_modes():
    assert (
        _resolve_ingress_cidr("1.2.3.4/32", True, public_ip_resolver=lambda: "5.6.7.8")
        == "1.2.3.4/32"
    )
    assert (
        _resolve_ingress_cidr(None, True, public_ip_resolver=lambda: "24.7.124.62")
        == "24.7.124.62/32"
    )
    assert (
        _resolve_ingress_cidr(None, False, public_ip_resolver=lambda: "24.7.124.62")
        == _DEFAULT_PRIVATE_INGRESS_CIDR
    )


@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_create_success(mock_mgr_cls, tmp_path):
    mock_mgr = _mock_stack_manager()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(
        build_app(), _namespaced(tmp_path, ["aurora", "create", "--vpc-id", "vpc-123"])
    )

    assert result.exit_code == 0, result.output
    assert "created" in _strip(result.output).lower() or "✓" in result.output
    mock_mgr.create_stack.assert_called_once()
    config = mock_mgr.create_stack.call_args.args[0]
    assert config.cluster_identifier == "dev"
    assert config.cidr == _DEFAULT_PRIVATE_INGRESS_CIDR


@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_create_publicly_accessible_resolves_current_ip(
    mock_mgr_cls, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        "daylily_tapdb.cli.aurora._detect_caller_public_ip",
        lambda: "24.7.124.62",
    )
    mock_mgr = _mock_stack_manager()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(
        build_app(),
        _namespaced(tmp_path, ["aurora", "create", "--publicly-accessible"]),
    )

    assert result.exit_code == 0, result.output
    config = mock_mgr.create_stack.call_args.args[0]
    assert config.publicly_accessible is True
    assert config.cidr == "24.7.124.62/32"


@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_create_failure(mock_mgr_cls, tmp_path):
    mock_mgr_cls.return_value.create_stack.side_effect = RuntimeError("boom")

    result = runner.invoke(build_app(), _namespaced(tmp_path, ["aurora", "create"]))

    assert result.exit_code == 1
    assert "boom" in _strip(result.output)


@patch("boto3.client")
@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_delete_force(mock_mgr_cls, _mock_boto3_client, tmp_path):
    mock_mgr = _mock_stack_manager()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(
        build_app(), _namespaced(tmp_path, ["aurora", "delete", "--force"])
    )

    assert result.exit_code == 0, result.output
    mock_mgr.delete_stack.assert_called_once_with("tapdb-dev", retain_networking=True)


@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_status_json(mock_mgr_cls, tmp_path):
    mock_mgr = _mock_stack_manager()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(
        build_app(), _namespaced(tmp_path, ["aurora", "status", "--json"])
    )

    assert result.exit_code == 0, result.output
    data = json.loads(_strip(result.output))
    assert data["status"] == "CREATE_COMPLETE"


@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_connect_export(mock_mgr_cls, tmp_path):
    mock_mgr = _mock_stack_manager()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(
        build_app(), _namespaced(tmp_path, ["aurora", "connect", "--export"])
    )

    assert result.exit_code == 0, result.output
    out = _strip(result.output)
    assert "export PGHOST=" in out
    assert "export PGDATABASE=tapdb_dev" in out


@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_list_json(mock_mgr_cls):
    mock_mgr = _mock_stack_manager()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(build_app(), ["aurora", "list", "--json"])

    assert result.exit_code == 0
    data = json.loads(_strip(result.output))
    assert "tapdb-dev" in data


@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_create_updates_explicit_target_config(mock_mgr_cls, tmp_path):
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    mock_mgr = _mock_stack_manager()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(build_app(), ["--config", str(cfg_path), "aurora", "create"])

    assert result.exit_code == 0, result.output
    payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    assert payload["meta"]["config_version"] == 4
    assert "environments" not in payload
    assert payload["target"]["engine_type"] == "aurora"
    assert payload["target"]["host"].startswith("tapdb-dev.cluster-xyz")
    assert payload["target"]["cluster_identifier"] == "dev"
    assert payload["target"]["secret_arn"].endswith(":secret:tapdb-dev")


@patch("daylily_tapdb.aurora.stack_manager.AuroraStackManager")
def test_background_returns_immediately(mock_mgr_cls, tmp_path):
    mock_mgr = _mock_stack_manager()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(
        build_app(),
        _namespaced(
            tmp_path, ["aurora", "create", "--background", "--vpc-id", "vpc-123"]
        ),
    )

    assert result.exit_code == 0, result.output
    out = _strip(result.output)
    assert "initiated" in out.lower()
    assert "tapdb aurora status" in out
    mock_mgr.initiate_create_stack.assert_called_once()
    mock_mgr.create_stack.assert_not_called()
