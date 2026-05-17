"""Tests for TAPDB Admin Cognito runtime resolution."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from admin.cognito import resolve_tapdb_pool_config
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    os.chmod(path, 0o600)


def _config_path(tmp_path: Path) -> Path:
    return tmp_path / ".config" / "tapdb" / "local" / "tapdb" / "tapdb-config.yaml"


def _write_test_registries(tmp_path: Path) -> tuple[Path, Path]:
    domain_registry = tmp_path / "domain_code_registry.json"
    prefix_registry = tmp_path / "prefix_ownership_registry.json"
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
    return domain_registry, prefix_registry


def _write_config(tmp_path: Path, target_extra: str = "") -> Path:
    cfg_path = _config_path(tmp_path)
    domain_registry, prefix_registry = _write_test_registries(tmp_path)
    _write(
        cfg_path,
        "meta:\n"
        "  config_version: 4\n"
        "  client_id: local\n"
        "  database_name: tapdb\n"
        "  owner_repo_name: daylily-tapdb\n"
        f"  domain_registry_path: {domain_registry}\n"
        f"  prefix_ownership_registry_path: {prefix_registry}\n"
        "target:\n"
        "  engine_type: local\n"
        "  host: localhost\n"
        "  port: 5432\n"
        "  ui_port: 8911\n"
        "  domain_code: Z\n"
        "  user: test\n"
        "  database: tapdb_shared\n"
        "  schema_name: tapdb_tapdb\n"
        f"{target_extra}"
        "safety:\n"
        "  safety_tier: shared\n"
        "  destructive_operations: confirm_required\n",
    )
    return cfg_path


@pytest.fixture(autouse=True)
def _clear_cli_context_fixture() -> None:
    clear_cli_context()
    yield
    clear_cli_context()


def test_resolve_tapdb_pool_config_from_tapdb_yaml(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path,
        "  cognito_user_pool_id: us-east-1_TESTPOOL\n"
        "  cognito_app_client_id: client123\n"
        "  cognito_client_name: tapdb\n"
        "  cognito_region: us-east-1\n"
        "  cognito_domain: tapdb-users.auth.us-east-1.amazoncognito.com\n"
        "  cognito_callback_url: https://localhost:8911/auth/callback\n"
        "  cognito_logout_url: https://localhost:8911/login\n"
        "  aws_profile: test-profile\n",
    )

    set_cli_context(config_path=cfg_path)
    cfg = resolve_tapdb_pool_config()
    assert cfg.pool_id == "us-east-1_TESTPOOL"
    assert cfg.app_client_id == "client123"
    assert cfg.region == "us-east-1"
    assert cfg.aws_profile == "test-profile"
    assert cfg.source_file == cfg_path


def test_resolve_tapdb_pool_config_requires_pool_id(tmp_path: Path):
    cfg_path = _write_config(tmp_path)

    set_cli_context(config_path=cfg_path)
    with pytest.raises(RuntimeError, match="cognito_user_pool_id"):
        resolve_tapdb_pool_config()


def test_resolve_tapdb_pool_config_requires_client_id(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path,
        "  cognito_user_pool_id: us-east-1_TESTPOOL\n"
        "  cognito_client_name: tapdb\n"
        "  cognito_region: us-east-1\n",
    )

    set_cli_context(config_path=cfg_path)
    with pytest.raises(RuntimeError, match="cognito_app_client_id"):
        resolve_tapdb_pool_config()


def test_resolve_tapdb_pool_config_requires_tapdb_client_name(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path,
        "  cognito_user_pool_id: us-east-1_TESTPOOL\n"
        "  cognito_app_client_id: client123\n"
        "  cognito_client_name: wrong-client\n"
        "  cognito_region: us-east-1\n",
    )

    set_cli_context(config_path=cfg_path)
    with pytest.raises(RuntimeError, match="cognito_client_name"):
        resolve_tapdb_pool_config()
