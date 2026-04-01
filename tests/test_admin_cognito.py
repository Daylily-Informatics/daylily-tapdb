"""Tests for TAPDB Admin Cognito runtime resolution."""

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


@pytest.fixture(autouse=True)
def _clear_cli_context_fixture() -> None:
    clear_cli_context()
    yield
    clear_cli_context()


def test_resolve_tapdb_pool_config_from_tapdb_yaml(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    cfg_path = _config_path(tmp_path)

    _write(
        cfg_path,
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: local\n"
        "  database_name: tapdb\n"
        "  euid_client_code: C\n"
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    ui_port: 8911\n"
        "    user: test\n"
        "    database: tapdb_dev\n"
        "    audit_log_euid_prefix: CGX\n"
        "    cognito_user_pool_id: us-east-1_TESTPOOL\n"
        "    cognito_app_client_id: client123\n"
        "    cognito_client_name: tapdb\n"
        "    cognito_region: us-east-1\n"
        "    cognito_domain: tapdb-dev-users.auth.us-east-1.amazoncognito.com\n"
        "    cognito_callback_url: https://localhost:8911/auth/callback\n"
        "    cognito_logout_url: https://localhost:8911/login\n"
        "    aws_profile: test-profile\n",
    )

    set_cli_context(config_path=cfg_path, env_name="dev")
    cfg = resolve_tapdb_pool_config("dev")
    assert cfg.pool_id == "us-east-1_TESTPOOL"
    assert cfg.app_client_id == "client123"
    assert cfg.region == "us-east-1"
    assert cfg.aws_profile == "test-profile"
    assert cfg.source_file == cfg_path


def test_resolve_tapdb_pool_config_requires_pool_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    cfg_path = _config_path(tmp_path)

    _write(
        cfg_path,
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: local\n"
        "  database_name: tapdb\n"
        "  euid_client_code: C\n"
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    ui_port: 8911\n"
        "    user: test\n"
        "    database: tapdb_dev\n"
        "    audit_log_euid_prefix: CGX\n",
    )

    set_cli_context(config_path=cfg_path, env_name="dev")
    with pytest.raises(RuntimeError, match="cognito_user_pool_id"):
        resolve_tapdb_pool_config("dev")


def test_resolve_tapdb_pool_config_requires_client_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    cfg_path = _config_path(tmp_path)

    _write(
        cfg_path,
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: local\n"
        "  database_name: tapdb\n"
        "  euid_client_code: C\n"
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    ui_port: 8911\n"
        "    user: test\n"
        "    database: tapdb_dev\n"
        "    audit_log_euid_prefix: CGX\n"
        "    cognito_user_pool_id: us-east-1_TESTPOOL\n"
        "    cognito_client_name: tapdb\n"
        "    cognito_region: us-east-1\n",
    )

    set_cli_context(config_path=cfg_path, env_name="dev")
    with pytest.raises(RuntimeError, match="cognito_app_client_id"):
        resolve_tapdb_pool_config("dev")


def test_resolve_tapdb_pool_config_requires_tapdb_client_name(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    cfg_path = _config_path(tmp_path)

    _write(
        cfg_path,
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: local\n"
        "  database_name: tapdb\n"
        "  euid_client_code: C\n"
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    ui_port: 8911\n"
        "    user: test\n"
        "    database: tapdb_dev\n"
        "    audit_log_euid_prefix: CGX\n"
        "    cognito_user_pool_id: us-east-1_TESTPOOL\n"
        "    cognito_app_client_id: client123\n"
        "    cognito_client_name: wrong-client\n"
        "    cognito_region: us-east-1\n",
    )

    set_cli_context(config_path=cfg_path, env_name="dev")
    with pytest.raises(RuntimeError, match="cognito_client_name"):
        resolve_tapdb_pool_config("dev")
