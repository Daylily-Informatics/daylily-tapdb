"""Tests for TAPDB Admin Cognito runtime resolution."""

from pathlib import Path

import pytest

from admin.cognito import resolve_tapdb_pool_config


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_resolve_tapdb_pool_config_from_daycog_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("TAPDB_CONFIG_PATH", raising=False)

    _write(
        tmp_path / ".config" / "tapdb" / "tapdb-config.yaml",
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    user: test\n"
        "    database: tapdb_dev\n"
        "    cognito_user_pool_id: us-east-1_TESTPOOL\n",
    )
    _write(
        tmp_path / ".config" / "daycog" / "tapdb-dev-users.us-east-1.env",
        "AWS_PROFILE=test-profile\n"
        "AWS_REGION=us-east-1\n"
        "COGNITO_REGION=us-east-1\n"
        "COGNITO_USER_POOL_ID=us-east-1_TESTPOOL\n"
        "COGNITO_APP_CLIENT_ID=client123\n",
    )

    cfg = resolve_tapdb_pool_config("dev")
    assert cfg.pool_id == "us-east-1_TESTPOOL"
    assert cfg.app_client_id == "client123"
    assert cfg.region == "us-east-1"
    assert cfg.aws_profile == "test-profile"
    assert cfg.source_file.name == "tapdb-dev-users.us-east-1.env"


def test_resolve_tapdb_pool_config_requires_pool_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("TAPDB_CONFIG_PATH", raising=False)

    _write(
        tmp_path / ".config" / "tapdb" / "tapdb-config.yaml",
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    user: test\n"
        "    database: tapdb_dev\n",
    )

    with pytest.raises(RuntimeError, match="cognito_user_pool_id"):
        resolve_tapdb_pool_config("dev")


def test_resolve_tapdb_pool_config_requires_matching_daycog_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("TAPDB_CONFIG_PATH", raising=False)

    _write(
        tmp_path / ".config" / "tapdb" / "tapdb-config.yaml",
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    user: test\n"
        "    database: tapdb_dev\n"
        "    cognito_user_pool_id: us-east-1_TESTPOOL\n",
    )
    _write(
        tmp_path / ".config" / "daycog" / "other-pool.us-east-1.env",
        "AWS_PROFILE=test-profile\n"
        "AWS_REGION=us-east-1\n"
        "COGNITO_USER_POOL_ID=us-east-1_OTHER\n"
        "COGNITO_APP_CLIENT_ID=client456\n",
    )

    with pytest.raises(RuntimeError, match="No daycog config found"):
        resolve_tapdb_pool_config("dev")


def test_resolve_tapdb_pool_config_prefers_pool_scoped_env_over_app_and_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("TAPDB_CONFIG_PATH", raising=False)

    _write(
        tmp_path / ".config" / "tapdb" / "tapdb-config.yaml",
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    user: test\n"
        "    database: tapdb_dev\n"
        "    cognito_user_pool_id: us-east-1_TESTPOOL\n",
    )
    _write(
        tmp_path / ".config" / "daycog" / "default.env",
        "AWS_PROFILE=default-profile\n"
        "AWS_REGION=us-east-1\n"
        "COGNITO_REGION=us-east-1\n"
        "COGNITO_USER_POOL_ID=us-east-1_TESTPOOL\n"
        "COGNITO_APP_CLIENT_ID=client-default\n",
    )
    _write(
        tmp_path / ".config" / "daycog" / "tapdb-dev-users.us-east-1.web-app.env",
        "AWS_PROFILE=app-profile\n"
        "AWS_REGION=us-east-1\n"
        "COGNITO_REGION=us-east-1\n"
        "COGNITO_CLIENT_NAME=web-app\n"
        "COGNITO_USER_POOL_ID=us-east-1_TESTPOOL\n"
        "COGNITO_APP_CLIENT_ID=client-app\n",
    )
    _write(
        tmp_path / ".config" / "daycog" / "tapdb-dev-users.us-east-1.env",
        "AWS_PROFILE=pool-profile\n"
        "AWS_REGION=us-east-1\n"
        "COGNITO_REGION=us-east-1\n"
        "COGNITO_CLIENT_NAME=selected-app\n"
        "COGNITO_USER_POOL_ID=us-east-1_TESTPOOL\n"
        "COGNITO_APP_CLIENT_ID=client-pool\n",
    )

    cfg = resolve_tapdb_pool_config("dev")
    assert cfg.app_client_id == "client-pool"
    assert cfg.aws_profile == "pool-profile"
    assert cfg.source_file.name == "tapdb-dev-users.us-east-1.env"
