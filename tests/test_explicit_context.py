from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from admin.db_metrics import current_metrics_path
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import (
    clear_cli_context,
    resolve_context,
    set_cli_context,
)
from daylily_tapdb.cli.db_config import get_db_config_for_env

runner = CliRunner()


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
            '{"version":"0.4.0","ownership":{"Z":{"TPX":{"issuer_app_code":"daylily-tapdb"},'
            '"EDG":{"issuer_app_code":"daylily-tapdb"},'
            '"ADT":{"issuer_app_code":"daylily-tapdb"},'
            '"SYS":{"issuer_app_code":"daylily-tapdb"},'
            '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
        ),
        encoding="utf-8",
    )
    path.write_text(
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: alpha\n"
        "  database_name: beta\n"
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
        "  db_pool_size: 5\n"
        "  db_max_overflow: 10\n"
        "  db_pool_timeout: 30\n"
        "  db_pool_recycle: 1800\n"
        "environments:\n"
        "  dev:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: '5533'\n"
        "    ui_port: '8911'\n"
        "    domain_code: Z\n"
        "    user: tapdb\n"
        "    password: filepw\n"
        "    database: tapdb_dev\n"
        "    schema_name: tapdb_beta_dev\n"
        "  test:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: '5534'\n"
        "    ui_port: '8912'\n"
        "    domain_code: Z\n"
        "    user: tapdb\n"
        "    password: ''\n"
        "    database: tapdb_test\n"
        "    schema_name: tapdb_beta_test\n",
        encoding="utf-8",
    )
    os.chmod(path, 0o600)
    return path


@pytest.fixture(autouse=True)
def _clear_cli_context_fixture() -> None:
    clear_cli_context()
    yield
    clear_cli_context()


def test_resolve_context_uses_explicit_config_metadata_and_runtime_dir(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "alpha" / "beta" / "tapdb-config.yaml"
    )

    set_cli_context(config_path=cfg_path, env_name="dev")
    ctx = resolve_context(require_keys=True, env_name="dev")

    assert ctx.client_id == "alpha"
    assert ctx.database_name == "beta"
    assert ctx.config_path() == cfg_path
    assert ctx.runtime_dir("dev") == cfg_path.parent / "dev"


def test_get_db_config_for_env_ignores_ambient_env_fallbacks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "alpha" / "beta" / "tapdb-config.yaml"
    )
    monkeypatch.setenv("TAPDB_CLIENT_ID", "from-env")
    monkeypatch.setenv("TAPDB_DATABASE_NAME", "wrong-db")
    monkeypatch.setenv("TAPDB_DEV_HOST", "db.example.com")
    monkeypatch.setenv("PGPORT", "9999")
    monkeypatch.setenv("PGPASSWORD", "envpw")

    set_cli_context(config_path=cfg_path, env_name="dev")
    cfg = get_db_config_for_env("dev")

    assert cfg["client_id"] == "alpha"
    assert cfg["database_name"] == "beta"
    assert cfg["host"] == "localhost"
    assert cfg["port"] == "5533"
    assert cfg["password"] == "filepw"
    assert cfg["schema_name"] == "tapdb_beta_dev"
    assert cfg["config_path"] == str(cfg_path)


def test_runtime_command_requires_explicit_config_and_env(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "alpha" / "beta" / "tapdb-config.yaml"
    )

    result = runner.invoke(app, ["info"])

    assert result.exit_code != 0
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "TapDB config path is required. Set --config."

    result = runner.invoke(app, ["--config", str(cfg_path), "--env", "dev", "info"])

    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "beta" in result.output


def test_metrics_runtime_dir_follows_config_parent(tmp_path: Path):
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "alpha" / "beta" / "tapdb-config.yaml"
    )

    set_cli_context(config_path=cfg_path, env_name="test")
    metrics_path = current_metrics_path("test")

    assert metrics_path.parent == cfg_path.parent / "test" / "metrics"


def test_ui_start_launches_admin_server_with_explicit_context(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "alpha" / "beta" / "tapdb-config.yaml"
    )
    cert_path = tmp_path / "certs" / "localhost.crt"
    key_path = tmp_path / "certs" / "localhost.key"
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    cert_path.write_text("cert", encoding="utf-8")
    key_path.write_text("key", encoding="utf-8")

    captured: dict[str, list[str]] = {}

    def _fake_popen(cmd, **kwargs):
        _ = kwargs
        captured["cmd"] = list(cmd)
        return SimpleNamespace(pid=12345, poll=lambda: None)

    monkeypatch.setattr(cli_mod, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _path: None)
    monkeypatch.setattr(cli_mod, "_port_is_available", lambda _host, _port: True)
    monkeypatch.setattr(
        cli_mod,
        "_ensure_tls_certificates",
        lambda _host, env_name=None, cert_file=None, key_file=None: (
            cert_path,
            key_path,
        ),
    )
    monkeypatch.setattr(cli_mod.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(cli_mod.time, "sleep", lambda _secs: None)

    result = runner.invoke(
        app,
        ["--config", str(cfg_path), "--env", "dev", "ui", "start", "--background"],
    )

    assert result.exit_code == 0
    assert captured["cmd"][:3] == [
        cli_mod.sys.executable,
        "-m",
        "daylily_tapdb.cli.admin_server",
    ]
    assert "--config" in captured["cmd"]
    assert str(cfg_path) in captured["cmd"]
    assert "--env" in captured["cmd"]
    assert "dev" in captured["cmd"]
