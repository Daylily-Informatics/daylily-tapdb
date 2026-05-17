from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from click.exceptions import Exit
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db import _require_destructive_confirmation
from daylily_tapdb.cli.db_config import get_db_config

runner = CliRunner()


def _write_registries(root: Path) -> tuple[Path, Path]:
    domain_registry = root / "domain_code_registry.json"
    prefix_registry = root / "prefix_ownership_registry.json"
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


def _init_explicit_config(tmp_path: Path) -> Path:
    cfg_path = tmp_path / "tapdb-config.yaml"
    domain_registry, prefix_registry = _write_registries(tmp_path)
    result = runner.invoke(
        app,
        [
            "--config",
            str(cfg_path),
            "config",
            "init",
            "--client-id",
            "alpha",
            "--database-name",
            "beta",
            "--owner-repo-name",
            "daylily-tapdb",
            "--domain-code",
            "Z",
            "--domain-registry-path",
            str(domain_registry),
            "--prefix-ownership-registry-path",
            str(prefix_registry),
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
            "tapdb_shared",
            "--schema-name",
            "tapdb_alpha_beta",
            "--safety-tier",
            "shared",
            "--destructive-operations",
            "confirm_required",
        ],
    )
    assert result.exit_code == 0, result.output
    return cfg_path


@pytest.fixture(autouse=True)
def _clear_context() -> None:
    clear_cli_context()
    yield
    clear_cli_context()


def test_root_rejects_legacy_env_selector(tmp_path: Path) -> None:
    cfg_path = _init_explicit_config(tmp_path)

    result = runner.invoke(app, ["--config", str(cfg_path), "--env", "dev", "info"])

    assert result.exit_code == 2
    assert "No such option" in result.output


def test_explicit_config_init_writes_single_resolved_target(tmp_path: Path) -> None:
    cfg_path = _init_explicit_config(tmp_path)
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))

    assert raw["meta"]["config_version"] == 4
    assert raw["meta"]["client_id"] == "alpha"
    assert raw["meta"]["database_name"] == "beta"
    assert "environments" not in raw
    assert raw["target"]["database"] == "tapdb_shared"
    assert raw["target"]["schema_name"] == "tapdb_alpha_beta"
    assert raw["safety"] == {
        "safety_tier": "shared",
        "destructive_operations": "confirm_required",
    }

    set_cli_context(config_path=cfg_path)
    cfg = get_db_config()
    assert cfg["client_id"] == "alpha"
    assert cfg["database_name"] == "beta"
    assert cfg["database"] == "tapdb_shared"
    assert cfg["schema_name"] == "tapdb_alpha_beta"


def test_legacy_v3_environment_config_is_rejected(tmp_path: Path) -> None:
    cfg_path = tmp_path / "tapdb-config.yaml"
    domain_registry, prefix_registry = _write_registries(tmp_path)
    cfg_path.write_text(
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: alpha\n"
        "  database_name: beta\n"
        "  owner_repo_name: daylily-tapdb\n"
        f"  domain_registry_path: {domain_registry}\n"
        f"  prefix_ownership_registry_path: {prefix_registry}\n"
        "environments:\n"
        "  dev:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: '5533'\n"
        "    ui_port: '8911'\n"
        "    domain_code: Z\n"
        "    user: tapdb\n"
        "    database: tapdb_dev\n"
        "    schema_name: tapdb_beta_dev\n",
        encoding="utf-8",
    )
    cfg_path.chmod(0o600)
    set_cli_context(config_path=cfg_path)

    with pytest.raises(RuntimeError, match="Unsupported config_version '3'"):
        get_db_config()


def test_ui_start_launches_admin_server_without_env_argument(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg_path = _init_explicit_config(tmp_path)
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
        lambda _host, cert_file=None, key_file=None: (cert_path, key_path),
    )
    monkeypatch.setattr(cli_mod.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(cli_mod.time, "sleep", lambda _secs: None)

    result = runner.invoke(
        app,
        ["--config", str(cfg_path), "ui", "start", "--background"],
    )

    assert result.exit_code == 0, result.output
    assert captured["cmd"][:3] == [
        cli_mod.sys.executable,
        "-m",
        "daylily_tapdb.cli.admin_server",
    ]
    assert "--config" in captured["cmd"]
    assert str(cfg_path) in captured["cmd"]
    assert "--env" not in captured["cmd"]


def test_destructive_confirmation_uses_resolved_target_label() -> None:
    cfg = {
        "client_id": "atlas",
        "database_name": "orders",
        "schema_name": "tapdb_atlas_dayfly5_dev",
        "database": "tapdb_dayfly5_dev",
        "destructive_operations": "confirm_required",
    }

    with pytest.raises(Exit):
        _require_destructive_confirmation(
            cfg,
            operation="reset schema",
            confirm_target=None,
        )

    _require_destructive_confirmation(
        cfg,
        operation="reset schema",
        confirm_target="atlas/orders/tapdb_atlas_dayfly5_dev@tapdb_dayfly5_dev",
    )
