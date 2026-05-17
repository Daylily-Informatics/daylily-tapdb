"""Root CLI branch coverage for explicit-target TapDB."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context

runner = CliRunner()


def _write_config(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    domain_registry = path.parent / "domain_code_registry.json"
    prefix_registry = path.parent / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test"}}}\n',
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
        "admin:\n"
        "  session:\n"
        "    secret: secret123\n"
        "  auth:\n"
        "    mode: tapdb\n"
        "target:\n"
        "  engine_type: local\n"
        "  host: localhost\n"
        "  port: '5533'\n"
        "  ui_port: '8911'\n"
        "  domain_code: Z\n"
        "  user: tapdb\n"
        "  password: ''\n"
        "  database: tapdb_shared\n"
        "  schema_name: tapdb_testdb\n"
        "safety:\n"
        "  safety_tier: shared\n"
        "  destructive_operations: confirm_required\n",
        encoding="utf-8",
    )
    os.chmod(path, 0o600)
    return path


@pytest.fixture(autouse=True)
def _explicit_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    clear_cli_context()
    set_cli_context(config_path=cfg_path)
    yield cfg_path
    clear_cli_context()


def test_tls_paths_resolve_from_config_runtime(tmp_path: Path):
    cert, key = cli_mod._resolve_tls_paths()

    assert cert == tmp_path / "runtime" / "ui" / "certs" / "localhost.crt"
    assert key == tmp_path / "runtime" / "ui" / "certs" / "localhost.key"


def test_tls_generation_uses_openssl(monkeypatch: pytest.MonkeyPatch):
    calls: list[list[str]] = []

    def _fake_run(cmd, capture_output=True, text=True):
        calls.append(list(cmd))
        cert = Path(cmd[cmd.index("-out") + 1])
        key = Path(cmd[cmd.index("-keyout") + 1])
        cert.parent.mkdir(parents=True, exist_ok=True)
        key.parent.mkdir(parents=True, exist_ok=True)
        cert.write_text("cert", encoding="utf-8")
        key.write_text("key", encoding="utf-8")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(cli_mod.shutil, "which", lambda name: "/usr/bin/openssl")
    monkeypatch.setattr(cli_mod.subprocess, "run", _fake_run)

    cert, key = cli_mod._ensure_tls_certificates("localhost")

    assert cert.exists()
    assert key.exists()
    assert calls


def test_config_update_and_info_share_single_target(tmp_path: Path):
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")

    update = runner.invoke(
        app,
        [
            "--config",
            str(cfg_path),
            "config",
            "update",
            "--schema-name",
            "tapdb_updated",
            "--destructive-operations",
            "blocked",
        ],
    )
    assert update.exit_code == 0, update.output

    root = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    assert root["target"]["schema_name"] == "tapdb_updated"
    assert root["safety"]["destructive_operations"] == "blocked"
    assert "environments" not in root

    info = runner.invoke(app, ["--config", str(cfg_path), "info", "--json"])
    assert info.exit_code == 0, info.output
    assert '"target": "explicit"' in info.output
    assert '"schema_name": "tapdb_updated"' in info.output


def test_ui_mkcert_generates_under_explicit_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands: list[list[str]] = []

    def _fake_run(cmd, capture_output=True, text=True):
        commands.append(list(cmd))
        if "-cert-file" in cmd:
            cert = Path(cmd[cmd.index("-cert-file") + 1])
            key = Path(cmd[cmd.index("-key-file") + 1])
            cert.parent.mkdir(parents=True, exist_ok=True)
            key.parent.mkdir(parents=True, exist_ok=True)
            cert.write_text("cert", encoding="utf-8")
            key.write_text("key", encoding="utf-8")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(cli_mod.shutil, "which", lambda name: "/usr/local/bin/mkcert")
    monkeypatch.setattr(cli_mod.subprocess, "run", _fake_run)

    result = runner.invoke(app, ["ui", "mkcert"])

    assert result.exit_code == 0, result.output
    assert commands[0] == ["/usr/local/bin/mkcert", "-install"]
    assert any("-cert-file" in cmd for cmd in commands)


def test_ui_logs_without_log_file_is_clear() -> None:
    result = runner.invoke(app, ["ui", "logs"])

    assert result.exit_code == 0
    assert "No log file found" in result.output


def test_root_env_option_is_not_registered() -> None:
    result = runner.invoke(app, ["--env", "dev", "version"])

    assert result.exit_code == 2
    assert "No such option" in result.output
