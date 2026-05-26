"""Cognito CLI coverage for the explicit-target TapDB contract."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from typer.testing import CliRunner

import daylily_tapdb.cli.cognito as cognito_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db import Environment

runner = CliRunner()


class _FakeConn:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def session_scope(self, commit: bool = False):
        _ = commit

        class _Scope:
            def __enter__(self):
                return object()

            def __exit__(self, exc_type, exc, tb):
                return False

        return _Scope()


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
        "target:\n"
        "  engine_type: local\n"
        "  host: localhost\n"
        "  port: '5533'\n"
        "  ui_port: '8911'\n"
        "  domain_code: Z\n"
        "  user: tapdb\n"
        "  password: filepw\n"
        "  database: tapdb_shared\n"
        "  schema_name: tapdb_testdb\n"
        "  cognito_user_pool_id: ''\n"
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


def test_detect_running_ui_port_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pid_file = tmp_path / "ui.pid"
    monkeypatch.setattr(cognito_mod, "_ui_pid_file", lambda: pid_file)

    assert cognito_mod._detect_running_ui_port() == (None, "ui not running")

    pid_file.write_text("1234\n", encoding="utf-8")
    monkeypatch.setattr(
        cognito_mod.os,
        "kill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ProcessLookupError()),
    )
    assert cognito_mod._detect_running_ui_port() == (None, "ui pid missing/stale")

    monkeypatch.setattr(cognito_mod.os, "kill", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        cognito_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout="uvicorn --port 9443", stderr=""
        ),
    )
    assert cognito_mod._detect_running_ui_port() == (9443, "running ui process")


def test_resolve_expected_ui_port_uses_target_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert cognito_mod._resolve_expected_ui_port() == (8911, "config target.ui_port")

    monkeypatch.setattr(cognito_mod, "get_db_config", lambda: {"ui_port": ""})
    monkeypatch.setattr(
        cognito_mod, "_detect_running_ui_port", lambda: (9555, "ui process")
    )
    assert cognito_mod._resolve_expected_ui_port() == (9555, "ui process")


def test_validate_bound_cognito_uris_uses_explicit_target_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cognito_mod,
        "_resolve_expected_ui_port",
        lambda: (8911, "config target.ui_port"),
    )

    expected_port, source, errors, notices = cognito_mod._validate_bound_cognito_uris(
        {
            "COGNITO_CALLBACK_URL": "https://localhost:8911/auth/callback",
            "COGNITO_LOGOUT_URL": "https://localhost:9443/",
            "COGNITO_REDIRECT_URI": "http://localhost:8911/auth/callback",
        }
    )

    assert expected_port == 8911
    assert source == "config target.ui_port"
    assert any("port 9443" in msg for msg in errors)
    assert any("must use https" in msg for msg in errors)
    assert notices == ["COGNITO_CALLBACK_URL: https://localhost:8911/auth/callback"]


def test_default_pool_name_uses_target_database() -> None:
    assert cognito_mod._default_pool_name() == "tapdb-tapdb-shared-users"


def test_write_pool_id_to_explicit_target_config(tmp_path: Path) -> None:
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")
    set_cli_context(config_path=cfg_path)

    written = cognito_mod._write_pool_id_to_tapdb_config("usw2_pool")

    assert written == cfg_path
    root = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    assert root["meta"]["config_version"] == 4
    assert root["target"]["cognito_user_pool_id"] == "usw2_pool"
    assert "environments" not in root


def test_build_daycog_setup_args_uses_required_callback_shape() -> None:
    args = cognito_mod._build_daycog_setup_args(
        command="setup",
        selected_pool_name="tapdb-users",
        region="us-west-2",
        domain_prefix="tapdb-login",
        attach_domain=True,
        port=8911,
        callback_path="auth/callback",
        oauth_flows="code",
        scopes="openid,email,profile",
        idps="COGNITO,Google",
        password_min_length=12,
        mfa="optional",
        profile="lsmc",
        client_name="tapdb",
        callback_url=None,
        logout_url=None,
        autoprovision=True,
        generate_secret=False,
        require_uppercase=True,
        require_lowercase=True,
        require_numbers=True,
        require_symbols=False,
        tags="owner=tapdb",
    )

    assert args[:2] == ["setup", "--name"]
    assert "--callback-url" in args
    assert "https://localhost:8911/auth/callback" in args
    assert "--client-name" in args
    assert "tapdb" in args
    assert "--attach-domain" in args
    assert "--no-require-symbols" in args


def test_pool_command_context_can_run_before_binding() -> None:
    selected_pool, proc_env, region, profile = (
        cognito_mod._resolve_pool_command_context(
            Environment.target,
            region="us-west-2",
            profile="lsmc",
        )
    )

    assert selected_pool == "tapdb-tapdb-shared-users"
    assert proc_env is None
    assert region == "us-west-2"
    assert profile == "lsmc"


def test_ensure_actor_user_row_passes_schema_to_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    def _fake_create_or_get(session, **kwargs):
        _ = session
        created.update(kwargs)
        return SimpleNamespace(is_active=True), True

    fake_conn = _FakeConn
    monkeypatch.setattr(cognito_mod, "TAPDBConnection", fake_conn)
    monkeypatch.setattr(cognito_mod, "create_or_get", _fake_create_or_get)

    cognito_mod._ensure_actor_user_row(
        Environment.target,
        email="Alice@Example.com",
        role="admin",
        display_name="Alice",
    )

    assert created["email"] == "alice@example.com"
    assert created["role"] == "admin"

    with pytest.raises(RuntimeError, match="invalid role"):
        cognito_mod._ensure_actor_user_row(
            Environment.target, email="a@b.com", role="x"
        )


def test_cognito_bind_cli_has_no_env_positional(tmp_path: Path) -> None:
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")

    ok = runner.invoke(
        app, ["--config", str(cfg_path), "cognito", "bind", "--pool-id", "pool-1"]
    )
    assert ok.exit_code == 0, ok.output

    legacy = runner.invoke(app, ["--config", str(cfg_path), "cognito", "status", "dev"])
    assert legacy.exit_code == 2
    assert "unexpected extra argument" in legacy.output.lower()


def test_cognito_management_commands_delegate_to_daycog_without_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], object]] = []

    def _fake_daycog(args, env=None):
        calls.append((args, env))
        return "ok"

    monkeypatch.setattr(cognito_mod, "_run_daycog", _fake_daycog)

    result = runner.invoke(
        app,
        [
            "cognito",
            "list-apps",
            "--pool-name",
            "pool",
            "--region",
            "us-west-2",
            "--profile",
            "lsmc",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls
    args, proc_env = calls[0]
    assert args == [
        "list-apps",
        "--pool-name",
        "pool",
        "--region",
        "us-west-2",
        "--profile",
        "lsmc",
    ]
    assert proc_env is None
