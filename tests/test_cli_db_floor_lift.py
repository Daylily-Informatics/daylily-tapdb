"""DB CLI unit coverage for the explicit-target model."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

import daylily_tapdb.cli.db as db_mod
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context

runner = CliRunner()


def _write_config(path: Path, *, safety: str = "confirm_required") -> Path:
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
        "  password: ''\n"
        "  database: tapdb_shared\n"
        "  schema_name: tapdb_testdb\n"
        "safety:\n"
        "  safety_tier: shared\n"
        f"  destructive_operations: {safety}\n",
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


def test_environment_and_config_are_single_target() -> None:
    assert [item.value for item in db_mod.Environment] == ["target"]

    cfg = db_mod._get_db_config(db_mod.Environment.target)

    assert cfg["client_id"] == "testclient"
    assert cfg["database_name"] == "testdb"
    assert cfg["database"] == "tapdb_shared"
    assert db_mod._get_schema_name(db_mod.Environment.target) == "tapdb_testdb"


def test_required_identity_prefixes_are_governance_backed() -> None:
    prefixes = db_mod._required_identity_prefixes(db_mod.Environment.target)

    assert prefixes["generic_template"] == "TPX"
    assert prefixes["generic_instance_lineage"] == "EDG"
    assert prefixes["audit_log"] == "ADT"


def test_connection_string_uses_target_database_and_schema_policy() -> None:
    assert (
        db_mod._get_connection_string(db_mod.Environment.target)
        == "postgresql://tapdb@localhost:5533/tapdb_shared"
    )
    assert (
        db_mod._get_connection_string(db_mod.Environment.target, database="postgres")
        == "postgresql://tapdb@localhost:5533/postgres"
    )


def test_destructive_confirmation_uses_resolved_target_label() -> None:
    cfg = db_mod._get_db_config(db_mod.Environment.target)
    label = "testclient/testdb/tapdb_testdb@tapdb_shared"

    with pytest.raises(typer.Exit):
        db_mod._require_destructive_confirmation(
            cfg, operation="delete database", confirm_target=None
        )

    db_mod._require_destructive_confirmation(
        cfg, operation="delete database", confirm_target=label
    )

    cfg["destructive_operations"] = "blocked"
    with pytest.raises(RuntimeError, match="blocked"):
        db_mod._require_destructive_confirmation(
            cfg, operation="delete database", confirm_target=label
        )

    cfg["destructive_operations"] = "allowed"
    db_mod._require_destructive_confirmation(
        cfg, operation="delete database", confirm_target=None
    )


def test_db_schema_apply_uses_explicit_target(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(db_mod, "_ensure_dirs", lambda: None)
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda env, db: True)
    monkeypatch.setattr(db_mod, "_find_schema_file", lambda: Path("schema.sql"))
    monkeypatch.setattr(
        db_mod, "_ensure_schema_exists", lambda env: calls.append(("ensure", env))
    )
    monkeypatch.setattr(db_mod, "_schema_exists", lambda env: False)
    monkeypatch.setattr(db_mod, "_run_psql", lambda env, **kwargs: (True, ""))
    monkeypatch.setattr(db_mod, "_log_operation", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        db_mod, "_sync_identity_prefix_config", lambda env: calls.append(("sync", env))
    )
    monkeypatch.setattr(
        db_mod, "_write_migration_baseline", lambda env: calls.append(("baseline", env))
    )

    db_mod.db_schema_apply(reinitialize=False)

    assert calls == [
        ("ensure", db_mod.Environment.target),
        ("sync", db_mod.Environment.target),
        ("baseline", db_mod.Environment.target),
    ]


def test_db_status_uses_explicit_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda env, db: True)
    monkeypatch.setattr(db_mod, "_schema_exists", lambda env: True)
    monkeypatch.setattr(
        db_mod, "_get_table_counts", lambda env: {"generic_template": 1}
    )

    db_mod.db_status()


def test_db_delete_old_env_argument_is_rejected() -> None:
    result = runner.invoke(app, ["db", "delete", "prod", "--confirm-target", "x"])

    assert result.exit_code == 2
    assert "unexpected extra argument" in result.output.lower()


def test_tapdb_connection_for_env_passes_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    class FakeConnection:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr(db_mod, "TAPDBConnection", FakeConnection)

    conn = db_mod._tapdb_connection_for_env(
        db_mod.Environment.target,
        app_username="tester",
    )

    assert isinstance(conn, FakeConnection)
    assert seen["schema_name"] == "tapdb_testdb"
    assert seen["db_name"] == "tapdb_shared"
    assert seen["app_username"] == "tester"


def test_create_default_admin_skips_without_insecure_flag() -> None:
    assert (
        db_mod._create_default_admin(
            db_mod.Environment.target, insecure_dev_defaults=False
        )
        is False
    )
