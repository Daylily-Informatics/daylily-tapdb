"""Helper-unit tests for explicit-target CLI internals."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import daylily_tapdb.cli.db as db_mod
import daylily_tapdb.cli.pg as pg_mod
import daylily_tapdb.cli.user as user_mod
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context


def _write_config(path: Path, *, engine_type: str = "local") -> Path:
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
        f"  engine_type: {engine_type}\n"
        "  host: localhost\n"
        "  port: '5533'\n"
        "  ui_port: '8911'\n"
        "  domain_code: Z\n"
        "  user: tapdb\n"
        "  password: ''\n"
        "  database: tapdb_shared\n"
        "  schema_name: tapdb_testdb\n"
        "  region: us-west-2\n"
        "  iam_auth: 'false'\n"
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


def test_identity_prefix_sync_writes_expected_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sql_calls: list[str] = []
    monkeypatch.setattr(
        db_mod,
        "_run_psql",
        lambda env, sql, **kwargs: sql_calls.append(sql) or (True, ""),
    )

    db_mod._sync_identity_prefix_config(db_mod.Environment.target)

    joined = "\n".join(sql_calls)
    assert "tapdb_identity_prefix_config" in joined
    assert "TPX" in joined
    assert "EDG" in joined
    assert "ADT" in joined


def test_identity_prefix_sync_raises_on_psql_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(db_mod, "_run_psql", lambda *args, **kwargs: (False, "boom"))

    with pytest.raises(RuntimeError, match="boom"):
        db_mod._sync_identity_prefix_config(db_mod.Environment.target)


def test_connection_string_adds_ssl_for_aurora(tmp_path: Path) -> None:
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml", engine_type="aurora")
    set_cli_context(config_path=cfg_path)

    assert db_mod._get_connection_string(db_mod.Environment.target).endswith(
        "?sslmode=verify-full"
    )


def test_tapdb_connection_for_env_uses_normalized_engine_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    class FakeConnection:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr(db_mod, "TAPDBConnection", FakeConnection)

    db_mod._tapdb_connection_for_env(db_mod.Environment.target, app_username="tester")

    assert seen["engine_type"] == "local"
    assert seen["iam_auth"] is False
    assert seen["schema_name"] == "tapdb_testdb"


def test_user_open_connection_maps_explicit_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    class FakeConnection:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr(user_mod, "TAPDBConnection", FakeConnection)

    user_mod._open_connection(db_mod.Environment.target, app_username="alice")

    assert seen["db_name"] == "tapdb_shared"
    assert seen["schema_name"] == "tapdb_testdb"
    assert seen["app_username"] == "alice"


def test_pg_active_env_and_lock_paths_are_explicit_target() -> None:
    assert pg_mod._active_env() is db_mod.Environment.target
    assert (
        pg_mod._get_instance_lock_file(db_mod.Environment.target).name
        == "instance.lock"
    )
