"""Helper-unit tests for explicit-target CLI internals."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import DBAPIError

import daylily_tapdb.cli.db as db_mod
import daylily_tapdb.cli.pg as pg_mod
import daylily_tapdb.cli.user as user_mod
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context


def _write_config(
    path: Path,
    *,
    engine_type: str = "local",
    owner_repo_name: str = "daylily-tapdb",
    prefix_owner_repo_name: str | None = None,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    prefix_owner = prefix_owner_repo_name or owner_repo_name
    domain_registry = path.parent / "domain_code_registry.json"
    prefix_registry = path.parent / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test"}}}\n',
        encoding="utf-8",
    )
    prefix_registry.write_text(
        json.dumps(
            {
                "version": "0.4.0",
                "ownership": {
                    "Z": {
                        "TPX": {"issuer_app_code": prefix_owner},
                        "EDG": {"issuer_app_code": prefix_owner},
                        "ADT": {"issuer_app_code": prefix_owner},
                        "SYS": {"issuer_app_code": prefix_owner},
                        "MSG": {"issuer_app_code": prefix_owner},
                    }
                },
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    path.write_text(
        "meta:\n"
        "  config_version: 4\n"
        "  client_id: testclient\n"
        "  database_name: testdb\n"
        f"  owner_repo_name: {owner_repo_name}\n"
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
        "  operator:\n"
        "    user: tapdb_operator\n"
        "    password: test-only-unused\n"
        "    iam_auth: false\n"
        "    secret_arn: ''\n"
        "  database: tapdb_shared\n"
        "  schema_name: tapdb_testdb\n"
        "  region: us-west-2\n"
        "  cluster_identifier: tapdb-shared\n"
        "  iam_auth: 'false'\n"
        "  ssl: 'true'\n"
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


def _record_sync(monkeypatch, sql_calls, *, failure=None):
    class Connection:
        def execute(self, statement):
            sql_calls.append(str(statement))
            if failure is not None:
                raise failure

    @contextmanager
    def connection(cfg, **kwargs):
        assert kwargs == {"isolation_level": "REPEATABLE READ"}
        yield Connection()

    monkeypatch.setattr(db_mod, "operator_connection", connection)
    monkeypatch.setattr(
        "daylily_tapdb.runtime_principal.grant_proven_runtime_sequences", lambda *a: []
    )


def test_identity_prefix_sync_writes_expected_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sql_calls: list[str] = []
    _record_sync(monkeypatch, sql_calls)

    db_mod._sync_identity_prefix_config(db_mod.Environment.target)

    joined = "\n".join(sql_calls)
    assert "tapdb_identity_prefix_config" in joined
    assert "TPX" in joined
    assert "EDG" in joined
    assert "ADT" in joined
    assert "daylily-tapdb" in joined
    assert "DO NOTHING" in joined
    assert "DO UPDATE" not in joined
    assert "Existing TapDB identity prefix configuration conflicts" in joined


def test_identity_prefix_sync_uses_configured_owner_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = _write_config(
        tmp_path / "zebra-tapdb-config.yaml",
        owner_repo_name="zebra-day",
        prefix_owner_repo_name="daylily-tapdb",
    )
    set_cli_context(config_path=cfg_path)
    sql_calls: list[str] = []
    _record_sync(monkeypatch, sql_calls)

    db_mod._sync_identity_prefix_config(db_mod.Environment.target)

    joined = "\n".join(sql_calls)
    assert "zebra-day" in joined
    assert "daylily-tapdb" not in joined


def test_identity_prefix_sync_requires_owner_registry_claim(
    tmp_path: Path,
) -> None:
    cfg_path = _write_config(
        tmp_path / "bad-owner-tapdb-config.yaml",
        owner_repo_name="zebra-day",
        prefix_owner_repo_name="zebra-day",
    )
    set_cli_context(config_path=cfg_path)

    with pytest.raises(ValueError, match="owned by 'zebra-day', not 'daylily-tapdb'"):
        db_mod._sync_identity_prefix_config(db_mod.Environment.target)


@pytest.mark.parametrize("known_conflict", [False, True])
def test_identity_prefix_sync_normalizes_driver_failure(
    monkeypatch: pytest.MonkeyPatch,
    known_conflict,
) -> None:
    conflict = "Existing TapDB identity prefix configuration conflicts with the required registry"
    original = Exception("private driver details")
    original.diag = SimpleNamespace(
        message_primary=conflict if known_conflict else "private row data"
    )
    error = DBAPIError("statement", {}, original)
    _record_sync(monkeypatch, [], failure=error)

    with pytest.raises(RuntimeError) as raised:
        db_mod._sync_identity_prefix_config(db_mod.Environment.target)
    assert "private" not in str(raised.value)
    assert ("conflicts" in str(raised.value)) is known_conflict
    assert raised.value.__cause__ is error


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
    assert seen["echo_sql"] is False


@pytest.mark.parametrize("connection_role", ["runtime", "operator"])
def test_connection_keeps_explicit_iam_transport_and_signing_identity(
    monkeypatch, connection_role
):
    cfg = db_mod._get_db_config(db_mod.Environment.target)
    cfg.update(
        engine_type="aurora",
        host="database.example.invalid",
        hostaddr="127.0.0.1",
        port="55434",
        server_port="5432",
        aws_profile="qualification-profile",
        sslrootcert="/explicit/qualification-ca.pem",
    )
    monkeypatch.setattr(db_mod, "_get_db_config", lambda _: cfg)
    seen = {}
    monkeypatch.setattr(db_mod, "TAPDBConnection", lambda **kw: seen.update(kw))
    db_mod._tapdb_connection_for_env(
        db_mod.Environment.target,
        app_username="test:iam-forwarding",
        connection_role=connection_role,
    )
    assert seen["db_hostname"] == "database.example.invalid:55434"
    assert seen["db_hostaddr"] == "127.0.0.1"
    assert seen["server_port"] == 5432
    assert seen["aws_profile"] == "qualification-profile"
    assert seen["sslrootcert"] == "/explicit/qualification-ca.pem"


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
