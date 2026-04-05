from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import daylily_tapdb.cli.db as db_mod
import daylily_tapdb.cli.pg as pg_mod
import daylily_tapdb.cli.user as user_mod
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.euid import (
    normalize_euid_client_code,
    resolve_client_scoped_core_prefix,
)


def test_normalize_meridian_prefix_accepts_valid_uppercases():
    assert db_mod._normalize_meridian_prefix("ab", "audit_log_euid_prefix") == "AB"


@pytest.mark.parametrize("value", ["", "   ", "GX", "TGX", "I0", "A1", "ABCD"])
def test_normalize_meridian_prefix_rejects_invalid_values(value: str):
    with pytest.raises(ValueError):
        db_mod._normalize_meridian_prefix(value, "audit_log_euid_prefix")


def test_required_identity_prefixes_reads_and_validates_config(monkeypatch):
    monkeypatch.setattr(
        db_mod,
        "_get_db_config",
        lambda _env: {
            "core_euid_prefix": "bcd",
            "audit_log_euid_prefix": "bcd",
        },
    )
    assert db_mod._required_identity_prefixes(db_mod.Environment.dev) == {
        "generic_template": "BCD",
        "generic_instance": "BCD",
        "generic_instance_lineage": "BCD",
        "audit_log": "BCD",
    }


def test_sync_identity_prefix_config_runs_expected_sql(monkeypatch):
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        db_mod,
        "_required_identity_prefixes",
        lambda _env: {
            "generic_template": "BCD",
            "generic_instance": "BCD",
            "generic_instance_lineage": "BCD",
            "audit_log": "BCD",
        },
    )

    def _fake_run_psql(_env, sql=None, file=None, database=None):
        _ = (file, database)
        captured["sql"] = sql or ""
        return True, ""

    monkeypatch.setattr(db_mod, "_run_psql", _fake_run_psql)

    db_mod._sync_identity_prefix_config(db_mod.Environment.dev)

    sql = captured["sql"]
    assert "tapdb_identity_prefix_config" in sql
    assert "('generic_template', '', '', 'BCD')" in sql
    assert "('generic_instance', '', '', 'BCD')" in sql
    assert "('generic_instance_lineage', '', '', 'BCD')" in sql
    assert "('audit_log', '', '', 'BCD')" in sql
    assert "ON CONFLICT (entity, domain_code, issuer_app_code)" in sql
    assert 'CREATE SEQUENCE IF NOT EXISTS "bcd_instance_seq"' in sql


def test_sync_identity_prefix_config_raises_on_psql_failure(monkeypatch):
    monkeypatch.setattr(
        db_mod,
        "_required_identity_prefixes",
        lambda _env: {
            "generic_template": "BCD",
            "generic_instance": "BCD",
            "generic_instance_lineage": "BCD",
            "audit_log": "BCD",
        },
    )
    monkeypatch.setattr(
        db_mod,
        "_run_psql",
        lambda _env, sql=None, file=None, database=None: (False, "boom"),
    )
    with pytest.raises(RuntimeError, match="Failed to sync identity prefix config"):
        db_mod._sync_identity_prefix_config(db_mod.Environment.dev)


def test_euid_client_code_helpers():
    assert normalize_euid_client_code("a") == "A"
    assert resolve_client_scoped_core_prefix("b") == "BGX"
    with pytest.raises(ValueError):
        normalize_euid_client_code("T")


def test_get_connection_string_aurora_and_local(monkeypatch):
    monkeypatch.setattr(
        db_mod,
        "_get_db_config",
        lambda _env: {
            "user": "alice",
            "host": "db.local",
            "port": "5432",
            "database": "tapdb_dev",
            "engine_type": "aurora",
            "password": "secret",
        },
    )
    aurora = db_mod._get_connection_string(db_mod.Environment.dev)
    assert aurora == "postgresql://alice@db.local:5432/tapdb_dev?sslmode=verify-full"
    assert "secret" not in aurora

    monkeypatch.setattr(
        db_mod,
        "_get_db_config",
        lambda _env: {
            "user": "alice",
            "host": "db.local",
            "port": "5432",
            "database": "tapdb_dev",
            "engine_type": "local",
        },
    )
    local = db_mod._get_connection_string(db_mod.Environment.dev, database="override")
    assert local == "postgresql://alice@db.local:5432/override"


def test_parse_single_int_parses_first_numeric_line():
    assert db_mod._parse_single_int("\nabc\n 42 \n77\n") == 42


def test_parse_single_int_raises_when_missing():
    with pytest.raises(ValueError, match="Could not parse int"):
        db_mod._parse_single_int("\nabc\n")


def test_template_code_and_template_key_helpers():
    template = {
        "category": "generic",
        "type": "actor",
        "subtype": "system_user",
        "version": "1.0",
    }
    assert db_mod._template_code(template) == "generic/actor/system_user/1.0/"
    assert db_mod._template_key(template) == (
        "generic",
        "actor",
        "system_user",
        "1.0",
    )


def test_tapdb_connection_for_env_uses_normalized_engine_flags(monkeypatch):
    monkeypatch.setattr(
        db_mod,
        "_get_db_config",
        lambda _env: {
            "host": "localhost",
            "port": "5432",
            "user": "tapdb",
            "password": "",
            "database": "tapdb_dev",
            "engine_type": "AURORA",
            "iam_auth": "yes",
            "region": "us-east-1",
        },
    )
    captured: dict[str, object] = {}

    class _FakeConn:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(db_mod, "TAPDBConnection", _FakeConn)

    conn = db_mod._tapdb_connection_for_env(
        db_mod.Environment.dev, app_username="tester"
    )
    assert isinstance(conn, _FakeConn)
    assert captured == {
        "db_hostname": "localhost:5432",
        "db_user": "tapdb",
        "db_pass": None,
        "db_name": "tapdb_dev",
        "engine_type": "aurora",
        "region": "us-east-1",
        "iam_auth": True,
        "app_username": "tester",
    }


def test_pg_build_pg_ctl_options_quotes_socket_path():
    opts = pg_mod._build_pg_ctl_options(5544, Path("/tmp/socket dir"))
    assert "-p 5544" in opts
    assert "-h localhost" in opts
    assert "-k '/tmp/socket dir'" in opts


def test_pg_port_conflict_details_with_listener_line(monkeypatch):
    proc = SimpleNamespace(returncode=0, stdout="COMMAND\npostgres 111  TCP *:5432")
    monkeypatch.setattr(pg_mod.subprocess, "run", lambda *args, **kwargs: proc)
    details = pg_mod._port_conflict_details(5432)
    assert "port 5432 is in use" in details
    assert "postgres 111" in details


def test_pg_port_conflict_details_fallback_on_exception(monkeypatch):
    def _raise(*args, **kwargs):
        raise RuntimeError("no lsof")

    monkeypatch.setattr(pg_mod.subprocess, "run", _raise)
    assert pg_mod._port_conflict_details(5432) == "port 5432 is already in use"


def test_pg_active_env_defaults_and_invalid(monkeypatch):
    clear_cli_context()
    assert pg_mod._active_env() == db_mod.Environment.dev

    set_cli_context(env_name="TEST")
    assert pg_mod._active_env() == db_mod.Environment.test
    clear_cli_context()


def test_ensure_local_role_repairs_missing_postgres_role(monkeypatch):
    monkeypatch.setattr(
        db_mod,
        "_get_db_config",
        lambda _env: {
            "engine_type": "local",
            "user": "postgres",
            "host": "localhost",
            "port": "5533",
            "password": "",
            "database": "tapdb_dev",
        },
    )
    monkeypatch.setenv("USER", "jmajor")

    calls: list[tuple[str, str | None, str | None]] = []

    def _fake_run_psql(_env, sql=None, file=None, database=None, user=None):
        _ = file
        calls.append((user or "", database, sql))
        if sql == "SELECT 1" and user == "postgres":
            return False, 'psql: error: FATAL:  role "postgres" does not exist'
        if sql == "SELECT 1" and user == "jmajor":
            return True, "1"
        if sql and "CREATE ROLE" in sql and user == "jmajor":
            return True, ""
        raise AssertionError((user, database, sql))

    monkeypatch.setattr(db_mod, "_run_psql", _fake_run_psql)

    db_mod._ensure_local_role(db_mod.Environment.dev, "postgres")

    assert calls == [
        ("postgres", "postgres", "SELECT 1"),
        ("jmajor", "postgres", "SELECT 1"),
        (
            "jmajor",
            "postgres",
            "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN CREATE ROLE \"postgres\" LOGIN SUPERUSER CREATEDB CREATEROLE; END IF; END $$;",
        ),
    ]


def test_user_open_connection_maps_config(monkeypatch):
    monkeypatch.setattr(
        user_mod,
        "get_db_config_for_env",
        lambda _env: {
            "host": "db.host",
            "port": "6000",
            "user": "usr",
            "password": "",
            "database": "tapdb_test",
            "engine_type": "LOCAL",
            "iam_auth": "on",
            "region": "us-west-1",
        },
    )
    captured: dict[str, object] = {}

    class _FakeConn:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(user_mod, "TAPDBConnection", _FakeConn)

    conn = user_mod._open_connection(db_mod.Environment.test, app_username="alice")
    assert isinstance(conn, _FakeConn)
    assert captured == {
        "db_hostname": "db.host:6000",
        "db_user": "usr",
        "db_pass": None,
        "db_name": "tapdb_test",
        "engine_type": "local",
        "region": "us-west-1",
        "iam_auth": True,
        "app_username": "alice",
    }


def test_user_format_date_handles_none_datetime_and_iso():
    assert user_mod._format_date(None) == "-"

    dt = datetime(2026, 3, 29, 10, 45)
    assert user_mod._format_date(dt) == "2026-03-29"
    assert user_mod._format_date(dt, include_time=True) == "2026-03-29 10:45"

    assert user_mod._format_date("2026-03-29T10:45:00Z") == "2026-03-29"
    assert (
        user_mod._format_date("2026-03-29T10:45:00Z", include_time=True)
        == "2026-03-29 10:45"
    )
    assert user_mod._format_date("not-a-date") == "not-a-date"
