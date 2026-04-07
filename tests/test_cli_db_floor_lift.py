from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer

import daylily_tapdb.cli.db as db_mod


def _base_cfg(**overrides):
    cfg = {
        "engine_type": "local",
        "host": "localhost",
        "port": "5533",
        "database": "tapdb_dev",
        "user": "tapdb",
        "password": "",
        "core_euid_prefix": "AGX",
        "audit_log_euid_prefix": "AGX",
        "region": "us-west-2",
        "iam_auth": "true",
    }
    cfg.update(overrides)
    return cfg


class _FakeConn:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def session_scope(self, commit: bool = False):
        _ = commit

        class _Scope:
            def __enter__(self_inner):
                return self.session

            def __exit__(self_inner, exc_type, exc, tb):
                return False

        return _Scope()


def test_db_normalization_and_schema_lookup_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="instance_prefix cannot be None"):
        db_mod._normalize_instance_prefix(None)

    with pytest.raises(ValueError, match="cannot be None"):
        db_mod._normalize_meridian_prefix(None, "audit_log_euid_prefix")

    monkeypatch.setattr(
        db_mod,
        "_get_db_config",
        lambda _env: {
            "core_euid_prefix": "AGX",
            "audit_log_euid_prefix": "BGX",
        },
    )
    with pytest.raises(ValueError, match="must match"):
        db_mod._required_identity_prefixes(db_mod.Environment.dev)

    monkeypatch.setattr(db_mod, "_schema_root_candidates", lambda: [])
    with pytest.raises(FileNotFoundError, match="Cannot find TAPDB schema root.$"):
        db_mod._find_schema_root()
    with pytest.raises(FileNotFoundError, match="schema/tapdb_schema.sql"):
        db_mod._find_schema_file()


def test_db_sequence_baseline_and_run_psql_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_run_psql = db_mod._run_psql
    monkeypatch.setattr(db_mod, "_normalize_instance_prefix", lambda _prefix: "A1")
    with pytest.raises(ValueError, match="alphabetic"):
        db_mod._ensure_instance_prefix_sequence(db_mod.Environment.dev, "A1")

    monkeypatch.setattr(db_mod, "_find_schema_root", lambda **_kwargs: (_ for _ in ()).throw(FileNotFoundError()))
    db_mod._write_migration_baseline(db_mod.Environment.dev)

    migrations_root = tmp_path / "schema"
    (migrations_root / "migrations").mkdir(parents=True)
    monkeypatch.setattr(db_mod, "_find_schema_root", lambda **_kwargs: migrations_root)
    db_mod._write_migration_baseline(db_mod.Environment.dev)

    (migrations_root / "migrations" / "20260407_init.sql").write_text("-- migration\n", encoding="utf-8")
    calls: list[str] = []

    def _run_create_fail(_env, sql=None, file=None, database=None, user=None):
        _ = (file, database, user)
        calls.append(sql or "")
        if "CREATE TABLE IF NOT EXISTS _tapdb_migrations" in (sql or ""):
            return False, "create failed"
        return True, ""

    monkeypatch.setattr(db_mod, "_run_psql", _run_create_fail)
    with pytest.raises(RuntimeError, match="create failed"):
        db_mod._write_migration_baseline(db_mod.Environment.dev)

    def _run_insert_fail(_env, sql=None, file=None, database=None, user=None):
        _ = (file, database, user)
        calls.append(sql or "")
        if "INSERT INTO _tapdb_migrations" in (sql or ""):
            return False, "insert failed"
        return True, ""

    monkeypatch.setattr(db_mod, "_run_psql", _run_insert_fail)
    with pytest.raises(RuntimeError, match="insert failed"):
        db_mod._write_migration_baseline(db_mod.Environment.dev)

    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg(password="secret"))
    captured_envs: list[dict[str, str]] = []

    def _fake_run(cmd, capture_output=True, text=True, env=None):
        _ = (cmd, capture_output, text)
        captured_envs.append(env)
        return SimpleNamespace(returncode=0, stdout="ok\n", stderr="")

    monkeypatch.setattr(db_mod.subprocess, "run", _fake_run)
    ok, out = real_run_psql(db_mod.Environment.dev, sql="SELECT 1")
    assert ok is True
    assert captured_envs[-1]["PGPASSWORD"] == "secret"

    monkeypatch.setattr(
        db_mod.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("psql boom")),
    )
    ok, out = real_run_psql(db_mod.Environment.dev, sql="SELECT 1")
    assert (ok, out) == (False, "psql boom")


def test_db_role_counts_schema_and_drift_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg(engine_type="aurora"))
    db_mod._ensure_local_role(db_mod.Environment.dev, "tapdb")
    db_mod._ensure_local_role(db_mod.Environment.prod, "tapdb")
    db_mod._ensure_local_role(db_mod.Environment.dev, "")

    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg())
    monkeypatch.setattr(db_mod, "_bootstrap_user_candidates", lambda _user: ["alice"])
    monkeypatch.setattr(db_mod, "_quoted_sql_literal", lambda value: f"'{value}'")
    monkeypatch.setattr(db_mod, "_quoted_sql_ident", lambda value: f'"{value}"')

    def _local_role_failure(_env, sql=None, file=None, database=None, user=None):
        _ = (file, database)
        if sql == "SELECT 1" and user == "tapdb":
            return False, 'psql: error: FATAL:  role "tapdb" does not exist'
        if sql == "SELECT 1" and user == "alice":
            return True, "1"
        if sql and "CREATE ROLE" in sql:
            return False, "create failed"
        return True, ""

    monkeypatch.setattr(db_mod, "_run_psql", _local_role_failure)
    with pytest.raises(RuntimeError, match="Failed to create missing local PostgreSQL role"):
        db_mod._ensure_local_role(db_mod.Environment.dev, "tapdb")

    responses = iter(
        [
            (True, "bad-int"),
            (False, ""),
            (True, "5"),
            (True, "7"),
            (True, "9"),
        ]
    )
    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: next(responses))
    counts = db_mod._get_table_counts(db_mod.Environment.dev)
    assert counts["generic_template"] == "?"
    assert counts["generic_instance"] is None

    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (False, "boom"))
    assert db_mod._schema_exists(db_mod.Environment.dev) is False
    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (True, "bad-int"))
    assert db_mod._schema_exists(db_mod.Environment.dev) is False

    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg())
    monkeypatch.setattr(db_mod, "_find_schema_root", lambda **_kwargs: tmp_path)
    monkeypatch.setattr(db_mod, "schema_asset_files", lambda _root: [tmp_path / "tapdb_schema.sql"])
    monkeypatch.setattr(
        db_mod,
        "_required_identity_prefixes",
        lambda _env: {"generic_template": "AGX"},
    )
    monkeypatch.setattr(db_mod, "_shared_sequence_name", lambda prefix: f"{prefix.lower()}_instance_seq")
    monkeypatch.setattr(db_mod, "build_expected_schema_inventory", lambda paths, dynamic_sequence_name: {"expected": paths, "seq": dynamic_sequence_name})
    monkeypatch.setattr(db_mod, "load_live_schema_inventory", lambda session: {"live": True, "session": session})

    class _Result:
        has_drift = True
        expected = {"tables": ["generic_template"]}
        live = {"tables": []}
        missing = {"tables": ["generic_template"]}
        unexpected = {"tables": []}

        def to_payload(self):
            return {
                "status": "drift",
                "database": "tapdb_dev",
                "schema_name": "public",
                "strict": True,
                "missing": {"tables": ["generic_template"]},
                "unexpected": {"tables": []},
            }

    monkeypatch.setattr(db_mod, "diff_schema_inventory", lambda *args, **kwargs: _Result())
    monkeypatch.setattr(db_mod, "inventory_counts", lambda value: {"count": len(value.get("tables", []))})
    monkeypatch.setattr(db_mod, "drift_entry_counts", lambda value: {"count": len(value.get("tables", []))})
    monkeypatch.setattr(
        db_mod,
        "_tapdb_connection_for_env",
        lambda _env, app_username: _FakeConn(session="session"),
    )

    payload, has_drift = db_mod._run_schema_drift_check(db_mod.Environment.dev, strict=True)
    assert has_drift is True
    assert payload["counts"]["expected"]["count"] == 1
    assert payload["counts"]["missing"]["count"] == 1


def test_db_callback_create_and_delete_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    db_mod._db_callback(SimpleNamespace(resilient_parsing=True, invoked_subcommand="schema"))
    db_mod._db_callback(SimpleNamespace(resilient_parsing=False, invoked_subcommand="config"))

    monkeypatch.setattr(
        "daylily_tapdb.cli._require_context",
        lambda: (_ for _ in ()).throw(RuntimeError("missing context")),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod._db_callback(SimpleNamespace(resilient_parsing=False, invoked_subcommand="schema"))
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg())
    monkeypatch.setattr(
        db_mod,
        "_ensure_local_role",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("role bootstrap failed")),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_create(db_mod.Environment.dev, owner=None)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_ensure_local_role", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (False, "cannot connect"))
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_create(db_mod.Environment.dev, owner=None)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (True, "1"))
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    db_mod.db_create(db_mod.Environment.dev, owner=None)

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    create_sqls: list[str] = []

    def _create_fail(_env, sql=None, file=None, database=None, user=None):
        _ = (file, user)
        create_sqls.append(sql or "")
        if database == "postgres" and sql == "SELECT 1":
            return True, "1"
        return False, "create failed"

    monkeypatch.setattr(db_mod, "_run_psql", _create_fail)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_create(db_mod.Environment.dev, owner=None)
    assert exc.value.exit_code == 1
    assert any('CREATE DATABASE "tapdb_dev" OWNER "tapdb"' in sql for sql in create_sqls)

    monkeypatch.setattr(
        db_mod,
        "_run_psql",
        lambda _env, sql=None, file=None, database=None, user=None: (
            (True, "1")
            if database == "postgres" and sql == "SELECT 1"
            else (True, "")
        ),
    )
    db_mod.db_create(db_mod.Environment.dev, owner="owner1")

    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (False, "cannot connect"))
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_delete(db_mod.Environment.dev, force=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (True, "1"))
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    db_mod.db_delete(db_mod.Environment.dev, force=False)

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(db_mod.Confirm, "ask", lambda *args, **kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_delete(db_mod.Environment.dev, force=False)
    assert exc.value.exit_code == 0

    monkeypatch.setattr(db_mod.Confirm, "ask", lambda *args, **kwargs: True)
    monkeypatch.setattr(db_mod.typer, "prompt", lambda _msg: "wrong-name")
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_delete(db_mod.Environment.prod, force=False)
    assert exc.value.exit_code == 1

    ops: list[tuple[str | None, str | None]] = []

    def _drop_fail(_env, sql=None, file=None, database=None, user=None):
        _ = (file, user)
        ops.append((database, sql))
        if database == "postgres" and sql == "SELECT 1":
            return True, "1"
        if sql and "DROP DATABASE" in sql:
            return False, "drop failed"
        return True, ""

    monkeypatch.setattr(db_mod, "_run_psql", _drop_fail)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_delete(db_mod.Environment.dev, force=True)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        db_mod,
        "_run_psql",
        lambda _env, sql=None, file=None, database=None, user=None: (
            (True, "1")
            if database == "postgres" and sql == "SELECT 1"
            else (True, "")
        ),
    )
    db_mod.db_delete(db_mod.Environment.dev, force=True)


def test_db_schema_apply_status_and_drift_command_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema_file = tmp_path / "tapdb_schema.sql"
    schema_file.write_text("-- schema\n", encoding="utf-8")

    monkeypatch.setattr(db_mod, "_ensure_dirs", lambda: None)
    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg())
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_apply(db_mod.Environment.dev, reinitialize=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        db_mod,
        "_find_schema_file",
        lambda: (_ for _ in ()).throw(FileNotFoundError("missing schema")),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_apply(db_mod.Environment.dev, reinitialize=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_find_schema_file", lambda: schema_file)
    monkeypatch.setattr(db_mod, "_schema_exists", lambda _env: True)
    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (False, "schema apply failed"))
    log_calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(db_mod, "_log_operation", lambda *args: log_calls.append(args))
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_apply(db_mod.Environment.dev, reinitialize=False)
    assert exc.value.exit_code == 1
    assert log_calls[-1][1] == "SCHEMA_APPLY_FAILED"

    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    monkeypatch.setattr(
        db_mod,
        "_sync_identity_prefix_config",
        lambda _env: (_ for _ in ()).throw(RuntimeError("sync failed")),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_apply(db_mod.Environment.dev, reinitialize=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_sync_identity_prefix_config", lambda _env: None)
    monkeypatch.setattr(
        db_mod,
        "_write_migration_baseline",
        lambda _env: (_ for _ in ()).throw(RuntimeError("baseline failed")),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_apply(db_mod.Environment.dev, reinitialize=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_status(db_mod.Environment.dev)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(db_mod, "_schema_exists", lambda _env: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_status(db_mod.Environment.dev)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg(engine_type="aurora", region="us-east-1", iam_auth="false"))
    monkeypatch.setattr(db_mod, "_schema_exists", lambda _env: True)
    monkeypatch.setattr(
        db_mod,
        "_get_table_counts",
        lambda _env: {
            "generic_template": None,
            "generic_instance": 1,
            "generic_instance_lineage": 2,
            "audit_log": 3,
            "tapdb_identity_prefix_config": 4,
        },
    )
    db_mod.db_status(db_mod.Environment.dev)

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_drift_check(db_mod.Environment.dev, json_output=False, strict=False)
    assert exc.value.exit_code == 2

    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_drift_check(db_mod.Environment.dev, json_output=True, strict=False)
    assert exc.value.exit_code == 2

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        db_mod,
        "_run_schema_drift_check",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("drift exploded")),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_drift_check(db_mod.Environment.dev, json_output=False, strict=False)
    assert exc.value.exit_code == 2
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_drift_check(db_mod.Environment.dev, json_output=True, strict=False)
    assert exc.value.exit_code == 2

    monkeypatch.setattr(
        db_mod,
        "_run_schema_drift_check",
        lambda *_args, **_kwargs: (
            {
                "database": "tapdb_dev",
                "schema_name": "public",
                "strict": True,
                "counts": {"expected": 1, "live": 0},
                "missing": {"tables": ["generic_template"], "columns": []},
                "unexpected": {"tables": [], "columns": []},
            },
            True,
        ),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_schema_drift_check(db_mod.Environment.dev, json_output=False, strict=True)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        db_mod,
        "_run_schema_drift_check",
        lambda *_args, **_kwargs: (
            {
                "database": "tapdb_dev",
                "schema_name": "public",
                "strict": False,
                "counts": {"expected": 1, "live": 1},
                "missing": {"tables": [], "columns": []},
                "unexpected": {"tables": [], "columns": []},
            },
            False,
        ),
    )
    db_mod.db_schema_drift_check(db_mod.Environment.dev, json_output=False, strict=False)


def test_db_nuke_migrate_backup_restore_validate_admin_seed_and_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg())
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    db_mod.db_nuke(db_mod.Environment.dev, force=False)

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        db_mod,
        "_get_table_counts",
        lambda _env: {
            "generic_template": 1,
            "generic_instance": 2,
            "generic_instance_lineage": 3,
            "audit_log": 4,
            "tapdb_identity_prefix_config": 5,
        },
    )
    monkeypatch.setattr(db_mod.Confirm, "ask", lambda *args, **kwargs: False)
    db_mod.db_nuke(db_mod.Environment.dev, force=False)

    monkeypatch.setattr(db_mod.Confirm, "ask", lambda *args, **kwargs: True)
    prompts = iter(["wrong-env"])
    monkeypatch.setattr(db_mod.Prompt, "ask", lambda _msg: next(prompts))
    db_mod.db_nuke(db_mod.Environment.dev, force=False)

    prompts = iter(["dev", "DELETE EVERYTHING"])
    monkeypatch.setattr(db_mod.Prompt, "ask", lambda _msg: next(prompts))
    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (False, "nuke failed"))
    log_calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(db_mod, "_log_operation", lambda *args: log_calls.append(args))
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_nuke(db_mod.Environment.dev, force=False)
    assert exc.value.exit_code == 1
    assert log_calls[-1][1] == "NUKE_FAILED"

    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    db_mod.db_nuke(db_mod.Environment.dev, force=True)

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_migrate(db_mod.Environment.dev, dry_run=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(db_mod, "_schema_exists", lambda _env: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_migrate(db_mod.Environment.dev, dry_run=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_schema_exists", lambda _env: True)
    monkeypatch.setattr(
        db_mod,
        "_find_schema_root",
        lambda **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    db_mod.db_migrate(db_mod.Environment.dev, dry_run=False)

    migrations_root = tmp_path / "schema"
    (migrations_root / "migrations").mkdir(parents=True)
    monkeypatch.setattr(db_mod, "_find_schema_root", lambda **_kwargs: migrations_root)
    db_mod.db_migrate(db_mod.Environment.dev, dry_run=False)

    migration_file = migrations_root / "migrations" / "20260407_init.sql"
    migration_file.write_text("-- migration\n", encoding="utf-8")
    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (False, "ensure table failed"))
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_migrate(db_mod.Environment.dev, dry_run=False)
    assert exc.value.exit_code == 1

    migration_state = {
        "ensure": True,
        "list": True,
        "apply": True,
    }

    def _migrate_dry_run(_env, sql=None, file=None, database=None, user=None):
        _ = (database, user)
        if sql and "CREATE TABLE IF NOT EXISTS _tapdb_migrations" in sql:
            return True, ""
        if sql == "SELECT filename FROM _tapdb_migrations":
            return True, ""
        if file is not None:
            return migration_state["apply"], "apply failed" if not migration_state["apply"] else ""
        return True, ""

    monkeypatch.setattr(db_mod, "_run_psql", _migrate_dry_run)
    db_mod.db_migrate(db_mod.Environment.dev, dry_run=True)
    migration_state["apply"] = False
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_migrate(db_mod.Environment.dev, dry_run=False)
    assert exc.value.exit_code == 1
    migration_state["apply"] = True
    db_mod.db_migrate(db_mod.Environment.dev, dry_run=False)

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_backup(db_mod.Environment.dev, backup_path=None, data_only=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    backup_path = tmp_path / "tapdb.sql"

    def _backup_run(cmd, capture_output=True, text=True, env=None):
        _ = (capture_output, text, env)
        Path(cmd[cmd.index("-f") + 1]).write_text("-- backup\n", encoding="utf-8")
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(db_mod.subprocess, "run", _backup_run)
    db_mod.db_backup(db_mod.Environment.dev, backup_path=backup_path, data_only=True)

    monkeypatch.setattr(
        db_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stderr="pg_dump failed"),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_backup(db_mod.Environment.dev, backup_path=backup_path, data_only=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        db_mod.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_backup(db_mod.Environment.dev, backup_path=backup_path, data_only=False)
    assert exc.value.exit_code == 1

    with pytest.raises(typer.Exit) as exc:
        db_mod.db_restore(db_mod.Environment.dev, tmp_path / "missing.sql", force=False)
    assert exc.value.exit_code == 1

    restore_input = tmp_path / "restore.sql"
    restore_input.write_text("-- restore\n", encoding="utf-8")
    monkeypatch.setattr(db_mod.Confirm, "ask", lambda *args, **kwargs: False)
    db_mod.db_restore(db_mod.Environment.dev, restore_input, force=False)

    monkeypatch.setattr(db_mod.Confirm, "ask", lambda *args, **kwargs: True)
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_restore(db_mod.Environment.dev, restore_input, force=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (False, "restore failed"))
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_restore(db_mod.Environment.dev, restore_input, force=True)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    monkeypatch.setattr(
        db_mod,
        "_get_table_counts",
        lambda _env: {
            "generic_template": 1,
            "generic_instance": 2,
            "generic_instance_lineage": 3,
            "audit_log": 4,
            "tapdb_identity_prefix_config": 5,
        },
    )
    db_mod.db_restore(db_mod.Environment.dev, restore_input, force=True)

    monkeypatch.setattr(
        db_mod,
        "_resolve_seed_config_dirs",
        lambda _path: (_ for _ in ()).throw(FileNotFoundError("missing config")),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_validate_config(config_path=None, strict=True, json_output=False)
    assert exc.value.exit_code == 1

    Issue = SimpleNamespace
    monkeypatch.setattr(db_mod, "_resolve_seed_config_dirs", lambda _path: [tmp_path])
    monkeypatch.setattr(
        db_mod,
        "_validate_template_configs",
        lambda *_args, **_kwargs: (
            [],
            [
                Issue(level="error", message="bad template", source_file="a.json", template_code="generic/a"),
                Issue(level="warning", message="warn template", source_file="b.json", template_code="generic/b"),
            ],
        ),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_validate_config(config_path=None, strict=True, json_output=True)
    assert exc.value.exit_code == 1
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_validate_config(config_path=None, strict=True, json_output=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_validate_template_configs", lambda *_args, **_kwargs: ([], []))
    db_mod.db_validate_config(config_path=None, strict=False, json_output=False)

    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg())
    assert db_mod._create_default_admin(db_mod.Environment.dev, insecure_dev_defaults=False) is False
    assert db_mod._create_default_admin(db_mod.Environment.prod, insecure_dev_defaults=True) is False

    monkeypatch.setattr(
        db_mod,
        "TAPDBConnection",
        lambda **kwargs: _FakeConn(session="session"),
    )
    monkeypatch.setattr(
        "daylily_tapdb.user_store.create_or_get",
        lambda *_args, **_kwargs: (SimpleNamespace(username="tapdb_admin"), False),
    )
    monkeypatch.setattr("daylily_tapdb.cli.user._hash_password", lambda _value: "hashed")
    assert db_mod._create_default_admin(db_mod.Environment.dev, insecure_dev_defaults=True) is False

    monkeypatch.setattr(
        "daylily_tapdb.user_store.create_or_get",
        lambda *_args, **_kwargs: (SimpleNamespace(username="tapdb_admin"), True),
    )
    assert db_mod._create_default_admin(db_mod.Environment.dev, insecure_dev_defaults=True) is True

    monkeypatch.setattr(
        db_mod,
        "TAPDBConnection",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("connection failed")),
    )
    assert db_mod._create_default_admin(db_mod.Environment.dev, insecure_dev_defaults=True) is False

    monkeypatch.setattr(
        db_mod,
        "_resolve_seed_config_dirs",
        lambda _path: (_ for _ in ()).throw(FileNotFoundError("missing config")),
    )
    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg())
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=False, skip_existing=True, dry_run=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_resolve_seed_config_dirs", lambda _path: [tmp_path])
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=False, skip_existing=True, dry_run=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(db_mod, "_schema_exists", lambda _env: False)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=False, skip_existing=True, dry_run=False)
    assert exc.value.exit_code == 1

    Issue = SimpleNamespace
    monkeypatch.setattr(
        db_mod,
        "_validate_template_configs",
        lambda *_args, **_kwargs: (
            [],
            [Issue(level="error", message="bad template", source_file="a.json", template_code="generic/a")],
        ),
    )
    monkeypatch.setattr(db_mod, "_schema_exists", lambda _env: True)
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=False, skip_existing=True, dry_run=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_validate_template_configs", lambda *_args, **_kwargs: ([], []))
    db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=False, skip_existing=True, dry_run=False)

    templates = [{"category": "generic", "type": "core", "subtype": "foo", "version": "1.0", "name": "Foo"}]
    monkeypatch.setattr(db_mod, "_validate_template_configs", lambda *_args, **_kwargs: (templates, []))
    monkeypatch.setattr(
        db_mod,
        "_find_duplicate_template_keys",
        lambda _templates: {("generic", "core", "foo", "1.0"): ["a.json", "b.json"]},
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=False, skip_existing=True, dry_run=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(db_mod, "_find_duplicate_template_keys", lambda _templates: {})
    db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=False, skip_existing=True, dry_run=True)

    monkeypatch.setattr(
        db_mod,
        "_tapdb_connection_for_env",
        lambda _env, app_username: (_ for _ in ()).throw(RuntimeError("seed failed")),
    )
    with pytest.raises(typer.Exit) as exc:
        db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=False, skip_existing=True, dry_run=False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        db_mod,
        "_tapdb_connection_for_env",
        lambda _env, app_username: _FakeConn(session="session"),
    )
    monkeypatch.setattr(db_mod, "_loader_find_tapdb_core_config_dir", lambda: tmp_path / "core")
    monkeypatch.setattr(
        db_mod,
        "_loader_seed_templates",
        lambda *_args, **_kwargs: SimpleNamespace(inserted=2, updated=1, skipped=3, prefixes_ensured=4),
    )
    db_mod.db_seed(db_mod.Environment.dev, config_path=None, include_workflow=True, skip_existing=False, dry_run=False)

    db_calls: list[str] = []
    monkeypatch.setattr(db_mod, "db_delete", lambda *_args, **_kwargs: db_calls.append("delete"))
    monkeypatch.setattr(db_mod, "db_create", lambda *_args, **_kwargs: db_calls.append("create"))
    monkeypatch.setattr(db_mod, "db_schema_apply", lambda *_args, **_kwargs: db_calls.append("apply"))
    monkeypatch.setattr(db_mod, "db_migrate", lambda *_args, **_kwargs: db_calls.append("migrate"))
    monkeypatch.setattr(db_mod, "db_seed", lambda *_args, **_kwargs: db_calls.append("seed"))
    monkeypatch.setattr(db_mod, "_create_default_admin", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(db_mod, "_get_connection_string", lambda _env: "postgresql://tapdb@localhost:5533/tapdb_dev")

    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg(engine_type="local"))
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda *_args, **_kwargs: True)
    db_mod.db_setup(
        db_mod.Environment.dev,
        force=True,
        include_workflow=False,
        insecure_dev_defaults=True,
    )
    assert db_calls[:6] == ["delete", "create", "apply", "migrate", "seed"]

    db_calls.clear()
    monkeypatch.setattr(db_mod, "_get_db_config", lambda _env: _base_cfg(engine_type="aurora"))
    db_mod.db_setup(
        db_mod.Environment.dev,
        force=True,
        include_workflow=True,
        insecure_dev_defaults=False,
    )
    assert "delete" not in db_calls
