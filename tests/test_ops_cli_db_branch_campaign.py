"""Behavior coverage for explicit-target database CLI branches."""

from __future__ import annotations

import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import typer

import daylily_tapdb.cli.db as db


@pytest.fixture
def cfg(tmp_path: Path) -> dict[str, Any]:
    return {
        "client_id": "alpha",
        "database_name": "beta",
        "engine_type": "local",
        "host": "localhost",
        "hostaddr": "",
        "port": "5533",
        "user": "tapdb",
        "password": "",
        "database": "tapdb_shared",
        "schema_name": "tapdb_beta",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "config_path": str(tmp_path / "tapdb-config.yaml"),
        "domain_registry_path": str(tmp_path / "domains.json"),
        "prefix_ownership_registry_path": str(tmp_path / "prefixes.json"),
        "region": "us-west-2",
        "iam_auth": "false",
        "secret_arn": "",
        "operator_configured": True,
        "operator_user": "tapdb_operator",
        "operator_password": "",
        "operator_secret_arn": "",
        "operator_iam_auth": False,
        "safety_tier": "shared",
        "destructive_operations": "allowed",
    }


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any]) -> dict[str, Any]:
    monkeypatch.setattr(db, "_get_db_config", lambda _env: cfg)
    return cfg


def test_prefix_and_sql_helpers_cover_validation_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for value, message in ((None, "None"), (" ", "empty"), ("I", "must match")):
        with pytest.raises(ValueError, match=message):
            db._normalize_instance_prefix(value)  # type: ignore[arg-type]
    assert db._normalize_instance_prefix(" ab1 ") == "AB1"

    for value, message in (
        (None, "cannot be None"),
        (" ", "cannot be empty"),
        ("I", "must match"),
        ("GX", "reserved"),
    ):
        with pytest.raises(ValueError, match=message):
            db._normalize_meridian_prefix(value, "prefix")  # type: ignore[arg-type]
    assert db._normalize_meridian_prefix(" edg ", "prefix") == "EDG"
    assert db._shared_sequence_name("TPX") == "tpx_instance_seq"
    assert db._quoted_sql_literal("a'b") == "'a''b'"
    assert db._quoted_sql_ident('a"b') == '"a""b"'
    assert db._set_search_path_sql('a"b') == 'SET search_path TO "a""b"'

    monkeypatch.setattr(db.getpass, "getuser", lambda: "operator")
    with pytest.raises(ValueError, match="domain_code"):
        db._set_runtime_context_sql("tapdb_beta", {})
    runtime_sql = db._set_runtime_context_sql(
        "tapdb_beta",
        {
            "domain_code": "Z",
            "owner_repo_name": "daylily-tapdb",
            "config_path": "/abs/tapdb-config.yaml",
        },
    )
    assert "cli:operator" in runtime_sql
    assert "session.current_owner_repo_name" in runtime_sql
    assert db._with_schema_search_path("tapdb_beta", "SELECT 1").endswith(";\nSELECT 1")
    assert "session.current_domain_code" in db._with_schema_search_path(
        "tapdb_beta",
        "SELECT 1",
        cfg={
            "domain_code": "Z",
            "owner_repo_name": "daylily-tapdb",
            "config_path": "/abs/tapdb-config.yaml",
        },
    )


def test_file_config_schema_and_parser_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cfg: dict[str, str],
) -> None:
    sql_file = tmp_path / "input.sql"
    sql_file.write_text("SELECT 2;\n", encoding="utf-8")
    assert "SELECT 2" in db._read_file_with_schema_search_path("tapdb_beta", sql_file)
    assert db._uses_configured_database(None) is True
    assert db._uses_configured_database("postgres") is False

    monkeypatch.setattr(db, "get_db_config", lambda: cfg)
    assert db._get_db_config(db.Environment.target) is cfg
    assert db._configured_schema_name(db.Environment.target) == "tapdb_beta"
    cfg["schema_name"] = "  "
    assert db._configured_schema_name(db.Environment.target) is None
    with pytest.raises(ValueError, match="schema_name"):
        db._get_schema_name(db.Environment.target)
    cfg["schema_name"] = "tapdb_beta"

    assert db._parse_single_int("\nnoise\n 42\n") == 42
    with pytest.raises(ValueError, match="Could not parse"):
        db._parse_single_int("\nnoise\n")
    monkeypatch.setenv("USER", "tapdb")
    monkeypatch.setattr(db.getpass, "getuser", lambda: "admin")


def test_connection_strings_cover_local_and_aurora(
    configured: dict[str, Any],
) -> None:
    assert db._get_connection_string(db.Environment.target).endswith("/tapdb_shared")
    configured.update(engine_type="aurora", hostaddr="127.0.0.1")
    uri = db._get_connection_string(db.Environment.target, database="postgres")
    assert "sslmode=verify-full" in uri
    assert "hostaddr=127.0.0.1" in uri
    configured["hostaddr"] = ""
    assert "hostaddr" not in db._get_connection_string(db.Environment.target)


def test_run_psql_local_success_failure_and_exceptions(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PGPASSWORD", "ambient-wrong-target")
    calls: list[tuple[list[str], dict[str, str]]] = []

    def run_ok(command, **kwargs):
        calls.append((list(command), dict(kwargs["env"])))
        return subprocess.CompletedProcess(command, 0, " 1\n", "")

    monkeypatch.setattr(db.subprocess, "run", run_ok)
    ok, output = db._run_psql(db.Environment.target, sql="SELECT 1")
    assert (ok, output) == (True, "1")
    assert "session.current_schema_name" in calls[-1][0][calls[-1][0].index("-c") + 1]
    assert calls[-1][0][-2:] == ["-c", "SELECT 1"]
    assert "PGPASSWORD" not in calls[-1][1]

    configured["password"] = "hidden"
    sql_file = tmp_path / "schema.sql"
    sql_file.write_text("SELECT 2;", encoding="utf-8")
    assert db._run_psql(
        db.Environment.target, file=sql_file, database="postgres", user="root"
    )[0]
    assert "-f" in calls[-1][0]
    assert calls[-1][1]["PGPASSWORD"] == "hidden"

    monkeypatch.setattr(
        db.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 2, "out", "err"),
    )
    assert db._run_psql(db.Environment.target, sql="bad") == (False, "outerr")

    def missing(*_args, **_kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(db.subprocess, "run", missing)
    assert "psql not found" in db._run_psql(db.Environment.target)[1]

    def broken(*_args, **_kwargs):
        raise TimeoutError("late")

    monkeypatch.setattr(db.subprocess, "run", broken)
    assert db._run_psql(db.Environment.target)[1] == "late"


def test_run_psql_aurora_wraps_sql_and_files(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configured.update(engine_type="aurora", iam_auth="yes", password="hidden")
    seen: list[dict[str, object]] = []

    def fake_run(**kwargs):
        seen.append(kwargs)
        return True, "ok"

    from daylily_tapdb.aurora.schema_deployer import AuroraSchemaDeployer

    monkeypatch.setattr(AuroraSchemaDeployer, "run_psql", staticmethod(fake_run))
    assert db._run_psql(db.Environment.target, sql="SELECT 1") == (True, "ok")
    assert "session.current_schema_name" in str(seen[-1]["sql"])

    sql_file = tmp_path / "schema.sql"
    sql_file.write_text("SELECT 2;", encoding="utf-8")
    db._run_psql(db.Environment.target, file=sql_file)
    assert seen[-1]["file"] is None
    assert "SELECT 2" in str(seen[-1]["sql"])

    configured["iam_auth"] = "false"
    db._run_psql(db.Environment.target, sql="SELECT 3", database="postgres")
    assert seen[-1]["iam_auth"] is False
    assert seen[-1]["sql"] == "SELECT 3"


def test_local_role_creation_paths(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, object]] = []
    configured["engine_type"] = "aurora"
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: pytest.fail())
    db._ensure_local_role(db.Environment.target, "tapdb")

    configured["engine_type"] = "local"
    db._ensure_local_role(db.Environment.target, " ")

    def existing(_env, **kwargs):
        calls.append(kwargs)
        return True, ""

    monkeypatch.setattr(db, "_run_psql", existing)
    db._ensure_local_role(db.Environment.target, "tapdb")
    assert len(calls) == 1

    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (False, "denied"))
    with pytest.raises(RuntimeError, match="denied"):
        db._ensure_local_role(db.Environment.target, "tapdb")


def test_database_schema_count_helpers(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (True, "1"))
    assert db._check_db_exists(db.Environment.target, "tapdb_shared") is True
    assert all(
        value == 1 for value in db._get_table_counts(db.Environment.target).values()
    )
    assert db._schema_exists(db.Environment.target) is True

    outputs = iter(
        [(True, "bad"), (False, "bad"), (True, "3"), (True, "4"), (True, "5")]
    )
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: next(outputs))
    counts = db._get_table_counts(db.Environment.target)
    assert list(counts.values())[:2] == ["?", None]

    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (False, "bad"))
    assert db._schema_exists(db.Environment.target) is False
    with pytest.raises(RuntimeError, match="Failed to create schema"):
        db._ensure_schema_exists(db.Environment.target)
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    db._ensure_schema_exists(db.Environment.target)


def test_schema_drift_inventory_payload_is_built_from_live_session(
    configured: dict[str, str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    assets = [tmp_path / "tapdb_schema.sql", tmp_path / "rls.sql"]
    expected = SimpleNamespace(counts=lambda: {"tables": 2})
    live = SimpleNamespace(counts=lambda: {"tables": 1})
    missing = SimpleNamespace(marker="missing")
    unexpected = SimpleNamespace(marker="unexpected")

    drift = SimpleNamespace(
        has_drift=True,
        expected=expected,
        live=live,
        missing=missing,
        unexpected=unexpected,
        to_payload=lambda: {
            "status": "drift",
            "database": "tapdb_shared",
            "schema_name": "tapdb_beta",
        },
    )

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @contextmanager
        def session_scope(self, **_kwargs):
            yield "session"

    monkeypatch.setattr(db, "_find_schema_root", lambda **_kwargs: tmp_path)
    monkeypatch.setattr(db, "schema_asset_files", lambda _root: assets)
    monkeypatch.setattr(
        db, "load_expected_schema_inventory", lambda *_args, **_kwargs: expected
    )
    monkeypatch.setattr(
        db, "load_live_schema_inventory", lambda session, **_kwargs: live
    )
    monkeypatch.setattr(db, "diff_schema_inventory", lambda *_args, **_kwargs: drift)
    monkeypatch.setattr(
        db,
        "drift_entry_counts",
        lambda value: {"tables": 1 if value is missing else 0},
    )
    monkeypatch.setattr(
        db, "_tapdb_connection_for_env", lambda *_args, **_kwargs: Connection()
    )

    payload, has_drift = db._run_schema_drift_check(db.Environment.target, strict=True)

    assert has_drift is True
    assert payload["counts"] == {
        "expected": {"tables": 2},
        "live": {"tables": 1},
        "missing": {"tables": 1},
        "unexpected": {"tables": 0},
    }


def test_directory_logging_and_loader_adapters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "config.yaml"
    monkeypatch.setattr(db, "get_config_path", lambda: config)
    monkeypatch.setattr(db, "utc_now", lambda: SimpleNamespace(isoformat=lambda: "now"))
    monkeypatch.setenv("USER", "operator")
    db._ensure_dirs()
    db._log_operation("target", "TEST", "details")
    assert "operator | target | TEST | details" in (
        tmp_path / "logs" / "db_operations.log"
    ).read_text(encoding="utf-8")

    monkeypatch.setattr(db, "_loader_find_config_dir", lambda: tmp_path / "templates")
    monkeypatch.setattr(
        db, "_loader_find_tapdb_core_config_dir", lambda: tmp_path / "core"
    )
    monkeypatch.setattr(db, "_loader_resolve_seed_config_dirs", lambda path: [path])
    assert db._find_config_dir() == tmp_path / "templates"
    assert db._find_tapdb_core_config_dir() == tmp_path / "core"
    assert db._resolve_seed_config_dirs(tmp_path) == [tmp_path]


def test_migration_baseline_absent_empty_success_and_failure(
    configured: dict[str, str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        db,
        "_find_schema_root",
        lambda **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    db._write_migration_baseline(db.Environment.target)

    root = tmp_path / "schema"
    migrations = root / "migrations"
    migrations.mkdir(parents=True)
    monkeypatch.setattr(db, "_find_schema_root", lambda **_kwargs: root)
    db._write_migration_baseline(db.Environment.target)
    (migrations / "a'b.sql").write_text("SELECT 1", encoding="utf-8")

    calls: list[str] = []
    monkeypatch.setattr(
        db,
        "_run_psql",
        lambda _env, sql, **_kwargs: calls.append(sql) or (True, ""),
    )
    db._write_migration_baseline(db.Environment.target)
    assert "a''b.sql" in calls[-1]

    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (False, "boom"))
    with pytest.raises(RuntimeError, match="boom"):
        db._write_migration_baseline(db.Environment.target)


def test_db_create_and_delete_paths(
    configured: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    monkeypatch.setattr(db, "_ensure_local_role", lambda *_args: None)
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        db,
        "_run_psql",
        lambda _env, sql=None, **_kwargs: events.append(str(sql)) or (True, ""),
    )
    db.db_create(owner="tapdb_operator")
    assert any(
        'CREATE DATABASE "tapdb_shared" OWNER "tapdb_operator"' in e for e in events
    )

    monkeypatch.setattr(db, "_check_db_exists", lambda *_args, **_kwargs: True)
    db.db_create(owner=None)

    monkeypatch.setattr(
        db,
        "_ensure_local_role",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("role")),
    )
    with pytest.raises(typer.Exit):
        db.db_create(owner=None)

    monkeypatch.setattr(db, "_ensure_local_role", lambda *_args: None)
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (False, "offline"))
    with pytest.raises(typer.Exit):
        db.db_create(owner=None)

    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args, **_kwargs: False)
    db.db_delete(confirm_target=None)
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args, **_kwargs: True)
    db.db_delete(confirm_target=None)
    monkeypatch.setattr(
        db, "_run_psql", lambda *_args, **_kwargs: (False, "drop failed")
    )
    with pytest.raises(typer.Exit):
        db.db_delete(confirm_target=None)


def test_schema_apply_success_and_refusal_paths(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    schema = tmp_path / "tapdb_schema.sql"
    rls = tmp_path / "rls.sql"
    schema.write_text("SELECT 1;", encoding="utf-8")
    rls.write_text("SELECT 2;", encoding="utf-8")
    events: list[str] = []
    monkeypatch.setattr(db, "_ensure_dirs", lambda: events.append("dirs"))
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_find_schema_file", lambda: schema)
    monkeypatch.setattr(db, "_ensure_schema_exists", lambda *_args: None)
    monkeypatch.setattr(db, "_schema_exists", lambda *_args: False)
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    monkeypatch.setattr(
        db, "_sync_identity_prefix_config", lambda *_args: events.append("sync")
    )
    monkeypatch.setattr(
        db, "_write_migration_baseline", lambda *_args: events.append("baseline")
    )
    monkeypatch.setattr(db, "_log_operation", lambda *_args, **_kwargs: None)
    db.db_schema_apply(reinitialize=False)
    assert events[-2:] == ["sync", "baseline"]

    monkeypatch.setattr(db, "_schema_exists", lambda *_args: True)
    db.db_schema_apply(reinitialize=False)
    db.db_schema_apply(reinitialize=True)

    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: False)
    with pytest.raises(typer.Exit):
        db.db_schema_apply()
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    monkeypatch.setattr(
        db,
        "_find_schema_file",
        lambda: (_ for _ in ()).throw(FileNotFoundError("missing")),
    )
    with pytest.raises(typer.Exit):
        db.db_schema_apply()
    monkeypatch.setattr(db, "_find_schema_file", lambda: schema)
    monkeypatch.setattr(
        db,
        "_ensure_schema_exists",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("schema")),
    )
    with pytest.raises(typer.Exit):
        db.db_schema_apply()

    monkeypatch.setattr(db, "_ensure_schema_exists", lambda *_args: None)
    rls.unlink()
    with pytest.raises(typer.Exit):
        db.db_schema_apply()


def test_status_drift_and_nuke_branches(
    configured: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: False)
    with pytest.raises(typer.Exit):
        db.db_status()
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_schema_exists", lambda *_args: False)
    with pytest.raises(typer.Exit):
        db.db_status()
    monkeypatch.setattr(db, "_schema_exists", lambda *_args: True)
    monkeypatch.setattr(
        db, "_get_table_counts", lambda *_args: {"good": 1, "bad": None}
    )
    db.db_status()
    configured.update(engine_type="aurora", iam_auth="true")
    db.db_status()

    payload = {
        "database": "tapdb_shared",
        "schema_name": "tapdb_beta",
        "strict": True,
        "counts": {"expected": {}, "live": {}},
        "missing": {"tables": ["one"], "views": []},
        "unexpected": {"tables": [], "views": ["extra"]},
    }
    monkeypatch.setattr(
        db, "_run_schema_drift_check", lambda *_args, **_kwargs: (payload, False)
    )
    with pytest.raises(typer.Exit) as clean:
        db.db_schema_drift_check(json_output=True, strict=True)
    assert clean.value.exit_code == 0
    db.db_schema_drift_check(json_output=False, strict=True)

    monkeypatch.setattr(
        db, "_run_schema_drift_check", lambda *_args, **_kwargs: (payload, True)
    )
    with pytest.raises(typer.Exit):
        db.db_schema_drift_check(json_output=False, strict=True)
    monkeypatch.setattr(
        db,
        "_run_schema_drift_check",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("drift")),
    )
    with pytest.raises(typer.Exit):
        db.db_schema_drift_check(json_output=True, strict=False)

    configured["engine_type"] = "local"
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: False)
    db.db_nuke(confirm_target=None)
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_get_table_counts", lambda *_args: {"generic_template": 2})
    logs: list[str] = []
    monkeypatch.setattr(db, "_log_operation", lambda *_args: logs.append(str(_args)))
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    db.db_nuke(confirm_target=None)
    assert logs
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (False, "no"))
    with pytest.raises(typer.Exit):
        db.db_nuke(confirm_target=None)


class _Transaction:
    def __init__(self, events: list[str]):
        self.events = events

    def commit(self) -> None:
        self.events.append("commit")

    def rollback(self) -> None:
        self.events.append("rollback")


class _EngineConnection:
    def __init__(self, events: list[str]):
        self.events = events

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def begin(self):
        return _Transaction(self.events)

    def exec_driver_sql(self, statement):
        self.events.append(str(statement))


class _TapdbConnection:
    def __init__(self, events: list[str]):
        self.engine = SimpleNamespace(connect=lambda: _EngineConnection(events))

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self, **_kwargs):
        yield object()


def test_migrate_preflight_apply_and_validation(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_schema_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_schema_root_candidates", lambda: [tmp_path])
    monkeypatch.setattr(db, "_find_schema_root", lambda **_kwargs: tmp_path)
    events: list[str] = []
    monkeypatch.setattr(
        db,
        "_tapdb_connection_for_env",
        lambda *_args, **_kwargs: _TapdbConnection(events),
    )
    monkeypatch.setattr(db, "_log_operation", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        db,
        "build_migration_preflight",
        lambda *_args, **_kwargs: {"pending_migrations": ["one"]},
    )
    monkeypatch.setattr(
        db,
        "write_json_receipt",
        lambda path, payload: events.append(f"write:{path.name}:{len(payload)}"),
    )
    preflight = tmp_path / "preflight.json"
    db.db_migrate(dry_run=True, apply=False, receipt=preflight, preflight_receipt=None)
    assert "rollback" in events

    result = SimpleNamespace(receipt={"applied_migrations": ["one"]})
    monkeypatch.setattr(db, "load_json_receipt", lambda _path: {"approved": True})
    monkeypatch.setattr(
        db, "apply_migration_preflight", lambda *_args, **_kwargs: result
    )
    receipt = tmp_path / "result.json"
    approved = tmp_path / "approved.json"
    db.db_migrate(
        dry_run=False, apply=True, receipt=receipt, preflight_receipt=approved
    )
    assert "commit" in events

    cases = [
        (True, True, tmp_path / "x1.json", None),
        (True, False, None, None),
        (True, False, Path("relative.json"), None),
        (False, True, tmp_path / "x2.json", None),
        (True, False, tmp_path / "x3.json", approved),
    ]
    for dry_run, apply, output, prior in cases:
        with pytest.raises(SystemExit):
            db.db_migrate(
                dry_run=dry_run,
                apply=apply,
                receipt=output,
                preflight_receipt=prior,
            )

    existing = tmp_path / "existing.json"
    existing.write_text("{}", encoding="utf-8")
    with pytest.raises(SystemExit):
        db.db_migrate(True, False, existing, None)


def test_legacy_backup_and_restore_paths(
    configured: dict[str, str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_log_operation", lambda *_args, **_kwargs: None)
    backup = tmp_path / "backup.sql"

    def dump_ok(command, **_kwargs):
        Path(command[command.index("-f") + 1]).write_bytes(b"x" * 2048)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(db.subprocess, "run", dump_ok)
    db.db_backup(backup_path=backup, data_only=True)
    assert backup.stat().st_size == 2048

    monkeypatch.setattr(
        db.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 2, "", "bad"),
    )
    with pytest.raises(typer.Exit):
        db.db_backup(backup_path=backup, data_only=False)
    monkeypatch.setattr(
        db.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    with pytest.raises(typer.Exit):
        db.db_backup(backup_path=backup, data_only=False)

    missing = tmp_path / "missing.sql"
    with pytest.raises(typer.Exit):
        db.db_restore(input_file=missing, confirm_target=None)
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    monkeypatch.setattr(db, "_get_table_counts", lambda *_args: {"generic_template": 1})
    db.db_restore(input_file=backup, confirm_target=None)
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (False, "bad"))
    with pytest.raises(typer.Exit):
        db.db_restore(input_file=backup, confirm_target=None)


def test_template_validation_seed_and_adapter_helpers(
    configured: dict[str, str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    template = {
        "category": "generic",
        "type": "sample",
        "subtype": "dna",
        "version": "1.0",
        "name": "DNA",
    }
    assert db._template_code(template) == "generic/sample/dna/1.0/"
    assert db._template_key(template) == ("generic", "sample", "dna", "1.0")
    monkeypatch.setattr(db, "_loader_load_template_configs", lambda dirs: [template])
    monkeypatch.setattr(db, "_loader_find_duplicate_template_keys", lambda items: {})
    monkeypatch.setattr(
        db, "_loader_validate_template_configs", lambda dirs, strict: ([template], [])
    )
    assert db._load_template_configs(tmp_path) == [template]
    assert db._find_duplicate_template_keys([template]) == {}
    assert db._validate_template_configs(tmp_path, strict=True) == ([template], [])

    monkeypatch.setattr(db, "_resolve_seed_config_dirs", lambda _path: [tmp_path])
    with pytest.raises(typer.Exit) as valid_json:
        db.db_validate_config(tmp_path, strict=True, json_output=True)
    assert valid_json.value.exit_code == 0
    db.db_validate_config(tmp_path, strict=False, json_output=False)

    issue_warning = db._ConfigIssue("warning", "warn", "f.json", "code")
    issue_error = db._ConfigIssue("error", "bad", "f.json", "code")
    monkeypatch.setattr(
        db,
        "_validate_template_configs",
        lambda *_args, **_kwargs: ([template], [issue_warning, issue_error]),
    )
    with pytest.raises(typer.Exit):
        db.db_validate_config(tmp_path, strict=True, json_output=False)
    with pytest.raises(typer.Exit):
        db.db_validate_config(tmp_path, strict=True, json_output=True)

    monkeypatch.setattr(
        db, "_validate_template_configs", lambda *_args, **_kwargs: ([template], [])
    )
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_schema_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_find_duplicate_template_keys", lambda *_args: {})
    db.db_seed(tmp_path, include_workflow=False, skip_existing=True, dry_run=True)

    summary = SimpleNamespace(inserted=1, updated=1, skipped=2, prefixes_ensured=3)
    monkeypatch.setattr(
        db, "_tapdb_connection_for_env", lambda *_args, **_kwargs: _TapdbConnection([])
    )
    monkeypatch.setattr(db, "_loader_seed_templates", lambda *_args, **_kwargs: summary)
    monkeypatch.setattr(db, "_loader_find_tapdb_core_config_dir", lambda: tmp_path)
    monkeypatch.setattr(db, "_log_operation", lambda *_args, **_kwargs: None)
    db.db_seed(tmp_path, include_workflow=True, skip_existing=False, dry_run=False)

    monkeypatch.setattr(
        db, "_validate_template_configs", lambda *_args, **_kwargs: ([], [])
    )
    db.db_seed(tmp_path, False, True, False)
    monkeypatch.setattr(
        db,
        "_resolve_seed_config_dirs",
        lambda _path: (_ for _ in ()).throw(FileNotFoundError("none")),
    )
    with pytest.raises(SystemExit):
        db.db_seed(tmp_path, False, True, False)


def test_default_admin_setup_and_public_adapters(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    configured["safety_tier"] = "production"
    assert db._create_default_admin(db.Environment.target, True) is False
    configured["safety_tier"] = "shared"

    fake = _TapdbConnection([])
    monkeypatch.setattr(db, "TAPDBConnection", lambda **_kwargs: fake)
    import daylily_tapdb.user_store as user_store

    monkeypatch.setattr(
        user_store,
        "create_or_get",
        lambda *_args, **_kwargs: (SimpleNamespace(username="tapdb_admin"), True),
    )
    assert db._create_default_admin(db.Environment.target, True) is True
    monkeypatch.setattr(
        user_store,
        "create_or_get",
        lambda *_args, **_kwargs: (SimpleNamespace(username="tapdb_admin"), False),
    )
    assert db._create_default_admin(db.Environment.target, True) is False
    monkeypatch.setattr(
        db,
        "TAPDBConnection",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    assert db._create_default_admin(db.Environment.target, True) is False

    events: list[tuple[str, object]] = []
    monkeypatch.setattr(db, "db_create", lambda owner: events.append(("create", owner)))
    monkeypatch.setattr(
        db,
        "db_delete",
        lambda confirm_target: events.append(("delete", confirm_target)),
    )
    monkeypatch.setattr(
        db,
        "db_schema_apply",
        lambda reinitialize: events.append(("apply", reinitialize)),
    )
    monkeypatch.setattr(db, "db_status", lambda: events.append(("status", None)))
    monkeypatch.setattr(
        db, "db_nuke", lambda confirm_target: events.append(("reset", confirm_target))
    )
    monkeypatch.setattr(db, "db_seed", lambda **kwargs: events.append(("seed", kwargs)))
    db.create_database(owner="owner")
    db.delete_database(confirm_target="label")
    db.apply_schema(reinitialize=True)
    db.schema_status()
    db.reset_schema(confirm_target="label")
    db.seed_templates(
        config_path=Path("/tmp/config"),
        include_workflow=True,
        skip_existing=False,
        dry_run=True,
    )
    assert {name for name, _ in events} == {
        "create",
        "delete",
        "apply",
        "status",
        "reset",
        "seed",
    }


def test_setup_local_aurora_and_recreate_paths(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    monkeypatch.setattr(db, "db_create", lambda **_kwargs: events.append("create"))
    monkeypatch.setattr(db, "db_delete", lambda **_kwargs: events.append("delete"))
    monkeypatch.setattr(
        db, "db_schema_apply", lambda **_kwargs: events.append("schema")
    )
    monkeypatch.setattr(db, "db_seed", lambda **_kwargs: events.append("seed"))
    monkeypatch.setattr(db, "_create_default_admin", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        db, "_get_connection_string", lambda *_args: "postgresql://safe"
    )
    monkeypatch.setattr(
        db, "_log_operation", lambda *_args, **_kwargs: events.append("log")
    )
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    db.db_setup(
        recreate=False,
        confirm_target=None,
        include_workflow=False,
        insecure_dev_defaults=True,
    )
    db.db_setup(
        recreate=True,
        confirm_target="label",
        include_workflow=True,
        insecure_dev_defaults=True,
    )
    assert "delete" in events

    configured["engine_type"] = "aurora"
    with pytest.raises(typer.Exit):
        db.db_setup(
            recreate=True,
            confirm_target=None,
            include_workflow=False,
            insecure_dev_defaults=False,
        )


def test_remaining_database_refusal_branches(
    configured: dict[str, str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "one.sql").write_text("SELECT 1", encoding="utf-8")
    monkeypatch.setattr(db, "_find_schema_root", lambda **_kwargs: tmp_path)
    responses = iter([(True, ""), (False, "insert failed")])
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: next(responses))
    with pytest.raises(RuntimeError, match="insert failed"):
        db._write_migration_baseline(db.Environment.target)

    monkeypatch.setattr(
        db, "_run_psql", lambda *_args, **_kwargs: (False, "sequence failed")
    )
    with pytest.raises(RuntimeError, match="sequence failed"):
        db._ensure_instance_prefix_sequence(db.Environment.target, "SMP")

    monkeypatch.setattr(
        db,
        "_find_schema_root",
        lambda **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    with pytest.raises(FileNotFoundError, match="tapdb_schema"):
        db._find_schema_file()

    monkeypatch.setattr(
        db,
        "_run_psql",
        lambda *_args, **_kwargs: (False, 'role "tapdb" does not exist'),
    )
    with pytest.raises(RuntimeError, match="does not exist"):
        db._ensure_local_role(db.Environment.target, "tapdb")

    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: False)
    with pytest.raises(SystemExit):
        db.db_migrate(True, False, tmp_path / "missing-db.json", None)
    with pytest.raises(typer.Exit):
        db.db_backup(None, False)

    backup = tmp_path / "backup.sql"
    backup.write_text("SELECT 1", encoding="utf-8")
    with pytest.raises(typer.Exit):
        db.db_restore(backup, None)

    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_schema_exists", lambda *_args: False)
    with pytest.raises(SystemExit):
        db.db_migrate(True, False, tmp_path / "missing-schema.json", None)
    with pytest.raises(SystemExit):
        db.db_seed(tmp_path, False, True, False)

    monkeypatch.setattr(db, "_schema_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_resolve_seed_config_dirs", lambda _path: [tmp_path])
    warning = db._ConfigIssue("warning", "warn", "warn.json", "code")
    error = db._ConfigIssue("error", "bad", "bad.json", "code")
    monkeypatch.setattr(
        db,
        "_validate_template_configs",
        lambda *_args, **_kwargs: ([{"category": "generic"}], [warning, error]),
    )
    with pytest.raises(SystemExit):
        db.db_seed(tmp_path, False, True, False)

    monkeypatch.setattr(
        db,
        "_validate_template_configs",
        lambda *_args, **_kwargs: ([{"category": "generic"}], [warning]),
    )
    monkeypatch.setattr(
        db,
        "_find_duplicate_template_keys",
        lambda *_args: {("generic", "sample", "dna", "1"): ["a.json", "b.json"]},
    )
    with pytest.raises(SystemExit):
        db.db_seed(tmp_path, False, True, False)


def test_backup_default_path_password_and_schema_apply_failures(
    configured: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(db, "_check_db_exists", lambda *_args: True)
    configured["password"] = "hidden"
    seen: dict[str, object] = {}

    def dump(command, **kwargs):
        seen["env"] = kwargs["env"]
        Path(command[command.index("-f") + 1]).write_bytes(b"small")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(db.subprocess, "run", dump)
    monkeypatch.setattr(db, "_log_operation", lambda *_args, **_kwargs: None)
    db.db_backup(None, False)
    assert seen["env"]["PGPASSWORD"] == "hidden"  # type: ignore[index]

    schema = tmp_path / "tapdb_schema.sql"
    rls = tmp_path / "rls.sql"
    schema.write_text("SELECT 1", encoding="utf-8")
    rls.write_text("SELECT 2", encoding="utf-8")
    monkeypatch.setattr(db, "_ensure_dirs", lambda: None)
    monkeypatch.setattr(db, "_find_schema_file", lambda: schema)
    monkeypatch.setattr(db, "_ensure_schema_exists", lambda *_args: None)
    monkeypatch.setattr(db, "_schema_exists", lambda *_args: True)
    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (False, "apply"))
    with pytest.raises(typer.Exit):
        db.db_schema_apply(False)

    monkeypatch.setattr(db, "_run_psql", lambda *_args, **_kwargs: (True, ""))
    monkeypatch.setattr(
        db,
        "_sync_identity_prefix_config",
        lambda *_args: (_ for _ in ()).throw(ValueError("prefix")),
    )
    with pytest.raises(typer.Exit):
        db.db_schema_apply(False)
