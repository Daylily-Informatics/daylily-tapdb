"""DB CLI unit coverage for the explicit-target model."""

from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
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
        "  tenant_id: 00000000-0000-4000-8000-000000000001\n"
        "  operator:\n"
        "    user: tapdb_operator\n"
        "    password: operator-password\n"
        "    secret_arn: ''\n"
        "    iam_auth: false\n"
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
    assert cfg["tenant_id"] == "00000000-0000-4000-8000-000000000001"
    assert cfg["operator_user"] == "tapdb_operator"
    assert cfg["operator_configured"] is True
    assert db_mod._get_schema_name(db_mod.Environment.target) == "tapdb_testdb"


def test_required_identity_prefixes_are_governance_backed() -> None:
    prefixes = db_mod._required_identity_prefixes(db_mod.Environment.target)

    assert prefixes["generic_template"] == "TPX"
    assert prefixes["generic_instance_lineage"] == "EDG"
    assert prefixes["audit_log"] == "ADT"


def test_identity_prefix_sync_uses_core_authority_in_client_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = db_mod._get_db_config(db_mod.Environment.target)
    cfg["owner_repo_name"] = "client-service"
    seen: dict[str, object] = {}

    class FakeGovernance:
        domain_code = "Z"

        def require_prefix(self, prefix: str) -> None:
            seen.setdefault("prefixes", []).append(prefix)  # type: ignore[union-attr]

    def fake_load(**kwargs):
        seen["governance"] = kwargs
        return FakeGovernance()

    monkeypatch.setattr(db_mod, "_get_db_config", lambda env: cfg)
    monkeypatch.setattr(db_mod.GovernanceContext, "load", fake_load)
    monkeypatch.setattr(
        db_mod,
        "_run_psql",
        lambda env, **kwargs: (seen.update(sql=kwargs["sql"]) or True, ""),
    )

    db_mod._sync_identity_prefix_config(db_mod.Environment.target)

    governance = seen["governance"]
    assert isinstance(governance, dict)
    assert governance["owner_repo_name"] == "daylily-tapdb"
    assert seen["prefixes"] == ["TPX", "EDG", "ADT"]
    assert "'client-service'" in str(seen["sql"])
    assert "'daylily-tapdb'" not in str(seen["sql"])


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


def test_db_schema_apply_uses_explicit_target(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, _explicit_target: Path
) -> None:
    calls: list[tuple[str, object]] = []

    schema_file = tmp_path / "schema.sql"
    schema_file.write_text("SELECT 1;\n", encoding="utf-8")
    (tmp_path / "rls.sql").write_text("SELECT 2;\n", encoding="utf-8")

    monkeypatch.setattr(db_mod, "_ensure_dirs", lambda: None)
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda env, db: True)
    monkeypatch.setattr(db_mod, "_find_schema_file", lambda: schema_file)
    monkeypatch.setattr(
        db_mod, "_ensure_schema_exists", lambda env: calls.append(("ensure", env))
    )
    monkeypatch.setattr(db_mod, "_schema_exists", lambda env: False)
    psql_calls: list[dict[str, object]] = []

    def _run_psql(env, **kwargs):
        psql_calls.append(kwargs)
        return True, ""

    monkeypatch.setattr(db_mod, "_run_psql", _run_psql)
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
    schema_sql = str(psql_calls[0]["sql"])
    assert str(_explicit_target) in schema_sql
    assert 'GRANT CONNECT ON DATABASE "tapdb_shared" TO "tapdb"' in schema_sql
    assert "set_config('session.current_config_identity', 'tapdb_testdb'" not in (
        schema_sql
    )


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
    assert seen["tenant_id"] == "00000000-0000-4000-8000-000000000001"
    assert seen["echo_sql"] is False


def test_operator_connection_uses_distinct_credentials_and_privilege_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    class FakeConnection:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr(db_mod, "TAPDBConnection", FakeConnection)

    db_mod._tapdb_connection_for_env(
        db_mod.Environment.target,
        app_username="migration:test",
        connection_role="operator",
    )

    assert seen["db_user"] == "tapdb_operator"
    assert seen["db_pass"] == "operator-password"
    assert seen["connection_role"] == "operator"
    assert db_mod._operator_role_assertion_sql() in db_mod._set_operator_context_sql(
        "tapdb_testdb", db_mod._get_db_config(db_mod.Environment.target)
    )


def test_runtime_psql_context_uses_fixed_config_tenant() -> None:
    sql = db_mod._set_runtime_context_sql(
        "tapdb_testdb", db_mod._get_db_config(db_mod.Environment.target)
    )

    assert (
        "SET session.current_tenant_id = '00000000-0000-4000-8000-000000000001'" in sql
    )
    assert "SET session.allow_global_rows = 'false'" in sql


def test_run_psql_strips_ambient_libpq_and_tapdb_targeting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(command, 0, "1\n", "")

    for key in ("PGHOSTADDR", "PGPORT", "PGOPTIONS", "PGSERVICE", "TAPDB_CONFIG"):
        monkeypatch.setenv(key, "ambient-override")
    monkeypatch.setattr(db_mod.subprocess, "run", fake_run)

    assert db_mod._run_psql(db_mod.Environment.target, sql="SELECT 1") == (True, "1")
    child_env = captured["env"]
    assert isinstance(child_env, dict)
    assert not any(key.startswith(("PG", "TAPDB_")) for key in child_env)
    assert captured["command"][captured["command"].index("-h") + 1] == "localhost"


def test_create_default_admin_skips_without_insecure_flag() -> None:
    assert (
        db_mod._create_default_admin(
            db_mod.Environment.target, insecure_dev_defaults=False
        )
        is False
    )


def test_bootstrap_run_migrations_preflights_then_applies_with_runtime_receipts(
    monkeypatch: pytest.MonkeyPatch, _explicit_target: Path
) -> None:
    calls: list[dict[str, object]] = []

    def fake_db_migrate(**kwargs) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(db_mod, "db_migrate", fake_db_migrate)

    db_mod.run_migrations(env=db_mod.Environment.target, dry_run=False)

    runtime_dir = (
        _explicit_target.resolve().parent / "runtime" / "migrations" / "receipts"
    )
    preflight = runtime_dir / "bootstrap-migrate-000001-preflight.json"
    result = runtime_dir / "bootstrap-migrate-000001-result.json"
    assert calls == [
        {
            "dry_run": True,
            "apply": False,
            "receipt": preflight,
            "preflight_receipt": None,
        },
        {
            "dry_run": False,
            "apply": True,
            "receipt": result,
            "preflight_receipt": preflight,
        },
    ]
    assert preflight.is_absolute()
    assert result.is_absolute()
    assert preflight != result


def test_bootstrap_run_migrations_propagates_preflight_failure_before_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def refusing_db_migrate(**kwargs) -> None:
        calls.append(kwargs)
        raise SystemExit(7)

    monkeypatch.setattr(db_mod, "db_migrate", refusing_db_migrate)

    with pytest.raises(SystemExit) as raised:
        db_mod.run_migrations(env=db_mod.Environment.target, dry_run=False)

    assert raised.value.code == 7
    assert len(calls) == 1
    assert calls[0]["dry_run"] is True
    assert calls[0]["apply"] is False


def test_bootstrap_receipt_paths_preserve_partial_attempt_and_advance_ordinal(
    _explicit_target: Path,
) -> None:
    runtime_dir = (
        _explicit_target.resolve().parent / "runtime" / "migrations" / "receipts"
    )
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "bootstrap-migrate-000001-preflight.json").write_text(
        "{}\n", encoding="utf-8"
    )

    preflight, result = db_mod._next_bootstrap_migration_receipt_paths()

    assert preflight.name == "bootstrap-migrate-000002-preflight.json"
    assert result.name == "bootstrap-migrate-000002-result.json"


def test_seed_loader_failure_propagates_as_process_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    template = {
        "category": "generic",
        "type": "failure",
        "subtype": "fixture",
        "version": "1.0",
        "name": "Failure fixture",
    }

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        @contextmanager
        def session_scope(self, *, commit: bool):
            assert commit is True
            yield object()

    monkeypatch.setattr(
        db_mod, "_resolve_seed_config_dirs", lambda config_path: [tmp_path]
    )
    monkeypatch.setattr(db_mod, "_check_db_exists", lambda env, database: True)
    monkeypatch.setattr(db_mod, "_schema_exists", lambda env: True)
    monkeypatch.setattr(
        db_mod,
        "_validate_template_configs",
        lambda directories, strict: ([template], []),
    )
    monkeypatch.setattr(db_mod, "_find_duplicate_template_keys", lambda rows: {})
    monkeypatch.setattr(
        db_mod, "_tapdb_connection_for_env", lambda *args, **kwargs: FakeConnection()
    )
    monkeypatch.setattr(db_mod, "_loader_find_tapdb_core_config_dir", lambda: tmp_path)

    def fail_seed(*args, **kwargs):
        raise ValueError("missing governed seed prefix")

    monkeypatch.setattr(db_mod, "_loader_seed_templates", fail_seed)

    with pytest.raises(SystemExit) as raised:
        db_mod.seed_templates(
            env=db_mod.Environment.target,
            config_path=None,
            include_workflow=False,
            skip_existing=True,
            dry_run=False,
        )

    assert raised.value.code == 1
