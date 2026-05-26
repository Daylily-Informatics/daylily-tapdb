from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _FakeUpsertSession:
    def __init__(self, existing=None):
        self.existing = existing
        self.added: list[object] = []
        self.flush_count = 0
        self.statements: list[object] = []

    def execute(self, stmt):
        self.statements.append(stmt)
        return _ScalarResult(self.existing)

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        self.flush_count += 1


def _template_payload():
    return {
        "name": "x",
        "polymorphic_discriminator": "generic_template",
        "category": "AGX",
        "type": "tube",
        "subtype": "micro",
        "version": "1.0",
        "instance_prefix": "AGX",
        "json_addl": {"k": "v"},
    }


def test_upsert_template_inserts_when_missing(monkeypatch):
    import daylily_tapdb.templates.loader as m

    class _FakeTemplate:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    monkeypatch.setattr(
        m, "_template_model_for_discriminator", lambda _disc: _FakeTemplate
    )
    session = _FakeUpsertSession(existing=None)

    outcome, created = m._upsert_template(
        session,
        template={
            **_template_payload(),
            "instance_prefix": "agx",
        },
        domain_code="Z",
        overwrite=True,
    )

    assert outcome == "inserted"
    assert created in session.added
    assert created.instance_prefix == "AGX"
    assert created.category == "AGX"
    assert created.domain_code == "Z"
    assert created.type == "tube"
    assert created.subtype == "micro"
    assert created.version == "1.0"
    assert session.flush_count == 1


def test_upsert_template_overwrite_false_skips_existing():
    import daylily_tapdb.templates.loader as m

    existing = SimpleNamespace(
        name="existing",
        polymorphic_discriminator="generic_template",
        domain_code="Z",
        category="AGX",
        type="tube",
        subtype="micro",
        version="1.0",
        instance_prefix="AGX",
        instance_polymorphic_identity=None,
        json_addl={"k": "old"},
        json_addl_schema=None,
        bstatus="active",
        is_singleton=False,
        is_deleted=False,
    )
    session = _FakeUpsertSession(existing=existing)

    outcome, returned = m._upsert_template(
        session, template=_template_payload(), domain_code="Z", overwrite=False
    )

    assert outcome == "skipped"
    assert returned is existing
    assert existing.name == "existing"
    assert session.flush_count == 0


def test_upsert_template_overwrite_true_updates_existing():
    import daylily_tapdb.templates.loader as m

    existing = SimpleNamespace(
        name="existing",
        polymorphic_discriminator="generic_template",
        domain_code="Z",
        category="AGX",
        type="tube",
        subtype="micro",
        version="1.0",
        instance_prefix="AGX",
        instance_polymorphic_identity=None,
        json_addl={"k": "old"},
        json_addl_schema=None,
        bstatus="inactive",
        is_singleton=False,
        is_deleted=True,
    )
    session = _FakeUpsertSession(existing=existing)

    outcome, returned = m._upsert_template(
        session,
        template={
            **_template_payload(),
            "name": "updated",
            "json_addl": {"k": "new"},
            "bstatus": "active",
        },
        domain_code="Z",
        overwrite=True,
    )

    assert outcome == "updated"
    assert returned is existing
    assert existing.name == "updated"
    assert existing.json_addl == {"k": "new"}
    assert existing.bstatus == "active"
    assert existing.is_deleted is False
    assert session.flush_count == 1


def test_db_migrate_idempotent_when_all_migrations_already_applied(
    tmp_path, monkeypatch
):
    """Safety: if migrations exist but have already been recorded as applied,
    db_migrate should not attempt to apply them again.
    """
    import daylily_tapdb.cli.db as m

    # Point db_migrate's computed migrations_dir at a temp tree.
    fake_cli_dir = tmp_path / "daylily_tapdb" / "cli"
    fake_cli_dir.mkdir(parents=True)
    fake_db_py = fake_cli_dir / "db.py"
    fake_db_py.write_text("# test stub\n")
    monkeypatch.setattr(m, "__file__", str(fake_db_py))

    migrations_dir = tmp_path / "schema" / "migrations"
    migrations_dir.mkdir(parents=True)
    (migrations_dir / "001_test.sql").write_text("SELECT 1;\n")

    monkeypatch.setattr(
        m,
        "_get_db_config",
        lambda env: {"database": "tapdb", "schema_name": "tapdb_app"},
    )
    monkeypatch.setattr(m, "_check_db_exists", lambda env, db: True)
    monkeypatch.setattr(m, "_schema_exists", lambda env: True)

    calls: list[dict] = []

    def fake_run_psql(env, *, sql=None, file: Path | None = None):
        calls.append({"sql": sql, "file": file})
        if sql and "SELECT filename FROM _tapdb_migrations" in sql:
            return True, "001_test.sql\n"
        return True, ""

    monkeypatch.setattr(m, "_run_psql", fake_run_psql)

    m.db_migrate(dry_run=False)

    # Ensure we never attempted to apply a migration file.
    assert not any(c["file"] is not None for c in calls)


def test_db_migrate_uses_installed_data_migrations(tmp_path, monkeypatch):
    """When repo schema is absent, migrations should resolve from data-dir schema."""
    import daylily_tapdb.cli.db as m

    fake_cli_dir = tmp_path / "site-packages" / "daylily_tapdb" / "cli"
    fake_cli_dir.mkdir(parents=True)
    fake_db_py = fake_cli_dir / "db.py"
    fake_db_py.write_text("# test stub\n")
    monkeypatch.setattr(m, "__file__", str(fake_db_py))

    data_root = tmp_path / "py-data"
    migrations_dir = data_root / "schema" / "migrations"
    migrations_dir.mkdir(parents=True)
    migration_file = migrations_dir / "001_test.sql"
    migration_file.write_text("SELECT 1;\n")

    monkeypatch.setattr(m.sysconfig, "get_paths", lambda: {"data": str(data_root)})
    monkeypatch.setattr(
        m,
        "_get_db_config",
        lambda env: {"database": "tapdb", "schema_name": "tapdb_app"},
    )
    monkeypatch.setattr(m, "_check_db_exists", lambda env, db: True)
    monkeypatch.setattr(m, "_schema_exists", lambda env: True)

    calls: list[dict] = []

    def fake_run_psql(env, *, sql=None, file: Path | None = None):
        calls.append({"sql": sql, "file": file})
        if sql and "SELECT filename FROM _tapdb_migrations" in sql:
            return True, ""
        return True, ""

    monkeypatch.setattr(m, "_run_psql", fake_run_psql)
    monkeypatch.setattr(m, "_log_operation", lambda *_args, **_kwargs: None)

    m.db_migrate(dry_run=False)

    assert any(c["file"] == migration_file for c in calls)


def test_db_nuke_drops_outbox_and_inbox_tables_before_scope_functions(monkeypatch):
    import daylily_tapdb.cli.db as m

    monkeypatch.setattr(
        m,
        "_get_db_config",
        lambda _env: {
            "database": "tapdb_dev",
            "host": "localhost",
            "port": "5533",
            "engine_type": "local",
            "schema_name": "tapdb_dev",
        },
    )
    monkeypatch.setattr(m, "_check_db_exists", lambda _env, _db: True)
    monkeypatch.setattr(
        m,
        "_get_table_counts",
        lambda _env: {
            "generic_template": 0,
            "generic_instance": 0,
            "generic_instance_lineage": 0,
            "audit_log": 0,
            "tapdb_identity_prefix_config": 0,
        },
    )
    monkeypatch.setattr(m, "_log_operation", lambda *_args, **_kwargs: None)

    captured: dict[str, str] = {}

    def fake_run_psql(env, *, sql=None, file=None):
        assert env == m.Environment.target
        assert file is None
        captured["sql"] = sql or ""
        return True, ""

    monkeypatch.setattr(m, "_run_psql", fake_run_psql)

    m.db_nuke(confirm_target="None/None/tapdb_dev@tapdb_dev")

    sql = captured["sql"]
    outbox_attempt_drop = "DROP TABLE IF EXISTS outbox_event_attempt CASCADE;"
    outbox_drop = "DROP TABLE IF EXISTS outbox_event CASCADE;"
    inbox_drop = "DROP TABLE IF EXISTS inbox_message CASCADE;"
    migrations_drop = "DROP TABLE IF EXISTS _tapdb_migrations CASCADE;"
    domain_fn_drop = "DROP FUNCTION IF EXISTS tapdb_current_domain_code();"
    app_fn_drop = "DROP FUNCTION IF EXISTS tapdb_current_owner_repo_name();"

    assert outbox_attempt_drop in sql
    assert outbox_drop in sql
    assert inbox_drop in sql
    assert migrations_drop in sql
    assert sql.index(outbox_attempt_drop) < sql.index(outbox_drop)
    assert sql.index(outbox_drop) < sql.index(domain_fn_drop)
    assert sql.index(inbox_drop) < sql.index(domain_fn_drop)
    assert sql.index(migrations_drop) < sql.index(domain_fn_drop)
    assert sql.index(outbox_drop) < sql.index(app_fn_drop)
    assert sql.index(inbox_drop) < sql.index(app_fn_drop)


# ---------------------------------------------------------------------------
# M5: _ensure_instance_prefix_sequence rejects non-Meridian prefixes
# ---------------------------------------------------------------------------


def test_ensure_instance_prefix_sequence_rejects_invalid_prefix():
    import pytest

    import daylily_tapdb.cli.db as m

    with pytest.raises(ValueError, match="must match"):
        m._ensure_instance_prefix_sequence(m.Environment.target, "ABCDE")

    with pytest.raises(ValueError, match="cannot be empty"):
        m._ensure_instance_prefix_sequence(m.Environment.target, "")

    with pytest.raises(ValueError, match="cannot be empty"):
        m._ensure_instance_prefix_sequence(m.Environment.target, "   ")

    m._normalize_instance_prefix("A1")


def test_ensure_instance_prefix_sequence_quotes_sql(monkeypatch):
    """Verify sequence names are double-quoted in SQL."""
    import daylily_tapdb.cli.db as m

    captured = {}

    def fake_run_psql(env, *, sql=None, file=None):
        captured["sql"] = sql
        return True, ""

    monkeypatch.setattr(m, "_run_psql", fake_run_psql)
    m._ensure_instance_prefix_sequence(m.Environment.target, "AGX")

    sql = captured["sql"]
    assert '"agx_instance_seq"' in sql


def test_prepare_seed_templates_accepts_core_operational_templates(tmp_path: Path):
    import daylily_tapdb.templates.loader as m

    core_dir = tmp_path / "core"
    client_dir = tmp_path / "client"
    core_file = core_dir / "system" / "system.json"
    client_file = client_dir / "atlas" / "atlas.json"
    core_file.parent.mkdir(parents=True)
    client_file.parent.mkdir(parents=True)

    prepared = m._prepare_seed_templates(
        [
            {
                "name": "System User",
                "polymorphic_discriminator": "generic_template",
                "category": "SYS",
                "type": "actor",
                "subtype": "system_user",
                "version": "1.0",
                "instance_prefix": "SYS",
                "_source_file": str(core_file),
            },
            {
                **_template_payload(),
                "subtype": "client",
                "_source_file": str(client_file),
            },
        ],
        core_config_dir=core_dir,
    )

    assert prepared[0]["instance_prefix"] == "SYS"
    assert prepared[1]["instance_prefix"] == "AGX"


def test_prepare_seed_templates_rejects_client_reserved_prefix(tmp_path: Path):
    import daylily_tapdb.templates.loader as m

    client_dir = tmp_path / "client"
    client_file = client_dir / "generic" / "client.json"
    client_file.parent.mkdir(parents=True)

    with pytest.raises(ValueError, match="cannot persist reserved TapDB operational"):
        m._prepare_seed_templates(
            [
                {
                    **_template_payload(),
                    "category": "SYS",
                    "instance_prefix": "SYS",
                    "_source_file": str(client_file),
                }
            ],
            core_config_dir=tmp_path / "core",
        )
