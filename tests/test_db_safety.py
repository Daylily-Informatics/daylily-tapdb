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
        "category": "generic",
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
        overwrite=True,
    )

    assert outcome == "inserted"
    assert created in session.added
    assert created.instance_prefix == "AGX"
    assert created.category == "generic"
    assert created.type == "tube"
    assert created.subtype == "micro"
    assert created.version == "1.0"
    assert session.flush_count == 1


def test_upsert_template_overwrite_false_skips_existing():
    import daylily_tapdb.templates.loader as m

    existing = SimpleNamespace(
        name="existing",
        polymorphic_discriminator="generic_template",
        category="generic",
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
        session, template=_template_payload(), overwrite=False
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
        category="generic",
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

    monkeypatch.setattr(m, "_get_db_config", lambda env: {"database": "tapdb"})
    monkeypatch.setattr(m, "_check_db_exists", lambda env, db: True)
    monkeypatch.setattr(m, "_schema_exists", lambda env: True)

    calls: list[dict] = []

    def fake_run_psql(env, *, sql=None, file: Path | None = None):
        calls.append({"sql": sql, "file": file})
        if sql and "SELECT filename FROM _tapdb_migrations" in sql:
            return True, "001_test.sql\n"
        return True, ""

    monkeypatch.setattr(m, "_run_psql", fake_run_psql)

    m.db_migrate(m.Environment.dev, dry_run=False)

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
    monkeypatch.setattr(m, "_get_db_config", lambda env: {"database": "tapdb"})
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

    m.db_migrate(m.Environment.dev, dry_run=False)

    assert any(c["file"] == migration_file for c in calls)


# ---------------------------------------------------------------------------
# M5: _ensure_instance_prefix_sequence rejects non-alpha prefixes
# ---------------------------------------------------------------------------


def test_ensure_instance_prefix_sequence_rejects_non_alpha():
    import pytest

    import daylily_tapdb.cli.db as m

    with pytest.raises(ValueError, match="letters only"):
        m._ensure_instance_prefix_sequence(m.Environment.dev, "AB123")

    with pytest.raises(ValueError, match="cannot be empty"):
        m._ensure_instance_prefix_sequence(m.Environment.dev, "")

    with pytest.raises(ValueError, match="cannot be empty"):
        m._ensure_instance_prefix_sequence(m.Environment.dev, "   ")


def test_ensure_instance_prefix_sequence_quotes_sql(monkeypatch):
    """Verify sequence names are double-quoted in SQL."""
    import daylily_tapdb.cli.db as m

    captured = {}

    def fake_run_psql(env, *, sql=None, file=None):
        captured["sql"] = sql
        return True, ""

    monkeypatch.setattr(m, "_run_psql", fake_run_psql)
    m._ensure_instance_prefix_sequence(m.Environment.dev, "AGX")

    sql = captured["sql"]
    assert '"agx_instance_seq"' in sql


def test_prepare_seed_templates_rewrites_core_placeholder(tmp_path: Path):
    import daylily_tapdb.templates.loader as m

    core_dir = tmp_path / "core"
    client_dir = tmp_path / "client"
    core_file = core_dir / "generic" / "generic.json"
    client_file = client_dir / "generic" / "client.json"
    core_file.parent.mkdir(parents=True)
    client_file.parent.mkdir(parents=True)

    prepared = m._prepare_seed_templates(
        [
            {
                **_template_payload(),
                "instance_prefix": "GX",
                "_source_file": str(core_file),
            },
            {
                **_template_payload(),
                "subtype": "client",
                "instance_prefix": "QGX",
                "_source_file": str(client_file),
            },
        ],
        core_config_dir=core_dir,
        core_instance_prefix="AGX",
    )

    assert prepared[0]["instance_prefix"] == "AGX"
    assert prepared[1]["instance_prefix"] == "QGX"


def test_prepare_seed_templates_rejects_client_reserved_prefix(tmp_path: Path):
    import daylily_tapdb.templates.loader as m

    client_dir = tmp_path / "client"
    client_file = client_dir / "generic" / "client.json"
    client_file.parent.mkdir(parents=True)

    with pytest.raises(ValueError, match="cannot persist reserved TapDB core"):
        m._prepare_seed_templates(
            [
                {
                    **_template_payload(),
                    "instance_prefix": "GX",
                    "_source_file": str(client_file),
                }
            ],
            core_config_dir=tmp_path / "core",
            core_instance_prefix="AGX",
        )
