from __future__ import annotations

from pathlib import Path


def test_upsert_template_overwrite_true_is_on_conflict_do_update(monkeypatch):
    import daylily_tapdb.cli.db as m

    captured = {}

    def fake_run_psql(env, *, sql=None, file=None):
        captured["sql"] = sql
        captured["file"] = file
        return True, "t"

    monkeypatch.setattr(m, "_run_psql", fake_run_psql)

    ok, _out = m._upsert_template(
        m.Environment.dev,
        template={
            "name": "x",
            "polymorphic_discriminator": "generic_template",
            "category": "generic",
            "type": "tube",
            "subtype": "micro",
            "version": "1.0",
            "instance_prefix": "GX",
            "json_addl": {"k": "v"},
        },
        overwrite=True,
    )

    assert ok is True
    assert captured["file"] is None
    sql = captured["sql"]
    assert "ON CONFLICT (category, type, subtype, version)" in sql
    assert "DO UPDATE SET" in sql
    assert "RETURNING" in sql


def test_upsert_template_overwrite_false_is_on_conflict_do_nothing(monkeypatch):
    import daylily_tapdb.cli.db as m

    captured = {}

    def fake_run_psql(env, *, sql=None, file=None):
        captured["sql"] = sql
        captured["file"] = file
        return True, "1"

    monkeypatch.setattr(m, "_run_psql", fake_run_psql)

    ok, _out = m._upsert_template(
        m.Environment.dev,
        template={
            "name": "x",
            "polymorphic_discriminator": "generic_template",
            "category": "generic",
            "type": "tube",
            "subtype": "micro",
            "version": "1.0",
            "instance_prefix": "GX",
            "json_addl": {"k": "v"},
        },
        overwrite=False,
    )

    assert ok is True
    assert captured["file"] is None
    sql = captured["sql"]
    assert "ON CONFLICT (category, type, subtype, version)" in sql
    assert "DO NOTHING" in sql
    assert "RETURNING 1" in sql


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
