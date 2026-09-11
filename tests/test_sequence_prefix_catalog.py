"""Exact optional-generator catalog mappings, without allocator state changes."""

from __future__ import annotations

import re
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "schema" / "tapdb_schema.sql"
MIGRATION = (
    ROOT / "schema" / "migrations" / "20260910_220000_sequence_prefix_bindings.sql"
)
BINDINGS = {
    "wx_instance_seq": "WX",
    "wsx_instance_seq": "WSX",
    "xx_instance_seq": "XX",
    "ay_instance_seq": "AY",
    "msg_instance_seq": "MSG",
}


def _sequence_section() -> str:
    return BASE.read_text(encoding="utf-8").split("-- SESSION CONTEXT HELPERS", 1)[0]


def _binding_block(sql: str) -> str:
    return sql.split("DO $tapdb_prefix_binding$", 1)[1].split(
        "$tapdb_prefix_binding$;", 1
    )[0]


def test_packaged_base_and_migration_use_identical_catalog_authority():
    base = _binding_block(_sequence_section())
    migration = _binding_block(MIGRATION.read_text(encoding="utf-8"))
    assert base == migration
    assert dict(re.findall(r"\('([a-z_]+)', '([A-Z]+)'\)", base)) == BINDINGS
    assert "'tapdb-prefix-binding/v1:' || binding.prefix" in base
    assert "COMMENT ON SEQUENCE %I.%I IS %L" in base
    assert "pg_catalog.obj_description(c.oid, 'pg_class')" in base


def test_packaged_migration_neither_creates_nor_advances_generators():
    sql = MIGRATION.read_text(encoding="utf-8")
    assert not re.search(
        r"CREATE\s+SEQUENCE|ALTER\s+SEQUENCE|setval\(|nextval\(", sql, re.I
    )
    assert "IF NOT FOUND THEN" in sql
    assert "CONTINUE;" in sql
    assert "refusing to overwrite existing catalog metadata" in sql


def test_packaged_fresh_schema_preserves_original_declarations_and_inline_comments():
    sql = _sequence_section()
    declarations = {
        "wx_instance_seq": "WX (workflow)",
        "wsx_instance_seq": "WSX (workflow_step)",
        "xx_instance_seq": "XX (action)",
        "ay_instance_seq": "AY (assay)",
        "msg_instance_seq": "MSG (system messages)",
    }
    for name, comment in declarations.items():
        match = re.search(
            rf"CREATE SEQUENCE IF NOT EXISTS {name};\s+-- {re.escape(comment)}",
            sql,
        )
        assert match
        assert match.end() < sql.index("DO $tapdb_prefix_binding$")


@pytest.fixture
def catalog(pg_instance):
    engine = create_engine(pg_instance["operator_dsn"])
    schema = "prefix_catalog_" + uuid4().hex
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(text(f'CREATE SCHEMA "{schema}"'))
                connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
                yield connection, schema, pg_instance
            finally:
                transaction.rollback()
    finally:
        engine.dispose()


def _migrate(connection) -> None:
    connection.execute(text(MIGRATION.read_text(encoding="utf-8")))


def _comments(connection, schema) -> dict[str, str | None]:
    return dict(
        connection.execute(
            text(
                "SELECT c.relname, pg_catalog.obj_description(c.oid, 'pg_class') "
                "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                "ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema ORDER BY c.relname"
            ),
            {"schema": schema},
        ).all()
    )


def test_pg_fresh_schema_records_five_exact_bindings_without_allocating(catalog):
    connection, schema, _ = catalog
    connection.execute(text(_sequence_section()))
    assert _comments(connection, schema) == {
        name: f"tapdb-prefix-binding/v1:{prefix}" for name, prefix in BINDINGS.items()
    }
    for name in BINDINGS:
        assert tuple(
            connection.execute(
                text(f'SELECT last_value, is_called FROM "{name}"')
            ).one()
        ) == (1, False)


def test_pg_migration_preserves_absence_other_schemas_and_allocator_state(catalog):
    connection, schema, _ = catalog
    adjacent_schema = schema + "_adjacent"
    connection.execute(text('CREATE SEQUENCE "wx_instance_seq"'))
    connection.execute(text(f'CREATE SCHEMA "{adjacent_schema}"'))
    connection.execute(text(f'CREATE SEQUENCE "{adjacent_schema}"."wsx_instance_seq"'))
    connection.execute(text("SELECT nextval('wx_instance_seq')"))
    connection.execute(text("SELECT nextval('wx_instance_seq')"))
    before = connection.execute(
        text("SELECT last_value, is_called FROM wx_instance_seq")
    ).one()

    _migrate(connection)
    _migrate(connection)

    assert _comments(connection, schema) == {
        "wx_instance_seq": "tapdb-prefix-binding/v1:WX"
    }
    assert _comments(connection, adjacent_schema) == {"wsx_instance_seq": None}
    assert (
        connection.execute(
            text("SELECT last_value, is_called FROM wx_instance_seq")
        ).one()
        == before
    )


@pytest.mark.parametrize(
    "comment",
    [
        "application-owned generator",
        "tapdb-prefix-binding/v1:AY",
        "tapdb-prefix-binding/v1:wx",
        "tapdb-prefix-binding/v2:WX",
        "tapdb-prefix-binding/v1:WX ",
    ],
)
def test_pg_migration_refuses_every_noncanonical_existing_comment(catalog, comment):
    connection, schema, _ = catalog
    connection.execute(text("CREATE SEQUENCE wx_instance_seq"))
    connection.execute(
        text("COMMENT ON SEQUENCE wx_instance_seq IS :comment"), {"comment": comment}
    )

    with pytest.raises(
        DBAPIError, match="refusing to overwrite existing catalog metadata"
    ):
        with connection.begin_nested():
            _migrate(connection)

    assert _comments(connection, schema) == {"wx_instance_seq": comment}


def test_pg_empty_comment_is_catalog_absence_before_annotation(catalog):
    connection, schema, _ = catalog
    connection.execute(text("CREATE SEQUENCE wx_instance_seq"))
    connection.execute(text("COMMENT ON SEQUENCE wx_instance_seq IS ''"))
    assert _comments(connection, schema) == {"wx_instance_seq": None}

    _migrate(connection)

    assert _comments(connection, schema) == {
        "wx_instance_seq": "tapdb-prefix-binding/v1:WX"
    }


@pytest.mark.parametrize(
    "ddl, error",
    [
        ("CREATE TABLE wx_instance_seq (uid bigint)", "expected a sequence"),
        ("CREATE SEQUENCE wx_instance_seq AS integer", "permanent bigint sequence"),
        ("CREATE UNLOGGED SEQUENCE wx_instance_seq", "permanent bigint sequence"),
        ("CREATE SEQUENCE wx_instance_seq START 2", "permanent bigint sequence"),
        ("CREATE SEQUENCE wx_instance_seq INCREMENT 2", "permanent bigint sequence"),
        ("CREATE SEQUENCE wx_instance_seq MINVALUE 0", "permanent bigint sequence"),
        ("CREATE SEQUENCE wx_instance_seq MAXVALUE 1000", "permanent bigint sequence"),
        ("CREATE SEQUENCE wx_instance_seq CACHE 2", "permanent bigint sequence"),
        ("CREATE SEQUENCE wx_instance_seq CYCLE", "permanent bigint sequence"),
        (
            "CREATE TABLE anchor (uid bigint); "
            "CREATE SEQUENCE wx_instance_seq OWNED BY anchor.uid",
            "must be standalone",
        ),
    ],
)
def test_pg_migration_rejects_unsafe_catalog_shapes(catalog, ddl, error):
    connection, schema, _ = catalog
    connection.execute(text(ddl))

    with pytest.raises(DBAPIError, match=error):
        with connection.begin_nested():
            _migrate(connection)

    assert _comments(connection, schema)["wx_instance_seq"] is None


def test_pg_migration_rejects_mixed_sequence_ownership(catalog):
    connection, schema, pg_instance = catalog
    connection.execute(text("CREATE SEQUENCE wx_instance_seq"))
    connection.execute(
        text(f'ALTER SEQUENCE wx_instance_seq OWNER TO "{pg_instance["user"]}"')
    )

    with pytest.raises(DBAPIError, match="sequence owner must equal schema owner"):
        with connection.begin_nested():
            _migrate(connection)

    assert _comments(connection, schema) == {"wx_instance_seq": None}


def test_pg_catalog_failure_rolls_back_all_annotations_in_the_statement(catalog):
    connection, schema, _ = catalog
    connection.execute(text("CREATE SEQUENCE wx_instance_seq"))
    connection.execute(text("CREATE SEQUENCE msg_instance_seq CACHE 2"))

    with pytest.raises(DBAPIError, match="permanent bigint sequence"):
        with connection.begin_nested():
            _migrate(connection)

    assert _comments(connection, schema) == {
        "msg_instance_seq": None,
        "wx_instance_seq": None,
    }


def test_pg_fresh_schema_does_not_reclaim_an_unrelated_existing_relation(catalog):
    connection, schema, _ = catalog
    connection.execute(text("CREATE TABLE wx_instance_seq (uid bigint)"))

    with pytest.raises(DBAPIError, match="expected a sequence"):
        with connection.begin_nested():
            connection.execute(text(_sequence_section()))

    assert _comments(connection, schema) == {"wx_instance_seq": None}


def test_pg_catalog_binding_requires_a_resolved_schema(catalog):
    connection, _, _ = catalog
    connection.execute(text("SET LOCAL search_path TO ''"))

    with pytest.raises(DBAPIError, match="requires an explicit existing schema"):
        with connection.begin_nested():
            _migrate(connection)
