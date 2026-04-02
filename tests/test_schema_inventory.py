from __future__ import annotations

import random
import time
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from daylily_tapdb.schema_inventory import (
    TapdbSchemaInventory,
    build_expected_schema_inventory,
    diff_schema_inventory,
    load_live_schema_inventory,
    schema_asset_files,
)
from tests.conftest import resolve_tapdb_test_dsn
from tests.test_integration import _drop_schema, _install_schema


def _repo_schema_root() -> Path:
    return Path(__file__).resolve().parents[1] / "schema"


def test_build_expected_schema_inventory_parses_repo_assets():
    inventory = build_expected_schema_inventory(
        schema_asset_files(_repo_schema_root()),
        dynamic_sequence_name="AGX_INSTANCE_SEQ",
    )

    assert "generic_template" in inventory.tables
    assert "outbox_event" in inventory.tables
    assert "_tapdb_migrations" in inventory.tables
    assert "tenant_id" in inventory.columns["audit_log"]
    assert "agx_instance_seq" in inventory.sequences
    assert "record_insert()" in inventory.functions
    assert "set_audit_log_euid()" in inventory.functions
    assert "audit_update_generic_instance" in inventory.triggers["generic_instance"]
    assert "idx_outbox_event_tenant_created_dt" in inventory.indexes["outbox_event"]


def test_diff_schema_inventory_strict_mode_flags_only_tapdb_owned_extras():
    expected = TapdbSchemaInventory(schema_name="public")
    expected.add_table("generic_template")
    expected.add_column("generic_template", "uid")
    expected.add_column("generic_template", "name")
    expected.add_sequence("agx_instance_seq")
    expected.add_function("tapdb_get_identity_prefix(entity_name text)")
    expected.add_trigger("generic_template", "audit_insert_generic_template")
    expected.add_index("generic_template", "idx_generic_template_type")

    live = TapdbSchemaInventory(schema_name="public")
    live.add_table("generic_template")
    live.add_column("generic_template", "uid")
    live.add_column("generic_template", "extra_col")
    live.add_sequence("zz_instance_seq")
    live.add_sequence("qa_audit_seq")
    live.add_function("tapdb_get_identity_prefix(entity_name text)")
    live.add_function("tapdb_extra_function()")
    live.add_function("overlay_helper()")
    live.add_trigger("generic_template", "audit_insert_generic_template")
    live.add_trigger("generic_template", "unexpected_trigger")
    live.add_table("overlay_table")
    live.add_column("overlay_table", "id")
    live.add_trigger("overlay_table", "overlay_trigger")
    live.add_index("overlay_table", "idx_overlay")

    non_strict = diff_schema_inventory(
        expected,
        live,
        env="dev",
        database="tapdb_dev",
        strict=False,
    )
    assert non_strict.has_drift is True
    assert "generic_template.name" in non_strict.missing["columns"]
    assert "agx_instance_seq" in non_strict.missing["sequences"]
    assert all(not values for values in non_strict.unexpected.values())

    strict = diff_schema_inventory(
        expected,
        live,
        env="dev",
        database="tapdb_dev",
        strict=True,
    )
    assert strict.has_drift is True
    assert "generic_template.extra_col" in strict.unexpected["columns"]
    assert "generic_template.unexpected_trigger" in strict.unexpected["triggers"]
    assert "qa_audit_seq" in strict.unexpected["sequences"]
    assert "tapdb_extra_function()" in strict.unexpected["functions"]
    assert "zz_instance_seq" not in strict.unexpected["sequences"]
    assert "overlay_helper()" not in strict.unexpected["functions"]
    assert strict.unexpected["tables"] == []


def test_live_inventory_and_diff_against_real_postgres(pytestconfig):
    dsn = resolve_tapdb_test_dsn(pytestconfig)

    schema_root = _repo_schema_root()
    schema_sql_path = schema_root / "tapdb_schema.sql"
    schema_name = f"tapdb_drift_{int(time.time())}_{random.randint(1, 1_000_000)}"
    _install_schema(dsn, schema_name, schema_sql_path)

    engine = create_engine(dsn)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f'CREATE SEQUENCE IF NOT EXISTS "{schema_name}"."agx_instance_seq"'
                )
            )

        expected = build_expected_schema_inventory(
            schema_asset_files(schema_root),
            dynamic_sequence_name="agx_instance_seq",
        )

        with Session(engine) as session:
            live = load_live_schema_inventory(session, schema_name=schema_name)
        clean = diff_schema_inventory(
            expected,
            live,
            env="test",
            database="tapdb_test",
            strict=True,
        )
        assert clean.has_drift is False

        with engine.begin() as conn:
            conn.execute(
                text(
                    f"DROP TRIGGER audit_update_generic_instance "
                    f'ON "{schema_name}"."generic_instance"'
                )
            )

        with Session(engine) as session:
            live = load_live_schema_inventory(session, schema_name=schema_name)
        drifted = diff_schema_inventory(
            expected,
            live,
            env="test",
            database="tapdb_test",
            strict=True,
        )
        assert drifted.has_drift is True
        assert (
            "generic_instance.audit_update_generic_instance"
            in drifted.missing["triggers"]
        )
    finally:
        engine.dispose()
        _drop_schema(dsn, schema_name)
