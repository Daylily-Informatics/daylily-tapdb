"""Exact tagged TapDB 9.0.9, 9.0.10 and 10.0.0 historical schema evidence.

Bloom's inspected source pins 9.0.10; this does not identify a live deployment.
All EUIDs and UIDs are minted by the owning, persisted TapDB triggers.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import NullPool

from daylily_tapdb.backup import service, verify
from daylily_tapdb.backup.recovery import retained_recovery_state
from daylily_tapdb.backup.source_contract import capture_source_contract
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db_config import get_backup_settings, get_db_config
from daylily_tapdb.migration_identity import (
    _expand_migration_source,
    _strip_transaction_control,
    apply_migration_preflight,
    build_migration_preflight,
    finalize_migration_abort,
    finalize_migration_recovery,
)
from daylily_tapdb.runtime_principal import operator_session
from daylily_tapdb.sequences import (
    acquire_database_writer_fence,
    apply_sequence_advance_plan,
    build_sequence_advance_plan,
    capture_sequence_inventory,
    record_sequence_advance_outcome,
    release_database_writer_fence,
)
from tests.test_backup_historical_assets import (
    RELEASE_COMMIT,
    SOURCE_ASSET_SHA256,
    SOURCE_ASSETS,
)
from tests.test_backup_release_100_assets import (
    SOURCE_ASSET_SHA256 as RELEASE_100_DIGESTS,
)
from tests.test_backup_release_100_assets import SOURCE_ASSETS as RELEASE_100_ASSETS
from tests.test_backup_release_9010_assets import (
    SOURCE_ASSET_SHA256 as RELEASE_9010_DIGESTS,
)
from tests.test_identity_inventory_helpers import verified_source_sequence_mappings


def test_frozen_9_0_9_assets_match_their_exact_release_digests():
    assert RELEASE_COMMIT == "52d5f498dc4751b7e42d346bee2811a14080e5a5"
    assert set(SOURCE_ASSETS) == set(SOURCE_ASSET_SHA256)
    for name, source in SOURCE_ASSETS.items():
        assert hashlib.sha256(source.encode()).hexdigest() == SOURCE_ASSET_SHA256[name]


def test_bloom_pinned_9_0_10_original_assets_are_byte_identical_to_9_0_9():
    assert set(SOURCE_ASSETS) == set(RELEASE_9010_DIGESTS)
    for name, source in SOURCE_ASSETS.items():
        assert hashlib.sha256(source.encode()).hexdigest() == RELEASE_9010_DIGESTS[name]


def test_original_10_0_assets_match_their_released_digests():
    assert set(RELEASE_100_ASSETS) == set(RELEASE_100_DIGESTS)
    for name, source in RELEASE_100_ASSETS.items():
        assert hashlib.sha256(source.encode()).hexdigest() == RELEASE_100_DIGESTS[name]


@pytest.fixture(params=["9.0.9", "9.0.10", "10.0.0"])
def released_source(pg_instance, tmp_path, request):
    release = request.param
    assets = RELEASE_100_ASSETS if release == "10.0.0" else SOURCE_ASSETS
    set_cli_context(config_path=pg_instance["config_path"])
    cfg = dict(get_db_config())
    cfg["schema_name"] = "released_" + release.replace(".", "") + "_" + uuid4().hex[:12]
    settings = dict(get_backup_settings())
    settings["config_dir"] = str(tmp_path)
    settings["storage_uri"] = f"file://{tmp_path / 'store'}"
    source_root = (tmp_path / ("release_" + release)).resolve()
    for name, source in assets.items():
        path = source_root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")
    source_path = source_root / "tapdb_schema.sql"
    pg_engine = create_engine(
        pg_instance["operator_dsn"],
        isolation_level="REPEATABLE READ",
        poolclass=NullPool,
    )
    with pg_engine.begin() as connection:
        schema = cfg["schema_name"]
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
        connection.execute(
            text("SELECT set_config('search_path', :schema, true)"), {"schema": schema}
        )
        connection.exec_driver_sql(assets["tapdb_schema.sql"].replace("%", "%%"))
        for name, source in assets.items():
            if name.startswith("migrations/"):
                connection.exec_driver_sql(
                    _strip_transaction_control(
                        _expand_migration_source(
                            source_root / name, schema_root=source_root
                        )
                    ).replace("%", "%%")
                )
                connection.execute(
                    text("INSERT INTO _tapdb_migrations(filename) VALUES (:name)"),
                    {"name": name.split("/", 1)[1]},
                )
        connection.exec_driver_sql(assets["rls.sql"].replace("%", "%%"))
        for setting, value in {
            "session.current_domain_code": cfg["domain_code"],
            "session.current_owner_repo_name": cfg["owner_repo_name"],
            "session.current_username": "fixture:released-" + release,
            "session.current_tenant_id": "",
        }.items():
            connection.execute(
                text("SELECT set_config(:setting, :value, true)"),
                {"setting": setting, "value": value},
            )
        connection.exec_driver_sql(
            "CREATE SEQUENCE tpx_instance_seq; CREATE SEQUENCE edg_instance_seq; CREATE SEQUENCE adt_instance_seq"
        )
        connection.execute(
            text(
                "INSERT INTO tapdb_identity_prefix_config(entity,domain_code,issuer_app_code,prefix) VALUES "
                "('generic_template',:domain,:owner,'TPX'),('generic_instance_lineage',:domain,:owner,'EDG'),('audit_log',:domain,:owner,'ADT')"
            ),
            {"domain": cfg["domain_code"], "owner": cfg["owner_repo_name"]},
        )
        template_uid = connection.execute(
            text(
                "INSERT INTO generic_template(name,polymorphic_discriminator,category,type,subtype,version,instance_prefix,bstatus,is_singleton) "
                "VALUES ('Historical message','generic_template','message','webhook','event','1.0','MSG','active',false) RETURNING uid"
            )
        ).scalar_one()
        instances = []
        for _ in range(2):
            instances.append(
                connection.execute(
                    text(
                        "INSERT INTO generic_instance(name,polymorphic_discriminator,category,type,subtype,version,template_uid,bstatus,is_deleted) "
                        "VALUES ('Historical instance','generic_instance','message','webhook','event','1.0',:template,'active',:deleted) RETURNING uid"
                    ),
                    {"template": template_uid, "deleted": False},
                ).scalar_one()
            )
            # The owning generators consume a genuine gap, never a made-up ID.
            connection.execute(
                text(
                    "SELECT nextval('generic_instance_uid_seq'), nextval('msg_instance_seq')"
                )
            )
        for deleted in (False, True):
            connection.execute(
                text(
                    "INSERT INTO generic_instance_lineage(name,polymorphic_discriminator,parent_instance_uid,child_instance_uid,relationship_type,is_deleted) "
                    "VALUES ('Historical assertion','generic_instance_lineage',:parent,:child,'historical-proof',:deleted)"
                ),
                {"parent": instances[0], "child": instances[1], "deleted": deleted},
            )
        # The original 10.0 endpoint guard requires live endpoints when the
        # relationship is created. The owning soft-delete operation may follow.
        connection.execute(
            text("UPDATE generic_instance SET is_deleted=true WHERE uid=:uid"),
            {"uid": instances[1]},
        )
        # Historical template drift does not authorize reminting stored instance EUIDs.
        connection.execute(
            text("UPDATE generic_template SET instance_prefix='WX' WHERE uid=:uid"),
            {"uid": template_uid},
        )
        # An explicitly identified extension tests exhaustive physical duplicate preservation.
        connection.exec_driver_sql(
            "CREATE TABLE historical_assertion (parent_uid bigint REFERENCES generic_instance(uid), child_uid bigint REFERENCES generic_instance(uid), assertion text NOT NULL)"
        )
        connection.execute(
            text(
                "INSERT INTO historical_assertion VALUES (:parent,:child,'retained'),(:parent,:child,'retained')"
            ),
            {"parent": instances[0], "child": instances[1]},
        )
        cfg["sequence_mappings"] = verified_source_sequence_mappings(
            connection, schema_name=schema, source_schema_path=source_path
        )
    with pg_engine.begin() as connection:
        contract = capture_source_contract(
            connection,
            schema_name=cfg["schema_name"],
            target=service.inventory_target(cfg),
            source_version=release,
        )
    yield cfg, settings, contract
    pg_engine.dispose()
    clear_cli_context()


def test_released_restore_and_separate_migration_preserve_all_original_rows(
    released_source,
):
    cfg, settings, contract = released_source
    assert contract["source_version_evidence"] == "operator_declared"
    tables = contract["identity_inventory"]["tables"]
    assert ("tapdb_runtime_principal_scope" in tables) == (
        contract["source_version"] == "10.0.0"
    )
    if contract["source_version"] == "10.0.0":
        assert all(
            tables[name]["rls_forced"]
            for name in (
                "generic_template",
                "generic_instance",
                "generic_instance_lineage",
                "audit_log",
            )
        )
    else:
        assert all(not table["rls_forced"] for table in tables.values())
    assert tables["historical_assertion"]["row_count"] == 2
    assert len(tables["generic_instance_lineage"]["rows"]) == 2
    assert {
        row["identity"]["euid_prefix"]
        for row in tables["generic_instance"]["rows"].values()
    } == {"MSG"}
    assert {
        row["identity"]["instance_prefix"]
        for row in tables["generic_template"]["rows"].values()
    } == {"WX"}
    backup = service.create_backup(cfg, settings, source_contract=contract)
    database = "released_restore_" + uuid4().hex[:12]
    restored = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=verify.RestoreOptions(target_database=database),
        recovery_source={"purpose": "isolated_rehearsal"},
    )
    assert restored.ok, restored.to_payload()
    assert restored.principal_binding_required
    restored_cfg = dict(cfg, database=database)
    migrations = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    receipts = Path(settings["config_dir"]) / "migration-receipts"
    with (
        operator_session(cfg, isolation_level="REPEATABLE READ") as control,
        operator_session(restored_cfg, isolation_level="REPEATABLE READ") as connection,
    ):
        with connection.begin():
            preflight = build_migration_preflight(
                connection,
                migrations_dir=migrations,
                target=service.inventory_target(restored_cfg),
                receipts_dir=receipts,
            )
        fence = acquire_database_writer_fence(
            connection,
            control_connection=control,
            inventory=preflight["sequence_inventory"],
            receipts_dir=receipts,
        )
        with connection.begin():
            result = apply_migration_preflight(
                connection,
                migrations_dir=migrations,
                preflight=preflight,
                target=service.inventory_target(restored_cfg),
                writer_fence=fence["fence"],
                receipts_dir=receipts,
            )
        with connection.begin():
            completion = finalize_migration_recovery(
                connection, result, receipts_dir=receipts, writer_fence=fence["fence"]
            )
        release_database_writer_fence(
            connection,
            fence,
            result=completion["allocator_result"],
            receipts_dir=receipts,
            control_connection=control,
        )
    after = result.receipt["postflight"]["identity_inventory"]["tables"]
    for name, table in tables.items():
        if name != "_tapdb_migrations":
            assert set(after[name]["rows"]) == set(table["rows"])
        for key, row in table["rows"].items():
            assert all(
                after[name]["rows"][key]["columns"][column] == digest
                for column, digest in row["columns"].items()
            )
    assert (
        retained_recovery_state(receipts, target=result.receipt["target"])["pending"]
        == {}
    )


def test_failed_migration_retains_consumed_values_after_actual_rollback(
    released_source, tmp_path
):
    cfg, _settings, _contract = released_source
    migrations = tmp_path / "failing-migrations"
    migrations.mkdir()
    (migrations / "20260910_235959_deliberate_abort.sql").write_text(
        "DO $$ BEGIN PERFORM nextval('msg_instance_seq'); "
        "RAISE EXCEPTION 'bounded migration abort'; END $$;\n",
        encoding="utf-8",
    )
    receipts = tmp_path / "abort-receipts"
    # This exact control database is created by the ephemeral initdb fixture.
    control_cfg = dict(cfg, database="postgres")
    with (
        operator_session(control_cfg, isolation_level="REPEATABLE READ") as control,
        operator_session(cfg, isolation_level="REPEATABLE READ") as connection,
    ):
        with connection.begin():
            preflight = build_migration_preflight(
                connection,
                migrations_dir=migrations,
                target=service.inventory_target(cfg),
                receipts_dir=receipts,
            )
        gate = acquire_database_writer_fence(
            connection,
            control_connection=control,
            inventory=preflight["sequence_inventory"],
            receipts_dir=receipts,
        )
        with pytest.raises(DBAPIError, match="bounded migration abort") as failed:
            with connection.begin():
                apply_migration_preflight(
                    connection,
                    migrations_dir=migrations,
                    preflight=preflight,
                    target=service.inventory_target(cfg),
                    writer_fence=gate["fence"],
                    receipts_dir=receipts,
                )
        with connection.begin():
            completion = finalize_migration_abort(
                connection,
                recovery_intent=failed.value.recovery_intent,
                sequence_result=failed.value.sequence_result,
                receipts_dir=receipts,
                writer_fence=gate["fence"],
            )
            observed = capture_sequence_inventory(
                connection,
                schema_name=cfg["schema_name"],
                target=service.inventory_target(cfg),
            )
        assert completion["status"] == "aborted"
        retained = retained_recovery_state(receipts, target=preflight["target"])
        assert retained["pending"] == {}
        old_message = next(
            sequence
            for sequence in preflight["sequences"]
            if sequence["name"] == "msg_instance_seq"
        )
        current_message = next(
            sequence
            for sequence in observed["sequences"]
            if sequence["name"] == "msg_instance_seq"
        )
        assert current_message["allocated_floor"] > old_message["allocated_floor"]
        assert (
            max(
                floor["value"]
                for floor in retained["floors"]
                if floor["name"] == "msg_instance_seq"
            )
            == current_message["allocated_floor"]
        )
        # Reopen only through an explicit, committed shared allocator proof.
        plan = build_sequence_advance_plan(observed, floors=retained["floors"])
        with connection.begin():
            applied = apply_sequence_advance_plan(
                connection, plan, writer_fence=gate["fence"], receipts_dir=receipts
            )
        committed = record_sequence_advance_outcome(
            applied,
            receipts_dir=receipts,
            outcome="committed",
            actor=cfg["operator_user"],
        )
        release_database_writer_fence(
            connection,
            gate,
            result=committed,
            receipts_dir=receipts,
            control_connection=control,
        )
