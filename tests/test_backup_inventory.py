"""State inventory: derived, not enumerated."""

from __future__ import annotations

from pathlib import Path

from daylily_tapdb.backup.errors import (
    BackupError,
    BackupNotFoundError,
    BackupPolicyBlockedError,
    BackupVerificationError,
    BackupVersionMismatchError,
    RestoreConfirmationError,
    RestoreStageStaleError,
)
from daylily_tapdb.backup.inventory import (
    DISPOSITION_CAPTURED,
    DISPOSITION_EXCLUDED,
    DISPOSITION_REFERENCED,
    STATE_INVENTORY,
    categories,
    excluded_state_payload,
    schema_asset_checksums,
    state_inventory_payload,
    summarize_inventory,
)
from daylily_tapdb.backup.manifest import sha256_file
from daylily_tapdb.schema_inventory import (
    TapdbSchemaInventory,
    load_expected_schema_inventory,
    schema_asset_files,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = REPO_ROOT / "schema"


def test_every_category_has_a_known_disposition():
    known = {DISPOSITION_CAPTURED, DISPOSITION_REFERENCED, DISPOSITION_EXCLUDED}

    assert {item.disposition for item in STATE_INVENTORY} <= known
    assert len(STATE_INVENTORY) == len({item.key for item in STATE_INVENTORY})


def test_every_exclusion_states_a_rationale():
    # An operator reading the runbook must learn *why* something is absent.
    excluded = categories(DISPOSITION_EXCLUDED)

    assert excluded, "no exclusions found -- the loop below would prove nothing"
    for item in excluded:
        assert item.rationale, f"{item.key} excludes state without explaining why"


def test_excluded_state_payload_documents_the_known_omissions():
    keys = {entry["key"] for entry in excluded_state_payload()}

    assert {
        "roles_grants",
        "extensions",
        "other_schemas",
        "cluster_settings",
        "config_file",
        "identity_provider_state",
    } <= keys


def test_config_file_is_excluded_because_it_carries_credentials():
    entry = next(i for i in STATE_INVENTORY if i.key == "config_file")

    assert entry.disposition == DISPOSITION_EXCLUDED
    assert "credential" in entry.rationale.lower()


def test_state_inventory_payload_is_json_shaped():
    payload = state_inventory_payload()

    assert len(payload) == len(STATE_INVENTORY)
    for entry in payload:
        assert set(entry) <= {"key", "title", "disposition", "detail", "rationale"}


def test_no_database_object_names_are_hardcoded_in_the_inventory():
    # The inventory describes *categories*. The moment it names a table, it
    # starts rotting the day someone adds one.
    blob = " ".join(
        f"{item.key} {item.title} {item.detail} {item.rationale}"
        for item in STATE_INVENTORY
    )

    for table in ("generic_template", "generic_instance_lineage", "audit_log"):
        assert table not in blob


def test_summarize_inventory_reports_counts_and_names():
    inventory = TapdbSchemaInventory(schema_name="tapdb_test")
    inventory.add_table("generic_instance")
    inventory.add_column("generic_instance", "uid")
    inventory.add_sequence("wx_instance_seq")
    inventory.add_function("tapdb_touch()")
    inventory.add_trigger("generic_instance", "audit_insert_generic_instance")

    summary = summarize_inventory(inventory)

    assert summary["schema_name"] == "tapdb_test"
    assert summary["tables"] == ["generic_instance"]
    assert summary["sequences"] == ["wx_instance_seq"]
    assert summary["counts"]["tables"] == 1
    assert summary["triggers"]["generic_instance"] == ["audit_insert_generic_instance"]


def test_summarize_inventory_omits_tables_without_triggers():
    inventory = TapdbSchemaInventory(schema_name="tapdb_test")
    inventory.add_table("outbox_event")

    assert summarize_inventory(inventory)["triggers"] == {}


def test_schema_asset_checksums_cover_schema_plus_every_migration():
    asset_paths = schema_asset_files(SCHEMA_ROOT)
    entries = schema_asset_checksums(asset_paths)

    assert len(entries) == len(asset_paths)
    assert entries[0]["name"] == "tapdb_schema.sql"
    assert entries[0]["sha256"] == sha256_file(SCHEMA_ROOT / "tapdb_schema.sql")
    assert entries[1]["name"] == "rls.sql"
    assert entries[1]["sha256"] == sha256_file(SCHEMA_ROOT / "rls.sql")
    assert all(entry["sha256"] for entry in entries)
    # Every migration on disk is represented; nothing is hardcoded.
    migration_names = {p.name for p in (SCHEMA_ROOT / "migrations").glob("*.sql")}
    assert migration_names <= {entry["name"] for entry in entries}


def test_missing_assets_are_recorded_rather_than_fatal(tmp_path: Path):
    entries = schema_asset_checksums([tmp_path / "absent.sql"])

    assert entries == [{"name": "absent.sql", "sha256": None}]


def test_expected_inventory_tracks_the_live_schema_source():
    # Guards plan section 3.7: expectations come from the schema assets, so a
    # new table added through the migration path needs no backup-code change.
    expected = load_expected_schema_inventory(
        schema_asset_files(SCHEMA_ROOT),
        dynamic_sequence_name="ax_instance_seq",
    )
    summary = summarize_inventory(expected)

    assert summary["counts"]["tables"] >= 9
    assert "ax_instance_seq" in summary["sequences"]


def test_error_codes_are_stable_and_distinct():
    errors = [
        BackupNotFoundError,
        BackupVerificationError,
        BackupVersionMismatchError,
        RestoreConfirmationError,
        RestoreStageStaleError,
        BackupPolicyBlockedError,
    ]
    codes = [cls.code for cls in errors]

    assert len(set(codes)) == len(codes)
    assert all(issubclass(cls, BackupError) for cls in errors)


def test_error_payload_carries_code_and_detail():
    error = BackupVerificationError("bad checksum", detail={"asset": "tapdb.dump"})

    assert error.to_payload() == {
        "error": "backup_verification_failed",
        "message": "bad checksum",
        "detail": {"asset": "tapdb.dump"},
    }
