import os
import stat
from pathlib import Path

import pytest

import daylily_tapdb.migration_identity as migration_identity
from daylily_tapdb.migration_identity import (
    MigrationPreflightError,
    MigrationReceiptMismatchError,
    _migration_assets,
    _sha256,
    _strip_transaction_control,
    _verify_preservation,
    write_json_receipt,
)


def test_identity_migration_leaves_existing_values_null():
    migration = (
        Path(__file__).resolve().parents[1]
        / "schema"
        / "migrations"
        / "20260902_010000_natural_identity_and_owner_uniqueness.sql"
    ).read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS identity_key VARCHAR(512)" in migration
    assert "UPDATE generic_instance" not in migration
    assert "issuer_app_code" in migration


def test_legacy_outbox_migration_preserves_uuid_and_records_sha256_mapping():
    migration = (
        Path(__file__).resolve().parents[1]
        / "schema"
        / "migrations"
        / "20260902_010100_legacy_outbox_message_conversion.sql"
    ).read_text(encoding="utf-8")
    assert "legacy_row.event_id" in migration
    assert "machine_uuid" in migration
    assert "old_outbox_id" in migration
    assert "old_event_id" in migration
    assert "sha256" in migration
    assert "set_config('TimeZone', 'UTC', true)" in migration
    assert "legacy_row.created_dt" in migration
    assert "tapdb-allow-column: outbox_event.message_uid" not in migration
    assert "tapdb-transformation: outbox_event.message_uid" in migration


def test_transaction_markers_are_removed_before_guarded_execution():
    source = "BEGIN;\nSELECT 1;\nCOMMIT;\n"
    assert _strip_transaction_control(source).strip() == "SELECT 1;"


def test_runner_expands_canonical_rls_and_binds_it_into_asset_fingerprint(
    tmp_path: Path,
):
    schema_root = tmp_path / "schema"
    migrations = schema_root / "migrations"
    migrations.mkdir(parents=True)
    include = schema_root / "rls.sql"
    include.write_text("SELECT 1;\n", encoding="utf-8")
    migration = migrations / "20260902_020000_rls.sql"
    migration.write_text(
        "-- tapdb-allow-column: audit_log.changed_by\n"
        "-- tapdb-transformation: "
        "audit_log.changed_by:null_or_empty_to_pre92_unattributed_v1\n"
        "-- tapdb-include: ../rls.sql\n",
        encoding="utf-8",
    )

    first = _migration_assets(migrations)[0]
    assert first["expanded_source"].endswith("SELECT 1;\n")
    assert "tapdb-include" not in first["expanded_source"]
    assert first["allowed_columns"] == ["audit_log.changed_by"]

    include.write_text("SELECT 2;\n", encoding="utf-8")
    second = _migration_assets(migrations)[0]
    assert second["sha256"] != first["sha256"]


def test_declared_backfills_have_explicit_preservation_allow_markers():
    migrations = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    validator = (
        migrations / "20260612_154200_add_template_validator_ref.sql"
    ).read_text(encoding="utf-8")
    rls = (
        migrations / "20260902_020000_force_rls_and_audit_attribution.sql"
    ).read_text(encoding="utf-8")
    runtime_ddl_guard = (
        migrations / "20260903_031820_runtime_ddl_guard.sql"
    ).read_text(encoding="utf-8")
    tenant_identity = (
        migrations / "20260904_061819_tenant_scoped_natural_identity.sql"
    ).read_text(encoding="utf-8")

    assert "tapdb-allow-column: generic_template.validator_ref" in validator
    assert "tapdb-transformation: generic_template.validator_ref" in validator
    assert "tapdb-allow-column: audit_log.changed_by" in rls
    assert "tapdb-transformation: audit_log.changed_by" in rls
    assert "\\ir" not in rls
    assert "REVOKE CREATE ON SCHEMA %I FROM PUBLIC" in runtime_ddl_guard
    assert "REVOKE CREATE ON SCHEMA %I FROM %I" in runtime_ddl_guard
    assert "pg_catalog.has_schema_privilege" in runtime_ddl_guard
    assert "ON DATABASE" not in runtime_ddl_guard
    assert "tapdb-allow-column:" not in runtime_ddl_guard
    assert "tapdb-allow-new-rows:" not in runtime_ddl_guard
    assert "tapdb-allow-sequence:" not in runtime_ddl_guard
    assert "tapdb-transformation:" not in runtime_ddl_guard
    assert "UPDATE generic_instance" not in tenant_identity
    assert "INSERT INTO" not in tenant_identity
    assert "nextval(" not in tenant_identity
    assert "setval(" not in tenant_identity
    assert "idx_generic_instance_natural_identity_global" in tenant_identity
    assert "idx_generic_instance_natural_identity_tenant" in tenant_identity

    assets = {item["filename"]: item for item in _migration_assets(migrations)}
    assert assets["20260612_154200_add_template_validator_ref.sql"][
        "allowed_transformations"
    ] == ["generic_template.validator_ref:null_or_empty_to_universal_pass_v1"]
    assert assets["20260902_020000_force_rls_and_audit_attribution.sql"][
        "allowed_transformations"
    ] == ["audit_log.changed_by:null_or_empty_to_pre92_unattributed_v1"]
    assert assets["20260902_010100_legacy_outbox_message_conversion.sql"][
        "allowed_transformations"
    ] == ["outbox_event.message_uid:null_to_legacy_mapping_v1"]
    guard_asset = assets["20260903_031820_runtime_ddl_guard.sql"]
    assert guard_asset["allowed_columns"] == []
    assert guard_asset["allowed_new_rows"] == []
    assert guard_asset["allowed_sequences"] == []
    assert guard_asset["allowed_transformations"] == []
    tenant_asset = assets["20260904_061819_tenant_scoped_natural_identity.sql"]
    assert tenant_asset["allowed_columns"] == []
    assert tenant_asset["allowed_new_rows"] == []
    assert tenant_asset["allowed_sequences"] == []
    assert tenant_asset["allowed_transformations"] == []


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (
            "-- tapdb-allow-column: generic_instance.euid\n"
            "-- tapdb-transformation: "
            "outbox_event.message_uid:null_to_legacy_mapping_v1\n",
            "immutable columns cannot be allowlisted",
        ),
        (
            "-- tapdb-allow-column: audit_log.changed_by\n",
            "require an exact transformation contract",
        ),
        (
            "-- tapdb-transformation: arbitrary prose\n",
            "unknown migration transformation contract",
        ),
        (
            "-- tapdb-transformation: "
            "audit_log.changed_by:null_or_empty_to_pre92_unattributed_v1\n",
            "unused migration transformation contract",
        ),
    ],
)
def test_migration_assets_reject_unsafe_or_unpaired_transformations(
    tmp_path: Path, source: str, message: str
):
    migration = tmp_path / "20260902_019999_contract.sql"
    migration.write_text(source + "SELECT 1;\n", encoding="utf-8")

    with pytest.raises(MigrationPreflightError, match=message):
        _migration_assets(tmp_path)


def _row(*, key: int, identity: dict, hashes: dict) -> dict:
    return {
        "key": [key],
        "identity": identity,
        "column_sha256": hashes,
    }


def _preservation_receipt(*, pending: dict, tables: dict) -> dict:
    return {"pending_migrations": [pending], "tables": tables, "sequences": []}


def test_declared_literal_transformation_enforces_exact_old_and_new_values():
    marker = "generic_template.validator_ref:null_or_empty_to_universal_pass_v1"
    pending = {
        "allowed_columns": ["generic_template.validator_ref"],
        "allowed_transformations": [marker],
        "allowed_new_rows": [],
        "allowed_sequences": [],
    }
    before = _preservation_receipt(
        pending=pending,
        tables={
            "generic_template": {
                "rows": [
                    _row(
                        key=1,
                        identity={"uid": 1},
                        hashes={"uid": _sha256(1), "validator_ref": _sha256(None)},
                    )
                ]
            }
        },
    )
    after = _preservation_receipt(
        pending=pending,
        tables={
            "generic_template": {
                "rows": [
                    _row(
                        key=1,
                        identity={"uid": 1},
                        hashes={
                            "uid": _sha256(1),
                            "validator_ref": _sha256("UNIVERSAL_PASS@1"),
                        },
                    )
                ]
            }
        },
    )
    _verify_preservation(before, after)

    after["tables"]["generic_template"]["rows"][0]["column_sha256"]["validator_ref"] = (
        _sha256("UNIVERSAL_PASS@2")
    )
    with pytest.raises(MigrationReceiptMismatchError, match="invalid declared"):
        _verify_preservation(before, after)

    after["tables"]["generic_template"]["rows"][0]["column_sha256"]["validator_ref"] = (
        _sha256(None)
    )
    with pytest.raises(MigrationReceiptMismatchError, match="invalid declared"):
        _verify_preservation(before, after)


def test_declared_literal_transformation_checks_newly_added_column_values():
    marker = "generic_template.validator_ref:null_or_empty_to_universal_pass_v1"
    pending = {
        "allowed_columns": ["generic_template.validator_ref"],
        "allowed_transformations": [marker],
        "allowed_new_rows": [],
        "allowed_sequences": [],
    }
    before = _preservation_receipt(
        pending=pending,
        tables={
            "generic_template": {
                "rows": [
                    _row(
                        key=1,
                        identity={"uid": 1},
                        hashes={"uid": _sha256(1)},
                    )
                ]
            }
        },
    )
    after = _preservation_receipt(
        pending=pending,
        tables={
            "generic_template": {
                "rows": [
                    _row(
                        key=1,
                        identity={"uid": 1},
                        hashes={
                            "uid": _sha256(1),
                            "validator_ref": _sha256("UNIVERSAL_PASS@1"),
                        },
                    )
                ]
            }
        },
    )
    _verify_preservation(before, after)

    after["tables"]["generic_template"]["rows"][0]["column_sha256"]["validator_ref"] = (
        _sha256("WRONG@1")
    )
    with pytest.raises(MigrationReceiptMismatchError, match="newly added"):
        _verify_preservation(before, after)


def test_outbox_transition_is_exact_and_preserves_existing_nonnull_linkage():
    marker = "outbox_event.message_uid:null_to_legacy_mapping_v1"
    pending = {
        "allowed_columns": [],
        "allowed_transformations": [marker],
        "allowed_new_rows": ["generic_instance", "tapdb_legacy_outbox_mapping"],
        "allowed_sequences": [],
    }
    before = _preservation_receipt(
        pending=pending,
        tables={
            "outbox_event": {
                "rows": [
                    _row(
                        key=1,
                        identity={"id": 1, "message_uid": None},
                        hashes={"id": _sha256(1), "message_uid": _sha256(None)},
                    ),
                    _row(
                        key=2,
                        identity={"id": 2, "message_uid": 77},
                        hashes={"id": _sha256(2), "message_uid": _sha256(77)},
                    ),
                ]
            }
        },
    )
    after = _preservation_receipt(
        pending=pending,
        tables={
            "outbox_event": {
                "rows": [
                    _row(
                        key=1,
                        identity={"id": 1, "message_uid": 88},
                        hashes={"id": _sha256(1), "message_uid": _sha256(88)},
                    ),
                    _row(
                        key=2,
                        identity={"id": 2, "message_uid": 77},
                        hashes={"id": _sha256(2), "message_uid": _sha256(77)},
                    ),
                ]
            },
            "tapdb_legacy_outbox_mapping": {
                "rows": [
                    _row(
                        key=1,
                        identity={"old_outbox_id": 1, "message_uid": 88},
                        hashes={},
                    )
                ]
            },
        },
    )
    _verify_preservation(before, after)

    after["tables"]["outbox_event"]["rows"][1]["identity"]["message_uid"] = 99
    after["tables"]["outbox_event"]["rows"][1]["column_sha256"]["message_uid"] = (
        _sha256(99)
    )
    with pytest.raises(MigrationReceiptMismatchError, match="pre-existing.*changed"):
        _verify_preservation(before, after)


def test_outbox_transition_rejects_mapping_mismatch():
    marker = "outbox_event.message_uid:null_to_legacy_mapping_v1"
    pending = {
        "allowed_columns": [],
        "allowed_transformations": [marker],
        "allowed_new_rows": ["generic_instance", "tapdb_legacy_outbox_mapping"],
        "allowed_sequences": [],
    }
    before = _preservation_receipt(
        pending=pending,
        tables={
            "outbox_event": {
                "rows": [
                    _row(
                        key=1,
                        identity={"id": 1, "message_uid": None},
                        hashes={"id": _sha256(1), "message_uid": _sha256(None)},
                    )
                ]
            }
        },
    )
    after = _preservation_receipt(
        pending=pending,
        tables={
            "outbox_event": {
                "rows": [
                    _row(
                        key=1,
                        identity={"id": 1, "message_uid": 88},
                        hashes={"id": _sha256(1), "message_uid": _sha256(88)},
                    )
                ]
            },
            "tapdb_legacy_outbox_mapping": {
                "rows": [
                    _row(
                        key=1,
                        identity={"old_outbox_id": 1, "message_uid": 89},
                        hashes={},
                    )
                ]
            },
        },
    )
    with pytest.raises(MigrationReceiptMismatchError, match="does not match"):
        _verify_preservation(before, after)


def test_verifier_rejects_forged_immutable_allowance_even_without_asset_parsing():
    pending = {
        "allowed_columns": ["generic_instance.euid"],
        "allowed_transformations": [],
        "allowed_new_rows": [],
        "allowed_sequences": [],
    }
    receipt = _preservation_receipt(
        pending=pending,
        tables={"generic_instance": {"rows": []}},
    )
    with pytest.raises(MigrationPreflightError, match="immutable columns"):
        migration_identity._verify_preservation(receipt, receipt)


def test_verifier_rejects_populated_new_columns_on_preexisting_rows():
    pending = {
        "allowed_columns": [],
        "allowed_transformations": [],
        "allowed_new_rows": [],
        "allowed_sequences": [],
    }
    before = _preservation_receipt(
        pending=pending,
        tables={
            "generic_instance": {
                "rows": [
                    _row(
                        key=1,
                        identity={"uid": 1},
                        hashes={"uid": _sha256(1)},
                    )
                ]
            }
        },
    )
    after = _preservation_receipt(
        pending=pending,
        tables={
            "generic_instance": {
                "rows": [
                    _row(
                        key=1,
                        identity={"uid": 1, "identity_key": "forbidden:claim"},
                        hashes={
                            "uid": _sha256(1),
                            "identity_key": _sha256("forbidden:claim"),
                        },
                    )
                ]
            }
        },
    )
    with pytest.raises(MigrationReceiptMismatchError, match="identity_key.*NULL"):
        _verify_preservation(before, after)

    after_row = after["tables"]["generic_instance"]["rows"][0]
    after_row["identity"]["identity_key"] = None
    after_row["column_sha256"]["identity_key"] = _sha256(None)
    after_row["column_sha256"]["new_nonidentity"] = _sha256("backfilled")
    with pytest.raises(
        MigrationReceiptMismatchError, match="new_nonidentity.*without.*contract"
    ):
        _verify_preservation(before, after)

    after_row["column_sha256"]["new_nonidentity"] = _sha256(None)
    _verify_preservation(before, after)


def test_receipt_publish_is_exclusive_fsynced_readonly_and_symlink_safe(
    tmp_path: Path,
):
    receipt = {"receipt_version": "test/v1", "stable": [2, 1]}
    published = (tmp_path / "published.json").resolve()
    write_json_receipt(published, receipt)

    assert published.read_text(encoding="utf-8") == (
        '{\n  "receipt_version": "test/v1",\n  "stable": [\n    2,\n    1\n  ]\n}\n'
    )
    assert stat.S_IMODE(published.stat().st_mode) == 0o444
    with pytest.raises(MigrationPreflightError, match="already exists"):
        write_json_receipt(published, {"different": True})
    assert '"different"' not in published.read_text(encoding="utf-8")

    referent = tmp_path / "referent.json"
    referent.write_text("preserve-me\n", encoding="utf-8")
    symlink = tmp_path / "symlink.json"
    os.symlink(referent, symlink)
    with pytest.raises(MigrationPreflightError, match="already exists"):
        write_json_receipt(symlink, {"different": True})
    assert referent.read_text(encoding="utf-8") == "preserve-me\n"


def test_operator_context_requires_exact_config_identity_without_schema_fallback():
    class RecordingConnection:
        def __init__(self):
            self.settings = []

        def in_transaction(self):
            return True

        def execute(self, _statement, parameters):
            self.settings.append(parameters)

    connection = RecordingConnection()
    with pytest.raises(
        MigrationPreflightError,
        match="missing session.current_config_identity",
    ):
        migration_identity._apply_operator_context(
            connection,
            {
                "schema_name": "same_schema_is_not_config_identity",
                "domain_code": "Z",
                "owner_repo_name": "daylily-tapdb",
            },
        )

    assert not any(
        item.get("name") == "session.current_config_identity"
        and item.get("value") == "same_schema_is_not_config_identity"
        for item in connection.settings
    )
