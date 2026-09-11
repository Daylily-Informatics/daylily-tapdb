"""Identity comparisons cannot turn a conversion declaration into a waiver."""

from __future__ import annotations

from copy import deepcopy

import pytest

from daylily_tapdb.identity_inventory import (
    CONVERSION_VERSION,
    INVENTORY_VERSION,
    IdentityInventoryError,
    content_hash,
    seal_receipt,
    validate_target,
    verify_identity_inventory,
)


def snapshot(values=None):
    values = (
        {"uid": 17, "created_dt": "2026-09-10T20:00:00+00:00", "note": None}
        if values is None
        else values
    )
    columns = [{"name": name} for name in values]
    row = {
        "sha256": content_hash(values),
        "count": 1,
        "columns": {key: content_hash(value) for key, value in values.items()},
        "identity": {"uid": values["uid"]},
    }
    rows = {content_hash([values["uid"]]): row}
    table = {
        "kind": "r",
        "owner": "operator",
        "rls_enabled": False,
        "rls_forced": False,
        "columns": columns,
        "constraints": [],
        "indexes": [],
        "triggers": [],
        "policies": [],
        "dependencies": [],
        "primary_key": ["uid"],
        "immutable_columns": ["uid", "created_dt"],
        "rows": rows,
        "row_count": 1,
        "content_sha256": content_hash(rows),
    }
    return seal_receipt(
        {
            "schema_version": INVENTORY_VERSION,
            "schema_name": "inventory",
            "target": {
                "engine_type": "local",
                "host": "localhost",
                "port": 15438,
                "database": "test",
                "schema_name": "inventory",
                "config_identity": "/tmp/test-config.yaml",
                "domain_code": "Z",
                "owner_repo_name": "test",
            },
            "physical_target": {"database": "test", "database_oid": 1234},
            "tables": {"history": table},
        }
    )


def conversion(**changes):
    return {
        "schema_version": CONVERSION_VERSION,
        "tables": {"history": changes},
        "added_tables": [],
    }


def test_unchanged_capture_verifies_deterministically():
    original = snapshot()
    result = verify_identity_inventory(original, deepcopy(original))
    assert result["ok"] and result["violations"] == []
    assert result == verify_identity_inventory(original, original)


@pytest.mark.parametrize("column", ["uid", "created_dt"])
def test_immutable_columns_cannot_be_declared_convertible(column):
    original = snapshot()
    with pytest.raises(IdentityInventoryError, match="Immutable"):
        verify_identity_inventory(
            original,
            original,
            conversion_manifest=conversion(
                changed_columns={
                    column: {"kind": "null_or_empty_to_literal", "value": "new"}
                }
            ),
        )


def test_nonidentity_changes_need_an_exact_declared_transform():
    before = snapshot()
    after = snapshot(
        {"uid": 17, "created_dt": "2026-09-10T20:00:00+00:00", "note": "verified"}
    )
    assert not verify_identity_inventory(before, after)["ok"]
    declared = conversion(
        changed_columns={
            "note": {"kind": "null_or_empty_to_literal", "value": "verified"}
        }
    )
    assert verify_identity_inventory(before, after, conversion_manifest=declared)["ok"]
    declared["tables"]["history"]["changed_columns"]["note"]["value"] = "different"
    assert not verify_identity_inventory(before, after, conversion_manifest=declared)[
        "ok"
    ]


def test_exact_hash_conversion_only_applies_to_its_original_row():
    before = snapshot()
    after = snapshot(
        {"uid": 17, "created_dt": "2026-09-10T20:00:00+00:00", "note": "mapped"}
    )
    key = next(iter(before["tables"]["history"]["rows"]))
    changed = {
        "note": {
            "kind": "exact_value_hashes",
            "changes": {
                key: {"before": content_hash(None), "after": content_hash("mapped")}
            },
        }
    }
    assert verify_identity_inventory(
        before, after, conversion_manifest=conversion(changed_columns=changed)
    )["ok"]
    changed["note"]["changes"] = {}
    assert not verify_identity_inventory(
        before, after, conversion_manifest=conversion(changed_columns=changed)
    )["ok"]


def test_original_rows_and_columns_cannot_be_removed_even_when_additions_allowed():
    before = snapshot()
    after = deepcopy(before)
    after["tables"]["history"]["rows"] = {}
    after = seal_receipt(after)
    result = verify_identity_inventory(
        before,
        after,
        conversion_manifest=conversion(added_rows=True, schema_changes=True),
    )
    assert not result["ok"] and any(
        "original_row_missing" in item for item in result["violations"]
    )
    after = snapshot({"uid": 17})
    result = verify_identity_inventory(
        before, after, conversion_manifest=conversion(schema_changes=True)
    )
    assert (
        not result["ok"] and "history:original_columns_missing" in result["violations"]
    )


def test_added_rows_tables_columns_and_metadata_require_declarations():
    before = snapshot()
    after = deepcopy(before)
    after["tables"]["extra_history"] = deepcopy(after["tables"]["history"])
    assert not verify_identity_inventory(before, seal_receipt(after))["ok"]
    declared = conversion()
    declared["added_tables"] = ["extra_history"]
    assert verify_identity_inventory(
        before, seal_receipt(after), conversion_manifest=declared
    )["ok"]
    after = deepcopy(before)
    after["tables"]["history"]["owner"] = "different_owner"
    assert (
        "history:undeclared_schema_change"
        in verify_identity_inventory(before, seal_receipt(after))["violations"]
    )


def test_explicit_restored_target_is_required_and_never_waives_row_identity():
    before = snapshot()
    after = deepcopy(before)
    after["target"]["database"] = "restored_database"
    after["physical_target"]["database"] = "restored_database"
    after["physical_target"]["database_oid"] = 2345
    after = seal_receipt(after)
    assert not verify_identity_inventory(before, after)["ok"]
    declared = conversion()
    declared["target"] = after["target"]
    assert verify_identity_inventory(before, after, conversion_manifest=declared)["ok"]
    after["tables"]["history"]["rows"] = {}
    assert not verify_identity_inventory(
        before, seal_receipt(after), conversion_manifest=declared
    )["ok"]


@pytest.mark.parametrize(
    "change", [{"schema_version": "unknown"}, {"sha256": "tampered"}]
)
def test_bad_receipt_version_and_checksum_fail(change):
    before = snapshot()
    with pytest.raises(IdentityInventoryError):
        verify_identity_inventory(dict(before, **change), before)


def test_unknown_manifest_fields_tables_and_transformations_fail():
    before = snapshot()
    with pytest.raises(IdentityInventoryError):
        verify_identity_inventory(
            before,
            before,
            conversion_manifest={
                "schema_version": CONVERSION_VERSION,
                "ignore_identity": True,
            },
        )
    with pytest.raises(IdentityInventoryError, match="unknown original table"):
        verify_identity_inventory(
            before,
            before,
            conversion_manifest={
                "schema_version": CONVERSION_VERSION,
                "tables": {"unknown": {}},
            },
        )
    after = snapshot(
        {"uid": 17, "created_dt": "2026-09-10T20:00:00+00:00", "note": "different"}
    )
    with pytest.raises(IdentityInventoryError, match="Unknown identity conversion"):
        verify_identity_inventory(
            before,
            after,
            conversion_manifest=conversion(
                changed_columns={"note": {"kind": "anything"}}
            ),
        )


@pytest.mark.parametrize(
    "field",
    [
        "host",
        "database",
        "schema_name",
        "config_identity",
        "domain_code",
        "owner_repo_name",
        "port",
        "engine_type",
    ],
)
def test_target_identity_has_no_inferred_defaults(field):
    target = snapshot()["target"]
    del target[field]
    with pytest.raises(IdentityInventoryError, match="Explicit"):
        validate_target(target, "inventory")


@pytest.mark.parametrize(
    "changes",
    [
        {"port": 0},
        {"port": True},
        {"config_identity": "relative.yaml"},
        {"schema_name": "different"},
        {"server_port": None},
        {"server_port": False},
        {"server_port": "bad"},
    ],
)
def test_invalid_target_fields_and_explicit_server_port_fail(changes):
    with pytest.raises(IdentityInventoryError):
        validate_target(dict(snapshot()["target"], **changes), "inventory")


def test_transport_port_and_explicit_server_port_are_distinct():
    target = dict(snapshot()["target"], port=55434, server_port=5432)
    assert validate_target(target, "inventory")["server_port"] == 5432
    assert validate_target(target, "inventory")["port"] == 55434
    with pytest.raises(IdentityInventoryError):
        validate_target(None, "inventory")


def test_conversion_manifest_cannot_silently_change_primary_key_or_duplicate_rows():
    before = snapshot()
    after = deepcopy(before)
    after["tables"]["history"]["primary_key"] = []
    assert (
        "history:primary_key_changed"
        in verify_identity_inventory(
            before,
            seal_receipt(after),
            conversion_manifest=conversion(schema_changes=True),
        )["violations"]
    )
    after = deepcopy(before)
    next(iter(after["tables"]["history"]["rows"].values()))["count"] = 2
    assert not verify_identity_inventory(before, seal_receipt(after))["ok"]
    assert verify_identity_inventory(
        before, seal_receipt(after), conversion_manifest=conversion(added_rows=True)
    )["ok"]


def test_added_columns_and_removed_tables_still_need_explicit_contract():
    before = snapshot()
    after = snapshot(
        {
            "uid": 17,
            "created_dt": "2026-09-10T20:00:00+00:00",
            "note": None,
            "new_column": True,
        }
    )
    assert not verify_identity_inventory(before, after)["ok"]
    assert verify_identity_inventory(
        before,
        after,
        conversion_manifest=conversion(
            added_columns=["new_column"], schema_changes=True
        ),
    )["ok"]
    after = deepcopy(before)
    after["tables"] = {}
    assert verify_identity_inventory(before, seal_receipt(after))["violations"] == [
        "history:table_missing"
    ]
