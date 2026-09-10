"""NULL reference fills require persisted mapping rows and validated FK proof."""

from copy import deepcopy

import pytest

from daylily_tapdb.identity_inventory import (
    IdentityInventoryError,
    content_hash,
    seal_receipt,
    verify_identity_inventory,
)
from tests.test_identity_inventory import snapshot


def _table(rows, *, primary_key, constraints=()):
    entry = deepcopy(snapshot()["tables"]["history"])
    names = list(rows[0])
    entry.update(
        columns=[
            {
                "name": name,
                "nullable": name == "message_uid",
                "identity": "",
                "generated": "",
            }
            for name in names
        ],
        primary_key=[primary_key],
        immutable_columns=names,
        constraints=list(constraints),
        rows={
            content_hash([row[primary_key]]): {
                "sha256": content_hash(row),
                "count": 1,
                "columns": {name: content_hash(value) for name, value in row.items()},
                "identity": dict(row),
            }
            for row in rows
        },
    )
    entry["row_count"] = len(rows)
    entry["content_sha256"] = content_hash(entry["rows"])
    return entry


def _fk(column):
    return {
        "name": f"{column}_fk",
        "kind": "f",
        "columns": [column],
        "validated": True,
        "definition": f"FOREIGN KEY ({column}) REFERENCES inventory.generic_instance(uid)",
        "referenced_schema": "inventory",
        "referenced_table": "generic_instance",
        "referenced_columns": ["uid"],
    }


@pytest.fixture
def reference_fill():
    before = snapshot()
    timestamp = "2026-09-10T20:00:00+00:00"
    originals = [
        {"id": 17, "message_uid": None, "created_dt": timestamp},
        {"id": 18, "message_uid": 22, "created_dt": timestamp},
    ]
    target_rows = [
        {"uid": 22, "created_dt": timestamp},
        {"uid": 29, "created_dt": timestamp},
    ]
    mapping_row = {
        "old_outbox_id": 17,
        "message_uid": 29,
        "source_sha256": content_hash(originals[0]),
    }
    before["tables"] = {
        "outbox_event": _table(originals, primary_key="id"),
        "generic_instance": _table(target_rows[:1], primary_key="uid"),
        "tapdb_legacy_outbox_mapping": _table(
            [mapping_row], primary_key="old_outbox_id", constraints=[_fk("message_uid")]
        ),
    }
    before["tables"]["tapdb_legacy_outbox_mapping"]["rows"] = {}
    before["tables"]["tapdb_legacy_outbox_mapping"]["row_count"] = 0
    after = deepcopy(before)
    after["tables"]["outbox_event"] = _table(
        [dict(originals[0], message_uid=29), originals[1]],
        primary_key="id",
        constraints=[_fk("message_uid")],
    )
    after["tables"]["generic_instance"] = _table(target_rows, primary_key="uid")
    after["tables"]["tapdb_legacy_outbox_mapping"] = _table(
        [mapping_row], primary_key="old_outbox_id", constraints=[_fk("message_uid")]
    )
    source_key, target_key = content_hash([17]), content_hash([29])
    declaration = {
        "kind": "null_reference_from_mapping/v1",
        "target_table": "generic_instance",
        "target_column": "uid",
        "mapping_table": "tapdb_legacy_outbox_mapping",
        "mapping_source_column": "old_outbox_id",
        "mapping_target_column": "message_uid",
        "changes": {
            source_key: {
                "before": content_hash(None),
                "after": content_hash(29),
                "target_row_key": target_key,
                "mapping_row_key": source_key,
                "mapping_row_sha256": content_hash(mapping_row),
            }
        },
    }
    conversion = {
        "schema_version": "tapdb-identity-conversion/v1",
        "tables": {
            "outbox_event": {
                "schema_changes": True,
                "changed_columns": {"message_uid": declaration},
            },
            "generic_instance": {"added_rows": True},
            "tapdb_legacy_outbox_mapping": {"added_rows": True},
        },
        "added_tables": [],
    }
    return before, after, conversion, declaration


def _verify(fixture):
    before, after, conversion, _ = fixture
    return verify_identity_inventory(
        seal_receipt(before), seal_receipt(after), conversion_manifest=conversion
    )


def test_null_reference_fill_preserves_existing_nonnull_reference(reference_fill):
    assert _verify(reference_fill)["ok"]


def test_mapping_table_may_be_explicitly_declared_new(reference_fill):
    before, _, conversion, _ = reference_fill
    del before["tables"]["tapdb_legacy_outbox_mapping"]
    del conversion["tables"]["tapdb_legacy_outbox_mapping"]
    conversion["added_tables"] = ["tapdb_legacy_outbox_mapping"]
    assert _verify(reference_fill)["ok"]


@pytest.mark.parametrize(
    "name", ["uid", "id", "euid", "created_dt", "tenant_id", "euid_seq"]
)
def test_null_fill_contract_never_changes_intrinsic_identity(reference_fill, name):
    _, _, conversion, declaration = reference_fill
    conversion["tables"]["outbox_event"]["changed_columns"] = {name: declaration}
    with pytest.raises(IdentityInventoryError, match="Intrinsic identities"):
        _verify(reference_fill)


def test_existing_nonnull_reference_cannot_be_remapped(reference_fill):
    before, _, _, declaration = reference_fill
    key = content_hash([17])
    row = before["tables"]["outbox_event"]["rows"][key]
    row["identity"]["message_uid"] = 22
    row["columns"]["message_uid"] = content_hash(22)
    declaration["changes"][key]["before"] = content_hash(22)
    with pytest.raises(IdentityInventoryError, match="original NULL"):
        _verify(reference_fill)


@pytest.mark.parametrize("table", ["outbox_event", "tapdb_legacy_outbox_mapping"])
@pytest.mark.parametrize(
    "change",
    [
        {"validated": False},
        {"referenced_schema": "other"},
        {"referenced_table": "other"},
        {"referenced_columns": ["other"]},
        {"columns": ["message_uid", "other"]},
    ],
)
def test_unverified_or_different_fk_dependency_rejected(reference_fill, table, change):
    reference_fill[1]["tables"][table]["constraints"][0].update(change)
    with pytest.raises(IdentityInventoryError, match="validated FK"):
        _verify(reference_fill)


@pytest.mark.parametrize("table", ["generic_instance", "tapdb_legacy_outbox_mapping"])
def test_target_and_mapping_rows_require_explicit_addition_approval(
    reference_fill, table
):
    reference_fill[2]["tables"][table] = {}
    with pytest.raises(IdentityInventoryError, match="not approved"):
        _verify(reference_fill)


@pytest.mark.parametrize(
    "field", ["target_row_key", "mapping_row_key", "mapping_row_sha256"]
)
def test_each_mapping_evidence_hash_is_bound_to_its_persisted_row(
    reference_fill, field
):
    reference_fill[3]["changes"][content_hash([17])][field] = content_hash("wrong")
    with pytest.raises(IdentityInventoryError, match="persisted"):
        _verify(reference_fill)


@pytest.mark.parametrize(
    "table,column",
    [
        ("generic_instance", "uid"),
        ("tapdb_legacy_outbox_mapping", "message_uid"),
        ("tapdb_legacy_outbox_mapping", "old_outbox_id"),
    ],
)
def test_source_mapping_target_values_must_join_exactly(reference_fill, table, column):
    rows = reference_fill[1]["tables"][table]["rows"]
    row_key = content_hash([29] if table == "generic_instance" else [17])
    rows[row_key]["columns"][column] = content_hash(999)
    with pytest.raises(IdentityInventoryError, match="exact persisted mapping"):
        _verify(reference_fill)


@pytest.mark.parametrize(
    "changes", [{"nullable": False}, {"identity": "d"}, {"generated": "s"}]
)
def test_only_originally_nullable_nongenerated_references_can_fill(
    reference_fill, changes
):
    attributes = reference_fill[0]["tables"]["outbox_event"]["columns"]
    next(column for column in attributes if column["name"] == "message_uid").update(
        changes
    )
    with pytest.raises(IdentityInventoryError, match="Intrinsic identities"):
        _verify(reference_fill)


def test_existing_target_row_is_not_an_approved_new_mapping_target(reference_fill):
    before, after, _, _ = reference_fill
    before["tables"]["generic_instance"]["rows"] = deepcopy(
        after["tables"]["generic_instance"]["rows"]
    )
    with pytest.raises(IdentityInventoryError, match="newly persisted"):
        _verify(reference_fill)


def test_old_exact_hash_contract_still_cannot_waive_reference_identity(reference_fill):
    _, _, conversion, declaration = reference_fill
    conversion["tables"]["outbox_event"]["changed_columns"]["message_uid"] = {
        "kind": "exact_value_hashes",
        "changes": declaration["changes"],
    }
    with pytest.raises(IdentityInventoryError, match="Immutable identities"):
        _verify(reference_fill)


@pytest.mark.parametrize(
    "change",
    [
        {"changes": {}},
        {"changes": []},
        {"target_table": ""},
        {"target_table": "absent"},
        {"mapping_table": "generic_instance"},
        {"target_column": "not_the_primary_key"},
    ],
)
def test_reference_contract_rejects_incomplete_or_ambiguous_shapes(
    reference_fill, change
):
    reference_fill[3].update(change)
    with pytest.raises(IdentityInventoryError):
        _verify(reference_fill)


def test_reference_contract_requires_complete_catalog_fk_metadata(reference_fill):
    del reference_fill[1]["tables"]["outbox_event"]["constraints"][0][
        "referenced_schema"
    ]
    with pytest.raises(IdentityInventoryError, match="validated FK"):
        _verify(reference_fill)


def test_reference_contract_rejects_unknown_cell_evidence_fields(reference_fill):
    reference_fill[3]["changes"][content_hash([17])]["ignore_existing_value"] = True
    with pytest.raises(IdentityInventoryError, match="exact cell"):
        _verify(reference_fill)


def test_reference_contract_cannot_ignore_an_unlisted_nonnull_change(reference_fill):
    row = reference_fill[1]["tables"]["outbox_event"]["rows"][content_hash([18])]
    row["identity"]["message_uid"] = 29
    row["columns"]["message_uid"] = content_hash(29)
    result = _verify(reference_fill)
    assert not result["ok"]
    assert any(
        "message_uid:value_changed" in violation for violation in result["violations"]
    )
