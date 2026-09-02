"""Behavior coverage for post-restore verification decision branches."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import daylily_tapdb.backup.postrestore as postrestore
import daylily_tapdb.euid as euid_mod
import daylily_tapdb.schema_inventory as schema_inventory
from daylily_tapdb.backup.service import STATUS_FAIL, STATUS_SKIP, STATUS_WARN


def _sequence(name: str, next_value: int | None, last_value: int = 1):
    return SimpleNamespace(
        name=name,
        next_value=next_value,
        last_value=last_value,
        is_called=True,
    )


class _Result:
    def __init__(self, *, first=None, scalar=None, scalars=None, all_rows=None):
        self._first = first
        self._scalar = scalar
        self._scalars = scalars or []
        self._all = all_rows or []

    def first(self):
        return self._first

    def scalar(self):
        return self._scalar

    def scalars(self):
        return self._scalars

    def all(self):
        return self._all


class _Session:
    def __init__(self, results):
        self.results = iter(results)
        self.calls: list[tuple[object, object]] = []

    def execute(self, statement, params=None):
        self.calls.append((statement, params))
        return next(self.results)


def test_optional_table_checks_skip_when_tables_are_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(postrestore, "_table_exists", lambda *_args: False)
    manifest = SimpleNamespace(row_counts={})
    assert (
        postrestore.check_template_references(object(), "schema").status == STATUS_SKIP
    )
    assert postrestore.check_lineage_integrity(object(), "schema").status == STATUS_SKIP
    assert (
        postrestore.check_audit_continuity(object(), manifest, "schema").status
        == STATUS_SKIP
    )


def test_audit_count_mismatch_without_identity_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(postrestore, "_table_exists", lambda *_args: True)
    monkeypatch.setattr(postrestore.introspect, "capture_sequences", lambda *_args: [])
    session = _Session([_Result(first=(2, 2))])
    result = postrestore.check_audit_continuity(
        session, SimpleNamespace(row_counts={"audit_log": 3}), "schema"
    )
    assert result.status == STATUS_FAIL
    assert result.data["count"] == {"expected": 3, "live": 2}


def test_euid_checks_skip_and_report_validation_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        postrestore.introspect, "euid_bearing_tables", lambda *_args: []
    )
    assert postrestore.check_euid_uniqueness(object(), "schema").status == STATUS_SKIP
    assert postrestore.check_euid_format(object(), "schema").status == STATUS_SKIP

    monkeypatch.setattr(
        postrestore.introspect, "euid_bearing_tables", lambda *_args: ["objects"]
    )

    def _validate(value: str) -> bool:
        if value == "raises":
            raise ValueError("bad encoding")
        return value == "valid"

    monkeypatch.setattr(euid_mod, "validate_euid", _validate)
    session = _Session([_Result(scalars=["valid", "invalid", "raises"])])
    result = postrestore.check_euid_format(session, "schema")
    assert result.status == STATUS_FAIL
    assert result.data["sampled"] == 3
    assert result.data["invalid"] == ["invalid", "raises"]


def test_sequence_high_water_covers_missing_old_and_current_sequences(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live = [_sequence("old", 9), _sequence("current", 20)]
    monkeypatch.setattr(
        postrestore.introspect, "capture_sequences", lambda *_args: live
    )
    manifest = SimpleNamespace(
        sequences=[
            _sequence("missing", 3),
            _sequence("old", None),
            _sequence("current", 10),
        ]
    )
    result = postrestore.check_sequence_high_water(object(), manifest, "schema")
    assert result.status == STATUS_FAIL
    assert result.data["missing"] == "missing from the restored schema"

    manifest.sequences = [_sequence("old", None)]
    result = postrestore.check_sequence_high_water(object(), manifest, "schema")
    assert result.status == STATUS_WARN
    assert result.data == {"uncomparable": ["old"]}


def test_prefix_projection_skips_missing_table_and_missing_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        postrestore.introspect, "euid_bearing_tables", lambda *_args: []
    )
    assert (
        postrestore.check_prefix_sequences_ahead(object(), "schema").status
        == STATUS_SKIP
    )

    monkeypatch.setattr(
        postrestore.introspect,
        "euid_bearing_tables",
        lambda *_args: ["generic_instance"],
    )
    monkeypatch.setattr(
        postrestore.introspect,
        "capture_sequences",
        lambda *_args: [_sequence("xyz_instance_seq", 10)],
    )
    session = _Session([_Result(all_rows=[("missing", 2), ("xyz", 2)])])
    result = postrestore.check_prefix_sequences_ahead(session, "schema")
    assert result.status != STATUS_WARN
    assert result.data == {}


def test_schema_drift_missing_assets_is_advisory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        schema_inventory,
        "find_schema_root",
        lambda *_args: (_ for _ in ()).throw(FileNotFoundError("assets absent")),
    )
    result = postrestore.check_schema_drift(object(), {"database": "db"}, "schema")
    assert result.status == STATUS_WARN
    assert "assets absent" in result.detail


def test_representative_objects_empty_and_unaddressable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = SimpleNamespace(representative_objects=[])
    assert (
        postrestore.check_representative_objects(object(), manifest).status
        == STATUS_SKIP
    )

    monkeypatch.setattr(
        postrestore, "_object_addressable_tables", lambda: {"generic_instance"}
    )
    manifest.representative_objects = [
        {"table": "generic_instance", "euid": ""},
        {"table": "audit_log", "euid": "event"},
    ]
    result = postrestore.check_representative_objects(object(), manifest)
    assert result.status == STATUS_SKIP
    assert result.data == {"not_addressable": 1}


def test_reconcile_sequences_applies_only_required_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live = [
        _sequence("ahead", 10),
        _sequence("behind", 2),
        _sequence("legacy", 1),
    ]
    monkeypatch.setattr(
        postrestore.introspect, "capture_sequences", lambda *_args: live
    )
    session = _Session([_Result()])
    floor = [
        _sequence("legacy", None),
        _sequence("absent", 4),
        _sequence("ahead", 5),
        _sequence("behind", 8),
    ]
    advanced = postrestore.reconcile_sequences_to_floor(session, "schema", floor=floor)
    assert advanced == {"behind": {"from_next": 2, "to_next": 8}}
    assert session.calls[0][1] == {"value": 8}
