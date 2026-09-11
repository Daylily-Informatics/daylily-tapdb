"""Behavior coverage for post-restore verification decision branches."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import daylily_tapdb.backup.postrestore as postrestore
import daylily_tapdb.euid as euid_mod
import daylily_tapdb.schema_inventory as schema_inventory
from daylily_tapdb.backup.service import STATUS_FAIL, STATUS_SKIP, STATUS_WARN
from daylily_tapdb.identity_inventory import seal_receipt
from tests.test_recovery_floor import TARGET, inventory


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
    monkeypatch.setattr(
        postrestore.introspect, "capture_sequences", lambda *_args, **_kw: []
    )
    session = _Session([_Result(first=(2, 2))])
    result = postrestore.check_audit_continuity(
        session, SimpleNamespace(row_counts={"audit_log": 3}), "schema", target=TARGET
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
    live = inventory(name="current", last=22)
    monkeypatch.setattr(
        "daylily_tapdb.sequences.capture_sequence_inventory", lambda *_args, **_kw: live
    )
    manifest = SimpleNamespace(sequence_inventory=inventory(name="missing"))
    result = postrestore.check_sequence_high_water(
        object(), manifest, "history", target=TARGET
    )
    assert result.status == STATUS_FAIL
    assert "retained generator is missing" in result.detail

    manifest.sequence_inventory = {}
    result = postrestore.check_sequence_high_water(
        object(), manifest, "history", target=TARGET
    )
    assert result.status == STATUS_FAIL
    assert "complete manifest sequence inventory" in result.detail


def test_prefix_projection_refuses_missing_target_and_missing_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        postrestore.introspect, "euid_bearing_tables", lambda *_args: []
    )
    assert (
        postrestore.check_prefix_sequences_ahead(object(), "schema").status
        == STATUS_FAIL
    )

    current = seal_receipt(
        {**inventory(), "missing_generators": ["unmapped_instance_seq"]}
    )
    monkeypatch.setattr(
        "daylily_tapdb.sequences.capture_sequence_inventory", lambda *_a, **_k: current
    )
    result = postrestore.check_prefix_sequences_ahead(
        object(), "history", target=TARGET
    )
    assert result.status == STATUS_FAIL
    assert "missing" in result.detail.lower()


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
    tmp_path,
) -> None:
    live = inventory()
    monkeypatch.setattr(
        "daylily_tapdb.sequences.capture_sequence_inventory", lambda *_args, **_kw: live
    )
    captured = {}
    monkeypatch.setattr(
        "daylily_tapdb.sequences.apply_sequence_advance_plan",
        lambda session, plan, **kw: (
            captured.update(plan=plan, **kw) or {"phase": "applied_pending_commit"}
        ),
    )
    session = _Session([])
    fence = {"mode": "database_connections_disabled", "database_oid": 123}
    floor = [{"name": "history_uid_seq", "value": 35, "source": "prior_attempt"}]
    result = postrestore.reconcile_sequences_to_floor(
        session,
        "history",
        floor=floor,
        target=TARGET,
        writer_fence=fence,
        receipts_dir=tmp_path,
    )
    assert result["phase"] == "applied_pending_commit"
    assert captured["plan"]["advances"] == [
        {"name": "history_uid_seq", "floor": 35, "next_value": 36}
    ]
    assert captured["writer_fence"] == fence
    assert captured["receipts_dir"] == tmp_path
    assert session.calls == []
    with pytest.raises(ValueError, match="lacks complete"):
        postrestore.reconcile_sequences_to_floor(
            session,
            "history",
            floor=[_sequence("legacy", None)],
            target=TARGET,
            writer_fence=fence,
            receipts_dir=tmp_path,
        )
