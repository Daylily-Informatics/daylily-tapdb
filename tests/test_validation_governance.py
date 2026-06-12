from __future__ import annotations

from types import SimpleNamespace

import pytest

from daylily_tapdb.models.audit import audit_log
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.validation.governance import (
    DEFAULT_VALIDATOR_REF,
    assess_object,
    create_repair_record,
    editor_data,
    editor_data_for_object,
    ensure_core_governance_objects,
    normalize_validator_ref,
    validator_ref_for_object,
)


class _Query:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter_by(self, **kwargs):
        return _Query(
            [
                row
                for row in self._rows
                if all(
                    getattr(row, key, None) == value for key, value in kwargs.items()
                )
            ]
        )

    def filter(self, *args, **kwargs):
        del args, kwargs
        return self

    def order_by(self, *args, **kwargs):
        del args, kwargs
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.added = []
        self.bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    def query(self, model):
        return _Query(self.rows.get(model, []))

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        for index, obj in enumerate(self.added, start=700):
            if getattr(obj, "uid", None) is None:
                obj.uid = index
            if getattr(obj, "euid", None) is None:
                obj.euid = f"Z-GVR-{index}Q"


def _template(
    *,
    uid=1,
    euid="Z-TPX-1Q",
    category="container",
    type_name="plate",
    subtype="96well-generic",
    validator_ref="UNIVERSAL_PASS@1",
):
    return SimpleNamespace(
        uid=uid,
        euid=euid,
        name=f"{category}/{type_name}/{subtype}",
        domain_code="Z",
        category=category,
        type=type_name,
        subtype=subtype,
        version="1.0",
        instance_prefix="SMP",
        instance_polymorphic_identity="generic_instance",
        validator_ref=validator_ref,
        is_deleted=False,
    )


def _instance():
    return SimpleNamespace(
        uid=10,
        euid="Z-SMP-1Q",
        name="Sample",
        template_uid=1,
        tenant_id=None,
        category="container",
        type="plate",
        subtype="96well-generic",
        version="1.0",
        json_addl={"properties": {"color": "blue"}},
        is_deleted=False,
    )


def test_assess_object_uses_template_validator_without_mutating_subject():
    subject = _instance()
    session = _Session(
        {
            generic_template: [_template(validator_ref="CUSTOM_VALIDATOR@1")],
            generic_instance: [subject],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )

    assessment = assess_object(session, "Z-SMP-1Q")

    assert assessment.validator_ref == "CUSTOM_VALIDATOR@1"
    assert assessment.state == "not_evaluated_current"
    assert assessment.subject_mutated is False
    assert subject.json_addl == {"properties": {"color": "blue"}}


def test_governance_helpers_cover_defaults_and_editor_data(monkeypatch):
    subject = _instance()
    session = _Session(
        {
            generic_template: [_template(validator_ref="  ")],
            generic_instance: [subject],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )

    assert normalize_validator_ref(None) == DEFAULT_VALIDATOR_REF
    assert (
        validator_ref_for_object(session, _template(validator_ref="V@2"), "template")
        == "V@2"
    )
    assert (
        validator_ref_for_object(session, SimpleNamespace(), "instance")
        == DEFAULT_VALIDATOR_REF
    )

    payload = editor_data(
        subject_ref="Z-SMP-1Q",
        validator_ref="CUSTOM@1",
        evidence={"bad": "shape"},
        context={"source": "unit"},
    )
    assert payload["assessment"]["state"] == "not_evaluated_current"
    assert payload["raw"]["supports_jump_to_finding"] is True
    assert payload["structured"]["last_valid_state_policy"] == (
        "freeze_structured_view_on_invalid_raw_json"
    )

    monkeypatch.setattr(
        "daylily_tapdb.services.object_lookup.find_object_by_euid",
        lambda _session, euid: (
            (subject, "instance") if euid == "Z-SMP-1Q" else (None, None)
        ),
    )
    object_payload = editor_data_for_object(session, "Z-SMP-1Q", context={"ui": "test"})
    assert object_payload["assessment"]["context"]["ui"] == "test"

    with pytest.raises(LookupError, match="Object not found"):
        assess_object(session, "Z-NOT-FOUND")


def test_ensure_core_governance_objects_inserts_and_skips_existing_refs():
    existing = SimpleNamespace(
        json_addl={"properties": {"governance_ref": "UNIVERSAL_PASS@1"}}
    )
    session = _Session(
        {
            generic_template: [
                _template(
                    category="governance", type_name="validator", subtype="definition"
                )
            ],
            generic_instance: [existing],
        }
    )

    inserted = ensure_core_governance_objects(session, domain_code="Z")

    assert inserted == 3
    assert len(session.added) == 3
    assert {obj.json_addl["properties"]["governance_ref"] for obj in session.added} == {
        "TAPDB_EMPTY_TERMINOLOGY@1",
        "TAPDB_ANY_RELATIONSHIP@1",
        "TAPDB_NO_POSITION@1",
    }


def test_ensure_core_governance_objects_skips_non_postgres_sessions():
    session = _Session({})
    session.bind = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

    assert ensure_core_governance_objects(session, domain_code="Z") == 0


def test_create_repair_record_creates_evidence_without_mutating_subject():
    subject = _instance()
    repair_template = _template(
        uid=99,
        euid="Z-TPX-99Q",
        category="evidence",
        type_name="repair",
        subtype="record",
    )
    session = _Session(
        {
            generic_template: [_template(), repair_template],
            generic_instance: [subject],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )

    result = create_repair_record(
        session,
        domain_code="Z",
        subject_euid="Z-SMP-1Q",
        actor="tester",
        reason="correct color",
        repair_payload={"properties": {"color": "red"}},
    )

    assert result["subject_euid"] == "Z-SMP-1Q"
    assert result["subject_mutated"] is False
    assert result["template_code"] == "evidence/repair/record/1.0/"
    assert session.added[0].json_addl["properties"]["repair_payload"] == {
        "properties": {"color": "red"}
    }
    assert session.added[0].json_addl["properties"]["recorded_at"]
    assert session.added[0].json_addl["properties"]["governance_context"] == {
        "repair_model": "explicit_evidence",
        "subject_mutated": False,
        "assessment_persistence": "ephemeral_not_persisted",
    }
    assert subject.json_addl == {"properties": {"color": "blue"}}


def test_create_repair_record_rejects_invalid_inputs(monkeypatch):
    session = _Session({generic_template: [], generic_instance: []})

    with pytest.raises(ValueError, match="subject_euid is required"):
        create_repair_record(
            session,
            domain_code="Z",
            subject_euid=" ",
            actor="tester",
            reason="fix",
            repair_payload={},
        )
    with pytest.raises(ValueError, match="reason is required"):
        create_repair_record(
            session,
            domain_code="Z",
            subject_euid="Z-SMP-1Q",
            actor="tester",
            reason=" ",
            repair_payload={},
        )
    with pytest.raises(ValueError, match="repair_payload must be a JSON object"):
        create_repair_record(
            session,
            domain_code="Z",
            subject_euid="Z-SMP-1Q",
            actor="tester",
            reason="fix",
            repair_payload=[],  # type: ignore[arg-type]
        )

    monkeypatch.setattr(
        "daylily_tapdb.services.object_lookup.find_object_by_euid",
        lambda _session, _euid: (None, None),
    )
    with pytest.raises(LookupError, match="Object not found"):
        create_repair_record(
            session,
            domain_code="Z",
            subject_euid="Z-SMP-1Q",
            actor="tester",
            reason="fix",
            repair_payload={},
        )


def test_create_repair_record_requires_seeded_repair_template(monkeypatch):
    subject = _instance()
    session = _Session({generic_template: [_template()], generic_instance: [subject]})
    monkeypatch.setattr(
        "daylily_tapdb.services.object_lookup.find_object_by_euid",
        lambda _session, _euid: (subject, "instance"),
    )

    with pytest.raises(LookupError, match="repair template is seeded"):
        create_repair_record(
            session,
            domain_code="Z",
            subject_euid="Z-SMP-1Q",
            actor="tester",
            reason="fix",
            repair_payload={},
        )
