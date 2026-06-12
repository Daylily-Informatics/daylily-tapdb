"""Governance validation contracts for TapDB evidence.

Validators observe evidence and return assessments. They do not mutate the
subject object, lineage row, claim, event, metadata, terminology, or template.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

DEFAULT_VALIDATOR_REF = "UNIVERSAL_PASS@1"
DEFAULT_TERMINOLOGY_REF = "TAPDB_EMPTY_TERMINOLOGY@1"
DEFAULT_RELATIONSHIP_CONSTRAINT_REF = "TAPDB_ANY_RELATIONSHIP@1"
DEFAULT_POSITION_SCHEME_REF = "TAPDB_NO_POSITION@1"

GOVERNANCE_VALIDATOR_TEMPLATE_CODE = "governance/validator/definition/1.0/"
GOVERNANCE_TERMINOLOGY_TEMPLATE_CODE = "governance/terminology/set/1.0/"
GOVERNANCE_RELATIONSHIP_TEMPLATE_CODE = "governance/relationship/constraint/1.0/"
GOVERNANCE_POSITION_TEMPLATE_CODE = "governance/position/scheme/1.0/"
REPAIR_RECORD_TEMPLATE_CODE = "evidence/repair/record/1.0/"

AssessmentState = Literal[
    "valid_current",
    "valid_historical",
    "nonconforming_current",
    "not_evaluated_current",
]

FindingSeverity = Literal["info", "warning", "error"]


@dataclass(frozen=True)
class Finding:
    """One validator observation."""

    code: str
    message: str
    severity: FindingSeverity = "info"
    path: str = ""
    recommendation: str = ""


@dataclass(frozen=True)
class RepairRecommendation:
    """A non-mutating suggestion that can be converted to explicit repair evidence."""

    code: str
    message: str
    target_path: str = ""
    proposed_value: Any | None = None


@dataclass(frozen=True)
class Assessment:
    """Non-mutating assessment result."""

    subject_ref: str
    validator_ref: str = DEFAULT_VALIDATOR_REF
    state: AssessmentState = "valid_current"
    findings: tuple[Finding, ...] = ()
    repair_recommendations: tuple[RepairRecommendation, ...] = ()
    assessed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    context: dict[str, Any] = field(default_factory=dict)
    subject_mutated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject_ref": self.subject_ref,
            "validator_ref": self.validator_ref,
            "state": self.state,
            "findings": [finding.__dict__ for finding in self.findings],
            "repair_recommendations": [
                recommendation.__dict__
                for recommendation in self.repair_recommendations
            ],
            "assessed_at": self.assessed_at.isoformat(),
            "context": dict(self.context),
            "subject_mutated": self.subject_mutated,
            "non_mutation_statement": (
                "Assessment did not mutate objects, relationships, lineage, "
                "claims, events, metadata, or terminology values."
            ),
        }


def normalize_validator_ref(value: str | None) -> str:
    """Return a materialized validator reference."""

    normalized = str(value or "").strip()
    return normalized or DEFAULT_VALIDATOR_REF


def _template_code(template: Any) -> str:
    return f"{template.category}/{template.type}/{template.subtype}/{template.version}/"


def _template_key(template_code: str) -> tuple[str, str, str, str]:
    parts = [part for part in str(template_code).strip("/").split("/") if part]
    if len(parts) != 4:
        raise ValueError(f"Invalid governance template code: {template_code!r}")
    return parts[0], parts[1], parts[2], parts[3]


def _postgres_session(session: Any) -> bool:
    bind = getattr(session, "bind", None)
    dialect = getattr(bind, "dialect", None)
    return str(getattr(dialect, "name", "") or "").strip().lower() == "postgresql"


def ensure_core_governance_objects(
    session: Any,
    *,
    domain_code: str,
) -> int:
    """Ensure built-in governance objects exist.

    Returns the number of inserted objects. Non-PostgreSQL sessions are skipped
    because TapDB EUID assignment is trigger-backed.
    """

    if not _postgres_session(session):
        return 0

    from daylily_tapdb.models.instance import generic_instance
    from daylily_tapdb.models.template import generic_template

    specs = [
        {
            "template_code": GOVERNANCE_VALIDATOR_TEMPLATE_CODE,
            "name": "Universal Pass Validator",
            "ref": DEFAULT_VALIDATOR_REF,
            "properties": {
                "governance_ref": DEFAULT_VALIDATOR_REF,
                "validator_type": "universal_pass",
                "version": "1",
                "description": "Accepts observed evidence without mutation.",
            },
        },
        {
            "template_code": GOVERNANCE_TERMINOLOGY_TEMPLATE_CODE,
            "name": "TapDB Empty Terminology",
            "ref": DEFAULT_TERMINOLOGY_REF,
            "properties": {
                "governance_ref": DEFAULT_TERMINOLOGY_REF,
                "terms": [],
                "description": "Default empty terminology set.",
            },
        },
        {
            "template_code": GOVERNANCE_RELATIONSHIP_TEMPLATE_CODE,
            "name": "TapDB Any Relationship Constraint",
            "ref": DEFAULT_RELATIONSHIP_CONSTRAINT_REF,
            "properties": {
                "governance_ref": DEFAULT_RELATIONSHIP_CONSTRAINT_REF,
                "allowed_relationship_types": ["*"],
                "description": "Default relationship constraint allowing any assertion.",
            },
        },
        {
            "template_code": GOVERNANCE_POSITION_TEMPLATE_CODE,
            "name": "TapDB No Position Scheme",
            "ref": DEFAULT_POSITION_SCHEME_REF,
            "properties": {
                "governance_ref": DEFAULT_POSITION_SCHEME_REF,
                "position_required": False,
                "description": "Default position scheme for relationships without positions.",
            },
        },
    ]

    inserted = 0
    for spec in specs:
        category, type_name, subtype, version = _template_key(spec["template_code"])
        template = (
            session.query(generic_template)
            .filter(
                generic_template.domain_code == domain_code,
                generic_template.category == category,
                generic_template.type == type_name,
                generic_template.subtype == subtype,
                generic_template.version == version,
                generic_template.is_deleted.is_(False),
            )
            .first()
        )
        if template is None:
            continue

        existing = (
            session.query(generic_instance)
            .filter(
                generic_instance.domain_code == domain_code,
                generic_instance.template_uid == template.uid,
                generic_instance.is_deleted.is_(False),
            )
            .all()
        )
        ref = spec["ref"]
        if any(
            isinstance(instance.json_addl, dict)
            and isinstance(instance.json_addl.get("properties"), dict)
            and instance.json_addl["properties"].get("governance_ref") == ref
            for instance in existing
        ):
            continue

        instance = generic_instance(
            template_uid=template.uid,
            tenant_id=None,
            domain_code=domain_code,
            name=str(spec["name"]),
            polymorphic_discriminator=(
                template.instance_polymorphic_identity or "generic_instance"
            ),
            category=template.category,
            type=template.type,
            subtype=template.subtype,
            version=template.version,
            bstatus="active",
            is_singleton=True,
            json_addl={"properties": dict(spec["properties"])},
            is_deleted=False,
        )
        session.add(instance)
        inserted += 1

    if inserted:
        session.flush()
    return inserted


def validator_ref_for_object(session: Any, obj: Any, record_type: str) -> str:
    """Resolve the validator reference for a TapDB object without mutating it."""

    from daylily_tapdb.models.template import generic_template

    if record_type == "template":
        return normalize_validator_ref(getattr(obj, "validator_ref", None))

    template_uid = getattr(obj, "template_uid", None)
    if template_uid is None:
        return DEFAULT_VALIDATOR_REF
    template = (
        session.query(generic_template)
        .filter_by(uid=template_uid, is_deleted=False)
        .first()
    )
    if template is None:
        return DEFAULT_VALIDATOR_REF
    return normalize_validator_ref(getattr(template, "validator_ref", None))


def assess_object(
    session: Any,
    euid: str,
    *,
    validator_ref: str | None = None,
    context: dict[str, Any] | None = None,
) -> Assessment:
    """Assess one TapDB object by EUID without mutating it."""

    from daylily_tapdb.services.object_lookup import find_object_by_euid

    subject, record_type = find_object_by_euid(session, euid)
    if subject is None or record_type is None:
        raise LookupError(f"Object not found: {euid}")

    resolved_ref = normalize_validator_ref(
        validator_ref or validator_ref_for_object(session, subject, record_type)
    )
    resolved_context = {
        "record_type": record_type,
        "category": getattr(subject, "category", None),
        "type": getattr(subject, "type", None),
        "subtype": getattr(subject, "subtype", None),
        "version": getattr(subject, "version", None),
        **dict(context or {}),
    }
    return assess_evidence(
        subject_ref=str(getattr(subject, "euid", euid)),
        validator_ref=resolved_ref,
        evidence=getattr(subject, "json_addl", None),
        context=resolved_context,
    )


def editor_data_for_object(
    session: Any,
    euid: str,
    *,
    validator_ref: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return editor metadata for one TapDB object by EUID."""

    assessment = assess_object(
        session,
        euid,
        validator_ref=validator_ref,
        context=context,
    )
    return {
        **editor_data(
            subject_ref=assessment.subject_ref,
            validator_ref=assessment.validator_ref,
            context=assessment.context,
        ),
        "assessment": assessment.to_dict(),
    }


def repair_template(session: Any) -> Any | None:
    """Return the governed repair-record template when it is seeded."""

    from daylily_tapdb.models.template import generic_template

    return (
        session.query(generic_template)
        .filter_by(
            category="evidence",
            type="repair",
            subtype="record",
            is_deleted=False,
        )
        .order_by(generic_template.version.desc())
        .first()
    )


def create_repair_record(
    session: Any,
    *,
    domain_code: str,
    subject_euid: str,
    actor: str,
    reason: str,
    repair_payload: dict[str, Any],
    approval: dict[str, Any] | None = None,
    governance_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create explicit repair evidence without mutating the subject."""

    from daylily_tapdb.models.instance import generic_instance
    from daylily_tapdb.services.object_lookup import find_object_by_euid

    normalized_subject = str(subject_euid or "").strip()
    if not normalized_subject:
        raise ValueError("subject_euid is required")
    normalized_reason = str(reason or "").strip()
    if not normalized_reason:
        raise ValueError("reason is required")
    if not isinstance(repair_payload, dict):
        raise ValueError("repair_payload must be a JSON object")

    subject, record_type = find_object_by_euid(session, normalized_subject)
    if subject is None or record_type is None:
        raise LookupError(f"Object not found: {normalized_subject}")

    template = repair_template(session)
    if template is None:
        raise LookupError("No evidence/repair/record/1.0 repair template is seeded.")

    validator_ref = validator_ref_for_object(session, subject, record_type)
    context = {
        "repair_model": "explicit_evidence",
        "subject_mutated": False,
        "assessment_persistence": "ephemeral_not_persisted",
        **dict(governance_context or {}),
    }
    properties = {
        "actor": str(actor or "").strip(),
        "recorded_at": datetime.now(UTC).isoformat(),
        "reason": normalized_reason,
        "subject_euid": normalized_subject,
        "subject_record_type": record_type,
        "validator_ref": validator_ref,
        "prior_evidence_ref": normalized_subject,
        "new_evidence_ref": "",
        "governance_context": context,
        "approval": dict(approval or {}),
        "repair_payload": dict(repair_payload),
        "subject_mutated": False,
    }
    instance = generic_instance(
        template_uid=template.uid,
        domain_code=str(domain_code or "").strip()
        or getattr(template, "domain_code", None),
        tenant_id=getattr(subject, "tenant_id", None),
        name=f"Repair record for {normalized_subject}",
        polymorphic_discriminator=(
            getattr(template, "instance_polymorphic_identity", None)
            or "generic_instance"
        ),
        category=template.category,
        type=template.type,
        subtype=template.subtype,
        version=template.version,
        bstatus="created",
        is_singleton=False,
        json_addl={"properties": properties},
        is_deleted=False,
    )
    session.add(instance)
    session.flush()
    return {
        "repair_euid": getattr(instance, "euid", None),
        "repair_uid": getattr(instance, "uid", None),
        "subject_euid": normalized_subject,
        "subject_mutated": False,
        "template_code": _template_code(template),
        "properties": properties,
    }


def assess_evidence(
    *,
    subject_ref: str,
    validator_ref: str | None = None,
    evidence: Any | None = None,
    context: dict[str, Any] | None = None,
) -> Assessment:
    """Assess evidence without mutating it.

    The initial implementation provides the explicit Universal Pass behavior and
    a deterministic not-evaluated response for unknown custom validators.
    """

    resolved_ref = normalize_validator_ref(validator_ref)
    resolved_context = dict(context or {})
    if resolved_ref == DEFAULT_VALIDATOR_REF:
        return Assessment(
            subject_ref=subject_ref,
            validator_ref=resolved_ref,
            state="valid_current",
            findings=(
                Finding(
                    code="universal_pass",
                    message="Universal Pass accepted the supplied evidence.",
                    severity="info",
                ),
            ),
            context=resolved_context,
        )

    _ = evidence
    return Assessment(
        subject_ref=subject_ref,
        validator_ref=resolved_ref,
        state="not_evaluated_current",
        findings=(
            Finding(
                code="validator_not_implemented",
                message=f"Validator behavior is not implemented: {resolved_ref}",
                severity="warning",
            ),
        ),
        context=resolved_context,
    )


def editor_data(
    *,
    subject_ref: str,
    validator_ref: str | None = None,
    evidence: Any | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return UI data for raw/structured/split editor surfaces."""

    assessment = assess_evidence(
        subject_ref=subject_ref,
        validator_ref=validator_ref,
        evidence=evidence,
        context=context,
    )
    return {
        "subject_ref": subject_ref,
        "validator_ref": assessment.validator_ref,
        "assessment": assessment.to_dict(),
        "raw": {
            "enabled": True,
            "supports_format": True,
            "supports_path_search": True,
            "supports_text_search": True,
            "supports_diff": True,
            "supports_jump_to_finding": True,
        },
        "structured": {
            "enabled": True,
            "sections": [],
            "last_valid_state_policy": "freeze_structured_view_on_invalid_raw_json",
        },
        "split": {"enabled": True},
    }
