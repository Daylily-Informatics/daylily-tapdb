"""Governed exact-selector object reads and mutations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import select

from daylily_tapdb.external_references import _is_xrf_coordinates
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.validation.governance import create_repair_record

RECORD_MODELS = {
    "template": generic_template,
    "instance": generic_instance,
    "lineage": generic_instance_lineage,
}
UPDATE_ALLOWLISTS = {
    "template": frozenset(),
    "instance": frozenset({"name", "bstatus", "json_addl"}),
    "lineage": frozenset({"name", "bstatus", "json_addl"}),
}


@dataclass(frozen=True)
class ObjectSelector:
    """One exact stable identifier and, when needed, an exact record type."""

    euid: str | None = None
    machine_uuid: str | UUID | None = None
    uid: int | None = None
    record_type: str | None = None

    def validated(self) -> "ObjectSelector":
        supplied = [
            self.euid is not None and str(self.euid).strip() != "",
            self.machine_uuid is not None and str(self.machine_uuid).strip() != "",
            self.uid is not None,
        ]
        if sum(supplied) != 1:
            raise ValueError("exactly one of euid, machine_uuid, or uid is required")
        kind = str(self.record_type or "").strip().lower() or None
        if kind is not None and kind not in RECORD_MODELS:
            raise ValueError("record_type must be template, instance, or lineage")
        if self.machine_uuid is not None and kind not in {None, "instance"}:
            raise ValueError("machine_uuid selects only instance records")
        if self.uid is not None and kind is None:
            raise ValueError("record_type is required with uid")
        if self.uid is not None and int(self.uid) < 1:
            raise ValueError("uid must be a positive integer")
        if self.machine_uuid is not None:
            UUID(str(self.machine_uuid))
        return ObjectSelector(
            euid=str(self.euid).strip() if self.euid is not None else None,
            machine_uuid=(
                str(UUID(str(self.machine_uuid)))
                if self.machine_uuid is not None
                else None
            ),
            uid=int(self.uid) if self.uid is not None else None,
            record_type=kind,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "euid": self.euid,
                "machine_uuid": (
                    str(self.machine_uuid) if self.machine_uuid is not None else None
                ),
                "uid": self.uid,
                "record_type": self.record_type,
            }.items()
            if value is not None
        }


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def object_payload(obj: Any, record_type: str) -> dict[str, Any]:
    """Return a scripting-safe object view without ORM internals."""

    payload = {
        "record_type": record_type,
        "uid": getattr(obj, "uid", None),
        "euid": getattr(obj, "euid", None),
        "machine_uuid": (
            str(getattr(obj, "machine_uuid"))
            if getattr(obj, "machine_uuid", None) is not None
            else None
        ),
        "name": getattr(obj, "name", None),
        "domain_code": getattr(obj, "domain_code", None),
        "issuer_app_code": getattr(obj, "issuer_app_code", None),
        "tenant_id": (
            str(getattr(obj, "tenant_id"))
            if getattr(obj, "tenant_id", None) is not None
            else None
        ),
        "category": getattr(obj, "category", None),
        "type": getattr(obj, "type", None),
        "subtype": getattr(obj, "subtype", None),
        "version": getattr(obj, "version", None),
        "bstatus": getattr(obj, "bstatus", None),
        "json_addl": getattr(obj, "json_addl", None),
        "is_deleted": bool(getattr(obj, "is_deleted", False)),
        "created_dt": _iso(getattr(obj, "created_dt", None)),
        "modified_dt": _iso(getattr(obj, "modified_dt", None)),
    }
    if record_type == "instance":
        payload["template_uid"] = getattr(obj, "template_uid", None)
    elif record_type == "lineage":
        payload.update(
            {
                "parent_instance_uid": getattr(obj, "parent_instance_uid", None),
                "child_instance_uid": getattr(obj, "child_instance_uid", None),
                "relationship_type": getattr(obj, "relationship_type", None),
            }
        )
    elif record_type == "template":
        payload.update(
            {
                "instance_prefix": getattr(obj, "instance_prefix", None),
                "instance_polymorphic_identity": getattr(
                    obj, "instance_polymorphic_identity", None
                ),
                "validator_ref": getattr(obj, "validator_ref", None),
                "json_addl_schema": getattr(obj, "json_addl_schema", None),
            }
        )
    return payload


def resolve_object(session: Any, selector: ObjectSelector) -> tuple[Any, str]:
    """Resolve exactly one object or raise a clear missing/ambiguous error."""

    normalized = selector.validated()
    if normalized.machine_uuid is not None:
        stmt = select(generic_instance).where(
            generic_instance.machine_uuid == UUID(str(normalized.machine_uuid))
        )
        row = session.execute(stmt).scalar_one_or_none()
        if row is None:
            raise LookupError("object not found for exact machine_uuid selector")
        return row, "instance"
    if normalized.uid is not None:
        kind = str(normalized.record_type)
        row = session.execute(
            select(RECORD_MODELS[kind]).where(RECORD_MODELS[kind].uid == normalized.uid)
        ).scalar_one_or_none()
        if row is None:
            raise LookupError("object not found for exact uid selector")
        return row, kind

    kinds = [normalized.record_type] if normalized.record_type else list(RECORD_MODELS)
    matches: list[tuple[Any, str]] = []
    for kind in kinds:
        if kind is None:  # Defensive guard for static and runtime callers.
            raise RuntimeError("record type resolution failed")
        model = RECORD_MODELS[kind]
        row = session.execute(
            select(model).where(model.euid == normalized.euid)
        ).scalar_one_or_none()
        if row is not None:
            matches.append((row, kind))
    if not matches:
        raise LookupError("object not found for exact euid selector")
    if len(matches) > 1:
        raise RuntimeError("euid selector is ambiguous across record types")
    return matches[0]


def get_object(
    session: Any,
    selector: ObjectSelector,
    *,
    include_deleted: bool = False,
) -> dict[str, Any]:
    obj, record_type = resolve_object(session, selector)
    if getattr(obj, "is_deleted", False) and not include_deleted:
        raise LookupError("object is soft-deleted; pass include_deleted explicitly")
    return object_payload(obj, record_type)


def _mutation_receipt(
    *,
    operation: str,
    selector: ObjectSelector,
    obj: Any,
    record_type: str,
    actor: str,
    dry_run: bool,
    changes: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "format": "tapdb.object-operation-receipt/v1",
        "operation": operation,
        "selector": selector.validated().to_dict(),
        "record_type": record_type,
        "euid": getattr(obj, "euid", None),
        "actor": str(actor or "").strip(),
        "recorded_at": datetime.now(UTC).isoformat(),
        "dry_run": bool(dry_run),
        "applied": not dry_run,
        "changes": dict(changes),
    }


def _reject_external_reference_mutation(obj: Any, record_type: str) -> None:
    if record_type == "instance" and _is_xrf_coordinates(obj):
        raise PermissionError(
            "core external references are writable only through "
            "daylily_tapdb.external_references.ExternalReferenceService"
        )


def update_object(
    session: Any,
    selector: ObjectSelector,
    changes: Mapping[str, Any],
    *,
    actor: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    obj, record_type = resolve_object(session, selector)
    if record_type == "template":
        raise PermissionError("templates are read-only through objects")
    _reject_external_reference_mutation(obj, record_type)
    if getattr(obj, "is_deleted", False):
        raise ValueError("cannot update a soft-deleted object")
    if not isinstance(changes, Mapping) or not changes:
        raise ValueError("at least one update field is required")
    unknown = sorted(set(changes).difference(UPDATE_ALLOWLISTS[record_type]))
    if unknown:
        raise ValueError(
            f"field(s) not allowed for {record_type}: {', '.join(unknown)}"
        )
    normalized: dict[str, Any] = {}
    for field, value in changes.items():
        if field in {"name", "bstatus"}:
            value = str(value or "").strip()
            if not value:
                raise ValueError(f"{field} must be a non-empty string")
        elif field == "json_addl" and not isinstance(value, dict):
            raise ValueError("json_addl must be a JSON object")
        normalized[field] = value
    delta = {
        field: {"old": getattr(obj, field, None), "new": value}
        for field, value in normalized.items()
        if getattr(obj, field, None) != value
    }
    if not dry_run:
        for field, change in delta.items():
            setattr(obj, field, change["new"])
        session.flush()
    return _mutation_receipt(
        operation="update",
        selector=selector,
        obj=obj,
        record_type=record_type,
        actor=actor,
        dry_run=dry_run,
        changes=delta,
    )


def repair_object(
    session: Any,
    selector: ObjectSelector,
    *,
    domain_code: str,
    actor: str,
    reason: str,
    repair_payload: dict[str, Any],
    dry_run: bool = True,
) -> dict[str, Any]:
    obj, record_type = resolve_object(session, selector)
    if record_type == "template":
        raise PermissionError("templates are read-only through objects")
    if not str(reason or "").strip():
        raise ValueError("reason is required")
    if not isinstance(repair_payload, dict):
        raise ValueError("repair_payload must be a JSON object")
    changes: dict[str, Any] = {
        "subject_mutated": False,
        "reason": str(reason).strip(),
        "repair_payload": repair_payload,
    }
    if not dry_run:
        changes["repair_record"] = create_repair_record(
            session,
            domain_code=str(domain_code),
            subject_euid=str(obj.euid),
            actor=actor,
            reason=reason,
            repair_payload=repair_payload,
            governance_context={"surface": "object_operations"},
        )
    return _mutation_receipt(
        operation="repair",
        selector=selector,
        obj=obj,
        record_type=record_type,
        actor=actor,
        dry_run=dry_run,
        changes=changes,
    )


def soft_delete_object(
    session: Any,
    selector: ObjectSelector,
    *,
    actor: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    obj, record_type = resolve_object(session, selector)
    if record_type == "template":
        raise PermissionError("templates are read-only through objects")
    _reject_external_reference_mutation(obj, record_type)
    if getattr(obj, "is_deleted", False):
        raise ValueError("object is already soft-deleted")
    if not dry_run:
        obj.is_deleted = True
        session.flush()
    return _mutation_receipt(
        operation="soft-delete",
        selector=selector,
        obj=obj,
        record_type=record_type,
        actor=actor,
        dry_run=dry_run,
        changes={"is_deleted": {"old": False, "new": True}},
    )


__all__ = [
    "ObjectSelector",
    "UPDATE_ALLOWLISTS",
    "get_object",
    "object_payload",
    "repair_object",
    "resolve_object",
    "soft_delete_object",
    "update_object",
]
