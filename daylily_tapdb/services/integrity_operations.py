"""Shared typed operations for Python, CLI and authenticated HTTP adapters."""

from __future__ import annotations
from dataclasses import asdict
from datetime import datetime
from typing import Any
from uuid import UUID

from daylily_tapdb.audit import query_audit_trail
from daylily_tapdb.external_references import (
    ExternalReferenceService,
    ExternalIdentifierTarget,
    TapDBObjectTarget,
    ExternalLinkSpec,
)
from daylily_tapdb.history import HistoryService
from daylily_tapdb.services.object_operations import ObjectSelector, resolve_object

READ_OPERATIONS = {
    "status",
    "boundary",
    "object-at",
    "graph-at",
    "plan-correction",
    "resolve",
    "audit",
}
WRITE_OPERATIONS = {
    "apply-correction",
    "register",
    "annotate",
    "attach",
    "detach",
    "reconcile",
}


def target_from_payload(payload: dict[str, Any]):
    value = dict(payload)
    kind = value.pop("target_type")
    if kind == "tapdb_object":
        if value.get("target_tenant_id") is not None:
            value["target_tenant_id"] = UUID(value["target_tenant_id"])
        return TapDBObjectTarget(**value)
    if kind == "opaque":
        if value.get("tenant_id") is not None:
            value["tenant_id"] = UUID(value["tenant_id"])
        return ExternalIdentifierTarget(**value)
    raise ValueError("target_type must be opaque or tapdb_object")


def identity(obj: Any) -> dict[str, Any] | None:
    if obj is None:
        return None
    return {
        "euid": obj.euid,
        "uid": obj.uid,
        "record_revision": obj.record_revision,
        "domain_code": obj.domain_code,
        "issuer_app_code": obj.issuer_app_code,
        "tenant_id": str(obj.tenant_id) if obj.tenant_id else None,
        "is_deleted": obj.is_deleted,
    }


def execute_integrity_operation(
    session: Any, operation: str, payload: dict[str, Any], *, owner_validators=None
):
    if operation not in READ_OPERATIONS | WRITE_OPERATIONS:
        raise ValueError("unknown integrity operation")
    history = HistoryService(session, owner_validators=owner_validators)
    references = ExternalReferenceService(session)
    if operation == "status":
        return history.describe()
    if operation == "boundary":
        return {"boundary": history.capture_boundary()}
    if operation == "object-at":
        return history.object_at(**payload)
    if operation == "graph-at":
        return history.graph_at(**payload)
    if operation == "plan-correction":
        return history.plan_correction(**payload)
    if operation == "apply-correction":
        return history.apply_correction(payload)
    if operation == "audit":
        values = dict(payload)
        for key in ("since", "until"):
            if key in values:
                values[key] = datetime.fromisoformat(values[key])
        if "cursor" in values:
            values["cursor"] = (
                datetime.fromisoformat(values["cursor"][0]),
                int(values["cursor"][1]),
            )
        return {"entries": [asdict(e) for e in query_audit_trail(session, **values)]}
    if operation in {"resolve", "register"}:
        return {
            "reference": identity(
                getattr(references, operation)(target_from_payload(payload))
            )
        }
    obj, _ = resolve_object(
        session, ObjectSelector(euid=payload["source_euid"], record_type="instance")
    )
    if operation == "annotate":
        return {
            "annotation": identity(
                references.annotate(
                    obj, **{k: v for k, v in payload.items() if k != "source_euid"}
                )
            )
        }
    if operation == "reconcile":
        desired = [
            ExternalLinkSpec(
                target=target_from_payload(item["target"]),
                relationship_type=item["relationship_type"],
                assertion_authority=payload["assertion_authority"],
                asserted_at=datetime.fromisoformat(item["asserted_at"]),
                assertion_provenance=item["assertion_provenance"],
            )
            for item in payload["desired"]
        ]
        results = references.reconcile(
            obj,
            payload["assertion_authority"],
            desired,
            expected_source_revision=payload["expected_revision"],
            expected_lineage_revisions=payload["expected_lineage_revisions"],
            deactivated_at=datetime.fromisoformat(payload["deactivated_at"]),
            deactivation_provenance=payload["deactivation_provenance"],
        )
        return {
            "results": [
                {
                    "status": r.status,
                    "reference": identity(r.reference),
                    "lineage": identity(r.lineage),
                }
                for r in results
            ]
        }
    target = target_from_payload(payload["target"])
    if operation == "attach":
        result = references.attach(
            obj,
            ExternalLinkSpec(
                target=target,
                relationship_type=payload["relationship_type"],
                assertion_authority=payload["assertion_authority"],
                asserted_at=datetime.fromisoformat(payload["asserted_at"]),
                assertion_provenance=payload["assertion_provenance"],
            ),
            expected_source_revision=payload["expected_revision"],
            expected_lineage_revision=payload.get("expected_lineage_revision"),
        )
    else:
        result = references.detach(
            obj,
            target,
            expected_source_revision=payload["expected_revision"],
            expected_lineage_revision=payload.get("expected_lineage_revision"),
            relationship_type=payload["relationship_type"],
            assertion_authority=payload["assertion_authority"],
            deactivated_at=datetime.fromisoformat(payload["deactivated_at"]),
            deactivation_provenance=payload["deactivation_provenance"],
        )
    return {
        "status": result.status,
        "reference": identity(result.reference),
        "lineage": identity(result.lineage),
    }
