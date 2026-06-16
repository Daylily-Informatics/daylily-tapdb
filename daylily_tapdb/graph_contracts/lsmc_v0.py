"""LSMC v0 graph contract helpers.

The v0 graph contract rides on existing TapDB lineage rows. It does not add
tables or columns; canonical edge metadata is stored in the least-invasive
existing JSON metadata location used by current TapDB conventions.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm.attributes import flag_modified

LSMC_V0_NODE_TYPES = frozenset(
    {
        "Order",
        "OrderTest",
        "SubjectRef",
        "ProductVersion",
        "SatisfactionPlanVersion",
        "FulfillmentInstance",
        "FulfillmentSlot",
        "AccessionCase",
        "Container",
        "Material",
        "WorkflowRun",
        "Artifact",
        "DecisionRecord",
    }
)

LSMC_V0_EDGE_TYPES = frozenset(
    {
        "ORDER_HAS_TEST",
        "TEST_HAS_SUBJECT",
        "ORDERED_AS",
        "PRODUCT_ALLOWS_PLAN_VERSION",
        "FULFILLS_TEST",
        "USES_PLAN_VERSION",
        "HAS_SLOT",
        "ACCESSION_OBSERVED",
        "ACCESSION_LINKS_TEST",
        "CONTAINS",
        "HOLDS_MATERIAL",
        "MATERIAL_FROM_SUBJECT",
        "SLOT_SATISFIED_BY",
        "RUN_FOR_FULFILLMENT",
        "RUN_CONSUMED",
        "RUN_PRODUCED",
        "DERIVED_FROM",
        "DECIDES_ON",
        "USES_EVIDENCE",
        "SUPERSEDES",
    }
)

_METADATA_PARENT_KEY = "properties"
_METADATA_KEY = "v0_edge"


def _mark_json_addl_dirty(lineage: Any) -> None:
    try:
        flag_modified(lineage, "json_addl")
    except (AttributeError, TypeError, KeyError):
        # Unit tests and small fakes are not SQLAlchemy-mapped rows.
        return


def metadata_location_label() -> str:
    """Return the documented v0 edge metadata location."""

    return f"json_addl.{_METADATA_PARENT_KEY}.{_METADATA_KEY}"


def _clean_text(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _iso(value: Any, *, field: str) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return _clean_text(value, field=field)


def canonical_edge_type(value: Any) -> str | None:
    """Return a canonical v0 edge type only for exact v0 edge names."""

    text = str(value or "").strip()
    if not text:
        return None
    return text if text in LSMC_V0_EDGE_TYPES else None


def is_strict_canonical_edge_type(value: Any) -> bool:
    """Return true only for explicit uppercase canonical v0 edge names."""

    text = str(value or "").strip()
    return text in LSMC_V0_EDGE_TYPES


def _normalize_evidence_refs(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("evidence_refs must be a non-empty list")
    refs: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if isinstance(item, str):
            text = item.strip()
            if not text:
                raise ValueError(f"evidence_refs[{index}] is empty")
            refs.append({"euid": text})
            continue
        if isinstance(item, dict):
            euid = str(item.get("euid") or item.get("root_euid") or "").strip()
            if not euid:
                raise ValueError(f"evidence_refs[{index}].euid is required")
            cleaned = dict(item)
            cleaned["euid"] = euid
            refs.append(cleaned)
            continue
        raise ValueError(f"evidence_refs[{index}] must be a string or object")
    return refs


def _normalize_semantic_endpoint(
    value: Any,
    *,
    fallback_euid: Any,
    fallback_role: str,
    field: str,
) -> dict[str, str]:
    if isinstance(value, dict):
        euid = _clean_text(value.get("euid") or fallback_euid, field=f"{field}.euid")
        role = str(value.get("role") or fallback_role).strip() or fallback_role
        system = str(value.get("system") or "").strip()
        payload = {"euid": euid, "role": role}
        if system:
            payload["system"] = system
        return payload
    return {
        "euid": _clean_text(fallback_euid, field=f"{field}.euid"),
        "role": fallback_role,
    }


def build_v0_edge_metadata(
    *,
    contract: Any | None = None,
    edge_type: Any,
    asserted_by_system: Any,
    evidence_refs: Any,
    correlation_id: Any,
    causation_id: Any,
    semantic_source: Any | None = None,
    semantic_target: Any | None = None,
    asserted_at: Any | None = None,
    valid_from: Any | None = None,
    valid_to: Any | None = None,
    edge_state: Any = "active",
) -> dict[str, Any]:
    """Validate and normalize canonical v0 edge metadata."""

    del contract
    canonical = canonical_edge_type(edge_type)
    if canonical is None or canonical not in LSMC_V0_EDGE_TYPES:
        raise ValueError(f"Unsupported LSMC v0 edge type: {edge_type!r}")
    asserted = (
        asserted_at.isoformat()
        if isinstance(asserted_at, datetime)
        else str(asserted_at or datetime.now(timezone.utc).isoformat()).strip()
    )
    if not asserted:
        raise ValueError("asserted_at is required")
    payload: dict[str, Any] = {
        "contract": "LSMC_V0",
        "edge_type": canonical,
        "semantic_source": _normalize_semantic_endpoint(
            semantic_source,
            fallback_euid=None,
            fallback_role="source",
            field="semantic_source",
        ),
        "semantic_target": _normalize_semantic_endpoint(
            semantic_target,
            fallback_euid=None,
            fallback_role="target",
            field="semantic_target",
        ),
        "asserted_by_system": _clean_text(
            asserted_by_system, field="asserted_by_system"
        ),
        "asserted_at": asserted,
        "evidence_refs": _normalize_evidence_refs(evidence_refs),
        "correlation_id": _clean_text(correlation_id, field="correlation_id"),
        "causation_id": _clean_text(causation_id, field="causation_id"),
        "edge_state": _clean_text(edge_state, field="edge_state"),
    }
    if valid_from is not None:
        payload["valid_from"] = _iso(valid_from, field="valid_from")
    if valid_to is not None:
        payload["valid_to"] = _iso(valid_to, field="valid_to")
    return payload


def v0_edge_metadata_from_json_addl(json_addl: Any) -> dict[str, Any] | None:
    if not isinstance(json_addl, dict):
        return None
    properties = json_addl.get(_METADATA_PARENT_KEY)
    if not isinstance(properties, dict):
        return None
    metadata = properties.get(_METADATA_KEY)
    return metadata if isinstance(metadata, dict) else None


def v0_edge_metadata_from_lineage(lineage: Any) -> dict[str, Any] | None:
    return v0_edge_metadata_from_json_addl(getattr(lineage, "json_addl", None))


def attach_v0_edge_metadata(lineage: Any, metadata: dict[str, Any]) -> dict[str, Any]:
    """Attach validated v0 metadata to a lineage row's existing JSON metadata."""

    normalized = build_v0_edge_metadata(**metadata)
    json_addl = getattr(lineage, "json_addl", None)
    if not isinstance(json_addl, dict):
        json_addl = {}
    properties = json_addl.get(_METADATA_PARENT_KEY)
    if not isinstance(properties, dict):
        properties = {}
        json_addl[_METADATA_PARENT_KEY] = properties
    properties[_METADATA_KEY] = normalized
    lineage.json_addl = json_addl
    _mark_json_addl_dirty(lineage)
    return normalized


def describe_lineage_contract(lineage: Any) -> dict[str, Any]:
    """Return public v0 contract status for a lineage row."""

    raw = v0_edge_metadata_from_lineage(lineage)
    if raw is not None:
        try:
            normalized = build_v0_edge_metadata(**raw)
        except (TypeError, ValueError) as exc:
            return {
                "contract": "LSMC_V0",
                "compliance_status": "invalid",
                "metadata_location": metadata_location_label(),
                "error": str(exc),
                "raw": raw,
            }
        return {
            "contract": "LSMC_V0",
            "compliance_status": "canonical",
            "metadata_location": metadata_location_label(),
            **normalized,
        }
    return {
        "contract": None,
        "compliance_status": "generic",
        "metadata_location": metadata_location_label(),
    }
