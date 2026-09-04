"""Read-only audit for canonical and legacy external-reference state."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from daylily_tapdb.external_references import (
    ExternalIdentifierTarget,
    ExternalReferenceContractError,
    TapDBObjectTarget,
    _is_xrf_coordinates,
    _target_from_reference,
)
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template

_EXPECTED_TEMPLATES = {
    "tapdb_object": {
        "target_service_id",
        "target_object_euid",
        "target_tenant_id",
        "target_object_kind",
    },
    "opaque": {"namespace", "kind", "value", "scope", "canonical_uri"},
}


def _properties(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("properties")
    return dict(value) if isinstance(value, dict) else None


def _template_state(session: Any) -> tuple[list[dict[str, Any]], int]:
    rows = (
        session.query(generic_template)
        .filter_by(category="reference", type="external_identifier", version="1.0")
        .order_by(generic_template.uid.asc())
        .all()
    )
    result: list[dict[str, Any]] = []
    violations = 0
    for subtype, expected_fields in _EXPECTED_TEMPLATES.items():
        matches = [row for row in rows if row.subtype == subtype]
        valid = False
        diagnostics: list[str] = []
        if len(matches) != 1:
            diagnostics.append(f"expected one template, found {len(matches)}")
        else:
            row = matches[0]
            schema = (
                row.json_addl_schema if isinstance(row.json_addl_schema, dict) else {}
            )
            schema_properties = schema.get("properties")
            object_schema = (
                schema_properties.get("properties")
                if isinstance(schema_properties, dict)
                else None
            )
            fields = (
                set(object_schema.get("properties", {}))
                if isinstance(object_schema, dict)
                and isinstance(object_schema.get("properties"), dict)
                else set()
            )
            additional = (
                object_schema.get("additionalProperties")
                if isinstance(object_schema, dict)
                else None
            )
            if fields != expected_fields or additional is not False:
                diagnostics.append("template schema is not the exact canonical shape")
            if row.instance_prefix != "XRF":
                diagnostics.append("template instance_prefix is not XRF")
            if row.is_deleted or row.bstatus != "active":
                diagnostics.append("template is not active")
            valid = not diagnostics
        if not valid:
            violations += 1
        result.append(
            {
                "subtype": subtype,
                "status": "valid" if valid else "invalid",
                "template_euids": [str(row.euid) for row in matches],
                "diagnostics": diagnostics,
            }
        )
    return result, violations


def audit_external_references(
    session: Any, *, sample_limit: int = 25
) -> dict[str, Any]:
    """Inspect external-reference state without flushing or mutating the session.

    Samples contain only TapDB-owned EUIDs, relationship names, and field names.
    Opaque identifier values, canonical URIs, and remote credentials are never
    emitted.
    """

    if (
        not isinstance(sample_limit, int)
        or isinstance(sample_limit, bool)
        or not 1 <= sample_limit <= 100
    ):
        raise ValueError("sample_limit must be an integer from 1 through 100")

    seed_state, seed_violations = _template_state(session)
    references = (
        session.query(generic_instance)
        .filter(
            generic_instance.category == "reference",
            generic_instance.type == "external_identifier",
            generic_instance.subtype.in_(("tapdb_object", "opaque")),
            generic_instance.version == "1.0",
        )
        .order_by(generic_instance.uid.asc())
        .all()
    )
    reference_uids = {row.uid for row in references}
    reference_euids = {str(row.euid) for row in references}
    scope_counts = {
        "tapdb_global": 0,
        "opaque_tenant": 0,
        "opaque_public_global": 0,
        "deleted": 0,
    }
    malformed: list[dict[str, str]] = []
    for reference in references:
        try:
            target = _target_from_reference(reference)
            if isinstance(target, TapDBObjectTarget):
                if reference.tenant_id is not None:
                    raise ExternalReferenceContractError(
                        "federated TapDB XRF identity must be global"
                    )
                scope_counts["tapdb_global"] += 1
            elif isinstance(target, ExternalIdentifierTarget):
                if target.scope == "tenant":
                    if reference.tenant_id != target.tenant_id:
                        raise ExternalReferenceContractError(
                            "tenant XRF identity scope does not match its target"
                        )
                    scope_counts["opaque_tenant"] += 1
                else:
                    if reference.tenant_id is not None:
                        raise ExternalReferenceContractError(
                            "public-global XRF identity must not carry tenant_id"
                        )
                    scope_counts["opaque_public_global"] += 1
            if reference.is_deleted:
                scope_counts["deleted"] += 1
        except (ExternalReferenceContractError, ValueError) as exc:
            malformed.append({"euid": str(reference.euid), "reason": str(exc)[:256]})

    raw_graph_samples: list[dict[str, str]] = []
    pseudo_edge_samples: list[dict[str, Any]] = []
    raw_graph_total = 0
    pseudo_edge_total = 0
    instance_rows = (
        session.query(generic_instance.euid, generic_instance.json_addl)
        .order_by(generic_instance.uid.asc())
        .yield_per(1_000)
    )
    for euid, json_addl in instance_rows:
        properties = _properties(json_addl)
        if properties is None:
            continue
        external_payload = properties.get("external_payload")
        if isinstance(external_payload, dict) and "tapdb_graph" in external_payload:
            raw_graph_total += 1
            if len(raw_graph_samples) < sample_limit:
                raw_graph_samples.append({"euid": str(euid)})
        copied_fields = (
            []
            if str(euid) in reference_euids
            else sorted(
                key
                for key in properties
                if isinstance(key, str)
                and (
                    key in {"object_euid", "target_object_euid"}
                    or key.endswith("_object_euid")
                )
            )
        )
        if copied_fields:
            pseudo_edge_total += 1
            if len(pseudo_edge_samples) < sample_limit:
                pseudo_edge_samples.append({"euid": str(euid), "fields": copied_fields})

    lineage_rows = (
        session.query(generic_instance_lineage)
        .filter(generic_instance_lineage.child_instance_uid.in_(reference_uids or {-1}))
        .order_by(generic_instance_lineage.uid.asc())
        .all()
    )
    duplicate_groups: dict[tuple[int, int, str], list[generic_instance_lineage]] = (
        defaultdict(list)
    )
    for lineage in lineage_rows:
        child = getattr(lineage, "child_instance", None)
        if child is not None and _is_xrf_coordinates(child):
            duplicate_groups[
                (
                    int(lineage.parent_instance_uid),
                    int(lineage.child_instance_uid),
                    str(lineage.relationship_type),
                )
            ].append(lineage)
    duplicates = [rows for rows in duplicate_groups.values() if len(rows) > 1]
    duplicate_samples = []
    for rows in duplicates[:sample_limit]:
        first = rows[0]
        duplicate_samples.append(
            {
                "source_euid": str(first.parent_instance.euid),
                "reference_euid": str(first.child_instance.euid),
                "relationship_type": str(first.relationship_type),
                "lineage_euids": [str(row.euid) for row in rows],
            }
        )

    violation_count = (
        seed_violations
        + len(malformed)
        + raw_graph_total
        + pseudo_edge_total
        + len(duplicates)
    )
    return {
        "contract": "tapdb.external-reference-audit/v1",
        "ok": violation_count == 0,
        "read_only": True,
        "sample_limit": sample_limit,
        "seed_state": seed_state,
        "canonical_references_by_scope": scope_counts,
        "violations": {
            "total": violation_count,
            "malformed_xrfs": {
                "count": len(malformed),
                "samples": malformed[:sample_limit],
            },
            "raw_graph_metadata": {
                "count": raw_graph_total,
                "samples": raw_graph_samples,
            },
            "copied_pseudo_edge_fields": {
                "count": pseudo_edge_total,
                "samples": pseudo_edge_samples,
            },
            "duplicate_historical_links": {
                "count": len(duplicates),
                "samples": duplicate_samples,
            },
        },
    }


__all__ = ["audit_external_references"]
