"""Reusable DAG payload builders."""

from __future__ import annotations

import hashlib
import json
from collections import deque
from datetime import datetime, timezone
from typing import Any

from daylily_tapdb.graph_contracts import describe_lineage_contract
from daylily_tapdb.services.external_refs import (
    external_ref_payloads,
    is_typed_external_reference,
    project_outbound_typed_references,
    validate_no_untyped_federation_metadata,
)

_CATEGORY_COLORS = {
    "workflow": "#00FF7F",
    "workflow_step": "#ADFF2F",
    "container": "#8B00FF",
    "content": "#00BFFF",
    "equipment": "#FF4500",
    "data": "#FFD700",
    "actor": "#FF69B4",
    "action": "#FF8C00",
    "test_requisition": "#FFA500",
    "health_event": "#DC143C",
    "file": "#00FF00",
    "subject": "#9370DB",
    "lineage": "#C49BFF",
    "generic": "#888888",
}

_GRAPH_PRESENTATION_FIELDS = (
    "role",
    "expected_fanout_max",
    "collapse_by_default",
    "fanout_reason",
)


def _isoformat_attr(obj: Any, attr: str) -> str | None:
    value = getattr(obj, attr, None)
    return value.isoformat() if value is not None else None


def _graph_presentation_payload(obj: Any) -> dict[str, Any]:
    json_addl = getattr(obj, "json_addl", None)
    if not isinstance(json_addl, dict):
        return {}
    properties = json_addl.get("properties")
    if not isinstance(properties, dict):
        return {}
    graph = properties.get("graph")
    if not isinstance(graph, dict):
        return {}
    return {key: graph[key] for key in _GRAPH_PRESENTATION_FIELDS if key in graph}


def build_object_detail_payload(
    obj: Any,
    *,
    record_type: str,
    service_name: str,
) -> dict[str, Any]:
    """Return the canonical object detail payload for the DAG API."""

    json_addl = getattr(obj, "json_addl", None)
    return {
        "uid": getattr(obj, "uid", None),
        "euid": getattr(obj, "euid", None),
        "name": getattr(obj, "name", None),
        "display_label": getattr(obj, "name", None) or getattr(obj, "euid", None),
        "system": service_name,
        "record_type": record_type,
        "category": getattr(obj, "category", None),
        "type": getattr(obj, "type", None),
        "subtype": getattr(obj, "subtype", None),
        "version": getattr(obj, "version", None),
        "bstatus": getattr(obj, "bstatus", None),
        "json_addl": json_addl,
        "href": f"/object/{getattr(obj, 'euid', '')}",
        "created_dt": _isoformat_attr(obj, "created_dt"),
        "modified_dt": _isoformat_attr(obj, "modified_dt"),
        "external_refs": external_ref_payloads(obj),
    }


def _node_payload(
    obj: Any,
    *,
    record_type: str,
    service_name: str,
) -> dict[str, Any]:
    category = (
        str(getattr(obj, "category", "") or "generic").strip().lower() or "generic"
    )
    data = {
        "id": getattr(obj, "euid", None),
        "euid": getattr(obj, "euid", None),
        "display_label": getattr(obj, "name", None) or getattr(obj, "euid", None),
        "name": getattr(obj, "name", None) or getattr(obj, "euid", None),
        "system": service_name,
        "record_type": record_type,
        "category": getattr(obj, "category", None),
        "type": getattr(obj, "type", None),
        "subtype": getattr(obj, "subtype", None),
        "href": f"/object/{getattr(obj, 'euid', '')}",
        "color": _CATEGORY_COLORS.get(category, _CATEGORY_COLORS["generic"]),
        "created_dt": _isoformat_attr(obj, "created_dt"),
        "modified_dt": _isoformat_attr(obj, "modified_dt"),
        "external_refs": external_ref_payloads(obj),
    }
    data.update(_graph_presentation_payload(obj))
    return {"data": data}


def _lineage_edge_payload(lineage: Any, *, service_name: str) -> dict[str, Any] | None:
    parent = getattr(lineage, "parent_instance", None)
    child = getattr(lineage, "child_instance", None)
    if parent is None or child is None:
        return None
    contract = describe_lineage_contract(lineage)
    semantic_source = contract.get("semantic_source")
    semantic_target = contract.get("semantic_target")
    return {
        "data": {
            "id": getattr(lineage, "euid", None),
            "euid": getattr(lineage, "euid", None),
            "source": getattr(child, "euid", None),
            "target": getattr(parent, "euid", None),
            "semantic_source_euid": (
                semantic_source.get("euid")
                if isinstance(semantic_source, dict)
                else None
            )
            or getattr(parent, "euid", None),
            "semantic_target_euid": (
                semantic_target.get("euid")
                if isinstance(semantic_target, dict)
                else None
            )
            or getattr(child, "euid", None),
            "relationship_type": getattr(lineage, "relationship_type", None)
            or "related",
            "system": service_name,
            "record_type": "lineage",
            "v0_edge": contract,
        }
    }


def build_graph_payload(
    obj: Any,
    *,
    record_type: str,
    service_name: str,
    depth: int,
) -> dict[str, Any]:
    """Return the canonical graph payload for the DAG API."""

    if record_type != "instance":
        return {
            "elements": {
                "nodes": [
                    _node_payload(
                        obj,
                        record_type=record_type,
                        service_name=service_name,
                    )
                ],
                "edges": [],
            }
        }

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    visited_nodes: set[str] = set()
    visited_edges: set[str] = set()

    def traverse(instance: Any, current_depth: int) -> None:
        if instance is None:
            return
        euid = str(getattr(instance, "euid", "") or "").strip()
        if not euid or current_depth > depth or euid in visited_nodes:
            return
        visited_nodes.add(euid)
        nodes.append(
            _node_payload(instance, record_type="instance", service_name=service_name)
        )

        for lineage in getattr(instance, "parent_of_lineages").filter_by(
            is_deleted=False
        ):
            edge_euid = str(getattr(lineage, "euid", "") or "").strip()
            if edge_euid and edge_euid not in visited_edges:
                payload = _lineage_edge_payload(lineage, service_name=service_name)
                if payload is not None:
                    edges.append(payload)
                    visited_edges.add(edge_euid)
            traverse(getattr(lineage, "child_instance", None), current_depth + 1)

        for lineage in getattr(instance, "child_of_lineages").filter_by(
            is_deleted=False
        ):
            edge_euid = str(getattr(lineage, "euid", "") or "").strip()
            if edge_euid and edge_euid not in visited_edges:
                payload = _lineage_edge_payload(lineage, service_name=service_name)
                if payload is not None:
                    edges.append(payload)
                    visited_edges.add(edge_euid)
            traverse(getattr(lineage, "parent_instance", None), current_depth + 1)

    traverse(obj, 0)
    if not nodes:
        nodes.append(
            _node_payload(obj, record_type=record_type, service_name=service_name)
        )
    return {"elements": {"nodes": nodes, "edges": edges}}


class DagV2GraphContractError(ValueError):
    """Raised when persisted lineage cannot be represented as a safe DAG v2."""


def _dict_attr(obj: Any, attr: str) -> dict[str, Any]:
    value = getattr(obj, attr, None)
    return dict(value) if isinstance(value, dict) else {}


def _properties(obj: Any) -> dict[str, Any]:
    value = _dict_attr(obj, "json_addl").get("properties")
    return dict(value) if isinstance(value, dict) else {}


def _active_related(value: Any) -> list[Any]:
    if value is None:
        return []
    query = value.filter_by(is_deleted=False) if hasattr(value, "filter_by") else value
    rows = query.all() if hasattr(query, "all") else list(query)
    return sorted(
        (row for row in rows if not bool(getattr(row, "is_deleted", False))),
        key=lambda row: str(getattr(row, "euid", "") or ""),
    )


def _clean_public_properties(obj: Any) -> dict[str, Any]:
    properties = _properties(obj)
    public: dict[str, Any] = {}
    for key, value in properties.items():
        if not isinstance(key, str):
            continue
        normalized_key = key.casefold()
        if normalized_key in {
            "external_payload",
            "base_url",
            "auth",
            "auth_mode",
            "graph_data_path",
            "object_detail_path_template",
        }:
            continue
        if normalized_key.startswith("auth_") or normalized_key.endswith("_url"):
            continue
        public[key] = value
    return public


def _v2_node_presentation(obj: Any) -> dict[str, Any]:
    raw = _properties(obj).get("graph_presentation")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise DagV2GraphContractError("graph_presentation must be an object")
    presentation: dict[str, Any] = {}
    if "role" in raw:
        role = str(raw["role"] or "")
        if role != role.strip() or not role or len(role) > 64:
            raise DagV2GraphContractError(
                "graph_presentation.role must be exact and 1-64 characters"
            )
        presentation["role"] = role
    if "collapse_by_default" in raw:
        if not isinstance(raw["collapse_by_default"], bool):
            raise DagV2GraphContractError(
                "graph_presentation.collapse_by_default must be boolean"
            )
        presentation["collapse_by_default"] = raw["collapse_by_default"]
    if "expected_fanout" in raw:
        fanout = raw["expected_fanout"]
        if not isinstance(fanout, dict):
            raise DagV2GraphContractError(
                "graph_presentation.expected_fanout must be an object"
            )
        relationship_types = fanout.get("relationship_types")
        max_degree = fanout.get("max_degree")
        reason = str(fanout.get("reason") or "")
        if (
            not isinstance(relationship_types, list)
            or not relationship_types
            or any(
                not isinstance(item, str) or not item or item != item.strip()
                for item in relationship_types
            )
        ):
            raise DagV2GraphContractError(
                "expected_fanout.relationship_types must be a non-empty string list"
            )
        if (
            not isinstance(max_degree, int)
            or isinstance(max_degree, bool)
            or max_degree < 1
        ):
            raise DagV2GraphContractError(
                "expected_fanout.max_degree must be a positive integer"
            )
        if reason != reason.strip() or not reason or len(reason) > 512:
            raise DagV2GraphContractError(
                "expected_fanout.reason must be exact and 1-512 characters"
            )
        presentation["expected_fanout"] = {
            "relationship_types": sorted(set(relationship_types)),
            "max_degree": max_degree,
            "reason": reason,
        }
    unsupported = set(raw) - {"role", "collapse_by_default", "expected_fanout"}
    if unsupported:
        raise DagV2GraphContractError(
            "Unsupported graph_presentation field(s): " + ", ".join(sorted(unsupported))
        )
    return presentation


def _v2_node(obj: Any, *, record_type: str, service_id: str) -> dict[str, Any]:
    validate_no_untyped_federation_metadata(obj)
    euid = getattr(obj, "euid", None)
    data = {
        "id": euid,
        "euid": euid,
        "display_label": getattr(obj, "name", None) or euid,
        "service_id": service_id,
        "record_type": record_type,
        "category": getattr(obj, "category", None),
        "type": getattr(obj, "type", None),
        "subtype": getattr(obj, "subtype", None),
        "version": getattr(obj, "version", None),
        "tenant_id": str(getattr(obj, "tenant_id", "") or "") or None,
        "created_dt": _isoformat_attr(obj, "created_dt"),
        "modified_dt": _isoformat_attr(obj, "modified_dt"),
        "properties": _clean_public_properties(obj),
        "external_refs": project_outbound_typed_references(obj),
    }
    data["presentation"] = _v2_node_presentation(obj)
    return {"data": data}


def build_object_detail_v2_payload(
    obj: Any,
    *,
    record_type: str,
    service_id: str,
) -> dict[str, Any]:
    """Return safe v2 object detail without legacy routing metadata."""

    return dict(_v2_node(obj, record_type=record_type, service_id=service_id)["data"])


def _scope_tuple(obj: Any) -> tuple[str, str, str | None]:
    tenant = str(getattr(obj, "tenant_id", "") or "") or None
    return (
        str(getattr(obj, "domain_code", "") or ""),
        str(getattr(obj, "issuer_app_code", "") or ""),
        tenant,
    )


def _validate_lineage_scope(root: Any, lineage: Any, neighbor: Any) -> None:
    root_domain, root_owner, root_tenant = _scope_tuple(root)
    neighbor_domain, neighbor_owner, neighbor_tenant = _scope_tuple(neighbor)
    lineage_domain, lineage_owner, lineage_tenant = _scope_tuple(lineage)
    if not root_domain or not root_owner:
        raise DagV2GraphContractError("DAG v2 root is missing domain/owner scope")
    if (neighbor_domain, neighbor_owner) != (root_domain, root_owner):
        raise DagV2GraphContractError(
            "Cross-domain or cross-owner lineage is forbidden"
        )
    if (lineage_domain, lineage_owner) != (root_domain, root_owner):
        raise DagV2GraphContractError("Lineage row scope differs from its endpoints")
    if neighbor_tenant == root_tenant and lineage_tenant == root_tenant:
        return
    properties = _properties(lineage)
    if (
        bool(properties.get("approved_global_link"))
        and neighbor_tenant is None
        and lineage_tenant == root_tenant
        and is_typed_external_reference(neighbor)
    ):
        return
    raise DagV2GraphContractError(
        "Cross-tenant lineage is forbidden unless it is an approved typed global link"
    )


def _v2_edge(lineage: Any, *, service_id: str) -> dict[str, Any]:
    parent = getattr(lineage, "parent_instance", None)
    child = getattr(lineage, "child_instance", None)
    if parent is None or child is None:
        raise DagV2GraphContractError("Lineage endpoint could not be resolved")
    parent_euid = str(getattr(parent, "euid", "") or "")
    child_euid = str(getattr(child, "euid", "") or "")
    if not parent_euid or not child_euid:
        raise DagV2GraphContractError("Lineage endpoints must have persisted EUIDs")
    if parent_euid == child_euid or getattr(parent, "uid", None) == getattr(
        child, "uid", None
    ):
        raise DagV2GraphContractError("Self-loop lineage is forbidden")
    properties = _properties(lineage)
    asserted_at = properties.get("asserted_at") or _isoformat_attr(
        lineage, "created_dt"
    )
    provenance = properties.get("assertion_provenance")
    if provenance is None:
        lineage_euid = str(getattr(lineage, "euid", "") or "")
        if not lineage_euid:
            raise DagV2GraphContractError(
                "Lineage must have a persisted EUID for authoritative provenance"
            )
        provenance = f"tapdb.lineage:{lineage_euid}"
    elif (
        not isinstance(provenance, str)
        or not provenance
        or provenance != provenance.strip()
        or len(provenance) > 512
    ):
        raise DagV2GraphContractError(
            "Lineage assertion_provenance must be an exact 1-512 character string"
        )
    return {
        "data": {
            "id": getattr(lineage, "euid", None),
            "euid": getattr(lineage, "euid", None),
            "source": parent_euid,
            "target": child_euid,
            "service_id": service_id,
            "relationship_type": getattr(lineage, "relationship_type", None),
            "presentation": {
                "semantics": getattr(lineage, "relationship_type", None),
                "asserted_at": asserted_at,
                "assertion_provenance": provenance,
                "evidence_refs": properties.get("evidence_refs") or [],
            },
        }
    }


def _assert_acyclic(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> None:
    node_ids = {str(node["data"]["id"]) for node in nodes}
    adjacency: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for edge in edges:
        source = str(edge["data"]["source"])
        target = str(edge["data"]["target"])
        if source in adjacency and target in node_ids:
            adjacency[source].append(target)
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visiting:
            raise DagV2GraphContractError("Cycle detected in persisted lineage")
        if node_id in visited:
            return
        visiting.add(node_id)
        for target in sorted(adjacency[node_id]):
            visit(target)
        visiting.remove(node_id)
        visited.add(node_id)

    for node_id in sorted(node_ids):
        visit(node_id)


def build_graph_v2_payload(
    obj: Any,
    *,
    record_type: str,
    service_id: str,
    depth: int,
    max_nodes: int,
    snapshot_at: datetime | None = None,
) -> dict[str, Any]:
    """Build one bounded, validated, lineage-only DAG v2 snapshot."""

    if not isinstance(depth, int) or isinstance(depth, bool) or depth < 0:
        raise ValueError("depth must be a non-negative integer")
    if not isinstance(max_nodes, int) or isinstance(max_nodes, bool) or max_nodes < 1:
        raise ValueError("max_nodes must be a positive integer")
    snapshot = snapshot_at or datetime.now(timezone.utc)
    if snapshot.tzinfo is None:
        raise ValueError("snapshot_at must include a timezone")

    if record_type != "instance":
        nodes = [_v2_node(obj, record_type=record_type, service_id=service_id)]
        edges: list[dict[str, Any]] = []
        truncated = False
        truncation_reason = None
    else:
        nodes = []
        edges = []
        root_scope = _scope_tuple(obj)
        queue: deque[tuple[Any, int]] = deque([(obj, 0)])
        visited: dict[str, Any] = {}
        included_edges: dict[str, Any] = {}
        truncated = False
        reasons: set[str] = set()
        while queue:
            current, current_depth = queue.popleft()
            current_euid = str(getattr(current, "euid", "") or "")
            if not current_euid or current_euid in visited:
                continue
            if len(visited) >= max_nodes:
                truncated = True
                reasons.add("max_nodes")
                continue
            if _scope_tuple(current)[:2] != root_scope[:2]:
                raise DagV2GraphContractError(
                    "Cross-domain or cross-owner graph node is forbidden"
                )
            visited[current_euid] = current
            relations: list[tuple[Any, Any]] = []
            for lineage in _active_related(
                getattr(current, "parent_of_lineages", None)
            ):
                relations.append((lineage, getattr(lineage, "child_instance", None)))
            for lineage in _active_related(getattr(current, "child_of_lineages", None)):
                relations.append((lineage, getattr(lineage, "parent_instance", None)))
            for lineage, neighbor in sorted(
                relations,
                key=lambda pair: str(getattr(pair[0], "euid", "") or ""),
            ):
                if neighbor is None:
                    raise DagV2GraphContractError(
                        "Lineage endpoint could not be resolved"
                    )
                _validate_lineage_scope(obj, lineage, neighbor)
                if current_depth >= depth:
                    neighbor_euid = str(getattr(neighbor, "euid", "") or "")
                    if neighbor_euid not in visited:
                        truncated = True
                        reasons.add("max_depth")
                    continue
                neighbor_euid = str(getattr(neighbor, "euid", "") or "")
                if (
                    neighbor_euid not in visited
                    and len(visited) + len(queue) >= max_nodes
                ):
                    truncated = True
                    reasons.add("max_nodes")
                    continue
                edge_euid = str(getattr(lineage, "euid", "") or "")
                if edge_euid:
                    included_edges[edge_euid] = lineage
                if neighbor_euid not in visited:
                    queue.append((neighbor, current_depth + 1))
        nodes = [
            _v2_node(node, record_type="instance", service_id=service_id)
            for _euid, node in sorted(visited.items())
        ]
        edges = [
            _v2_edge(lineage, service_id=service_id)
            for _euid, lineage in sorted(included_edges.items())
            if str(getattr(lineage.parent_instance, "euid", "") or "") in visited
            and str(getattr(lineage.child_instance, "euid", "") or "") in visited
        ]
        _assert_acyclic(nodes, edges)
        truncation_reason = "+".join(sorted(reasons)) if reasons else None

    revision_input = {"nodes": nodes, "edges": edges}
    graph_revision = hashlib.sha256(
        json.dumps(
            revision_input, sort_keys=True, separators=(",", ":"), default=str
        ).encode("utf-8")
    ).hexdigest()
    return {
        "elements": {"nodes": nodes, "edges": edges},
        "meta": {
            "contract": "dag:v2",
            "service_id": service_id,
            "graph_revision": graph_revision,
            "snapshot_at": snapshot.astimezone(timezone.utc).isoformat(),
            "truncated": truncated,
            "truncation_reason": truncation_reason,
            "effective_limits": {"max_depth": depth, "max_nodes": max_nodes},
        },
    }
