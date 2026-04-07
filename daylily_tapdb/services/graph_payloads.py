"""Reusable DAG payload builders."""

from __future__ import annotations

from typing import Any

from daylily_tapdb.services.external_refs import external_ref_payloads

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
        "created_dt": (
            getattr(obj, "created_dt", None).isoformat()
            if getattr(obj, "created_dt", None) is not None
            else None
        ),
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
    return {
        "data": {
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
        }
    }


def _lineage_edge_payload(lineage: Any, *, service_name: str) -> dict[str, Any] | None:
    parent = getattr(lineage, "parent_instance", None)
    child = getattr(lineage, "child_instance", None)
    if parent is None or child is None:
        return None
    return {
        "data": {
            "id": getattr(lineage, "euid", None),
            "euid": getattr(lineage, "euid", None),
            "source": getattr(child, "euid", None),
            "target": getattr(parent, "euid", None),
            "relationship_type": getattr(lineage, "relationship_type", None)
            or "related",
            "system": service_name,
            "record_type": "lineage",
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
