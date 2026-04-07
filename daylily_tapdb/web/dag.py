"""Canonical DAG contract surface for TapDB-backed services."""

from __future__ import annotations

import importlib
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from daylily_tapdb.cli.context import resolve_context, set_cli_context

CONTRACT_VERSION = "dag:v1"

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


@lru_cache(maxsize=8)
def _load_admin_main(config_path: str, env_name: str):
    resolved_config = str(config_path or "").strip()
    resolved_env = str(env_name or "").strip().lower()
    set_cli_context(config_path=resolved_config, env_name=resolved_env)
    admin_main = importlib.import_module("admin.main")
    return importlib.reload(admin_main)


def _service_name_for(config_path: str, env_name: str, service_name: str | None) -> str:
    normalized = str(service_name or "").strip()
    if normalized:
        return normalized
    context = resolve_context(
        require_keys=True,
        config_path=config_path,
        env_name=env_name,
    )
    return str(context.client_id or "tapdb").strip() or "tapdb"


def _object_detail_payload(
    admin_main,
    obj: Any,
    *,
    record_type: str,
    service_name: str,
) -> dict[str, Any]:
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
        "external_refs": admin_main._external_ref_payloads(obj),
    }


def _node_payload(
    obj: Any,
    *,
    record_type: str,
    service_name: str,
) -> dict[str, Any]:
    category = str(getattr(obj, "category", "") or "generic").strip().lower() or "generic"
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
            "relationship_type": getattr(lineage, "relationship_type", None) or "related",
            "system": service_name,
            "record_type": "lineage",
        }
    }


def _build_graph_payload(
    admin_main,
    obj: Any,
    *,
    record_type: str,
    service_name: str,
    depth: int,
) -> dict[str, Any]:
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
        nodes.append(_node_payload(instance, record_type="instance", service_name=service_name))

        for lineage in getattr(instance, "parent_of_lineages").filter_by(is_deleted=False):
            edge_euid = str(getattr(lineage, "euid", "") or "").strip()
            if edge_euid and edge_euid not in visited_edges:
                payload = _lineage_edge_payload(lineage, service_name=service_name)
                if payload is not None:
                    edges.append(payload)
                    visited_edges.add(edge_euid)
            traverse(getattr(lineage, "child_instance", None), current_depth + 1)

        for lineage in getattr(instance, "child_of_lineages").filter_by(is_deleted=False):
            edge_euid = str(getattr(lineage, "euid", "") or "").strip()
            if edge_euid and edge_euid not in visited_edges:
                payload = _lineage_edge_payload(lineage, service_name=service_name)
                if payload is not None:
                    edges.append(payload)
                    visited_edges.add(edge_euid)
            traverse(getattr(lineage, "parent_instance", None), current_depth + 1)

    traverse(obj, 0)
    if not nodes:
        nodes.append(_node_payload(obj, record_type=record_type, service_name=service_name))
    return {"elements": {"nodes": nodes, "edges": edges}}


def build_dag_capability_advertisement(
    *,
    base_path: str = "/api/dag",
    auth: str = "operator_or_service_token",
) -> dict[str, Any]:
    """Return canonical obs_services-style metadata for the DAG contract."""

    normalized_base = "/" + str(base_path or "/api/dag").strip().strip("/")
    return {
        "endpoints": [
            {
                "path": f"{normalized_base}/object/{{euid}}",
                "auth": auth,
                "kind": "dag_exact_lookup",
            },
            {
                "path": f"{normalized_base}/data",
                "auth": auth,
                "kind": "dag_native_graph",
            },
            {
                "path": f"{normalized_base}/external",
                "auth": auth,
                "kind": "dag_external_graph",
            },
            {
                "path": f"{normalized_base}/external/object",
                "auth": auth,
                "kind": "dag_external_object",
            },
        ],
        "extensions": ["tapdb.dag_v1"],
        "capabilities": [
            "exact_lookup",
            "native_graph",
            "external_graph_expansion",
        ],
        "contract_version": CONTRACT_VERSION,
    }


def create_tapdb_dag_router(
    *,
    config_path: str,
    env_name: str,
    service_name: str | None = None,
) -> APIRouter:
    """Build the canonical `/api/dag/*` router for a TapDB-backed service."""

    admin_main = _load_admin_main(str(config_path or "").strip(), str(env_name or "").strip())
    resolved_service_name = _service_name_for(config_path, env_name, service_name)
    router = APIRouter()

    @router.get("/api/dag/object/{euid}")
    async def dag_object_detail(euid: str) -> dict[str, Any]:
        with admin_main.get_db() as conn:
            with conn.session_scope() as session:
                obj, record_type = admin_main._find_object_by_euid(session, euid)
                if obj is None or not record_type:
                    raise HTTPException(status_code=404, detail=f"Object not found: {euid}")
                return _object_detail_payload(
                    admin_main,
                    obj,
                    record_type=record_type,
                    service_name=resolved_service_name,
                )

    @router.get("/api/dag/data")
    async def dag_graph_data(
        start_euid: str,
        depth: int = Query(4, ge=0, le=10),
    ) -> dict[str, Any]:
        with admin_main.get_db() as conn:
            with conn.session_scope() as session:
                obj, record_type = admin_main._find_object_by_euid(session, start_euid)
                if obj is None or not record_type:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Object not found: {start_euid}",
                    )
                payload = _build_graph_payload(
                    admin_main,
                    obj,
                    record_type=record_type,
                    service_name=resolved_service_name,
                    depth=depth,
                )
                payload["meta"] = {
                    "start_euid": start_euid,
                    "depth": depth,
                    "owner_service": resolved_service_name,
                    "root_record_type": record_type,
                    "contract_version": CONTRACT_VERSION,
                }
                return payload

    @router.get("/api/dag/external")
    async def dag_external_graph(
        request: Request,
        source_euid: str,
        ref_index: int = Query(..., ge=0),
        depth: int = Query(4, ge=0, le=10),
    ) -> dict[str, Any]:
        with admin_main.get_db() as conn:
            with conn.session_scope() as session:
                obj, _record_type = admin_main._find_object_by_euid(session, source_euid)
                if obj is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Object not found: {source_euid}",
                    )
                try:
                    ref = admin_main.get_external_ref_by_index(obj, ref_index)
                except IndexError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                try:
                    payload = admin_main.fetch_remote_graph(request, ref, depth=depth)
                    out = admin_main.namespace_external_graph(
                        payload,
                        ref=ref,
                        ref_index=ref_index,
                        source_euid=source_euid,
                    )
                    out.setdefault("meta", {})
                    out["meta"].update(
                        {
                            "source_euid": source_euid,
                            "ref_index": ref_index,
                            "depth": depth,
                            "owner_service": resolved_service_name,
                            "contract_version": CONTRACT_VERSION,
                        }
                    )
                    return out
                except Exception as exc:
                    raise HTTPException(status_code=502, detail=str(exc)) from exc

    @router.get("/api/dag/external/object")
    async def dag_external_object_detail(
        request: Request,
        source_euid: str,
        ref_index: int = Query(..., ge=0),
        euid: str = Query(...),
    ) -> dict[str, Any]:
        with admin_main.get_db() as conn:
            with conn.session_scope() as session:
                obj, _record_type = admin_main._find_object_by_euid(session, source_euid)
                if obj is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Object not found: {source_euid}",
                    )
                try:
                    ref = admin_main.get_external_ref_by_index(obj, ref_index)
                except IndexError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                try:
                    payload = admin_main.fetch_remote_object_detail(request, ref, euid=euid)
                    payload.setdefault("system", ref.system)
                    payload.setdefault("contract_version", CONTRACT_VERSION)
                    return payload
                except Exception as exc:
                    raise HTTPException(status_code=502, detail=str(exc)) from exc

    return router
