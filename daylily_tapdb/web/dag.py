"""Canonical DAG contract surface for TapDB-backed services."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from daylily_tapdb.cli.context import resolve_context
from daylily_tapdb.services.external_refs import (
    V1ProxyPolicy,
    fetch_remote_graph,
    fetch_remote_object_detail,
    get_external_ref_by_index,
    namespace_external_graph,
)
from daylily_tapdb.services.graph_payloads import (
    build_graph_payload,
    build_object_detail_payload,
)
from daylily_tapdb.services.object_lookup import find_object_by_euid
from daylily_tapdb.services.object_search import search_objects

from . import runtime as dag_runtime

CONTRACT_VERSION = "dag:v1"
DagAuthDependency = Callable[[Request], Any | Awaitable[Any]]


def _actor_from_auth(authenticated: Any) -> str:
    """Return the authenticated actor used for fail-closed DB attribution."""
    if isinstance(authenticated, dict):
        for key in ("username", "email", "sub", "uid"):
            value = str(authenticated.get(key) or "").strip()
            if value:
                return value
    value = str(getattr(authenticated, "username", "") or "").strip()
    if value:
        return value
    raise HTTPException(status_code=401, detail="tapdb_dag_v1_auth_identity_required")


def _service_name_for(config_path: str, service_name: str | None) -> str:
    normalized = str(service_name or "").strip()
    if normalized:
        return normalized
    context = resolve_context(
        require_keys=True,
        config_path=config_path,
    )
    return str(context.client_id or "tapdb").strip() or "tapdb"


def build_dag_capability_advertisement(
    *,
    base_path: str = "/api/dag",
    auth: str = "operator_or_service_token",
    v1_proxy_enabled: bool = False,
) -> dict[str, Any]:
    """Return canonical obs_services-style metadata for the DAG contract."""

    normalized_base = "/" + str(base_path or "/api/dag").strip().strip("/")
    payload = {
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
                "path": f"{normalized_base}/search",
                "auth": auth,
                "kind": "dag_object_search",
            },
        ],
        "extensions": ["tapdb.dag_v1"],
        "capabilities": [
            "exact_lookup",
            "native_graph",
            "object_search",
        ],
        "external_ref_models": [
            "external_payload.tapdb_graph",
            "typed_external_identifier",
        ],
        "contract_version": CONTRACT_VERSION,
    }
    if v1_proxy_enabled:
        payload["endpoints"].extend(
            [
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
            ]
        )
        payload["capabilities"].append("external_graph_expansion")
    return payload


def create_tapdb_dag_router(
    *,
    config_path: str,
    auth_dependency: DagAuthDependency,
    service_name: str | None = None,
    v1_proxy_policy: V1ProxyPolicy | None = None,
) -> APIRouter:
    """Build the authenticated legacy `/api/dag/*` router."""

    resolved_config_path = str(config_path or "").strip()
    resolved_service_name = _service_name_for(resolved_config_path, service_name)
    if not callable(auth_dependency):
        raise ValueError("legacy DAG v1 requires an explicit auth_dependency")
    router = APIRouter()

    @router.get("/api/dag/object/{euid}")
    async def dag_object_detail(
        euid: str,
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        actor = _actor_from_auth(authenticated)
        with dag_runtime.get_db(resolved_config_path) as conn:
            conn.app_username = actor
            with conn.session_scope() as session:
                obj, record_type = find_object_by_euid(session, euid)
                if obj is None or not record_type:
                    raise HTTPException(
                        status_code=404, detail=f"Object not found: {euid}"
                    )
                return build_object_detail_payload(
                    obj,
                    record_type=record_type,
                    service_name=resolved_service_name,
                )

    @router.get("/api/dag/data")
    async def dag_graph_data(
        start_euid: str,
        depth: int = Query(4, ge=0, le=10),
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        actor = _actor_from_auth(authenticated)
        with dag_runtime.get_db(resolved_config_path) as conn:
            conn.app_username = actor
            with conn.session_scope() as session:
                obj, record_type = find_object_by_euid(session, start_euid)
                if obj is None or not record_type:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Object not found: {start_euid}",
                    )
                payload = build_graph_payload(
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

    @router.get("/api/dag/search")
    async def dag_search(
        q: str = "",
        euid: str = "",
        record_type: str = "all",
        category: str = "",
        type: str = "",
        subtype: str = "",
        tenant_id: str = "",
        relationship_type: str = "",
        limit: int = Query(25, ge=1, le=100),
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        actor = _actor_from_auth(authenticated)
        with dag_runtime.get_db(resolved_config_path) as conn:
            conn.app_username = actor
            with conn.session_scope() as session:
                payload = search_objects(
                    session,
                    service_name=resolved_service_name,
                    q=q,
                    euid=euid,
                    record_type=record_type,
                    category=category,
                    type_name=type,
                    subtype=subtype,
                    tenant_id=tenant_id,
                    relationship_type=relationship_type,
                    limit=limit,
                )
                payload["meta"] = {
                    "owner_service": resolved_service_name,
                    "contract_version": CONTRACT_VERSION,
                }
                return payload

    @router.get("/api/dag/external")
    async def dag_external_graph(
        request: Request,
        source_euid: str,
        ref_index: int = Query(..., ge=0),
        depth: int = Query(4, ge=0, le=10),
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        actor = _actor_from_auth(authenticated)
        if v1_proxy_policy is None:
            raise HTTPException(status_code=404, detail="dag_v1_proxy_disabled")
        with dag_runtime.get_db(resolved_config_path) as conn:
            conn.app_username = actor
            with conn.session_scope() as session:
                obj, _record_type = find_object_by_euid(session, source_euid)
                if obj is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Object not found: {source_euid}",
                    )
                try:
                    ref = get_external_ref_by_index(obj, ref_index)
                except IndexError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                try:
                    payload = fetch_remote_graph(
                        request, ref, depth=depth, policy=v1_proxy_policy
                    )
                    out = namespace_external_graph(
                        payload,
                        ref=ref,
                        ref_index=ref_index,
                        source_euid=source_euid,
                    )
                    meta = out.get("meta")
                    if not isinstance(meta, dict):
                        raise HTTPException(
                            status_code=502,
                            detail="external DAG response missing meta object",
                        )
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
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        actor = _actor_from_auth(authenticated)
        if v1_proxy_policy is None:
            raise HTTPException(status_code=404, detail="dag_v1_proxy_disabled")
        with dag_runtime.get_db(resolved_config_path) as conn:
            conn.app_username = actor
            with conn.session_scope() as session:
                obj, _record_type = find_object_by_euid(session, source_euid)
                if obj is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Object not found: {source_euid}",
                    )
                try:
                    ref = get_external_ref_by_index(obj, ref_index)
                except IndexError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                try:
                    payload = fetch_remote_object_detail(
                        request, ref, euid=euid, policy=v1_proxy_policy
                    )
                    payload["system"] = ref.system
                    payload["contract_version"] = CONTRACT_VERSION
                    return payload
                except Exception as exc:
                    raise HTTPException(status_code=502, detail=str(exc)) from exc

    return router
