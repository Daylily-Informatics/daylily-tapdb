"""Strict, authenticated TapDB DAG v2 mount and manifest contract."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from daylily_tapdb.cli.context import resolve_context
from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.services.graph_payloads import (
    DagV2GraphContractError,
    build_graph_v2_payload,
    build_object_detail_v2_payload,
)
from daylily_tapdb.services.object_lookup import find_object_by_euid
from daylily_tapdb.services.object_search import (
    search_external_reference_sources,
    search_objects,
)

from . import runtime as dag_runtime

DAG_V2_CONTRACT = "dag:v2"
DAG_V2_EXTENSION = "tapdb.dag_v2"
DagAuthDependency = Callable[[Request], Any | Awaitable[Any]]
_SERVICE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class DagV2EligibilityReason(str, Enum):
    """Stable diagnostics for local mounting and fleet admission."""

    MISSING_CONFIG = "missing_config"
    INVALID_CONFIG = "invalid_config"
    SERVICE_IDENTITY_MISMATCH = "service_identity_mismatch"
    AUTH_REQUIRED = "auth_required"
    INVALID_LIMITS = "invalid_limits"
    MOUNT_UNAVAILABLE = "mount_unavailable"
    VERSION_MISMATCH = "version_mismatch"
    MISSING_MANIFEST = "missing_manifest"


@dataclass(frozen=True)
class DagV2Limits:
    """Positive service-owned bounds for every DAG v2 query."""

    max_depth: int
    max_nodes: int
    max_search_page_size: int

    def __post_init__(self) -> None:
        values = {
            "max_depth": self.max_depth,
            "max_nodes": self.max_nodes,
            "max_search_page_size": self.max_search_page_size,
        }
        for name, value in values.items():
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_depth > 32:
            raise ValueError("max_depth must be <= 32")
        if self.max_nodes > 10_000:
            raise ValueError("max_nodes must be <= 10000")
        if self.max_search_page_size > 100:
            raise ValueError("max_search_page_size must be <= 100")


@dataclass(frozen=True)
class DagV2Manifest:
    """Immutable self-advertisement emitted only after a successful mount."""

    extension: str
    contract: str
    service_id: str
    display_name: str
    eligible: bool
    endpoints: tuple[dict[str, str], ...]
    features: dict[str, bool]
    limits: DagV2Limits
    manifest_revision: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["endpoints"] = [dict(item) for item in self.endpoints]
        return payload


@dataclass(frozen=True)
class DagV2MountResult:
    """Atomic mount outcome; failures never carry an advertisement."""

    mounted: bool
    manifest: DagV2Manifest | None
    advertisement: dict[str, Any] | None
    reason: DagV2EligibilityReason | None
    diagnostic: str | None


def _require_exact_service_id(value: str) -> str:
    text = str(value or "")
    if text != text.strip() or not text or len(text) > 128:
        raise ValueError("service_id must be exact, non-empty, and <= 128 characters")
    if _SERVICE_ID_RE.fullmatch(text) is None:
        raise ValueError(
            "service_id must start with an alphanumeric character and contain "
            "only alphanumerics, '.', '_' or '-'"
        )
    return text


def _require_display_name(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("display_name must be a string")
    text = value
    if text != text.strip() or not text or len(text) > 128:
        raise ValueError("display_name must be exact, non-empty, and <= 128 characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ValueError("display_name must not contain control characters")
    return text


def _require_absolute_config(config_path: str) -> str:
    raw = str(config_path or "")
    if not raw:
        raise FileNotFoundError("An explicit absolute TapDB config path is required")
    path = Path(raw)
    if not path.is_absolute():
        raise FileNotFoundError("TapDB config path must be absolute")
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"TapDB config file does not exist: {resolved}")
    resolve_context(require_keys=True, config_path=resolved)
    # Resolve the entire explicit target before advertising eligibility. A file
    # with only namespace metadata is not a runnable TapDB target and must not
    # produce a manifest that fails on its first database-backed request.
    get_db_config(config_path=resolved)
    return str(resolved)


def _manifest_for(
    *, service_id: str, display_name: str, limits: DagV2Limits
) -> DagV2Manifest:
    endpoints = (
        {"kind": "dag_exact_lookup", "path": "/api/dag/v2/object/{euid}"},
        {"kind": "dag_native_graph", "path": "/api/dag/v2/data"},
        {"kind": "dag_object_search", "path": "/api/dag/v2/search"},
    )
    features = {
        "typed_external_references": True,
        "typed_external_identifiers": True,
        "external_reference_search": True,
        "typed_graph_presentation": True,
        "snapshot_metadata": True,
        "outbound_fetch": False,
    }
    revision_input = {
        "extension": DAG_V2_EXTENSION,
        "contract": DAG_V2_CONTRACT,
        "service_id": service_id,
        "display_name": display_name,
        "endpoints": endpoints,
        "features": features,
        "limits": asdict(limits),
    }
    revision = hashlib.sha256(
        json.dumps(revision_input, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return DagV2Manifest(
        extension=DAG_V2_EXTENSION,
        contract=DAG_V2_CONTRACT,
        service_id=service_id,
        display_name=display_name,
        eligible=True,
        endpoints=endpoints,
        features=features,
        limits=limits,
        manifest_revision=revision,
    )


def validate_dag_v2_manifest(
    payload: dict[str, Any] | None,
    *,
    expected_service_id: str,
) -> DagV2EligibilityReason | None:
    """Validate one fetched manifest against an exact fleet registration ID."""

    try:
        exact_expected_service_id = _require_exact_service_id(expected_service_id)
    except ValueError:
        return DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH
    if not isinstance(payload, dict):
        return DagV2EligibilityReason.MISSING_MANIFEST
    if (
        payload.get("extension") != DAG_V2_EXTENSION
        or payload.get("contract") != DAG_V2_CONTRACT
    ):
        return DagV2EligibilityReason.VERSION_MISMATCH
    if payload.get("service_id") != exact_expected_service_id:
        return DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH
    if payload.get("eligible") is not True:
        return DagV2EligibilityReason.MOUNT_UNAVAILABLE
    endpoints = payload.get("endpoints")
    expected_endpoints = {
        "dag_exact_lookup": "/api/dag/v2/object/{euid}",
        "dag_native_graph": "/api/dag/v2/data",
        "dag_object_search": "/api/dag/v2/search",
    }
    if not isinstance(endpoints, list) or len(endpoints) != len(expected_endpoints):
        return DagV2EligibilityReason.VERSION_MISMATCH
    actual_endpoints = {
        str(item.get("kind")): item.get("path")
        for item in endpoints
        if isinstance(item, dict)
    }
    if (
        len(actual_endpoints) != len(endpoints)
        or actual_endpoints != expected_endpoints
    ):
        return DagV2EligibilityReason.VERSION_MISMATCH
    limits = payload.get("limits")
    if not isinstance(limits, dict):
        return DagV2EligibilityReason.INVALID_LIMITS
    try:
        resolved_limits = DagV2Limits(**limits)
    except (TypeError, ValueError):
        return DagV2EligibilityReason.INVALID_LIMITS
    display_name = payload.get("display_name")
    try:
        exact_display_name = _require_display_name(display_name)
    except ValueError:
        return DagV2EligibilityReason.VERSION_MISMATCH
    expected = _manifest_for(
        service_id=exact_expected_service_id,
        display_name=exact_display_name,
        limits=resolved_limits,
    ).to_dict()
    if payload.get("features") != expected["features"]:
        return DagV2EligibilityReason.VERSION_MISMATCH
    if payload.get("manifest_revision") != expected["manifest_revision"]:
        return DagV2EligibilityReason.VERSION_MISMATCH
    return None


def _actor_from_auth(authenticated: Any) -> str:
    if isinstance(authenticated, dict):
        for key in ("username", "email", "sub", "uid"):
            value = str(authenticated.get(key) or "").strip()
            if value:
                return value
    value = str(getattr(authenticated, "username", "") or "").strip()
    if value:
        return value
    raise HTTPException(status_code=401, detail="tapdb_dag_v2_auth_identity_required")


def _canonical_v2_search_payload(
    payload: dict[str, Any], *, service_id: str
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for raw_item in payload.get("items") or []:
        if not isinstance(raw_item, dict):
            raise ValueError("search item must be an object")
        euid = str(raw_item.get("euid") or "")
        record_type = str(raw_item.get("record_type") or "")
        if not euid or not record_type:
            raise ValueError("search item requires euid and record_type")
        item = {
            key: value
            for key, value in raw_item.items()
            if key not in {"system", "service", "kind", "href", "graph_href"}
        }
        item["service_id"] = service_id
        item["record_type"] = record_type
        item["href"] = f"/api/dag/v2/object/{euid}"
        item["graph_href"] = f"/api/dag/v2/data?start_euid={euid}"
        items.append(item)
    return {
        "items": items,
        "page": dict(payload.get("page") or {}),
        "filters": dict(payload.get("filters") or {}),
    }


def _build_router(
    *,
    config_path: str,
    manifest: DagV2Manifest,
    auth_dependency: DagAuthDependency,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/dag/manifest")
    async def dag_v2_manifest(
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        _actor_from_auth(authenticated)
        return manifest.to_dict()

    @router.get("/api/dag/v2/object/{euid}")
    async def dag_v2_object(
        euid: str,
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        actor = _actor_from_auth(authenticated)
        with dag_runtime.get_db(config_path) as conn:
            conn.app_username = actor
            with conn.session_scope() as session:
                obj, record_type = find_object_by_euid(session, euid)
                if obj is None or record_type is None:
                    raise HTTPException(status_code=404, detail="object_not_owned")
                try:
                    return build_object_detail_v2_payload(
                        obj,
                        record_type=record_type,
                        service_id=manifest.service_id,
                    )
                except (ValueError, DagV2GraphContractError) as exc:
                    raise HTTPException(
                        status_code=409, detail=f"invalid_local_graph_contract: {exc}"
                    ) from exc

    @router.get("/api/dag/v2/data")
    async def dag_v2_data(
        start_euid: str,
        depth: int = Query(0, ge=0),
        max_nodes: int | None = Query(None, ge=1),
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        actor = _actor_from_auth(authenticated)
        if depth > manifest.limits.max_depth:
            raise HTTPException(status_code=422, detail="depth_exceeds_service_limit")
        effective_nodes = max_nodes or manifest.limits.max_nodes
        if effective_nodes > manifest.limits.max_nodes:
            raise HTTPException(status_code=422, detail="nodes_exceed_service_limit")
        with dag_runtime.get_db(config_path) as conn:
            conn.app_username = actor
            with conn.session_scope() as session:
                obj, record_type = find_object_by_euid(session, start_euid)
                if obj is None or record_type is None:
                    raise HTTPException(status_code=404, detail="object_not_owned")
                try:
                    return build_graph_v2_payload(
                        obj,
                        record_type=record_type,
                        service_id=manifest.service_id,
                        depth=depth,
                        max_nodes=effective_nodes,
                    )
                except (ValueError, DagV2GraphContractError) as exc:
                    raise HTTPException(
                        status_code=409, detail=f"invalid_local_graph_contract: {exc}"
                    ) from exc

    @router.get("/api/dag/v2/search")
    async def dag_v2_search(
        q: str = "",
        euid: str = "",
        record_type: str = "all",
        category: str = "",
        type: str = "",
        subtype: str = "",
        tenant_id: str = "",
        relationship_type: str = "",
        external_service_id: str = "",
        external_object_euid: str = "",
        external_namespace: str = "",
        external_kind: str = "",
        external_value: str = "",
        external_relationship_type: str = "",
        limit: int | None = Query(None, ge=1),
        cursor: str = "",
        authenticated: Any = Depends(auth_dependency),
    ) -> dict[str, Any]:
        actor = _actor_from_auth(authenticated)
        effective_limit = (
            min(25, manifest.limits.max_search_page_size) if limit is None else limit
        )
        if effective_limit > manifest.limits.max_search_page_size:
            raise HTTPException(
                status_code=422, detail="search_page_exceeds_service_limit"
            )
        with dag_runtime.get_db(config_path) as conn:
            conn.app_username = actor
            with conn.session_scope() as session:
                try:
                    external_values = (
                        external_service_id,
                        external_object_euid,
                        external_namespace,
                        external_kind,
                        external_value,
                        external_relationship_type,
                    )
                    if any(value != "" for value in external_values):
                        if record_type != "instance":
                            raise ValueError(
                                "external-reference filters require record_type=instance"
                            )
                        if any(
                            value != ""
                            for value in (
                                q,
                                euid,
                                category,
                                type,
                                subtype,
                                tenant_id,
                                relationship_type,
                            )
                        ):
                            raise ValueError(
                                "external-reference filters cannot mix with generic filters"
                            )
                        search_payload = search_external_reference_sources(
                            session,
                            service_name=manifest.service_id,
                            external_service_id=external_service_id,
                            external_object_euid=external_object_euid,
                            external_namespace=external_namespace,
                            external_kind=external_kind,
                            external_value=external_value,
                            external_relationship_type=external_relationship_type,
                            limit=effective_limit,
                            cursor=cursor,
                        )
                    else:
                        search_payload = dict(
                            search_objects(
                                session,
                                service_name=manifest.service_id,
                                q=q,
                                euid=euid,
                                record_type=record_type,
                                category=category,
                                type_name=type,
                                subtype=subtype,
                                tenant_id=tenant_id,
                                relationship_type=relationship_type,
                                limit=effective_limit,
                                cursor=cursor,
                            )
                        )
                    payload = _canonical_v2_search_payload(
                        search_payload, service_id=manifest.service_id
                    )
                except ValueError as exc:
                    raise HTTPException(status_code=422, detail=str(exc)) from exc
                payload["meta"] = {
                    "contract": DAG_V2_CONTRACT,
                    "service_id": manifest.service_id,
                    "ownership_proof": False,
                }
                return payload

    return router


def _failed_mount(reason: DagV2EligibilityReason, diagnostic: str) -> DagV2MountResult:
    return DagV2MountResult(
        mounted=False,
        manifest=None,
        advertisement=None,
        reason=reason,
        diagnostic=diagnostic,
    )


def _route_collision(app: Any, router: APIRouter) -> str | None:
    """Return the first method/path already owned by the host application."""
    for candidate in router.routes:
        candidate_path = getattr(candidate, "path", None)
        candidate_methods = set(getattr(candidate, "methods", set()) or set())
        for existing in app.router.routes:
            if getattr(existing, "path", None) != candidate_path:
                continue
            overlap = candidate_methods & set(
                getattr(existing, "methods", set()) or set()
            )
            if overlap:
                return f"{','.join(sorted(overlap))} {candidate_path}"
    return None


def mount_tapdb_dag_surfaces(
    app: Any,
    *,
    config_path: str,
    service_id: str,
    display_name: str,
    auth_dependency: DagAuthDependency | None,
    limits: DagV2Limits,
) -> DagV2MountResult:
    """Atomically mount authenticated v2 routes and publish one advertisement."""

    try:
        resolved_config = _require_absolute_config(config_path)
    except FileNotFoundError as exc:
        return _failed_mount(DagV2EligibilityReason.MISSING_CONFIG, str(exc))
    except Exception as exc:
        return _failed_mount(DagV2EligibilityReason.INVALID_CONFIG, str(exc))
    try:
        exact_service_id = _require_exact_service_id(service_id)
        exact_display_name = _require_display_name(display_name)
    except ValueError as exc:
        return _failed_mount(DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH, str(exc))
    if auth_dependency is None or not callable(auth_dependency):
        return _failed_mount(
            DagV2EligibilityReason.AUTH_REQUIRED,
            "DAG v2 requires an explicit authentication dependency",
        )
    try:
        if not isinstance(limits, DagV2Limits):
            raise ValueError("limits must be a DagV2Limits instance")
        manifest = _manifest_for(
            service_id=exact_service_id,
            display_name=exact_display_name,
            limits=limits,
        )
        fingerprint = (
            resolved_config,
            exact_service_id,
            exact_display_name,
            limits,
            id(auth_dependency),
        )
        existing = getattr(app.state, "tapdb_dag_v2_mount", None)
        if isinstance(existing, DagV2MountResult) and existing.manifest is not None:
            if (
                getattr(app.state, "tapdb_dag_v2_mount_fingerprint", None)
                == fingerprint
            ):
                return existing
            return _failed_mount(
                DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH,
                "DAG v2 is already mounted with different immutable mount inputs",
            )
        router = _build_router(
            config_path=resolved_config,
            manifest=manifest,
            auth_dependency=auth_dependency,
        )
        collision = _route_collision(app, router)
        if collision is not None:
            return _failed_mount(
                DagV2EligibilityReason.MOUNT_UNAVAILABLE,
                f"DAG v2 route collision: {collision}",
            )
    except ValueError as exc:
        return _failed_mount(DagV2EligibilityReason.INVALID_LIMITS, str(exc))
    except Exception as exc:
        return _failed_mount(DagV2EligibilityReason.MOUNT_UNAVAILABLE, str(exc))

    prior_routes = list(app.router.routes)
    missing = object()
    prior_mount = getattr(app.state, "tapdb_dag_v2_mount", missing)
    prior_advertisement = getattr(app.state, "tapdb_dag_v2_advertisement", missing)
    prior_fingerprint = getattr(app.state, "tapdb_dag_v2_mount_fingerprint", missing)
    try:
        app.include_router(router)
        advertisement = {
            "extensions": [DAG_V2_EXTENSION],
            "dag_v2": manifest.to_dict(),
        }
        result = DagV2MountResult(
            mounted=True,
            manifest=manifest,
            advertisement=advertisement,
            reason=None,
            diagnostic=None,
        )
        app.state.tapdb_dag_v2_mount = result
        app.state.tapdb_dag_v2_advertisement = advertisement
        app.state.tapdb_dag_v2_mount_fingerprint = fingerprint
        return result
    except Exception as exc:
        app.router.routes[:] = prior_routes
        for attribute, previous in (
            ("tapdb_dag_v2_mount", prior_mount),
            ("tapdb_dag_v2_advertisement", prior_advertisement),
            ("tapdb_dag_v2_mount_fingerprint", prior_fingerprint),
        ):
            if previous is missing:
                if hasattr(app.state, attribute):
                    delattr(app.state, attribute)
            else:
                setattr(app.state, attribute, previous)
        return _failed_mount(DagV2EligibilityReason.MOUNT_UNAVAILABLE, str(exc))


__all__ = [
    "DAG_V2_CONTRACT",
    "DAG_V2_EXTENSION",
    "DagV2EligibilityReason",
    "DagV2Limits",
    "DagV2Manifest",
    "DagV2MountResult",
    "mount_tapdb_dag_surfaces",
    "validate_dag_v2_manifest",
]
