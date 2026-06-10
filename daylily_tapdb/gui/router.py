"""Embeddable TapDB GUI router.

This module is intentionally separate from ``admin.main``. The legacy admin app
keeps its current routes, while this package exposes mount-friendly pages that
client FastAPI apps can adopt without rewriting their existing TapDB usage.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from admin.db_metrics import build_metrics_page_context
from daylily_tapdb import InstanceFactory, TemplateManager, __version__
from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.euid import validate_euid
from daylily_tapdb.governance import GovernanceContext
from daylily_tapdb.models.audit import audit_log
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.services.external_refs import external_ref_payloads
from daylily_tapdb.services.graph_payloads import build_graph_payload
from daylily_tapdb.services.object_lookup import find_object_by_euid
from daylily_tapdb.services.object_search import search_objects
from daylily_tapdb.templates.loader import (
    ConfigIssue,
    find_tapdb_core_config_dir,
    seed_templates,
)
from daylily_tapdb.validation.instantiation_layouts import (
    format_validation_error,
    validate_instantiation_layouts,
)
from daylily_tapdb.web.bridge import (
    TapdbHostBridge,
    resolve_host_context,
    resolve_host_shell,
)
from daylily_tapdb.web.factory import TapdbHostBridgeMount
from daylily_tapdb.web.runtime import get_db

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
SEARCH_RECORD_TYPES = {"all", "template", "instance", "lineage"}
IMMUTABLE_OBJECT_FIELDS = {
    "kind",
    "record_type",
    "uid",
    "euid",
    "created_dt",
    "modified_dt",
    "category",
    "type",
    "subtype",
    "version",
    "template_uid",
    "template_ref",
    "template_euid",
    "polymorphic_discriminator",
}


def _build_templates(bridge: TapdbHostBridge | None) -> Environment:
    override_dirs: list[str] = []
    if bridge is not None:
        override_dirs = [
            str(Path(item).expanduser().resolve())
            for item in bridge.template_override_dirs
            if str(item).strip() and Path(item).expanduser().exists()
        ]
    loaders = [FileSystemLoader(path) for path in override_dirs]
    loaders.append(FileSystemLoader(str(TEMPLATES_DIR)))
    env = Environment(
        loader=ChoiceLoader(loaders),
        autoescape=select_autoescape(["html", "htm", "xml"]),
    )
    env.globals["tapdb_gui_url"] = gui_url
    env.globals["tapdb_gui_host_shell"] = lambda request: resolve_host_shell(
        bridge, request
    )
    env.globals["tapdb_gui_host_context"] = lambda request: resolve_host_context(
        bridge, request
    )
    env.globals["tapdb_version"] = __version__
    return env


def gui_base_path(request: Request) -> str:
    return str(request.scope.get("root_path") or "").rstrip("/")


def gui_url(request: Request, path: str) -> str:
    suffix = "/" + str(path or "/").lstrip("/")
    return f"{gui_base_path(request)}{suffix}"


def gui_url_with_query(request: Request, path: str, **query: str) -> str:
    base = gui_url(request, path)
    clean = {key: value for key, value in query.items() if str(value or "").strip()}
    if not clean:
        return base
    return f"{base}?{urlencode(clean)}"


async def require_tapdb_gui_user(request: Request) -> dict[str, Any]:
    """Require a host-injected or TapDB-authenticated GUI user."""

    host_user = request.scope.get("tapdb_host_user")
    if isinstance(host_user, dict) and host_user.get("username"):
        request.state.user = host_user
        return host_user

    from admin.auth import get_current_user

    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="tapdb_gui_auth_required")
    request.state.user = user
    return user


async def require_tapdb_gui_admin(
    user: dict[str, Any] = Depends(require_tapdb_gui_user),
) -> dict[str, Any]:
    role = str(user.get("role") or "").strip().lower()
    if role != "admin":
        raise HTTPException(status_code=403, detail="tapdb_gui_admin_required")
    return user


def _render(
    templates: Environment,
    request: Request,
    template_name: str,
    *,
    user: dict[str, Any],
    **context: Any,
) -> HTMLResponse:
    html = templates.get_template(template_name).render(
        request=request,
        user=user,
        **context,
    )
    return HTMLResponse(html)


def _record_to_dict(obj: Any, record_type: str) -> dict[str, Any]:
    return {
        "uid": getattr(obj, "uid", None),
        "euid": getattr(obj, "euid", None),
        "record_type": record_type,
        "name": getattr(obj, "name", None),
        "category": getattr(obj, "category", None),
        "type": getattr(obj, "type", None),
        "subtype": getattr(obj, "subtype", None),
        "version": getattr(obj, "version", None),
        "bstatus": getattr(obj, "bstatus", None),
        "json_addl": getattr(obj, "json_addl", None),
        "created_dt": getattr(obj, "created_dt", None),
        "modified_dt": getattr(obj, "modified_dt", None),
    }


def _new_lineage(
    *,
    parent: generic_instance,
    child: generic_instance,
    relationship_type: str,
) -> generic_instance_lineage:
    rel = (relationship_type or "").strip() or "generic"
    return generic_instance_lineage(
        name=f"{parent.euid}->{child.euid}:{rel}",
        polymorphic_discriminator="generic_instance_lineage",
        category="lineage",
        type="lineage",
        subtype="generic",
        version="1.0",
        bstatus="active",
        parent_instance_uid=parent.uid,
        child_instance_uid=child.uid,
        relationship_type=rel,
        parent_type=parent.polymorphic_discriminator,
        child_type=child.polymorphic_discriminator,
        json_addl={},
    )


def _resolve_instance(session: Any, euid: str, *, label: str) -> generic_instance:
    obj = (
        session.query(generic_instance)
        .filter_by(euid=str(euid or "").strip(), is_deleted=False)
        .first()
    )
    if obj is None:
        raise HTTPException(status_code=404, detail=f"{label} not found: {euid}")
    return obj


def _object_relationships(
    obj: Any, record_type: str
) -> dict[str, list[dict[str, Any]]]:
    parent_of: list[dict[str, Any]] = []
    child_of: list[dict[str, Any]] = []
    if record_type != "instance":
        return {"parent_of": parent_of, "child_of": child_of}
    for lineage in obj.parent_of_lineages.filter_by(is_deleted=False).all():
        child = getattr(lineage, "child_instance", None)
        parent_of.append(
            {
                "lineage_euid": lineage.euid,
                "related_euid": getattr(child, "euid", None),
                "related_name": getattr(child, "name", None),
                "relationship_type": lineage.relationship_type,
            }
        )
    for lineage in obj.child_of_lineages.filter_by(is_deleted=False).all():
        parent = getattr(lineage, "parent_instance", None)
        child_of.append(
            {
                "lineage_euid": lineage.euid,
                "related_euid": getattr(parent, "euid", None),
                "related_name": getattr(parent, "name", None),
                "relationship_type": lineage.relationship_type,
            }
        )
    return {"parent_of": parent_of, "child_of": child_of}


def _lineage_external_refs(obj: Any, record_type: str) -> list[dict[str, Any]]:
    if record_type != "instance":
        return []
    refs: list[dict[str, Any]] = []
    for lineage in obj.parent_of_lineages.filter_by(is_deleted=False).all():
        child = getattr(lineage, "child_instance", None)
        if child is None:
            continue
        markers = {
            str(getattr(child, "category", "") or "").strip().lower(),
            str(getattr(child, "type", "") or "").strip().lower(),
            str(getattr(child, "subtype", "") or "").strip().lower(),
        }
        if not (
            markers
            & {
                "external_identifier",
                "external_id",
                "external_reference",
                "tapdb_external_identifier",
            }
        ):
            continue
        for ref in external_ref_payloads(child):
            item = dict(ref)
            item["link_euid"] = getattr(child, "euid", None)
            item["lineage_euid"] = getattr(lineage, "euid", None)
            item["relationship_type"] = getattr(lineage, "relationship_type", None)
            refs.append(item)
    return refs


def _audit_rows(session: Any, euid: str, limit: int = 100) -> list[dict[str, Any]]:
    rows = (
        session.query(audit_log)
        .filter_by(rel_table_euid_fk=euid, is_deleted=False)
        .order_by(audit_log.changed_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "euid": row.euid,
            "table": row.rel_table_name,
            "column": row.column_name,
            "operation": row.operation_type,
            "old_value": row.old_value,
            "new_value": row.new_value,
            "changed_by": row.changed_by,
            "changed_at": row.changed_at,
        }
        for row in rows
    ]


def _object_detail_context(
    session: Any,
    euid: str,
) -> dict[str, Any]:
    obj, record_type = find_object_by_euid(session, euid)
    if obj is None or record_type is None:
        raise HTTPException(status_code=404, detail=f"Object not found: {euid}")
    payload = _record_to_dict(obj, record_type)
    relationships = _object_relationships(obj, record_type)
    refs = external_ref_payloads(obj)
    refs.extend(_lineage_external_refs(obj, record_type))
    return {
        "obj": payload,
        "record_type": record_type,
        "relationships": relationships,
        "audit_rows": _audit_rows(session, euid),
        "external_refs": refs,
    }


def _parse_json_object(raw: str, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400, detail=f"{label} invalid JSON: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail=f"{label} must be a JSON object")
    return payload


def _reject_immutable_object_fields(payload: dict[str, Any]) -> None:
    immutable = sorted(IMMUTABLE_OBJECT_FIELDS.intersection(payload))
    if immutable:
        raise HTTPException(
            status_code=400,
            detail=(
                "Immutable object field(s) cannot be edited through TapDB GUI/API: "
                + ", ".join(immutable)
            ),
        )


async def _read_urlencoded_form(request: Request) -> dict[str, str]:
    content_type = str(request.headers.get("content-type") or "").split(";", 1)[0]
    if content_type and content_type != "application/x-www-form-urlencoded":
        raise HTTPException(
            status_code=415,
            detail="TapDB GUI form posts require application/x-www-form-urlencoded",
        )
    body = (await request.body()).decode("utf-8")
    parsed = parse_qs(body, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def _template_code(template: Any) -> str:
    return f"{template.category}/{template.type}/{template.subtype}/{template.version}/"


def _template_row(template: generic_template) -> dict[str, Any]:
    return {
        "uid": template.uid,
        "euid": template.euid,
        "name": template.name,
        "category": template.category,
        "type": template.type,
        "subtype": template.subtype,
        "version": template.version,
        "instance_prefix": template.instance_prefix,
        "bstatus": template.bstatus,
        "code": _template_code(template),
    }


def _validate_template_payload(payload: dict[str, Any]) -> list[ConfigIssue]:
    issues: list[ConfigIssue] = []
    templates = payload.get("templates")
    if not isinstance(templates, list) or not templates:
        return [
            ConfigIssue(level="error", message="templates must be a non-empty array")
        ]

    required = (
        "name",
        "polymorphic_discriminator",
        "category",
        "type",
        "subtype",
        "version",
        "instance_prefix",
    )
    seen: set[tuple[str, str, str, str]] = set()
    for index, template in enumerate(templates):
        if not isinstance(template, dict):
            issues.append(
                ConfigIssue(
                    level="error",
                    message=f"templates[{index}] must be a JSON object",
                )
            )
            continue
        for key in required:
            if not str(template.get(key) or "").strip():
                issues.append(
                    ConfigIssue(
                        level="error",
                        message=f"templates[{index}] missing required field {key!r}",
                    )
                )
        key = (
            str(template.get("category") or ""),
            str(template.get("type") or ""),
            str(template.get("subtype") or ""),
            str(template.get("version") or ""),
        )
        if key in seen:
            issues.append(
                ConfigIssue(
                    level="error",
                    template_code="/".join(key),
                    message=f"duplicate template key: {key!r}",
                )
            )
        seen.add(key)
        json_addl = template.get("json_addl")
        if json_addl is not None and not isinstance(json_addl, dict):
            issues.append(
                ConfigIssue(
                    level="error",
                    template_code="/".join(key),
                    message="json_addl must be a JSON object",
                )
            )
        if isinstance(json_addl, dict) and json_addl.get("instantiation_layouts"):
            try:
                validate_instantiation_layouts(json_addl.get("instantiation_layouts"))
            except ValidationError as exc:
                issues.append(
                    ConfigIssue(
                        level="error",
                        template_code="/".join(key),
                        message=(
                            "Invalid instantiation_layouts: "
                            f"{format_validation_error(exc)}"
                        ),
                    )
                )
    return issues


def _external_link_template(session: Any) -> generic_template | None:
    return (
        session.query(generic_template)
        .filter_by(
            category="XRF",
            type="external_identifier",
            subtype="tapdb_object",
            is_deleted=False,
        )
        .order_by(generic_template.version.desc())
        .first()
    )


def _external_link_properties(
    *,
    system: str,
    foreign_uid: str,
    display_url: str = "",
    graph_base_url: str = "",
    graph_data_path: str = "",
    object_detail_path_template: str = "",
    auth_mode: str = "none",
) -> dict[str, Any]:
    return {
        "system": system.strip(),
        "foreign_uid": foreign_uid.strip(),
        "root_euid": foreign_uid.strip(),
        "href": display_url.strip(),
        "external_identifier": {
            "system": system.strip(),
            "target_euid": foreign_uid.strip(),
            "href": display_url.strip() or None,
            "base_url": graph_base_url.strip() or None,
            "graph_data_path": graph_data_path.strip() or None,
            "object_detail_path_template": object_detail_path_template.strip() or None,
            "auth_mode": auth_mode.strip() or "none",
        },
    }


def _create_external_link(
    session: Any,
    *,
    cfg: dict[str, Any],
    source_euid: str,
    system: str,
    foreign_uid: str,
    relationship_type: str,
    display_url: str = "",
    graph_base_url: str = "",
    graph_data_path: str = "",
    object_detail_path_template: str = "",
    auth_mode: str = "none",
) -> dict[str, Any]:
    missing = [
        label
        for label, value in (
            ("system", system),
            ("foreign_uid", foreign_uid),
            ("relationship_type", relationship_type),
        )
        if not str(value or "").strip()
    ]
    if missing:
        raise HTTPException(
            status_code=400,
            detail="Missing required external link field(s): " + ", ".join(missing),
        )
    source = _resolve_instance(session, source_euid, label="Source object")
    template = _external_link_template(session)
    if template is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "No XRF/external_identifier/tapdb_object external link "
                "template is seeded."
            ),
        )
    factory = InstanceFactory(TemplateManager(), domain_code=str(cfg["domain_code"]))
    link = factory.create_instance(
        session,
        _template_code(template),
        name=f"{system.strip()}:{foreign_uid.strip()}",
        properties=_external_link_properties(
            system=system,
            foreign_uid=foreign_uid,
            display_url=display_url,
            graph_base_url=graph_base_url,
            graph_data_path=graph_data_path,
            object_detail_path_template=object_detail_path_template,
            auth_mode=auth_mode,
        ),
        create_children=False,
    )
    lineage = _new_lineage(
        parent=source,
        child=link,
        relationship_type=relationship_type,
    )
    session.add(lineage)
    session.flush()
    return {
        "source_euid": source.euid,
        "link_euid": link.euid,
        "lineage_euid": getattr(lineage, "euid", None),
        "relationship_type": relationship_type.strip(),
    }


def _create_instance_from_template(
    session: Any,
    *,
    cfg: dict[str, Any],
    template_euid: str,
    name: str,
    properties: dict[str, Any],
    create_children: bool,
) -> dict[str, Any]:
    template = (
        session.query(generic_template)
        .filter_by(euid=template_euid, is_deleted=False)
        .first()
    )
    if template is None:
        raise HTTPException(
            status_code=404, detail=f"Template not found: {template_euid}"
        )
    factory = InstanceFactory(TemplateManager(), domain_code=str(cfg["domain_code"]))
    instance = factory.create_instance(
        session,
        _template_code(template),
        name=name.strip(),
        properties=properties,
        create_children=create_children,
    )
    return {
        "template_euid": template.euid,
        "template_code": _template_code(template),
        "instance_euid": instance.euid,
        "create_children": create_children,
    }


def _update_object_json(
    session: Any, *, euid: str, json_addl: dict[str, Any]
) -> dict[str, Any]:
    obj, record_type = find_object_by_euid(session, euid)
    if obj is None or record_type is None:
        raise HTTPException(status_code=404, detail=f"Object not found: {euid}")
    if record_type == "template":
        raise HTTPException(status_code=403, detail="Templates are read-only")
    obj.json_addl = json_addl
    return {"euid": euid, "json_addl": json_addl}


def _update_object_name(session: Any, *, euid: str, name: str) -> dict[str, Any]:
    value = str(name or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="name is required")
    obj, record_type = find_object_by_euid(session, euid)
    if obj is None or record_type is None:
        raise HTTPException(status_code=404, detail=f"Object not found: {euid}")
    if record_type == "template":
        raise HTTPException(status_code=403, detail="Templates are read-only")
    obj.name = value
    return {"euid": euid, "name": value}


def _update_object_status(session: Any, *, euid: str, bstatus: str) -> dict[str, Any]:
    status = str(bstatus or "").strip()
    if not status:
        raise HTTPException(status_code=400, detail="bstatus is required")
    obj, record_type = find_object_by_euid(session, euid)
    if obj is None or record_type is None:
        raise HTTPException(status_code=404, detail=f"Object not found: {euid}")
    if record_type == "template":
        raise HTTPException(status_code=403, detail="Templates are read-only")
    obj.bstatus = status
    return {"euid": euid, "bstatus": status}


def _add_object_lineage(
    session: Any,
    *,
    euid: str,
    related_euid: str,
    direction: str,
    relationship_type: str,
) -> dict[str, Any]:
    current = _resolve_instance(session, euid, label="Object")
    related = _resolve_instance(session, related_euid, label="Related object")
    if direction == "child":
        parent, child = current, related
    else:
        parent, child = related, current
    lineage = _new_lineage(
        parent=parent,
        child=child,
        relationship_type=relationship_type,
    )
    session.add(lineage)
    try:
        session.flush()
    except IntegrityError as exc:
        raise HTTPException(
            status_code=409,
            detail="Lineage already exists or violates a DB constraint",
        ) from exc
    return {
        "lineage_euid": getattr(lineage, "euid", None),
        "parent_euid": parent.euid,
        "child_euid": child.euid,
        "relationship_type": (relationship_type or "").strip() or "generic",
    }


def _meridian_validation_payload(
    *,
    config_path: str,
    euid: str,
    prefix: str,
) -> dict[str, Any]:
    cfg = get_db_config(config_path=config_path)
    governance = GovernanceContext.load(
        domain_code=str(cfg["domain_code"]),
        owner_repo_name=str(cfg["owner_repo_name"]),
        domain_registry_path=str(cfg["domain_registry_path"]),
        prefix_ownership_registry_path=str(cfg["prefix_ownership_registry_path"]),
    )
    euid_valid = None
    if euid:
        euid_valid = validate_euid(euid, allowed_domain_codes=[governance.domain_code])
    prefix_owner = None
    prefix_error = None
    if prefix:
        try:
            prefix_owner = governance.require_prefix(prefix)
        except ValueError as exc:
            prefix_error = str(exc)
    return {
        "config": cfg,
        "governance": governance,
        "domain_code": governance.domain_code,
        "euid": euid,
        "euid_valid": euid_valid,
        "prefix": prefix,
        "prefix_owner": prefix_owner,
        "prefix_error": prefix_error,
        "public_domain_registry": {
            "repository": governance.public_domain_registry_repository,
            "version": governance.public_domain_registry_version,
            "index_url": governance.public_domain_registry_index_url,
        },
    }


def _readiness_payload(*, config_path: str) -> dict[str, Any]:
    cfg = get_db_config(config_path=config_path)
    checks: list[dict[str, Any]] = [
        {
            "name": "config",
            "ok": True,
            "detail": f"Loaded explicit config path: {config_path}",
        }
    ]
    governance = GovernanceContext.load(
        domain_code=str(cfg["domain_code"]),
        owner_repo_name=str(cfg["owner_repo_name"]),
        domain_registry_path=str(cfg["domain_registry_path"]),
        prefix_ownership_registry_path=str(cfg["prefix_ownership_registry_path"]),
    )
    checks.append(
        {
            "name": "governance",
            "ok": True,
            "detail": (
                f"Domain {governance.domain_code}; owner "
                f"{governance.owner_repo_name}; public registry "
                f"{governance.public_domain_registry_version}"
            ),
        }
    )
    with get_db(config_path) as conn:
        with conn.session_scope() as session:
            external_template = _external_link_template(session)
            external_template_detail = (
                _template_code(external_template)
                if external_template is not None
                else "No XRF external link template found"
            )
            template_count = len(
                session.query(generic_template)
                .filter_by(is_deleted=False)
                .limit(500)
                .all()
            )
    checks.append(
        {
            "name": "external_link_template",
            "ok": external_template is not None,
            "detail": external_template_detail,
        }
    )
    checks.append(
        {
            "name": "template_inventory",
            "ok": bool(template_count),
            "detail": f"{template_count} active template(s) visible",
        }
    )
    return {
        "ready": all(check["ok"] for check in checks),
        "config_path": config_path,
        "client_id": cfg.get("client_id"),
        "domain_code": cfg.get("domain_code"),
        "owner_repo_name": cfg.get("owner_repo_name"),
        "public_domain_registry": {
            "repository": governance.public_domain_registry_repository,
            "version": governance.public_domain_registry_version,
            "index_url": governance.public_domain_registry_index_url,
        },
        "checks": checks,
    }


def create_tapdb_gui_router(
    *,
    config_path: str,
    host_bridge: TapdbHostBridge | None = None,
) -> APIRouter:
    """Build the embeddable TapDB GUI router."""

    resolved_config_path = str(config_path or "").strip()
    if not resolved_config_path:
        raise ValueError("config_path is required for TapDB GUI")
    templates = _build_templates(host_bridge)
    router = APIRouter()

    @router.get("/static/tapdb-gui.css")
    async def gui_css():
        css_path = BASE_DIR / "static" / "css" / "tapdb-gui.css"
        return HTMLResponse(css_path.read_text(encoding="utf-8"), media_type="text/css")

    @router.get("/static/lsmc-ui.js")
    async def gui_lsmc_ui_js():
        js_path = BASE_DIR / "static" / "js" / "lsmc-ui.js"
        return HTMLResponse(
            js_path.read_text(encoding="utf-8"), media_type="application/javascript"
        )

    @router.get("/static/tapdb-json-editor.js")
    async def gui_json_editor_js():
        js_path = BASE_DIR / "static" / "js" / "tapdb-json-editor.js"
        return HTMLResponse(
            js_path.read_text(encoding="utf-8"), media_type="application/javascript"
        )

    @router.get("/", response_class=HTMLResponse)
    async def home(
        request: Request,
        q: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        return await search_page(
            request,
            q=q,
            record_type="all",
            category="",
            type="",
            subtype="",
            limit=25,
            user=user,
        )

    @router.get("/search", response_class=HTMLResponse)
    async def search_page(
        request: Request,
        q: str = "",
        record_type: str = "all",
        category: str = "",
        type: str = "",
        subtype: str = "",
        limit: int = Query(25, ge=1, le=100),
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        if record_type not in SEARCH_RECORD_TYPES:
            raise HTTPException(
                status_code=400, detail=f"Invalid record_type: {record_type}"
            )
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                results = search_objects(
                    session,
                    service_name=str(cfg.get("client_id") or "tapdb"),
                    q=q,
                    record_type=record_type,
                    category=category,
                    type_name=type,
                    subtype=subtype,
                    limit=limit,
                )
        return _render(
            templates,
            request,
            "search.html",
            user=user,
            results=results,
            query={
                "q": q,
                "record_type": record_type,
                "category": category,
                "type": type,
                "subtype": subtype,
                "limit": limit,
            },
        )

    @router.get("/api/search")
    async def search_api(
        q: str = "",
        record_type: str = "all",
        category: str = "",
        type: str = "",
        subtype: str = "",
        limit: int = Query(25, ge=1, le=100),
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        if record_type not in SEARCH_RECORD_TYPES:
            raise HTTPException(
                status_code=400, detail=f"Invalid record_type: {record_type}"
            )
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                return search_objects(
                    session,
                    service_name=str(cfg.get("client_id") or "tapdb"),
                    q=q,
                    record_type=record_type,
                    category=category,
                    type_name=type,
                    subtype=subtype,
                    limit=limit,
                )

    @router.get("/templates", response_class=HTMLResponse)
    async def templates_page(
        request: Request,
        category: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                query = session.query(generic_template).filter_by(is_deleted=False)
                if category:
                    query = query.filter_by(category=category)
                items = (
                    query.order_by(
                        generic_template.category,
                        generic_template.type,
                        generic_template.subtype,
                        generic_template.version,
                    )
                    .limit(500)
                    .all()
                )
                rows = [_template_row(item) for item in items]
        return _render(
            templates,
            request,
            "templates.html",
            user=user,
            items=rows,
            category=category,
        )

    @router.get("/templates/new", response_class=HTMLResponse)
    async def template_new_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        return _render(
            templates,
            request,
            "template_editor.html",
            user=user,
            raw_json=json.dumps(_example_template_pack(), indent=2),
            issues=[],
            saved=None,
        )

    @router.get("/templates/validate", response_class=HTMLResponse)
    async def template_validate_get_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        return _render(
            templates,
            request,
            "template_editor.html",
            user=user,
            raw_json=json.dumps(_example_template_pack(), indent=2),
            issues=[
                ConfigIssue(
                    level="info",
                    message="Use Validate after editing the template pack JSON.",
                )
            ],
            saved=None,
        )

    @router.post("/templates/validate", response_class=HTMLResponse)
    async def template_validate_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        template_json = str(form.get("template_json") or "")
        payload = _parse_json_object(template_json, label="template_json")
        issues = _validate_template_payload(payload)
        return _render(
            templates,
            request,
            "template_editor.html",
            user=user,
            raw_json=json.dumps(payload, indent=2, sort_keys=True),
            issues=issues,
            saved=None,
        )

    @router.post("/api/templates/validate")
    async def template_validate_api(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        del user
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"template payload invalid JSON: {exc}",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="template payload must be a JSON object",
            )
        issues = _validate_template_payload(payload)
        return {
            "valid": not any(issue.level == "error" for issue in issues),
            "issues": [jsonable_encoder(issue) for issue in issues],
        }

    @router.post("/templates/save", response_class=HTMLResponse)
    async def template_save_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        template_json = str(form.get("template_json") or "")
        payload = _parse_json_object(template_json, label="template_json")
        issues = _validate_template_payload(payload)
        if issues:
            return _render(
                templates,
                request,
                "template_editor.html",
                user=user,
                raw_json=json.dumps(payload, indent=2, sort_keys=True),
                issues=issues,
                saved=None,
            )
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                for template in payload["templates"]:
                    existing = (
                        session.query(generic_template)
                        .filter_by(
                            domain_code=cfg["domain_code"],
                            category=str(template["category"]),
                            type=str(template["type"]),
                            subtype=str(template["subtype"]),
                            version=str(template["version"]),
                            is_deleted=False,
                        )
                        .first()
                    )
                    if existing is not None:
                        raise HTTPException(
                            status_code=409,
                            detail=(
                                "Template already exists and is read-only: "
                                f"{_template_code(existing)}"
                            ),
                        )
                try:
                    summary = seed_templates(
                        session,
                        [dict(item) for item in payload["templates"]],
                        overwrite=False,
                        core_config_dir=find_tapdb_core_config_dir(),
                        domain_code=str(cfg["domain_code"]),
                        owner_repo_name=str(cfg["owner_repo_name"]),
                        domain_registry_path=Path(str(cfg["domain_registry_path"])),
                        prefix_registry_path=Path(
                            str(cfg["prefix_ownership_registry_path"])
                        ),
                    )
                except ValueError as exc:
                    issues = [ConfigIssue(level="error", message=str(exc))]
                    return _render(
                        templates,
                        request,
                        "template_editor.html",
                        user=user,
                        raw_json=json.dumps(payload, indent=2, sort_keys=True),
                        issues=issues,
                        saved=None,
                    )
        return _render(
            templates,
            request,
            "template_editor.html",
            user=user,
            raw_json=json.dumps(payload, indent=2, sort_keys=True),
            issues=[],
            saved=summary,
        )

    @router.get("/create/{template_euid}", response_class=HTMLResponse)
    async def create_page(
        request: Request,
        template_euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                template = (
                    session.query(generic_template)
                    .filter_by(euid=template_euid, is_deleted=False)
                    .first()
                )
                if template is None:
                    raise HTTPException(
                        status_code=404, detail=f"Template not found: {template_euid}"
                    )
                template_payload = _record_to_dict(template, "template")
        return _render(
            templates,
            request,
            "create.html",
            user=user,
            template=template_payload,
            template_code=(
                f"{template_payload['category']}/{template_payload['type']}/"
                f"{template_payload['subtype']}/{template_payload['version']}/"
            ),
            error=None,
            form={},
        )

    @router.post("/create/{template_euid}")
    async def create_submit(
        request: Request,
        template_euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        name = str(form.get("name") or "")
        properties_json = str(form.get("properties_json") or "{}")
        create_children = str(form.get("create_children") or "")
        properties = _parse_json_object(
            properties_json or "{}", label="properties_json"
        )
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                created = _create_instance_from_template(
                    session,
                    cfg=cfg,
                    template_euid=template_euid,
                    name=name.strip(),
                    properties=properties,
                    create_children=str(create_children).lower() in {"true", "1", "on"},
                )
                instance_euid = created["instance_euid"]
        return RedirectResponse(
            gui_url_with_query(
                request, f"/object/{instance_euid}", notice="instance_created"
            ),
            status_code=303,
        )

    @router.post("/api/create/{template_euid}")
    async def create_api(
        request: Request,
        template_euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="create payload must be a JSON object",
            )
        properties = payload.get("properties") or {}
        if not isinstance(properties, dict):
            raise HTTPException(
                status_code=400, detail="properties must be a JSON object"
            )
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                return jsonable_encoder(
                    _create_instance_from_template(
                        session,
                        cfg=cfg,
                        template_euid=template_euid,
                        name=str(payload.get("name") or ""),
                        properties=properties,
                        create_children=bool(payload.get("create_children")),
                    )
                )

    @router.get("/object/{euid}/graph", response_class=HTMLResponse)
    async def object_graph_page(
        request: Request,
        euid: str,
        depth: int = Query(4, ge=0, le=10),
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                obj, record_type = find_object_by_euid(session, euid)
                if obj is None or record_type is None:
                    raise HTTPException(
                        status_code=404, detail=f"Object not found: {euid}"
                    )
                graph = build_graph_payload(
                    obj,
                    record_type=record_type,
                    service_name=str(cfg.get("client_id") or "tapdb"),
                    depth=depth,
                )
        return _render(
            templates,
            request,
            "graph.html",
            user=user,
            euid=euid,
            depth=depth,
            graph=graph,
        )

    @router.get("/api/object/{euid}/graph")
    async def object_graph_api(
        euid: str,
        depth: int = Query(4, ge=0, le=10),
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                obj, record_type = find_object_by_euid(session, euid)
                if obj is None or record_type is None:
                    raise HTTPException(
                        status_code=404, detail=f"Object not found: {euid}"
                    )
                return build_graph_payload(
                    obj,
                    record_type=record_type,
                    service_name=str(cfg.get("client_id") or "tapdb"),
                    depth=depth,
                )

    @router.get("/object/{euid}", response_class=HTMLResponse)
    async def object_page(
        request: Request,
        euid: str,
        notice: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                context = _object_detail_context(session, euid)
        return _render(
            templates,
            request,
            "object.html",
            user=user,
            obj=context["obj"],
            relationships=context["relationships"],
            audit_rows=context["audit_rows"],
            external_refs=context["external_refs"],
            notice=notice,
            json_text=json.dumps(
                context["obj"]["json_addl"] or {}, indent=2, sort_keys=True
            ),
        )

    @router.get("/api/object/{euid}")
    async def object_api(
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                return jsonable_encoder(_object_detail_context(session, euid))

    @router.post("/object/{euid}/edit-json")
    async def edit_json(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        json_addl = str(form.get("json_addl") or "")
        payload = _parse_json_object(json_addl, label="json_addl")
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _update_object_json(session, euid=euid, json_addl=payload)
        return RedirectResponse(
            gui_url_with_query(request, f"/object/{euid}", notice="json_saved"),
            status_code=303,
        )

    @router.post("/object/{euid}/name")
    async def edit_name(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        name = str(form.get("name") or "")
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _update_object_name(session, euid=euid, name=name)
        return RedirectResponse(
            gui_url_with_query(request, f"/object/{euid}", notice="name_updated"),
            status_code=303,
        )

    @router.post("/api/object/{euid}/name")
    async def edit_name_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400, detail="name payload must be a JSON object"
            )
        _reject_immutable_object_fields(payload)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                return jsonable_encoder(
                    _update_object_name(
                        session,
                        euid=euid,
                        name=str(payload.get("name") or ""),
                    )
                )

    @router.post("/api/object/{euid}/edit-json")
    async def edit_json_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400, detail="json_addl must be a JSON object"
            )
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                return jsonable_encoder(
                    _update_object_json(session, euid=euid, json_addl=payload)
                )

    @router.post("/object/{euid}/status")
    async def edit_status(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        bstatus = str(form.get("bstatus") or "")
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _update_object_status(session, euid=euid, bstatus=bstatus)
        return RedirectResponse(
            gui_url_with_query(request, f"/object/{euid}", notice="status_updated"),
            status_code=303,
        )

    @router.post("/api/object/{euid}/status")
    async def edit_status_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400, detail="status payload must be a JSON object"
            )
        _reject_immutable_object_fields(payload)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                return jsonable_encoder(
                    _update_object_status(
                        session,
                        euid=euid,
                        bstatus=str(payload.get("bstatus") or ""),
                    )
                )

    @router.post("/object/{euid}/lineage")
    async def add_lineage(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        related_euid = str(form.get("related_euid") or "")
        direction = str(form.get("direction") or "parent")
        relationship_type = str(form.get("relationship_type") or "generic")
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _add_object_lineage(
                    session,
                    euid=euid,
                    related_euid=related_euid,
                    direction=direction,
                    relationship_type=relationship_type,
                )
        return RedirectResponse(
            gui_url_with_query(request, f"/object/{euid}", notice="lineage_added"),
            status_code=303,
        )

    @router.post("/api/object/{euid}/lineage")
    async def add_lineage_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400, detail="lineage payload must be a JSON object"
            )
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                return jsonable_encoder(
                    _add_object_lineage(
                        session,
                        euid=euid,
                        related_euid=str(payload.get("related_euid") or ""),
                        direction=str(payload.get("direction") or "parent"),
                        relationship_type=str(
                            payload.get("relationship_type") or "generic"
                        ),
                    )
                )

    @router.get("/object/{euid}/external-links/new", response_class=HTMLResponse)
    async def external_link_page(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        return _render(
            templates,
            request,
            "external_link.html",
            user=user,
            euid=euid,
            error=None,
            form={},
        )

    @router.post("/object/{euid}/external-links/new")
    async def external_link_submit(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        system = str(form.get("system") or "")
        foreign_uid = str(form.get("foreign_uid") or "")
        relationship_type = str(form.get("relationship_type") or "external_ref")
        display_url = str(form.get("display_url") or "")
        graph_base_url = str(form.get("graph_base_url") or "")
        graph_data_path = str(form.get("graph_data_path") or "")
        object_detail_path_template = str(form.get("object_detail_path_template") or "")
        auth_mode = str(form.get("auth_mode") or "none")
        missing = [
            label
            for label, value in (
                ("system", system),
                ("foreign_uid", foreign_uid),
                ("relationship_type", relationship_type),
            )
            if not str(value or "").strip()
        ]
        if missing:
            raise HTTPException(
                status_code=400,
                detail="Missing required external link field(s): " + ", ".join(missing),
            )
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                created = _create_external_link(
                    session,
                    cfg=cfg,
                    source_euid=euid,
                    system=system,
                    foreign_uid=foreign_uid,
                    relationship_type=relationship_type,
                    display_url=display_url,
                    graph_base_url=graph_base_url,
                    graph_data_path=graph_data_path,
                    object_detail_path_template=object_detail_path_template,
                    auth_mode=auth_mode,
                )
                link_euid = created["link_euid"]
        return RedirectResponse(
            gui_url_with_query(
                request, f"/object/{link_euid}", notice="external_link_created"
            ),
            status_code=303,
        )

    @router.post("/api/object/{euid}/external-links")
    async def external_link_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="external link payload must be a JSON object",
            )
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                created = _create_external_link(
                    session,
                    cfg=cfg,
                    source_euid=euid,
                    system=str(payload.get("system") or ""),
                    foreign_uid=str(payload.get("foreign_uid") or ""),
                    relationship_type=str(
                        payload.get("relationship_type") or "external_ref"
                    ),
                    display_url=str(payload.get("display_url") or ""),
                    graph_base_url=str(payload.get("graph_base_url") or ""),
                    graph_data_path=str(payload.get("graph_data_path") or ""),
                    object_detail_path_template=str(
                        payload.get("object_detail_path_template") or ""
                    ),
                    auth_mode=str(payload.get("auth_mode") or "none"),
                )
        return jsonable_encoder(created)

    @router.get("/admin/readiness", response_class=HTMLResponse)
    async def readiness_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        return _render(
            templates,
            request,
            "readiness.html",
            user=user,
            readiness=_readiness_payload(config_path=resolved_config_path),
        )

    @router.get("/api/admin/readiness")
    async def readiness_api(
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        del user
        return jsonable_encoder(_readiness_payload(config_path=resolved_config_path))

    @router.get("/admin/meridian", response_class=HTMLResponse)
    async def meridian_page(
        request: Request,
        euid: str = "",
        prefix: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        validation = _meridian_validation_payload(
            config_path=resolved_config_path,
            euid=euid,
            prefix=prefix,
        )
        return _render(
            templates,
            request,
            "meridian.html",
            user=user,
            cfg=validation["config"],
            governance=validation["governance"],
            euid=validation["euid"],
            euid_valid=validation["euid_valid"],
            prefix=validation["prefix"],
            prefix_owner=validation["prefix_owner"],
            prefix_error=validation["prefix_error"],
        )

    @router.get("/api/admin/meridian/validate")
    async def meridian_validate_api(
        euid: str = "",
        prefix: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        del user
        validation = _meridian_validation_payload(
            config_path=resolved_config_path,
            euid=euid,
            prefix=prefix,
        )
        return jsonable_encoder(
            {
                key: value
                for key, value in validation.items()
                if key not in {"governance"}
            }
        )

    @router.get("/admin/metrics", response_class=HTMLResponse)
    async def metrics_page(
        request: Request,
        limit: int = Query(5000, ge=1, le=50000),
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        metrics = build_metrics_page_context(
            "target", limit=limit, config_path=resolved_config_path
        )
        return _render(
            templates,
            request,
            "metrics.html",
            user=user,
            metrics=metrics,
            limit=limit,
        )

    @router.get("/api/admin/metrics")
    async def metrics_api(
        limit: int = Query(5000, ge=1, le=50000),
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        del user
        return jsonable_encoder(
            build_metrics_page_context(
                "target", limit=limit, config_path=resolved_config_path
            )
        )

    return router


def create_tapdb_gui_app(
    *,
    config_path: str,
    host_bridge: TapdbHostBridge | None = None,
):
    """Build a mountable TapDB GUI ASGI app."""

    app = FastAPI(title="TapDB GUI", version=__version__)
    app.state.tapdb_host_bridge = host_bridge
    app.include_router(
        create_tapdb_gui_router(config_path=config_path, host_bridge=host_bridge)
    )
    if host_bridge is not None and host_bridge.auth_mode == "host_session":
        return TapdbHostBridgeMount(app, host_bridge)
    return app


def _example_template_pack() -> dict[str, Any]:
    return {
        "templates": [
            {
                "name": "Example Actor",
                "polymorphic_discriminator": "generic_template",
                "category": "ACT",
                "type": "actor",
                "subtype": "example_actor",
                "version": "1.0",
                "instance_prefix": "ACT",
                "instance_polymorphic_identity": "generic_instance",
                "json_addl": {
                    "properties": {
                        "display_name": "",
                        "email": "",
                    },
                    "instantiation_layouts": [],
                },
            },
            {
                "name": "Example Plate",
                "polymorphic_discriminator": "generic_template",
                "category": "PLT",
                "type": "container",
                "subtype": "example_plate",
                "version": "1.0",
                "instance_prefix": "PLT",
                "instance_polymorphic_identity": "generic_instance",
                "json_addl": {
                    "properties": {
                        "plate_type": "custom",
                    },
                    "instantiation_layouts": [
                        {
                            "relationship_type": "contains",
                            "name_pattern": "{parent_name}_well_{index}",
                            "child_templates": [
                                {
                                    "template_code": "WEL/container/example_well/1.0",
                                    "count": 2,
                                }
                            ],
                        }
                    ],
                },
            },
        ]
    }
