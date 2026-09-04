"""Canonical standalone and embeddable TapDB GUI."""

from __future__ import annotations

import json
import secrets
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    HTMLResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from admin.auth import SESSION_COOKIE_NAME
from admin.db_metrics import (
    build_metrics_page_context,
    request_method_var,
    request_path_var,
    stop_all_writers,
)
from admin.db_pool import dispose_all_engines
from admin.domain_access import (
    build_allowed_origin_regex,
    build_trusted_hosts,
    is_allowed_origin,
    validate_allowed_origins,
)
from daylily_tapdb import InstanceFactory, TemplateManager, __version__
from daylily_tapdb.cli.context import set_cli_context
from daylily_tapdb.cli.db_config import get_admin_settings, get_db_config
from daylily_tapdb.euid import validate_euid
from daylily_tapdb.external_references import (
    _is_xrf_coordinates,
    _project_outbound_external_references,
)
from daylily_tapdb.governance import GovernanceContext
from daylily_tapdb.graph_contracts import (
    attach_v0_edge_metadata,
    describe_lineage_contract,
    is_strict_canonical_edge_type,
)
from daylily_tapdb.models.audit import audit_log
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.services.graph_payloads import (
    build_graph_v2_payload,
    build_visible_graph_v2_payload,
)
from daylily_tapdb.services.object_lookup import find_object_by_euid
from daylily_tapdb.services.object_operations import (
    ObjectSelector,
    repair_object,
    soft_delete_object,
    update_object,
)
from daylily_tapdb.services.object_search import search_objects
from daylily_tapdb.templates.loader import (
    ConfigIssue,
    find_tapdb_core_config_dir,
    seed_templates,
)
from daylily_tapdb.templates.repository import (
    export_repository_pack,
    import_repository_pack,
    repository_inventory,
    repository_pack_bytes,
)
from daylily_tapdb.validation.governance import (
    assess_evidence,
    assess_object,
    editor_data_for_object,
    normalize_validator_ref,
)
from daylily_tapdb.validation.instantiation_layouts import (
    format_validation_error,
    validate_instantiation_layouts,
)
from daylily_tapdb.web.bridge import (
    TapdbHostBridge,
    TapdbHostBridgeMount,
    resolve_host_context,
    resolve_host_shell,
)
from daylily_tapdb.web.dag_v2 import DagV2Limits, mount_tapdb_dag_surfaces
from daylily_tapdb.web.runtime import dispose_all_runtime_engines, get_db

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
    env.globals["tapdb_gui_nav_links"] = gui_nav_links
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


def gui_nav_links(request: Request, shell: dict[str, Any]) -> list[dict[str, str]]:
    """Merge host shell navigation with TapDB's built-in GUI links."""

    user = getattr(getattr(request, "state", None), "user", {}) or {}
    is_admin = str(user.get("role") or "").strip().lower() == "admin"
    built_in = [
        {"label": "Overview", "href": gui_url(request, "/admin/overview")},
        {"label": "Search", "href": gui_url(request, "/search")},
        {"label": "Graph", "href": gui_url(request, "/graph")},
        {"label": "Templates", "href": gui_url(request, "/templates")},
        {"label": "Audit", "href": gui_url(request, "/audit")},
        {"label": "Help", "href": gui_url(request, "/help")},
    ]
    if is_admin:
        built_in.extend(
            [
                {
                    "label": "Readiness",
                    "href": gui_url(request, "/admin/readiness"),
                },
                {
                    "label": "Inventory",
                    "href": gui_url(request, "/admin/inventory"),
                },
                {
                    "label": "Meridian",
                    "href": gui_url(request, "/admin/meridian"),
                },
                {"label": "Metrics", "href": gui_url(request, "/admin/metrics")},
                {"label": "Runtime", "href": gui_url(request, "/admin/runtime")},
                {"label": "Backups", "href": gui_url(request, "/admin/backups")},
            ]
        )
    account_url = shell.get("change_password_url") or gui_url(
        request, "/change-password"
    )
    logout_url = shell.get("logout_url") or gui_url(request, "/logout")
    built_in.extend(
        [
            {"label": "Password", "href": str(account_url)},
            {"label": "Sign out", "href": str(logout_url)},
        ]
    )
    candidates = list(shell.get("nav_links") or []) + built_in
    seen_labels: set[str] = set()
    seen_hrefs: set[str] = set()
    links: list[dict[str, str]] = []
    for item in candidates:
        label = str(item.get("label") or "").strip()
        href = str(item.get("href") or "").strip()
        if not label or not href:
            continue
        label_key = label.casefold()
        href_key = href.rstrip("/") or href
        if label_key in seen_labels or href_key in seen_hrefs:
            continue
        seen_labels.add(label_key)
        seen_hrefs.add(href_key)
        links.append({"label": label, "href": href})
    return links


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
        path = str(request.scope.get("path") or "")
        if path == "/api" or path.startswith("/api/"):
            raise HTTPException(status_code=401, detail="tapdb_gui_auth_required")
        next_path = str(request.scope.get("root_path") or "") + path
        query_string = request.scope.get("query_string") or b""
        if query_string:
            next_path += f"?{query_string.decode('utf-8')}"
        location = gui_url_with_query(request, "/login", next=next_path or "/")
        raise HTTPException(status_code=302, headers={"Location": location})
    if user.get("require_password_change"):
        path = str(request.scope.get("path") or "")
        if path == "/api" or path.startswith("/api/"):
            raise HTTPException(
                status_code=403, detail="tapdb_gui_password_change_required"
            )
        raise HTTPException(
            status_code=302,
            headers={"Location": gui_url(request, "/change-password")},
        )
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
    v0_edge: dict[str, Any] | None = None,
) -> generic_instance_lineage:
    rel = (relationship_type or "").strip() or "generic"
    lineage = generic_instance_lineage(
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
    if v0_edge is not None:
        attach_v0_edge_metadata(lineage, v0_edge)
    return lineage


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
                "v0_edge": describe_lineage_contract(lineage),
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
                "v0_edge": describe_lineage_contract(lineage),
            }
        )
    return {"parent_of": parent_of, "child_of": child_of}


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
    external = (
        _project_outbound_external_references(obj)
        if record_type == "instance"
        else {"external_refs": [], "external_identifiers": []}
    )
    return {
        "obj": payload,
        "record_type": record_type,
        "relationships": relationships,
        "audit_rows": _audit_rows(session, euid),
        "external_refs": external["external_refs"],
        "external_identifiers": external["external_identifiers"],
        "manual_create_allowed": not _is_xrf_coordinates(obj),
        "editor": editor_data_for_object(session, euid),
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


def _parse_evidence_refs(raw: str) -> list[dict[str, Any]]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = [
            item.strip()
            for line in text.splitlines()
            for item in line.split(",")
            if item.strip()
        ]
    if isinstance(parsed, list):
        refs: list[dict[str, Any]] = []
        for item in parsed:
            if isinstance(item, dict):
                refs.append(item)
            else:
                refs.append({"euid": str(item).strip()})
        return refs
    raise HTTPException(
        status_code=400,
        detail="evidence_refs must be JSON list or comma/newline separated EUIDs",
    )


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


def _reject_unknown_payload_fields(
    payload: dict[str, Any], *, allowed: set[str]
) -> None:
    unknown = sorted(set(payload).difference(allowed))
    if unknown:
        raise HTTPException(
            status_code=400,
            detail="Unexpected payload field(s): " + ", ".join(unknown),
        )


def _require_form_apply(form: dict[str, str]) -> None:
    if form.get("apply") != "true":
        raise HTTPException(
            status_code=400,
            detail="This mutation requires the clicked Apply button (apply=true)",
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


async def _read_optional_json_object(request: Request) -> dict[str, Any]:
    body = await request.body()
    if not body.strip():
        return {}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400, detail=f"JSON body invalid: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")
    return payload


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
        "validator_ref": normalize_validator_ref(
            getattr(template, "validator_ref", None)
        ),
        "bstatus": template.bstatus,
        "code": _template_code(template),
    }


def _template_payload_and_code(
    template: generic_template,
) -> tuple[dict[str, Any], str]:
    payload = _record_to_dict(template, "template")
    code = (
        f"{payload['category']}/{payload['type']}/"
        f"{payload['subtype']}/{payload['version']}/"
    )
    return payload, code


def _template_properties_form_json(template_payload: dict[str, Any]) -> str:
    json_addl = template_payload.get("json_addl")
    if not isinstance(json_addl, dict):
        return "{}"
    properties = json_addl.get("properties")
    if not isinstance(properties, dict):
        return "{}"
    return json.dumps(properties, indent=2, sort_keys=True)


def _template_seed_pack(template: generic_template) -> dict[str, Any]:
    return {
        "templates": [
            {
                "name": template.name,
                "polymorphic_discriminator": getattr(
                    template, "polymorphic_discriminator", "generic_template"
                ),
                "category": template.category,
                "type": template.type,
                "subtype": template.subtype,
                "version": template.version,
                "instance_prefix": template.instance_prefix,
                "instance_polymorphic_identity": getattr(
                    template,
                    "instance_polymorphic_identity",
                    "generic_instance",
                ),
                "validator_ref": normalize_validator_ref(
                    getattr(template, "validator_ref", None)
                ),
                "json_addl": template.json_addl or {},
            }
        ]
    }


def _builder_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


def _default_builder_seed() -> dict[str, Any]:
    return {
        "seed_euid": "",
        "name": "96 Well Generic Plate",
        "category": "container",
        "type": "plate",
        "subtype": "96well-generic",
        "version": "1.0",
        "instance_prefix": "PAT",
        "properties": [{"key": "plate_format", "value": "96"}],
        "children": [
            {
                "template_code": "container/well/generic/1.0",
                "count": 96,
                "relationship_type": "contains",
            }
        ],
    }


def _builder_seed_from_template(
    template: dict[str, Any],
    *,
    seed_euid: str = "",
) -> dict[str, Any]:
    json_addl = template.get("json_addl") if isinstance(template, dict) else {}
    json_addl = json_addl if isinstance(json_addl, dict) else {}
    properties = json_addl.get("properties") if isinstance(json_addl, dict) else {}
    properties = properties if isinstance(properties, dict) else {}
    property_rows = [
        {"key": str(key), "value": _builder_value(value)}
        for key, value in properties.items()
    ] or [{"key": "", "value": ""}]

    child_rows: list[dict[str, Any]] = []
    layouts = json_addl.get("instantiation_layouts")
    if isinstance(layouts, list):
        for layout in layouts:
            if not isinstance(layout, dict):
                continue
            relationship_type = str(layout.get("relationship_type") or "contains")
            child_templates = layout.get("child_templates")
            if not isinstance(child_templates, list):
                continue
            for child in child_templates:
                if not isinstance(child, dict):
                    continue
                template_code = str(child.get("template_code") or "").strip()
                count = child.get("count")
                child_rows.append(
                    {
                        "template_code": template_code,
                        "count": count if isinstance(count, int) and count > 0 else 1,
                        "relationship_type": relationship_type,
                    }
                )
    if not child_rows:
        child_rows = [
            {"template_code": "", "count": 1, "relationship_type": "contains"}
        ]

    return {
        "seed_euid": seed_euid,
        "name": str(template.get("name") or ""),
        "category": str(template.get("category") or ""),
        "type": str(template.get("type") or ""),
        "subtype": str(template.get("subtype") or ""),
        "version": str(template.get("version") or "1.0"),
        "instance_prefix": str(template.get("instance_prefix") or ""),
        "properties": property_rows,
        "children": child_rows,
    }


def _template_editor_context(
    payload: dict[str, Any],
    *,
    seed_template: dict[str, Any] | None = None,
    use_default_builder: bool = False,
) -> dict[str, Any]:
    templates = payload.get("templates") if isinstance(payload, dict) else None
    first_template = templates[0] if isinstance(templates, list) and templates else None

    if use_default_builder and seed_template is None:
        builder_seed = _default_builder_seed()
    elif isinstance(first_template, dict):
        builder_seed = _builder_seed_from_template(
            first_template,
            seed_euid=str(seed_template.get("euid") or "") if seed_template else "",
        )
    else:
        builder_seed = _default_builder_seed()

    return {
        "raw_json": json.dumps(payload, indent=2, sort_keys=True),
        "builder_seed": builder_seed,
        "seed_template": seed_template,
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


def _create_object_repair(
    session: Any,
    *,
    cfg: dict[str, Any],
    euid: str,
    actor: str,
    reason: str,
    repair_payload: dict[str, Any],
    dry_run: bool = False,
) -> dict[str, Any]:
    try:
        return repair_object(
            session,
            ObjectSelector(euid=euid),
            domain_code=str(cfg.get("domain_code") or ""),
            actor=actor,
            reason=reason,
            repair_payload=repair_payload,
            dry_run=dry_run,
        )
    except LookupError as exc:
        message = str(exc)
        status = 404 if message.lower().startswith("object not found") else 422
        raise HTTPException(status_code=status, detail=message) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _update_object_name(
    session: Any,
    *,
    euid: str,
    name: str,
    actor: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    value = str(name or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="name is required")
    obj, record_type = find_object_by_euid(session, euid)
    if obj is None or record_type is None:
        raise HTTPException(status_code=404, detail=f"Object not found: {euid}")
    if record_type == "template":
        raise HTTPException(status_code=403, detail="Templates are read-only")
    try:
        return update_object(
            session,
            ObjectSelector(euid=euid),
            {"name": value},
            actor=actor,
            dry_run=dry_run,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _update_object_status(
    session: Any,
    *,
    euid: str,
    bstatus: str,
    actor: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    status = str(bstatus or "").strip()
    if not status:
        raise HTTPException(status_code=400, detail="bstatus is required")
    obj, record_type = find_object_by_euid(session, euid)
    if obj is None or record_type is None:
        raise HTTPException(status_code=404, detail=f"Object not found: {euid}")
    if record_type == "template":
        raise HTTPException(status_code=403, detail="Templates are read-only")
    try:
        return update_object(
            session,
            ObjectSelector(euid=euid),
            {"bstatus": status},
            actor=actor,
            dry_run=dry_run,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _add_object_lineage(
    session: Any,
    *,
    euid: str,
    related_euid: str,
    direction: str,
    relationship_type: str,
    v0_edge: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = _resolve_instance(session, euid, label="Object")
    related = _resolve_instance(session, related_euid, label="Related object")
    if direction == "child":
        parent, child = current, related
    else:
        parent, child = related, current
    canonical = is_strict_canonical_edge_type(relationship_type)
    metadata = v0_edge
    if metadata is None and canonical:
        raise HTTPException(
            status_code=400,
            detail=(
                "Canonical LSMC v0 edge writes require v0_edge metadata with "
                "evidence_refs, correlation_id, and causation_id"
            ),
        )
    if metadata is not None:
        metadata = {
            **metadata,
            "edge_type": metadata.get("edge_type") or relationship_type,
            "semantic_source": metadata.get("semantic_source")
            or {"euid": parent.euid, "role": "source"},
            "semantic_target": metadata.get("semantic_target")
            or {"euid": child.euid, "role": "target"},
        }
    try:
        lineage = _new_lineage(
            parent=parent,
            child=child,
            relationship_type=relationship_type,
            v0_edge=metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    session.add(lineage)
    try:
        session.flush()
    except IntegrityError as exc:
        raise HTTPException(
            status_code=409,
            detail="Lineage already exists or violates a DB constraint",
        ) from exc
    assessment = assess_evidence(
        subject_ref=f"lineage:{parent.euid}->{child.euid}:{relationship_type}",
        context={
            "operation": "create_relationship",
            "relationship_type": (relationship_type or "").strip() or "generic",
            "parent_euid": parent.euid,
            "child_euid": child.euid,
        },
    )
    return {
        "lineage_euid": getattr(lineage, "euid", None),
        "parent_euid": parent.euid,
        "child_euid": child.euid,
        "relationship_type": (relationship_type or "").strip() or "generic",
        "v0_edge": describe_lineage_contract(lineage),
        "assessment": assessment.to_dict(),
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
            external_templates = (
                session.query(generic_template)
                .filter_by(
                    category="reference",
                    type="external_identifier",
                    version="1.0",
                    is_deleted=False,
                )
                .filter(generic_template.subtype.in_(("tapdb_object", "opaque")))
                .all()
            )
            external_template_codes = sorted(
                _template_code(template) for template in external_templates
            )
            external_templates_ready = external_template_codes == [
                "reference/external_identifier/opaque/1.0/",
                "reference/external_identifier/tapdb_object/1.0/",
            ]
            template_count = len(
                session.query(generic_template)
                .filter_by(is_deleted=False)
                .limit(500)
                .all()
            )
    checks.append(
        {
            "name": "canonical_external_reference_templates",
            "ok": external_templates_ready,
            "detail": ", ".join(external_template_codes)
            or "Canonical external-reference templates are not seeded",
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


def _overview_payload(*, config_path: str, username: str) -> dict[str, Any]:
    with get_db(config_path) as conn:
        conn.app_username = username
        with conn.session_scope() as session:
            counts = {
                "templates": session.query(generic_template)
                .filter_by(is_deleted=False)
                .count(),
                "instances": session.query(generic_instance)
                .filter_by(is_deleted=False)
                .count(),
                "lineages": session.query(generic_instance_lineage)
                .filter_by(is_deleted=False)
                .count(),
            }
    return {"counts": counts, "total": sum(counts.values())}


def _inventory_payload(*, config_path: str, username: str) -> dict[str, Any]:
    """Return a bounded, schema-local PostgreSQL inventory for operators."""

    from daylily_tapdb.schema_inventory import load_live_schema_inventory

    with get_db(config_path) as conn:
        conn.app_username = username
        with conn.session_scope() as session:
            database_name = str(
                session.execute(text("SELECT current_database() ")).scalar() or ""
            )
            active_schema = str(
                session.execute(text("SELECT current_schema() ")).scalar() or ""
            ).strip()
            if not active_schema:
                raise RuntimeError("Active PostgreSQL schema is not configured")
            search_path = [
                str(item)
                for item in (
                    session.execute(
                        text("SELECT current_schemas(false) AS schema_names")
                    ).scalar()
                    or []
                )
            ]
            schemas = [
                str(row["schema_name"])
                for row in session.execute(
                    text(
                        """
                        SELECT nspname AS schema_name
                        FROM pg_namespace
                        WHERE nspname NOT IN ('pg_catalog', 'information_schema')
                          AND nspname NOT LIKE 'pg_toast%'
                          AND nspname NOT LIKE 'pg_temp_%'
                        ORDER BY nspname
                        """
                    )
                ).mappings()
            ]
            live = load_live_schema_inventory(session, schema_name=active_schema)
            views = [
                str(row["view_name"])
                for row in session.execute(
                    text(
                        """
                        SELECT viewname AS view_name
                        FROM pg_views
                        WHERE schemaname = :schema_name
                        ORDER BY viewname
                        """
                    ),
                    {"schema_name": active_schema},
                ).mappings()
            ]
            materialized_views = [
                str(row["view_name"])
                for row in session.execute(
                    text(
                        """
                        SELECT matviewname AS view_name
                        FROM pg_matviews
                        WHERE schemaname = :schema_name
                        ORDER BY matviewname
                        """
                    ),
                    {"schema_name": active_schema},
                ).mappings()
            ]
    trigger_rows = [
        {"table": table_name, "name": trigger_name}
        for table_name, trigger_names in sorted(live.triggers.items())
        for trigger_name in sorted(trigger_names)
    ]
    index_rows = [
        {"table": table_name, "name": index_name}
        for table_name, index_names in sorted(live.indexes.items())
        for index_name in sorted(index_names)
    ]
    counts = live.counts() | {
        "schemas": len(schemas),
        "views": len(views),
        "materialized_views": len(materialized_views),
    }
    return {
        "database_name": database_name,
        "active_schema": active_schema,
        "search_path": search_path,
        "schemas": schemas,
        "counts": counts,
        "tables": sorted(live.tables),
        "columns": {
            table_name: sorted(column_names)
            for table_name, column_names in sorted(live.columns.items())
        },
        "views": views,
        "materialized_views": materialized_views,
        "sequences": sorted(live.sequences),
        "functions": sorted(live.functions),
        "triggers": trigger_rows,
        "indexes": index_rows,
    }


def _audit_payload(
    *,
    config_path: str,
    user: dict[str, Any],
    euid: str,
    changed_by: str,
    operation_type: str,
    limit: int,
) -> dict[str, Any]:
    from daylily_tapdb.audit import query_audit_trail

    is_admin = str(user.get("role") or "").strip().lower() == "admin"
    current_identifier = str(user.get("username") or user.get("email") or "").strip()
    requested_actor = str(changed_by or "").strip()
    effective_actor = requested_actor if is_admin else current_identifier
    warning = None
    if not is_admin and requested_actor and requested_actor != current_identifier:
        warning = "Non-admin users can view only their own audit activity."
    normalized_operation = str(operation_type or "").strip().upper()
    if normalized_operation in {"", "ALL"}:
        normalized_operation = ""
    elif normalized_operation not in {"INSERT", "UPDATE", "DELETE"}:
        raise ValueError("operation_type must be ALL, INSERT, UPDATE, or DELETE")
    with get_db(config_path) as conn:
        conn.app_username = current_identifier
        with conn.session_scope() as session:
            entries = query_audit_trail(
                session,
                changed_by=effective_actor or None,
                euid=str(euid or "").strip() or None,
                operation_type=normalized_operation or None,
                limit=limit,
            )
    return {
        "items": [asdict(entry) for entry in entries],
        "filters": {
            "euid": str(euid or "").strip(),
            "changed_by": effective_actor,
            "operation_type": normalized_operation or "ALL",
            "limit": limit,
        },
        "can_query_any_actor": is_admin,
        "warning": warning,
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

    @router.get("/static/tapdb-graph.js")
    async def gui_graph_js():
        js_path = BASE_DIR / "static" / "js" / "tapdb-graph.js"
        return HTMLResponse(
            js_path.read_text(encoding="utf-8"), media_type="application/javascript"
        )

    @router.get("/", response_class=HTMLResponse)
    async def home(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        overview = _overview_payload(
            config_path=resolved_config_path,
            username=str(user.get("username") or ""),
        )
        recent = _audit_payload(
            config_path=resolved_config_path,
            user=user,
            euid="",
            changed_by="",
            operation_type="ALL",
            limit=10,
        )
        return _render(
            templates,
            request,
            "overview.html",
            user=user,
            overview=overview,
            recent_audit=recent,
        )

    @router.get("/admin/overview", response_class=HTMLResponse)
    async def overview_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        return await home(request=request, user=user)

    @router.get("/api/admin/overview")
    async def overview_api(
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        return _overview_payload(
            config_path=resolved_config_path,
            username=str(user.get("username") or ""),
        )

    @router.get("/audit", response_class=HTMLResponse)
    async def audit_page(
        request: Request,
        euid: str = "",
        changed_by: str = "",
        operation_type: str = "ALL",
        limit: int = Query(50, ge=1, le=500),
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        try:
            audit = _audit_payload(
                config_path=resolved_config_path,
                user=user,
                euid=euid,
                changed_by=changed_by,
                operation_type=operation_type,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return _render(
            templates,
            request,
            "audit.html",
            user=user,
            audit=audit,
        )

    @router.get("/api/audit")
    async def audit_api(
        euid: str = "",
        changed_by: str = "",
        operation_type: str = "ALL",
        limit: int = Query(50, ge=1, le=500),
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        try:
            return jsonable_encoder(
                _audit_payload(
                    config_path=resolved_config_path,
                    user=user,
                    euid=euid,
                    changed_by=changed_by,
                    operation_type=operation_type,
                    limit=limit,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.get("/help", response_class=HTMLResponse)
    async def help_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        return _render(
            templates,
            request,
            "help.html",
            user=user,
        )

    @router.get("/search", response_class=HTMLResponse)
    async def search_page(
        request: Request,
        q: str = "",
        name_like: str = "",
        euid_like: str = "",
        record_type: str = "all",
        category: str = "",
        type: str = "",
        subtype: str = "",
        limit: int = Query(25, ge=1, le=100),
        cursor: str = "",
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
                    name_like=name_like,
                    euid_like=euid_like,
                    record_type=record_type,
                    category=category,
                    type_name=type,
                    subtype=subtype,
                    limit=limit,
                    cursor=cursor,
                )
        query = {
            "q": q,
            "name_like": name_like,
            "euid_like": euid_like,
            "record_type": record_type,
            "category": category,
            "type": type,
            "subtype": subtype,
            "limit": limit,
            "cursor": cursor,
        }
        next_cursor = str(results["page"].get("next_cursor") or "")
        return _render(
            templates,
            request,
            "search.html",
            user=user,
            results=results,
            query=query,
            next_url=(
                gui_url_with_query(
                    request, "/search", **{**query, "cursor": next_cursor}
                )
                if next_cursor
                else None
            ),
        )

    @router.get("/api/search")
    async def search_api(
        q: str = "",
        name_like: str = "",
        euid_like: str = "",
        record_type: str = "all",
        category: str = "",
        type: str = "",
        subtype: str = "",
        limit: int = Query(25, ge=1, le=100),
        cursor: str = "",
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
                    name_like=name_like,
                    euid_like=euid_like,
                    record_type=record_type,
                    category=category,
                    type_name=type,
                    subtype=subtype,
                    limit=limit,
                    cursor=cursor,
                )

    @router.get("/templates", response_class=HTMLResponse)
    async def templates_page(
        request: Request,
        category: str = "",
        repository_pack: str = "",
        repository_error: str = "",
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
                inventory = None
                if repository_pack and str(user.get("role") or "").lower() == "admin":
                    cfg = get_db_config(config_path=resolved_config_path)
                    try:
                        inventory = repository_inventory(
                            session,
                            repository_pack,
                            domain_code=str(cfg["domain_code"]),
                            issuer_app_code=str(cfg["owner_repo_name"]),
                        )
                    except Exception as exc:
                        inventory = {"status": "failed", "error": str(exc), "items": []}
                inventory_by_euid = {
                    str(item.get("stored_euid")): str(item.get("status") or "failed")
                    for item in (inventory or {}).get("items", [])
                    if isinstance(item, dict) and item.get("stored_euid")
                }
                for row in rows:
                    row["repository_status"] = inventory_by_euid.get(
                        str(row.get("euid")), "pending"
                    )
        return _render(
            templates,
            request,
            "templates.html",
            user=user,
            items=rows,
            category=category,
            repository_pack=repository_pack,
            repository_inventory=inventory,
            repository_error=repository_error,
        )

    @router.post("/templates/repository/export")
    async def templates_repository_export_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        repository_pack = str(form.get("repository_pack") or "")
        cfg = get_db_config(config_path=resolved_config_path)
        try:
            with get_db(resolved_config_path) as conn:
                conn.app_username = user.get("username")
                with conn.session_scope() as session:
                    export_repository_pack(
                        session,
                        repository_pack,
                        domain_code=str(cfg["domain_code"]),
                        issuer_app_code=str(cfg["owner_repo_name"]),
                        prefix_registry_path=str(cfg["prefix_ownership_registry_path"]),
                        actor=str(user.get("username") or ""),
                    )
        except Exception as exc:
            return RedirectResponse(
                gui_url_with_query(
                    request,
                    "/templates",
                    repository_pack=repository_pack,
                    repository_error=str(exc)[:200],
                ),
                status_code=303,
            )
        return RedirectResponse(
            gui_url_with_query(request, "/templates", repository_pack=repository_pack),
            status_code=303,
        )

    @router.get("/api/templates/repository/status")
    async def templates_repository_status_api(
        repository_pack: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                try:
                    return repository_inventory(
                        session,
                        repository_pack,
                        domain_code=str(cfg["domain_code"]),
                        issuer_app_code=str(cfg["owner_repo_name"]),
                    )
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/templates/repository/download")
    async def templates_repository_download_api(
        euid: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Download canonical pack bytes without creating a server-side artifact."""

        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                try:
                    content = repository_pack_bytes(
                        session,
                        domain_code=str(cfg["domain_code"]),
                        issuer_app_code=str(cfg["owner_repo_name"]),
                        template_euid=euid.strip() or None,
                    )
                except LookupError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
        return Response(
            content=content,
            media_type="application/json",
            headers={
                "Content-Disposition": (
                    'attachment; filename="tapdb-repository-template-pack.json"'
                ),
                "X-Content-Type-Options": "nosniff",
            },
        )

    @router.post("/api/templates/repository/export")
    async def templates_repository_export_api(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await _read_optional_json_object(request)
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                try:
                    return export_repository_pack(
                        session,
                        str(payload.get("repository_pack") or ""),
                        domain_code=str(cfg["domain_code"]),
                        issuer_app_code=str(cfg["owner_repo_name"]),
                        prefix_registry_path=str(cfg["prefix_ownership_registry_path"]),
                        actor=str(user.get("username") or ""),
                        template_euid=str(payload.get("euid") or "") or None,
                    )
                except FileExistsError as exc:
                    raise HTTPException(status_code=409, detail=str(exc)) from exc
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/api/templates/repository/import")
    async def templates_repository_import_api(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await _read_optional_json_object(request)
        cfg = get_db_config(config_path=resolved_config_path)
        apply = payload.get("apply") is True
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=apply) as session:
                try:
                    return asdict(
                        import_repository_pack(
                            session,
                            str(payload.get("repository_pack") or ""),
                            domain_code=str(cfg["domain_code"]),
                            owner_repo_name=str(cfg["owner_repo_name"]),
                            domain_registry_path=str(cfg["domain_registry_path"]),
                            prefix_registry_path=str(
                                cfg["prefix_ownership_registry_path"]
                            ),
                            dry_run=not apply,
                        )
                    )
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/templates/new", response_class=HTMLResponse)
    async def template_new_page(
        request: Request,
        seed_euid: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = _example_template_pack()
        seed_template = None
        if str(seed_euid or "").strip():
            with get_db(resolved_config_path) as conn:
                conn.app_username = user.get("username")
                with conn.session_scope() as session:
                    template = (
                        session.query(generic_template)
                        .filter_by(
                            euid=str(seed_euid).strip(),
                            is_deleted=False,
                        )
                        .first()
                    )
                    if template is None:
                        raise HTTPException(
                            status_code=404,
                            detail=f"Template seed not found: {seed_euid}",
                        )
                    payload = _template_seed_pack(template)
                    seed_template = _template_row(template)
        return _render(
            templates,
            request,
            "template_editor.html",
            user=user,
            **_template_editor_context(
                payload,
                seed_template=seed_template,
                use_default_builder=True,
            ),
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
            **_template_editor_context(
                _example_template_pack(),
                use_default_builder=True,
            ),
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
            **_template_editor_context(payload),
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
                **_template_editor_context(payload),
                issues=issues,
                saved=None,
            )
        try:
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
        except HTTPException:
            raise
        except (
            json.JSONDecodeError,
            KeyError,
            OSError,
            RuntimeError,
            ValueError,
        ) as exc:
            issues = [
                ConfigIssue(level="error", message=f"Template save failed: {exc}")
            ]
            return _render(
                templates,
                request,
                "template_editor.html",
                user=user,
                **_template_editor_context(payload),
                issues=issues,
                saved=None,
            )
        return _render(
            templates,
            request,
            "template_editor.html",
            user=user,
            **_template_editor_context(payload),
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
                template_payload, template_code = _template_payload_and_code(template)
        return _render(
            templates,
            request,
            "create.html",
            user=user,
            template=template_payload,
            template_code=template_code,
            error=None,
            form={
                "properties_json": _template_properties_form_json(template_payload),
            },
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
        try:
            with get_db(resolved_config_path) as conn:
                conn.app_username = user.get("username")
                with conn.session_scope(commit=True) as session:
                    created = _create_instance_from_template(
                        session,
                        cfg=cfg,
                        template_euid=template_euid,
                        name=name.strip(),
                        properties=properties,
                        create_children=str(create_children).lower()
                        in {"true", "1", "on"},
                    )
                    instance_euid = created["instance_euid"]
        except ValueError as exc:
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
                            status_code=404,
                            detail=f"Template not found: {template_euid}",
                        ) from exc
                    template_payload, template_code = _template_payload_and_code(
                        template
                    )
            return _render(
                templates,
                request,
                "create.html",
                user=user,
                template=template_payload,
                template_code=template_code,
                error=str(exc),
                form={
                    "name": name,
                    "properties_json": json.dumps(properties, indent=2, sort_keys=True),
                    "create_children": create_children,
                },
            )
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

    def _graph_payload(
        session: Any,
        *,
        service_id: str,
        start_euid: str,
        depth: int,
        max_nodes: int,
        max_edges: int,
    ) -> dict[str, Any]:
        exact_start = str(start_euid or "").strip()
        if exact_start:
            obj, record_type = find_object_by_euid(session, exact_start)
            if obj is None or record_type is None:
                raise HTTPException(
                    status_code=404, detail=f"Object not found: {exact_start}"
                )
            return build_graph_v2_payload(
                obj,
                record_type=record_type,
                service_id=service_id,
                depth=depth,
                max_nodes=max_nodes,
            )

        instances = (
            session.query(generic_instance)
            .filter_by(is_deleted=False)
            .order_by(generic_instance.uid.asc())
            .limit(max_nodes + 1)
            .all()
        )
        visible_uids = [int(row.uid) for row in instances[:max_nodes]]
        if visible_uids:
            lineages = (
                session.query(generic_instance_lineage)
                .filter(
                    generic_instance_lineage.is_deleted.is_(False),
                    generic_instance_lineage.parent_instance_uid.in_(visible_uids),
                    generic_instance_lineage.child_instance_uid.in_(visible_uids),
                )
                .order_by(generic_instance_lineage.uid.asc())
                .limit(max_edges + 1)
                .all()
            )
        else:
            lineages = []
        return build_visible_graph_v2_payload(
            instances,
            lineages,
            service_id=service_id,
            max_nodes=max_nodes,
            max_edges=max_edges,
        )

    @router.get("/graph", response_class=HTMLResponse)
    async def graph_page(
        request: Request,
        start_euid: str = "",
        depth: int = Query(4, ge=0, le=10),
        max_nodes: int = Query(200, ge=1, le=1_000),
        max_edges: int = Query(500, ge=1, le=5_000),
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                graph = _graph_payload(
                    session,
                    service_id=str(cfg.get("client_id") or "tapdb"),
                    start_euid=start_euid,
                    depth=depth,
                    max_nodes=max_nodes,
                    max_edges=max_edges,
                )
        return _render(
            templates,
            request,
            "graph.html",
            user=user,
            euid=str(start_euid or "").strip(),
            depth=depth,
            max_nodes=max_nodes,
            max_edges=max_edges,
            graph=graph,
        )

    @router.get("/api/graph")
    async def graph_api(
        start_euid: str = "",
        depth: int = Query(4, ge=0, le=10),
        max_nodes: int = Query(200, ge=1, le=1_000),
        max_edges: int = Query(500, ge=1, le=5_000),
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                return _graph_payload(
                    session,
                    service_id=str(cfg.get("client_id") or "tapdb"),
                    start_euid=start_euid,
                    depth=depth,
                    max_nodes=max_nodes,
                    max_edges=max_edges,
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
                graph = build_graph_v2_payload(
                    obj,
                    record_type=record_type,
                    service_id=str(cfg.get("client_id") or "tapdb"),
                    depth=depth,
                    max_nodes=1_000,
                )
        return _render(
            templates,
            request,
            "graph.html",
            user=user,
            euid=euid,
            depth=depth,
            max_nodes=1_000,
            max_edges=500,
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
                return build_graph_v2_payload(
                    obj,
                    record_type=record_type,
                    service_id=str(cfg.get("client_id") or "tapdb"),
                    depth=depth,
                    max_nodes=1_000,
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
            external_identifiers=context["external_identifiers"],
            manual_create_allowed=context["manual_create_allowed"],
            editor=context["editor"],
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

    @router.patch("/api/objects/{euid}")
    async def governed_object_update_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await _read_optional_json_object(request)
        changes = payload.get("changes")
        if not isinstance(changes, dict):
            raise HTTPException(status_code=400, detail="changes must be an object")
        apply = payload.get("apply") is True
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=apply) as session:
                try:
                    return update_object(
                        session,
                        ObjectSelector(euid=euid),
                        changes,
                        actor=str(user.get("username") or ""),
                        dry_run=not apply,
                    )
                except LookupError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                except PermissionError as exc:
                    raise HTTPException(status_code=403, detail=str(exc)) from exc
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.delete("/api/objects/{euid}")
    async def governed_object_delete_api(
        euid: str,
        apply: bool = False,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=apply) as session:
                try:
                    return soft_delete_object(
                        session,
                        ObjectSelector(euid=euid),
                        actor=str(user.get("username") or ""),
                        dry_run=not apply,
                    )
                except LookupError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                except PermissionError as exc:
                    raise HTTPException(status_code=403, detail=str(exc)) from exc
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/object/{euid}/editor-data")
    async def object_editor_data_api(
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                try:
                    return jsonable_encoder(editor_data_for_object(session, euid))
                except LookupError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.post("/api/object/{euid}/assess")
    async def object_assess_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        payload = await _read_optional_json_object(request)
        context = (
            payload.get("context") if isinstance(payload.get("context"), dict) else {}
        )
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                try:
                    assessment = assess_object(
                        session,
                        euid,
                        validator_ref=str(payload.get("validator_ref") or ""),
                        context=context,
                    )
                except LookupError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
        return jsonable_encoder(assessment.to_dict())

    @router.post("/api/object/{euid}/revalidate")
    async def object_revalidate_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        payload = await _read_optional_json_object(request)
        context = (
            payload.get("context") if isinstance(payload.get("context"), dict) else {}
        )
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                try:
                    assessment = assess_object(
                        session,
                        euid,
                        validator_ref=str(payload.get("validator_ref") or ""),
                        context={**context, "operation": "revalidate"},
                    )
                except LookupError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
        return jsonable_encoder(
            {"revalidated": True, "assessment": assessment.to_dict()}
        )

    @router.get("/api/object/{euid}/repair-recommendations")
    async def object_repair_recommendations_api(
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_user),
    ):
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                try:
                    assessment = assess_object(session, euid)
                except LookupError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
        return jsonable_encoder(
            {
                "subject_ref": assessment.subject_ref,
                "validator_ref": assessment.validator_ref,
                "repair_recommendations": [
                    recommendation.__dict__
                    for recommendation in assessment.repair_recommendations
                ],
                "subject_mutated": False,
            }
        )

    @router.post("/object/{euid}/repairs")
    async def create_repair(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        _require_form_apply(form)
        payload = _parse_json_object(
            str(form.get("repair_payload") or "{}"), label="repair_payload"
        )
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _create_object_repair(
                    session,
                    cfg=cfg,
                    euid=euid,
                    actor=str(user.get("username") or ""),
                    reason=str(form.get("reason") or ""),
                    repair_payload=payload,
                )
        return RedirectResponse(
            gui_url_with_query(request, f"/object/{euid}", notice="repair_created"),
            status_code=303,
        )

    @router.post("/api/object/{euid}/repairs")
    async def create_repair_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await _read_optional_json_object(request)
        _reject_immutable_object_fields(payload)
        _reject_unknown_payload_fields(
            payload, allowed={"apply", "reason", "repair_payload"}
        )
        repair_payload = payload.get("repair_payload")
        if not isinstance(repair_payload, dict):
            raise HTTPException(
                status_code=400, detail="repair_payload must be a JSON object"
            )
        cfg = get_db_config(config_path=resolved_config_path)
        apply = payload.get("apply") is True
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=apply) as session:
                return jsonable_encoder(
                    _create_object_repair(
                        session,
                        cfg=cfg,
                        euid=euid,
                        actor=str(user.get("username") or ""),
                        reason=str(payload.get("reason") or ""),
                        repair_payload=repair_payload,
                        dry_run=not apply,
                    )
                )

    @router.post("/object/{euid}/edit-json")
    async def edit_json(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        _require_form_apply(form)
        json_addl = str(form.get("json_addl") or "")
        payload = _parse_json_object(json_addl, label="json_addl")
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _create_object_repair(
                    session,
                    cfg=cfg,
                    euid=euid,
                    actor=str(user.get("username") or ""),
                    reason="JSON repair submitted through legacy edit-json route",
                    repair_payload=payload,
                )
        return RedirectResponse(
            gui_url_with_query(request, f"/object/{euid}", notice="repair_created"),
            status_code=303,
        )

    @router.post("/object/{euid}/name")
    async def edit_name(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        _require_form_apply(form)
        name = str(form.get("name") or "")
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _update_object_name(
                    session,
                    euid=euid,
                    name=name,
                    actor=str(user.get("username") or ""),
                )
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
        payload = await _read_optional_json_object(request)
        _reject_immutable_object_fields(payload)
        _reject_unknown_payload_fields(payload, allowed={"apply", "name"})
        apply = payload.get("apply") is True
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=apply) as session:
                return jsonable_encoder(
                    _update_object_name(
                        session,
                        euid=euid,
                        name=str(payload.get("name") or ""),
                        actor=str(user.get("username") or ""),
                        dry_run=not apply,
                    )
                )

    @router.post("/api/object/{euid}/edit-json")
    async def edit_json_api(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        payload = await _read_optional_json_object(request)
        _reject_immutable_object_fields(payload)
        _reject_unknown_payload_fields(
            payload, allowed={"apply", "json_addl", "reason"}
        )
        json_addl = payload.get("json_addl")
        if not isinstance(json_addl, dict):
            raise HTTPException(
                status_code=400, detail="json_addl must be a JSON object"
            )
        apply = payload.get("apply") is True
        cfg = get_db_config(config_path=resolved_config_path)
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=apply) as session:
                return jsonable_encoder(
                    _create_object_repair(
                        session,
                        cfg=cfg,
                        euid=euid,
                        actor=str(user.get("username") or ""),
                        reason=str(payload.get("reason") or "").strip()
                        or "JSON repair submitted through compatibility edit-json API",
                        repair_payload=json_addl,
                        dry_run=not apply,
                    )
                )

    @router.post("/object/{euid}/status")
    async def edit_status(
        request: Request,
        euid: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        form = await _read_urlencoded_form(request)
        _require_form_apply(form)
        bstatus = str(form.get("bstatus") or "")
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _update_object_status(
                    session,
                    euid=euid,
                    bstatus=bstatus,
                    actor=str(user.get("username") or ""),
                )
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
        payload = await _read_optional_json_object(request)
        _reject_immutable_object_fields(payload)
        _reject_unknown_payload_fields(payload, allowed={"apply", "bstatus"})
        apply = payload.get("apply") is True
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=apply) as session:
                return jsonable_encoder(
                    _update_object_status(
                        session,
                        euid=euid,
                        bstatus=str(payload.get("bstatus") or ""),
                        actor=str(user.get("username") or ""),
                        dry_run=not apply,
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
        evidence_refs = _parse_evidence_refs(str(form.get("evidence_refs") or ""))
        v0_edge = None
        if evidence_refs or is_strict_canonical_edge_type(relationship_type):
            v0_edge = {
                "edge_type": relationship_type,
                "asserted_by_system": str(
                    form.get("asserted_by_system") or "tapdb-gui"
                ),
                "evidence_refs": evidence_refs,
                "correlation_id": str(form.get("correlation_id") or ""),
                "causation_id": str(form.get("causation_id") or ""),
                "edge_state": str(form.get("edge_state") or "active"),
            }
        with get_db(resolved_config_path) as conn:
            conn.app_username = user.get("username")
            with conn.session_scope(commit=True) as session:
                _add_object_lineage(
                    session,
                    euid=euid,
                    related_euid=related_euid,
                    direction=direction,
                    relationship_type=relationship_type,
                    v0_edge=v0_edge,
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
                        v0_edge=payload.get("v0_edge")
                        if isinstance(payload.get("v0_edge"), dict)
                        else None,
                    )
                )

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

    @router.get("/admin/inventory", response_class=HTMLResponse)
    async def inventory_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        try:
            inventory = _inventory_payload(
                config_path=resolved_config_path,
                username=str(user.get("username") or ""),
            )
            error = None
        except Exception as exc:
            inventory = None
            error = str(exc)
        return _render(
            templates,
            request,
            "inventory.html",
            user=user,
            inventory=inventory,
            error=error,
        )

    @router.get("/api/admin/inventory")
    async def inventory_api(
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        try:
            return _inventory_payload(
                config_path=resolved_config_path,
                username=str(user.get("username") or ""),
            )
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

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

    @router.get("/admin/runtime", response_class=HTMLResponse)
    async def runtime_info_page(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        from daylily_tapdb.runtime_info import build_runtime_info

        return _render(
            templates,
            request,
            "runtime.html",
            user=user,
            runtime_info=build_runtime_info(
                config_path=resolved_config_path,
                resolved_config=get_db_config(config_path=resolved_config_path),
            ),
        )

    @router.get("/api/admin/runtime")
    async def runtime_info_api(
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        del user
        from daylily_tapdb.runtime_info import build_runtime_info

        return build_runtime_info(
            config_path=resolved_config_path,
            resolved_config=get_db_config(config_path=resolved_config_path),
        )

    # ------------------------------------------------------------------
    # Backup and recovery lifecycle
    #
    # Every page is paired with a JSON route so the GUI is self-sufficient
    # when a host app embeds it. All of it
    # is admin-gated, and the restore form posts into
    # ``views.apply_restore_from_review`` -- the same function the management
    # JSON routes call, so HTML and JSON cannot enforce different rules.
    # ------------------------------------------------------------------

    def _backup_env():
        from daylily_tapdb.cli.db_config import get_backup_settings

        try:
            return (
                get_db_config(config_path=resolved_config_path),
                get_backup_settings(config_path=resolved_config_path),
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "backup_unavailable",
                    "message": f"Cannot resolve TapDB backup configuration: {exc}",
                },
            ) from exc

    def _validated_backup_ref(ref: str) -> str:
        """Validate a backup reference from the URL, or 400.

        Delegates to the shared validator so GUI HTML and JSON cannot disagree
        about what a valid reference is.
        """
        from daylily_tapdb.backup.views import validate_backup_ref

        try:
            return validate_backup_ref(ref)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid_backup_ref") from exc

    def _receipt_url(
        request: Request,
        receipt_id: str | None,
        *,
        fallback_notice: str = "",
        fallback_error: str = "",
    ) -> str:
        """Link to the evidence page, or fall back if no receipt was written.

        ``record_receipt=False`` paths and dry runs legitimately produce no
        receipt; those still need somewhere to land.
        """
        if receipt_id:
            return gui_url(request, f"/admin/backups/receipts/{receipt_id}")
        return gui_url_with_query(
            request, "/admin/backups", notice=fallback_notice, error=fallback_error
        )

    def _backups_context() -> dict[str, Any]:
        from daylily_tapdb.backup import views as backup_views

        cfg, settings = _backup_env()
        return backup_views.inventory_context(cfg, settings)

    @router.get("/admin/backups", response_class=HTMLResponse)
    async def backups_page(
        request: Request,
        notice: str = "",
        error: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        return _render(
            templates,
            request,
            "backups.html",
            user=user,
            backups=_backups_context(),
            notice=notice,
            error=error,
            create_url=gui_url(request, "/admin/backups/create"),
            verify_url_for=lambda ref: gui_url(request, f"/admin/backups/{ref}/verify"),
            rehearse_url_for=lambda ref: gui_url(
                request, f"/admin/backups/{ref}/rehearse"
            ),
            restore_url_for=lambda ref: gui_url(
                request, f"/admin/backups/{ref}/restore"
            ),
            receipt_url_for=lambda rid: gui_url(
                request, f"/admin/backups/receipts/{rid}"
            ),
        )

    @router.get("/admin/backups/receipts/{receipt_id}", response_class=HTMLResponse)
    async def backups_receipt(
        request: Request,
        receipt_id: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Render one receipt, including the checks that were actually run.

        This is the evidence page. Operations used to redirect here-less, with
        a one-word notice, discarding the verification results entirely.
        """
        from daylily_tapdb.backup import service as backup_service
        from daylily_tapdb.backup.receipts import read_receipts

        _cfg, settings = _backup_env()
        wanted = _validated_backup_ref(receipt_id)
        match = next(
            (
                r
                for r in read_receipts(backup_service.receipts_directory(settings))
                if r.receipt_id == wanted
            ),
            None,
        )
        if match is None:
            raise HTTPException(status_code=404, detail="no such receipt")

        checks = list(match.detail.get("checks") or [])
        return _render(
            templates,
            request,
            "backup_receipt.html",
            user=user,
            receipt=match,
            checks=checks,
            failed_checks=[c for c in checks if c.get("status") == "fail"],
            backups_url=gui_url(request, "/admin/backups"),
        )

    @router.get("/api/admin/backups")
    async def backups_api(
        backup_class: str | None = Query(None, alias="class"),
        limit: int | None = Query(None, ge=0, le=1_000),
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        del user
        from admin import backups as backups_api

        cfg, settings = _backup_env()
        return jsonable_encoder(
            backups_api.list_payload(
                cfg,
                settings,
                backup_class=backup_class,
                limit=limit,
            )
        )

    @router.get("/api/admin/backups/status")
    async def backups_status_api(
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        del user
        from daylily_tapdb.backup import views as backup_views

        cfg, settings = _backup_env()
        return jsonable_encoder(backup_views.status_context(cfg, settings))

    @router.get("/api/admin/backups/health")
    async def backups_health_api(
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Return the same recovery-health verdict as the backup CLI."""
        del user
        from admin import backups as backups_api

        cfg, settings = _backup_env()
        return jsonable_encoder(backups_api.health_payload(cfg, settings))

    @router.get("/api/admin/backups/plan")
    async def backups_plan_api(
        backup_class: str | None = Query(None, alias="class"),
        strict: bool = Query(False),
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Preview a backup without writing an artifact or receipt."""
        del user
        from admin import backups as backups_api

        cfg, settings = _backup_env()
        return jsonable_encoder(
            backups_api.plan_payload(
                cfg,
                settings,
                backup_class=backup_class,
                strict=strict,
            )
        )

    @router.post("/api/admin/backups", status_code=201)
    async def backups_create_api(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Create a backup through the canonical GUI JSON surface."""
        del user
        from admin import backups as backups_api

        cfg, settings = _backup_env()
        return jsonable_encoder(
            backups_api.create_payload(
                cfg,
                settings,
                body=await _read_optional_json_object(request),
                actor=backups_api.api_actor(request),
            )
        )

    @router.post("/api/admin/backups/{ref}/verify")
    async def backups_verify_api(
        request: Request,
        ref: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Verify one backup and return its complete evidence payload."""
        del user
        from admin import backups as backups_api

        cfg, settings = _backup_env()
        body = await _read_optional_json_object(request)
        return jsonable_encoder(
            backups_api.verify_payload(
                cfg,
                settings,
                ref=ref,
                level=str(body.get("level") or "deep"),
                actor=backups_api.api_actor(request),
            )
        )

    @router.post("/api/admin/backups/{ref}/restore/stage")
    async def backups_restore_stage_api(
        request: Request,
        ref: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Stage a restore and return the fingerprint; never mutates the target."""
        del user
        from admin import backups as backups_api

        cfg, settings = _backup_env()
        return jsonable_encoder(
            backups_api.stage_payload(
                cfg,
                settings,
                ref=ref,
                body=await _read_optional_json_object(request),
            )
        )

    @router.post("/api/admin/backups/{ref}/restore/apply")
    async def backups_restore_apply_api(
        request: Request,
        ref: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Apply an exactly staged restore through the shared recovery view."""
        del user
        from admin import backups as backups_api

        cfg, settings = _backup_env()
        return jsonable_encoder(
            backups_api.apply_payload(
                cfg,
                settings,
                ref=ref,
                body=await _read_optional_json_object(request),
                actor=backups_api.api_actor(request),
            )
        )

    @router.post("/api/admin/backups/{ref}/rehearse")
    async def backups_rehearse_api(
        request: Request,
        ref: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        """Rehearse a restore into a throwaway target and return receipts."""
        del user
        from admin import backups as backups_api

        cfg, settings = _backup_env()
        return jsonable_encoder(
            backups_api.rehearse_payload(
                cfg,
                settings,
                ref=ref,
                body=await _read_optional_json_object(request),
                actor=backups_api.api_actor(request),
            )
        )

    @router.post("/admin/backups/create")
    async def backups_create(
        request: Request,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        from daylily_tapdb.backup import service as backup_service
        from daylily_tapdb.backup.receipts import SURFACE_GUI, Actor

        form = await request.form()
        cfg, settings = _backup_env()
        actor = Actor(surface=SURFACE_GUI, username=user.get("email"))
        try:
            created = backup_service.create_backup(
                cfg,
                settings,
                backup_class=str(form.get("backup_class") or "full"),
                allow_drift=bool(form.get("allow_drift")),
                note=str(form.get("note") or "") or None,
                actor=actor,
            )
        except Exception as exc:
            return RedirectResponse(
                gui_url_with_query(request, "/admin/backups", error=str(exc)[:200]),
                status_code=303,
            )
        # Same evidence page as verify/rehearse/restore -- a create runs its
        # own quick verification, and those verdicts are worth showing.
        return RedirectResponse(
            _receipt_url(request, created.receipt_id, fallback_notice="backup_created"),
            status_code=303,
        )

    @router.post("/admin/backups/{ref}/verify")
    async def backups_verify(
        request: Request,
        ref: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        from daylily_tapdb.backup import service as backup_service
        from daylily_tapdb.backup.receipts import SURFACE_GUI, Actor

        cfg, settings = _backup_env()
        try:
            report = backup_service.verify_backup(
                cfg,
                settings,
                backup_id=_validated_backup_ref(ref),
                actor=Actor(surface=SURFACE_GUI, username=user.get("email")),
            )
        except Exception as exc:
            return RedirectResponse(
                gui_url_with_query(request, "/admin/backups", error=str(exc)[:200]),
                status_code=303,
            )
        return RedirectResponse(
            _receipt_url(
                request,
                report.receipt_id,
                fallback_error="" if report.ok else "verify_failed",
            ),
            status_code=303,
        )

    @router.post("/admin/backups/{ref}/rehearse")
    async def backups_rehearse(
        request: Request,
        ref: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        from daylily_tapdb.backup import verify as backup_verify
        from daylily_tapdb.backup.receipts import SURFACE_GUI, Actor

        cfg, settings = _backup_env()
        try:
            evidence = backup_verify.rehearse_restore(
                cfg,
                settings,
                backup_id=_validated_backup_ref(ref),
                actor=Actor(surface=SURFACE_GUI, username=user.get("email")),
            )
        except Exception as exc:
            return RedirectResponse(
                gui_url_with_query(request, "/admin/backups", error=str(exc)[:200]),
                status_code=303,
            )
        return RedirectResponse(
            _receipt_url(
                request,
                evidence.receipt_id,
                fallback_error="" if evidence.ok else "rehearsal_failed",
            ),
            status_code=303,
        )

    def _review(request: Request, ref: str, mode: str) -> dict[str, Any]:
        from daylily_tapdb.backup import verify as backup_verify
        from daylily_tapdb.backup import views as backup_views

        cfg, settings = _backup_env()
        options = backup_verify.RestoreOptions(mode=mode or "isolated")
        return backup_views.restore_review_context(
            cfg, settings, backup_id=_validated_backup_ref(ref), options=options
        )

    @router.get("/admin/backups/{ref}/restore", response_class=HTMLResponse)
    async def backups_restore_review(
        request: Request,
        ref: str,
        mode: str = "isolated",
        error: str = "",
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        return _render(
            templates,
            request,
            "restore_review.html",
            user=user,
            review=_review(request, ref, mode),
            error=error,
            apply_url=gui_url(request, f"/admin/backups/{ref}/restore"),
            review_url=gui_url(request, f"/admin/backups/{ref}/restore"),
            backups_url=gui_url(request, "/admin/backups"),
        )

    @router.get("/api/admin/backups/{ref}/restore")
    async def backups_restore_review_api(
        ref: str,
        mode: str = "isolated",
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        del user
        from daylily_tapdb.backup import verify as backup_verify
        from daylily_tapdb.backup import views as backup_views

        cfg, settings = _backup_env()
        return jsonable_encoder(
            backup_views.restore_review_context(
                cfg,
                settings,
                backup_id=_validated_backup_ref(ref),
                options=backup_verify.RestoreOptions(mode=mode or "isolated"),
            )
        )

    @router.post("/admin/backups/{ref}/restore", response_class=HTMLResponse)
    async def backups_restore_apply(
        request: Request,
        ref: str,
        user: dict[str, Any] = Depends(require_tapdb_gui_admin),
    ):
        from daylily_tapdb.backup import verify as backup_verify
        from daylily_tapdb.backup import views as backup_views
        from daylily_tapdb.backup.receipts import SURFACE_GUI, Actor

        form = await request.form()
        mode = str(form.get("mode") or "isolated")
        cfg, settings = _backup_env()
        options = backup_verify.RestoreOptions(
            mode=mode,
            target_database=str(form.get("target_database") or "") or None,
            target_schema=str(form.get("target_schema") or "") or None,
            keep_superseded=bool(form.get("keep_superseded")),
        )
        # The CLI and management JSON routes both validate these before use. quote_ident
        # holds the line either way, but a surface that skips the shared
        # validator is a surface that can drift -- and the operator gets a SQL
        # error instead of a clear rejection.
        try:
            options.validated_target_database()
            options.validated_target_schema()
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail=f"invalid restore options: {exc}"
            ) from exc

        try:
            result = backup_views.apply_restore_from_review(
                cfg,
                settings,
                backup_id=_validated_backup_ref(ref),
                plan_fingerprint=str(form.get("plan_fingerprint") or "") or None,
                confirm_target=str(form.get("confirm_target") or "") or None,
                options=options,
                actor=Actor(surface=SURFACE_GUI, username=user.get("email")),
            )
        except Exception as exc:
            # Re-render the review with a *fresh* fingerprint. Handing back the
            # stale one would let the operator retry into the same refusal.
            return _render(
                templates,
                request,
                "restore_review.html",
                user=user,
                review=_review(request, ref, mode),
                error=str(exc)[:300],
                apply_url=gui_url(request, f"/admin/backups/{ref}/restore"),
                review_url=gui_url(request, f"/admin/backups/{ref}/restore"),
                backups_url=gui_url(request, "/admin/backups"),
            )

        return RedirectResponse(
            _receipt_url(request, result.receipt_id, fallback_notice="restore_applied"),
            status_code=303,
        )

    return router


def create_tapdb_gui_app(
    *,
    config_path: str,
    host_bridge: TapdbHostBridge | None = None,
):
    """Build TapDB's single standalone and embeddable ASGI application."""

    resolved_config_path = str(Path(config_path).expanduser().resolve())
    set_cli_context(config_path=resolved_config_path)
    settings = get_admin_settings(config_path=resolved_config_path)
    target_name = str(settings.get("target_name") or "target").strip().lower()
    production_like = (
        bool(settings.get("production_like"))
        or target_name
        in {
            "prod",
            "production",
        }
        or target_name.startswith("prod-")
        or target_name.endswith("-prod")
    )
    auth_mode = str(settings.get("auth_mode") or "").strip().lower()
    if production_like:
        if auth_mode == "disabled":
            raise RuntimeError(
                "Refusing to start production-like TapDB GUI with disabled auth"
            )
        if (
            auth_mode == "shared_host"
            and not str(settings.get("shared_host_session_secret") or "").strip()
        ):
            raise RuntimeError(
                "Refusing to start production-like TapDB GUI shared_host auth "
                "without admin.auth.shared_host.session_secret"
            )
        if not str(settings.get("session_secret") or "").strip():
            raise RuntimeError(
                "Refusing to start production-like TapDB GUI without "
                "admin.session.secret"
            )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        try:
            yield
        finally:
            stop_all_writers()
            dispose_all_engines()
            dispose_all_runtime_engines()

    app = FastAPI(
        title="TapDB GUI",
        description=(
            "Canonical TapDB object, lineage, audit, recovery, and DAG-v2 interface"
        ),
        version=__version__,
        lifespan=lifespan,
    )
    app.state.tapdb_host_bridge = host_bridge
    app.state.tapdb_gui_canonical = True
    app.state.tapdb_config_path = resolved_config_path

    @app.middleware("http")
    async def metrics_request_context(request: Request, call_next):
        token_path = request_path_var.set(request.url.path)
        token_method = request_method_var.set(request.method)
        try:
            return await call_next(request)
        finally:
            request_path_var.reset(token_path)
            request_method_var.reset(token_method)

    session_secret = str(settings.get("session_secret") or "").strip()
    app.add_middleware(
        SessionMiddleware,
        secret_key=session_secret or secrets.token_hex(32),
        session_cookie=SESSION_COOKIE_NAME,
        max_age=86400,
        same_site="lax",
        https_only=production_like,
    )
    allow_local = not production_like
    allowed_origins = validate_allowed_origins(
        [str(item) for item in settings.get("allowed_origins") or []],
        allow_local=allow_local,
    )
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=build_trusted_hosts(allow_local=allow_local),
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_origin_regex=(
            None
            if allowed_origins
            else build_allowed_origin_regex(allow_local=allow_local)
        ),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def enforce_origin_allowlist(request: Request, call_next):
        origin = request.headers.get("origin")
        if origin and not is_allowed_origin(origin, allow_local=allow_local):
            return PlainTextResponse("Origin not allowed", status_code=403)
        return await call_next(request)

    templates = _build_templates(host_bridge)
    from daylily_tapdb.gui.auth_routes import create_tapdb_gui_auth_router

    app.include_router(create_tapdb_gui_auth_router(templates=templates))
    app.include_router(
        create_tapdb_gui_router(
            config_path=resolved_config_path,
            host_bridge=host_bridge,
        )
    )
    from daylily_tapdb.admin_health import install_tapdb_admin_health_routes

    install_tapdb_admin_health_routes(app, config_path=resolved_config_path)
    cfg = get_db_config(config_path=resolved_config_path)
    service_id = str(
        (host_bridge.service_name if host_bridge is not None else "")
        or cfg.get("client_id")
        or "tapdb"
    ).strip()
    display_name = str(
        (host_bridge.app_name if host_bridge is not None else "") or service_id
    ).strip()
    dag_mount = mount_tapdb_dag_surfaces(
        app,
        config_path=resolved_config_path,
        service_id=service_id,
        display_name=display_name,
        auth_dependency=require_tapdb_gui_user,
        limits=DagV2Limits(
            max_depth=8,
            max_nodes=1_000,
            max_search_page_size=100,
        ),
    )
    if not dag_mount.mounted:
        raise RuntimeError(f"TapDB DAG v2 mount failed: {dag_mount.diagnostic}")
    app.state.tapdb_dag_router_attached = True
    if host_bridge is not None and host_bridge.auth_mode == "host_session":
        return TapdbHostBridgeMount(app, host_bridge)
    return app


def _example_template_pack() -> dict[str, Any]:
    return {
        "templates": [
            {
                "name": "Example Actor",
                "polymorphic_discriminator": "generic_template",
                "category": "actor",
                "type": "person",
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
                "name": "Example Well",
                "polymorphic_discriminator": "generic_template",
                "category": "container",
                "type": "well",
                "subtype": "generic",
                "version": "1.0",
                "instance_prefix": "WEN",
                "instance_polymorphic_identity": "generic_instance",
                "json_addl": {
                    "properties": {
                        "position": "",
                    },
                    "instantiation_layouts": [],
                },
            },
            {
                "name": "Example Plate",
                "polymorphic_discriminator": "generic_template",
                "category": "container",
                "type": "plate",
                "subtype": "96well-generic",
                "version": "1.0",
                "instance_prefix": "PAT",
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
                                    "template_code": "container/well/generic/1.0",
                                    "count": 96,
                                }
                            ],
                        }
                    ],
                },
            },
        ]
    }
