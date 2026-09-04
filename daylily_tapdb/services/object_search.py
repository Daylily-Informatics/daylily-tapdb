"""SQL-filtered, keyset-paginated TapDB object search."""

from __future__ import annotations

import base64
import json
from typing import Any

from sqlalchemy import String, cast, or_
from sqlalchemy.orm import aliased

from daylily_tapdb.external_references import (
    ExternalIdentifierTarget,
    TapDBObjectTarget,
)
from daylily_tapdb.external_references import (
    _decode_cursor as _decode_external_cursor,
)
from daylily_tapdb.external_references import (
    _encode_cursor as _encode_external_cursor,
)
from daylily_tapdb.external_references import (
    _projection as _external_projection,
)
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template

SEARCH_RECORD_TYPES = {"all", "template", "instance", "lineage"}
_KINDS = ("template", "instance", "lineage")
_MODELS = {
    "template": generic_template,
    "instance": generic_instance,
    "lineage": generic_instance_lineage,
}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else _clean(value) or None


def _record_type(value: Any) -> str:
    normalized = _clean(value).lower() or "all"
    if normalized not in SEARCH_RECORD_TYPES:
        raise ValueError("record_type must be one of: all, template, instance, lineage")
    return normalized


def _encode_cursor(kind: str, uid: int) -> str:
    raw = json.dumps({"kind": kind, "uid": int(uid)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _decode_cursor(value: str) -> tuple[str, int] | None:
    raw = _clean(value)
    if not raw:
        return None
    try:
        padded = raw + "=" * (-len(raw) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        kind = str(payload["kind"])
        uid = int(payload["uid"])
    except Exception as exc:
        raise ValueError("cursor is malformed") from exc
    if kind not in _KINDS or uid < 0:
        raise ValueError("cursor is malformed")
    return kind, uid


def _to_search_result(
    row: Any, *, record_type: str, service_name: str
) -> dict[str, Any]:
    euid = getattr(row, "euid", None)
    name = getattr(row, "name", None)
    return {
        "system": service_name,
        "service": service_name,
        "record_type": record_type,
        "kind": record_type,
        "uid": getattr(row, "uid", None),
        "euid": euid,
        "name": name,
        "display_label": name or euid,
        "category": getattr(row, "category", None),
        "type": getattr(row, "type", None),
        "subtype": getattr(row, "subtype", None),
        "version": getattr(row, "version", None),
        "bstatus": getattr(row, "bstatus", None),
        "tenant_id": _clean(getattr(row, "tenant_id", None)) or None,
        "relationship_type": getattr(row, "relationship_type", None),
        "href": f"/object/{euid or ''}",
        "graph_href": f"/api/dag/v2/data?start_euid={euid or ''}",
        "created_dt": _iso(getattr(row, "created_dt", None)),
        "modified_dt": _iso(getattr(row, "modified_dt", None)),
    }


def _apply_filters(
    query: Any,
    model: Any,
    *,
    q: str,
    euid: str,
    name_like: str,
    euid_like: str,
    category: str,
    type_name: str,
    subtype: str,
    tenant_id: str,
    relationship_type: str,
) -> Any:
    query = query.filter(model.is_deleted.is_(False))
    if euid:
        query = query.filter(model.euid == euid)
    if name_like:
        query = query.filter(cast(model.name, String).ilike(f"%{name_like}%"))
    if euid_like:
        query = query.filter(cast(model.euid, String).ilike(f"%{euid_like}%"))
    if category:
        query = query.filter(model.category == category)
    if type_name:
        query = query.filter(model.type == type_name)
    if subtype:
        query = query.filter(model.subtype == subtype)
    if tenant_id:
        query = query.filter(cast(model.tenant_id, String) == tenant_id)
    if relationship_type:
        if model is not generic_instance_lineage:
            return query.filter(False)
        query = query.filter(model.relationship_type == relationship_type)
    if q:
        pattern = f"%{q}%"
        columns = [
            model.euid,
            model.name,
            model.category,
            model.type,
            model.subtype,
            model.version,
            model.bstatus,
        ]
        if model is generic_instance_lineage:
            columns.append(model.relationship_type)
        query = query.filter(
            or_(*(cast(column, String).ilike(pattern) for column in columns))
        )
    return query


def search_objects(
    session: Any,
    *,
    service_name: str,
    q: str = "",
    euid: str = "",
    name_like: str = "",
    euid_like: str = "",
    record_type: str = "all",
    category: str = "",
    type_name: str = "",
    subtype: str = "",
    tenant_id: str = "",
    relationship_type: str = "",
    limit: int = 25,
    cursor: str = "",
) -> dict[str, Any]:
    """Search using SQL predicates and a stable ``(kind, uid)`` keyset."""

    normalized_record_type = _record_type(record_type)
    selected = (
        list(_KINDS) if normalized_record_type == "all" else [normalized_record_type]
    )
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer from 1 through 100")
    normalized_limit = limit
    decoded = _decode_cursor(cursor)
    if decoded and decoded[0] not in selected:
        raise ValueError("cursor does not belong to the selected record_type")
    cursor_kind_index = _KINDS.index(decoded[0]) if decoded else -1
    cursor_uid = decoded[1] if decoded else -1
    filters: dict[str, Any] = {
        "q": _clean(q),
        "euid": _clean(euid),
        "name_like": _clean(name_like),
        "euid_like": _clean(euid_like),
        "record_type": normalized_record_type,
        "category": _clean(category),
        "type": _clean(type_name),
        "subtype": _clean(subtype),
        "tenant_id": _clean(tenant_id),
        "relationship_type": _clean(relationship_type),
        "limit": normalized_limit,
    }

    found: list[tuple[str, Any]] = []
    for kind in selected:
        kind_index = _KINDS.index(kind)
        if decoded and kind_index < cursor_kind_index:
            continue
        model = _MODELS[kind]
        query = _apply_filters(
            session.query(model),
            model,
            q=filters["q"],
            euid=filters["euid"],
            name_like=filters["name_like"],
            euid_like=filters["euid_like"],
            category=filters["category"],
            type_name=filters["type"],
            subtype=filters["subtype"],
            tenant_id=filters["tenant_id"],
            relationship_type=filters["relationship_type"],
        )
        if decoded and kind_index == cursor_kind_index:
            query = query.filter(model.uid > cursor_uid)
        remaining = normalized_limit + 1 - len(found)
        if remaining <= 0:
            break
        rows = query.order_by(model.uid.asc()).limit(remaining).all()
        found.extend((kind, row) for row in rows)

    has_more = len(found) > normalized_limit
    page_rows = found[:normalized_limit]
    items = [
        _to_search_result(row, record_type=kind, service_name=service_name)
        for kind, row in page_rows
    ]
    next_cursor = None
    if has_more and page_rows:
        last_kind, last_row = page_rows[-1]
        next_cursor = _encode_cursor(last_kind, int(last_row.uid))
    return {
        "items": items,
        "page": {
            "limit": normalized_limit,
            "returned": len(items),
            "next_cursor": next_cursor,
        },
        "filters": filters,
    }


def search_external_reference_sources(
    session: Any,
    *,
    service_name: str,
    external_service_id: str = "",
    external_object_euid: str = "",
    external_namespace: str = "",
    external_kind: str = "",
    external_value: str = "",
    external_relationship_type: str = "",
    limit: int = 25,
    cursor: str = "",
) -> dict[str, Any]:
    """Return local sources matching one complete exact external target group."""

    tapdb_values = [str(external_service_id or ""), str(external_object_euid or "")]
    opaque_values = [
        str(external_namespace or ""),
        str(external_kind or ""),
        str(external_value or ""),
    ]
    has_tapdb = any(tapdb_values)
    has_opaque = any(opaque_values)
    target: TapDBObjectTarget | ExternalIdentifierTarget
    if has_tapdb and has_opaque:
        raise ValueError("external target filter groups are mutually exclusive")
    if has_tapdb:
        if not all(tapdb_values):
            raise ValueError(
                "external_service_id and external_object_euid are required together"
            )
        target = TapDBObjectTarget(tapdb_values[0], tapdb_values[1])
        subtype = "tapdb_object"
    elif has_opaque:
        if not all(opaque_values):
            raise ValueError(
                "external_namespace, external_kind, and external_value are required together"
            )
        target = ExternalIdentifierTarget(
            namespace=opaque_values[0],
            kind=opaque_values[1],
            value=opaque_values[2],
            scope="public_global",
        )
        subtype = "opaque"
    else:
        raise ValueError("one complete external target filter group is required")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer from 1 through 100")
    after_uid = _decode_external_cursor(cursor)
    relationship = str(external_relationship_type or "")
    if relationship and (
        relationship != relationship.strip() or len(relationship) > 128
    ):
        raise ValueError(
            "external_relationship_type must be exact and at most 128 characters"
        )

    source_instance = aliased(generic_instance, name="external_source")
    reference_instance = aliased(generic_instance, name="external_reference")
    query = (
        session.query(
            generic_instance_lineage,
            source_instance,
            reference_instance,
        )
        .join(
            source_instance,
            source_instance.uid == generic_instance_lineage.parent_instance_uid,
        )
        .join(
            reference_instance,
            reference_instance.uid == generic_instance_lineage.child_instance_uid,
        )
        .filter(
            generic_instance_lineage.is_deleted.is_(False),
            generic_instance_lineage.uid > after_uid,
            source_instance.is_deleted.is_(False),
            reference_instance.is_deleted.is_(False),
            reference_instance.category == "reference",
            reference_instance.type == "external_identifier",
            reference_instance.subtype == subtype,
            reference_instance.version == "1.0",
            reference_instance.identity_key == target.identity_key,
        )
    )
    if relationship:
        query = query.filter(generic_instance_lineage.relationship_type == relationship)
    rows = query.order_by(generic_instance_lineage.uid.asc()).limit(limit + 1).all()
    page_rows = rows[:limit]
    items: list[dict[str, Any]] = []
    for lineage, source, reference in page_rows:
        item = _to_search_result(
            source, record_type="instance", service_name=service_name
        )
        _, matched = _external_projection(lineage, reference)
        item["matched_external_reference"] = matched
        items.append(item)
    return {
        "items": items,
        "page": {
            "limit": limit,
            "returned": len(items),
            "next_cursor": (
                _encode_external_cursor(int(page_rows[-1][0].uid))
                if len(rows) > limit and page_rows
                else None
            ),
        },
        "filters": {
            "external_service_id": tapdb_values[0],
            "external_object_euid": tapdb_values[1],
            "external_namespace": opaque_values[0],
            "external_kind": opaque_values[1],
            "external_value": opaque_values[2],
            "external_relationship_type": relationship,
            "record_type": "instance",
            "limit": limit,
        },
    }


__all__ = [
    "SEARCH_RECORD_TYPES",
    "search_external_reference_sources",
    "search_objects",
]
