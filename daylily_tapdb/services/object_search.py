"""Reusable TapDB object search for DAG federation."""

from __future__ import annotations

from typing import Any

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template

SEARCH_RECORD_TYPES = {"all", "template", "instance", "lineage"}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _clean(value).lower()


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return value.isoformat()
    except Exception:
        return _clean(value) or None


def _record_type(value: Any) -> str:
    normalized = _lower(value) or "all"
    return normalized if normalized in SEARCH_RECORD_TYPES else "all"


def _matches_text(row: Any, q: str) -> bool:
    if not q:
        return True
    haystack = " ".join(
        _lower(getattr(row, name, None))
        for name in (
            "euid",
            "name",
            "category",
            "type",
            "subtype",
            "version",
            "bstatus",
            "relationship_type",
        )
    )
    return q in haystack


def _matches_filters(
    row: Any,
    *,
    q: str,
    euid: str,
    category: str,
    type_name: str,
    subtype: str,
    tenant_id: str,
    relationship_type: str,
) -> bool:
    if q and not _matches_text(row, q):
        return False
    if euid and _lower(getattr(row, "euid", None)) != euid:
        return False
    if category and _lower(getattr(row, "category", None)) != category:
        return False
    if type_name and _lower(getattr(row, "type", None)) != type_name:
        return False
    if subtype and _lower(getattr(row, "subtype", None)) != subtype:
        return False
    if tenant_id and _lower(getattr(row, "tenant_id", None)) != tenant_id:
        return False
    if (
        relationship_type
        and _lower(getattr(row, "relationship_type", None)) != relationship_type
    ):
        return False
    return True


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
        "graph_href": f"/api/dag/data?start_euid={euid or ''}",
        "created_dt": _iso(getattr(row, "created_dt", None)),
        "modified_dt": _iso(getattr(row, "modified_dt", None)),
    }


def search_objects(
    session: Any,
    *,
    service_name: str,
    q: str = "",
    euid: str = "",
    record_type: str = "all",
    category: str = "",
    type_name: str = "",
    subtype: str = "",
    tenant_id: str = "",
    relationship_type: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Search TapDB objects across templates, instances, and lineages."""

    normalized_record_type = _record_type(record_type)
    selected = (
        ["template", "instance", "lineage"]
        if normalized_record_type == "all"
        else [normalized_record_type]
    )
    normalized_limit = max(1, min(100, int(limit or 25)))
    filters = {
        "q": _lower(q),
        "euid": _lower(euid),
        "record_type": normalized_record_type,
        "category": _lower(category),
        "type": _lower(type_name),
        "subtype": _lower(subtype),
        "tenant_id": _lower(tenant_id),
        "relationship_type": _lower(relationship_type),
        "limit": normalized_limit,
    }
    models = {
        "template": generic_template,
        "instance": generic_instance,
        "lineage": generic_instance_lineage,
    }

    results: list[dict[str, Any]] = []
    for kind in selected:
        rows = session.query(models[kind]).filter_by(is_deleted=False).all()
        for row in rows:
            if _matches_filters(
                row,
                q=filters["q"],
                euid=filters["euid"],
                category=filters["category"],
                type_name=filters["type"],
                subtype=filters["subtype"],
                tenant_id=filters["tenant_id"],
                relationship_type=filters["relationship_type"],
            ):
                results.append(
                    _to_search_result(row, record_type=kind, service_name=service_name)
                )

    results.sort(
        key=lambda item: (
            str(item.get("created_dt") or ""),
            str(item.get("record_type") or ""),
            str(item.get("euid") or ""),
        ),
        reverse=True,
    )
    trimmed = results[:normalized_limit]
    return {
        "items": trimmed,
        "page": {
            "limit": normalized_limit,
            "total": len(results),
            "next_cursor": None,
        },
        "filters": filters,
    }
