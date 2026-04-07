"""Reusable exact-object lookup helpers."""

from __future__ import annotations

from typing import Any

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template


def find_object_by_euid(session: Any, euid: str) -> tuple[Any | None, str | None]:
    """Return the first non-deleted object matching an exact EUID."""

    normalized_euid = str(euid or "").strip()
    if not normalized_euid:
        return None, None

    obj = (
        session.query(generic_template)
        .filter_by(euid=normalized_euid, is_deleted=False)
        .first()
    )
    if obj is not None:
        return obj, "template"

    obj = (
        session.query(generic_instance)
        .filter_by(euid=normalized_euid, is_deleted=False)
        .first()
    )
    if obj is not None:
        return obj, "instance"

    obj = (
        session.query(generic_instance_lineage)
        .filter_by(euid=normalized_euid, is_deleted=False)
        .first()
    )
    if obj is not None:
        return obj, "lineage"

    return None, None
