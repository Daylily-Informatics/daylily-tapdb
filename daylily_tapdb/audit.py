"""Audit trail query utilities for TapDB.

Provides typed query helpers for the trigger-populated ``audit_log`` table,
with optional resolution of entity names across template/instance/lineage tables.

Example::

    from daylily_tapdb.audit import query_audit_trail

    with conn.session_scope(commit=False) as session:
        entries = query_audit_trail(session, changed_by="jmajor", limit=50)
        for e in entries:
            print(e.euid, e.operation_type, e.changed_at)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class AuditEntry:
    """Single row from an audit trail query with resolved entity metadata."""

    euid: str
    changed_by: Optional[str]
    operation_type: Optional[str]
    changed_at: datetime
    name: Optional[str]
    polymorphic_discriminator: Optional[str]
    category: Optional[str]
    type: Optional[str]
    subtype: Optional[str]
    bstatus: Optional[str]
    old_value: Optional[str]
    new_value: Optional[str]


_AUDIT_TRAIL_SQL = """
SELECT
    al.rel_table_euid_fk AS euid,
    al.changed_by,
    al.operation_type,
    al.changed_at,
    COALESCE(gt.name, gi.name, gil.name) AS name,
    COALESCE(
        gt.polymorphic_discriminator,
        gi.polymorphic_discriminator,
        gil.polymorphic_discriminator
    ) AS polymorphic_discriminator,
    COALESCE(gt.category, gi.category, gil.category) AS category,
    COALESCE(gt.type, gi.type, gil.type) AS type,
    COALESCE(gt.subtype, gi.subtype, gil.subtype) AS subtype,
    COALESCE(gt.bstatus, gi.bstatus, gil.bstatus) AS bstatus,
    al.old_value,
    al.new_value
FROM
    audit_log al
    LEFT JOIN generic_template gt ON al.rel_table_uid_fk = gt.uid
    LEFT JOIN generic_instance gi ON al.rel_table_uid_fk = gi.uid
    LEFT JOIN generic_instance_lineage gil ON al.rel_table_uid_fk = gil.uid
"""


def query_audit_trail(
    session: Session,
    *,
    changed_by: Optional[str] = None,
    euid: Optional[str] = None,
    since: Optional[datetime] = None,
    limit: int = 500,
    order: Literal["asc", "desc"] = "desc",
) -> list[AuditEntry]:
    """Query audit trail with optional filters.

    Args:
        session: Active SQLAlchemy session.
        changed_by: Filter by the user who made the change.
        euid: Filter by the EUID of the changed entity.
        since: Only return entries after this datetime.
        limit: Maximum number of rows to return (default 500).
        order: Sort order by changed_at ('asc' or 'desc').

    Returns:
        List of AuditEntry dataclasses.
    """
    clauses: list[str] = []
    params: dict[str, Any] = {}

    if changed_by is not None:
        clauses.append("al.changed_by = :changed_by")
        params["changed_by"] = changed_by
    if euid is not None:
        clauses.append("al.rel_table_euid_fk = :euid")
        params["euid"] = euid
    if since is not None:
        clauses.append("al.changed_at >= :since")
        params["since"] = since

    sql = _AUDIT_TRAIL_SQL
    if clauses:
        sql += "\nWHERE " + " AND ".join(clauses)

    direction = "ASC" if order == "asc" else "DESC"
    sql += f"\nORDER BY al.changed_at {direction}"
    sql += "\nLIMIT :limit"
    params["limit"] = limit

    rows = session.execute(text(sql), params).mappings().all()
    return [
        AuditEntry(
            euid=row["euid"],
            changed_by=row["changed_by"],
            operation_type=row["operation_type"],
            changed_at=row["changed_at"],
            name=row["name"],
            polymorphic_discriminator=row["polymorphic_discriminator"],
            category=row["category"],
            type=row["type"],
            subtype=row["subtype"],
            bstatus=row["bstatus"],
            old_value=row["old_value"],
            new_value=row["new_value"],
        )
        for row in rows
    ]
