"""Lineage traversal utilities for TapDB instances.

Provides query helpers for navigating parent/child relationships
via the ``generic_instance_lineage`` table, including recursive
graph traversal (descendants, ancestor walks, full graph extraction).

Example::

    from daylily_tapdb.lineage import get_parent_lineages, get_child_lineages

    parents = get_parent_lineages(my_instance)
    for lin in parents:
        print(lin.parent_instance_uid, lin.child_instance_uid)

    # Recursive traversal
    from daylily_tapdb.lineage import get_all_descendants, get_lineage_graph

    descendants = get_all_descendants(session, "GT-00001-BXKQ7")
    graph = get_lineage_graph(session, "GT-00001-BXKQ7", max_depth=3)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session, object_session

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage


class LineageQueryProxy:
    """Lightweight proxy that preserves iterable/.all() behavior for lineage queries."""

    def __init__(self, query):
        self._query = query

    def __iter__(self):
        return iter(self._query)

    def __len__(self):
        if isinstance(self._query, list):
            return len(self._query)
        return self._query.count()

    def __bool__(self):
        if isinstance(self._query, list):
            return bool(self._query)
        return self._query.first() is not None

    def all(self):
        if isinstance(self._query, list):
            return list(self._query)
        return self._query.all()

    def first(self):
        if isinstance(self._query, list):
            return self._query[0] if self._query else None
        return self._query.first()

    def count(self):
        if isinstance(self._query, list):
            return len(self._query)
        return self._query.count()

    def __getitem__(self, item):
        return self._query[item]

    def __getattr__(self, name):
        return getattr(self._query, name)


def _query_lineages_for_instance(instance, *, fk_attr_name: str) -> LineageQueryProxy:
    """Return a query-like proxy for lineage traversal using canonical TapDB FKs."""
    session: Session | None = object_session(instance)
    if session is None:
        return LineageQueryProxy([])

    lineage_attr = getattr(generic_instance_lineage, fk_attr_name)
    query = session.query(generic_instance_lineage).filter(lineage_attr == instance.uid)
    return LineageQueryProxy(query)


def get_parent_lineages(instance) -> LineageQueryProxy:
    """Return lineage rows where *instance* is the parent."""
    return _query_lineages_for_instance(instance, fk_attr_name="parent_instance_uid")


def get_child_lineages(instance) -> LineageQueryProxy:
    """Return lineage rows where *instance* is the child."""
    return _query_lineages_for_instance(instance, fk_attr_name="child_instance_uid")


def resolve_parent_instance(lineage):
    """Given a lineage row, resolve and return the parent instance."""
    session: Session | None = object_session(lineage)
    if session is None:
        return None
    instance_uid = getattr(lineage, "parent_instance_uid", None)
    if instance_uid is None:
        return None
    return (
        session.query(generic_instance)
        .filter(generic_instance.uid == instance_uid)
        .first()
    )


def resolve_child_instance(lineage):
    """Given a lineage row, resolve and return the child instance."""
    session: Session | None = object_session(lineage)
    if session is None:
        return None
    instance_uid = getattr(lineage, "child_instance_uid", None)
    if instance_uid is None:
        return None
    return (
        session.query(generic_instance)
        .filter(generic_instance.uid == instance_uid)
        .first()
    )


# ────────────────────────────────────────────────────────────────────
# Recursive graph traversal (session-based, not instance-attached)
# ────────────────────────────────────────────────────────────────────


@dataclass
class DescendantRow:
    """A single row from a recursive descendant walk."""

    euid: str
    uid: int
    json_addl: Optional[dict[str, Any]]


@dataclass
class GraphNode:
    """A node in a lineage graph traversal."""

    euid: str
    uid: int
    name: Optional[str]
    type: str
    category: str
    subtype: str
    version: str
    depth: int


@dataclass
class GraphEdge:
    """An edge in a lineage graph traversal."""

    lineage_euid: Optional[str]
    parent_euid: Optional[str]
    child_euid: Optional[str]
    relationship_type: Optional[str]


@dataclass
class LineageGraph:
    """Combined node + edge result from a graph traversal."""

    nodes: list[GraphNode]
    edges: list[GraphEdge]


_DESCENDANTS_SQL = """
WITH RECURSIVE descendants AS (
    SELECT gi.uid, gi.euid, gi.json_addl, gi.created_dt
    FROM generic_instance gi
    WHERE gi.euid = :root_euid

    UNION ALL

    SELECT child_gi.uid, child_gi.euid, child_gi.json_addl, child_gi.created_dt
    FROM generic_instance_lineage gil
    JOIN descendants d ON gil.parent_instance_uid = d.uid
    JOIN generic_instance child_gi ON gil.child_instance_uid = child_gi.uid
    WHERE NOT child_gi.is_deleted
)
SELECT d.uid, d.euid, d.json_addl
FROM descendants d
ORDER BY d.created_dt DESC
LIMIT :max_rows
"""


def get_all_descendants(
    session: Session,
    root_euid: str,
    *,
    max_rows: int = 10000,
) -> list[DescendantRow]:
    """Recursively walk child lineages and return all descendant instances.

    Args:
        session: Active SQLAlchemy session.
        root_euid: EUID of the root instance.
        max_rows: Safety limit on result size (default 10,000).

    Returns:
        List of DescendantRow (euid, uid, json_addl) ordered newest first.
    """
    rows = (
        session.execute(
            text(_DESCENDANTS_SQL),
            {"root_euid": str(root_euid), "max_rows": max_rows},
        )
        .mappings()
        .all()
    )
    return [
        DescendantRow(euid=r["euid"], uid=r["uid"], json_addl=r["json_addl"])
        for r in rows
    ]


_GRAPH_SQL = """
WITH RECURSIVE graph_data AS (
    SELECT
        gi.euid, gi.uid, gi.name, gi.type, gi.category, gi.subtype, gi.version,
        0 AS depth,
        NULL::text AS lineage_euid,
        NULL::text AS lineage_parent_euid,
        NULL::text AS lineage_child_euid,
        NULL::text AS relationship_type
    FROM generic_instance gi
    WHERE gi.euid = :start_euid AND gi.is_deleted = FALSE

    UNION

    SELECT
        gi.euid, gi.uid, gi.name, gi.type, gi.category, gi.subtype, gi.version,
        gd.depth + 1,
        gil.euid AS lineage_euid,
        parent_inst.euid AS lineage_parent_euid,
        child_inst.euid AS lineage_child_euid,
        gil.relationship_type
    FROM generic_instance_lineage gil
    JOIN generic_instance gi
        ON gi.uid = gil.child_instance_uid OR gi.uid = gil.parent_instance_uid
    JOIN generic_instance parent_inst ON gil.parent_instance_uid = parent_inst.uid
    JOIN generic_instance child_inst ON gil.child_instance_uid = child_inst.uid
    JOIN graph_data gd
        ON (gil.parent_instance_uid = gd.uid AND gi.uid = gil.child_instance_uid)
        OR (gil.child_instance_uid = gd.uid AND gi.uid = gil.parent_instance_uid)
    WHERE gi.is_deleted = FALSE AND gil.is_deleted = FALSE AND gd.depth < :max_depth
)
SELECT DISTINCT * FROM graph_data
"""


def get_lineage_graph(
    session: Session,
    start_euid: str,
    *,
    max_depth: int = 3,
) -> LineageGraph:
    """Traverse the lineage graph bidirectionally from a starting node.

    Args:
        session: Active SQLAlchemy session.
        start_euid: EUID of the starting instance.
        max_depth: Maximum hops from the start node (default 3).

    Returns:
        LineageGraph with deduplicated nodes and edges.
    """
    rows = (
        session.execute(
            text(_GRAPH_SQL),
            {"start_euid": str(start_euid), "max_depth": int(max_depth)},
        )
        .mappings()
        .all()
    )

    seen_euids: set[str] = set()
    nodes: list[GraphNode] = []
    edges: list[GraphEdge] = []

    for r in rows:
        euid = r["euid"]
        if euid not in seen_euids:
            seen_euids.add(euid)
            nodes.append(
                GraphNode(
                    euid=euid,
                    uid=r["uid"],
                    name=r["name"],
                    type=r["type"],
                    category=r["category"],
                    subtype=r["subtype"],
                    version=r["version"],
                    depth=r["depth"],
                )
            )
        if r["lineage_euid"] is not None:
            edges.append(
                GraphEdge(
                    lineage_euid=r["lineage_euid"],
                    parent_euid=r["lineage_parent_euid"],
                    child_euid=r["lineage_child_euid"],
                    relationship_type=r["relationship_type"],
                )
            )

    return LineageGraph(nodes=nodes, edges=edges)
