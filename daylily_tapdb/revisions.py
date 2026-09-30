"""Shared row locks and compare-and-set preconditions for native writers."""

from __future__ import annotations
from typing import Any

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage


class RevisionConflict(ValueError):
    """The caller must reread and explicitly reconcile its stale proposal."""


def lock_records(session: Any, objects: list[Any]) -> None:
    records = {(obj.__table__.name, obj.uid): obj for obj in objects}
    for obj in list(records.values()):
        if isinstance(obj, generic_instance_lineage):
            for uid in (obj.parent_instance_uid, obj.child_instance_uid):
                endpoint = session.query(generic_instance).filter_by(uid=uid).one()
                records[("generic_instance", uid)] = endpoint
    for _, obj in sorted(records.items()):
        session.refresh(obj, with_for_update=True)


def require_revision(obj: Any, expected: int | None) -> None:
    if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
        raise ValueError("an explicit nonnegative expected_revision is required")
    if obj.record_revision != expected:
        raise RevisionConflict(
            f"stale revision: expected {expected}, current {obj.record_revision}"
        )


def apply_guarded_fields(
    session: Any, obj: Any, changes: dict[str, Any], *, expected_revision: int
) -> None:
    """Common persisted mutation path after the caller's domain validation.

    Correction and ordinary object operations share this final CAS/write path.
    Native database scope, template, lineage and audit constraints remain active.
    """
    require_revision(obj, expected_revision)
    if set(changes) - {"name", "bstatus", "json_addl", "is_deleted"}:
        raise ValueError("guarded field mutation cannot change identity or endpoints")
    for field, value in changes.items():
        setattr(obj, field, value)
    session.flush()
    session.refresh(obj)
