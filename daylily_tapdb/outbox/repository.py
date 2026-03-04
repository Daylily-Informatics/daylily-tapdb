"""Transactional outbox repository.

This module provides a minimal, Postgres-backed transactional outbox that is safe
to use inside the same SQLAlchemy session/transaction as domain writes.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import Select, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from daylily_tapdb.models.outbox import outbox_event

_OUTBOX_PENDING_STATUSES = ("pending", "failed", "delivering")


def _build_enqueue_stmt(
    *,
    event_id: uuid.UUID,
    tenant_id: uuid.UUID | None,
    event_type: str,
    aggregate_euid: str | None,
    payload: dict,
    destination: str,
    dedupe_key: str,
):
    return (
        pg_insert(outbox_event)
        .values(
            event_id=event_id,
            tenant_id=tenant_id,
            event_type=event_type,
            aggregate_euid=aggregate_euid,
            payload=payload,
            destination=destination,
            dedupe_key=dedupe_key,
        )
        .on_conflict_do_nothing(index_elements=["destination", "dedupe_key"])
        .returning(outbox_event.event_id)
    )


def enqueue_event(
    session: Session,
    tenant_id: uuid.UUID,
    event_type: str,
    aggregate_euid: str | None,
    payload: dict,
    destination: str,
    dedupe_key: str,
) -> uuid.UUID:
    """Insert an outbox row, using (destination, dedupe_key) for idempotency.

    Returns the row's `event_id`. If a row already exists for (destination, dedupe_key),
    this returns the existing row's event_id.
    """
    event_id = uuid.uuid4()
    inserted = session.execute(
        _build_enqueue_stmt(
            event_id=event_id,
            tenant_id=tenant_id,
            event_type=event_type,
            aggregate_euid=aggregate_euid,
            payload=payload,
            destination=destination,
            dedupe_key=dedupe_key,
        )
    ).scalar_one_or_none()
    if inserted is not None:
        return inserted

    existing = session.execute(
        select(outbox_event.event_id).where(
            outbox_event.destination == destination,
            outbox_event.dedupe_key == dedupe_key,
        )
    ).scalar_one()
    return existing


def _build_claim_select(*, batch_size: int) -> Select:
    # Note: `next_attempt_at` serves as both retry schedule and "lease timeout" for
    # in-flight deliveries (reclaimable once `next_attempt_at <= now()`).
    return (
        select(outbox_event)
        .where(
            outbox_event.status.in_(_OUTBOX_PENDING_STATUSES),
            outbox_event.next_attempt_at <= func.now(),
        )
        .order_by(outbox_event.created_dt.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )


def claim_events(
    session: Session,
    batch_size: int = 50,
    lock_timeout_s: int = 300,
) -> list[outbox_event]:
    """Claim a batch of eligible outbox rows for delivery.

    - Locks rows with FOR UPDATE SKIP LOCKED to allow concurrent workers
    - Marks them delivering and bumps attempt_count
    - Sets next_attempt_at to now() + lock_timeout to enable reclaim on crash

    Does not commit; callers should commit after claiming to release locks quickly.
    """
    rows = list(
        session.execute(_build_claim_select(batch_size=batch_size)).scalars().all()
    )
    if not rows:
        return []

    now = datetime.now(UTC)
    lease_expires = now + timedelta(seconds=int(lock_timeout_s))

    for row in rows:
        row.status = "delivering"
        row.attempt_count = int(row.attempt_count or 0) + 1
        row.next_attempt_at = lease_expires
        row.last_error = None

    session.flush()
    return rows


def mark_delivered(session: Session, row_id: int) -> None:
    """Mark an outbox row delivered."""
    session.execute(
        update(outbox_event)
        .where(outbox_event.id == row_id)
        .values(status="delivered", delivered_dt=func.now())
    )
    session.flush()


def mark_failed(
    session: Session,
    row_id: int,
    *,
    error: str,
    next_attempt_at: datetime,
) -> None:
    """Mark an outbox row failed and schedule a retry."""
    session.execute(
        update(outbox_event)
        .where(outbox_event.id == row_id)
        .values(status="failed", last_error=error, next_attempt_at=next_attempt_at)
    )
    session.flush()
