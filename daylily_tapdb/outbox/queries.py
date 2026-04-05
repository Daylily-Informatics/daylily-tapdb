"""Admin query helpers for outbox and inbox observability.

All functions accept an active SQLAlchemy session and return lightweight
result objects. No mutations are performed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from daylily_tapdb.models.outbox import inbox_message, outbox_event, outbox_event_attempt


@dataclass(frozen=True, slots=True)
class OutboxStatusSummary:
    """Aggregate counts by status for outbox_event."""

    pending: int = 0
    delivering: int = 0
    received: int = 0
    processed: int = 0
    failed: int = 0
    dead_letter: int = 0
    rejected: int = 0
    canceled: int = 0
    delivered: int = 0  # deprecated status


def outbox_status_summary(
    session: Session,
    *,
    domain_code: str | None = None,
    issuer_app_code: str | None = None,
) -> OutboxStatusSummary:
    """Return aggregate counts of outbox events by status."""
    q = select(
        outbox_event.status,
        func.count().label("cnt"),
    ).group_by(outbox_event.status)
    if domain_code is not None:
        q = q.where(outbox_event.domain_code == domain_code)
    if issuer_app_code is not None:
        q = q.where(outbox_event.issuer_app_code == issuer_app_code)

    rows = {r.status: r.cnt for r in session.execute(q).all()}
    return OutboxStatusSummary(**{k: rows.get(k, 0) for k in OutboxStatusSummary.__dataclass_fields__})


def list_failed_events(
    session: Session,
    *,
    domain_code: str | None = None,
    issuer_app_code: str | None = None,
    limit: int = 100,
) -> list[outbox_event]:
    """List outbox events in failed or dead_letter status."""
    q = (
        select(outbox_event)
        .where(outbox_event.status.in_(("failed", "dead_letter")))
        .order_by(outbox_event.last_attempt_dt.desc().nullslast())
        .limit(limit)
    )
    if domain_code is not None:
        q = q.where(outbox_event.domain_code == domain_code)
    if issuer_app_code is not None:
        q = q.where(outbox_event.issuer_app_code == issuer_app_code)
    return list(session.execute(q).scalars().all())


def list_stale_delivering(
    session: Session,
    *,
    domain_code: str | None = None,
    limit: int = 100,
) -> list[outbox_event]:
    """List events stuck in 'delivering' past their lease expiry."""
    q = (
        select(outbox_event)
        .where(
            outbox_event.status == "delivering",
            outbox_event.lease_expires_dt < func.now(),
        )
        .order_by(outbox_event.claimed_dt.asc().nullslast())
        .limit(limit)
    )
    if domain_code is not None:
        q = q.where(outbox_event.domain_code == domain_code)
    return list(session.execute(q).scalars().all())


def get_event_attempts(
    session: Session,
    outbox_event_id: int,
) -> list[outbox_event_attempt]:
    """Return all delivery attempts for a given outbox event."""
    q = (
        select(outbox_event_attempt)
        .where(outbox_event_attempt.outbox_event_id == outbox_event_id)
        .order_by(outbox_event_attempt.attempt_no.asc())
    )
    return list(session.execute(q).scalars().all())


@dataclass(frozen=True, slots=True)
class InboxStatusSummary:
    """Aggregate counts by status for inbox_message."""

    received: int = 0
    processing: int = 0
    processed: int = 0
    failed: int = 0
    rejected: int = 0


def inbox_status_summary(
    session: Session,
    *,
    domain_code: str | None = None,
    issuer_app_code: str | None = None,
) -> InboxStatusSummary:
    """Return aggregate counts of inbox messages by status."""
    q = select(
        inbox_message.status,
        func.count().label("cnt"),
    ).group_by(inbox_message.status)
    if domain_code is not None:
        q = q.where(inbox_message.domain_code == domain_code)
    if issuer_app_code is not None:
        q = q.where(inbox_message.issuer_app_code == issuer_app_code)

    rows = {r.status: r.cnt for r in session.execute(q).all()}
    return InboxStatusSummary(**{k: rows.get(k, 0) for k in InboxStatusSummary.__dataclass_fields__})
