"""Transactional outbox repository — message unification architecture.

Canonical message objects live in ``generic_instance`` (template-backed,
with ``machine_uuid`` for external idempotency).  ``outbox_event`` is a thin
execution/dispatch index that references the message via ``message_uid``.

One canonical message can fan out to many ``outbox_event`` rows (one per
destination / subscription).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import uuid6
from sqlalchemy import Select, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.outbox import outbox_event

_OUTBOX_PENDING_STATUSES = ("pending", "failed", "delivering")

# Template code for canonical webhook event messages
MESSAGE_TEMPLATE_CODE = "system/message/webhook_event/1.0/"


def _build_enqueue_stmt(
    *,
    message_uid: int,
    destination: str,
    dedupe_key: str,
    tenant_id: uuid.UUID | None = None,
    domain_code: str = "",
    issuer_app_code: str = "",
):
    """Build an INSERT ... ON CONFLICT DO NOTHING for the execution index."""
    values = dict(
        message_uid=message_uid,
        destination=destination,
        dedupe_key=dedupe_key,
        domain_code=domain_code,
        issuer_app_code=issuer_app_code,
    )
    if tenant_id is not None:
        values["tenant_id"] = tenant_id
    return (
        pg_insert(outbox_event)
        .values(**values)
        .on_conflict_do_nothing(index_elements=["destination", "dedupe_key"])
        .returning(outbox_event.id)
    )


def _create_message_instance(
    session: Session,
    *,
    tenant_id: uuid.UUID | None,
    event_type: str,
    aggregate_euid: str | None,
    payload: dict,
    metadata: dict | None = None,
) -> generic_instance:
    """Create a canonical message generic_instance with a UUIDv7 machine_uuid.

    Uses the ``system/message/webhook_event/1.0`` template.  The message
    payload, event_type, and aggregate_euid are stored in ``json_addl``.
    """
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    template = tm.get_template(session, MESSAGE_TEMPLATE_CODE)
    if template is None:
        raise ValueError(
            f"Message template not found: {MESSAGE_TEMPLATE_CODE}. "
            "Has the template been seeded?"
        )

    machine_id = uuid6.uuid7()

    msg = generic_instance(
        name=f"msg:{event_type}:{aggregate_euid or 'none'}",
        tenant_id=tenant_id,
        machine_uuid=machine_id,
        polymorphic_discriminator=(
            template.instance_polymorphic_identity
            or template.polymorphic_discriminator.replace("_template", "_instance")
        ),
        category=template.category,
        type=template.type,
        subtype=template.subtype,
        version=template.version,
        template_uid=template.uid,
        json_addl={
            "event_type": event_type,
            "aggregate_euid": aggregate_euid,
            "payload": payload,
            "metadata": metadata or {},
        },
        bstatus="active",
        is_singleton=False,
    )
    session.add(msg)
    session.flush()  # assigns uid + euid via DB trigger
    return msg


def _lookup_existing_machine_uuid(
    session: Session,
    destination: str,
    dedupe_key: str,
) -> uuid.UUID | None:
    """Return the machine_uuid of an existing outbox row, or None."""
    row = session.execute(
        select(outbox_event.message_uid).where(
            outbox_event.destination == destination,
            outbox_event.dedupe_key == dedupe_key,
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    return session.execute(
        select(generic_instance.machine_uuid).where(
            generic_instance.uid == row,
        )
    ).scalar_one()


def enqueue_event(
    session: Session,
    tenant_id: uuid.UUID,
    event_type: str,
    aggregate_euid: str | None,
    payload: dict,
    destination: str,
    dedupe_key: str,
    *,
    domain_code: str = "",
    issuer_app_code: str = "",
) -> uuid.UUID:
    """Create a canonical message and enqueue a delivery row.

    1. Check if an ``outbox_event`` already exists for
       ``(destination, dedupe_key)``.  If so, return its ``machine_uuid``
       immediately — no message is created.
    2. Otherwise open a **savepoint**, create the ``generic_instance``
       message, and insert the ``outbox_event`` row inside that savepoint.
    3. If the insert hits ``ON CONFLICT`` due to a race, **roll back the
       savepoint** (which removes the orphan ``generic_instance``) and
       re-read the existing row.
    4. Returns the ``machine_uuid`` (the external idempotency key).
    """
    # ── fast path: row already exists ──
    existing = _lookup_existing_machine_uuid(session, destination, dedupe_key)
    if existing is not None:
        return existing

    # ── slow path: create inside a savepoint ──
    nested = session.begin_nested()
    try:
        msg = _create_message_instance(
            session,
            tenant_id=tenant_id,
            event_type=event_type,
            aggregate_euid=aggregate_euid,
            payload=payload,
        )
        inserted_id = session.execute(
            _build_enqueue_stmt(
                message_uid=msg.uid,
                destination=destination,
                dedupe_key=dedupe_key,
                tenant_id=tenant_id,
                domain_code=domain_code,
                issuer_app_code=issuer_app_code,
            )
        ).scalar_one_or_none()

        if inserted_id is not None:
            nested.commit()
            return msg.machine_uuid

        # ON CONFLICT hit — race condition.  Roll back the savepoint so
        # the just-created generic_instance is discarded cleanly.
        nested.rollback()
    except Exception:
        nested.rollback()
        raise

    # Re-read the winner's machine_uuid
    winner = _lookup_existing_machine_uuid(session, destination, dedupe_key)
    if winner is None:
        raise RuntimeError(
            f"outbox_event({destination!r}, {dedupe_key!r}) disappeared "
            "between conflict and re-read"
        )
    return winner


def enqueue_fanout(
    session: Session,
    message_uid: int,
    destinations: list[tuple[str, str]],
) -> list[int]:
    """Fan out a single canonical message to multiple destinations.

    Args:
        session: Active SQLAlchemy session.
        message_uid: The uid of the canonical generic_instance message.
        destinations: List of (destination, dedupe_key) tuples.

    Returns:
        List of outbox_event IDs that were inserted (skips conflicts).
    """
    inserted_ids = []
    for destination, dedupe_key in destinations:
        row_id = session.execute(
            _build_enqueue_stmt(
                message_uid=message_uid,
                destination=destination,
                dedupe_key=dedupe_key,
            )
        ).scalar_one_or_none()
        if row_id is not None:
            inserted_ids.append(row_id)
    session.flush()
    return inserted_ids


def _build_claim_select(
    *,
    batch_size: int,
    domain_code: str | None = None,
    issuer_app_code: str | None = None,
) -> Select:
    """Build the claim SELECT with JOIN to generic_instance for payload."""
    q = select(outbox_event).where(
        outbox_event.status.in_(_OUTBOX_PENDING_STATUSES),
        outbox_event.next_attempt_at <= func.now(),
    )
    if domain_code is not None:
        q = q.where(outbox_event.domain_code == domain_code)
    if issuer_app_code is not None:
        q = q.where(outbox_event.issuer_app_code == issuer_app_code)
    return (
        q.order_by(outbox_event.created_dt.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )


def claim_events(
    session: Session,
    batch_size: int = 50,
    lock_timeout_s: int = 300,
    *,
    domain_code: str | None = None,
    issuer_app_code: str | None = None,
) -> list[outbox_event]:
    """Claim a batch of eligible outbox rows for delivery.

    - Locks rows with FOR UPDATE SKIP LOCKED to allow concurrent workers
    - Marks them delivering and bumps attempt_count
    - Sets next_attempt_at to now() + lock_timeout to enable reclaim on crash

    The returned outbox_event rows have their ``.message`` relationship
    eagerly loaded so the worker can read payload from ``message.json_addl``
    and ``message.machine_uuid`` without extra queries.

    Does not commit; callers should commit after claiming to release locks quickly.
    """
    rows = list(
        session.execute(
            _build_claim_select(
                batch_size=batch_size,
                domain_code=domain_code,
                issuer_app_code=issuer_app_code,
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return []

    now = datetime.now(UTC)
    lease_expires = now + timedelta(seconds=int(lock_timeout_s))
    token = uuid.uuid4()

    for row in rows:
        row.status = "delivering"
        row.attempt_count = int(row.attempt_count or 0) + 1
        row.next_attempt_at = lease_expires
        row.last_error = None
        row.claim_token = token
        row.claimed_dt = now
        row.lease_expires_dt = lease_expires

    session.flush()
    return rows


def mark_received(
    session: Session,
    row_id: int,
    *,
    receipt_machine_uuid: uuid.UUID | None = None,
    receipt_status: str = "received",
) -> None:
    """Mark an outbox row received (receiver durably persisted the message)."""
    values: dict = {
        "status": "received",
        "receipt_status": receipt_status,
        "receipt_received_dt": func.now(),
    }
    if receipt_machine_uuid is not None:
        values["receipt_machine_uuid"] = receipt_machine_uuid
    session.execute(
        update(outbox_event).where(outbox_event.id == row_id).values(**values)
    )
    session.flush()


def mark_processed(
    session: Session,
    row_id: int,
    *,
    receipt_machine_uuid: uuid.UUID | None = None,
) -> None:
    """Mark an outbox row processed (receiver completed business processing)."""
    values: dict = {
        "status": "processed",
        "receipt_status": "processed",
        "receipt_processed_dt": func.now(),
    }
    if receipt_machine_uuid is not None:
        values["receipt_machine_uuid"] = receipt_machine_uuid
    session.execute(
        update(outbox_event).where(outbox_event.id == row_id).values(**values)
    )
    session.flush()


def mark_rejected(session: Session, row_id: int, *, error: str = "") -> None:
    """Mark an outbox row rejected (receiver explicitly rejected)."""
    session.execute(
        update(outbox_event)
        .where(outbox_event.id == row_id)
        .values(
            status="rejected",
            receipt_status="rejected",
            rejected_dt=func.now(),
            last_error=error[:10_000] if error else None,
        )
    )
    session.flush()


def mark_dead_letter(session: Session, row_id: int, *, error: str = "") -> None:
    """Mark an outbox row as dead-lettered (retries exhausted)."""
    session.execute(
        update(outbox_event)
        .where(outbox_event.id == row_id)
        .values(
            status="dead_letter",
            dead_letter_dt=func.now(),
            last_error=error[:10_000] if error else None,
        )
    )
    session.flush()


def requeue_dead_letter(
    session: Session,
    row_id: int,
    *,
    reset_attempt_count: bool = False,
) -> None:
    """Requeue a dead-lettered event for re-delivery.

    Transitions ``dead_letter`` → ``pending``, resets next_attempt_at,
    and optionally resets the attempt counter.
    """
    values: dict = {
        "status": "pending",
        "next_attempt_at": func.now(),
        "last_error": None,
        "claim_token": None,
        "claimed_dt": None,
        "lease_expires_dt": None,
        "dead_letter_dt": None,
    }
    if reset_attempt_count:
        values["attempt_count"] = 0
    session.execute(
        update(outbox_event)
        .where(outbox_event.id == row_id, outbox_event.status == "dead_letter")
        .values(**values)
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
        .values(
            status="failed",
            last_error=error,
            next_attempt_at=next_attempt_at,
            last_attempt_dt=func.now(),
        )
    )
    session.flush()


def record_attempt(
    session: Session,
    *,
    outbox_event_id: int,
    attempt_no: int,
    transport_status: str,
    tenant_id: uuid.UUID | None = None,
    domain_code: str = "",
    issuer_app_code: str = "",
    worker_id: str | None = None,
    claim_token: uuid.UUID | None = None,
    http_status: int | None = None,
    transport_error: str | None = None,
    response_headers: dict | None = None,
    response_body_excerpt: str | None = None,
    receipt_machine_uuid: uuid.UUID | None = None,
    receipt_status: str | None = None,
    receipt_received_dt: datetime | None = None,
    receipt_processed_dt: datetime | None = None,
    retry_scheduled_dt: datetime | None = None,
) -> int:
    """Record a delivery attempt in the append-only history table."""
    from daylily_tapdb.models.outbox import outbox_event_attempt

    attempt = outbox_event_attempt(
        outbox_event_id=outbox_event_id,
        attempt_no=attempt_no,
        transport_status=transport_status,
        tenant_id=tenant_id,
        domain_code=domain_code,
        issuer_app_code=issuer_app_code,
        worker_id=worker_id,
        claim_token=claim_token,
        attempt_finished_dt=func.now(),
        http_status=http_status,
        transport_error=transport_error[:10_000] if transport_error else None,
        response_headers=response_headers,
        response_body_excerpt=response_body_excerpt[:10_000]
        if response_body_excerpt
        else None,
        receipt_machine_uuid=receipt_machine_uuid,
        receipt_status=receipt_status,
        receipt_received_dt=receipt_received_dt,
        receipt_processed_dt=receipt_processed_dt,
        retry_scheduled_dt=retry_scheduled_dt,
    )
    session.add(attempt)
    session.flush()
    return int(attempt.uid)
