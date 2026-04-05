"""Inbox receiver — receiver-side durable receipt substrate.

Provides idempotent message receipt and processing state transitions.
Each inbound message is identified by its ``message_machine_uuid`` (UUIDv7).
The receiver issues a ``receipt_machine_uuid`` on first receipt.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from daylily_tapdb.models.outbox import inbox_message
from daylily_tapdb.outbox.contracts import InboundReceipt


def receive_message(
    session: Session,
    *,
    message_machine_uuid: uuid.UUID,
    payload: dict,
    tenant_id: uuid.UUID | None = None,
    domain_code: str = "",
    issuer_app_code: str = "",
    source_domain_code: str = "",
    source_issuer_app_code: str = "",
    source_destination: str | None = None,
) -> InboundReceipt:
    """Idempotently receive an inbound message.

    If the message has already been received (same ``message_machine_uuid``),
    returns the existing receipt without creating a duplicate.

    Returns:
        InboundReceipt with the receipt_machine_uuid and status.
    """
    receipt_uuid = uuid.uuid4()
    now = datetime.utcnow()

    stmt = (
        pg_insert(inbox_message)
        .values(
            message_machine_uuid=message_machine_uuid,
            receipt_machine_uuid=receipt_uuid,
            tenant_id=tenant_id,
            domain_code=domain_code,
            issuer_app_code=issuer_app_code,
            source_domain_code=source_domain_code,
            source_issuer_app_code=source_issuer_app_code,
            source_destination=source_destination,
            payload=payload,
            status="received",
        )
        .on_conflict_do_nothing(index_elements=["message_machine_uuid"])
        .returning(
            inbox_message.receipt_machine_uuid,
            inbox_message.status,
            inbox_message.received_dt,
        )
    )

    result = session.execute(stmt).first()

    if result is not None:
        # New row was inserted
        session.flush()
        return InboundReceipt(
            message_machine_uuid=message_machine_uuid,
            receipt_machine_uuid=result.receipt_machine_uuid,
            status=result.status,
            received_dt=result.received_dt,
        )

    # Row already existed — fetch the existing receipt
    existing = (
        session.query(inbox_message)
        .filter(inbox_message.message_machine_uuid == message_machine_uuid)
        .one()
    )
    return InboundReceipt(
        message_machine_uuid=message_machine_uuid,
        receipt_machine_uuid=existing.receipt_machine_uuid,
        status=existing.status,
        received_dt=existing.received_dt,
        processed_dt=existing.processed_dt,
    )


def mark_inbox_processing(session: Session, message_machine_uuid: uuid.UUID) -> None:
    """Transition an inbox message to 'processing' status."""
    session.execute(
        update(inbox_message)
        .where(
            inbox_message.message_machine_uuid == message_machine_uuid,
            inbox_message.status == "received",
        )
        .values(status="processing")
    )
    session.flush()


def mark_inbox_processed(session: Session, message_machine_uuid: uuid.UUID) -> None:
    """Transition an inbox message to 'processed' status."""
    session.execute(
        update(inbox_message)
        .where(
            inbox_message.message_machine_uuid == message_machine_uuid,
            inbox_message.status.in_(("received", "processing")),
        )
        .values(status="processed", processed_dt=func.now())
    )
    session.flush()


def mark_inbox_failed(
    session: Session,
    message_machine_uuid: uuid.UUID,
    *,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    """Transition an inbox message to 'failed' status."""
    session.execute(
        update(inbox_message)
        .where(
            inbox_message.message_machine_uuid == message_machine_uuid,
            inbox_message.status.in_(("received", "processing")),
        )
        .values(
            status="failed",
            error_code=error_code,
            error_message=error_message[:10_000] if error_message else None,
        )
    )
    session.flush()


def mark_inbox_rejected(
    session: Session,
    message_machine_uuid: uuid.UUID,
    *,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    """Transition an inbox message to 'rejected' status."""
    session.execute(
        update(inbox_message)
        .where(inbox_message.message_machine_uuid == message_machine_uuid)
        .values(
            status="rejected",
            error_code=error_code,
            error_message=error_message[:10_000] if error_message else None,
        )
    )
    session.flush()



def get_inbox_message_by_machine_uuid(
    session: Session,
    machine_uuid: uuid.UUID,
) -> inbox_message | None:
    """Look up an inbox message by its message_machine_uuid."""
    return (
        session.query(inbox_message)
        .filter(inbox_message.message_machine_uuid == machine_uuid)
        .first()
    )