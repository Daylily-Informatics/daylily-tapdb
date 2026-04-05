"""Transactional outbox ORM models.

outbox_event:  execution/dispatch index (current state).
outbox_event_attempt: append-only delivery attempt history.
inbox_message: receiver-side durable receipt substrate.
"""

from sqlalchemy import BIGINT, Column, DateTime, FetchedValue, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from daylily_tapdb.models.base import Base


class outbox_event(Base):
    """Execution/dispatch index for transactional outbox messages.

    The canonical event payload, event_type, aggregate_euid, and machine_uuid
    live on the related generic_instance (via message_uid).
    This table holds only operational delivery state.
    """

    __tablename__ = "outbox_event"

    id = Column(BIGINT, primary_key=True, nullable=False, server_default=FetchedValue())

    # FK to the canonical message object in generic_instance
    message_uid = Column(
        BIGINT, ForeignKey("generic_instance.uid"), nullable=False
    )

    # Scoping — denormalized from the message for direct filtering
    tenant_id = Column(UUID(as_uuid=True), nullable=True)
    domain_code = Column(Text, nullable=False, server_default=FetchedValue())
    issuer_app_code = Column(Text, nullable=False, server_default=FetchedValue())

    destination = Column(Text, nullable=False)
    dedupe_key = Column(Text, nullable=False)

    status = Column(Text, nullable=False, server_default=FetchedValue())
    attempt_count = Column(Integer, nullable=False, server_default=FetchedValue())
    next_attempt_at = Column(
        DateTime(timezone=True), nullable=False, server_default=FetchedValue()
    )
    last_error = Column(Text, nullable=True)

    created_dt = Column(
        DateTime(timezone=True), nullable=False, server_default=FetchedValue()
    )
    delivered_dt = Column(DateTime(timezone=True), nullable=True)  # deprecated

    # Claim / lease management
    claimed_by = Column(Text, nullable=True)
    claim_token = Column(UUID(as_uuid=True), nullable=True)
    claimed_dt = Column(DateTime(timezone=True), nullable=True)
    lease_expires_dt = Column(DateTime(timezone=True), nullable=True)

    # Last attempt details (materialized for quick admin queries)
    last_attempt_dt = Column(DateTime(timezone=True), nullable=True)
    last_http_status = Column(Integer, nullable=True)
    last_response_headers = Column(JSONB, nullable=True)
    last_response_body_excerpt = Column(Text, nullable=True)

    # Receipt tracking
    receipt_machine_uuid = Column(UUID(as_uuid=True), nullable=True)
    receipt_status = Column(Text, nullable=True)
    receipt_received_dt = Column(DateTime(timezone=True), nullable=True)
    receipt_processed_dt = Column(DateTime(timezone=True), nullable=True)

    # Terminal state timestamps
    dead_letter_dt = Column(DateTime(timezone=True), nullable=True)
    rejected_dt = Column(DateTime(timezone=True), nullable=True)
    canceled_dt = Column(DateTime(timezone=True), nullable=True)

    # Relationship to canonical message object
    message = relationship(
        "generic_instance",
        primaryjoin="outbox_event.message_uid == generic_instance.uid",
        foreign_keys=[message_uid],
        lazy="joined",
    )

    # Attempt history
    attempts = relationship(
        "outbox_event_attempt",
        back_populates="event",
        order_by="outbox_event_attempt.attempt_no",
        lazy="dynamic",
    )

    def __repr__(self) -> str:
        return (
            "<outbox_event(id="
            f"{self.id!r}, destination={self.destination!r}, status={self.status!r})>"
        )


class outbox_event_attempt(Base):
    """Append-only delivery attempt history for outbox events."""

    __tablename__ = "outbox_event_attempt"

    uid = Column(BIGINT, primary_key=True, nullable=False, server_default=FetchedValue())
    outbox_event_id = Column(
        BIGINT, ForeignKey("outbox_event.id"), nullable=False
    )
    tenant_id = Column(UUID(as_uuid=True), nullable=True)
    domain_code = Column(Text, nullable=False, server_default=FetchedValue())
    issuer_app_code = Column(Text, nullable=False, server_default=FetchedValue())
    attempt_no = Column(Integer, nullable=False)
    worker_id = Column(Text, nullable=True)
    claim_token = Column(UUID(as_uuid=True), nullable=True)
    attempt_started_dt = Column(
        DateTime(timezone=True), nullable=False, server_default=FetchedValue()
    )
    attempt_finished_dt = Column(DateTime(timezone=True), nullable=True)
    transport_status = Column(Text, nullable=False, server_default=FetchedValue())
    http_status = Column(Integer, nullable=True)
    transport_error = Column(Text, nullable=True)
    response_headers = Column(JSONB, nullable=True)
    response_body_excerpt = Column(Text, nullable=True)
    receipt_machine_uuid = Column(UUID(as_uuid=True), nullable=True)
    receipt_status = Column(Text, nullable=True)
    receipt_received_dt = Column(DateTime(timezone=True), nullable=True)
    receipt_processed_dt = Column(DateTime(timezone=True), nullable=True)
    retry_scheduled_dt = Column(DateTime(timezone=True), nullable=True)
    json_addl = Column(JSONB, nullable=True)

    event = relationship("outbox_event", back_populates="attempts")

    def __repr__(self) -> str:
        return (
            f"<outbox_event_attempt(uid={self.uid!r}, event_id={self.outbox_event_id!r}, "
            f"attempt_no={self.attempt_no!r}, status={self.transport_status!r})>"
        )


class inbox_message(Base):
    """Receiver-side durable receipt substrate for inbound messages."""

    __tablename__ = "inbox_message"

    uid = Column(BIGINT, primary_key=True, nullable=False, server_default=FetchedValue())
    message_machine_uuid = Column(UUID(as_uuid=True), unique=True, nullable=False)
    receipt_machine_uuid = Column(UUID(as_uuid=True), unique=True, nullable=False)
    tenant_id = Column(UUID(as_uuid=True), nullable=True)
    domain_code = Column(Text, nullable=False, server_default=FetchedValue())
    issuer_app_code = Column(Text, nullable=False, server_default=FetchedValue())
    source_domain_code = Column(Text, nullable=False, server_default=FetchedValue())
    source_issuer_app_code = Column(Text, nullable=False, server_default=FetchedValue())
    source_destination = Column(Text, nullable=True)
    status = Column(Text, nullable=False, server_default=FetchedValue())
    received_dt = Column(
        DateTime(timezone=True), nullable=False, server_default=FetchedValue()
    )
    processed_dt = Column(DateTime(timezone=True), nullable=True)
    payload = Column(JSONB, nullable=False, server_default=FetchedValue())
    error_code = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    json_addl = Column(JSONB, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<inbox_message(uid={self.uid!r}, "
            f"msg_uuid={self.message_machine_uuid!r}, status={self.status!r})>"
        )
