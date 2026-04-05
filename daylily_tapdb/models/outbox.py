"""Transactional outbox ORM model.

The outbox_event table is an execution/dispatch index only.
The canonical message object lives in generic_instance.
"""

from sqlalchemy import BIGINT, Column, DateTime, FetchedValue, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import UUID
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
    delivered_dt = Column(DateTime(timezone=True), nullable=True)

    # Relationship to canonical message object
    message = relationship(
        "generic_instance",
        primaryjoin="outbox_event.message_uid == generic_instance.uid",
        foreign_keys=[message_uid],
        lazy="joined",
    )

    def __repr__(self) -> str:
        return (
            "<outbox_event(id="
            f"{self.id!r}, destination={self.destination!r}, status={self.status!r})>"
        )
