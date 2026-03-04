"""Transactional outbox ORM model."""

from sqlalchemy import BIGINT, Column, DateTime, FetchedValue, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from daylily_tapdb.models.base import Base


class outbox_event(Base):
    """ORM mapping for the `outbox_event` table."""

    __tablename__ = "outbox_event"

    id = Column(BIGINT, primary_key=True, nullable=False, server_default=FetchedValue())

    event_id = Column(UUID(as_uuid=True), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), nullable=True)

    event_type = Column(Text, nullable=False)
    aggregate_euid = Column(Text, nullable=True)

    payload = Column(JSONB, nullable=False)

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

    def __repr__(self) -> str:
        return (
            "<outbox_event(id="
            f"{self.id!r}, destination={self.destination!r}, status={self.status!r})>"
        )

