"""Audit log ORM model.

The `audit_log` table is populated by Postgres triggers and should be treated as
**read-only** from the Python layer.

Phase 2 spec: audit log is trigger-based but ORM-available for querying/display.
"""

from sqlalchemy import BIGINT, Boolean, Column, DateTime, FetchedValue, Text
from sqlalchemy.dialects.postgresql import JSONB

from daylily_tapdb.models.base import Base


class audit_log(Base):
    """Read-only ORM mapping for the trigger-populated `audit_log` table."""

    __tablename__ = "audit_log"

    uuid = Column(BIGINT, primary_key=True, nullable=False, server_default=FetchedValue())
    euid = Column(Text, nullable=False, server_default=FetchedValue())
    euid_prefix = Column(Text, nullable=False, server_default=FetchedValue())
    euid_seq = Column(BIGINT, nullable=False, server_default=FetchedValue())

    rel_table_name = Column(Text, nullable=False)
    column_name = Column(Text, nullable=True)

    rel_table_uuid_fk = Column(BIGINT, nullable=False)
    rel_table_euid_fk = Column(Text, nullable=False)

    old_value = Column(Text, nullable=True)
    new_value = Column(Text, nullable=True)

    changed_by = Column(Text, nullable=True)
    changed_at = Column(
        DateTime(timezone=True), nullable=False, server_default=FetchedValue()
    )

    operation_type = Column(Text, nullable=True)

    json_addl = Column(JSONB, nullable=True)
    category = Column(Text, nullable=True)
    deleted_record_json = Column(JSONB, nullable=True)

    is_deleted = Column(Boolean, nullable=False, server_default=FetchedValue())
    is_singleton = Column(Boolean, nullable=False, server_default=FetchedValue())

    def __repr__(self) -> str:
        return (
            "<audit_log(rel_table_name="
            f"{self.rel_table_name!r}, rel_table_euid_fk={self.rel_table_euid_fk!r}, "
            f"operation_type={self.operation_type!r})>"
        )
