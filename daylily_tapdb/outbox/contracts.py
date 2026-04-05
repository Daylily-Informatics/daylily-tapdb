"""Typed delivery result and receipt contracts for the transactional outbox.

These dataclasses replace the previous "no-exception = success" pattern
with explicit, structured delivery semantics.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """Structured result returned by ``deliver_fn``.

    The worker uses this to decide how to update ``outbox_event`` state
    and what to record in ``outbox_event_attempt``.

    Attributes:
        success: True if the receiver returned a valid receipt.
        transport_status: One of ``attempted``, ``transport_failed``,
            ``receipt_valid``, ``receipt_invalid``, ``processed``, ``rejected``.
        receipt_machine_uuid: The receiver-issued receipt UUID (if any).
        receipt_status: Status from the receipt (``received``, ``processed``,
            ``rejected``, etc.).
        receipt_received_dt: When the receiver persisted the message.
        receipt_processed_dt: When the receiver completed business processing.
        http_status: HTTP status code (if applicable).
        response_headers: Response headers (if applicable).
        response_body_excerpt: First N chars of response body (for debugging).
        error_code: Machine-readable error code.
        error_message: Human-readable error description.
        retryable: Whether this failure should be retried.
        extra: Arbitrary additional data from the adapter.
    """

    success: bool
    transport_status: str = "attempted"
    receipt_machine_uuid: uuid.UUID | None = None
    receipt_status: str | None = None
    receipt_received_dt: datetime | None = None
    receipt_processed_dt: datetime | None = None
    http_status: int | None = None
    response_headers: dict[str, str] | None = None
    response_body_excerpt: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool = True
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def received(
        cls,
        receipt_machine_uuid: uuid.UUID,
        *,
        receipt_received_dt: datetime | None = None,
        http_status: int | None = None,
    ) -> DeliveryResult:
        """Factory for a successful 'received' result."""
        return cls(
            success=True,
            transport_status="receipt_valid",
            receipt_machine_uuid=receipt_machine_uuid,
            receipt_status="received",
            receipt_received_dt=receipt_received_dt,
            http_status=http_status,
        )

    @classmethod
    def processed(
        cls,
        receipt_machine_uuid: uuid.UUID,
        *,
        receipt_received_dt: datetime | None = None,
        receipt_processed_dt: datetime | None = None,
        http_status: int | None = None,
    ) -> DeliveryResult:
        """Factory for a synchronous 'processed' result."""
        return cls(
            success=True,
            transport_status="processed",
            receipt_machine_uuid=receipt_machine_uuid,
            receipt_status="processed",
            receipt_received_dt=receipt_received_dt,
            receipt_processed_dt=receipt_processed_dt,
            http_status=http_status,
        )

    @classmethod
    def transport_failed(
        cls,
        error_message: str,
        *,
        http_status: int | None = None,
        retryable: bool = True,
    ) -> DeliveryResult:
        """Factory for a transport-level failure."""
        return cls(
            success=False,
            transport_status="transport_failed",
            error_message=error_message,
            http_status=http_status,
            retryable=retryable,
        )

    @classmethod
    def rejected(
        cls,
        error_message: str,
        *,
        error_code: str | None = None,
        http_status: int | None = None,
    ) -> DeliveryResult:
        """Factory for an explicit receiver rejection."""
        return cls(
            success=False,
            transport_status="rejected",
            receipt_status="rejected",
            error_code=error_code,
            error_message=error_message,
            http_status=http_status,
            retryable=False,
        )


@dataclass(frozen=True, slots=True)
class InboundReceipt:
    """Structured receipt returned by ``receive_message``.

    Issued by the receiver and sent back to the sender.
    """

    message_machine_uuid: uuid.UUID
    receipt_machine_uuid: uuid.UUID
    status: str  # received | processing | processed | failed | rejected
    received_dt: datetime
    processed_dt: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None
