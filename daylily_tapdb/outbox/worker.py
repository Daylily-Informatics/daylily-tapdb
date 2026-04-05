"""Transactional outbox dispatcher loop.

The worker claims ``outbox_event`` rows (execution index) and reads the
canonical message payload from the eagerly-loaded ``message`` relationship
(a ``generic_instance`` row).

``deliver_fn`` receives an ``outbox_event`` and returns a ``DeliveryResult``.
The worker uses the result to transition outbox state and record an attempt.
Legacy ``deliver_fn`` callables that return ``None`` (success) or raise
(failure) are still supported via automatic wrapping.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Callable

from sqlalchemy.orm import Session

from daylily_tapdb.models.outbox import outbox_event
from daylily_tapdb.outbox.contracts import DeliveryResult
from daylily_tapdb.outbox.repository import (
    claim_events,
    mark_dead_letter,
    mark_failed,
    mark_processed,
    mark_received,
    mark_rejected,
    record_attempt,
)

logger = logging.getLogger(__name__)


def _retry_delay_s(attempt_count: int) -> float:
    # Exponential backoff capped at 30 minutes.
    attempt_count = max(0, int(attempt_count))
    # Clamp exponent to avoid float overflow and unreasonable sleeps.
    exp = min(attempt_count, 31)
    return min(30 * 60.0, float(2**exp))


def run_dispatch_loop(
    session_factory: Callable[[], Session],
    deliver_fn: Callable[[outbox_event], None],
    *,
    batch_size: int = 50,
    poll_interval_s: float = 1.0,
    lock_timeout_s: int = 300,
    max_attempts: int = 10,
    domain_code: str | None = None,
    issuer_app_code: str | None = None,
) -> None:
    """Continuously dispatch outbox events.

    - Claims events with FOR UPDATE SKIP LOCKED
    - Commits immediately after claim (releases locks quickly)
    - Delivers outside the claim transaction
    - Marks delivered/failed in separate transactions

    The ``deliver_fn`` callback receives an ``outbox_event`` with its
    ``.message`` relationship populated.  Read the payload from
    ``ev.message.json_addl`` and the external UUID from
    ``ev.message.machine_uuid``.
    """
    while True:
        dispatched = dispatch_batch(
            session_factory,
            deliver_fn,
            batch_size=batch_size,
            lock_timeout_s=lock_timeout_s,
            max_attempts=max_attempts,
            domain_code=domain_code,
            issuer_app_code=issuer_app_code,
        )
        if dispatched == 0:
            time.sleep(max(0.0, poll_interval_s))


def dispatch_batch(
    session_factory: Callable[[], Session],
    deliver_fn: Callable[[outbox_event], None],
    *,
    batch_size: int = 50,
    lock_timeout_s: int = 300,
    max_attempts: int = 10,
    domain_code: str | None = None,
    issuer_app_code: str | None = None,
) -> int:
    """Claim and deliver one batch of outbox events.

    Returns the number of events claimed (0 means nothing to do).
    Suitable for use in a scheduler that calls this periodically
    rather than running an infinite loop.
    """
    claimed: list[outbox_event] = []

    try:
        with session_factory() as session:
            with session.begin():
                claimed = claim_events(
                    session,
                    batch_size=batch_size,
                    lock_timeout_s=lock_timeout_s,
                    domain_code=domain_code,
                    issuer_app_code=issuer_app_code,
                )
                # Prevent expire-on-commit from forcing lazy DB reads during
                # delivery.  Expunge both the outbox row and the joined message.
                for ev in claimed:
                    if ev.message is not None:
                        session.expunge(ev.message)
                    session.expunge(ev)
    except Exception:
        logger.exception("Outbox claim failed")
        return 0

    if not claimed:
        return 0

    for ev in claimed:
        result = _invoke_deliver_fn(deliver_fn, ev)
        attempts = int(ev.attempt_count or 0)

        with session_factory() as session:
            with session.begin():
                # Record the attempt
                record_attempt(
                    session,
                    outbox_event_id=int(ev.id),
                    attempt_no=attempts,
                    transport_status=result.transport_status,
                    tenant_id=getattr(ev, "tenant_id", None),
                    domain_code=getattr(ev, "domain_code", "") or "",
                    issuer_app_code=getattr(ev, "issuer_app_code", "") or "",
                    http_status=result.http_status,
                    transport_error=result.error_message,
                    response_headers=result.response_headers,
                    response_body_excerpt=result.response_body_excerpt,
                    receipt_machine_uuid=result.receipt_machine_uuid,
                    receipt_status=result.receipt_status,
                    receipt_received_dt=result.receipt_received_dt,
                    receipt_processed_dt=result.receipt_processed_dt,
                )

                # Transition outbox_event state
                if result.success:
                    if result.receipt_status == "processed":
                        mark_processed(
                            session,
                            int(ev.id),
                            receipt_machine_uuid=result.receipt_machine_uuid,
                        )
                    else:
                        mark_received(
                            session,
                            int(ev.id),
                            receipt_machine_uuid=result.receipt_machine_uuid,
                            receipt_status=result.receipt_status or "received",
                        )
                elif result.transport_status == "rejected":
                    mark_rejected(
                        session,
                        int(ev.id),
                        error=result.error_message or "",
                    )
                elif attempts >= int(max_attempts) and not result.retryable:
                    mark_dead_letter(
                        session,
                        int(ev.id),
                        error=result.error_message or "max attempts exceeded",
                    )
                elif attempts >= int(max_attempts):
                    mark_dead_letter(
                        session,
                        int(ev.id),
                        error=result.error_message or "max attempts exceeded",
                    )
                else:
                    next_attempt_at = datetime.now(UTC) + timedelta(
                        seconds=_retry_delay_s(attempts)
                    )
                    mark_failed(
                        session,
                        int(ev.id),
                        error=result.error_message or "delivery failed",
                        next_attempt_at=next_attempt_at,
                    )

    return len(claimed)


def _invoke_deliver_fn(deliver_fn: Callable, ev: outbox_event) -> DeliveryResult:
    """Call deliver_fn. Must return a DeliveryResult."""
    try:
        result = deliver_fn(ev)
    except Exception as e:
        logger.exception(
            "Outbox delivery failed (id=%s, destination=%s, attempt=%s)",
            ev.id,
            ev.destination,
            ev.attempt_count,
        )
        return DeliveryResult.transport_failed(str(e)[:10_000])

    if not isinstance(result, DeliveryResult):
        raise TypeError(
            f"deliver_fn must return DeliveryResult, got {type(result).__name__}. "
            f"Legacy None-return is no longer supported."
        )

    return result
