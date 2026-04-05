"""Transactional outbox dispatcher loop.

The worker claims ``outbox_event`` rows (execution index) and reads the
canonical message payload from the eagerly-loaded ``message`` relationship
(a ``generic_instance`` row).

``deliver_fn`` receives an ``outbox_event`` whose ``.message`` attribute
contains the full ``generic_instance`` with:
- ``message.machine_uuid``: the external idempotency UUID (UUIDv7)
- ``message.json_addl["event_type"]``: semantic event type
- ``message.json_addl["aggregate_euid"]``: related domain entity EUID
- ``message.json_addl["payload"]``: canonical event payload
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Callable

from sqlalchemy.orm import Session

from daylily_tapdb.models.outbox import outbox_event
from daylily_tapdb.outbox.repository import claim_events, mark_delivered, mark_failed

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
        try:
            deliver_fn(ev)
        except Exception as e:
            logger.exception(
                "Outbox delivery failed (id=%s, destination=%s, attempt=%s)",
                ev.id,
                ev.destination,
                ev.attempt_count,
            )
            attempts = int(ev.attempt_count or 0)
            if attempts >= int(max_attempts):
                next_attempt_at = datetime.now(UTC) + timedelta(days=365)
            else:
                next_attempt_at = datetime.now(UTC) + timedelta(
                    seconds=_retry_delay_s(attempts)
                )

            # Persist failure + schedule retry.
            with session_factory() as session:
                with session.begin():
                    mark_failed(
                        session,
                        int(ev.id),
                        error=str(e)[:10_000],
                        next_attempt_at=next_attempt_at,
                    )
            continue

        # Persist success.
        with session_factory() as session:
            with session.begin():
                mark_delivered(session, int(ev.id))

    return len(claimed)
