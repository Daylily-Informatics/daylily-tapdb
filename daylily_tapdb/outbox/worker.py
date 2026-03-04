"""Transactional outbox dispatcher loop."""

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
    """
    while True:
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
                    # delivery.
                    for ev in claimed:
                        session.expunge(ev)
        except Exception:
            logger.exception("Outbox claim failed")
            time.sleep(max(0.1, poll_interval_s))
            continue

        if not claimed:
            time.sleep(max(0.0, poll_interval_s))
            continue

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
