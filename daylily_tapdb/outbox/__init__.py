"""Transactional outbox helpers.

Architecture: canonical message objects live in ``generic_instance``;
``outbox_event`` is a thin execution/dispatch index.
"""

from daylily_tapdb.outbox.repository import (
    claim_events,
    enqueue_event,
    enqueue_fanout,
    mark_delivered,
    mark_failed,
)
from daylily_tapdb.outbox.worker import dispatch_batch, run_dispatch_loop

__all__ = [
    "enqueue_event",
    "enqueue_fanout",
    "claim_events",
    "mark_delivered",
    "mark_failed",
    "dispatch_batch",
    "run_dispatch_loop",
]
