"""Transactional outbox helpers.

Architecture: canonical message objects live in ``generic_instance``;
``outbox_event`` is a thin execution/dispatch index.
"""

from daylily_tapdb.outbox.contracts import DeliveryResult, InboundReceipt
from daylily_tapdb.outbox.repository import (
    claim_events,
    enqueue_event,
    enqueue_fanout,
    mark_dead_letter,
    mark_delivered,
    mark_failed,
    mark_processed,
    mark_received,
    mark_rejected,
    record_attempt,
)
from daylily_tapdb.outbox.worker import dispatch_batch, run_dispatch_loop

__all__ = [
    "DeliveryResult",
    "InboundReceipt",
    "enqueue_event",
    "enqueue_fanout",
    "claim_events",
    "mark_delivered",
    "mark_received",
    "mark_processed",
    "mark_rejected",
    "mark_dead_letter",
    "mark_failed",
    "record_attempt",
    "dispatch_batch",
    "run_dispatch_loop",
]
