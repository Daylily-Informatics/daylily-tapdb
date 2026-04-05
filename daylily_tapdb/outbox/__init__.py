"""Transactional outbox helpers.

Architecture: canonical message objects live in ``generic_instance``;
``outbox_event`` is a thin execution/dispatch index.
"""

from daylily_tapdb.outbox.contracts import DeliveryResult, InboundReceipt
from daylily_tapdb.outbox.inbox import (
    mark_inbox_failed,
    mark_inbox_processed,
    mark_inbox_processing,
    mark_inbox_rejected,
    receive_message,
)
from daylily_tapdb.outbox.queries import (
    InboxStatusSummary,
    OutboxStatusSummary,
    get_event_attempts,
    inbox_status_summary,
    list_failed_events,
    list_stale_delivering,
    outbox_status_summary,
)
from daylily_tapdb.outbox.repository import (
    claim_events,
    enqueue_event,
    enqueue_fanout,
    mark_dead_letter,
    mark_failed,
    mark_processed,
    mark_received,
    mark_rejected,
    record_attempt,
)
from daylily_tapdb.outbox.worker import dispatch_batch, run_dispatch_loop

__all__ = [
    # Contracts
    "DeliveryResult",
    "InboundReceipt",
    # Outbox producer
    "enqueue_event",
    "enqueue_fanout",
    "claim_events",
    "mark_received",
    "mark_processed",
    "mark_rejected",
    "mark_dead_letter",
    "mark_failed",
    "record_attempt",
    # Worker
    "dispatch_batch",
    "run_dispatch_loop",
    # Inbox receiver
    "receive_message",
    "mark_inbox_processing",
    "mark_inbox_processed",
    "mark_inbox_failed",
    "mark_inbox_rejected",
    # Admin queries
    "OutboxStatusSummary",
    "InboxStatusSummary",
    "outbox_status_summary",
    "inbox_status_summary",
    "list_failed_events",
    "list_stale_delivering",
    "get_event_attempts",
]
