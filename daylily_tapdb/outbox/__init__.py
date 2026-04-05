"""Transactional outbox helpers.

Architecture: canonical message objects live in ``generic_instance``;
``outbox_event`` is a thin execution/dispatch index.
"""

from daylily_tapdb.outbox.contracts import DeliveryResult, InboundReceipt
from daylily_tapdb.outbox.inbox import (
    get_inbox_message_by_machine_uuid,
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
    get_outbox_event_by_receipt_uuid,
    inbox_status_summary,
    list_events_by_destination,
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
    requeue_dead_letter,
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
    "requeue_dead_letter",
    # Worker
    "dispatch_batch",
    "run_dispatch_loop",
    # Inbox receiver
    "receive_message",
    "mark_inbox_processing",
    "mark_inbox_processed",
    "mark_inbox_failed",
    "mark_inbox_rejected",
    "get_inbox_message_by_machine_uuid",
    # Admin queries
    "OutboxStatusSummary",
    "InboxStatusSummary",
    "outbox_status_summary",
    "inbox_status_summary",
    "list_failed_events",
    "list_stale_delivering",
    "get_event_attempts",
    "list_events_by_destination",
    "get_outbox_event_by_receipt_uuid",
]
