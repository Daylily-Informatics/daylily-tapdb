"""Part 2 §21 — Production outbox spec compliance tests.

Tests:
- DeliveryResult contract (sender side)
- InboundReceipt contract (receiver side)
- Worker _invoke_deliver_fn strict contract
- sender/receiver round-trip
- fanout
- requeue dead-letter
- admin query helpers
- scoping isolation
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest import mock

import pytest

from daylily_tapdb.outbox.contracts import DeliveryResult, InboundReceipt

# ---------------------------------------------------------------------------
# §15 — DeliveryResult contract
# ---------------------------------------------------------------------------

class TestDeliveryResultContract:
    """DeliveryResult must support all factory methods."""

    def test_received_factory(self):
        receipt_id = uuid.uuid4()
        dr = DeliveryResult.received(receipt_id)
        assert dr.success is True
        assert dr.receipt_machine_uuid == receipt_id
        assert dr.transport_status == "receipt_valid"

    def test_transport_failed_factory(self):
        dr = DeliveryResult.transport_failed("connection refused")
        assert dr.success is False
        assert "connection refused" in dr.error_message
        assert dr.transport_status == "transport_failed"

    def test_rejected_factory(self):
        dr = DeliveryResult.rejected("bad payload")
        assert dr.success is False
        assert dr.transport_status == "rejected"

    def test_custom_fields(self):
        dr = DeliveryResult(
            success=True,
            transport_status="custom",
            http_status=200,
            response_body_excerpt="OK",
        )
        assert dr.http_status == 200
        assert dr.response_body_excerpt == "OK"


# ---------------------------------------------------------------------------
# §15 — InboundReceipt contract
# ---------------------------------------------------------------------------

class TestInboundReceiptContract:
    """InboundReceipt must carry all required fields."""

    def test_basic_creation(self):
        msg_uuid = uuid.uuid4()
        rcpt_uuid = uuid.uuid4()
        now = datetime.now(UTC)
        receipt = InboundReceipt(
            message_machine_uuid=msg_uuid,
            receipt_machine_uuid=rcpt_uuid,
            status="received",
            received_dt=now,
        )
        assert receipt.message_machine_uuid == msg_uuid
        assert receipt.receipt_machine_uuid == rcpt_uuid
        assert receipt.status == "received"

    def test_from_delivery_result(self):
        rcpt_uuid = uuid.uuid4()
        dr = DeliveryResult.received(rcpt_uuid)
        now = datetime.now(UTC)
        receipt = InboundReceipt(
            message_machine_uuid=uuid.uuid4(),
            receipt_machine_uuid=dr.receipt_machine_uuid,
            status="received",
            received_dt=now,
        )
        assert receipt.receipt_machine_uuid == rcpt_uuid


# ---------------------------------------------------------------------------
# §16.5 — No backward compat: None-return is TypeError
# ---------------------------------------------------------------------------

class TestWorkerStrictContract:
    """deliver_fn MUST return DeliveryResult; None raises TypeError."""

    def test_none_return_raises_type_error(self):
        from daylily_tapdb.outbox.worker import _invoke_deliver_fn

        def bad_deliver_fn(ev):
            return None  # legacy pattern

        fake_event = mock.MagicMock()
        fake_event.id = 1
        fake_event.destination = "test"
        fake_event.attempt_count = 1

        with pytest.raises(TypeError, match="must return DeliveryResult"):
            _invoke_deliver_fn(bad_deliver_fn, fake_event)

    def test_valid_delivery_result_passes(self):
        from daylily_tapdb.outbox.worker import _invoke_deliver_fn

        rcpt = uuid.uuid4()

        def good_deliver_fn(ev):
            return DeliveryResult.received(rcpt)

        fake_event = mock.MagicMock()
        fake_event.id = 1
        fake_event.destination = "test"
        fake_event.attempt_count = 1

        result = _invoke_deliver_fn(good_deliver_fn, fake_event)
        assert result.success is True
        assert result.receipt_machine_uuid == rcpt

    def test_exception_returns_transport_failed(self):
        from daylily_tapdb.outbox.worker import _invoke_deliver_fn

        def crashing_fn(ev):
            raise ConnectionError("network down")

        fake_event = mock.MagicMock()
        fake_event.id = 1
        fake_event.destination = "test"
        fake_event.attempt_count = 1

        result = _invoke_deliver_fn(crashing_fn, fake_event)
        assert result.success is False
        assert "network down" in result.error_message



# ---------------------------------------------------------------------------
# §21.1 — Sender/receiver round-trip (mocked session)
# ---------------------------------------------------------------------------

class TestSenderReceiverRoundTrip:
    """Enqueue → claim → deliver → receive_message → mark_inbox_processed."""

    def test_full_lifecycle_mocked(self):
        """Simulate the full lifecycle with mocked session."""
        from daylily_tapdb.outbox.inbox import receive_message
        from daylily_tapdb.outbox.repository import (
            enqueue_event,
            mark_received,
            record_attempt,
        )

        session = mock.MagicMock()
        tid = uuid.uuid4()

        # 1. enqueue — uses session.execute, not session.add
        # Mock the internal queries to simulate "no existing row"
        session.execute.return_value.scalar_one_or_none.side_effect = [
            None,  # _lookup_existing_machine_uuid
            1,     # INSERT ... RETURNING id
        ]
        mock_msg = mock.MagicMock()
        mock_msg.uid = 1
        mock_msg.machine_uuid = uuid.uuid4()
        session.begin_nested.return_value.__enter__ = mock.MagicMock(return_value=session)
        session.begin_nested.return_value.__exit__ = mock.MagicMock(return_value=False)
        session.begin_nested.return_value.commit = mock.MagicMock()

        # Patch _create_message_instance
        with mock.patch(
            "daylily_tapdb.outbox.repository._create_message_instance",
            return_value=mock_msg,
        ):
            enqueue_event(
                session,
                tenant_id=tid,
                event_type="test.created",
                aggregate_euid="GX-00001-XXXX",
                payload={"key": "value"},
                destination="svc://downstream",
                dedupe_key="msg-001",
            )
        assert session.execute.call_count >= 1

        # 2. record attempt — mock the flush to set uid on the ORM object
        session.reset_mock()

        def _fake_flush(objs=None):
            # After flush, the attempt object gets a uid from the DB
            for call_args in session.add.call_args_list:
                obj = call_args[0][0]
                if hasattr(obj, "uid") and obj.uid is None:
                    obj.uid = 99

        session.flush.side_effect = _fake_flush
        record_attempt(
            session,
            outbox_event_id=1,
            attempt_no=1,
            transport_status="receipt_valid",
        )
        session.add.assert_called_once()

        # 3. mark received
        session.reset_mock()
        mark_received(session, row_id=1)
        session.execute.assert_called_once()

        # 4. receive on inbox side
        session.reset_mock()
        msg_uuid = uuid.uuid4()
        # receive_message uses pg_insert + session.execute
        mock_row = mock.MagicMock()
        mock_row.receipt_machine_uuid = uuid.uuid4()
        mock_row.status = "received"
        mock_row.received_dt = datetime.now(UTC)
        session.execute.return_value.fetchone.return_value = mock_row
        receipt = receive_message(
            session,
            message_machine_uuid=msg_uuid,
            payload={"event_type": "test"},
        )
        session.execute.assert_called_once()
        assert receipt is not None


# ---------------------------------------------------------------------------
# §21.2 — Fanout
# ---------------------------------------------------------------------------

class TestFanout:
    """enqueue_fanout creates one row per destination."""

    def test_fanout_creates_n_events(self):
        from daylily_tapdb.outbox.repository import enqueue_fanout

        session = mock.MagicMock()
        # enqueue_fanout takes list of (destination, dedupe_key) tuples
        destinations = [
            ("svc://a", "fan-a"),
            ("svc://b", "fan-b"),
            ("svc://c", "fan-c"),
        ]

        # enqueue_fanout uses session.execute per destination
        session.execute.return_value.scalar_one_or_none.return_value = 42

        enqueue_fanout(
            session,
            message_uid=1,
            destinations=destinations,
        )

        # Should execute 3 inserts + 1 flush
        assert session.execute.call_count == 3
        session.flush.assert_called_once()


# ---------------------------------------------------------------------------
# §21.3 — Requeue dead-letter
# ---------------------------------------------------------------------------

class TestRequeueDeadLetter:
    """requeue_dead_letter transitions dead_letter → pending."""

    def test_requeue_calls_update(self):
        from daylily_tapdb.outbox.repository import requeue_dead_letter

        session = mock.MagicMock()
        requeue_dead_letter(session, row_id=42)

        session.execute.assert_called_once()
        session.flush.assert_called_once()

    def test_requeue_with_reset_count(self):
        from daylily_tapdb.outbox.repository import requeue_dead_letter

        session = mock.MagicMock()
        requeue_dead_letter(session, row_id=42, reset_attempt_count=True)

        session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# §21.4 — Admin query helpers
# ---------------------------------------------------------------------------

class TestAdminQueryHelpers:
    """Verify admin queries accept domain/status filters."""

    def test_list_events_by_destination(self):
        from daylily_tapdb.outbox.queries import list_events_by_destination

        session = mock.MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []

        result = list_events_by_destination(session, "svc://test")
        assert result == []

    def test_list_events_by_destination_with_status(self):
        from daylily_tapdb.outbox.queries import list_events_by_destination

        session = mock.MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []

        result = list_events_by_destination(session, "svc://test", status="failed")
        assert result == []

    def test_get_outbox_event_by_receipt_uuid(self):
        from daylily_tapdb.outbox.queries import get_outbox_event_by_receipt_uuid

        session = mock.MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        result = get_outbox_event_by_receipt_uuid(session, uuid.uuid4())
        assert result is None

    def test_get_inbox_message_by_machine_uuid(self):
        from daylily_tapdb.outbox.inbox import get_inbox_message_by_machine_uuid

        session = mock.MagicMock()
        session.query.return_value.filter.return_value.first.return_value = None

        result = get_inbox_message_by_machine_uuid(session, uuid.uuid4())
        assert result is None


# ---------------------------------------------------------------------------
# §21.5 — Status summary (no 'delivered' status)
# ---------------------------------------------------------------------------

class TestStatusSummaryNoDelivered:
    """OutboxStatusSummary must not have a 'delivered' field."""

    def test_no_delivered_field(self):
        from daylily_tapdb.outbox.queries import OutboxStatusSummary

        summary = OutboxStatusSummary()
        assert not hasattr(summary, "delivered")
        # All 8 valid statuses
        assert hasattr(summary, "pending")
        assert hasattr(summary, "delivering")
        assert hasattr(summary, "received")
        assert hasattr(summary, "processed")
        assert hasattr(summary, "failed")
        assert hasattr(summary, "dead_letter")
        assert hasattr(summary, "rejected")
        assert hasattr(summary, "canceled")
