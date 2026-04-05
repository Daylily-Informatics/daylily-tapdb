"""Unit tests for outbox contracts (DeliveryResult, InboundReceipt)."""

import uuid
from datetime import UTC, datetime

from daylily_tapdb.outbox.contracts import DeliveryResult, InboundReceipt


class TestDeliveryResult:
    def test_received_factory(self):
        rid = uuid.uuid4()
        r = DeliveryResult.received(rid, http_status=200)
        assert r.success is True
        assert r.transport_status == "receipt_valid"
        assert r.receipt_machine_uuid == rid
        assert r.receipt_status == "received"
        assert r.http_status == 200

    def test_processed_factory(self):
        rid = uuid.uuid4()
        now = datetime.now(UTC)
        r = DeliveryResult.processed(rid, receipt_received_dt=now, receipt_processed_dt=now)
        assert r.success is True
        assert r.transport_status == "processed"
        assert r.receipt_status == "processed"
        assert r.receipt_received_dt == now
        assert r.receipt_processed_dt == now

    def test_transport_failed_factory(self):
        r = DeliveryResult.transport_failed("connection refused", http_status=503)
        assert r.success is False
        assert r.transport_status == "transport_failed"
        assert r.error_message == "connection refused"
        assert r.http_status == 503
        assert r.retryable is True

    def test_transport_failed_not_retryable(self):
        r = DeliveryResult.transport_failed("bad request", retryable=False)
        assert r.retryable is False

    def test_rejected_factory(self):
        r = DeliveryResult.rejected(
            "invalid payload", error_code="E_SCHEMA", http_status=422
        )
        assert r.success is False
        assert r.transport_status == "rejected"
        assert r.receipt_status == "rejected"
        assert r.error_code == "E_SCHEMA"
        assert r.retryable is False

    def test_frozen(self):
        r = DeliveryResult(success=True)
        try:
            r.success = False  # type: ignore[misc]
            assert False, "Should have raised"
        except AttributeError:
            pass

    def test_extra_default_empty(self):
        r = DeliveryResult(success=True)
        assert r.extra == {}


class TestInboundReceipt:
    def test_basic_fields(self):
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
        assert receipt.processed_dt is None
        assert receipt.error_code is None
