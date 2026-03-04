import uuid

from sqlalchemy.dialects import postgresql

from daylily_tapdb.outbox.repository import _build_claim_select, _build_enqueue_stmt


def test_outbox_claim_select_includes_for_update_skip_locked():
    sql = str(
        _build_claim_select(batch_size=10).compile(dialect=postgresql.dialect())
    ).lower()
    assert "for update" in sql
    assert "skip locked" in sql


def test_outbox_enqueue_uses_conflict_handling():
    sql = str(
        _build_enqueue_stmt(
            event_id=uuid.uuid4(),
            tenant_id=uuid.uuid4(),
            event_type="order.created",
            aggregate_euid="GX-ABC",
            payload={"hello": "world"},
            destination="atlas",
            dedupe_key="atlas|order.created|GX-ABC",
        ).compile(dialect=postgresql.dialect())
    ).lower()
    assert "on conflict" in sql
    assert "do nothing" in sql

