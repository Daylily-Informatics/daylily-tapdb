from sqlalchemy.dialects import postgresql

from daylily_tapdb.outbox.repository import _build_claim_select, _build_enqueue_stmt


def test_outbox_claim_select_includes_for_update_skip_locked():
    sql = str(
        _build_claim_select(batch_size=10).compile(dialect=postgresql.dialect())
    ).lower()
    assert "for update" in sql
    assert "skip locked" in sql


def test_outbox_enqueue_uses_conflict_handling():
    """Execution index insert uses ON CONFLICT DO NOTHING on (destination, dedupe_key)."""
    sql = str(
        _build_enqueue_stmt(
            message_uid=42,
            destination="atlas",
            dedupe_key="atlas|order.created|TGX-ABC",
        ).compile(dialect=postgresql.dialect())
    ).lower()
    assert "on conflict" in sql
    assert "do nothing" in sql


def test_outbox_enqueue_stmt_does_not_contain_payload():
    """outbox_event execution index must not store payload columns."""
    sql = str(
        _build_enqueue_stmt(
            message_uid=42,
            destination="atlas",
            dedupe_key="atlas|order.created|TGX-ABC",
        ).compile(dialect=postgresql.dialect())
    ).lower()
    assert "payload" not in sql
    assert "event_type" not in sql
    assert "aggregate_euid" not in sql
    assert "event_id" not in sql
    assert "message_uid" in sql
