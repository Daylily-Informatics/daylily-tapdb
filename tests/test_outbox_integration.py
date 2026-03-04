import os
import random
import time
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select, text

from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.models.outbox import outbox_event
from daylily_tapdb.outbox.repository import claim_events, enqueue_event, mark_delivered
from tests.test_integration import _drop_schema, _install_schema


def test_postgres_outbox_enqueue_claim_and_mark_delivered():
    dsn = os.environ.get("TAPDB_TEST_DSN")
    if not dsn:
        pytest.skip("Set TAPDB_TEST_DSN to run Postgres integration tests")

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    schema_name = (
        f"tapdb_test_outbox_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    _install_schema(dsn, schema_name, schema_sql_path)

    try:
        conn = TAPDBConnection(db_url=dsn, app_username="pytest")
        with conn.session_scope(commit=False) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))

            tenant_id = uuid.uuid4()
            event_id = enqueue_event(
                session=session,
                tenant_id=tenant_id,
                event_type="order.created",
                aggregate_euid="GX-ABC",
                payload={"order_number": "ORD-1"},
                destination="atlas",
                dedupe_key="atlas|order.created|GX-ABC",
            )

            row = session.execute(
                select(outbox_event).where(outbox_event.event_id == event_id)
            ).scalar_one()
            assert row.status == "pending"
            assert str(row.tenant_id) == str(tenant_id)

            claimed = claim_events(session, batch_size=10, lock_timeout_s=5)
            assert len(claimed) == 1
            assert claimed[0].status == "delivering"

            mark_delivered(session, claimed[0].id)
            delivered = session.execute(
                select(outbox_event).where(outbox_event.id == claimed[0].id)
            ).scalar_one()
            assert delivered.status == "delivered"
            assert delivered.delivered_dt is not None
    finally:
        _drop_schema(dsn, schema_name)
