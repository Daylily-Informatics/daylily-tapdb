import random
import time
import uuid
from pathlib import Path

from sqlalchemy import select, text

from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.outbox import outbox_event
from daylily_tapdb.outbox import list_by_destination, lookup_by_machine_uuid
from daylily_tapdb.outbox.repository import (
    claim_events,
    enqueue_event,
    enqueue_fanout,
    mark_received,
)
from tests.conftest import resolve_tapdb_test_dsn
from tests.test_integration import (
    _drop_schema,
    _install_schema,
    _seed_identity_prefixes,
    _seed_templates,
)

# Minimal message template definition for tests
_MSG_TEMPLATE = {
    "name": "Webhook Event Message",
    "polymorphic_discriminator": "generic_template",
    "category": "system",
    "type": "message",
    "subtype": "webhook_event",
    "version": "1.0",
    "instance_prefix": "MSG",
    "is_singleton": False,
    "bstatus": "active",
    "json_addl": {
        "description": "Canonical message object for webhook/outbox events",
    },
}


def _setup_schema(pytestconfig, suffix="outbox"):
    """Create a fresh test schema with the message template seeded."""
    dsn = resolve_tapdb_test_dsn(pytestconfig)
    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"
    schema_name = (
        f"tapdb_test_{suffix}_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    _install_schema(dsn, schema_name, schema_sql_path)

    # Seed identity prefixes and message template
    conn = TAPDBConnection(db_url=dsn, app_username="pytest")
    with conn.session_scope(commit=True) as session:
        session.execute(text(f"SET search_path TO {schema_name}"))
        _seed_identity_prefixes(session, prefix="TST")
        _seed_templates(session, [_MSG_TEMPLATE])

    return dsn, schema_name


def test_postgres_outbox_enqueue_creates_message_instance(pytestconfig):
    """enqueue_event creates a generic_instance message + thin outbox_event row."""
    dsn, schema_name = _setup_schema(pytestconfig)

    try:
        conn = TAPDBConnection(db_url=dsn, app_username="pytest")
        with conn.session_scope(commit=False) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))

            tenant_id = uuid.uuid4()
            machine_uuid = enqueue_event(
                session=session,
                tenant_id=tenant_id,
                event_type="order.created",
                aggregate_euid="TGX-ABC",
                payload={"order_number": "ORD-1"},
                destination="atlas",
                dedupe_key="atlas|order.created|TGX-ABC",
            )

            # Verify the returned value is a UUIDv7
            assert isinstance(machine_uuid, uuid.UUID)
            assert machine_uuid.version == 7

            # Verify the canonical message instance was created
            msg = session.execute(
                select(generic_instance).where(
                    generic_instance.machine_uuid == machine_uuid
                )
            ).scalar_one()
            assert msg.domain_code == "T"
            assert msg.issuer_app_code == "TAPD"
            assert msg.category == "system"
            assert msg.type == "message"
            assert msg.subtype == "webhook_event"
            assert msg.json_addl["event_type"] == "order.created"
            assert msg.json_addl["aggregate_euid"] == "TGX-ABC"
            assert msg.json_addl["payload"] == {"order_number": "ORD-1"}
            assert str(msg.tenant_id) == str(tenant_id)

            # Verify the execution index row was created
            oe = session.execute(
                select(outbox_event).where(outbox_event.message_uid == msg.uid)
            ).scalar_one()
            assert oe.domain_code == "T"
            assert oe.issuer_app_code == "TAPD"
            assert oe.status == "pending"
            assert oe.destination == "atlas"
            assert oe.dedupe_key == "atlas|order.created|TGX-ABC"

            # Verify outbox row does NOT have payload columns
            assert not hasattr(oe, "payload") or not isinstance(
                getattr(type(oe), "payload", None), property
            )
    finally:
        _drop_schema(dsn, schema_name)


def test_postgres_outbox_domain_scoping_isolates_dedupe_and_queries(pytestconfig):
    """Same destination+dedupe can exist in separate domain/app scopes."""
    dsn, schema_name = _setup_schema(pytestconfig, suffix="scope")

    try:
        tenant_id = uuid.uuid4()
        conn_a = TAPDBConnection(
            db_url=dsn,
            app_username="pytest-a",
            domain_code="A",
            issuer_app_code="APPA",
        )
        conn_b = TAPDBConnection(
            db_url=dsn,
            app_username="pytest-b",
            domain_code="B",
            issuer_app_code="APPB",
        )

        with conn_a.session_scope(commit=True) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))
            machine_a = enqueue_event(
                session=session,
                tenant_id=tenant_id,
                event_type="order.created",
                aggregate_euid="ORD-A",
                payload={"order_number": "ORD-A"},
                destination="https://tenant.example.com/webhook",
                dedupe_key="shared-dedupe-key",
            )

        with conn_b.session_scope(commit=True) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))
            machine_b = enqueue_event(
                session=session,
                tenant_id=tenant_id,
                event_type="order.created",
                aggregate_euid="ORD-B",
                payload={"order_number": "ORD-B"},
                destination="https://tenant.example.com/webhook",
                dedupe_key="shared-dedupe-key",
            )

        assert machine_a != machine_b

        with conn_a.session_scope(commit=False) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))
            rows = list_by_destination(
                session,
                "https://tenant.example.com/webhook",
                domain_code="A",
                issuer_app_code="APPA",
            )
            assert len(rows) == 1
            assert rows[0].domain_code == "A"
            assert rows[0].issuer_app_code == "APPA"
            assert lookup_by_machine_uuid(
                session,
                machine_a,
                domain_code="A",
                issuer_app_code="APPA",
            )
            assert (
                lookup_by_machine_uuid(
                    session,
                    machine_b,
                    domain_code="A",
                    issuer_app_code="APPA",
                )
                is None
            )

        with conn_b.session_scope(commit=False) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))
            rows = list_by_destination(
                session,
                "https://tenant.example.com/webhook",
                domain_code="B",
                issuer_app_code="APPB",
            )
            assert len(rows) == 1
            assert rows[0].domain_code == "B"
            assert rows[0].issuer_app_code == "APPB"
            assert lookup_by_machine_uuid(
                session,
                machine_b,
                domain_code="B",
                issuer_app_code="APPB",
            )
            assert (
                lookup_by_machine_uuid(
                    session,
                    machine_a,
                    domain_code="B",
                    issuer_app_code="APPB",
                )
                is None
            )
    finally:
        _drop_schema(dsn, schema_name)


def test_postgres_outbox_claim_and_deliver(pytestconfig):
    """claim_events returns outbox rows with eagerly-loaded message."""
    dsn, schema_name = _setup_schema(pytestconfig, suffix="claim")

    try:
        conn = TAPDBConnection(db_url=dsn, app_username="pytest")
        with conn.session_scope(commit=False) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))

            tenant_id = uuid.uuid4()
            machine_uuid = enqueue_event(
                session=session,
                tenant_id=tenant_id,
                event_type="trf.transition",
                aggregate_euid="TRF-1234",
                payload={"from_state": "DRAFT", "to_state": "SUBMITTED"},
                destination="https://inflection.example.com/webhook",
                dedupe_key="sub1:TRF-1234",
            )

            claimed = claim_events(session, batch_size=10, lock_timeout_s=5)
            assert len(claimed) == 1
            ev = claimed[0]
            assert ev.status == "delivering"

            # Worker reads payload from the eagerly-loaded message
            assert ev.message is not None
            assert ev.message.machine_uuid == machine_uuid
            assert ev.message.json_addl["event_type"] == "trf.transition"
            assert ev.message.json_addl["payload"]["to_state"] == "SUBMITTED"

            mark_received(session, ev.id)
            received = session.execute(
                select(outbox_event).where(outbox_event.id == ev.id)
            ).scalar_one()
            assert received.status == "received"
            assert received.receipt_received_dt is not None
    finally:
        _drop_schema(dsn, schema_name)


def test_postgres_outbox_fanout_multiple_destinations(pytestconfig):
    """One canonical message can fan out to multiple outbox_event rows."""
    dsn, schema_name = _setup_schema(pytestconfig, suffix="fanout")

    try:
        conn = TAPDBConnection(db_url=dsn, app_username="pytest")
        with conn.session_scope(commit=False) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))

            tenant_id = uuid.uuid4()
            # Create the canonical message via a single enqueue
            machine_uuid = enqueue_event(
                session=session,
                tenant_id=tenant_id,
                event_type="trf.transition",
                aggregate_euid="TRF-5678",
                payload={"from_state": "SUBMITTED", "to_state": "IN_EXTRACTION"},
                destination="https://customer-a.example.com/webhook",
                dedupe_key="subA:TRF-5678",
            )

            # Look up the message uid
            msg = session.execute(
                select(generic_instance).where(
                    generic_instance.machine_uuid == machine_uuid
                )
            ).scalar_one()

            # Fan out to additional destinations
            extra_ids = enqueue_fanout(
                session,
                message_uid=msg.uid,
                destinations=[
                    ("https://customer-b.example.com/webhook", "subB:TRF-5678"),
                    ("https://internal-audit.example.com/events", "audit:TRF-5678"),
                ],
            )
            assert len(extra_ids) == 2

            # Verify total outbox rows: 1 original + 2 fanout = 3
            all_rows = (
                session.execute(
                    select(outbox_event).where(outbox_event.message_uid == msg.uid)
                )
                .scalars()
                .all()
            )
            assert len(all_rows) == 3

            # All rows reference the same canonical message
            destinations = sorted(r.destination for r in all_rows)
            assert destinations == [
                "https://customer-a.example.com/webhook",
                "https://customer-b.example.com/webhook",
                "https://internal-audit.example.com/events",
            ]
    finally:
        _drop_schema(dsn, schema_name)
