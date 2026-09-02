import random
import time
import uuid
from pathlib import Path

from sqlalchemy import select

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.outbox import outbox_event
from daylily_tapdb.outbox import list_events_by_destination, lookup_by_machine_uuid
from daylily_tapdb.outbox.repository import (
    claim_events,
    enqueue_event,
    enqueue_fanout,
    mark_received,
)
from tests.test_integration import (
    _drop_schema,
    _install_bound_schema,
    _provision_runtime_principal,
    _runtime_connection,
    _seed_identity_prefixes,
    _seed_templates,
)

# Minimal message template definition for tests
_MSG_TEMPLATE = {
    "name": "Webhook Event Message",
    "polymorphic_discriminator": "generic_template",
    "category": "message",
    "type": "webhook",
    "subtype": "event",
    "version": "1.0",
    "instance_prefix": "MSG",
    "is_singleton": False,
    "bstatus": "active",
    "json_addl": {
        "description": "Canonical message object for webhook/outbox events",
    },
}


def _setup_schema(
    pytestconfig,
    *,
    tenant_id: uuid.UUID,
    suffix: str = "outbox",
    domain_code: str = "T",
    owner_repo_name: str = "daylily-tapdb",
):
    """Create a fresh test schema with the message template seeded."""
    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"
    schema_name = (
        f"tapdb_test_{suffix}_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    operator_dsn, dsn, config_identity = _install_bound_schema(
        pytestconfig,
        schema_name,
        schema_sql_path,
        domain_code=domain_code,
        owner_repo_name=owner_repo_name,
        tenant_id=tenant_id,
    )

    # Seed identity prefixes and message template
    conn = _runtime_connection(
        dsn=operator_dsn,
        schema_name=schema_name,
        config_identity=config_identity,
        app_username="pytest-outbox-template-seed",
        domain_code=domain_code,
        owner_repo_name=owner_repo_name,
        tenant_id=tenant_id,
        allow_global_rows=True,
        connection_role="operator",
    )
    with conn.session_scope(commit=True) as session:
        _seed_identity_prefixes(
            session,
            prefix="TST",
            domain_code=domain_code,
            owner_repo_name=owner_repo_name,
        )
        _seed_templates(session, [_MSG_TEMPLATE])

    return operator_dsn, dsn, schema_name, config_identity


def test_postgres_outbox_enqueue_creates_message_instance(pytestconfig):
    """enqueue_event creates a generic_instance message + thin outbox_event row."""
    tenant_id = uuid.uuid4()
    operator_dsn, dsn, schema_name, config_identity = _setup_schema(
        pytestconfig, tenant_id=tenant_id
    )

    try:
        conn = _runtime_connection(
            dsn=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-outbox-enqueue",
            tenant_id=tenant_id,
            allow_global_rows=True,
        )
        with conn.session_scope(commit=False) as session:
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
            assert msg.issuer_app_code == "daylily-tapdb"
            assert msg.category == "message"
            assert msg.type == "webhook"
            assert msg.subtype == "event"
            assert msg.json_addl["event_type"] == "order.created"
            assert msg.json_addl["aggregate_euid"] == "TGX-ABC"
            assert msg.json_addl["payload"] == {"order_number": "ORD-1"}
            assert str(msg.tenant_id) == str(tenant_id)

            # Verify the execution index row was created
            oe = session.execute(
                select(outbox_event).where(outbox_event.message_uid == msg.uid)
            ).scalar_one()
            assert oe.domain_code == "T"
            assert oe.issuer_app_code == "daylily-tapdb"
            assert oe.status == "pending"
            assert oe.destination == "atlas"
            assert oe.dedupe_key == "atlas|order.created|TGX-ABC"

            # Verify outbox row does NOT have payload columns
            assert not hasattr(oe, "payload") or not isinstance(
                getattr(type(oe), "payload", None), property
            )
    finally:
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn,))


def test_postgres_outbox_domain_scoping_isolates_dedupe_and_queries(pytestconfig):
    """Same destination+dedupe can exist in separate domain/app scopes."""
    tenant_id = uuid.uuid4()
    operator_dsn, dsn_a, schema_name, config_identity = _setup_schema(
        pytestconfig,
        tenant_id=tenant_id,
        suffix="scope",
        domain_code="A",
        owner_repo_name="appa",
    )
    dsn_b = _provision_runtime_principal(
        operator_dsn,
        schema_name,
        config_identity=config_identity,
        domain_code="B",
        owner_repo_name="appb",
        tenant_id=tenant_id,
        allow_global_rows=True,
    )

    try:
        for domain_code, owner_repo_name in (("B", "appb"),):
            seed_conn = _runtime_connection(
                dsn=operator_dsn,
                schema_name=schema_name,
                config_identity=config_identity,
                app_username=f"pytest-{domain_code.lower()}-template-seed",
                domain_code=domain_code,
                owner_repo_name=owner_repo_name,
                tenant_id=tenant_id,
                allow_global_rows=True,
                connection_role="operator",
            )
            with seed_conn.session_scope(commit=True) as session:
                _seed_identity_prefixes(
                    session,
                    prefix="TST",
                    domain_code=domain_code,
                    owner_repo_name=owner_repo_name,
                )
                _seed_templates(session, [_MSG_TEMPLATE])

        conn_a = _runtime_connection(
            dsn=dsn_a,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-a",
            domain_code="A",
            owner_repo_name="appa",
            tenant_id=tenant_id,
            allow_global_rows=True,
        )
        conn_b = _runtime_connection(
            dsn=dsn_b,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-b",
            domain_code="B",
            owner_repo_name="appb",
            tenant_id=tenant_id,
            allow_global_rows=True,
        )

        with conn_a.session_scope(commit=True) as session:
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
            rows = list_events_by_destination(
                session,
                "https://tenant.example.com/webhook",
                domain_code="A",
                issuer_app_code="appa",
            )
            assert len(rows) == 1
            assert rows[0].domain_code == "A"
            assert rows[0].issuer_app_code == "appa"
            assert lookup_by_machine_uuid(
                session,
                machine_a,
                domain_code="A",
                issuer_app_code="appa",
            )
            assert (
                lookup_by_machine_uuid(
                    session,
                    machine_b,
                    domain_code="A",
                    issuer_app_code="appa",
                )
                is None
            )

        with conn_b.session_scope(commit=False) as session:
            rows = list_events_by_destination(
                session,
                "https://tenant.example.com/webhook",
                domain_code="B",
                issuer_app_code="appb",
            )
            assert len(rows) == 1
            assert rows[0].domain_code == "B"
            assert rows[0].issuer_app_code == "appb"
            assert lookup_by_machine_uuid(
                session,
                machine_b,
                domain_code="B",
                issuer_app_code="appb",
            )
            assert (
                lookup_by_machine_uuid(
                    session,
                    machine_a,
                    domain_code="B",
                    issuer_app_code="appb",
                )
                is None
            )
    finally:
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn_a, dsn_b))


def test_postgres_outbox_claim_and_deliver(pytestconfig):
    """claim_events returns outbox rows with eagerly-loaded message."""
    tenant_id = uuid.uuid4()
    operator_dsn, dsn, schema_name, config_identity = _setup_schema(
        pytestconfig, tenant_id=tenant_id, suffix="claim"
    )

    try:
        conn = _runtime_connection(
            dsn=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-outbox-claim",
            tenant_id=tenant_id,
            allow_global_rows=True,
        )
        with conn.session_scope(commit=False) as session:
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
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn,))


def test_postgres_outbox_fanout_multiple_destinations(pytestconfig):
    """One canonical message can fan out to multiple outbox_event rows."""
    tenant_id = uuid.uuid4()
    operator_dsn, dsn, schema_name, config_identity = _setup_schema(
        pytestconfig, tenant_id=tenant_id, suffix="fanout"
    )

    try:
        conn = _runtime_connection(
            dsn=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-outbox-fanout",
            tenant_id=tenant_id,
            allow_global_rows=True,
        )
        with conn.session_scope(commit=False) as session:
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
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn,))
