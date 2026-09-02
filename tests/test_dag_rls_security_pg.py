"""Real-PostgreSQL security proofs for DAG lineage and migration provenance."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]
TENANT_A = uuid.UUID("00000000-0000-0000-0000-00000000000a")
TENANT_B = uuid.UUID("00000000-0000-0000-0000-00000000000b")
OWNER = "daylily-tapdb"
OTHER_OWNER = "other-owner"


def _runtime_role(schema_name: str, tenant_id: uuid.UUID | None) -> str:
    if tenant_id is None:
        suffix = "global"
    elif tenant_id == TENANT_A:
        suffix = "tenant_a"
    elif tenant_id == TENANT_B:
        suffix = "tenant_b"
    else:  # pragma: no cover - this fixture deliberately defines two tenants
        raise ValueError(f"No runtime role is bound for tenant {tenant_id}")
    return f"{schema_name}_{suffix}"


def _set_context(
    cursor,
    *,
    schema_name: str,
    tenant_id: uuid.UUID | None,
    owner: str = OWNER,
    allow_global_rows: bool = False,
) -> None:
    values = (
        ("search_path", schema_name),
        ("session.current_config_identity", f"pytest:{schema_name}"),
        ("session.current_schema_name", schema_name),
        ("session.current_domain_code", "Z"),
        ("session.current_owner_repo_name", owner),
        ("session.current_tenant_id", "" if tenant_id is None else str(tenant_id)),
        ("session.current_username", "pytest:dag-rls-security"),
        ("session.allow_global_rows", "true" if allow_global_rows else "false"),
    )
    for name, value in values:
        cursor.execute("SELECT set_config(%s, %s, true)", (name, value))


@contextmanager
def _transaction(
    pg_instance,
    schema_name: str,
    *,
    tenant_id: uuid.UUID | None,
    owner: str = OWNER,
    allow_global_rows: bool = False,
):
    connection = psycopg2.connect(
        host="localhost",
        port=pg_instance["port"],
        dbname=pg_instance["database"],
        user=_runtime_role(schema_name, tenant_id),
    )
    try:
        with connection.cursor() as cursor:
            _set_context(
                cursor,
                schema_name=schema_name,
                tenant_id=tenant_id,
                owner=owner,
                allow_global_rows=allow_global_rows,
            )
            cursor.execute("SELECT tapdb_assert_runtime_role()")
            yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


@pytest.fixture
def dag_rls_schema(pg_instance):
    schema_name = f"tapdb_dag_rls_{uuid.uuid4().hex[:12]}"
    runtime_bindings = (
        (None, True),
        (TENANT_A, False),
        (TENANT_B, False),
    )
    connection = psycopg2.connect(pg_instance["operator_dsn"])
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema_name}"')
            _set_context(cursor, schema_name=schema_name, tenant_id=None)
            cursor.execute((ROOT / "schema" / "tapdb_schema.sql").read_text())
            for prefix in ("adt", "edg", "tpx", "xrf"):
                cursor.execute(f"CREATE SEQUENCE IF NOT EXISTS {prefix}_instance_seq")
            for owner in (OWNER, OTHER_OWNER):
                cursor.executemany(
                    """
                    INSERT INTO tapdb_identity_prefix_config (
                        entity, domain_code, issuer_app_code, prefix
                    ) VALUES (%s, 'Z', %s, %s)
                    """,
                    (
                        ("generic_template", owner, "TPX"),
                        ("generic_instance_lineage", owner, "EDG"),
                        ("audit_log", owner, "ADT"),
                    ),
                )
            cursor.execute((ROOT / "schema" / "rls.sql").read_text())
            for tenant_id, allow_global_rows in runtime_bindings:
                role = _runtime_role(schema_name, tenant_id)
                cursor.execute(
                    f'CREATE ROLE "{role}" LOGIN NOSUPERUSER NOBYPASSRLS '
                    "NOCREATEDB NOCREATEROLE NOREPLICATION"
                )
                cursor.execute(
                    f'GRANT CONNECT ON DATABASE "{pg_instance["database"]}" TO "{role}"'
                )
                cursor.execute(f'GRANT USAGE ON SCHEMA "{schema_name}" TO "{role}"')
                cursor.execute(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
                    f'"{schema_name}" TO "{role}"'
                )
                cursor.execute(
                    "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA "
                    f'"{schema_name}" TO "{role}"'
                )
                cursor.execute(
                    "INSERT INTO tapdb_runtime_principal_scope ("
                    "role_name, config_identity, schema_name, domain_code, "
                    "issuer_app_code, tenant_id, allow_global_rows) VALUES ("
                    "%s, %s, %s, 'Z', %s, %s, %s)",
                    (
                        role,
                        f"pytest:{schema_name}",
                        schema_name,
                        OWNER,
                        None if tenant_id is None else str(tenant_id),
                        allow_global_rows,
                    ),
                )
                cursor.execute(
                    f'REVOKE ALL ON TABLE "{schema_name}".'
                    f'tapdb_runtime_principal_scope FROM "{role}"'
                )
        connection.commit()
        yield schema_name
    finally:
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            for tenant_id, _allow_global_rows in runtime_bindings:
                role = _runtime_role(schema_name, tenant_id)
                cursor.execute(
                    f'REVOKE CONNECT ON DATABASE "{pg_instance["database"]}" '
                    f'FROM "{role}"'
                )
                cursor.execute(f'DROP OWNED BY "{role}"')
                cursor.execute(f'DROP ROLE "{role}"')
        connection.commit()
        connection.close()


def _insert_template(cursor, *, typed_xrf: bool = False) -> int:
    coordinates = (
        ("reference", "external_identifier", "tapdb_object", "1.0", "XRF")
        if typed_xrf
        else ("message", "webhook", "event", "1.0", "MSG")
    )
    cursor.execute(
        """
        INSERT INTO generic_template (
            name, tenant_id, polymorphic_discriminator,
            category, type, subtype, version, instance_prefix,
            bstatus, is_singleton, json_addl
        ) VALUES (
            %s, NULL, 'generic_template', %s, %s, %s, %s, %s,
            'active', FALSE, '{}'::jsonb
        ) RETURNING uid
        """,
        ("typed-xrf" if typed_xrf else "message", *coordinates),
    )
    return int(cursor.fetchone()[0])


def _insert_instance(
    cursor,
    *,
    template_uid: int,
    tenant_id: uuid.UUID | None,
    typed_xrf: bool = False,
    name: str,
) -> tuple[int, str, int]:
    coordinates = (
        ("reference", "external_identifier", "tapdb_object", "1.0")
        if typed_xrf
        else ("message", "webhook", "event", "1.0")
    )
    cursor.execute(
        """
        INSERT INTO generic_instance (
            name, tenant_id, polymorphic_discriminator,
            category, type, subtype, version, template_uid,
            json_addl, bstatus, is_singleton
        ) VALUES (
            %s, %s, 'generic_instance', %s, %s, %s, %s, %s,
            '{}'::jsonb, 'active', FALSE
        ) RETURNING uid, euid, euid_seq
        """,
        (
            name,
            None if tenant_id is None else str(tenant_id),
            *coordinates,
            template_uid,
        ),
    )
    uid, euid, euid_seq = cursor.fetchone()
    return int(uid), str(euid), int(euid_seq)


def _insert_lineage(
    cursor,
    *,
    parent_uid: int,
    child_uid: int,
    tenant_id: uuid.UUID,
    approved_global_link: bool = False,
) -> int:
    cursor.execute(
        """
        INSERT INTO generic_instance_lineage (
            name, tenant_id, polymorphic_discriminator,
            category, type, subtype, version, bstatus,
            parent_instance_uid, child_instance_uid, relationship_type,
            json_addl
        ) VALUES (
            'security-proof', %s, 'generic_instance_lineage',
            'lineage', 'lineage', 'external_reference', '1.0', 'active',
            %s, %s, 'references',
            jsonb_build_object(
                'properties', jsonb_build_object(
                    'approved_global_link', %s,
                    'asserted_at', '2026-09-02T00:00:00+00:00',
                    'assertion_provenance', 'real PostgreSQL security proof'
                )
            )
        ) RETURNING uid
        """,
        (str(tenant_id), parent_uid, child_uid, approved_global_link),
    )
    return int(cursor.fetchone()[0])


def _lineage_rejection_message(
    pg_instance,
    schema_name: str,
    *,
    parent_uid: int,
    child_uid: int,
) -> str:
    with pytest.raises(psycopg2.Error) as exc_info:
        with _transaction(pg_instance, schema_name, tenant_id=TENANT_A) as cursor:
            _insert_lineage(
                cursor,
                parent_uid=parent_uid,
                child_uid=child_uid,
                tenant_id=TENANT_A,
            )
    return str(exc_info.value.diag.message_primary)


def test_dag_rls_pg_enforces_lineage_endpoint_scope_and_typed_global_exception(
    pg_instance, dag_rls_schema
) -> None:
    with _transaction(
        pg_instance,
        dag_rls_schema,
        tenant_id=None,
        allow_global_rows=True,
    ) as cursor:
        message_template = _insert_template(cursor)
        xrf_template = _insert_template(cursor, typed_xrf=True)
        global_xrf_uid, _, _ = _insert_instance(
            cursor,
            template_uid=xrf_template,
            tenant_id=None,
            typed_xrf=True,
            name="global typed reference",
        )
        global_ordinary_uid, _, _ = _insert_instance(
            cursor,
            template_uid=message_template,
            tenant_id=None,
            name="global ordinary object",
        )

    with _transaction(pg_instance, dag_rls_schema, tenant_id=TENANT_A) as cursor:
        tenant_a_parent, _, _ = _insert_instance(
            cursor,
            template_uid=message_template,
            tenant_id=TENANT_A,
            name="tenant A parent",
        )
        tenant_a_child, _, _ = _insert_instance(
            cursor,
            template_uid=message_template,
            tenant_id=TENANT_A,
            name="tenant A child",
        )
        assert (
            _insert_lineage(
                cursor,
                parent_uid=tenant_a_parent,
                child_uid=tenant_a_child,
                tenant_id=TENANT_A,
            )
            > 0
        )
        assert (
            _insert_lineage(
                cursor,
                parent_uid=tenant_a_parent,
                child_uid=global_xrf_uid,
                tenant_id=TENANT_A,
                approved_global_link=True,
            )
            > 0
        )

    with _transaction(pg_instance, dag_rls_schema, tenant_id=TENANT_B) as cursor:
        tenant_b_child, _, _ = _insert_instance(
            cursor,
            template_uid=message_template,
            tenant_id=TENANT_B,
            name="tenant B hidden child",
        )

    hidden_message = _lineage_rejection_message(
        pg_instance,
        dag_rls_schema,
        parent_uid=tenant_a_parent,
        child_uid=tenant_b_child,
    )
    nonexistent_message = _lineage_rejection_message(
        pg_instance,
        dag_rls_schema,
        parent_uid=tenant_a_parent,
        child_uid=9_223_372_036_854_775_000,
    )
    assert (
        hidden_message
        == nonexistent_message
        == ("TapDB lineage endpoints are unavailable in the current scope")
    )

    with pytest.raises(psycopg2.Error, match="unavailable in the current scope"):
        with _transaction(pg_instance, dag_rls_schema, tenant_id=TENANT_A) as cursor:
            _insert_lineage(
                cursor,
                parent_uid=tenant_a_parent,
                child_uid=global_ordinary_uid,
                tenant_id=TENANT_A,
                approved_global_link=True,
            )


def test_dag_rls_pg_scopes_and_freezes_legacy_outbox_mapping(
    pg_instance, dag_rls_schema
) -> None:
    with _transaction(
        pg_instance,
        dag_rls_schema,
        tenant_id=None,
        allow_global_rows=True,
    ) as cursor:
        message_template = _insert_template(cursor)

    mapping_ids = {}
    for tenant_id, label in ((TENANT_A, "a"), (TENANT_B, "b")):
        with _transaction(pg_instance, dag_rls_schema, tenant_id=tenant_id) as cursor:
            message_uid, message_euid, message_euid_seq = _insert_instance(
                cursor,
                template_uid=message_template,
                tenant_id=tenant_id,
                name=f"tenant {label} message",
            )
            cursor.execute(
                """
                INSERT INTO outbox_event (
                    message_uid, tenant_id, destination, dedupe_key
                ) VALUES (%s, %s, %s, %s) RETURNING id
                """,
                (
                    message_uid,
                    str(tenant_id),
                    f"https://{label}.example",
                    f"dedupe-{label}",
                ),
            )
            outbox_id = int(cursor.fetchone()[0])
            cursor.execute(
                """
                INSERT INTO tapdb_legacy_outbox_mapping (
                    old_outbox_id, old_event_id, message_uid,
                    message_euid, message_euid_seq, source_sha256
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    outbox_id,
                    str(uuid.uuid4()),
                    message_uid,
                    message_euid,
                    message_euid_seq,
                    label * 64,
                ),
            )
            mapping_ids[tenant_id] = outbox_id

    for tenant_id in (TENANT_A, TENANT_B):
        with _transaction(pg_instance, dag_rls_schema, tenant_id=tenant_id) as cursor:
            cursor.execute(
                "SELECT old_outbox_id FROM tapdb_legacy_outbox_mapping ORDER BY old_outbox_id"
            )
            assert cursor.fetchall() == [(mapping_ids[tenant_id],)]

    for statement in (
        "UPDATE tapdb_legacy_outbox_mapping SET source_sha256 = 'changed'",
        "DELETE FROM tapdb_legacy_outbox_mapping",
    ):
        with pytest.raises(psycopg2.Error, match="is immutable"):
            with _transaction(
                pg_instance, dag_rls_schema, tenant_id=TENANT_A
            ) as cursor:
                cursor.execute(statement)
