"""Shared provisioning and scope-limited runtime mapping on actual TapDB schema."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool
from typer.testing import CliRunner

from daylily_tapdb.cli import app
from daylily_tapdb.sequences import (
    SequenceProtectionError,
    capture_runtime_sequence_bindings,
    ensure_instance_prefix_sequence,
)


@pytest.fixture(scope="module")
def provisioned_pg(pg_instance):
    runner = CliRunner()
    for command in (
        ["db", "schema", "apply"],
        ["db", "data", "seed", "--skip-existing"],
    ):
        result = runner.invoke(
            app, ["--config", str(pg_instance["config_path"]), *command]
        )
        assert result.exit_code == 0, result.output
    return pg_instance


@pytest.fixture
def provision(provisioned_pg):
    pg_instance = provisioned_pg
    engine = create_engine(
        pg_instance["operator_dsn"],
        isolation_level="REPEATABLE READ",
        poolclass=NullPool,
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    target = {
        "engine_type": "local",
        "host": "localhost",
        "port": pg_instance["port"],
        "database": pg_instance["database"],
        "schema_name": pg_instance["schema_name"],
        "config_identity": str(pg_instance["config_path"]),
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
    }
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            yield connection, target
            transaction.rollback()
    finally:
        engine.dispose()


def test_provision_creates_catalog_mapping_and_preserves_safe_existing_state(provision):
    connection, target = provision
    with Session(bind=connection) as session:
        ensure_instance_prefix_sequence(session, "ABC")
    assert (
        connection.execute(
            text("SELECT obj_description('abc_instance_seq'::regclass,'pg_class')")
        ).scalar_one()
        == "tapdb-prefix-binding/v1:ABC"
    )
    before = connection.execute(
        text("SELECT last_value,is_called FROM abc_instance_seq")
    ).one()
    ensure_instance_prefix_sequence(connection, "ABC")
    assert (
        connection.execute(
            text("SELECT last_value,is_called FROM abc_instance_seq")
        ).one()
        == before
    )
    assert tuple(before) == (1, False)


def test_provision_shared_arithmetic_handles_nonunit_cache_and_every_euid_relation(
    provision,
):
    connection, _ = provision
    connection.execute(
        text(
            "CREATE SEQUENCE abc_instance_seq START 2 MINVALUE 2 MAXVALUE 101 INCREMENT 3 CACHE 4"
        )
    )
    connection.execute(
        text("CREATE TABLE extra_identity_history(euid_prefix text,euid_seq bigint)")
    )
    connection.execute(text("INSERT INTO extra_identity_history VALUES('ABC',20)"))
    connection.execute(text("SELECT setval('abc_instance_seq',23,false)"))
    ensure_instance_prefix_sequence(connection, "ABC")
    assert tuple(
        connection.execute(
            text("SELECT last_value,is_called FROM abc_instance_seq")
        ).one()
    ) == (23, False)
    connection.execute(text("SELECT setval('abc_instance_seq',20,false)"))
    with pytest.raises(SequenceProtectionError, match="receipt-bound"):
        ensure_instance_prefix_sequence(connection, "ABC")
    assert tuple(
        connection.execute(
            text("SELECT last_value,is_called FROM abc_instance_seq")
        ).one()
    ) == (20, False)


@pytest.mark.parametrize(
    "change",
    ["public", "unmanaged", "partial", "cycle", "owned_column", "comment", "temporary"],
)
def test_provision_refuses_unsupported_scope_or_generator_without_repair(
    provision, change
):
    connection, _ = provision
    if change == "public":
        connection.execute(text("SET LOCAL search_path=public"))
    elif change == "unmanaged":
        connection.execute(text("CREATE SCHEMA empty_scope"))
        connection.execute(text("SET LOCAL search_path=empty_scope"))
    elif change == "partial":
        connection.execute(text("CREATE TABLE incomplete_identity(euid_prefix text)"))
    elif change == "owned_column":
        connection.execute(text("CREATE TABLE extra_owned (id bigint)"))
        connection.execute(
            text("CREATE SEQUENCE abc_instance_seq OWNED BY extra_owned.id")
        )
    elif change == "temporary":
        connection.execute(text("CREATE UNLOGGED SEQUENCE abc_instance_seq"))
    else:
        connection.execute(text("CREATE SEQUENCE abc_instance_seq"))
        if change == "cycle":
            connection.execute(text("ALTER SEQUENCE abc_instance_seq CYCLE"))
        else:
            connection.execute(
                text(
                    "COMMENT ON SEQUENCE abc_instance_seq IS 'unrelated preserved owner annotation'"
                )
            )
    with pytest.raises(SequenceProtectionError):
        ensure_instance_prefix_sequence(connection, "ABC")


def test_runtime_candidates_scope_prefix_evidence_and_do_not_read_unknown_extras(
    provision,
):
    connection, target = provision
    connection.execute(
        text("CREATE TABLE unknown_extra (id bigint GENERATED ALWAYS AS IDENTITY)")
    )
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(connection, "before_cursor_execute", record)
    try:
        bindings = capture_runtime_sequence_bindings(
            connection,
            schema_name=target["schema_name"],
            target=target,
            managed_tables=[
                "generic_template",
                "generic_instance",
                "generic_instance_lineage",
                "audit_log",
                "tapdb_identity_prefix_config",
            ],
        )
    finally:
        event.remove(connection, "before_cursor_execute", record)
    assert "generic_instance_uid_seq" in bindings
    assert "unknown_extra_id_seq" not in bindings
    assert not any(
        "last_value" in sql and '"unknown_extra_id_seq"' in sql for sql in statements
    )
    scoped = [
        item
        for value in bindings.values()
        for item in value["mapping"].get("evidence", [])
        if item["source"] in {"stored_rows", "template_binding", "identity_binding"}
    ]
    assert scoped
    assert all(
        item["scope"] == {"domain_code": "Z", "owner_repo_name": "daylily-tapdb"}
        for item in scoped
    )


def test_runtime_candidate_scope_never_claims_another_owner_prefix_rows(provision):
    connection, target = provision
    bindings = capture_runtime_sequence_bindings(
        connection,
        schema_name=target["schema_name"],
        target={**target, "owner_repo_name": "different-explicit-repo"},
        managed_tables=[
            "generic_template",
            "generic_instance",
            "tapdb_identity_prefix_config",
        ],
    )
    assert all(
        not any(
            item["source"] in {"stored_rows", "template_binding", "identity_binding"}
            for item in value["mapping"].get("evidence", [])
        )
        for value in bindings.values()
    )


def test_refused_new_generator_rolls_back_only_its_provisioning_savepoint(provision):
    connection, _ = provision
    connection.execute(
        text("CREATE TABLE extra_identity_history(euid_prefix text,euid_seq bigint)")
    )
    connection.execute(text("INSERT INTO extra_identity_history VALUES('ABC',20)"))
    with pytest.raises(SequenceProtectionError, match="receipt-bound"):
        ensure_instance_prefix_sequence(connection, "ABC")
    assert (
        connection.execute(text("SELECT to_regclass('abc_instance_seq')")).scalar_one()
        is None
    )
    assert (
        connection.execute(
            text("SELECT euid_seq FROM extra_identity_history")
        ).scalar_one()
        == 20
    )
    assert connection.in_transaction()
