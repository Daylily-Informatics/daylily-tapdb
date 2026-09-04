from __future__ import annotations

import json
import shutil
import subprocess
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from typer.testing import CliRunner

import daylily_tapdb.cli.db as db_mod
import daylily_tapdb.migration_identity as migration_identity
from daylily_tapdb.advisory_locks import (
    AdvisoryLockTimeoutError,
    acquire_transaction_advisory_lock,
)
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.factory import (
    IdentityClaimOutcome,
    IdentityScope,
    InstanceFactory,
)
from daylily_tapdb.migration_identity import (
    apply_migration_preflight,
    build_migration_preflight,
)
from daylily_tapdb.sequences import ensure_instance_prefix_sequence
from daylily_tapdb.templates.manager import TemplateManager

runner = CliRunner()


def _set_operator_context(connection, schema_name: str, username: str) -> None:
    connection.execute(
        text(
            "SELECT set_config('search_path', :schema, true), "
            "set_config('session.current_config_identity', :schema, true), "
            "set_config('session.current_schema_name', :schema, true), "
            "set_config('session.current_domain_code', 'Z', true), "
            "set_config('session.current_owner_repo_name', "
            "'daylily-tapdb', true), "
            "set_config('session.current_tenant_id', '', true), "
            "set_config('session.current_username', :username, true), "
            "set_config('session.allow_global_rows', 'true', true)"
        ),
        {"schema": schema_name, "username": username},
    )


def _connection(pg_instance, app_username: str) -> TAPDBConnection:
    runtime_user = pg_instance["runtime_user"]
    runtime_dsn = (
        f"postgresql://{runtime_user}:@localhost:{pg_instance['port']}/"
        f"{pg_instance['database']}"
    )
    return TAPDBConnection(
        db_url=runtime_dsn,
        db_user=runtime_user,
        app_username=app_username,
        domain_code="Z",
        owner_repo_name="daylily-tapdb",
        schema_name=pg_instance["schema_name"],
        engine_type="local",
        allow_global_rows=True,
        config_identity=str(pg_instance["config_path"]),
    )


def _tenant_connection(
    pg_instance,
    *,
    role: str,
    tenant_id: uuid.UUID,
    config_identity: str,
    app_username: str,
) -> TAPDBConnection:
    return TAPDBConnection(
        db_url=(
            f"postgresql://{role}:@localhost:{pg_instance['port']}/"
            f"{pg_instance['database']}"
        ),
        db_user=role,
        app_username=app_username,
        domain_code="Z",
        owner_repo_name="daylily-tapdb",
        schema_name=pg_instance["schema_name"],
        tenant_id=str(tenant_id),
        allow_global_rows=True,
        config_identity=config_identity,
        engine_type="local",
    )


def _migration_target(pg_instance) -> dict[str, str]:
    return {
        "engine_type": "local",
        "host": "localhost",
        "port": str(pg_instance["port"]),
        "database": pg_instance["database"],
        "schema_name": pg_instance["schema_name"],
        "config_identity": str(pg_instance["config_path"]),
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
    }


def _seed_owner_message_template(connection, *, owner: str) -> int:
    connection.execute(
        text(
            "SELECT set_config('session.current_owner_repo_name', :owner, true), "
            "set_config('session.current_username', 'migration:owner-template', true)"
        ),
        {"owner": owner},
    )
    connection.execute(
        text(
            "INSERT INTO tapdb_identity_prefix_config "
            "(entity, domain_code, issuer_app_code, prefix) VALUES "
            "('generic_template', 'Z', :owner, 'TPX'), "
            "('audit_log', 'Z', :owner, 'ADT')"
        ),
        {"owner": owner},
    )
    template_uid = connection.execute(
        text(
            "INSERT INTO generic_template ("
            "name, polymorphic_discriminator, category, type, subtype, version, "
            "instance_prefix, instance_polymorphic_identity, json_addl, "
            "validator_ref, json_addl_schema, bstatus, is_singleton, is_deleted"
            ") SELECT name, polymorphic_discriminator, category, type, subtype, "
            "version, instance_prefix, instance_polymorphic_identity, json_addl, "
            "validator_ref, json_addl_schema, bstatus, is_singleton, is_deleted "
            "FROM generic_template WHERE domain_code = 'Z' "
            "AND issuer_app_code = 'daylily-tapdb' AND category = 'message' "
            "AND type = 'webhook' AND subtype = 'event' AND version = '1.0' "
            "RETURNING uid"
        )
    ).scalar_one()
    connection.execute(
        text(
            "SELECT set_config('session.current_owner_repo_name', "
            "'daylily-tapdb', true)"
        )
    )
    return int(template_uid)


@pytest.fixture(scope="module", autouse=True)
def _schema_and_templates(pg_instance):
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    applied = runner.invoke(app, ["db", "schema", "apply"])
    assert applied.exit_code == 0, applied.output
    seeded = runner.invoke(app, ["db", "data", "seed"])
    assert seeded.exit_code == 0, seeded.output
    # pg_instance deliberately provisions its database owner as
    # NOSUPERUSER/NOBYPASSRLS; exercise runtime claims with that role rather
    # than manufacturing a privileged test-only bypass.
    pg_instance["runtime_user"] = pg_instance["user"]
    yield
    clear_cli_context()


def test_postgresql_separate_sessions_claim_one_global_identity(pg_instance):
    barrier = threading.Barrier(2)
    identity_key = "labcore:sequencing_run:<persisted-euid>"

    def claim(index: int) -> tuple[int, IdentityClaimOutcome, object]:
        with _connection(pg_instance, f"identity-claim-{index}") as connection:
            with connection.session_scope(commit=True) as session:
                factory = InstanceFactory(TemplateManager(), domain_code="Z")
                barrier.wait(timeout=5)
                result = factory.claim_instance_by_identity(
                    session,
                    template_code="message/webhook/event/1.0/",
                    identity_key=identity_key,
                    name="Labcore sequencing run",
                    scope=IdentityScope.GLOBAL,
                    properties={"source": "labcore"},
                    command_evidence={"command": "register-run"},
                )
                return result.instance.uid, result.outcome, result.instance.tenant_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(claim, range(2)))

    assert {row[0] for row in results} == {results[0][0]}
    assert sorted(row[1] for row in results) == sorted(
        [IdentityClaimOutcome.CREATED, IdentityClaimOutcome.EXISTING]
    )
    assert {row[2] for row in results} == {None}


def test_postgresql_tenant_identity_separates_tenants_and_serializes_same_tenant(
    pg_instance,
):
    suffix = uuid.uuid4().hex[:10]
    tenants = [uuid.uuid4(), uuid.uuid4()]
    roles = [f"tapdb_claim_tenant_a_{suffix}", f"tapdb_claim_tenant_b_{suffix}"]
    config_identities = [
        f"pytest://natural-identity/tenant-a/{suffix}",
        f"pytest://natural-identity/tenant-b/{suffix}",
    ]
    tenant_identity_keys: list[str] = []
    operator = create_engine(pg_instance["operator_dsn"])
    created_roles: list[str] = []
    try:
        with operator.begin() as connection:
            _set_operator_context(
                connection,
                pg_instance["schema_name"],
                "pytest:natural-identity-principal-fixture",
            )
            for role, tenant_id, config_identity in zip(
                roles, tenants, config_identities, strict=True
            ):
                connection.exec_driver_sql(
                    f'CREATE ROLE "{role}" LOGIN NOSUPERUSER NOBYPASSRLS '
                    "NOCREATEDB NOCREATEROLE NOREPLICATION"
                )
                created_roles.append(role)
                connection.exec_driver_sql(
                    f'GRANT CONNECT ON DATABASE "{pg_instance["database"]}" TO "{role}"'
                )
                connection.exec_driver_sql(
                    f'GRANT USAGE ON SCHEMA "{pg_instance["schema_name"]}" TO "{role}"'
                )
                connection.exec_driver_sql(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
                    f'"{pg_instance["schema_name"]}" TO "{role}"'
                )
                connection.exec_driver_sql(
                    "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA "
                    f'"{pg_instance["schema_name"]}" TO "{role}"'
                )
                connection.exec_driver_sql(
                    f'REVOKE ALL ON TABLE "{pg_instance["schema_name"]}".'
                    f'tapdb_runtime_principal_scope FROM "{role}"'
                )
                connection.execute(
                    text(
                        "INSERT INTO tapdb_runtime_principal_scope ("
                        "role_name, config_identity, schema_name, domain_code, "
                        "issuer_app_code, tenant_id, allow_global_rows) VALUES ("
                        ":role, :config_identity, :schema_name, 'Z', "
                        "'daylily-tapdb', :tenant_id, true)"
                    ),
                    {
                        "role": role,
                        "config_identity": config_identity,
                        "schema_name": pg_instance["schema_name"],
                        "tenant_id": str(tenant_id),
                    },
                )
            posture = connection.execute(
                text(
                    "SELECT rolname, rolsuper, rolbypassrls, rolcreatedb, "
                    "rolcreaterole, rolreplication FROM pg_roles "
                    "WHERE rolname IN (:role_a, :role_b) ORDER BY rolname"
                ),
                {"role_a": roles[0], "role_b": roles[1]},
            ).all()
            assert len(posture) == 2
            assert all(not any(row[1:]) for row in posture)

        barrier = threading.Barrier(2)
        identity_key = f"migration-test:cross-tenant:{uuid.uuid4()}"
        tenant_identity_keys.append(identity_key)

        def claim(index: int) -> tuple[int, IdentityClaimOutcome, uuid.UUID]:
            with _tenant_connection(
                pg_instance,
                role=roles[index],
                tenant_id=tenants[index],
                config_identity=config_identities[index],
                app_username=f"pytest:tenant-claim-{index}",
            ) as runtime:
                with runtime.session_scope(commit=True) as session:
                    barrier.wait(timeout=5)
                    result = InstanceFactory(
                        TemplateManager(), domain_code="Z"
                    ).claim_instance_by_identity(
                        session,
                        template_code="message/webhook/event/1.0/",
                        identity_key=identity_key,
                        name="Tenant-scoped claim",
                        scope=IdentityScope.TENANT,
                        tenant_id=tenants[index],
                        properties={"stable": True},
                    )
                    return (
                        result.instance.uid,
                        result.outcome,
                        result.instance.tenant_id,
                    )

        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(claim, range(2)))

        assert len({claim[0] for claim in claims}) == 2
        assert {claim[1] for claim in claims} == {IdentityClaimOutcome.CREATED}
        assert {claim[2] for claim in claims} == set(tenants)
        with operator.begin() as connection:
            _set_operator_context(
                connection,
                pg_instance["schema_name"],
                "pytest:natural-identity-principal-proof",
            )
            winners = connection.execute(
                text(
                    "SELECT uid, tenant_id, json_addl FROM generic_instance "
                    "WHERE identity_key = :identity_key ORDER BY tenant_id"
                ),
                {"identity_key": identity_key},
            ).all()
            assert {row.uid for row in winners} == {claim[0] for claim in claims}
            assert {row.tenant_id for row in winners} == set(tenants)
            assert all(
                row.json_addl["identity_claim"]["scope"] == "tenant"
                and uuid.UUID(row.json_addl["identity_claim"]["tenant_id"])
                == row.tenant_id
                for row in winners
            )

        same_tenant_key = f"migration-test:same-tenant:{uuid.uuid4()}"
        tenant_identity_keys.append(same_tenant_key)
        same_tenant_barrier = threading.Barrier(2)

        def same_tenant_claim(index: int) -> tuple[int, IdentityClaimOutcome]:
            with _tenant_connection(
                pg_instance,
                role=roles[0],
                tenant_id=tenants[0],
                config_identity=config_identities[0],
                app_username=f"pytest:same-tenant-claim-{index}",
            ) as runtime:
                with runtime.session_scope(commit=True) as session:
                    same_tenant_barrier.wait(timeout=5)
                    result = InstanceFactory(
                        TemplateManager(), domain_code="Z"
                    ).claim_instance_by_identity(
                        session,
                        template_code="message/webhook/event/1.0/",
                        identity_key=same_tenant_key,
                        name="Same tenant concurrent claim",
                        scope=IdentityScope.TENANT,
                        tenant_id=tenants[0],
                        properties={"stable": True},
                    )
                    return result.instance.uid, result.outcome

        with ThreadPoolExecutor(max_workers=2) as executor:
            same_tenant_claims = list(executor.map(same_tenant_claim, range(2)))

        assert len({claim[0] for claim in same_tenant_claims}) == 1
        assert {claim[1] for claim in same_tenant_claims} == {
            IdentityClaimOutcome.CREATED,
            IdentityClaimOutcome.EXISTING,
        }
    finally:
        if created_roles:
            with operator.begin() as connection:
                _set_operator_context(
                    connection,
                    pg_instance["schema_name"],
                    "pytest:natural-identity-principal-cleanup",
                )
                connection.execute(
                    text(
                        "DELETE FROM tapdb_runtime_principal_scope "
                        "WHERE role_name IN (:role_a, :role_b)"
                    ),
                    {
                        "role_a": roles[0],
                        "role_b": roles[1],
                    },
                )
                # This session-scoped PostgreSQL fixture is shared with tests
                # that reconstruct older releases. Physically remove only the
                # tenant rows minted by this test so a later historical
                # migration replay is not accidentally tested as a downgrade.
                if tenant_identity_keys:
                    connection.exec_driver_sql(
                        "ALTER TABLE generic_instance DISABLE TRIGGER "
                        "soft_delete_generic_instance"
                    )
                    connection.execute(
                        text(
                            "DELETE FROM generic_instance "
                            "WHERE identity_key = ANY(:identity_keys)"
                        ),
                        {"identity_keys": tenant_identity_keys},
                    )
                    connection.exec_driver_sql(
                        "ALTER TABLE generic_instance ENABLE TRIGGER "
                        "soft_delete_generic_instance"
                    )
                for role in created_roles:
                    connection.exec_driver_sql(f'DROP OWNED BY "{role}"')
                    connection.exec_driver_sql(f'DROP ROLE "{role}"')
        operator.dispose()


def test_postgresql_different_natural_identity_keys_proceed_independently(pg_instance):
    barrier = threading.Barrier(2)
    keys = [f"migration-test:independent:{uuid.uuid4()}" for _ in range(2)]

    def claim(index: int) -> tuple[int, IdentityClaimOutcome]:
        with _connection(pg_instance, f"identity-independent-{index}") as connection:
            with connection.session_scope(commit=True) as session:
                barrier.wait(timeout=5)
                result = InstanceFactory(
                    TemplateManager(), domain_code="Z"
                ).claim_instance_by_identity(
                    session,
                    template_code="message/webhook/event/1.0/",
                    identity_key=keys[index],
                    name=f"Independent identity {index}",
                    scope=IdentityScope.GLOBAL,
                    properties={"index": index},
                )
                return result.instance.uid, result.outcome

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(claim, range(2)))

    assert {row[1] for row in results} == {IdentityClaimOutcome.CREATED}
    assert len({row[0] for row in results}) == 2


def test_postgresql_rolled_back_first_claim_permits_second_create(pg_instance):
    first_inserted = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()
    identity_key = f"migration-test:rollback-winner:{uuid.uuid4()}"
    claim_args = {
        "template_code": "message/webhook/event/1.0/",
        "identity_key": identity_key,
        "name": "Rollback claim",
        "scope": IdentityScope.GLOBAL,
        "properties": {"stable": True},
    }

    def rolled_back_claim() -> int:
        with _connection(pg_instance, "identity-rollback-first") as connection:
            with connection.session_scope(commit=False) as session:
                result = InstanceFactory(
                    TemplateManager(), domain_code="Z"
                ).claim_instance_by_identity(session, **claim_args)
                first_inserted.set()
                assert release_first.wait(timeout=5)
                return result.instance.uid

    def committed_claim() -> tuple[int, IdentityClaimOutcome]:
        assert first_inserted.wait(timeout=5)
        second_started.set()
        with _connection(pg_instance, "identity-rollback-second") as connection:
            with connection.session_scope(commit=True) as session:
                result = InstanceFactory(
                    TemplateManager(), domain_code="Z"
                ).claim_instance_by_identity(session, **claim_args)
                return result.instance.uid, result.outcome

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(rolled_back_claim)
        assert first_inserted.wait(timeout=5)
        second = executor.submit(committed_claim)
        assert second_started.wait(timeout=5)
        assert not second.done()
        release_first.set()
        rolled_back_uid = first.result(timeout=5)
        committed_uid, outcome = second.result(timeout=5)

    assert outcome is IdentityClaimOutcome.CREATED
    assert committed_uid != rolled_back_uid
    with _connection(pg_instance, "identity-rollback-proof") as connection:
        with connection.session_scope(commit=False) as session:
            rows = session.execute(
                text(
                    "SELECT uid FROM generic_instance WHERE identity_key = :identity_key"
                ),
                {"identity_key": identity_key},
            ).scalars()
            assert list(rows) == [committed_uid]


def test_postgresql_committed_claim_becomes_visible_to_waiting_replay(pg_instance):
    first_inserted = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()
    identity_key = f"migration-test:committed-winner:{uuid.uuid4()}"
    claim_args = {
        "template_code": "message/webhook/event/1.0/",
        "identity_key": identity_key,
        "name": "Committed claim",
        "scope": IdentityScope.GLOBAL,
        "properties": {"stable": True},
    }

    def committed_first() -> tuple[int, IdentityClaimOutcome]:
        with _connection(pg_instance, "identity-commit-first") as connection:
            with connection.session_scope(commit=True) as session:
                result = InstanceFactory(
                    TemplateManager(), domain_code="Z"
                ).claim_instance_by_identity(session, **claim_args)
                first_inserted.set()
                assert release_first.wait(timeout=5)
                return result.instance.uid, result.outcome

    def waiting_replay() -> tuple[int, IdentityClaimOutcome]:
        assert first_inserted.wait(timeout=5)
        second_started.set()
        with _connection(pg_instance, "identity-commit-second") as connection:
            with connection.session_scope(commit=True) as session:
                result = InstanceFactory(
                    TemplateManager(), domain_code="Z"
                ).claim_instance_by_identity(session, **claim_args)
                return result.instance.uid, result.outcome

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(committed_first)
        assert first_inserted.wait(timeout=5)
        second = executor.submit(waiting_replay)
        assert second_started.wait(timeout=5)
        assert not second.done()
        release_first.set()
        winner_uid, winner_outcome = first.result(timeout=5)
        replay_uid, replay_outcome = second.result(timeout=5)

    assert winner_outcome is IdentityClaimOutcome.CREATED
    assert replay_outcome is IdentityClaimOutcome.EXISTING
    assert replay_uid == winner_uid


def test_postgresql_divergent_replay_returns_existing_winner_for_client_comparison(
    pg_instance,
):
    identity_key = f"migration-test:divergent:{uuid.uuid4()}"
    common = {
        "template_code": "message/webhook/event/1.0/",
        "identity_key": identity_key,
        "name": "Immutable identity claim",
        "scope": IdentityScope.GLOBAL,
        "properties": {"source": "first-request"},
    }
    with _connection(pg_instance, "identity-divergent-create") as connection:
        with connection.session_scope(commit=True) as session:
            created = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).claim_instance_by_identity(session, **common)
            original_uid = created.instance.uid

    with _connection(pg_instance, "identity-divergent-replay") as connection:
        with connection.session_scope(commit=False) as session:
            replay = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).claim_instance_by_identity(
                session,
                **{**common, "properties": {"source": "different-request"}},
            )
            assert replay.outcome is IdentityClaimOutcome.EXISTING
            assert replay.instance.uid == original_uid
            assert replay.instance.json_addl["properties"]["source"] == "first-request"
            rows = (
                session.execute(
                    text(
                        "SELECT uid FROM generic_instance "
                        "WHERE identity_key = :identity_key"
                    ),
                    {"identity_key": identity_key},
                )
                .scalars()
                .all()
            )
            assert rows == [original_uid]


def test_postgresql_existing_replay_does_not_damage_caller_transaction(pg_instance):
    identity_key = f"migration-test:outer-transaction:{uuid.uuid4()}"
    original = {
        "template_code": "message/webhook/event/1.0/",
        "identity_key": identity_key,
        "name": "Outer transaction identity",
        "scope": IdentityScope.GLOBAL,
        "properties": {"source": "original"},
    }
    with _connection(pg_instance, "identity-outer-create") as connection:
        with connection.session_scope(commit=True) as session:
            InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).claim_instance_by_identity(session, **original)

    with _connection(pg_instance, "identity-outer-divergence") as connection:
        with connection.session_scope(commit=True) as session:
            unrelated = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).create_instance(
                session,
                "message/webhook/event/1.0/",
                "Caller-owned unrelated work",
                create_children=False,
            )
            unrelated_uid = unrelated.uid
            replay = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).claim_instance_by_identity(
                session,
                **{**original, "properties": {"source": "client-compares-this"}},
            )
            assert replay.outcome is IdentityClaimOutcome.EXISTING
            assert replay.instance.json_addl["properties"]["source"] == "original"
            assert session.execute(text("SELECT 1")).scalar_one() == 1

    with _connection(pg_instance, "identity-outer-proof") as connection:
        with connection.session_scope(commit=False) as session:
            assert (
                session.execute(
                    text("SELECT count(*) FROM generic_instance WHERE uid = :uid"),
                    {"uid": unrelated_uid},
                ).scalar_one()
                == 1
            )


def test_postgresql_soft_deleted_identity_is_replayed_not_reassigned(pg_instance):
    identity_key = f"migration-test:soft-deleted:{uuid.uuid4()}"
    claim_args = {
        "template_code": "message/webhook/event/1.0/",
        "identity_key": identity_key,
        "name": "Soft-deleted identity claim",
        "scope": IdentityScope.GLOBAL,
        "properties": {"source": "preservation-test"},
    }
    with _connection(pg_instance, "identity-soft-delete-create") as connection:
        with connection.session_scope(commit=True) as session:
            created = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).claim_instance_by_identity(session, **claim_args)
            original_uid = created.instance.uid
            created.instance.is_deleted = True

    with _connection(pg_instance, "identity-soft-delete-replay") as connection:
        with connection.session_scope(commit=False) as session:
            replay = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).claim_instance_by_identity(session, **claim_args)
            assert replay.outcome is IdentityClaimOutcome.EXISTING
            assert replay.instance.uid == original_uid
            assert replay.instance.is_deleted is True
            assert (
                session.execute(
                    text(
                        "SELECT count(*) FROM generic_instance "
                        "WHERE identity_key = :identity_key"
                    ),
                    {"identity_key": identity_key},
                ).scalar_one()
                == 1
            )


def test_postgresql_separate_session_advisory_lock_timeout_and_release(pg_instance):
    acquired = threading.Event()
    release = threading.Event()

    def holder() -> None:
        with _connection(pg_instance, "lock-holder") as connection:
            with connection.session_scope(commit=True) as session:
                receipt = acquire_transaction_advisory_lock(
                    session, "tapdb.test", "same-object"
                )
                assert receipt.acquired is True
                assert "same-object" not in receipt.lock_fingerprint
                acquired.set()
                assert release.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(holder)
        assert acquired.wait(timeout=5)
        with _connection(pg_instance, "lock-contender") as connection:
            with pytest.raises(AdvisoryLockTimeoutError):
                with connection.session_scope(commit=False) as session:
                    acquire_transaction_advisory_lock(
                        session,
                        "tapdb.test",
                        "same-object",
                        timeout_seconds=0.1,
                        poll_interval_seconds=0.01,
                    )
        release.set()
        future.result(timeout=5)

    with _connection(pg_instance, "lock-after-release") as connection:
        with connection.session_scope(commit=False) as session:
            receipt = acquire_transaction_advisory_lock(
                session,
                "tapdb.test",
                "same-object",
                timeout_seconds=0,
            )
            assert receipt.acquired is True


def test_postgresql_advisory_lock_serializes_same_key_but_not_different_keys(
    pg_instance,
):
    holder_acquired = threading.Event()
    waiter_attempting = threading.Event()
    waiter_acquired = threading.Event()
    release_holder = threading.Event()

    def holder() -> None:
        with _connection(pg_instance, "lock-serialization-holder") as connection:
            with connection.session_scope(commit=True) as session:
                acquire_transaction_advisory_lock(
                    session, "tapdb.test", "serialized-object"
                )
                holder_acquired.set()
                assert release_holder.wait(timeout=5)

    def waiter() -> None:
        assert holder_acquired.wait(timeout=5)
        with _connection(pg_instance, "lock-serialization-waiter") as connection:
            with connection.session_scope(commit=False) as session:
                waiter_attempting.set()
                acquire_transaction_advisory_lock(
                    session, "tapdb.test", "serialized-object"
                )
                waiter_acquired.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        holder_future = executor.submit(holder)
        waiter_future = executor.submit(waiter)
        assert holder_acquired.wait(timeout=5)
        assert waiter_attempting.wait(timeout=5)
        assert not waiter_acquired.wait(timeout=0.1)

        with _connection(pg_instance, "lock-independent-key") as connection:
            with connection.session_scope(commit=False) as session:
                independent = acquire_transaction_advisory_lock(
                    session,
                    "tapdb.test",
                    "different-object",
                    timeout_seconds=0,
                )
                assert independent.acquired is True

        release_holder.set()
        holder_future.result(timeout=5)
        waiter_future.result(timeout=5)
        assert waiter_acquired.is_set()


def test_postgresql_advisory_xact_lock_rollback_leaves_no_session_lock(pg_instance):
    with _connection(pg_instance, "lock-rollback-proof") as connection:
        with connection.session_scope(commit=False) as session:
            acquire_transaction_advisory_lock(session, "tapdb.test", "rollback-object")
            assert (
                session.execute(
                    text(
                        "SELECT count(*) FROM pg_locks "
                        "WHERE locktype = 'advisory' AND pid = pg_backend_pid()"
                    )
                ).scalar_one()
                == 1
            )

        with connection.session_scope(commit=False) as session:
            assert (
                session.execute(
                    text(
                        "SELECT count(*) FROM pg_locks "
                        "WHERE locktype = 'advisory' AND pid = pg_backend_pid()"
                    )
                ).scalar_one()
                == 0
            )
            replay = acquire_transaction_advisory_lock(
                session,
                "tapdb.test",
                "rollback-object",
                timeout_seconds=0,
            )
            assert replay.acquired is True


def test_cli_migration_preflight_apply_and_second_run_noop(pg_instance, tmp_path):
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    with _connection(pg_instance, "migration:sparse-soft-delete-fixture") as connection:
        with connection.session_scope(commit=True) as session:
            factory = InstanceFactory(TemplateManager(), domain_code="Z")
            first = factory.create_instance(
                session,
                "message/webhook/event/1.0/",
                "Sparse identity predecessor",
                create_children=False,
            )
            first_uid = first.uid
            first_euid_seq = first.euid_seq
        with connection.session_scope(commit=True) as session:
            session.execute(text("SELECT nextval('generic_instance_uid_seq')"))
            session.execute(text("SELECT nextval('msg_instance_seq')"))
        with connection.session_scope(commit=True) as session:
            second = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).create_instance(
                session,
                "message/webhook/event/1.0/",
                "Sparse soft-deleted identity successor",
                create_children=False,
            )
            second.is_deleted = True
            second_uid = second.uid
            second_euid_seq = second.euid_seq
    assert second_uid == first_uid + 2
    assert second_euid_seq == first_euid_seq + 2

    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:test-preflight"
        )
        before = connection.execute(
            text(
                "SELECT uid, euid, euid_prefix, euid_seq, machine_uuid, "
                "identity_key, template_uid, created_dt FROM generic_instance "
                "ORDER BY uid"
            )
        ).all()
        sparse_before = connection.execute(
            text(
                "SELECT uid, euid, euid_prefix, euid_seq, machine_uuid, "
                "identity_key, tenant_id, domain_code, issuer_app_code, "
                "template_uid, is_deleted, created_dt, modified_dt "
                "FROM generic_instance WHERE uid IN (:first_uid, :second_uid) "
                "ORDER BY uid"
            ),
            {"first_uid": first_uid, "second_uid": second_uid},
        ).all()
        connection.execute(
            text(
                "DELETE FROM _tapdb_migrations WHERE filename IN "
                "('20260902_010000_natural_identity_and_owner_uniqueness.sql', "
                "'20260902_010100_legacy_outbox_message_conversion.sql')"
            )
        )

    preflight_path = (tmp_path / "preflight.json").resolve()
    result_path = (tmp_path / "result.json").resolve()
    dry = runner.invoke(
        app,
        ["db", "schema", "migrate", "--dry-run", "--receipt", str(preflight_path)],
    )
    assert dry.exit_code == 0, dry.output
    preflight = json.loads(preflight_path.read_text(encoding="utf-8"))
    assert len(preflight["pending_migrations"]) == 2
    assert preflight["target"]["config_identity"] == str(pg_instance["config_path"])

    applied = runner.invoke(
        app,
        [
            "db",
            "schema",
            "migrate",
            "--apply",
            "--preflight-receipt",
            str(preflight_path),
            "--receipt",
            str(result_path),
        ],
    )
    assert applied.exit_code == 0, applied.output
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert result["status"] == "applied"
    assert result["sequence_pre_state"] == result["sequence_post_state"]

    with engine.connect() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:test-postflight"
        )
        after = connection.execute(
            text(
                "SELECT uid, euid, euid_prefix, euid_seq, machine_uuid, "
                "identity_key, template_uid, created_dt FROM generic_instance "
                "ORDER BY uid"
            )
        ).all()
    assert after == before
    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:sparse-proof"
        )
        sparse_after = connection.execute(
            text(
                "SELECT uid, euid, euid_prefix, euid_seq, machine_uuid, "
                "identity_key, tenant_id, domain_code, issuer_app_code, "
                "template_uid, is_deleted, created_dt, modified_dt "
                "FROM generic_instance WHERE uid IN (:first_uid, :second_uid) "
                "ORDER BY uid"
            ),
            {"first_uid": first_uid, "second_uid": second_uid},
        ).all()
    assert sparse_after == sparse_before
    assert [row.is_deleted for row in sparse_after] == [False, True]

    noop_preflight = (tmp_path / "noop-preflight.json").resolve()
    noop_result = (tmp_path / "noop-result.json").resolve()
    assert (
        runner.invoke(
            app,
            ["db", "schema", "migrate", "--dry-run", "--receipt", str(noop_preflight)],
        ).exit_code
        == 0
    )
    replay = runner.invoke(
        app,
        [
            "db",
            "schema",
            "migrate",
            "--apply",
            "--preflight-receipt",
            str(noop_preflight),
            "--receipt",
            str(noop_result),
        ],
    )
    assert replay.exit_code == 0, replay.output
    assert json.loads(noop_result.read_text(encoding="utf-8"))["status"] == "no-op"


def test_migration_receipt_cannot_replay_under_different_config_identity(pg_instance):
    migrations_dir = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    target = _migration_target(pg_instance)
    changed_target = {
        **target,
        "config_identity": "pytest://different-config-identity",
    }
    engine = create_engine(pg_instance["operator_dsn"])
    with engine.connect() as connection:
        transaction = connection.begin()
        preflight = build_migration_preflight(
            connection,
            migrations_dir=migrations_dir,
            target=target,
        )
        assert preflight["target"]["config_identity"] == target["config_identity"]
        with pytest.raises(
            migration_identity.MigrationReceiptMismatchError,
            match="live target no longer matches",
        ):
            apply_migration_preflight(
                connection,
                migrations_dir=migrations_dir,
                preflight=preflight,
                target=changed_target,
            )
        transaction.rollback()
    engine.dispose()


def test_legacy_outbox_conversion_preserves_event_identity_and_mapping(
    pg_instance, tmp_path
):
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    event_id = uuid.uuid4()
    payload = {"run": "<persisted-euid>", "state": "ready"}
    delivered_dt = datetime(2026, 8, 31, 12, 34, 56, tzinfo=UTC)
    with _connection(pg_instance, "migration:mixed-outbox-fixture") as connection:
        with connection.session_scope(commit=True) as session:
            existing_message = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).create_instance(
                session,
                "message/webhook/event/1.0/",
                "Existing canonical outbox message",
                create_children=False,
            )
            existing_message_uid = existing_message.uid
    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:legacy-fixture"
        )
        connection.execute(
            text(
                "ALTER TABLE outbox_event ALTER COLUMN message_uid DROP NOT NULL; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS event_id UUID; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS event_type TEXT; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS aggregate_euid TEXT; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS payload JSONB; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS delivered_dt "
                "TIMESTAMP WITH TIME ZONE"
            )
        )
        existing_outbox_id = connection.execute(
            text(
                "INSERT INTO outbox_event ("
                "message_uid, tenant_id, domain_code, issuer_app_code, "
                "destination, dedupe_key, status, attempt_count, next_attempt_at, "
                "created_dt"
                ") VALUES ("
                ":message_uid, NULL, 'Z', 'daylily-tapdb', "
                "'svc://canonical', :dedupe_key, 'pending', 2, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP"
                ") RETURNING id"
            ),
            {
                "message_uid": existing_message_uid,
                "dedupe_key": f"canonical-{event_id}",
            },
        ).scalar_one()
        existing_before = (
            connection.execute(
                text("SELECT * FROM outbox_event WHERE id = :id"),
                {"id": existing_outbox_id},
            )
            .mappings()
            .one()
        )
        old_outbox_id = connection.execute(
            text(
                "INSERT INTO outbox_event ("
                "message_uid, event_id, tenant_id, domain_code, issuer_app_code, "
                "event_type, aggregate_euid, payload, destination, dedupe_key, "
                "status, attempt_count, next_attempt_at, created_dt, delivered_dt"
                ") VALUES ("
                "NULL, :event_id, NULL, 'Z', 'daylily-tapdb', "
                "'run.ready', '<persisted-euid>', CAST(:payload AS JSONB), "
                "'svc://consumer', :dedupe_key, 'pending', 0, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :delivered_dt"
                ") RETURNING id"
            ),
            {
                "event_id": event_id,
                "payload": json.dumps(payload),
                "dedupe_key": f"legacy-{event_id}",
                "delivered_dt": delivered_dt,
            },
        ).scalar_one()
        before = (
            connection.execute(
                text("SELECT * FROM outbox_event WHERE id = :id"),
                {"id": old_outbox_id},
            )
            .mappings()
            .one()
        )
        connection.execute(
            text(
                "DELETE FROM _tapdb_migrations WHERE filename = "
                "'20260902_010100_legacy_outbox_message_conversion.sql'"
            )
        )

    preflight_path = (tmp_path / "legacy-preflight.json").resolve()
    result_path = (tmp_path / "legacy-result.json").resolve()
    dry = runner.invoke(
        app,
        ["db", "schema", "migrate", "--dry-run", "--receipt", str(preflight_path)],
    )
    assert dry.exit_code == 0, dry.output
    applied = runner.invoke(
        app,
        [
            "db",
            "schema",
            "migrate",
            "--apply",
            "--preflight-receipt",
            str(preflight_path),
            "--receipt",
            str(result_path),
        ],
    )
    assert applied.exit_code == 0, applied.output
    result = json.loads(result_path.read_text(encoding="utf-8"))
    sequence_pre = {row["name"]: row for row in result["sequence_pre_state"]}
    sequence_post = {row["name"]: row for row in result["sequence_post_state"]}
    assert {
        name for name in sequence_pre if sequence_pre[name] != sequence_post[name]
    } == {"generic_instance_uid_seq", "msg_instance_seq"}, sorted(sequence_pre)

    with engine.connect() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:legacy-proof"
        )
        after = (
            connection.execute(
                text("SELECT * FROM outbox_event WHERE id = :id"),
                {"id": old_outbox_id},
            )
            .mappings()
            .one()
        )
        mapping = (
            connection.execute(
                text(
                    "SELECT * FROM tapdb_legacy_outbox_mapping "
                    "WHERE old_outbox_id = :id"
                ),
                {"id": old_outbox_id},
            )
            .mappings()
            .one()
        )
        message = (
            connection.execute(
                text(
                    "SELECT uid, euid, euid_seq, machine_uuid, tenant_id, "
                    "domain_code, issuer_app_code, json_addl, created_dt "
                    "FROM generic_instance WHERE uid = :uid"
                ),
                {"uid": mapping["message_uid"]},
            )
            .mappings()
            .one()
        )
        existing_after = (
            connection.execute(
                text("SELECT * FROM outbox_event WHERE id = :id"),
                {"id": existing_outbox_id},
            )
            .mappings()
            .one()
        )

    for column in (
        "id",
        "event_id",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "event_type",
        "aggregate_euid",
        "payload",
        "destination",
        "dedupe_key",
        "status",
        "attempt_count",
        "next_attempt_at",
        "last_error",
        "created_dt",
        "delivered_dt",
    ):
        assert after[column] == before[column]
    assert mapping["old_event_id"] == event_id
    assert mapping["message_uid"] == after["message_uid"] == message["uid"]
    assert mapping["message_euid"] == message["euid"]
    assert mapping["message_euid_seq"] == message["euid_seq"]
    assert len(mapping["source_sha256"]) == 64
    assert mapping["mapped_dt"] == before["created_dt"]
    assert existing_after == existing_before
    assert existing_after["message_uid"] == existing_message_uid
    assert message["machine_uuid"] == event_id
    assert message["json_addl"]["payload"] == payload
    assert (
        datetime.fromisoformat(message["json_addl"]["metadata"]["delivered_dt"])
        == delivered_dt
    )


def test_legacy_outbox_conversion_uses_exact_client_owner_core_template(
    pg_instance,
):
    owner = f"client-{uuid.uuid4().hex[:10]}"
    event_id = uuid.uuid4()
    migrations_dir = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    target = _migration_target(pg_instance)
    engine = create_engine(pg_instance["operator_dsn"])
    with engine.begin() as connection:
        _set_operator_context(
            connection,
            pg_instance["schema_name"],
            "migration:client-owner-fixture",
        )
        connection.execute(
            text(
                "ALTER TABLE outbox_event ALTER COLUMN message_uid DROP NOT NULL; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS event_id UUID; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS event_type TEXT; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS aggregate_euid TEXT; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS payload JSONB; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS delivered_dt "
                "TIMESTAMP WITH TIME ZONE"
            )
        )
        old_outbox_id = connection.execute(
            text(
                "INSERT INTO outbox_event ("
                "message_uid, event_id, tenant_id, domain_code, issuer_app_code, "
                "event_type, aggregate_euid, payload, destination, dedupe_key, "
                "status, attempt_count, next_attempt_at, created_dt"
                ") VALUES ("
                "NULL, :event_id, NULL, 'Z', :owner, 'client.event', "
                "'<persisted-euid>', '{\"client\": true}'::jsonb, "
                "'svc://client-owner', :dedupe_key, 'pending', 0, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP"
                ") RETURNING id"
            ),
            {
                "event_id": event_id,
                "owner": owner,
                "dedupe_key": f"client-owner-{event_id}",
            },
        ).scalar_one()
        connection.execute(
            text(
                "DELETE FROM _tapdb_migrations WHERE filename = "
                "'20260902_010100_legacy_outbox_message_conversion.sql'"
            )
        )

    with engine.connect() as connection:
        transaction = connection.begin()
        missing_template_preflight = build_migration_preflight(
            connection,
            migrations_dir=migrations_dir,
            target=target,
        )
        transaction.rollback()

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            with pytest.raises(
                Exception,
                match="requires exactly one active .* owner .* found 0",
            ):
                apply_migration_preflight(
                    connection,
                    migrations_dir=migrations_dir,
                    preflight=missing_template_preflight,
                    target=target,
                )
        finally:
            transaction.rollback()

    with engine.connect() as connection:
        transaction = connection.begin()
        after_failed_conversion = build_migration_preflight(
            connection,
            migrations_dir=migrations_dir,
            target=target,
        )
        transaction.rollback()

    assert after_failed_conversion["tables"] == missing_template_preflight["tables"]
    assert (
        after_failed_conversion["sequences"] == missing_template_preflight["sequences"]
    )

    with engine.begin() as connection:
        _set_operator_context(
            connection,
            pg_instance["schema_name"],
            "migration:client-owner-template",
        )
        client_template_uid = _seed_owner_message_template(connection, owner=owner)

    with engine.connect() as connection:
        transaction = connection.begin()
        preflight = build_migration_preflight(
            connection,
            migrations_dir=migrations_dir,
            target=target,
        )
        result = apply_migration_preflight(
            connection,
            migrations_dir=migrations_dir,
            preflight=preflight,
            target=target,
        )
        transaction.commit()

    with engine.begin() as connection:
        _set_operator_context(
            connection,
            pg_instance["schema_name"],
            "migration:client-owner-proof",
        )
        message = connection.execute(
            text(
                "SELECT gi.uid, gi.template_uid, gi.domain_code, gi.issuer_app_code, "
                "gi.machine_uuid, oe.message_uid "
                "FROM outbox_event oe JOIN generic_instance gi "
                "ON gi.uid = oe.message_uid WHERE oe.id = :id"
            ),
            {"id": old_outbox_id},
        ).one()
        mapping = connection.execute(
            text(
                "SELECT message_uid FROM tapdb_legacy_outbox_mapping "
                "WHERE old_outbox_id = :id"
            ),
            {"id": old_outbox_id},
        ).one()

    assert result.receipt["status"] == "applied"
    assert message.template_uid == client_template_uid
    assert message.domain_code == "Z"
    assert message.issuer_app_code == owner
    assert message.machine_uuid == event_id
    assert mapping.message_uid == message.message_uid == message.uid


def test_preflight_refuses_euid_generator_behind_assigned_identity(pg_instance):
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    migrations_dir = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    target = _migration_target(pg_instance)
    with engine.begin() as connection:
        _set_operator_context(
            connection,
            pg_instance["schema_name"],
            "migration:behind-sequence-fixture",
        )
        old_state = connection.execute(
            text("SELECT last_value, is_called FROM msg_instance_seq")
        ).one()
        assigned_max = connection.execute(
            text("SELECT max(euid_seq) FROM generic_instance WHERE euid_prefix = 'MSG'")
        ).scalar_one()
        assert assigned_max is not None
        connection.execute(
            text("SELECT setval('msg_instance_seq', :value, false)"),
            {"value": int(assigned_max)},
        )
        try:
            with pytest.raises(Exception, match="behind assigned identity values"):
                build_migration_preflight(
                    connection, migrations_dir=migrations_dir, target=target
                )
        finally:
            connection.execute(
                text("SELECT setval('msg_instance_seq', :value, :is_called)"),
                {"value": int(old_state[0]), "is_called": bool(old_state[1])},
            )


def test_restored_backup_migrates_to_identical_identity_and_sequence_evidence(
    pg_instance, tmp_path
):
    pg_dump = shutil.which("pg_dump")
    pg_restore = shutil.which("pg_restore")
    assert pg_dump is not None
    assert pg_restore is not None
    schema_name = pg_instance["schema_name"]
    migrations_dir = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    source_target = _migration_target(pg_instance)
    source_engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": "-c TimeZone=America/Los_Angeles"},
    )
    operator_user = str(pg_instance["operator_user"])

    with source_engine.begin() as connection:
        connection.execute(
            text("SELECT set_config('search_path', :schema, true)"),
            {"schema": schema_name},
        )
        connection.execute(
            text(
                "SELECT set_config('session.current_domain_code', 'Z', true), "
                "set_config('session.current_owner_repo_name', "
                "'daylily-tapdb', true), "
                "set_config('session.current_username', "
                "'migration:restore-fixture', true), "
                "set_config('session.current_tenant_id', '', true), "
                "set_config('session.allow_global_rows', 'true', true)"
            )
        )
        connection.execute(
            text(
                "DELETE FROM generic_instance "
                "WHERE identity_key = "
                "'labcore:sequencing_run:<persisted-euid>'"
            )
        )
        connection.execute(text("SELECT setval('msg_instance_seq', 1000, true)"))
        deterministic_event_id = uuid.UUID("ba930d7a-3974-4b4d-a81f-26a8bd13f663")
        deterministic_created_dt = datetime(2026, 8, 30, 9, 8, 7, tzinfo=UTC)
        connection.execute(
            text(
                "ALTER TABLE outbox_event ALTER COLUMN message_uid DROP NOT NULL; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS event_id UUID; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS event_type TEXT; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS aggregate_euid TEXT; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS payload JSONB; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS delivered_dt "
                "TIMESTAMP WITH TIME ZONE; "
                "INSERT INTO outbox_event ("
                "message_uid, event_id, tenant_id, domain_code, issuer_app_code, "
                "event_type, aggregate_euid, payload, destination, dedupe_key, "
                "status, attempt_count, next_attempt_at, created_dt, delivered_dt"
                ") VALUES ("
                "NULL, :event_id, NULL, 'Z', 'daylily-tapdb', "
                "'restore.fixture', '<persisted-euid>', '{\"stable\": true}'::jsonb, "
                "'svc://restore', :dedupe_key, 'pending', 3, "
                ":created_dt, :created_dt, :created_dt"
                ")"
            ),
            {
                "event_id": deterministic_event_id,
                "dedupe_key": f"restore-{deterministic_event_id}",
                "created_dt": deterministic_created_dt,
            },
        )
        connection.execute(
            text(
                "DELETE FROM _tapdb_migrations WHERE filename IN "
                "('20260902_010000_natural_identity_and_owner_uniqueness.sql', "
                "'20260902_010100_legacy_outbox_message_conversion.sql')"
            )
        )

    dump_path = tmp_path / "historical.dump"
    subprocess.run(
        [
            pg_dump,
            "-h",
            "localhost",
            "-p",
            str(pg_instance["port"]),
            "-U",
            operator_user,
            "-d",
            pg_instance["database"],
            "--schema",
            schema_name,
            "--format",
            "custom",
            "--file",
            str(dump_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    restored_database = f"tapdb_restore_{uuid.uuid4().hex[:12]}"
    admin_url = (
        f"postgresql://{pg_instance['operator_user']}:@localhost:"
        f"{pg_instance['port']}/postgres"
    )
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{restored_database}"'))
    subprocess.run(
        [
            pg_restore,
            "-h",
            "localhost",
            "-p",
            str(pg_instance["port"]),
            "-U",
            pg_instance["operator_user"],
            "-d",
            restored_database,
            "--no-owner",
            str(dump_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    restored_url = (
        f"postgresql://{pg_instance['operator_user']}:@localhost:"
        f"{pg_instance['port']}/{restored_database}"
    )
    restored_target = dict(source_target, database=restored_database)
    source_result = None
    restored_result = None
    for engine, target in (
        (source_engine, source_target),
        (
            create_engine(
                restored_url,
                connect_args={"options": "-c TimeZone=Asia/Tokyo"},
            ),
            restored_target,
        ),
    ):
        with engine.connect() as connection:
            transaction = connection.begin()
            preflight = build_migration_preflight(
                connection, migrations_dir=migrations_dir, target=target
            )
            result = apply_migration_preflight(
                connection,
                migrations_dir=migrations_dir,
                preflight=preflight,
                target=target,
            )
            transaction.commit()
        if target is source_target:
            source_result = result.receipt["postflight"]
        else:
            restored_result = result.receipt["postflight"]

    assert source_result is not None
    assert restored_result is not None
    assert restored_result["tables"] == source_result["tables"]
    assert restored_result["sequences"] == source_result["sequences"]
    mapping_rows = source_result["tables"]["tapdb_legacy_outbox_mapping"]["rows"]
    deterministic_mapping = next(
        row
        for row in mapping_rows
        if row["identity"]["old_event_id"] == str(deterministic_event_id)
    )
    assert deterministic_mapping["identity"]["mapped_dt"] == (
        deterministic_created_dt.isoformat()
    )
    assert len(deterministic_mapping["identity"]["source_sha256"]) == 64


def test_released_9_1_0_schema_migrates_populated_rows_without_identity_change(
    pg_instance,
):
    repository = Path(__file__).resolve().parents[1]
    release_commit = subprocess.run(
        ["git", "rev-parse", "9.1.0^{commit}"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert release_commit == "9a60412a1292902126fe34929162c45486e109f5"
    released_schema = subprocess.run(
        ["git", "show", "9.1.0:schema/tapdb_schema.sql"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert "identity_key" not in released_schema
    assert "CREATE TABLE IF NOT EXISTS _tapdb_migrations" in released_schema

    historical_schema = f"tapdb_historical_{uuid.uuid4().hex[:12]}"
    migrations_dir = repository / "schema" / "migrations"
    target = {
        **_migration_target(pg_instance),
        "schema_name": historical_schema,
    }
    engine = create_engine(pg_instance["operator_dsn"])
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'CREATE SCHEMA "{historical_schema}"')
            connection.execute(
                text("SELECT set_config('search_path', :schema, true)"),
                {"schema": historical_schema},
            )
            connection.exec_driver_sql(released_schema.replace("%", "%%"))
            _set_operator_context(
                connection, historical_schema, "migration:released-9.1-fixture"
            )
            connection.exec_driver_sql(
                "CREATE SEQUENCE tpx_instance_seq CACHE 1; "
                "CREATE SEQUENCE edg_instance_seq CACHE 1; "
                "CREATE SEQUENCE adt_instance_seq CACHE 1"
            )
            connection.execute(
                text(
                    "INSERT INTO tapdb_identity_prefix_config "
                    "(entity, domain_code, issuer_app_code, prefix) VALUES "
                    "('generic_template', 'Z', 'daylily-tapdb', 'TPX'), "
                    "('generic_instance_lineage', 'Z', 'daylily-tapdb', 'EDG'), "
                    "('audit_log', 'Z', 'daylily-tapdb', 'ADT')"
                )
            )
            template_uid = connection.execute(
                text(
                    "INSERT INTO generic_template ("
                    "name, polymorphic_discriminator, category, type, subtype, "
                    "version, instance_prefix, json_addl, validator_ref, bstatus, "
                    "is_singleton, is_deleted"
                    ") VALUES ("
                    "'Released 9.1 message', 'generic_template', 'message', "
                    "'webhook', 'event', '1.0', 'MSG', '{}'::jsonb, "
                    "'UNIVERSAL_PASS@1', 'active', false, false"
                    ") RETURNING uid"
                )
            ).scalar_one()
            first_uid = connection.execute(
                text(
                    "INSERT INTO generic_instance ("
                    "name, polymorphic_discriminator, category, type, subtype, "
                    "version, template_uid, json_addl, bstatus, is_singleton, "
                    "is_deleted"
                    ") VALUES ("
                    "'Released 9.1 active row', 'generic_instance', 'message', "
                    "'webhook', 'event', '1.0', :template_uid, "
                    "'{\"historical\": true}'::jsonb, 'active', false, false"
                    ") RETURNING uid"
                ),
                {"template_uid": template_uid},
            ).scalar_one()
            connection.execute(text("SELECT nextval('generic_instance_uid_seq')"))
            connection.execute(text("SELECT nextval('msg_instance_seq')"))
            second_uid = connection.execute(
                text(
                    "INSERT INTO generic_instance ("
                    "name, polymorphic_discriminator, category, type, subtype, "
                    "version, template_uid, json_addl, bstatus, is_singleton, "
                    "is_deleted"
                    ") VALUES ("
                    "'Released 9.1 soft-deleted row', 'generic_instance', "
                    "'message', 'webhook', 'event', '1.0', :template_uid, "
                    "'{\"historical\": true}'::jsonb, 'active', false, true"
                    ") RETURNING uid"
                ),
                {"template_uid": template_uid},
            ).scalar_one()
            assert second_uid == first_uid + 2
            connection.execute(
                text(
                    "INSERT INTO generic_instance_lineage ("
                    "name, polymorphic_discriminator, parent_instance_uid, "
                    "child_instance_uid, relationship_type"
                    ") VALUES ("
                    "'Released 9.1 edge', 'generic_instance_lineage', :parent, "
                    ":child, 'historical-proof'"
                    ")"
                ),
                {"parent": first_uid, "child": second_uid},
            )
            connection.execute(
                text(
                    "INSERT INTO outbox_event ("
                    "message_uid, domain_code, issuer_app_code, destination, "
                    "dedupe_key, status"
                    ") VALUES ("
                    ":message_uid, 'Z', 'daylily-tapdb', 'svc://released-9.1', "
                    "'released-9.1-proof', 'pending'"
                    ")"
                ),
                {"message_uid": first_uid},
            )
            prior_migrations = [
                path.name
                for path in sorted(migrations_dir.glob("*.sql"))
                if path.name < "20260902_010000"
            ]
            connection.execute(
                text(
                    "INSERT INTO _tapdb_migrations (filename) "
                    "SELECT value FROM jsonb_array_elements_text(CAST(:names AS jsonb))"
                ),
                {"names": json.dumps(prior_migrations)},
            )

        with engine.connect() as connection:
            transaction = connection.begin()
            preflight = build_migration_preflight(
                connection, migrations_dir=migrations_dir, target=target
            )
            result = apply_migration_preflight(
                connection,
                migrations_dir=migrations_dir,
                preflight=preflight,
                target=target,
            )
            transaction.commit()

        postflight = result.receipt["postflight"]
        assert result.receipt["applied_migrations"] == [
            "20260902_010000_natural_identity_and_owner_uniqueness.sql",
            "20260902_010100_legacy_outbox_message_conversion.sql",
            "20260902_020000_force_rls_and_audit_attribution.sql",
            "20260903_031820_runtime_ddl_guard.sql",
            "20260904_061819_tenant_scoped_natural_identity.sql",
        ]
        assert (
            result.receipt["sequence_pre_state"]
            == result.receipt["sequence_post_state"]
        )
        for table_name in (
            "generic_template",
            "generic_instance",
            "generic_instance_lineage",
            "audit_log",
            "outbox_event",
            "tapdb_identity_prefix_config",
        ):
            historical_rows = {
                tuple(row["key"]): row
                for row in preflight["tables"][table_name]["rows"]
            }
            migrated_rows = {
                tuple(row["key"]): row
                for row in postflight["tables"][table_name]["rows"]
            }
            assert migrated_rows.keys() == historical_rows.keys()
            for key, historical_row in historical_rows.items():
                for column, historical_hash in historical_row["column_sha256"].items():
                    assert migrated_rows[key]["column_sha256"][column] == (
                        historical_hash
                    )
        instances = postflight["tables"]["generic_instance"]
        assert instances["active_count"] == 1
        assert instances["soft_deleted_count"] == 1
        assert all(row["identity"]["identity_key"] is None for row in instances["rows"])
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS "{historical_schema}" CASCADE'
            )
        engine.dispose()


def test_released_9_2_2_migrates_to_tenant_identity_without_mutating_rows(
    pg_instance,
):
    repository = Path(__file__).resolve().parents[1]
    release_commit = subprocess.run(
        ["git", "rev-parse", "9.2.2^{commit}"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert release_commit == "be0e4b063f78f63f43d21c37070ac61a8450c619"
    released_schema = subprocess.run(
        ["git", "show", "9.2.2:schema/tapdb_schema.sql"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert "idx_generic_instance_natural_identity_tenant" not in released_schema
    assert "ck_generic_instance_identity_key_global" in released_schema

    historical_schema = f"tapdb_historical_{uuid.uuid4().hex[:12]}"
    filename = "20260904_061819_tenant_scoped_natural_identity.sql"
    migrations_dir = repository / "schema" / "migrations"
    target = {**_migration_target(pg_instance), "schema_name": historical_schema}
    engine = create_engine(pg_instance["operator_dsn"])
    template_uid = 0
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'CREATE SCHEMA "{historical_schema}"')
            connection.execute(
                text("SELECT set_config('search_path', :schema, true)"),
                {"schema": historical_schema},
            )
            connection.exec_driver_sql(released_schema.replace("%", "%%"))
            _set_operator_context(
                connection, historical_schema, "migration:released-9.2.2-fixture"
            )
            connection.exec_driver_sql(
                "CREATE SEQUENCE IF NOT EXISTS tpx_instance_seq CACHE 1; "
                "CREATE SEQUENCE IF NOT EXISTS msg_instance_seq CACHE 1; "
                "CREATE SEQUENCE IF NOT EXISTS adt_instance_seq CACHE 1"
            )
            connection.execute(
                text(
                    "INSERT INTO tapdb_identity_prefix_config "
                    "(entity, domain_code, issuer_app_code, prefix) VALUES "
                    "('generic_template', 'Z', 'daylily-tapdb', 'TPX'), "
                    "('audit_log', 'Z', 'daylily-tapdb', 'ADT')"
                )
            )
            template_uid = int(
                connection.execute(
                    text(
                        "INSERT INTO generic_template ("
                        "name, polymorphic_discriminator, category, type, subtype, "
                        "version, instance_prefix, instance_polymorphic_identity, "
                        "json_addl, validator_ref, bstatus, is_singleton, is_deleted"
                        ") VALUES ("
                        "'Released 9.2.2 message', 'generic_template', 'message', "
                        "'webhook', 'event', '1.0', 'MSG', 'generic_instance', "
                        "'{}'::jsonb, 'UNIVERSAL_PASS@1', 'active', false, false"
                        ") RETURNING uid"
                    )
                ).scalar_one()
            )
            connection.execute(
                text(
                    "INSERT INTO generic_instance ("
                    "name, polymorphic_discriminator, category, type, subtype, "
                    "version, template_uid, identity_key, json_addl, bstatus, "
                    "is_singleton, is_deleted"
                    ") VALUES ("
                    "'Released global identity', 'generic_instance', 'message', "
                    "'webhook', 'event', '1.0', :template_uid, "
                    "'catalog:released-global', '{}'::jsonb, 'active', false, false"
                    ")"
                ),
                {"template_uid": template_uid},
            )
            prior_migrations = [
                path.name
                for path in sorted(migrations_dir.glob("*.sql"))
                if path.name < filename
            ]
            connection.execute(
                text(
                    "INSERT INTO _tapdb_migrations (filename) "
                    "SELECT value FROM jsonb_array_elements_text(CAST(:names AS jsonb))"
                ),
                {"names": json.dumps(prior_migrations)},
            )

        with engine.connect() as connection:
            transaction = connection.begin()
            preflight = build_migration_preflight(
                connection, migrations_dir=migrations_dir, target=target
            )
            assert [item["filename"] for item in preflight["pending_migrations"]] == [
                filename
            ]
            pending = preflight["pending_migrations"][0]
            assert pending["allowed_columns"] == []
            assert pending["allowed_new_rows"] == []
            assert pending["allowed_sequences"] == []
            result = apply_migration_preflight(
                connection,
                migrations_dir=migrations_dir,
                preflight=preflight,
                target=target,
            )
            transaction.commit()

        assert (
            result.receipt["sequence_pre_state"]
            == result.receipt["sequence_post_state"]
        )
        assert result.receipt["postflight"]["tables"] == preflight["tables"]

        with engine.begin() as connection:
            _set_operator_context(
                connection, historical_schema, "migration:released-9.2.2-proof"
            )
            indexes = set(
                connection.execute(
                    text(
                        "SELECT indexname FROM pg_indexes WHERE schemaname = :schema "
                        "AND tablename = 'generic_instance'"
                    ),
                    {"schema": historical_schema},
                ).scalars()
            )
            assert "idx_generic_instance_natural_identity" not in indexes
            assert "idx_generic_instance_natural_identity_global" in indexes
            assert "idx_generic_instance_natural_identity_tenant" in indexes
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM pg_constraint c "
                        "JOIN pg_class t ON t.oid = c.conrelid "
                        "JOIN pg_namespace n ON n.oid = t.relnamespace "
                        "WHERE n.nspname = :schema "
                        "AND c.conname = 'ck_generic_instance_identity_key_global'"
                    ),
                    {"schema": historical_schema},
                ).scalar_one()
                == 0
            )
            lineage_scope_definition = connection.execute(
                text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"),
                {
                    "signature": (
                        f"{historical_schema}.tapdb_validate_lineage_endpoint_scope()"
                    )
                },
            ).scalar_one()
            assert "child_subtype = 'opaque'" in lineage_scope_definition
            assert "public_global" in lineage_scope_definition

        for tenant in (uuid.uuid4(), uuid.uuid4()):
            with engine.begin() as connection:
                _set_operator_context(
                    connection, historical_schema, "migration:tenant-identity-proof"
                )
                connection.execute(
                    text(
                        "SELECT set_config('session.current_tenant_id', :tenant, true)"
                    ),
                    {"tenant": str(tenant)},
                )
                connection.execute(
                    text(
                        "INSERT INTO generic_instance ("
                        "name, polymorphic_discriminator, category, type, subtype, "
                        "version, template_uid, tenant_id, identity_key, json_addl, "
                        "bstatus, is_singleton, is_deleted"
                        ") VALUES ("
                        "'Tenant identity', 'generic_instance', 'message', 'webhook', "
                        "'event', '1.0', :template_uid, :tenant, "
                        "'catalog:shared-tenant-key', '{}'::jsonb, 'active', false, false"
                        ")"
                    ),
                    {"template_uid": template_uid, "tenant": tenant},
                )

        with engine.connect() as connection:
            transaction = connection.begin()
            replay = build_migration_preflight(
                connection, migrations_dir=migrations_dir, target=target
            )
            transaction.rollback()
        assert replay["pending_migrations"] == []
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS "{historical_schema}" CASCADE'
            )
        engine.dispose()


def test_failed_guarded_migration_rolls_back_rows_and_sequences(pg_instance, tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    legacy_migration = (
        Path(__file__).resolve().parents[1]
        / "schema"
        / "migrations"
        / "20260902_010100_legacy_outbox_message_conversion.sql"
    )
    (migrations / "99999999_999998_legacy.sql").write_text(
        legacy_migration.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (migrations / "99999999_999999_failure.sql").write_text(
        "INSERT INTO tapdb_identity_prefix_config "
        "(entity, domain_code, issuer_app_code, prefix) "
        "VALUES ('failure-fixture', 'Z', 'daylily-tapdb', 'MSG');\n"
        "SELECT 1 / 0;\n",
        encoding="utf-8",
    )
    target = _migration_target(pg_instance)
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    failed_event_id = uuid.uuid4()
    with engine.begin() as connection:
        _set_operator_context(
            connection,
            pg_instance["schema_name"],
            "migration:rollback-fixture",
        )
        connection.execute(
            text("ALTER TABLE outbox_event ALTER COLUMN message_uid DROP NOT NULL")
        )
        connection.execute(
            text(
                "INSERT INTO outbox_event ("
                "message_uid, event_id, tenant_id, domain_code, issuer_app_code, "
                "event_type, aggregate_euid, payload, destination, dedupe_key, "
                "status, attempt_count, next_attempt_at, created_dt"
                ") VALUES ("
                "NULL, :event_id, NULL, 'Z', 'daylily-tapdb', "
                "'rollback.fixture', '<persisted-euid>', '{}'::jsonb, "
                "'svc://rollback', :dedupe_key, 'pending', 0, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP"
                ")"
            ),
            {
                "event_id": failed_event_id,
                "dedupe_key": f"rollback-{failed_event_id}",
            },
        )
    with engine.connect() as connection:
        transaction = connection.begin()
        preflight = build_migration_preflight(
            connection, migrations_dir=migrations, target=target
        )
        transaction.rollback()

    with engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(Exception, match="division by zero"):
            apply_migration_preflight(
                connection,
                migrations_dir=migrations,
                preflight=preflight,
                target=target,
            )
        transaction.rollback()

    with engine.connect() as connection:
        transaction = connection.begin()
        after = build_migration_preflight(
            connection, migrations_dir=migrations, target=target
        )
        transaction.rollback()

    assert after["tables"] == preflight["tables"]
    assert after["sequences"] == preflight["sequences"]


def test_failure_after_transactional_sequence_restart_rolls_back_exactly(
    pg_instance, tmp_path, monkeypatch
):
    migrations = tmp_path / "post-advance-migrations"
    migrations.mkdir()
    legacy_migration = (
        Path(__file__).resolve().parents[1]
        / "schema"
        / "migrations"
        / "20260902_010100_legacy_outbox_message_conversion.sql"
    )
    (migrations / "99999999_999997_post_advance_legacy.sql").write_text(
        legacy_migration.read_text(encoding="utf-8"), encoding="utf-8"
    )
    target = _migration_target(pg_instance)
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    event_id = uuid.uuid4()
    with engine.begin() as connection:
        _set_operator_context(
            connection,
            pg_instance["schema_name"],
            "migration:post-advance-fixture",
        )
        connection.execute(
            text(
                "ALTER TABLE outbox_event ALTER COLUMN message_uid DROP NOT NULL; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS event_id UUID; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS event_type TEXT; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS aggregate_euid TEXT; "
                "ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS payload JSONB"
            )
        )
        connection.execute(
            text(
                "INSERT INTO outbox_event ("
                "message_uid, event_id, tenant_id, domain_code, issuer_app_code, "
                "event_type, aggregate_euid, payload, destination, dedupe_key, "
                "status, attempt_count, next_attempt_at, created_dt"
                ") VALUES ("
                "NULL, :event_id, NULL, 'Z', 'daylily-tapdb', "
                "'post-advance.fixture', '<persisted-euid>', '{}'::jsonb, "
                "'svc://post-advance', :dedupe_key, 'pending', 0, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP"
                ")"
            ),
            {
                "event_id": event_id,
                "dedupe_key": f"post-advance-{event_id}",
            },
        )

    with engine.connect() as connection:
        transaction = connection.begin()
        preflight = build_migration_preflight(
            connection, migrations_dir=migrations, target=target
        )
        transaction.rollback()

    observed = {"advanced": False}
    original_advance = migration_identity._advance_permitted_identity_sequences

    def advance_then_fail(*args, **kwargs):
        original_advance(*args, **kwargs)
        connection = args[0]
        before = {row["name"]: row for row in preflight["sequences"]}
        for name in ("generic_instance_uid_seq", "msg_instance_seq"):
            qualified = f'"{pg_instance["schema_name"]}"."{name}"'
            current = connection.exec_driver_sql(
                f"SELECT last_value, is_called FROM {qualified}"
            ).one()
            assert (int(current[0]), bool(current[1])) != (
                int(before[name]["last_value"]),
                bool(before[name]["is_called"]),
            )
            assert bool(current[1]) is False
        observed["advanced"] = True
        raise RuntimeError("injected failure after sequence restart")

    monkeypatch.setattr(
        migration_identity,
        "_advance_permitted_identity_sequences",
        advance_then_fail,
    )
    with engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(
            RuntimeError, match="injected failure after sequence restart"
        ):
            apply_migration_preflight(
                connection,
                migrations_dir=migrations,
                preflight=preflight,
                target=target,
            )
        transaction.rollback()
    assert observed["advanced"] is True

    with engine.connect() as connection:
        transaction = connection.begin()
        after = build_migration_preflight(
            connection, migrations_dir=migrations, target=target
        )
        transaction.rollback()

    assert after["tables"] == preflight["tables"]
    assert after["sequences"] == preflight["sequences"]


def test_seed_sequence_alignment_is_exact_noop_and_required_restart_rolls_back(
    pg_instance,
):
    identity_key = f"migration-test:sequence-alignment:{uuid.uuid4()}"
    with _connection(pg_instance, "sequence-alignment-claim") as tapdb_connection:
        with tapdb_connection.session_scope(commit=True) as session:
            InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).claim_instance_by_identity(
                session,
                template_code="message/webhook/event/1.0/",
                identity_key=identity_key,
                name="Sequence alignment fixture",
                scope=IdentityScope.GLOBAL,
                properties={"source": "migration-test"},
            )

    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:sequence-state-capture"
        )
        assigned_max = int(
            connection.execute(
                text(
                    "SELECT max(euid_seq) FROM generic_instance "
                    "WHERE euid_prefix = 'MSG'"
                )
            ).scalar_one()
        )
        original_state = connection.execute(
            text("SELECT last_value, is_called FROM msg_instance_seq")
        ).one()

    try:
        # This representation has exactly the desired next issuance. Alignment
        # must preserve both stored fields, not merely the effective next value.
        with engine.begin() as connection:
            connection.execute(
                text("SELECT setval('msg_instance_seq', :value, true)"),
                {"value": assigned_max},
            )
        with engine.begin() as connection:
            _set_operator_context(
                connection, pg_instance["schema_name"], "migration:sequence-noop"
            )
            before = connection.execute(
                text("SELECT last_value, is_called FROM msg_instance_seq")
            ).one()
            with Session(bind=connection) as session:
                ensure_instance_prefix_sequence(session, "MSG")
            after = connection.execute(
                text("SELECT last_value, is_called FROM msg_instance_seq")
            ).one()
            assert tuple(after) == tuple(before) == (assigned_max, True)

        # A behind generator requires a forward restart. Injecting rollback after
        # that restart must restore the exact committed pre-state.
        with engine.begin() as connection:
            connection.execute(
                text("SELECT setval('msg_instance_seq', :value, false)"),
                {"value": assigned_max},
            )
        behind_state = (assigned_max, False)
        with engine.connect() as connection:
            transaction = connection.begin()
            _set_operator_context(
                connection, pg_instance["schema_name"], "migration:sequence-rollback"
            )
            with Session(bind=connection) as session:
                ensure_instance_prefix_sequence(session, "MSG")
            advanced = connection.execute(
                text("SELECT last_value, is_called FROM msg_instance_seq")
            ).one()
            assert tuple(advanced) == (assigned_max + 1, False)
            transaction.rollback()
        with engine.begin() as connection:
            restored = connection.execute(
                text("SELECT last_value, is_called FROM msg_instance_seq")
            ).one()
            assert tuple(restored) == behind_state
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("SELECT setval('msg_instance_seq', :value, :called)"),
                {"value": int(original_state[0]), "called": bool(original_state[1])},
            )


def test_prefix_registry_reapply_is_byte_stable_and_mismatch_fails_closed(pg_instance):
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )

    def rows(connection) -> list[str]:
        return list(
            connection.execute(
                text(
                    "SELECT row_to_json(config)::text "
                    "FROM tapdb_identity_prefix_config AS config "
                    "ORDER BY entity, domain_code, issuer_app_code"
                )
            ).scalars()
        )

    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:prefix-before"
        )
        before = rows(connection)
        sequence_before = tuple(
            connection.execute(
                text("SELECT last_value, is_called FROM tpx_instance_seq")
            ).one()
        )
    db_mod._sync_identity_prefix_config(db_mod.Environment.target)
    db_mod._ensure_instance_prefix_sequence(db_mod.Environment.target, "TPX")
    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:prefix-after"
        )
        assert rows(connection) == before
        assert (
            tuple(
                connection.execute(
                    text("SELECT last_value, is_called FROM tpx_instance_seq")
                ).one()
            )
            == sequence_before
        )
        original = connection.execute(
            text(
                "SELECT prefix, updated_dt "
                "FROM tapdb_identity_prefix_config "
                "WHERE entity = 'generic_template' AND domain_code = 'Z' "
                "AND issuer_app_code = 'daylily-tapdb'"
            )
        ).one()
        connection.execute(
            text(
                "UPDATE tapdb_identity_prefix_config SET prefix = 'ZZ' "
                "WHERE entity = 'generic_template' AND domain_code = 'Z' "
                "AND issuer_app_code = 'daylily-tapdb'"
            )
        )

    try:
        with engine.begin() as connection:
            _set_operator_context(
                connection,
                pg_instance["schema_name"],
                "migration:prefix-mismatch-before",
            )
            mismatch_before = rows(connection)
        with pytest.raises(RuntimeError, match="conflicts with the required registry"):
            db_mod._sync_identity_prefix_config(db_mod.Environment.target)
        with engine.begin() as connection:
            _set_operator_context(
                connection,
                pg_instance["schema_name"],
                "migration:prefix-mismatch-after",
            )
            assert rows(connection) == mismatch_before
    finally:
        with engine.begin() as connection:
            _set_operator_context(
                connection, pg_instance["schema_name"], "migration:prefix-restore"
            )
            connection.execute(
                text(
                    "UPDATE tapdb_identity_prefix_config "
                    "SET prefix = :prefix, updated_dt = :updated "
                    "WHERE entity = 'generic_template' AND domain_code = 'Z' "
                    "AND issuer_app_code = 'daylily-tapdb'"
                ),
                {
                    "prefix": original.prefix,
                    "updated": original.updated_dt,
                },
            )


def test_runtime_schema_create_guard_migrates_921_without_identity_changes(
    pg_instance,
):
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    filename = "20260903_031820_runtime_ddl_guard.sql"
    migrations_dir = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    target = _migration_target(pg_instance)
    try:
        with engine.begin() as connection:
            _set_operator_context(
                connection,
                pg_instance["schema_name"],
                "migration:runtime-schema-create-fixture",
            )
            connection.exec_driver_sql(
                f'GRANT CREATE ON SCHEMA "{pg_instance["schema_name"]}" '
                f'TO "{pg_instance["user"]}"'
            )
            connection.execute(
                text("DELETE FROM _tapdb_migrations WHERE filename = :filename"),
                {"filename": filename},
            )

        with engine.connect() as connection:
            transaction = connection.begin()
            preflight = build_migration_preflight(
                connection,
                migrations_dir=migrations_dir,
                target=target,
            )
            assert [item["filename"] for item in preflight["pending_migrations"]] == [
                filename
            ]
            result = apply_migration_preflight(
                connection,
                migrations_dir=migrations_dir,
                preflight=preflight,
                target=target,
            )
            transaction.commit()

        assert (
            result.receipt["sequence_post_state"]
            == result.receipt["sequence_pre_state"]
        )
        for table_name, before in preflight["tables"].items():
            assert (
                result.receipt["postflight"]["tables"][table_name]["immutable_sha256"]
                == before["immutable_sha256"]
            )
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT pg_catalog.has_schema_privilege("
                        ":role, :schema, 'CREATE')"
                    ),
                    {
                        "role": pg_instance["user"],
                        "schema": pg_instance["schema_name"],
                    },
                ).scalar_one()
                is False
            )
    finally:
        with engine.begin() as connection:
            _set_operator_context(
                connection,
                pg_instance["schema_name"],
                "migration:runtime-schema-create-cleanup",
            )
            connection.exec_driver_sql(
                f'REVOKE CREATE ON SCHEMA "{pg_instance["schema_name"]}" '
                f'FROM "{pg_instance["user"]}"'
            )
            connection.execute(
                text(
                    "INSERT INTO _tapdb_migrations (filename) VALUES (:filename) "
                    "ON CONFLICT (filename) DO NOTHING"
                ),
                {"filename": filename},
            )
        engine.dispose()


def test_runner_executes_canonical_rls_include_with_declared_attribution(pg_instance):
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    filename = "20260902_020000_force_rls_and_audit_attribution.sql"
    migrations_dir = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    target = _migration_target(pg_instance)
    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:rls-history-fixture"
        )
        audit_uid = connection.execute(
            text("SELECT min(uid) FROM audit_log")
        ).scalar_one()
        assert audit_uid is not None
        connection.execute(
            text("ALTER TABLE audit_log ALTER COLUMN changed_by DROP NOT NULL")
        )
        connection.execute(
            text("UPDATE audit_log SET changed_by = '' WHERE uid = :uid"),
            {"uid": audit_uid},
        )
        connection.execute(
            text("DELETE FROM _tapdb_migrations WHERE filename = :filename"),
            {"filename": filename},
        )
        connection.execute(
            text(
                "INSERT INTO _tapdb_migrations (filename) VALUES "
                "('20260902_010000_natural_identity_and_owner_uniqueness.sql'), "
                "('20260902_010100_legacy_outbox_message_conversion.sql') "
                "ON CONFLICT (filename) DO NOTHING"
            )
        )

    with engine.connect() as connection:
        transaction = connection.begin()
        preflight = build_migration_preflight(
            connection, migrations_dir=migrations_dir, target=target
        )
        transaction.rollback()
    assert [item["filename"] for item in preflight["pending_migrations"]] == [filename]
    assert preflight["pending_migrations"][0]["allowed_columns"] == [
        "audit_log.changed_by"
    ]

    with engine.connect() as connection:
        transaction = connection.begin()
        result = apply_migration_preflight(
            connection,
            migrations_dir=migrations_dir,
            preflight=preflight,
            target=target,
        )
        transaction.commit()

    assert result.receipt["sequence_post_state"] == result.receipt["sequence_pre_state"]
    for table_name, before in preflight["tables"].items():
        assert (
            result.receipt["postflight"]["tables"][table_name]["immutable_sha256"]
            == before["immutable_sha256"]
        )
    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:rls-history-proof"
        )
        assert (
            connection.execute(
                text("SELECT changed_by FROM audit_log WHERE uid = :uid"),
                {"uid": audit_uid},
            ).scalar_one()
            == "migration:pre-9.2-unattributed"
        )


def test_declared_validator_backfill_preserves_all_other_values_and_sequences(
    pg_instance, tmp_path
):
    migrations = tmp_path / "validator-schema" / "migrations"
    migrations.mkdir(parents=True)
    source = (
        Path(__file__).resolve().parents[1]
        / "schema"
        / "migrations"
        / "20260612_154200_add_template_validator_ref.sql"
    )
    filename = "99999999_999996_declared_validator_backfill.sql"
    (migrations / filename).write_text(
        source.read_text(encoding="utf-8"), encoding="utf-8"
    )
    target = _migration_target(pg_instance)
    engine = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    with engine.begin() as connection:
        _set_operator_context(
            connection,
            pg_instance["schema_name"],
            "migration:validator-history-fixture",
        )
        template_uid = connection.execute(
            text("SELECT min(uid) FROM generic_template")
        ).scalar_one()
        assert template_uid is not None
        connection.execute(
            text(
                "ALTER TABLE generic_template ALTER COLUMN validator_ref DROP NOT NULL; "
                "ALTER TABLE generic_template DISABLE TRIGGER audit_update_generic_template; "
                "ALTER TABLE generic_template "
                "DISABLE TRIGGER update_modified_dt_generic_template"
            )
        )
        connection.execute(
            text("UPDATE generic_template SET validator_ref = '' WHERE uid = :uid"),
            {"uid": template_uid},
        )
        connection.execute(
            text(
                "ALTER TABLE generic_template "
                "ENABLE TRIGGER update_modified_dt_generic_template; "
                "ALTER TABLE generic_template ENABLE TRIGGER audit_update_generic_template"
            )
        )

    with engine.connect() as connection:
        transaction = connection.begin()
        preflight = build_migration_preflight(
            connection, migrations_dir=migrations, target=target
        )
        transaction.rollback()
    with engine.connect() as connection:
        transaction = connection.begin()
        result = apply_migration_preflight(
            connection,
            migrations_dir=migrations,
            preflight=preflight,
            target=target,
        )
        transaction.commit()

    assert result.receipt["sequence_post_state"] == result.receipt["sequence_pre_state"]
    for table_name, before in preflight["tables"].items():
        assert (
            result.receipt["postflight"]["tables"][table_name]["immutable_sha256"]
            == before["immutable_sha256"]
        )
    with engine.begin() as connection:
        _set_operator_context(
            connection, pg_instance["schema_name"], "migration:validator-proof"
        )
        assert (
            connection.execute(
                text("SELECT validator_ref FROM generic_template WHERE uid = :uid"),
                {"uid": template_uid},
            ).scalar_one()
            == "UNIVERSAL_PASS@1"
        )
