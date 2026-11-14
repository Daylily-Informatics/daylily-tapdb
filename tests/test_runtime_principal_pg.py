"""Actual PostgreSQL proofs with an operator lacking SUPERUSER and BYPASSRLS.

This exercises Aurora-compatible privilege semantics; an actual isolated
Aurora run remains a separate release acceptance requirement.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import psycopg2
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from daylily_tapdb import runtime_principal as rp
from daylily_tapdb.security_context import (
    TapdbTransactionContext,
    apply_transaction_context,
    assert_operator_role,
    operator_role_assertion_sql,
)

ROOT = Path(__file__).resolve().parents[1]
TENANT_A = "00000000-0000-0000-0000-00000000000a"
TENANT_B = "00000000-0000-0000-0000-00000000000b"


def test_additional_tenant_allowlist_is_bound_and_cannot_be_widened(
    principal_database, tmp_path
):
    cfg = {**principal_database, "additional_tenant_ids": [TENANT_B]}
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    receipt = tmp_path / "allowlist.json"
    plan = rp.bind_runtime_principal(cfg, receipt_path=receipt)
    assert plan["scope"]["additional_tenant_ids"] == [TENANT_B]
    rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    engine = _runtime_engine(cfg)
    try:
        with engine.begin() as connection:
            apply_transaction_context(connection, _context(cfg))
            assert connection.execute(
                text("SELECT name FROM generic_template ORDER BY name")
            ).scalars().all() == ["row-0", "row-1", "row-2"]
            assert (
                connection.execute(text("SELECT session_user")).scalar_one()
                == cfg["user"]
            )
            assert (
                connection.execute(
                    text("SELECT has_database_privilege(current_database(), 'TEMP')")
                ).scalar_one()
                is False
            )
            assert (
                connection.execute(
                    text(
                        "UPDATE generic_template SET name = 'allowed-write' WHERE name = 'row-1' RETURNING tenant_id::text"
                    )
                ).scalar_one()
                == TENANT_B
            )
        for sql in (
            "CREATE TABLE forbidden_table(id integer)",
            "CREATE TEMP TABLE forbidden_temp(id integer)",
            "UPDATE generic_template SET tenant_id = '00000000-0000-0000-0000-00000000000c' WHERE name = 'allowed-write'",
            "UPDATE generic_template SET name = 'forbidden' WHERE name = 'row-2'",
            "UPDATE tapdb_runtime_principal_scope SET additional_tenant_ids = '{}'",
        ):
            with pytest.raises(Exception):
                with engine.begin() as connection:
                    apply_transaction_context(connection, _context(cfg))
                    connection.execute(text(sql))
        with engine.begin() as connection:
            apply_transaction_context(connection, _context(cfg))
            connection.execute(
                text(
                    "SELECT set_config('session.additional_tenant_ids', '{00000000-0000-0000-0000-00000000000c}', true)"
                )
            )
            assert connection.execute(
                text("SELECT tapdb_allowed_tenant_ids()::text[]")
            ).scalar_one() == [TENANT_A, TENANT_B]
            with pytest.raises(Exception, match="does not match immutable"):
                connection.execute(text("SELECT tapdb_assert_runtime_role()"))
        with pytest.raises(rp.RuntimePrincipalError, match="conflicting immutable"):
            rp.bind_runtime_principal(
                {**cfg, "additional_tenant_ids": []},
                receipt_path=tmp_path / "conflict.json",
            )
        with pytest.raises(Exception, match="immutable"):
            with rp.operator_connection(cfg) as connection:
                connection.execute(
                    text(
                        f"UPDATE \"{cfg['schema_name']}\".tapdb_runtime_principal_scope SET additional_tenant_ids = '{{}}'"
                    )
                )
    finally:
        engine.dispose()


def test_allowlisted_lineage_accepts_visible_scopes_and_rejects_hidden_endpoint(
    principal_database, tmp_path
):
    from dataclasses import replace

    from sqlalchemy.orm import Session

    from daylily_tapdb.factory import InstanceFactory
    from daylily_tapdb.models.lineage import generic_instance_lineage
    from daylily_tapdb.templates import TemplateManager

    cfg = {**principal_database, "additional_tenant_ids": [TENANT_B]}
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    receipt = tmp_path / "lineage-allowlist.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    factory = InstanceFactory(TemplateManager(), domain_code="Z")
    with rp.operator_connection(cfg) as connection:
        apply_transaction_context(
            connection,
            replace(
                _context(cfg),
                tenant_id=None,
                allow_global_rows=True,
            ),
            assert_runtime_role=False,
        )
        with Session(connection) as session:
            hidden = factory.create_instance(
                session,
                "set/scope-2/generic/1.0/",
                "hidden endpoint",
                create_children=False,
                tenant_id=uuid.UUID(int=12),
            )
            hidden_uid = hidden.uid
    engine = _runtime_engine(cfg)
    try:
        with Session(engine) as session, session.begin():
            apply_transaction_context(session, _context(cfg))
            endpoints = [
                factory.create_instance(
                    session,
                    f"set/scope-{index}/generic/1.0/",
                    f"allowed endpoint {index}",
                    create_children=False,
                    tenant_id=uuid.UUID(tenant),
                )
                for index, tenant in enumerate((TENANT_A, TENANT_B))
            ]
            parent_uid = endpoints[0].uid
            edge = generic_instance_lineage(
                name="permitted service lineage",
                parent_instance_uid=parent_uid,
                child_instance_uid=endpoints[1].uid,
                tenant_id=uuid.UUID(TENANT_A),
                category="generic",
                type="lineage",
                subtype="instance_lineage",
                version="1.0",
                relationship_type="contains",
                bstatus="active",
                json_addl={"properties": {}},
            )
            session.add(edge)
            session.flush()
            assert edge.uid is not None and edge.euid
        with pytest.raises(Exception, match="endpoints are unavailable"):
            with Session(engine) as session, session.begin():
                apply_transaction_context(session, _context(cfg))
                session.add(
                    generic_instance_lineage(
                        name="forbidden hidden endpoint",
                        parent_instance_uid=parent_uid,
                        child_instance_uid=hidden_uid,
                        tenant_id=uuid.UUID(TENANT_A),
                        category="generic",
                        type="lineage",
                        subtype="instance_lineage",
                        version="1.0",
                        relationship_type="contains",
                        bstatus="active",
                        json_addl={"properties": {}},
                    )
                )
                session.flush()
    finally:
        engine.dispose()


def test_canonical_security_catalog(principal_database):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg, isolation_level="REPEATABLE READ") as connection:
        plan = rp.build_runtime_principal_binding_plan(connection, cfg)
    assert len(plan["routine_grants"]) == 31
    assert len(plan["triggers"]) == 19
    assert set(plan["security_asset_sha256"]) == {
        "tapdb_schema.sql",
        "rls.sql",
        "allocator_functions.sql",
    }


def test_allocator_helper_and_public_factory_keep_persistent_schema_authority(
    principal_database, tmp_path
):
    from sqlalchemy.orm import Session

    from daylily_tapdb.factory import InstanceFactory
    from daylily_tapdb.templates import TemplateManager

    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    receipt = tmp_path / "allocator-runtime.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    engine = _runtime_engine(cfg)
    try:
        with Session(engine) as session, session.begin():
            apply_transaction_context(session, _context(cfg))
            before = session.execute(
                text(
                    f'SELECT last_value, is_called FROM "{cfg["schema_name"]}".gse_instance_seq'
                )
            ).one()
            instance = InstanceFactory(
                TemplateManager(), domain_code="Z"
            ).create_instance(
                session,
                "set/scope-0/generic/1.0/",
                "ordinary allocator regression",
                create_children=False,
                tenant_id=uuid.UUID(TENANT_A),
            )
            session.refresh(instance)
            assert instance.euid_seq == before.last_value + (
                1 if before.is_called else 0
            )
            assert (
                session.execute(
                    text(
                        f'SELECT last_value FROM "{cfg["schema_name"]}".gse_instance_seq'
                    )
                ).scalar_one()
                == instance.euid_seq
            )
            # A directly called control-table resolver keeps its declared
            # schema even when the caller's path has no application schema.
            session.execute(text("SET LOCAL search_path TO pg_catalog"))
            assert (
                session.execute(
                    text(
                        f"SELECT \"{cfg['schema_name']}\".tapdb_get_identity_prefix('generic_template')"
                    )
                ).scalar_one()
                == "TPX"
            )
    finally:
        engine.dispose()


def test_allocator_migration_restores_pins_without_changing_catalog_or_floors(
    principal_database,
):
    from daylily_tapdb.migration_identity import _expand_migration_source

    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    schema = ROOT / "schema"
    migration = (
        schema / "migrations/20260910_233000_pin_managed_allocator_resolution.sql"
    )
    with rp.operator_connection(cfg, isolation_level="REPEATABLE READ") as connection:
        connection.execute(text(f'SET LOCAL search_path TO "{cfg["schema_name"]}"'))
        before = rp.build_runtime_principal_binding_plan(connection, cfg)
        for signature in (
            "tapdb_get_identity_prefix(text)",
            "set_generic_template_euid()",
            "set_generic_instance_euid()",
            "set_generic_instance_lineage_euid()",
            "set_audit_log_euid()",
        ):
            connection.execute(text(f"ALTER FUNCTION {signature} RESET search_path"))
        connection.exec_driver_sql(
            _expand_migration_source(migration, schema_root=schema),
            execution_options={"no_parameters": True},
        )
        after = rp.build_runtime_principal_binding_plan(connection, cfg)
        assert after == before


@pytest.mark.parametrize(
    "principal_database", ["principal_runtime", "Principal_Runtime"], indirect=True
)
def test_real_bind_denies_public_and_direct_temp_without_claiming_session_closure(
    principal_database, tmp_path
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(f'GRANT TEMPORARY ON DATABASE "{cfg["database"]}" TO "{cfg["user"]}"')
        )
    engine = _runtime_engine(cfg)
    try:
        with engine.connect() as old_session:
            old_pid = old_session.execute(text("SELECT pg_backend_pid()")).scalar_one()
            old_session.commit()
            receipt = tmp_path / "temp-denial.json"
            plan = rp.bind_runtime_principal(cfg, receipt_path=receipt)
            assert plan["database_access"]["runtime_temp"] is True
            assert [entry["grantee"] for entry in plan["database_revokes"]] == [
                "PUBLIC",
                cfg["user"],
            ]
            assert plan["operator_database_grants"] == []
            result = rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
            assert result["runtime_temp_denied"] is True
            assert result["database_access"]["runtime_temp"] is False
            assert result["database_access"]["operator_temp"] is True
            assert result["runtime_session_requirement"]["verified"] is False
            assert result["runtime_session_requirement"]["performed_by_bind"] is False
            # Binding does not terminate an existing application connection.
            assert (
                old_session.execute(text("SELECT pg_backend_pid()")).scalar_one()
                == old_pid
            )
        # A fresh physical connection must not create either temporary form.
        engine.dispose()
        for statement in (
            "CREATE TEMPORARY TABLE runtime_scratch (value integer)",
            "CREATE TEMPORARY SEQUENCE runtime_scratch_allocator",
        ):
            with pytest.raises(Exception, match="permission denied.*temporary"):
                with engine.begin() as connection:
                    connection.execute(text(statement))
            engine.dispose()
        with rp.operator_connection(cfg) as connection:
            connection.execute(
                text("CREATE TEMPORARY TABLE operator_scratch (value integer)")
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("initial_operator_temp", [True, False])
def test_real_bind_preserves_operator_temp_without_expanding_authority(
    principal_database, tmp_path, initial_operator_temp
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(
                f'REVOKE TEMPORARY ON DATABASE "{cfg["database"]}" FROM "{cfg["operator_user"]}"'
            )
        )
        if not initial_operator_temp:
            connection.execute(
                text(f'REVOKE TEMPORARY ON DATABASE "{cfg["database"]}" FROM PUBLIC')
            )
    receipt = tmp_path / "operator-temp.json"
    plan = rp.bind_runtime_principal(cfg, receipt_path=receipt)
    assert plan["database_access"]["operator_temp"] is initial_operator_temp
    assert plan["database_access"]["operator_temp_after_revokes"] is False
    assert plan["operator_database_grants"] == (
        [
            {
                "database": cfg["database"],
                "grantee": cfg["operator_user"],
                "privileges": ["TEMPORARY"],
            }
        ]
        if initial_operator_temp
        else []
    )
    result = rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert result["database_access"]["operator_temp"] is initial_operator_temp
    assert result["database_access"]["runtime_temp"] is False


def test_real_temp_dependent_grant_refuses_cascade_and_rolls_back(
    principal_database, pg_instance, tmp_path
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(
                f'GRANT TEMPORARY ON DATABASE "{cfg["database"]}" TO "{cfg["user"]}" WITH GRANT OPTION'
            )
        )
    engine = _runtime_engine(cfg)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f'GRANT TEMPORARY ON DATABASE "{cfg["database"]}" TO "{pg_instance["user"]}"'
                )
            )
    finally:
        engine.dispose()
    receipt = tmp_path / "dependent-temp.json"
    plan = rp.bind_runtime_principal(cfg, receipt_path=receipt)
    with pytest.raises(Exception, match="dependent privileges exist"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert not receipt.with_name("dependent-temp.result.json").exists()
    with rp.operator_connection(cfg) as connection:
        assert (
            rp._database_access(connection, rp._target(cfg)) == plan["database_access"]
        )
        assert (
            connection.execute(
                text(
                    f'SELECT count(*) FROM "{cfg["schema_name"]}".tapdb_runtime_principal_scope'
                )
            ).scalar_one()
            == 0
        )


@pytest.fixture
def principal_database(pg_instance, tmp_path, request):
    suffix = uuid.uuid4().hex[:12]
    operator = f"principal_operator_{suffix}"
    runtime = f"{getattr(request, 'param', 'principal_runtime')}_{suffix}"
    database = f"principal_database_{suffix}"
    cfg = {
        "config_path": str(tmp_path / "target.yaml"),
        "engine_type": "local",
        "host": "localhost",
        "port": pg_instance["port"],
        "database": database,
        "schema_name": "principal_schema",
        "user": runtime,
        "operator_user": operator,
        "operator_configured": True,
        "operator_password": "",
        "password": "",
        "iam_auth": False,
        "operator_iam_auth": False,
        "client_id": "test",
        "database_name": "test",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "tenant_id": TENANT_A,
        "allow_global_claims": False,
    }
    admin = psycopg2.connect(pg_instance["operator_dsn"])
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(
            f'CREATE ROLE "{operator}" LOGIN CREATEROLE NOCREATEDB NOSUPERUSER NOBYPASSRLS'
        )
        cursor.execute(f'CREATE DATABASE "{database}" OWNER "{operator}"')
    try:
        yield cfg
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
            cursor.execute(f'DROP ROLE IF EXISTS "{runtime}"')
            cursor.execute(f'DROP ROLE "{operator}"')
        admin.close()


def _install_schema(cfg):
    with psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["operator_user"],
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{cfg["schema_name"]}"')
            cursor.execute(f'SET search_path TO "{cfg["schema_name"]}"')
            cursor.execute(
                "SELECT set_config('session.current_domain_code', 'Z', true)"
            )
            cursor.execute(
                "SELECT set_config('session.current_owner_repo_name', 'daylily-tapdb', true)"
            )
            cursor.execute("SELECT set_config('session.current_tenant_id', '', true)")
            cursor.execute(
                "SELECT set_config('session.current_username', 'test:principal-fixture', true)"
            )
            cursor.execute(
                "SELECT set_config('session.allow_global_rows', 'true', true)"
            )
            cursor.execute((ROOT / "schema/tapdb_schema.sql").read_text())
            for prefix in ("adt", "edg", "tpx", "gse"):
                cursor.execute(f"CREATE SEQUENCE {prefix}_instance_seq")
            cursor.execute((ROOT / "schema/rls.sql").read_text())
            for owner in ("daylily-tapdb", "another-owner"):
                for entity, prefix in (
                    ("generic_template", "TPX"),
                    ("generic_instance_lineage", "EDG"),
                    ("audit_log", "ADT"),
                ):
                    cursor.execute(
                        "INSERT INTO tapdb_identity_prefix_config(entity, domain_code, issuer_app_code, prefix) VALUES (%s, 'Z', %s, %s)",
                        (entity, owner, prefix),
                    )
            # Every identifier below is minted by the real schema, including the
            # audit records, from the owning service's installed generators.
            for index, (owner, tenant) in enumerate(
                (
                    ("daylily-tapdb", TENANT_A),
                    ("daylily-tapdb", TENANT_B),
                    ("daylily-tapdb", None),
                    ("another-owner", TENANT_A),
                )
            ):
                cursor.execute(
                    "SELECT set_config('session.current_owner_repo_name', %s, true)",
                    (owner,),
                )
                cursor.execute(
                    """INSERT INTO generic_template (
                    name, tenant_id, polymorphic_discriminator, category, type, subtype, version,
                    instance_prefix, bstatus, is_singleton, json_addl
                ) VALUES (%s, %s, 'generic_template', 'set', %s, 'generic', '1.0', 'GSE', 'active', FALSE, '{}'::jsonb)""",
                    (f"row-{index}", tenant, f"scope-{index}"),
                )


def _runtime_engine(cfg):
    return create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=cfg["user"],
            password="",
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
        )
    )


def _context(cfg):
    return TapdbTransactionContext(
        config_identity=cfg["config_path"],
        schema_name=cfg["schema_name"],
        domain_code=cfg["domain_code"],
        owner_repo_name=cfg["owner_repo_name"],
        tenant_id=cfg["tenant_id"],
        actor="test:runtime-principal",
        allow_global_rows=False,
        additional_tenant_ids=tuple(cfg.get("additional_tenant_ids", ())),
    )


def test_real_bootstrap_is_connect_only_and_idempotent(principal_database):
    cfg = principal_database
    first = rp.bootstrap_runtime_principal(cfg, apply=True)
    assert first["created"] is True
    assert rp.bootstrap_runtime_principal(cfg, apply=True)["created"] is False
    with rp.operator_connection(cfg) as connection:
        assert (
            connection.execute(
                text("SELECT count(*) FROM pg_namespace WHERE nspname = :schema"),
                {"schema": cfg["schema_name"]},
            ).scalar_one()
            == 0
        )
        attrs = connection.execute(
            text(
                "SELECT rolsuper, rolbypassrls, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = current_user"
            )
        ).one()
        assert tuple(attrs) == (False, False, False, True)
        assert connection.execute(
            text(
                "SELECT has_database_privilege(:role, current_database(), 'CONNECT') AND NOT has_database_privilege(:role, current_database(), 'CREATE')"
            ),
            {"role": cfg["user"]},
        ).scalar_one()


def test_real_bind_preserves_force_rls_and_complete_operator_visibility(
    principal_database, tmp_path
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    receipt = tmp_path / "binding.json"
    plan = rp.bind_runtime_principal(cfg, receipt_path=receipt)
    assert plan["status"] == "planned"
    result = rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert result["status"] == "applied"
    assert (
        json.loads(receipt.with_name("binding.result.json").read_text())["plan_sha256"]
        == plan["sha256"]
    )
    with rp.operator_connection(cfg) as connection:
        connection.execute(text(f'SET search_path TO "{cfg["schema_name"]}"'))
        assert_operator_role(connection)
        assert (
            connection.execute(
                text("SELECT count(*) FROM generic_template")
            ).scalar_one()
            == 4
        )
        for table in sorted(rp._WRITABLE | rp._READABLE | {rp._SCOPE}):
            connection.execute(text(f'SELECT count(*) FROM "{table}"')).scalar_one()
        assert connection.execute(
            text(
                "SELECT bool_and(relrowsecurity AND relforcerowsecurity) FROM pg_class WHERE relnamespace = :oid AND relkind = 'r' AND relname <> '_tapdb_migrations'"
            ),
            {"oid": plan["schema"]["oid"]},
        ).scalar_one()
    engine = _runtime_engine(cfg)
    try:
        with engine.begin() as connection:
            apply_transaction_context(connection, _context(cfg))
            assert connection.execute(
                text("SELECT name FROM generic_template ORDER BY name")
            ).scalars().all() == ["row-0", "row-2"]
            # Custom GUCs cannot move the authenticated login's owner or tenant.
            connection.execute(
                text(
                    "SELECT set_config('session.current_owner_repo_name', 'another-owner', true)"
                )
            )
            connection.execute(
                text("SELECT set_config('session.current_tenant_id', :tenant, true)"),
                {"tenant": TENANT_B},
            )
            connection.execute(
                text("SELECT set_config('session.allow_global_rows', 'true', true)")
            )
            assert connection.execute(
                text("SELECT name FROM generic_template ORDER BY name")
            ).scalars().all() == ["row-0", "row-2"]
            with pytest.raises(Exception, match="does not match immutable"):
                connection.execute(text("SELECT tapdb_assert_runtime_role()"))
        for sql in (
            "CREATE TABLE forbidden_table(id integer)",
            "UPDATE tapdb_runtime_principal_scope SET allow_global_rows = TRUE",
            "UPDATE _tapdb_migrations SET filename = 'forbidden'",
            "SELECT setval('tpx_instance_seq', 1, false)",
            "UPDATE generic_template SET name = 'forbidden' WHERE name = 'row-2'",
        ):
            with pytest.raises(Exception):
                with engine.begin() as connection:
                    apply_transaction_context(connection, _context(cfg))
                    connection.execute(text(sql))
    finally:
        engine.dispose()


def test_real_receipt_stale_and_scope_immutable(principal_database, tmp_path):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    receipt = tmp_path / "binding.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(f'GRANT USAGE ON SCHEMA "{cfg["schema_name"]}" TO "{cfg["user"]}"')
        )
    with pytest.raises(rp.RuntimePrincipalError, match="stale"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    second = tmp_path / "binding-current.json"
    rp.bind_runtime_principal(cfg, receipt_path=second)
    rp.bind_runtime_principal(cfg, apply=True, receipt_path=second)
    with pytest.raises(Exception, match="immutable"):
        with rp.operator_connection(cfg) as connection:
            connection.execute(
                text(
                    f'UPDATE "{cfg["schema_name"]}".tapdb_runtime_principal_scope SET allow_global_rows = TRUE'
                )
            )
    changed = {**cfg, "tenant_id": TENANT_B}
    with pytest.raises(rp.RuntimePrincipalError, match="conflicting immutable"):
        rp.bind_runtime_principal(changed, receipt_path=tmp_path / "conflict.json")


@pytest.mark.parametrize("change_after_plan", [False, True])
def test_real_post_restore_bind_establishes_only_reviewed_connect(
    principal_database, tmp_path, change_after_plan
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(
                f'REVOKE ALL ON DATABASE "{cfg["database"]}" FROM PUBLIC, "{cfg["user"]}"'
            )
        )
    engine = _runtime_engine(cfg)
    try:
        with pytest.raises(Exception, match="permission denied for database"):
            with engine.connect():
                pytest.fail("Runtime connected before receipt-bound target activation")
        receipt = tmp_path / "post-restore.json"
        plan = rp.bind_runtime_principal(cfg, receipt_path=receipt)
        assert plan["database_access"]["runtime_connect"] is False
        if change_after_plan:
            with rp.operator_connection(cfg) as connection:
                connection.execute(
                    text(
                        f'GRANT CONNECT ON DATABASE "{cfg["database"]}" TO "{cfg["user"]}"'
                    )
                )
            with pytest.raises(rp.RuntimePrincipalError, match="stale"):
                rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
            assert not receipt.with_name("post-restore.result.json").exists()
        else:
            result = rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
            assert result["database_access"]["runtime_connect"] is True
            with engine.begin() as connection:
                apply_transaction_context(connection, _context(cfg))
                assert connection.execute(
                    text("SELECT name FROM generic_template ORDER BY name")
                ).scalars().all() == ["row-0", "row-2"]
                assert not connection.execute(
                    text(
                        "SELECT has_database_privilege(current_user, current_database(), 'CREATE') "
                        "OR has_database_privilege(current_user, current_database(), 'TEMPORARY')"
                    )
                ).scalar_one()
    finally:
        engine.dispose()


def test_real_operator_check_rejects_missing_policy(principal_database):
    cfg = principal_database
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(
                f'DROP POLICY tapdb_operator_access ON "{cfg["schema_name"]}".generic_instance'
            )
        )
    with pytest.raises(Exception, match="complete FORCE-RLS"):
        with rp.operator_connection(cfg) as connection:
            connection.execute(
                text(
                    operator_role_assertion_sql(
                        schema_name=cfg["schema_name"],
                        operator_user=cfg["operator_user"],
                    )
                )
            )


def test_real_operator_check_rejects_applicable_restrictive_policy(
    principal_database, tmp_path
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(
                f'CREATE POLICY unexpected_restriction ON "{cfg["schema_name"]}".generic_template AS RESTRICTIVE TO PUBLIC USING (false)'
            )
        )
    with pytest.raises(Exception, match="complete FORCE-RLS"):
        rp.bind_runtime_principal(cfg, receipt_path=tmp_path / "restricted-bind.json")


def test_real_historical_nonforced_owner_capture_needs_no_schema_mutation(
    principal_database,
):
    cfg = principal_database
    with rp.operator_connection(cfg) as connection:
        connection.execute(text(f'CREATE SCHEMA "{cfg["schema_name"]}"'))
        connection.execute(
            text(f'CREATE TABLE "{cfg["schema_name"]}".historical_rows (id integer)')
        )
        connection.execute(
            text(f'INSERT INTO "{cfg["schema_name"]}".historical_rows VALUES (1), (2)')
        )
        connection.execute(
            text(
                f'ALTER TABLE "{cfg["schema_name"]}".historical_rows ENABLE ROW LEVEL SECURITY'
            )
        )
        connection.execute(
            text(
                f'CREATE POLICY historical_runtime_filter ON "{cfg["schema_name"]}".historical_rows USING (false)'
            )
        )
    with rp.operator_connection(
        cfg, isolation_level="REPEATABLE READ", read_only=True
    ) as connection:
        assert_operator_role(
            connection,
            schema_name=cfg["schema_name"],
            operator_user=cfg["operator_user"],
        )
        connection.execute(text("SET LOCAL row_security = on"))
        assert (
            connection.execute(
                text(f'SELECT count(*) FROM "{cfg["schema_name"]}".historical_rows')
            ).scalar_one()
            == 2
        )
        assert connection.execute(
            text(
                "SELECT relrowsecurity AND NOT relforcerowsecurity FROM pg_class WHERE oid = :relation::regclass".replace(
                    ":relation::", "CAST(:relation AS text)::"
                )
            ),
            {"relation": f"{cfg['schema_name']}.historical_rows"},
        ).scalar_one()


def test_real_public_sequence_update_leak_rolls_back_binding(
    principal_database, tmp_path
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(
                f'GRANT UPDATE ON SEQUENCE "{cfg["schema_name"]}".tpx_instance_seq TO PUBLIC'
            )
        )
    receipt = tmp_path / "public-leak.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    with pytest.raises(rp.RuntimePrincipalError, match="forbidden effective object"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    with rp.operator_connection(cfg) as connection:
        assert (
            connection.execute(
                text(
                    f'SELECT count(*) FROM "{cfg["schema_name"]}".tapdb_runtime_principal_scope'
                )
            ).scalar_one()
            == 0
        )
    assert not receipt.with_name("public-leak.result.json").exists()


def test_real_fresh_grants_and_future_allocators_remain_constrained(principal_database):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg, isolation_level="REPEATABLE READ") as connection:
        connection.execute(text(rp.runtime_scope_binding_sql(cfg["schema_name"], cfg)))
        connection.execute(
            text(
                rp.runtime_schema_grants_sql(
                    cfg["schema_name"], cfg["user"], operator_user=cfg["operator_user"]
                )
            )
        )
        connection.execute(
            text(f'CREATE SEQUENCE "{cfg["schema_name"]}".future_allocator')
        )
        connection.execute(
            text(f'CREATE TABLE "{cfg["schema_name"]}".operator_only (uid bigint)')
        )
        grants = rp.grant_proven_runtime_sequences(connection, cfg)
        assert "future_allocator" not in {grant["name"] for grant in grants}
    engine = _runtime_engine(cfg)
    try:
        with engine.begin() as connection:
            apply_transaction_context(connection, _context(cfg))
            assert (
                connection.execute(
                    text("SELECT count(*) FROM generic_template")
                ).scalar_one()
                == 2
            )
            assert (
                connection.execute(
                    text("SELECT nextval('gse_instance_seq')")
                ).scalar_one()
                == 1
            )
        for sql in (
            "SELECT nextval('future_allocator')",
            "SELECT setval('future_allocator', 10)",
            "SELECT * FROM operator_only",
            "UPDATE _tapdb_migrations SET filename = 'forbidden'",
        ):
            with pytest.raises(Exception):
                with engine.begin() as connection:
                    apply_transaction_context(connection, _context(cfg))
                    connection.execute(text(sql))
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "sql, error",
    [
        (
            "DROP POLICY generic_instance_scope_isolation ON generic_instance; CREATE POLICY generic_instance_scope_isolation ON generic_instance USING (true) WITH CHECK (true)",
            "policy authority",
        ),
        (
            "DROP TRIGGER tapdb_runtime_scope_immutable ON tapdb_runtime_principal_scope; CREATE TRIGGER tapdb_runtime_scope_immutable BEFORE UPDATE OR DELETE ON tapdb_runtime_principal_scope FOR EACH ROW WHEN (false) EXECUTE FUNCTION tapdb_reject_runtime_scope_mutation()",
            "trigger",
        ),
        (
            "DROP TRIGGER tapdb_runtime_scope_immutable ON tapdb_runtime_principal_scope; CREATE TRIGGER tapdb_runtime_scope_immutable BEFORE UPDATE ON tapdb_runtime_principal_scope FOR EACH ROW EXECUTE FUNCTION tapdb_reject_runtime_scope_mutation()",
            "trigger",
        ),
        (
            "CREATE OR REPLACE FUNCTION tapdb_reject_runtime_scope_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$",
            "routine body",
        ),
        ("ALTER FUNCTION tapdb_current_tenant_id() SECURITY INVOKER", "routine body"),
        (
            "ALTER FUNCTION tapdb_current_domain_code() SET search_path TO pg_temp, pg_catalog",
            "routine body",
        ),
        (
            "ALTER TABLE tapdb_runtime_principal_scope DISABLE TRIGGER tapdb_runtime_scope_immutable",
            "trigger",
        ),
    ],
)
def test_real_binding_rejects_noncanonical_security(
    principal_database, tmp_path, sql, error
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(text(f'SET LOCAL search_path TO "{cfg["schema_name"]}"'))
        connection.exec_driver_sql(sql, execution_options={"no_parameters": True})
    receipt = tmp_path / "unsafe-catalog.json"
    with pytest.raises(rp.RuntimePrincipalError, match=error):
        rp.bind_runtime_principal(cfg, receipt_path=receipt)
    assert not receipt.exists()


def _install_preserved_extras(cfg):
    with rp.operator_connection(cfg) as connection:
        connection.execute(text(f'SET LOCAL search_path TO "{cfg["schema_name"]}"'))
        connection.execute(
            text("CREATE TABLE historical_assertion (uid bigint, payload text)")
        )
        connection.execute(
            text("INSERT INTO historical_assertion VALUES (1, 'retained assertion')")
        )
        connection.execute(
            text(
                "CREATE VIEW historical_view AS SELECT payload FROM historical_assertion"
            )
        )
        connection.execute(text("CREATE SEQUENCE historical_allocator"))
        connection.execute(
            text(
                "CREATE FUNCTION historical_operator_export() RETURNS text LANGUAGE sql SECURITY DEFINER AS $$ SELECT 'operator only'::text $$"
            )
        )
        connection.execute(
            text("REVOKE ALL ON FUNCTION historical_operator_export() FROM PUBLIC")
        )
        connection.execute(
            text(
                "CREATE AGGREGATE historical_collect (text) "
                "(SFUNC = pg_catalog.textcat, STYPE = text, INITCOND = '')"
            )
        )
        connection.execute(
            text("REVOKE ALL ON FUNCTION historical_collect(text) FROM PUBLIC")
        )


@pytest.mark.parametrize("mixed_owner", [False, True])
def test_real_preserved_extras_are_unchanged_and_ungranted(
    principal_database, pg_instance, tmp_path, mixed_owner
):
    from sqlalchemy.engine import make_url

    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    _install_preserved_extras(cfg)
    if mixed_owner:
        admin = create_engine(
            make_url(pg_instance["operator_dsn"]).set(database=cfg["database"])
        )
        try:
            with admin.begin() as connection:
                table = f'"{cfg["schema_name"]}".historical_assertion'
                connection.execute(
                    text(f'ALTER TABLE {table} OWNER TO "{pg_instance["user"]}"')
                )
                connection.execute(
                    text(f'GRANT SELECT ON TABLE {table} TO "{cfg["operator_user"]}"')
                )
        finally:
            admin.dispose()
    receipt = tmp_path / "extras.json"
    before = rp.bind_runtime_principal(cfg, receipt_path=receipt)
    assert not any(
        grant["name"].startswith("historical_") for grant in before["grants"]
    )
    assert not any(
        routine["name"].startswith("historical_")
        for routine in before["routine_grants"]
    )
    rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    with rp.operator_connection(cfg, isolation_level="REPEATABLE READ") as connection:
        connection.execute(text("SET LOCAL search_path TO pg_catalog, public"))
        after = rp.build_runtime_principal_binding_plan(connection, cfg)
        assert (
            connection.execute(text("SHOW search_path")).scalar_one()
            == "pg_catalog, public"
        )
        assert (
            connection.execute(
                text(f'SELECT payload FROM "{cfg["schema_name"]}".historical_assertion')
            ).scalar_one()
            == "retained assertion"
        )
    for catalog in ("objects", "functions"):
        assert [
            row for row in before[catalog] if row["name"].startswith("historical_")
        ] == [row for row in after[catalog] if row["name"].startswith("historical_")]


@pytest.mark.parametrize(
    "sql",
    [
        "GRANT SELECT ON historical_assertion TO PUBLIC",
        "GRANT SELECT (payload) ON historical_assertion TO {runtime}",
        "GRANT USAGE ON SEQUENCE historical_allocator TO PUBLIC",
        "GRANT EXECUTE ON FUNCTION historical_operator_export() TO PUBLIC",
        "GRANT EXECUTE ON FUNCTION historical_collect(text) TO PUBLIC",
        "GRANT UPDATE (prefix) ON tapdb_identity_prefix_config TO PUBLIC",
    ],
)
def test_real_binding_refuses_effective_column_routine_and_sequence_leaks(
    principal_database, tmp_path, sql
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    _install_preserved_extras(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(text(f'SET LOCAL search_path TO "{cfg["schema_name"]}"'))
        connection.execute(text(sql.format(runtime=f'"{cfg["user"]}"')))
    receipt = tmp_path / "leak.json"
    with pytest.raises(rp.RuntimePrincipalError, match="forbidden effective"):
        rp.bind_runtime_principal(cfg, receipt_path=receipt)
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert not receipt.with_name("leak.result.json").exists()
    with rp.operator_connection(cfg) as connection:
        assert (
            connection.execute(
                text(
                    f'SELECT count(*) FROM "{cfg["schema_name"]}".tapdb_runtime_principal_scope'
                )
            ).scalar_one()
            == 0
        )


def test_real_binding_removes_managed_column_grant_options(
    principal_database, tmp_path
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(
                f'GRANT SELECT (prefix) ON "{cfg["schema_name"]}".tapdb_identity_prefix_config TO "{cfg["user"]}" WITH GRANT OPTION'
            )
        )
    receipt = tmp_path / "column-options.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    with rp.operator_connection(cfg) as connection:
        assert not connection.execute(
            text(
                "SELECT has_any_column_privilege(:role, :object, 'SELECT WITH GRANT OPTION')"
            ),
            {
                "role": cfg["user"],
                "object": f'"{cfg["schema_name"]}".tapdb_identity_prefix_config',
            },
        ).scalar_one()


@pytest.mark.parametrize("recipient", ["runtime", "PUBLIC"])
def test_real_binding_denies_pg17_maintain_without_breaking_pg16(
    principal_database, tmp_path, recipient
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    _install_preserved_extras(cfg)
    with rp.operator_connection(cfg) as connection:
        server_version = int(
            connection.execute(text("SHOW server_version_num")).scalar_one()
        )
        if server_version >= 170000:
            role = f'"{cfg["user"]}"' if recipient == "runtime" else "PUBLIC"
            connection.execute(
                text(
                    f'GRANT MAINTAIN ON "{cfg["schema_name"]}".historical_assertion TO {role}'
                )
            )
    receipt = tmp_path / "maintain.json"
    if server_version >= 170000:
        with pytest.raises(rp.RuntimePrincipalError, match="forbidden effective"):
            rp.bind_runtime_principal(cfg, receipt_path=receipt)
        assert not receipt.exists()
    else:
        # PG16's privilege parser rejects MAINTAIN. The conditional proof must
        # remain usable on this supported version, not skip the whole check.
        assert (
            rp.bind_runtime_principal(cfg, receipt_path=receipt)["status"] == "planned"
        )


def test_real_global_default_privilege_leak_rejected_before_binding(
    principal_database, tmp_path
):
    cfg = principal_database
    rp.bootstrap_runtime_principal(cfg, apply=True)
    _install_schema(cfg)
    with rp.operator_connection(cfg) as connection:
        connection.execute(
            text(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE "{cfg["operator_user"]}" GRANT UPDATE ON SEQUENCES TO PUBLIC'
            )
        )
    with pytest.raises(Exception, match="Global or PUBLIC default privileges"):
        rp.bind_runtime_principal(cfg, receipt_path=tmp_path / "unsafe-defaults.json")


def test_real_retained_operator_session_owns_transactions_and_holds_session_locks(
    principal_database,
):
    cfg = principal_database
    key = int(uuid.uuid4().hex[:15], 16)
    with rp.operator_session(cfg, isolation_level="REPEATABLE READ") as connection:
        assert not connection.in_transaction()
        with connection.begin():
            first_pid = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            assert (
                connection.execute(text("SHOW transaction_isolation")).scalar_one()
                == "repeatable read"
            )
            connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": key})
        assert not connection.in_transaction()
        with rp.operator_connection(cfg) as other:
            assert (
                other.execute(
                    text("SELECT pg_try_advisory_lock(:key)"), {"key": key}
                ).scalar_one()
                is False
            )
        with connection.begin():
            assert (
                connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
                == first_pid
            )
            assert (
                connection.execute(
                    text("SELECT pg_advisory_unlock(:key)"), {"key": key}
                ).scalar_one()
                is True
            )
