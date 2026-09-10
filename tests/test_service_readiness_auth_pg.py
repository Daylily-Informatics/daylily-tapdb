"""Independent authorization qualification against real packaged SQL assets.

All lifecycle SQL is confined to this test's newly created local databases and
roles. No application identifier is supplied: installed owning triggers mint it.
The non-superuser operator models PostgreSQL privileges, not an Aurora claim.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL, Engine

from daylily_tapdb import runtime_principal as rp
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.runtime_catalog_contract import security_assets
from daylily_tapdb.security_context import (
    TapdbTransactionContext,
    apply_transaction_context,
    assert_operator_role,
)

TENANT_A = "00000000-0000-0000-0000-000000000001"
TENANT_B = "00000000-0000-0000-0000-000000000002"
ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_PATHS = (
    "daylily_tapdb/runtime_principal.py",
    "daylily_tapdb/runtime_catalog_contract.py",
    "daylily_tapdb/security_context.py",
    "daylily_tapdb/connection.py",
    "daylily_tapdb/sequences.py",
    "daylily_tapdb/identity_inventory.py",
    "schema/tapdb_schema.sql",
    "schema/rls.sql",
    "tests/conftest.py",
    "tests/test_service_readiness_qualification_auth.py",
    "tests/test_service_readiness_auth_pg.py",
)


def _snapshot():
    return {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
        for path in SNAPSHOT_PATHS
    }


@pytest.fixture(scope="module")
def auth_snapshot(pg_instance, record_testsuite_property):
    before = _snapshot()
    record_testsuite_property(
        "auth_snapshot_before", json.dumps(before, sort_keys=True)
    )
    with psycopg2.connect(pg_instance["operator_dsn"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW server_version")
            version = cursor.fetchone()[0]
    record_testsuite_property("auth_exact_server_version", version)
    yield
    record_testsuite_property(
        "auth_snapshot_after", json.dumps(_snapshot(), sort_keys=True)
    )


def _context(cfg, *, operator=False, owner=None, domain=None, tenant=None):
    return TapdbTransactionContext(
        config_identity=cfg["config_path"],
        schema_name=cfg["schema_name"],
        domain_code=domain or cfg["domain_code"],
        owner_repo_name=owner or cfg["owner_repo_name"],
        tenant_id=tenant if operator else cfg["tenant_id"] or None,
        actor="qualification:operator" if operator else "qualification:runtime",
        allow_global_rows=True if operator else cfg["allow_global_claims"],
    )


def _runtime_url(cfg):
    return URL.create(
        "postgresql+psycopg2",
        username=cfg["user"],
        password="",
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
    )


@contextmanager
def _runtime(cfg):
    engine = create_engine(_runtime_url(cfg))
    try:
        with engine.begin() as connection:
            apply_transaction_context(connection, _context(cfg))
            yield connection
    finally:
        engine.dispose()


@contextmanager
def _operator(cfg):
    with rp.operator_connection(cfg, isolation_level="REPEATABLE READ") as connection:
        apply_transaction_context(
            connection, _context(cfg, operator=True), assert_runtime_role=False
        )
        yield connection


@pytest.fixture
def auth_database(pg_instance, auth_snapshot, tmp_path):
    suffix = uuid4().hex[:12]
    operator = "qualification_operator_" + suffix
    runtime = "qualification_runtime_" + suffix
    historical_owner = "qualification_history_" + suffix
    database = "qualification_auth_" + suffix
    cfg = {
        "config_path": str(tmp_path / "qualification.yaml"),
        "engine_type": "local",
        "host": "localhost",
        "port": pg_instance["port"],
        "database": database,
        "schema_name": "qualification_scope",
        "user": runtime,
        "operator_user": operator,
        "operator_configured": True,
        "operator_password": "",
        "password": "",
        "iam_auth": False,
        "operator_iam_auth": False,
        "client_id": "qualification",
        "database_name": "qualification",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "tenant_id": TENANT_A,
        "allow_global_claims": False,
    }
    admin = psycopg2.connect(pg_instance["operator_dsn"])
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "CREATE ROLE {} LOGIN CREATEROLE NOCREATEDB NOSUPERUSER NOBYPASSRLS"
            ).format(sql.Identifier(operator))
        )
        cursor.execute(
            sql.SQL("CREATE ROLE {} NOLOGIN NOSUPERUSER NOBYPASSRLS").format(
                sql.Identifier(historical_owner)
            )
        )
        cursor.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(database), sql.Identifier(operator)
            )
        )
    try:
        rp.bootstrap_runtime_principal(cfg, apply=True)
        with rp.operator_connection(cfg) as connection:
            connection.execute(text('CREATE SCHEMA "qualification_scope"'))
            apply_transaction_context(
                connection, _context(cfg, operator=True), assert_runtime_role=False
            )
            assets = security_assets()
            with connection.connection.cursor() as cursor:
                cursor.execute(assets["tapdb_schema.sql"])
            for prefix in ("tpx", "adt", "edg"):
                connection.exec_driver_sql(f"CREATE SEQUENCE {prefix}_instance_seq")
            with connection.connection.cursor() as cursor:
                cursor.execute(assets["rls.sql"])
            for domain, owner in (
                ("Z", "daylily-tapdb"),
                ("Z", "other-owner"),
                ("Y", "daylily-tapdb"),
            ):
                for entity, prefix in (
                    ("generic_template", "TPX"),
                    ("generic_instance_lineage", "EDG"),
                    ("audit_log", "ADT"),
                ):
                    connection.execute(
                        text(
                            "INSERT INTO tapdb_identity_prefix_config(entity,domain_code,issuer_app_code,prefix) VALUES (:entity,:domain,:owner,:prefix)"
                        ),
                        dict(entity=entity, domain=domain, owner=owner, prefix=prefix),
                    )
            for label, domain, owner, tenant in (
                ("local", "Z", "daylily-tapdb", TENANT_A),
                ("other-tenant", "Z", "daylily-tapdb", TENANT_B),
                ("global", "Z", "daylily-tapdb", None),
                ("other-owner", "Z", "other-owner", TENANT_A),
                ("other-domain", "Y", "daylily-tapdb", TENANT_A),
            ):
                apply_transaction_context(
                    connection,
                    _context(
                        cfg, operator=True, domain=domain, owner=owner, tenant=tenant
                    ),
                    assert_runtime_role=False,
                )
                template = connection.execute(
                    text(
                        "INSERT INTO generic_template(name,polymorphic_discriminator,category,type,subtype,version,instance_prefix,bstatus,is_singleton,json_addl,tenant_id) VALUES (:name,'generic_template','set',:name,'generic','1.0','MSG','active',false,'{}',:tenant) RETURNING uid"
                    ),
                    dict(name=label, tenant=tenant),
                ).scalar_one()
                connection.execute(
                    text(
                        "INSERT INTO generic_instance(name,polymorphic_discriminator,category,type,subtype,version,template_uid,bstatus,tenant_id) VALUES (:name,'generic_instance','set',:name,'generic','1.0',:template,'active',:tenant)"
                    ),
                    dict(name=label, template=template, tenant=tenant),
                )
            # Preserve duplicate unkeyed historical rows, plus several kinds of
            # non-core authority. Their ACLs deliberately deny the runtime.
            connection.exec_driver_sql(
                "CREATE TABLE historical_assertion (payload text NOT NULL)"
            )
            connection.exec_driver_sql(
                "INSERT INTO historical_assertion VALUES ('retained'),('retained')"
            )
            connection.exec_driver_sql(
                "CREATE VIEW historical_view AS SELECT payload FROM historical_assertion"
            )
            connection.exec_driver_sql("CREATE SEQUENCE historical_allocator")
            connection.exec_driver_sql(
                "CREATE FUNCTION historical_operator_export() RETURNS text LANGUAGE sql SECURITY DEFINER SET search_path=qualification_scope,pg_catalog,pg_temp AS $$ SELECT min(payload) FROM historical_assertion $$"
            )
            connection.exec_driver_sql(
                "REVOKE ALL ON FUNCTION historical_operator_export() FROM PUBLIC"
            )
            connection.exec_driver_sql(
                "CREATE AGGREGATE historical_collect(text) (SFUNC=pg_catalog.textcat, STYPE=text, INITCOND='')"
            )
            connection.exec_driver_sql(
                "REVOKE ALL ON FUNCTION historical_collect(text) FROM PUBLIC"
            )
        yield cfg, historical_owner
    finally:
        # Test-owned disposable targets only; no pre-existing database is used.
        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(database)
                )
            )
            for role in (runtime, historical_owner, operator):
                cursor.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role))
                )
        admin.close()


def _bind(cfg, tmp_path):
    path = tmp_path / "binding.json"
    plan = rp.bind_runtime_principal(cfg, receipt_path=path)
    result = rp.bind_runtime_principal(cfg, apply=True, receipt_path=path)
    assert result["plan_sha256"] == plan["sha256"]
    return plan


@pytest.mark.parametrize("mixed_owner", [False, True])
def test_independent_preserved_objects_and_actual_denial(
    auth_database, pg_instance, tmp_path, mixed_owner
):
    cfg, historical_owner = auth_database
    if mixed_owner:
        with psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=pg_instance["operator_user"],
        ) as admin:
            with admin.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        "ALTER TABLE qualification_scope.historical_assertion OWNER TO {}"
                    ).format(sql.Identifier(historical_owner))
                )
                cursor.execute(
                    sql.SQL(
                        "GRANT SELECT ON qualification_scope.historical_assertion TO {}"
                    ).format(sql.Identifier(cfg["operator_user"]))
                )
    before = _bind(cfg, tmp_path)
    assert len(before["routine_grants"]) == 30
    assert len(before["triggers"]) == 19
    assert not any(
        grant["name"].startswith("historical_")
        for grant in before["grants"] + before["routine_grants"]
    )
    with _operator(cfg) as connection:
        assert_operator_role(
            connection,
            schema_name=cfg["schema_name"],
            operator_user=cfg["operator_user"],
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM generic_instance")
            ).scalar_one()
            == 5
        )
        assert connection.execute(
            text("SELECT payload FROM historical_assertion")
        ).scalars().all() == ["retained", "retained"]
        after = rp.build_runtime_principal_binding_plan(connection, cfg)
    for category in ("objects", "functions"):
        assert [
            row for row in before[category] if row["name"].startswith("historical_")
        ] == [row for row in after[category] if row["name"].startswith("historical_")]
    for statement in (
        "SELECT * FROM historical_assertion",
        "SELECT * FROM historical_view",
        "SELECT nextval('historical_allocator')",
        "SELECT historical_operator_export()",
        "SELECT historical_collect(value) FROM (VALUES ('x'::text)) sample(value)",
    ):
        with pytest.raises(Exception, match="permission denied"):
            with _runtime(cfg) as connection:
                connection.execute(text(statement))


@pytest.mark.parametrize(
    "mutation", ["true_policy", "when_false", "mutable_function", "search_path"]
)
def test_independent_security_tampering_rejected_after_review(
    auth_database, tmp_path, mutation
):
    cfg, _ = auth_database
    path = tmp_path / "reviewed.json"
    rp.bind_runtime_principal(cfg, receipt_path=path)
    with _operator(cfg) as connection:
        if mutation == "true_policy":
            connection.exec_driver_sql(
                "ALTER POLICY generic_instance_scope_isolation ON generic_instance USING (true) WITH CHECK (true)"
            )
        elif mutation == "when_false":
            connection.exec_driver_sql(
                "DROP TRIGGER tapdb_runtime_scope_immutable ON tapdb_runtime_principal_scope"
            )
            connection.exec_driver_sql(
                "CREATE TRIGGER tapdb_runtime_scope_immutable BEFORE UPDATE OR DELETE ON tapdb_runtime_principal_scope FOR EACH ROW WHEN (false) EXECUTE FUNCTION tapdb_reject_runtime_scope_mutation()"
            )
        elif mutation == "mutable_function":
            connection.exec_driver_sql(
                "CREATE OR REPLACE FUNCTION tapdb_reject_runtime_scope_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$"
            )
        else:
            connection.exec_driver_sql(
                "ALTER FUNCTION tapdb_current_tenant_id() RESET search_path"
            )
    for apply, receipt in ((False, tmp_path / "fresh.json"), (True, path)):
        with pytest.raises(rp.RuntimePrincipalError, match="canonical|Canonical"):
            rp.bind_runtime_principal(cfg, apply=apply, receipt_path=receipt)
    with _operator(cfg) as connection:
        assert (
            connection.execute(
                text("SELECT count(*) FROM tapdb_runtime_principal_scope")
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize(
    "grant",
    [
        "GRANT SELECT ON historical_assertion TO PUBLIC",
        "GRANT UPDATE (payload) ON historical_assertion TO {runtime}",
        "GRANT USAGE ON SEQUENCE historical_allocator TO PUBLIC",
        "GRANT EXECUTE ON FUNCTION historical_operator_export() TO PUBLIC",
        "GRANT EXECUTE ON FUNCTION historical_collect(text) TO {runtime}",
        "GRANT UPDATE (prefix) ON tapdb_identity_prefix_config TO PUBLIC",
    ],
)
def test_independent_privilege_drift_prevents_commit(auth_database, tmp_path, grant):
    cfg, _ = auth_database
    path = tmp_path / "before-grant.json"
    rp.bind_runtime_principal(cfg, receipt_path=path)
    with _operator(cfg) as connection:
        connection.exec_driver_sql(grant.format(runtime='"' + cfg["user"] + '"'))
    with pytest.raises(rp.RuntimePrincipalError, match="privileges|stale"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=path)
    assert not path.with_name("before-grant.result.json").exists()
    with _operator(cfg) as connection:
        assert (
            connection.execute(
                text("SELECT count(*) FROM tapdb_runtime_principal_scope")
            ).scalar_one()
            == 0
        )


def test_independent_fixed_tenant_global_scope_and_immutable_binding(
    auth_database, tmp_path
):
    cfg, _ = auth_database
    _bind(cfg, tmp_path)
    with _runtime(cfg) as connection:
        assert connection.execute(
            text("SELECT name FROM generic_instance ORDER BY name")
        ).scalars().all() == ["global", "local"]
        assert (
            connection.execute(
                text(
                    "UPDATE generic_instance SET bstatus='accepted' WHERE name='local' RETURNING name"
                )
            ).scalar_one()
            == "local"
        )
        for key, value in (
            ("session.current_tenant_id", TENANT_B),
            ("session.current_owner_repo_name", "other-owner"),
            ("session.current_domain_code", "Y"),
            ("session.allow_global_rows", "true"),
        ):
            connection.execute(
                text("SELECT set_config(:key,:value,true)"), dict(key=key, value=value)
            )
        assert connection.execute(
            text("SELECT name FROM generic_instance ORDER BY name")
        ).scalars().all() == ["global", "local"]
    for statement in (
        "UPDATE generic_instance SET bstatus='forbidden' WHERE name='global'",
        "UPDATE tapdb_runtime_principal_scope SET allow_global_rows=true",
        "SELECT * FROM tapdb_runtime_principal_scope",
        "CREATE TABLE qualification_scope.runtime_ddl_forbidden (n integer)",
    ):
        with pytest.raises(Exception, match="permission denied|row-level security"):
            with _runtime(cfg) as connection:
                connection.execute(text(statement))
    with pytest.raises(Exception, match="immutable"):
        with _operator(cfg) as connection:
            connection.execute(
                text("UPDATE tapdb_runtime_principal_scope SET allow_global_rows=true")
            )
    with pytest.raises(rp.RuntimePrincipalError, match="conflicting immutable"):
        rp.bind_runtime_principal(
            {**cfg, "tenant_id": TENANT_B}, receipt_path=tmp_path / "wrong-tenant.json"
        )


def test_independent_runtime_startup_issues_no_ddl(auth_database, tmp_path):
    cfg, _ = auth_database
    _bind(cfg, tmp_path)
    statements = []

    def record(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(Engine, "before_cursor_execute", record)
    try:
        with TAPDBConnection(
            db_url=_runtime_url(cfg).render_as_string(hide_password=False),
            db_user=cfg["user"],
            engine_type="local",
            app_username="qualification:startup",
            domain_code=cfg["domain_code"],
            owner_repo_name=cfg["owner_repo_name"],
            schema_name=cfg["schema_name"],
            config_identity=cfg["config_path"],
            tenant_id=cfg["tenant_id"],
            allow_global_rows=False,
        ) as runtime:
            with runtime.session_scope(commit=False) as session:
                assert (
                    session.execute(
                        text("SELECT count(*) FROM generic_instance")
                    ).scalar_one()
                    == 2
                )
    finally:
        event.remove(Engine, "before_cursor_execute", record)
    assert any("tapdb_assert_runtime_role()" in statement for statement in statements)
    assert all(
        not statement.lstrip()
        .upper()
        .startswith(
            (
                "CREATE ",
                "ALTER ",
                "DROP ",
                "GRANT ",
                "REVOKE ",
                "INSERT ",
                "UPDATE ",
                "DELETE ",
            )
        )
        for statement in statements
    )


def test_independent_temp_sequence_cannot_redirect_persistent_allocation(
    auth_database, tmp_path
):
    cfg, _ = auth_database
    _bind(cfg, tmp_path)
    with _operator(cfg) as connection:
        allocated = connection.execute(
            text("SELECT nextval('qualification_scope.msg_instance_seq')")
        ).scalar_one()
    with _runtime(cfg) as connection:
        assert connection.execute(
            text(
                "SELECT has_database_privilege(current_user,current_database(),'TEMP')"
            )
        ).scalar_one()
        connection.exec_driver_sql(
            "CREATE TEMP SEQUENCE msg_instance_seq START WITH 1000000"
        )
        persisted = connection.execute(
            text(
                "INSERT INTO qualification_scope.generic_instance(name,polymorphic_discriminator,category,type,subtype,version,template_uid,bstatus,tenant_id) SELECT 'temp-shadow-attempt','generic_instance',category,type,subtype,version,uid,'active',tenant_id FROM qualification_scope.generic_template WHERE name='local' RETURNING euid_seq"
            )
        ).scalar_one()
        durable = connection.execute(
            text("SELECT last_value FROM qualification_scope.msg_instance_seq")
        ).scalar_one()
        temporary = connection.execute(
            text("SELECT is_called FROM pg_temp.msg_instance_seq")
        ).scalar_one()
        assert (persisted, durable, temporary) == (
            allocated + 1,
            allocated + 1,
            False,
        ), (
            "Runtime TEMP must not redirect a persisted object's owning allocator",
            dict(persisted=persisted, durable=durable, temporary_called=temporary),
        )
