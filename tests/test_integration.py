"""Postgres integration test for Phase 2 acceptance.

Gated by TAPDB_TEST_DSN.
Creates an isolated schema, installs tapdb_schema.sql, seeds templates from config,
creates an instance+children+lineage, executes an action, verifies audit+soft delete.
"""

import json
import os
import random
import time
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text

from daylily_tapdb.actions.dispatcher import ActionDispatcher
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.factory.instance import InstanceFactory
from daylily_tapdb.models.audit import audit_log
from daylily_tapdb.models.instance import action_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import (
    action_template,
    generic_template,
    workflow_step_template,
    workflow_template,
)
from daylily_tapdb.templates.manager import TemplateManager


def _install_schema(dsn: str, schema_name: str, schema_sql_path: Path) -> None:
    try:
        import psycopg2
    except Exception as e:  # pragma: no cover
        pytest.skip(f"psycopg2 unavailable: {e}")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA {schema_name};")
    finally:
        cur.close()
        conn.close()

    _apply_schema(dsn, schema_name, schema_sql_path)


def _apply_schema(dsn: str, schema_name: str, schema_sql_path: Path) -> None:
    """Apply schema/tapdb_schema.sql into an existing schema.

    This intentionally does *not* pre-install pgcrypto.
    schema/tapdb_schema.sql already handles pgcrypto availability/privileges gracefully.
    """
    try:
        import psycopg2
    except Exception as e:  # pragma: no cover
        pytest.skip(f"psycopg2 unavailable: {e}")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f"SET search_path TO {schema_name};")
        cur.execute(schema_sql_path.read_text())
    finally:
        cur.close()
        conn.close()


def _drop_schema(dsn: str, schema_name: str) -> None:
    try:
        import psycopg2
    except Exception:
        return

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    finally:
        cur.close()
        conn.close()


def _seed_templates(session, config_path: Path) -> None:
    data = json.loads(config_path.read_text())
    tmpl_list = data.get("templates", [])

    disc_to_cls = {
        "action_template": action_template,
        "workflow_template": workflow_template,
        "workflow_step_template": workflow_step_template,
    }

    for t in tmpl_list:
        disc = t["polymorphic_discriminator"]
        cls = disc_to_cls.get(disc, generic_template)

        obj = cls(
            name=t["name"],
            polymorphic_discriminator=disc,
            category=t["category"],
            type=t["type"],
            subtype=t["subtype"],
            version=t["version"],
            bstatus=t.get("bstatus", "active"),
            instance_prefix=t["instance_prefix"],
            instance_polymorphic_identity=t.get("instance_polymorphic_identity"),
            json_addl_schema=t.get("json_addl_schema"),
            json_addl=t.get("json_addl", {}),
            is_singleton=bool(t.get("is_singleton", False)),
        )
        session.add(obj)

    session.flush()


def test_postgres_schema_seed_action_audit_soft_delete():
    dsn = os.environ.get("TAPDB_TEST_DSN")
    if not dsn:
        pytest.skip("Set TAPDB_TEST_DSN to run Postgres integration tests")

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    schema_name = f"tapdb_test_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    _install_schema(dsn, schema_name, schema_sql_path)

    try:
        conn = TAPDBConnection(db_url=dsn, app_username="pytest")
        tm = TemplateManager()
        factory = InstanceFactory(tm)

        class TestDispatcher(ActionDispatcher):
            def do_action_create_note(self, instance, action_ds, captured_data):
                return {"status": "success", "message": "ok"}

        dispatcher = TestDispatcher()

        with conn.session_scope(commit=False) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))

            _seed_templates(session, repo_root / "config" / "action" / "core.json")
            _seed_templates(session, repo_root / "config" / "workflow_step" / "queue.json")
            _seed_templates(session, repo_root / "config" / "workflow" / "assay.json")

            wf = factory.create_instance(
                session=session,
                template_code="workflow/assay/hla-typing/1.2",
                name="pytest-workflow",
                create_children=True,
            )

            assert session.query(generic_instance_lineage).count() > 0

            action_ds = wf.json_addl["action_groups"]["core_actions"]["create_note"]
            res = dispatcher.execute_action(
                session=session,
                instance=wf,
                action_group="core_actions",
                action_key="create_note",
                action_ds=action_ds,
                captured_data={"note_text": "hi"},
                user="pytest",
            )
            assert res["status"] == "success"

            a = (
                session.query(action_instance)
                .filter(action_instance.subtype == "create_note")
                .order_by(action_instance.created_dt.desc())
                .first()
            )
            assert a is not None
            assert a.euid.startswith("XX")

            action_tmpl = tm.get_template(session, "action/core/create-note/1.0")
            assert action_tmpl is not None
            assert str(a.template_uuid) == str(action_tmpl.uuid)

            assert session.query(audit_log).count() > 0

            wf_uuid = wf.uuid
            session.delete(wf)
            session.flush()
            is_deleted = session.execute(
                text("SELECT is_deleted FROM generic_instance WHERE uuid = :u"),
                {"u": wf_uuid},
            ).scalar_one()
            assert is_deleted is True

        conn.engine.dispose()
    finally:
        _drop_schema(dsn, schema_name)


def test_postgres_schema_install_is_idempotent():
    dsn = os.environ.get("TAPDB_TEST_DSN")
    if not dsn:
        pytest.skip("Set TAPDB_TEST_DSN to run Postgres integration tests")

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    schema_name = f"tapdb_test_idem_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    _install_schema(dsn, schema_name, schema_sql_path)

    try:
        # Re-applying the schema to the same schema should not error.
        _apply_schema(dsn, schema_name, schema_sql_path)
    finally:
        _drop_schema(dsn, schema_name)


def test_postgres_restricted_role_schema_install_and_uuid_fallback():
    """Production-like behavior:

    - Connect as a non-superuser role to a fresh DB
    - Schema install must not fail if pgcrypto can't be installed
    - tapdb_gen_uuid() must still work (fallback UUID generation)
    """
    dsn = os.environ.get("TAPDB_TEST_DSN")
    if not dsn:
        pytest.skip("Set TAPDB_TEST_DSN to run Postgres integration tests")

    try:
        import psycopg2
    except Exception as e:  # pragma: no cover
        pytest.skip(f"psycopg2 unavailable: {e}")

    from sqlalchemy.engine import make_url

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    suffix = uuid.uuid4().hex[:10]
    role = f"tapdb_restricted_{suffix}"
    db = f"tapdb_restricted_db_{suffix}"
    pwd = f"pw_{suffix}"

    from psycopg2 import sql as psql

    admin_conn = psycopg2.connect(dsn)
    admin_conn.autocommit = True
    admin_cur = admin_conn.cursor()
    try:
        admin_cur.execute(psql.SQL("DROP DATABASE IF EXISTS {};").format(psql.Identifier(db)))
        admin_cur.execute(psql.SQL("DROP ROLE IF EXISTS {};").format(psql.Identifier(role)))
        admin_cur.execute(
            psql.SQL("CREATE ROLE {} LOGIN PASSWORD %s;").format(psql.Identifier(role)),
            [pwd],
        )
        # DB is owned by the admin user, not the restricted role.
        admin_cur.execute(psql.SQL("CREATE DATABASE {};").format(psql.Identifier(db)))
        admin_cur.execute(
            psql.SQL("GRANT CONNECT ON DATABASE {} TO {};").format(
                psql.Identifier(db), psql.Identifier(role)
            )
        )
        # Explicitly prevent extension installs by the restricted role.
        admin_cur.execute(
            psql.SQL("REVOKE CREATE ON DATABASE {} FROM PUBLIC;").format(psql.Identifier(db))
        )
        admin_cur.execute(
            psql.SQL("REVOKE CREATE ON DATABASE {} FROM {};").format(
                psql.Identifier(db), psql.Identifier(role)
            )
        )
    finally:
        admin_cur.close()
        admin_conn.close()

    admin_db_dsn = str(make_url(dsn).set(database=db))
    admin_db_conn = psycopg2.connect(admin_db_dsn)
    admin_db_conn.autocommit = True
    admin_db_cur = admin_db_conn.cursor()

    schema_name = f"tapdb_restricted_schema_{suffix}"
    try:
        # Ensure pgcrypto is absent at start.
        admin_db_cur.execute("DROP EXTENSION IF EXISTS pgcrypto;")
        admin_db_cur.execute(
            psql.SQL("CREATE SCHEMA {} AUTHORIZATION {};").format(
                psql.Identifier(schema_name), psql.Identifier(role)
            )
        )
    finally:
        admin_db_cur.close()
        admin_db_conn.close()

    role_db_dsn = str(make_url(dsn).set(username=role, password=pwd, database=db))

    try:
        # Apply into the pre-created schema; extension install should be skipped (insufficient_privilege).
        _apply_schema(role_db_dsn, schema_name, schema_sql_path)

        role_conn = psycopg2.connect(role_db_dsn)
        role_conn.autocommit = True
        role_cur = role_conn.cursor()
        try:
            role_cur.execute(psql.SQL("SET search_path TO {};").format(psql.Identifier(schema_name)))

            # Force the tapdb_gen_uuid() fallback branch in a portable way even
            # if the cluster provides gen_random_uuid() (via pgcrypto or otherwise).
            role_cur.execute(
                """
                CREATE OR REPLACE FUNCTION gen_random_uuid()
                RETURNS uuid AS $$
                BEGIN
                    RAISE EXCEPTION 'blocked for test' USING ERRCODE = '0A000';
                END;
                $$ LANGUAGE plpgsql;
                """
            )

            role_cur.execute("SELECT tapdb_gen_uuid()::text;")
            (uuid_txt,) = role_cur.fetchone()
            uuid.UUID(uuid_txt)
        finally:
            role_cur.close()
            role_conn.close()
    finally:
        # Clean up DB + role using admin connection
        admin_conn = psycopg2.connect(dsn)
        admin_conn.autocommit = True
        admin_cur = admin_conn.cursor()
        try:
            admin_cur.execute(psql.SQL("DROP DATABASE IF EXISTS {};").format(psql.Identifier(db)))
            admin_cur.execute(psql.SQL("DROP ROLE IF EXISTS {};").format(psql.Identifier(role)))
        finally:
            admin_cur.close()
            admin_conn.close()

