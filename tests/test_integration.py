"""Postgres integration test for Phase 2 acceptance.

Gated by TAPDB_TEST_DSN.
Creates an isolated schema, installs tapdb_schema.sql, seeds templates from config,
creates an instance+children+lineage, executes an action, verifies audit+soft delete.
"""

import json
import os
import random
import time
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
        # If pgcrypto exists on this server, install it for stronger UUID generation
        # (gen_random_uuid). If it doesn't exist (e.g. minimal Postgres builds),
        # schema/tapdb_schema.sql falls back gracefully.
        cur.execute(
            "SELECT 1 FROM pg_available_extensions WHERE name = 'pgcrypto' LIMIT 1;"
        )
        pgcrypto_available = cur.fetchone() is not None
        if pgcrypto_available:
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
            except Exception as e:
                pytest.skip(f"Need pgcrypto extension privileges: {e}")

        cur.execute(f"CREATE SCHEMA {schema_name};")
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

