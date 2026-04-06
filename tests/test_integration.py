"""Postgres integration test for Phase 2 acceptance."""

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
from daylily_tapdb.schema_inventory import (
    build_expected_schema_inventory,
    diff_schema_inventory,
    load_live_schema_inventory,
    schema_asset_files,
)
from daylily_tapdb.templates.manager import TemplateManager
from daylily_tapdb.templates.mutation import allow_template_mutations
from tests.conftest import resolve_tapdb_test_dsn

_UNSET = object()


def _set_runtime_prefix_env(monkeypatch, prefix=_UNSET) -> None:
    monkeypatch.delenv("MERIDIAN_ENVIRONMENT", raising=False)
    monkeypatch.delenv("LSMC_ENV", raising=False)
    if prefix is _UNSET:
        monkeypatch.setenv("MERIDIAN_DOMAIN_CODE", "T")
    else:
        monkeypatch.setenv("MERIDIAN_DOMAIN_CODE", prefix)


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


def _seed_templates(session, tmpl_list: list[dict]) -> None:
    disc_to_cls = {
        "action_template": action_template,
        "workflow_template": workflow_template,
        "workflow_step_template": workflow_step_template,
    }

    with allow_template_mutations():
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


def _seed_identity_prefixes(session, prefix: str = "AGX") -> None:
    session.execute(
        text(
            """
            INSERT INTO tapdb_identity_prefix_config(
                entity,
                domain_code,
                issuer_app_code,
                prefix
            )
            VALUES
                ('generic_template', '', '', :prefix),
                ('generic_instance', '', '', :prefix),
                ('generic_instance_lineage', '', '', :prefix),
                ('audit_log', '', '', :prefix)
            ON CONFLICT (entity, domain_code, issuer_app_code) DO UPDATE
              SET prefix = EXCLUDED.prefix, updated_dt = NOW();
            """
        ),
        {"prefix": prefix},
    )
    session.execute(
        text(f'CREATE SEQUENCE IF NOT EXISTS "{prefix.lower()}_instance_seq"')
    )


def _integration_templates() -> list[dict]:
    return [
        {
            "name": "Create Note",
            "polymorphic_discriminator": "action_template",
            "category": "action",
            "type": "core",
            "subtype": "create-note",
            "version": "1.0",
            "instance_prefix": "XX",
            "is_singleton": False,
            "bstatus": "active",
            "json_addl": {
                "action_definition": {
                    "description": "Add a note to any object",
                    "properties": {"name": "Create Note", "comments": ""},
                    "action_type": "annotation",
                    "required_fields": ["note_text"],
                }
            },
        },
        {
            "name": "Available Queue",
            "polymorphic_discriminator": "workflow_step_template",
            "category": "workflow_step",
            "type": "queue",
            "subtype": "available",
            "version": "1.0",
            "instance_prefix": "WSX",
            "is_singleton": False,
            "bstatus": "active",
            "json_addl": {
                "properties": {"name": "Available Queue"},
                "instantiation_layouts": [],
            },
        },
        {
            "name": "HLA Typing",
            "polymorphic_discriminator": "workflow_template",
            "category": "workflow",
            "type": "assay",
            "subtype": "hla-typing",
            "version": "1.2",
            "instance_prefix": "WX",
            "is_singleton": False,
            "bstatus": "active",
            "json_addl": {
                "properties": {"name": "HLA Typing"},
                "action_imports": {
                    "create_note": "action/core/create-note/1.0",
                },
                "instantiation_layouts": [
                    {
                        "relationship_type": "contains",
                        "child_templates": [
                            "workflow_step/queue/available/1.0",
                        ],
                    }
                ],
            },
        },
    ]


def test_postgres_schema_seed_action_audit_soft_delete(monkeypatch, pytestconfig):
    dsn = resolve_tapdb_test_dsn(pytestconfig)
    _set_runtime_prefix_env(monkeypatch)

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
            _seed_identity_prefixes(session, "AGX")
            _seed_templates(session, _integration_templates())

            tenant_id = uuid.uuid4()
            wf = factory.create_instance(
                session=session,
                template_code="workflow/assay/hla-typing/1.2",
                name="pytest-workflow",
                create_children=True,
                tenant_id=tenant_id,
            )

            assert session.query(generic_instance_lineage).count() > 0
            stored_tenant_id = session.execute(
                text("SELECT tenant_id FROM generic_instance WHERE uid = :u"),
                {"u": wf.uid},
            ).scalar_one()
            assert str(stored_tenant_id) == str(tenant_id)

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
            assert a.euid.startswith("T:XX-")

            action_tmpl = tm.get_template(session, "action/core/create-note/1.0")
            assert action_tmpl is not None
            assert str(a.template_uid) == str(action_tmpl.uid)

            assert session.query(audit_log).count() > 0
            latest_audit_euid = session.execute(
                text("SELECT euid FROM audit_log ORDER BY uid DESC LIMIT 1")
            ).scalar_one()
            assert latest_audit_euid.startswith("T:AGX-")

            wf_uid = wf.uid
            session.delete(wf)
            session.flush()
            is_deleted = session.execute(
                text("SELECT is_deleted FROM generic_instance WHERE uid = :u"),
                {"u": wf_uid},
            ).scalar_one()
            assert is_deleted is True

        conn.engine.dispose()
    finally:
        _drop_schema(dsn, schema_name)


@pytest.mark.parametrize(
    ("prefix_env", "expected_prefix"),
    [
        ("T", "T:AGX-"),
        ("S", "S:AGX-"),
    ],
)
def test_postgres_identity_triggers_respect_runtime_prefix_override(
    monkeypatch, prefix_env, expected_prefix, pytestconfig
):
    dsn = resolve_tapdb_test_dsn(pytestconfig)
    _set_runtime_prefix_env(monkeypatch, prefix_env)

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    schema_name = (
        f"tapdb_test_prefix_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    _install_schema(dsn, schema_name, schema_sql_path)

    try:
        conn = TAPDBConnection(db_url=dsn, app_username="pytest")
        with conn.session_scope(commit=False) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))
            _seed_identity_prefixes(session, "AGX")
            row = session.execute(
                text(
                    """
                    INSERT INTO generic_template (
                        name, polymorphic_discriminator, category, type, subtype, version,
                        instance_prefix, bstatus
                    ) VALUES (
                        'prefix-template', 'generic_template',
                        'generic', 'test', 'prefix', '1.0',
                        'AGX', 'active'
                    )
                    RETURNING uid, euid, euid_prefix, euid_seq;
                    """
                )
            ).one()
            assert row.euid.startswith(expected_prefix)
            assert row.euid_prefix == "AGX"
            assert row.euid_seq > 0

            updated = session.execute(
                text(
                    """
                    UPDATE generic_template
                    SET name = 'prefix-template-renamed'
                    WHERE uid = :uid
                    RETURNING euid, euid_prefix, euid_seq;
                    """
                ),
                {"uid": row.uid},
            ).one()
            assert updated.euid == row.euid
            assert updated.euid_prefix == row.euid_prefix
            assert updated.euid_seq == row.euid_seq
    finally:
        _drop_schema(dsn, schema_name)


def test_postgres_schema_install_is_idempotent(pytestconfig):
    dsn = resolve_tapdb_test_dsn(pytestconfig)

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    schema_name = (
        f"tapdb_test_idem_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    _install_schema(dsn, schema_name, schema_sql_path)

    try:
        # Re-applying the schema to the same schema should not error.
        _apply_schema(dsn, schema_name, schema_sql_path)
    finally:
        _drop_schema(dsn, schema_name)


def test_postgres_schema_drift_check_smoke(pytestconfig):
    dsn = resolve_tapdb_test_dsn(pytestconfig)

    repo_root = Path(__file__).resolve().parents[1]
    schema_root = repo_root / "schema"
    schema_sql_path = schema_root / "tapdb_schema.sql"

    schema_name = (
        f"tapdb_test_drift_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    _install_schema(dsn, schema_name, schema_sql_path)

    try:
        conn = TAPDBConnection(db_url=dsn, app_username="pytest")
        with conn.session_scope(commit=True) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))
            _seed_identity_prefixes(session, "AGX")

        expected = build_expected_schema_inventory(
            schema_asset_files(schema_root),
            dynamic_sequence_name="agx_instance_seq",
        )

        with conn.session_scope(commit=False) as session:
            live = load_live_schema_inventory(session, schema_name=schema_name)
        clean_diff = diff_schema_inventory(
            expected,
            live,
            env="test",
            database="tapdb_test",
            strict=True,
        )
        assert clean_diff.has_drift is False

        with conn.session_scope(commit=True) as session:
            session.execute(text(f"SET LOCAL search_path TO {schema_name}"))
            session.execute(text("DROP INDEX IF EXISTS idx_generic_instance_euid"))

        with conn.session_scope(commit=False) as session:
            drifted_live = load_live_schema_inventory(session, schema_name=schema_name)
        drifted = diff_schema_inventory(
            expected,
            drifted_live,
            env="test",
            database="tapdb_test",
            strict=True,
        )
        assert drifted.has_drift is True
        assert (
            "generic_instance.idx_generic_instance_euid" in drifted.missing["indexes"]
        )
    finally:
        _drop_schema(dsn, schema_name)


def test_postgres_restricted_role_schema_install_and_identity_triggers(pytestconfig):
    """Production-like behavior under restricted role privileges.

    - Connect as a non-superuser role to a fresh DB
    - Schema install succeeds without UUID extension helpers
    - Identity/EUID triggers produce bigint ID + Meridian EUID fields
    """
    dsn = resolve_tapdb_test_dsn(pytestconfig)

    try:
        import psycopg2
    except Exception as e:  # pragma: no cover
        pytest.skip(f"psycopg2 unavailable: {e}")

    from sqlalchemy.engine import make_url

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    suffix = f"{int(time.time())}{random.randint(1, 1_000_000)}"[-10:]
    role = f"tapdb_restricted_{suffix}"
    db = f"tapdb_restricted_db_{suffix}"
    pwd = f"pw_{suffix}"

    from psycopg2 import sql as psql

    admin_conn = psycopg2.connect(dsn)
    admin_conn.autocommit = True
    admin_cur = admin_conn.cursor()
    try:
        admin_cur.execute(
            psql.SQL("DROP DATABASE IF EXISTS {};").format(psql.Identifier(db))
        )
        admin_cur.execute(
            psql.SQL("DROP ROLE IF EXISTS {};").format(psql.Identifier(role))
        )
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
            psql.SQL("REVOKE CREATE ON DATABASE {} FROM PUBLIC;").format(
                psql.Identifier(db)
            )
        )
        admin_cur.execute(
            psql.SQL("REVOKE CREATE ON DATABASE {} FROM {};").format(
                psql.Identifier(db), psql.Identifier(role)
            )
        )
    finally:
        admin_cur.close()
        admin_conn.close()

    # NOTE: SQLAlchemy URL stringification hides passwords by default (e.g. "***"),
    # which breaks psycopg2 auth when we pass the DSN onward.
    admin_db_dsn = make_url(dsn).set(database=db).render_as_string(hide_password=False)
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

    role_db_dsn = (
        make_url(dsn)
        .set(username=role, password=pwd, database=db)
        .render_as_string(hide_password=False)
    )

    try:
        # Apply into the pre-created schema; extension install
        # should be skipped (insufficient_privilege).
        _apply_schema(role_db_dsn, schema_name, schema_sql_path)

        role_conn = psycopg2.connect(role_db_dsn)
        role_conn.autocommit = True
        role_cur = role_conn.cursor()
        try:
            role_cur.execute(
                psql.SQL("SET search_path TO {};").format(psql.Identifier(schema_name))
            )
            role_cur.execute("SET session.current_domain_code = 'T'")
            role_cur.execute("SET session.current_app_code = 'TAPD'")
            role_cur.execute(
                """
                INSERT INTO tapdb_identity_prefix_config(
                    entity,
                    domain_code,
                    issuer_app_code,
                    prefix
                )
                VALUES
                    ('generic_template', '', '', 'AGX'),
                    ('generic_instance', '', '', 'AGX'),
                    ('generic_instance_lineage', '', '', 'AGX'),
                    ('audit_log', '', '', 'AGX')
                ON CONFLICT (entity, domain_code, issuer_app_code) DO UPDATE
                  SET prefix = EXCLUDED.prefix, updated_dt = NOW();
                """
            )
            role_cur.execute('CREATE SEQUENCE IF NOT EXISTS "agx_instance_seq"')

            role_cur.execute(
                """
                INSERT INTO generic_template (
                    name, polymorphic_discriminator, category, type, subtype, version,
                    instance_prefix, bstatus
                ) VALUES (
                    'restricted-template', 'generic_template',
                    'generic', 'test', 'restricted', '1.0',
                    'AGX', 'active'
                )
                RETURNING uid, euid, euid_prefix, euid_seq;
                """
            )
            row = role_cur.fetchone()
            assert row is not None
            assert isinstance(row[0], int)
            assert row[0] > 0
            assert isinstance(row[1], str) and row[1].startswith("T:AGX-")
            assert row[2] == "AGX"
            assert isinstance(row[3], int) and row[3] > 0
        finally:
            role_cur.close()
            role_conn.close()
    finally:
        # Clean up DB + role using admin connection
        admin_conn = psycopg2.connect(dsn)
        admin_conn.autocommit = True
        admin_cur = admin_conn.cursor()
        try:
            admin_cur.execute(
                psql.SQL("DROP DATABASE IF EXISTS {};").format(psql.Identifier(db))
            )
            admin_cur.execute(
                psql.SQL("DROP ROLE IF EXISTS {};").format(psql.Identifier(role))
            )
        finally:
            admin_cur.close()
            admin_conn.close()
