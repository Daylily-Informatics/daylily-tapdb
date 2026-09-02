"""Postgres integration test for Phase 2 acceptance."""

import random
import secrets
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import pytest
from sqlalchemy import text

from daylily_tapdb.actions.dispatcher import ActionDispatcher
from daylily_tapdb.backup.service import connection_config_for_role
from daylily_tapdb.cli.db_config import get_db_config
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
    diff_schema_inventory,
    load_expected_schema_inventory,
    load_live_schema_inventory,
    schema_asset_files,
)
from daylily_tapdb.templates.manager import TemplateManager
from daylily_tapdb.templates.mutation import allow_template_mutations

_UNSET = object()


def _conn_kwargs(**overrides):
    values = {
        "db_user": "tapdb",
        "app_username": "pytest",
        "domain_code": "T",
        "owner_repo_name": "daylily-tapdb",
        "echo_sql": False,
        "engine_type": "local",
    }
    values.update(overrides)
    return values


def _config_identity(pytestconfig) -> str:
    config_path = str(pytestconfig.getoption("--tapdb-config") or "").strip()
    if not config_path:
        pytest.skip("Set --tapdb-config to run Postgres integration tests")
    return str(Path(config_path).resolve())


def _operator_dsn(pytestconfig) -> str:
    config_identity = _config_identity(pytestconfig)
    cfg = get_db_config(config_path=config_identity)
    operator = connection_config_for_role(cfg, "operator")
    return (
        "postgresql://"
        f"{quote(str(operator['user']), safe='')}:"
        f"{quote(str(operator.get('password') or ''), safe='')}@"
        f"{operator['host']}:{operator['port']}/"
        f"{quote(str(operator['database']), safe='')}"
    )


def _dsn_for_role(dsn: str, role: str, password: str) -> str:
    from sqlalchemy.engine import make_url

    return (
        make_url(dsn)
        .set(username=role, password=password)
        .render_as_string(hide_password=False)
    )


def _provision_runtime_principal(
    operator_dsn: str,
    schema_name: str,
    *,
    config_identity: str,
    domain_code: str,
    owner_repo_name: str,
    tenant_id: uuid.UUID | None,
    allow_global_rows: bool,
) -> str:
    import psycopg2
    from psycopg2 import sql

    role = f"tapdb_test_runtime_{uuid.uuid4().hex[:20]}"
    password = secrets.token_urlsafe(24)
    connection = psycopg2.connect(operator_dsn)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        cursor.execute(
            sql.SQL(
                "CREATE ROLE {} LOGIN NOSUPERUSER NOBYPASSRLS "
                "NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD {}"
            ).format(sql.Identifier(role), sql.Literal(password))
        )
        cursor.execute("SELECT current_database()")
        database = cursor.fetchone()[0]
        cursor.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database), sql.Identifier(role)
            )
        )
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(role)
            )
        )
        cursor.execute(
            sql.SQL(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO {}"
            ).format(sql.Identifier(schema_name), sql.Identifier(role))
        )
        cursor.execute(
            sql.SQL(
                "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {} TO {}"
            ).format(sql.Identifier(schema_name), sql.Identifier(role))
        )
        cursor.execute("SELECT current_user")
        operator_role = cursor.fetchone()[0]
        cursor.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} "
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}"
            ).format(
                sql.Identifier(operator_role),
                sql.Identifier(schema_name),
                sql.Identifier(role),
            )
        )
        cursor.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} "
                "GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {}"
            ).format(
                sql.Identifier(operator_role),
                sql.Identifier(schema_name),
                sql.Identifier(role),
            )
        )
        cursor.execute(
            sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema_name))
        )
        cursor.execute(
            "INSERT INTO tapdb_runtime_principal_scope ("
            "role_name, config_identity, schema_name, domain_code, "
            "issuer_app_code, tenant_id, allow_global_rows) VALUES ("
            "%s, %s, %s, %s, %s, %s, %s)",
            (
                role,
                config_identity,
                schema_name,
                domain_code,
                owner_repo_name,
                None if tenant_id is None else str(tenant_id),
                allow_global_rows,
            ),
        )
        cursor.execute(
            sql.SQL("REVOKE ALL ON TABLE tapdb_runtime_principal_scope FROM {}").format(
                sql.Identifier(role)
            )
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()
    return _dsn_for_role(operator_dsn, role, password)


def _install_bound_schema(
    pytestconfig,
    schema_name: str,
    schema_sql_path: Path,
    *,
    domain_code: str,
    owner_repo_name: str = "daylily-tapdb",
    tenant_id: uuid.UUID | None = None,
    allow_global_rows: bool = True,
) -> tuple[str, str, str]:
    config_identity = _config_identity(pytestconfig)
    operator_dsn = _operator_dsn(pytestconfig)
    _install_schema(
        operator_dsn,
        schema_name,
        schema_sql_path,
        config_identity=config_identity,
    )
    runtime_dsn = _provision_runtime_principal(
        operator_dsn,
        schema_name,
        config_identity=config_identity,
        domain_code=domain_code,
        owner_repo_name=owner_repo_name,
        tenant_id=tenant_id,
        allow_global_rows=allow_global_rows,
    )
    return operator_dsn, runtime_dsn, config_identity


def _runtime_connection(
    *,
    dsn: str,
    schema_name: str,
    config_identity: str,
    app_username: str = "pytest",
    domain_code: str = "T",
    owner_repo_name: str = "daylily-tapdb",
    tenant_id: uuid.UUID | None = None,
    allow_global_rows: bool = False,
    connection_role: str = "runtime",
) -> TAPDBConnection:
    return TAPDBConnection(
        **_conn_kwargs(
            db_url=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username=app_username,
            domain_code=domain_code,
            owner_repo_name=owner_repo_name,
            tenant_id=tenant_id,
            allow_global_rows=allow_global_rows,
            connection_role=connection_role,
        )
    )


def _set_runtime_prefix_env(monkeypatch, prefix=_UNSET) -> None:
    monkeypatch.delenv("MERIDIAN_ENVIRONMENT", raising=False)
    monkeypatch.delenv("LSMC_ENV", raising=False)
    if prefix is _UNSET:
        monkeypatch.setenv("MERIDIAN_DOMAIN_CODE", "T")
    else:
        monkeypatch.setenv("MERIDIAN_DOMAIN_CODE", prefix)


def _install_schema(
    dsn: str, schema_name: str, schema_sql_path: Path, *, config_identity: str
) -> None:
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

    _apply_schema(
        dsn,
        schema_name,
        schema_sql_path,
        config_identity=config_identity,
    )


def _apply_schema(
    dsn: str, schema_name: str, schema_sql_path: Path, *, config_identity: str
) -> None:
    """Apply the canonical schema and RLS assets into an existing schema.

    This intentionally does *not* pre-install pgcrypto.
    schema/tapdb_schema.sql already handles pgcrypto availability/privileges gracefully.
    """
    try:
        import psycopg2
    except Exception as e:  # pragma: no cover
        pytest.skip(f"psycopg2 unavailable: {e}")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute(f"SET search_path TO {schema_name};")
        for name, value in (
            ("session.current_config_identity", config_identity),
            ("session.current_schema_name", schema_name),
            ("session.current_domain_code", "T"),
            ("session.current_owner_repo_name", "daylily-tapdb"),
            ("session.current_tenant_id", ""),
            ("session.current_username", "migration:pytest-schema-apply"),
            ("session.allow_global_rows", "true"),
        ):
            cur.execute("SELECT set_config(%s, %s, true)", (name, value))
        cur.execute(schema_sql_path.read_text())
        cur.execute((schema_sql_path.parent / "rls.sql").read_text())
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _drop_schema(
    dsn: str, schema_name: str, *, runtime_dsns: tuple[str, ...] = ()
) -> None:
    try:
        import psycopg2
    except Exception:
        return

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
        if runtime_dsns:
            from psycopg2 import sql
            from sqlalchemy.engine import make_url

            for runtime_dsn in runtime_dsns:
                role = make_url(runtime_dsn).username
                if not role:
                    raise RuntimeError("test runtime DSN is missing its role")
                cur.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                cur.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
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


def _seed_identity_prefixes(
    session,
    prefix: str = "AGX",
    *,
    domain_code: str = "T",
    owner_repo_name: str = "daylily-tapdb",
) -> None:
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
                ('generic_template', :domain_code, :owner_repo_name, :prefix),
                ('generic_instance', :domain_code, :owner_repo_name, :prefix),
                ('generic_instance_lineage', :domain_code, :owner_repo_name, :prefix),
                ('audit_log', :domain_code, :owner_repo_name, :prefix)
            ON CONFLICT (entity, domain_code, issuer_app_code) DO NOTHING;
            """
        ),
        {
            "prefix": prefix,
            "domain_code": domain_code,
            "owner_repo_name": owner_repo_name,
        },
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
    _set_runtime_prefix_env(monkeypatch)

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    schema_name = f"tapdb_test_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    tenant_id = uuid.uuid4()
    operator_dsn, dsn, config_identity = _install_bound_schema(
        pytestconfig,
        schema_name,
        schema_sql_path,
        domain_code="T",
        tenant_id=tenant_id,
    )

    try:
        seed_conn = _runtime_connection(
            dsn=operator_dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-template-seed",
            tenant_id=tenant_id,
            allow_global_rows=True,
            connection_role="operator",
        )
        with seed_conn.session_scope(commit=True) as session:
            _seed_identity_prefixes(session, "AGX")
            _seed_templates(session, _integration_templates())

        conn = _runtime_connection(
            dsn=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-action-runtime",
            tenant_id=tenant_id,
            allow_global_rows=True,
        )
        tm = TemplateManager()
        factory = InstanceFactory(tm, domain_code="T")

        class TestDispatcher(ActionDispatcher):
            def do_action_create_note(self, instance, action_ds, captured_data):
                return {"status": "success", "message": "ok"}

        dispatcher = TestDispatcher()

        with conn.session_scope(commit=False) as session:
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
            assert a.euid.startswith("T-XX-")

            action_tmpl = tm.get_template(
                session,
                "action/core/create-note/1.0",
                domain_code="T",
            )
            assert action_tmpl is not None
            assert str(a.template_uid) == str(action_tmpl.uid)

            assert session.query(audit_log).count() > 0
            latest_audit_euid = session.execute(
                text("SELECT euid FROM audit_log ORDER BY uid DESC LIMIT 1")
            ).scalar_one()
            assert latest_audit_euid.startswith("T-AGX-")

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
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn,))


@pytest.mark.parametrize(
    ("prefix_env", "expected_prefix"),
    [
        ("T", "T-AGX-"),
        ("S", "S-AGX-"),
    ],
)
def test_postgres_identity_triggers_respect_runtime_prefix_override(
    monkeypatch, prefix_env, expected_prefix, pytestconfig
):
    _set_runtime_prefix_env(monkeypatch, prefix_env)

    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    schema_name = (
        f"tapdb_test_prefix_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    operator_dsn, dsn, config_identity = _install_bound_schema(
        pytestconfig,
        schema_name,
        schema_sql_path,
        domain_code=prefix_env,
    )

    try:
        seed_conn = _runtime_connection(
            dsn=operator_dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username=f"pytest-prefix-{prefix_env.lower()}-seed",
            domain_code=prefix_env,
            allow_global_rows=True,
            connection_role="operator",
        )
        with seed_conn.session_scope(commit=True) as session:
            _seed_identity_prefixes(
                session,
                "AGX",
                domain_code=prefix_env,
            )

        conn = _runtime_connection(
            dsn=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username=f"pytest-prefix-{prefix_env.lower()}",
            domain_code=prefix_env,
            allow_global_rows=True,
        )
        with conn.session_scope(commit=False) as session:
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
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn,))


def test_postgres_schema_install_is_idempotent(pytestconfig):
    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"

    schema_name = (
        f"tapdb_test_idem_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    operator_dsn, dsn, config_identity = _install_bound_schema(
        pytestconfig,
        schema_name,
        schema_sql_path,
        domain_code="T",
    )

    try:
        # Re-applying the schema to the same schema should not error.
        _apply_schema(
            operator_dsn,
            schema_name,
            schema_sql_path,
            config_identity=config_identity,
        )
    finally:
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn,))


def test_postgres_schema_drift_check_smoke(pytestconfig):
    repo_root = Path(__file__).resolve().parents[1]
    schema_root = repo_root / "schema"
    schema_sql_path = schema_root / "tapdb_schema.sql"

    schema_name = (
        f"tapdb_test_drift_{int(time.time())}_{random.randint(1, 1_000_000_000)}"
    )
    operator_dsn, dsn, config_identity = _install_bound_schema(
        pytestconfig,
        schema_name,
        schema_sql_path,
        domain_code="T",
    )

    try:
        conn = _runtime_connection(
            dsn=operator_dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-schema-drift",
            allow_global_rows=True,
            connection_role="operator",
        )
        seed_conn = _runtime_connection(
            dsn=operator_dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-schema-drift-seed",
            allow_global_rows=True,
            connection_role="operator",
        )
        with seed_conn.session_scope(commit=True) as session:
            _seed_identity_prefixes(session, "AGX")

        expected = load_expected_schema_inventory(
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

        operator = _runtime_connection(
            dsn=operator_dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-schema-drift-operator",
            connection_role="operator",
            allow_global_rows=True,
        )
        with operator.session_scope(commit=True) as session:
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
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn,))


def test_postgres_restricted_role_schema_install_and_identity_triggers(pytestconfig):
    """Configured NOBYPASSRLS runtime role is constrained by forced RLS."""
    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"
    suffix = f"{int(time.time())}{random.randint(1, 1_000_000)}"[-10:]
    schema_name = f"tapdb_restricted_schema_{suffix}"
    operator_dsn, dsn, config_identity = _install_bound_schema(
        pytestconfig,
        schema_name,
        schema_sql_path,
        domain_code="T",
    )
    other_dsn = _provision_runtime_principal(
        operator_dsn,
        schema_name,
        config_identity=config_identity,
        domain_code="S",
        owner_repo_name="daylily-tapdb",
        tenant_id=None,
        allow_global_rows=True,
    )

    try:
        seed_conn = _runtime_connection(
            dsn=operator_dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-restricted-seed",
            allow_global_rows=True,
            connection_role="operator",
        )
        with seed_conn.session_scope(commit=True) as session:
            _seed_identity_prefixes(session, "AGX")

        runtime = _runtime_connection(
            dsn=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-restricted-runtime",
            allow_global_rows=True,
        )
        with runtime.session_scope(commit=True) as session:
            role_state = session.execute(
                text(
                    "SELECT current_user, rolsuper, rolbypassrls "
                    "FROM pg_roles WHERE rolname = current_user"
                )
            ).one()
            assert role_state.current_user
            assert role_state.rolsuper is False
            assert role_state.rolbypassrls is False
            row = session.execute(
                text(
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
            ).one()
            assert isinstance(row.uid, int) and row.uid > 0
            assert isinstance(row.euid, str) and row.euid.startswith("T-AGX-")
            assert row.euid_prefix == "AGX"
            assert isinstance(row.euid_seq, int) and row.euid_seq > 0

        other_scope = _runtime_connection(
            dsn=other_dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            app_username="pytest-restricted-other-scope",
            domain_code="S",
            allow_global_rows=True,
        )
        with other_scope.session_scope(commit=False) as session:
            assert (
                session.execute(
                    text(
                        "SELECT count(*) FROM generic_template "
                        "WHERE subtype = 'restricted'"
                    )
                ).scalar_one()
                == 0
            )
            assert (
                session.execute(
                    text("SELECT current_setting('session.current_schema_name')")
                ).scalar_one()
                == schema_name
            )
            assert (
                session.execute(
                    text("SELECT current_setting('session.current_config_identity')")
                ).scalar_one()
                == config_identity
            )
    finally:
        _drop_schema(operator_dsn, schema_name, runtime_dsns=(dsn, other_dsn))
