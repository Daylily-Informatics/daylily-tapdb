"""Real-PostgreSQL proofs for fixed-tenant runtime and complete operator scope."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.migration_identity import (
    MigrationPreflightError,
    build_migration_preflight,
)

runner = CliRunner()


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
    yield
    clear_cli_context()


def _target(pg_instance) -> dict[str, str]:
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


def _runtime_connection(
    pg_instance,
    *,
    role: str,
    tenant_id: uuid.UUID | None,
    config_identity: str,
    allow_global_claims: bool,
) -> TAPDBConnection:
    return TAPDBConnection(
        db_url=(
            f"postgresql://{role}:@localhost:{pg_instance['port']}/"
            f"{pg_instance['database']}"
        ),
        db_user=role,
        app_username="pytest:fixed-tenant-runtime",
        domain_code="Z",
        owner_repo_name="daylily-tapdb",
        schema_name=pg_instance["schema_name"],
        tenant_id=None if tenant_id is None else str(tenant_id),
        allow_global_rows=allow_global_claims,
        config_identity=config_identity,
        engine_type="local",
    )


def test_operator_inventory_is_complete_while_runtime_is_fixed_tenant(pg_instance):
    tenant_a = uuid.uuid4()
    tenant_b = uuid.uuid4()
    keys: dict[uuid.UUID | None, str | None] = {
        None: f"tenant-context-proof:{uuid.uuid4()}",
        tenant_a: None,
        tenant_b: None,
    }
    names = {
        None: f"Global scope {uuid.uuid4()}",
        tenant_a: f"Tenant A scope {uuid.uuid4()}",
        tenant_b: f"Tenant B scope {uuid.uuid4()}",
    }
    suffix = uuid.uuid4().hex[:8]
    roles = {
        tenant_a: f"tapdb_tenant_a_{suffix}",
        tenant_b: f"tapdb_tenant_b_{suffix}",
        None: f"tapdb_global_{suffix}",
    }
    config_identities = {
        tenant_a: f"pytest://tenant-a/{suffix}",
        tenant_b: f"pytest://tenant-b/{suffix}",
        None: f"pytest://global/{suffix}",
    }
    euids: set[str] = set()
    instance_uids: dict[uuid.UUID | None, int] = {}
    operator = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={pg_instance['schema_name']}"},
    )
    with operator.begin() as connection:
        connection.execute(
            text(
                "SELECT set_config('session.current_domain_code', 'Z', true), "
                "set_config('session.current_owner_repo_name', "
                "'daylily-tapdb', true), "
                "set_config('session.current_username', "
                "'pytest:operator-tenant-fixture', true), "
                "set_config('session.current_tenant_id', '', true), "
                "set_config('session.allow_global_rows', 'true', true)"
            )
        )
        template_uid = connection.execute(
            text(
                "SELECT uid FROM generic_template "
                "WHERE category='message' AND type='webhook' "
                "AND subtype='event' AND version='1.0' "
                "AND domain_code='Z' AND issuer_app_code='daylily-tapdb'"
            )
        ).scalar_one()
        for tenant_id, role in roles.items():
            connection.exec_driver_sql(
                f'CREATE ROLE "{role}" LOGIN NOSUPERUSER NOBYPASSRLS '
                "NOCREATEDB NOCREATEROLE NOREPLICATION"
            )
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
                    "'daylily-tapdb', :tenant_id, :allow_global_rows)"
                ),
                {
                    "role": role,
                    "config_identity": config_identities[tenant_id],
                    "schema_name": pg_instance["schema_name"],
                    "tenant_id": None if tenant_id is None else str(tenant_id),
                    "allow_global_rows": tenant_id == tenant_a,
                },
            )
        for tenant_id, identity_key in keys.items():
            inserted_instance = connection.execute(
                text(
                    "INSERT INTO generic_instance ("
                    "name, identity_key, tenant_id, polymorphic_discriminator, "
                    "category, type, subtype, version, template_uid, bstatus, "
                    "is_singleton, json_addl) VALUES ("
                    ":name, :identity_key, :tenant_id, 'generic_instance', "
                    "'message', 'webhook', 'event', '1.0', :template_uid, "
                    "'active', false, '{}'::jsonb) RETURNING uid, euid"
                ),
                {
                    "name": names[tenant_id],
                    "identity_key": identity_key,
                    "tenant_id": None if tenant_id is None else str(tenant_id),
                    "template_uid": template_uid,
                },
            ).one()
            instance_uids[tenant_id] = int(inserted_instance[0])
            euids.add(str(inserted_instance[1]))

    with _runtime_connection(
        pg_instance,
        role=roles[tenant_a],
        tenant_id=tenant_a,
        config_identity=config_identities[tenant_a],
        allow_global_claims=True,
    ) as runtime:
        with runtime.session_scope(commit=False) as session:
            visible = set(
                session.execute(
                    text("SELECT name FROM generic_instance WHERE name IN (:a, :b)"),
                    {"a": names[tenant_a], "b": names[tenant_b]},
                ).scalars()
            )
            assert visible == {names[tenant_a]}
            session.execute(
                text(
                    "CREATE TEMP TABLE audit_log ("
                    "rel_table_name text, rel_table_uid_fk bigint, "
                    "rel_table_euid_fk text, tenant_id uuid, domain_code text, "
                    "issuer_app_code text, changed_by text, operation_type text)"
                )
            )
            audited_uid = session.execute(
                text(
                    f'INSERT INTO "{pg_instance["schema_name"]}".'
                    "generic_instance (name, tenant_id, polymorphic_discriminator, "
                    "category, type, subtype, version, template_uid, bstatus, "
                    "is_singleton, json_addl) VALUES ("
                    ":name, :tenant_id, 'generic_instance', 'message', 'webhook', "
                    "'event', '1.0', :template_uid, 'active', false, '{}'::jsonb) "
                    "RETURNING uid"
                ),
                {
                    "name": f"Temp shadow audit proof {suffix}",
                    "tenant_id": str(tenant_a),
                    "template_uid": template_uid,
                },
            ).scalar_one()
            assert (
                session.execute(
                    text(
                        f'SELECT count(*) FROM "{pg_instance["schema_name"]}".'
                        "audit_log WHERE rel_table_name='generic_instance' "
                        "AND rel_table_uid_fk=:uid"
                    ),
                    {"uid": audited_uid},
                ).scalar_one()
                == 1
            )

            session.execute(
                text(
                    "CREATE TEMP TABLE generic_instance ("
                    "uid bigint, domain_code text, issuer_app_code text, "
                    "tenant_id uuid, category text, type text, subtype text, "
                    "version text, is_deleted boolean)"
                )
            )
            session.execute(
                text(
                    "INSERT INTO pg_temp.generic_instance VALUES "
                    "(:parent, 'Z', 'daylily-tapdb', :tenant, 'message', "
                    "'webhook', 'event', '1.0', false), "
                    "(:child, 'Z', 'daylily-tapdb', :tenant, 'message', "
                    "'webhook', 'event', '1.0', false)"
                ),
                {
                    "parent": instance_uids[tenant_a],
                    "child": instance_uids[tenant_b],
                    "tenant": str(tenant_a),
                },
            )
            nested = session.begin_nested()
            with pytest.raises(Exception, match="endpoints are unavailable"):
                session.execute(
                    text(
                        f'INSERT INTO "{pg_instance["schema_name"]}".'
                        "generic_instance_lineage (name, tenant_id, "
                        "polymorphic_discriminator, parent_instance_uid, "
                        "child_instance_uid, relationship_type, json_addl) VALUES ("
                        "'temp-shadow-attack', :tenant, 'generic_instance_lineage', "
                        ":parent, :child, 'contains', '{}'::jsonb)"
                    ),
                    {
                        "tenant": str(tenant_a),
                        "parent": instance_uids[tenant_a],
                        "child": instance_uids[tenant_b],
                    },
                )
            nested.rollback()
            session.execute(
                text("SELECT set_config('session.current_tenant_id', :tenant, true)"),
                {"tenant": str(tenant_b)},
            )
            session.execute(
                text(
                    "CREATE TEMP TABLE tapdb_runtime_principal_scope ("
                    "role_name name, tenant_id uuid)"
                )
            )
            session.execute(
                text(
                    "INSERT INTO pg_temp.tapdb_runtime_principal_scope "
                    "VALUES (session_user, :tenant)"
                ),
                {"tenant": str(tenant_b)},
            )
            session.execute(text("SET LOCAL search_path TO pg_temp, public"))
            assert (
                session.execute(
                    text(
                        f'SELECT "{pg_instance["schema_name"]}".'
                        "tapdb_current_tenant_id()"
                    )
                ).scalar_one()
                == tenant_a
            )
            still_visible = set(
                session.execute(
                    text(
                        f'SELECT name FROM "{pg_instance["schema_name"]}".'
                        "generic_instance WHERE name IN (:a, :b)"
                    ),
                    {"a": names[tenant_a], "b": names[tenant_b]},
                ).scalars()
            )
            assert still_visible == {names[tenant_a]}
            with pytest.raises(Exception, match="immutable scope binding"):
                session.execute(
                    text(
                        f'SELECT "{pg_instance["schema_name"]}".'
                        "tapdb_assert_runtime_role()"
                    )
                )

    with _runtime_connection(
        pg_instance,
        role=roles[None],
        tenant_id=None,
        config_identity=config_identities[None],
        allow_global_claims=False,
    ) as runtime:
        with runtime.session_scope(commit=False) as session:
            assert (
                session.execute(
                    text(
                        "SELECT count(*) FROM generic_instance "
                        "WHERE name IN (:global_name, :a, :b)"
                    ),
                    {
                        "global_name": names[None],
                        "a": names[tenant_a],
                        "b": names[tenant_b],
                    },
                ).scalar_one()
                == 1
            )

    migrations = Path(__file__).resolve().parents[1] / "schema" / "migrations"
    runtime_engine = create_engine(pg_instance["dsn"])
    with runtime_engine.begin() as connection:
        with pytest.raises(MigrationPreflightError, match="operator connection"):
            build_migration_preflight(
                connection, migrations_dir=migrations, target=_target(pg_instance)
            )

    with operator.begin() as connection:
        preflight = build_migration_preflight(
            connection, migrations_dir=migrations, target=_target(pg_instance)
        )
    inventory_euids = {
        row["identity"].get("euid")
        for row in preflight["tables"]["generic_instance"]["rows"]
    }
    assert euids <= inventory_euids
