"""Real-PostgreSQL proof for client-owned use of TapDB reserved core types."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import yaml
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.euid import EUIDConfig
from daylily_tapdb.factory import InstanceFactory
from daylily_tapdb.services.external_refs import (
    TypedExternalReferenceSpec,
    create_or_reuse_typed_external_reference,
    project_outbound_typed_references,
)
from daylily_tapdb.services.graph_payloads import _v2_edge
from daylily_tapdb.templates.manager import TemplateManager

runner = CliRunner()


def test_client_runtime_creates_typed_xrf_from_operator_seeded_core(
    pg_instance, tmp_path
):
    clear_cli_context()
    set_cli_context(config_path=pg_instance["config_path"])
    applied = runner.invoke(
        app,
        ["--config", str(pg_instance["config_path"]), "db", "schema", "apply"],
    )
    assert applied.exit_code == 0, applied.output
    seeded = runner.invoke(
        app,
        ["--config", str(pg_instance["config_path"]), "db", "data", "seed"],
    )
    assert seeded.exit_code == 0, seeded.output

    schema_name = str(pg_instance["schema_name"])
    client_owner = "client-service"
    client_role = f"tapdb_client_{uuid.uuid4().hex[:10]}"
    tenant_id = uuid.uuid4()

    operator = create_engine(
        pg_instance["operator_dsn"],
        connect_args={"options": f"-csearch_path={schema_name}"},
    )
    with operator.begin() as connection:
        connection.exec_driver_sql(
            f'CREATE ROLE "{client_role}" LOGIN NOSUPERUSER NOBYPASSRLS '
            "NOCREATEDB NOCREATEROLE NOREPLICATION"
        )
        target_euid = connection.execute(
            text(
                "SELECT euid FROM generic_template "
                "WHERE issuer_app_code='daylily-tapdb' ORDER BY uid LIMIT 1"
            )
        ).scalar_one()

    domain_registry = tmp_path / "domains.json"
    prefix_registry = tmp_path / "prefixes.json"
    domain_registry.write_text(
        json.dumps({"version": "0.4.0", "domains": {"Z": {"name": "test"}}}),
        encoding="utf-8",
    )
    reserved = {
        prefix: {"issuer_app_code": "daylily-tapdb"}
        for prefix in EUIDConfig().CORE_PREFIXES.values()
    }
    reserved["SMP"] = {"issuer_app_code": client_owner}
    prefix_registry.write_text(
        json.dumps({"version": "0.4.0", "ownership": {"Z": reserved}}),
        encoding="utf-8",
    )
    client_config_dir = tmp_path / "client"
    client_source = client_config_dir / "sample" / "templates.json"
    client_source.parent.mkdir(parents=True)
    client_template = {
        "_source_file": str(client_source),
        "name": "Client sample",
        "polymorphic_discriminator": "generic_template",
        "category": "content",
        "type": "specimen",
        "subtype": "sample",
        "version": "1.0",
        "instance_prefix": "SMP",
        "bstatus": "active",
        "is_singleton": False,
        "json_addl": {"properties": {}, "instantiation_layouts": []},
    }
    client_source.write_text(
        json.dumps({"templates": [client_template]}), encoding="utf-8"
    )
    client_config = tmp_path / "client-tapdb-config.yaml"
    client_config.write_text(
        yaml.safe_dump(
            {
                "meta": {
                    "config_version": 4,
                    "client_id": "client-service",
                    "database_name": "client-service-test",
                    "owner_repo_name": client_owner,
                    "domain_registry_path": str(domain_registry),
                    "prefix_ownership_registry_path": str(prefix_registry),
                },
                "target": {
                    "engine_type": "local",
                    "host": "localhost",
                    "port": pg_instance["port"],
                    "ui_port": 18912,
                    "domain_code": "Z",
                    "user": client_role,
                    "password": "",
                    "tenant_id": str(tenant_id),
                    "allow_global_claims": True,
                    "operator": {
                        "user": pg_instance["operator_user"],
                        "password": "",
                        "secret_arn": "",
                        "iam_auth": False,
                    },
                    "database": pg_instance["database"],
                    "schema_name": schema_name,
                    "unix_socket_dir": str(pg_instance["socket_dir"]),
                },
                "safety": {
                    "safety_tier": "local",
                    "destructive_operations": "confirm_required",
                },
            }
        ),
        encoding="utf-8",
    )
    os.chmod(client_config, 0o600)
    set_cli_context(config_path=client_config)
    client_applied = runner.invoke(
        app, ["--config", str(client_config), "db", "schema", "apply"]
    )
    assert client_applied.exit_code == 0, client_applied.output
    assert client_role in client_applied.output, client_applied.output
    client_seeded = runner.invoke(
        app,
        [
            "--config",
            str(client_config),
            "db",
            "data",
            "seed",
            "--config",
            str(client_config_dir),
            "--include-workflow",
        ],
    )
    assert client_seeded.exit_code == 0, client_seeded.output

    with operator.connect() as connection:
        client_templates = connection.execute(
            text(
                "SELECT category, type, subtype, instance_prefix "
                "FROM generic_template WHERE issuer_app_code=:owner"
            ),
            {"owner": client_owner},
        ).all()
    assert len(client_templates) == 10
    assert ("reference", "external_identifier", "tapdb_object", "XRF") in (
        client_templates
    )
    assert ("actor", "user", "system", "SYS") in client_templates

    runtime_dsn = (
        f"postgresql://{client_role}:@localhost:{pg_instance['port']}/"
        f"{pg_instance['database']}"
    )
    runtime_connection = TAPDBConnection(
        db_url=runtime_dsn,
        db_user=client_role,
        app_username="pytest:client-service",
        domain_code="Z",
        owner_repo_name=client_owner,
        schema_name=schema_name,
        tenant_id=str(tenant_id),
        allow_global_rows=True,
        config_identity=str(client_config.resolve()),
        engine_type="local",
    )
    manager = TemplateManager()
    factory = InstanceFactory(manager, domain_code="Z")
    with runtime_connection as connection:
        with connection.session_scope(commit=True) as session:
            source = factory.create_instance(
                session,
                template_code="content/specimen/sample/1.0/",
                name="Client source",
                tenant_id=tenant_id,
                create_children=False,
            )
            child = factory.create_instance(
                session,
                template_code="content/specimen/sample/1.0/",
                name="Client child",
                tenant_id=tenant_id,
                create_children=False,
            )
            ordinary_lineage = factory.link_instances(
                session, source, child, relationship_type="contains"
            )
            ordinary_edge = _v2_edge(ordinary_lineage, service_id="client-service")
            result = create_or_reuse_typed_external_reference(
                session,
                source=source,
                instance_factory=factory,
                spec=TypedExternalReferenceSpec(
                    target_service_id="owning-service",
                    target_object_euid=str(target_euid),
                    relationship_type="references",
                    asserted_at=datetime.now(timezone.utc),
                    assertion_provenance="authenticated exact ownership lookup",
                ),
            )
            session.refresh(source)
            projection = project_outbound_typed_references(source)
            assert result.reference.issuer_app_code == client_owner
            assert result.reference.tenant_id is None
            assert result.lineage.issuer_app_code == client_owner
            assert result.lineage.tenant_id == tenant_id
            assert (
                ordinary_edge["data"]["presentation"]["assertion_provenance"]
                == f"tapdb.lineage:{ordinary_lineage.euid}"
            )
            assert projection[0]["target_object_euid"] == target_euid
            assert projection[0]["external_reference_euid"] == result.reference.euid
            assert projection[0]["lineage_euid"] == result.lineage.euid
    clear_cli_context()
