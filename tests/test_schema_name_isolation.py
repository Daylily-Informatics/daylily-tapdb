from __future__ import annotations

from pathlib import Path

import yaml
from sqlalchemy import func, select

from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.models.template import generic_template
from tests.test_integration import (
    _drop_schema,
    _install_schema,
    _provision_runtime_principal,
    _seed_identity_prefixes,
    _seed_templates,
)


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


def _write_tapdb_config(
    *,
    path: Path,
    pg_instance: dict,
    database_name: str,
    schema_name: str,
) -> Path:
    source_config = yaml.safe_load(
        Path(pg_instance["config_path"]).read_text(encoding="utf-8")
    )
    meta = dict(source_config["meta"])
    meta["database_name"] = database_name
    meta["config_version"] = 4
    payload = {
        "meta": meta,
        "target": {
            "engine_type": "local",
            "host": "localhost",
            "port": pg_instance["port"],
            "ui_port": 18911,
            "domain_code": "Z",
            "user": pg_instance["user"],
            "password": "",
            "database": pg_instance["database"],
            "schema_name": schema_name,
        },
        "safety": {
            "safety_tier": "local",
            "destructive_operations": "confirm_required",
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    path.chmod(0o600)
    return path


def _seed_marker_template(
    *,
    dsn: str,
    schema_name: str,
    config_identity: str,
    prefix: str,
    marker: str,
) -> None:
    with TAPDBConnection(
        **_conn_kwargs(
            db_url=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            allow_global_rows=True,
            connection_role="operator",
        )
    ) as conn:
        with conn.session_scope(commit=True) as session:
            _seed_identity_prefixes(session, prefix=prefix)
            _seed_templates(
                session,
                [
                    {
                        "name": f"{marker} Schema Marker",
                        "polymorphic_discriminator": "generic_template",
                        "category": "schema",
                        "type": "isolation",
                        "subtype": marker.lower(),
                        "version": "1.0",
                        "instance_prefix": prefix,
                        "is_singleton": False,
                        "bstatus": "active",
                        "json_addl": {"marker": marker},
                    }
                ],
            )


def _template_names(*, dsn: str, schema_name: str, config_identity: str) -> set[str]:
    with TAPDBConnection(
        **_conn_kwargs(
            db_url=dsn,
            schema_name=schema_name,
            config_identity=config_identity,
            allow_global_rows=True,
        )
    ) as conn:
        with conn.session_scope(commit=False) as session:
            return set(session.scalars(select(generic_template.name)).all())


def test_two_configured_schemas_share_one_physical_database_without_cross_reads(
    pg_instance,
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    schema_sql_path = repo_root / "schema" / "tapdb_schema.sql"
    schema_a = "tapdb_atlas_dayfly5_dev"
    schema_b = "tapdb_bloom_dayfly5_dev"
    config_a = _write_tapdb_config(
        path=tmp_path / "alpha" / "tapdb-config.yaml",
        pg_instance=pg_instance,
        database_name="atlas-dayfly5",
        schema_name=schema_a,
    )
    config_b = _write_tapdb_config(
        path=tmp_path / "beta" / "tapdb-config.yaml",
        pg_instance=pg_instance,
        database_name="bloom-dayfly5",
        schema_name=schema_b,
    )

    try:
        set_cli_context(config_path=config_a)
        cfg_a = get_db_config()
        set_cli_context(config_path=config_b)
        cfg_b = get_db_config()
    finally:
        clear_cli_context()
    assert cfg_a["database"] == cfg_b["database"] == pg_instance["database"]
    assert cfg_a["schema_name"] == schema_a
    assert cfg_b["schema_name"] == schema_b

    runtime_a = ""
    runtime_b = ""
    try:
        _install_schema(
            pg_instance["operator_dsn"],
            schema_a,
            schema_sql_path,
            config_identity=str(config_a.resolve()),
        )
        _install_schema(
            pg_instance["operator_dsn"],
            schema_b,
            schema_sql_path,
            config_identity=str(config_b.resolve()),
        )
        runtime_a = _provision_runtime_principal(
            pg_instance["operator_dsn"],
            schema_a,
            config_identity=str(config_a.resolve()),
            domain_code="T",
            owner_repo_name="daylily-tapdb",
            tenant_id=None,
            allow_global_rows=True,
        )
        runtime_b = _provision_runtime_principal(
            pg_instance["operator_dsn"],
            schema_b,
            config_identity=str(config_b.resolve()),
            domain_code="T",
            owner_repo_name="daylily-tapdb",
            tenant_id=None,
            allow_global_rows=True,
        )
        _seed_marker_template(
            dsn=pg_instance["operator_dsn"],
            schema_name=schema_a,
            config_identity=str(config_a.resolve()),
            prefix="AX",
            marker="Alpha",
        )
        _seed_marker_template(
            dsn=pg_instance["operator_dsn"],
            schema_name=schema_b,
            config_identity=str(config_b.resolve()),
            prefix="BT",
            marker="Beta",
        )

        names_a = _template_names(
            dsn=runtime_a,
            schema_name=schema_a,
            config_identity=str(config_a.resolve()),
        )
        names_b = _template_names(
            dsn=runtime_b,
            schema_name=schema_b,
            config_identity=str(config_b.resolve()),
        )

        assert "Alpha Schema Marker" in names_a
        assert "Beta Schema Marker" not in names_a
        assert "Beta Schema Marker" in names_b
        assert "Alpha Schema Marker" not in names_b

        with TAPDBConnection(
            **_conn_kwargs(
                db_url=runtime_a,
                schema_name=schema_a,
                config_identity=str(config_a.resolve()),
                allow_global_rows=True,
            )
        ) as conn_a:
            with conn_a.session_scope(commit=False) as session_a:
                count_a = session_a.scalar(
                    select(func.count()).select_from(generic_template)
                )
        with TAPDBConnection(
            **_conn_kwargs(
                db_url=runtime_b,
                schema_name=schema_b,
                config_identity=str(config_b.resolve()),
                allow_global_rows=True,
            )
        ) as conn_b:
            with conn_b.session_scope(commit=False) as session_b:
                count_b = session_b.scalar(
                    select(func.count()).select_from(generic_template)
                )

        assert count_a == len(names_a)
        assert count_b == len(names_b)
    finally:
        _drop_schema(
            pg_instance["operator_dsn"],
            schema_a,
            runtime_dsns=(runtime_a,) if runtime_a else (),
        )
        _drop_schema(
            pg_instance["operator_dsn"],
            schema_b,
            runtime_dsns=(runtime_b,) if runtime_b else (),
        )
