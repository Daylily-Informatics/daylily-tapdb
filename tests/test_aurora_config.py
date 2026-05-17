"""Tests for Aurora and explicit-target database configuration support."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from daylily_tapdb.aurora.config import AuroraConfig
from daylily_tapdb.cli.context import (
    clear_cli_context,
    resolve_context,
    set_cli_context,
)
from daylily_tapdb.cli.db_config import get_db_config


@pytest.fixture(autouse=True)
def _reset_context():
    clear_cli_context()
    yield
    clear_cli_context()


class TestAuroraConfigDefaults:
    def test_default_region(self):
        assert AuroraConfig().region == "us-west-2"

    def test_default_tags_present(self):
        cfg = AuroraConfig()
        assert cfg.tags["lsmc-cost-center"] == "global"
        assert cfg.tags["lsmc-project"] == "tapdb-us-west-2"

    def test_custom_region_updates_project_tag(self):
        cfg = AuroraConfig(region="eu-west-1")
        assert cfg.tags["lsmc-project"] == "tapdb-eu-west-1"

    def test_custom_tags_preserve_mandatory(self):
        cfg = AuroraConfig(tags={"team": "genomics"})
        assert cfg.tags["lsmc-cost-center"] == "global"
        assert cfg.tags["team"] == "genomics"

    def test_defaults(self):
        cfg = AuroraConfig()
        assert cfg.iam_auth is True
        assert cfg.ssl is True
        assert cfg.publicly_accessible is False
        assert cfg.deletion_protection is True


class TestAuroraConfigFromDict:
    def test_round_trip(self):
        data = {
            "region": "us-east-1",
            "cluster_identifier": "my-cluster",
            "instance_class": "db.r6g.xlarge",
            "iam_auth": False,
            "ssl": False,
        }
        cfg = AuroraConfig.from_dict(data)
        assert cfg.region == "us-east-1"
        assert cfg.cluster_identifier == "my-cluster"
        assert cfg.instance_class == "db.r6g.xlarge"
        assert cfg.iam_auth is False
        assert cfg.ssl is False

    def test_ignores_unknown_keys(self):
        cfg = AuroraConfig.from_dict({"region": "us-west-2", "unknown_key": "ignored"})
        assert cfg.region == "us-west-2"


def _write_registries(tmp_path: Path) -> tuple[Path, Path]:
    domain_registry = tmp_path / "domain_code_registry.json"
    prefix_registry = tmp_path / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test-localhost"}}}\n',
        encoding="utf-8",
    )
    prefix_registry.write_text(
        (
            '{"version":"0.4.0","ownership":{"Z":{'
            '"TPX":{"issuer_app_code":"daylily-tapdb"},'
            '"EDG":{"issuer_app_code":"daylily-tapdb"},'
            '"ADT":{"issuer_app_code":"daylily-tapdb"},'
            '"SYS":{"issuer_app_code":"daylily-tapdb"},'
            '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
        ),
        encoding="utf-8",
    )
    return domain_registry, prefix_registry


def _write_config(tmp_path: Path, target: dict[str, object]) -> Path:
    domain_registry, prefix_registry = _write_registries(tmp_path)
    cfg_file = tmp_path / "tapdb-config.yaml"
    cfg_file.write_text(
        yaml.safe_dump(
            {
                "meta": {
                    "config_version": 4,
                    "client_id": "clientx",
                    "database_name": "dbx",
                    "owner_repo_name": "daylily-tapdb",
                    "domain_registry_path": str(domain_registry),
                    "prefix_ownership_registry_path": str(prefix_registry),
                },
                "target": target,
                "safety": {
                    "safety_tier": "shared",
                    "destructive_operations": "confirm_required",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    cfg_file.chmod(0o600)
    set_cli_context(config_path=cfg_file)
    return cfg_file


def _local_target(schema_name: str = "tapdb_dbx") -> dict[str, object]:
    return {
        "engine_type": "local",
        "host": "localhost",
        "port": 5432,
        "ui_port": 8911,
        "domain_code": "Z",
        "user": "daylily",
        "password": "",
        "database": "tapdb_shared",
        "schema_name": schema_name,
    }


def _aurora_target() -> dict[str, object]:
    return {
        "engine_type": "aurora",
        "host": "my-cluster.us-west-2.rds.amazonaws.com",
        "port": 5432,
        "ui_port": 8911,
        "domain_code": "Z",
        "user": "tapdb_admin",
        "password": "",
        "database": "tapdb_shared",
        "schema_name": "tapdb_dbx_aurora",
        "region": "us-west-2",
        "cluster_identifier": "my-cluster",
        "iam_auth": True,
        "ssl": True,
    }


def test_local_target_has_engine_type_and_socket_dir(tmp_path: Path):
    cfg_file = _write_config(tmp_path, _local_target())

    cfg = get_db_config()
    context = resolve_context(config_path=cfg_file)

    assert cfg["engine_type"] == "local"
    assert cfg["schema_name"] == "tapdb_dbx"
    assert cfg["unix_socket_dir"] == str(context.postgres_socket_dir())


def test_local_target_unix_socket_dir_override(tmp_path: Path):
    target = _local_target()
    target["unix_socket_dir"] = "/tmp/from-config"
    _write_config(tmp_path, target)

    cfg = get_db_config()

    assert cfg["unix_socket_dir"] == "/tmp/from-config"


def test_aurora_target_has_aurora_fields_and_no_socket_dir(tmp_path: Path):
    _write_config(tmp_path, _aurora_target())

    cfg = get_db_config()

    assert cfg["engine_type"] == "aurora"
    assert cfg["region"] == "us-west-2"
    assert cfg["cluster_identifier"] == "my-cluster"
    assert cfg["iam_auth"] == "True"
    assert cfg["ssl"] == "True"
    assert "unix_socket_dir" not in cfg


class TestConfigPathScoping:
    def test_explicit_config_path_used_directly(self, tmp_path: Path):
        from daylily_tapdb.cli.db_config import get_config_paths

        override = tmp_path / "custom.yaml"
        assert get_config_paths(config_path=override) == [override]

    def test_missing_config_raises(self):
        from daylily_tapdb.cli.context import set_cli_context
        from daylily_tapdb.cli.db_config import get_config_paths

        set_cli_context(config_path="")

        with pytest.raises(RuntimeError, match="config path is required"):
            get_config_paths()


class TestDatabaseNameNormalization:
    def test_default_database_name_for_namespace_normalizes_hyphens(self):
        from daylily_tapdb.cli.db_config import default_database_name_for_namespace

        assert (
            default_database_name_for_namespace("auruse1-daylily-tapdb")
            == "tapdb_auruse1_daylily_tapdb"
        )

    def test_default_schema_name_for_database_normalizes_hyphens(self):
        from daylily_tapdb.cli.db_config import default_schema_name_for_database

        assert (
            default_schema_name_for_database("auruse1-daylily-tapdb")
            == "tapdb_auruse1_daylily_tapdb"
        )

    @pytest.mark.parametrize(
        "schema_name",
        ["tapdb_app_dev", "_tapdb_app_dev", "tapdb_app_123"],
    )
    def test_validate_postgres_identifier_component_accepts_safe_values(
        self, schema_name: str
    ):
        from daylily_tapdb.cli.db_config import validate_postgres_identifier_component

        assert (
            validate_postgres_identifier_component(
                schema_name,
                field_name="schema_name",
            )
            == schema_name
        )

    @pytest.mark.parametrize(
        "schema_name",
        ["", "TapDB_App", "tapdb-app", "1tapdb", "tapdb.app", "a" * 64],
    )
    def test_validate_postgres_identifier_component_rejects_unsafe_values(
        self, schema_name: str
    ):
        from daylily_tapdb.cli.db_config import validate_postgres_identifier_component

        with pytest.raises(RuntimeError, match="schema_name"):
            validate_postgres_identifier_component(
                schema_name,
                field_name="schema_name",
            )

    def test_get_db_config_requires_explicit_schema_name(self, tmp_path: Path):
        _write_config(tmp_path, _local_target(schema_name=""))

        with pytest.raises(RuntimeError, match="target.schema_name"):
            get_db_config()

    def test_get_db_config_rejects_unsafe_schema_name(self, tmp_path: Path):
        _write_config(tmp_path, _local_target(schema_name="tapdb-dev"))

        with pytest.raises(RuntimeError, match="target.schema_name"):
            get_db_config()
