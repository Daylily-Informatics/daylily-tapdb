"""Tests for Aurora configuration support (T1).

Covers:
- AuroraConfig dataclass defaults and tag enforcement
- AuroraConfig.from_dict() construction
- get_db_config_for_env() engine_type field for local and aurora envs
- Aurora-specific fields in get_db_config_for_env()
- namespaced database identifier normalization
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from daylily_tapdb.aurora.config import AuroraConfig
from daylily_tapdb.cli.context import (
    clear_cli_context,
    resolve_context,
    set_cli_context,
)


@pytest.fixture(autouse=True)
def _reset_namespace_env(monkeypatch):
    monkeypatch.setenv("TAPDB_STRICT_NAMESPACE", "0")
    clear_cli_context()
    yield
    clear_cli_context()


# ---------------------------------------------------------------------------
# AuroraConfig dataclass
# ---------------------------------------------------------------------------


class TestAuroraConfigDefaults:
    def test_default_region(self):
        cfg = AuroraConfig()
        assert cfg.region == "us-west-2"

    def test_default_tags_present(self):
        cfg = AuroraConfig()
        assert "lsmc-cost-center" in cfg.tags
        assert "lsmc-project" in cfg.tags

    def test_default_tag_values(self):
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

    def test_iam_auth_default_true(self):
        cfg = AuroraConfig()
        assert cfg.iam_auth is True

    def test_ssl_default_true(self):
        cfg = AuroraConfig()
        assert cfg.ssl is True

    def test_publicly_accessible_default_false(self):
        cfg = AuroraConfig()
        assert cfg.publicly_accessible is False

    def test_deletion_protection_default_true(self):
        cfg = AuroraConfig()
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
        data = {"region": "us-west-2", "unknown_key": "ignored"}
        cfg = AuroraConfig.from_dict(data)
        assert cfg.region == "us-west-2"

    def test_empty_dict_gives_defaults(self):
        cfg = AuroraConfig.from_dict({})
        assert cfg.region == "us-west-2"
        assert cfg.tags["lsmc-cost-center"] == "global"


# ---------------------------------------------------------------------------
# get_db_config_for_env — engine_type and aurora fields
# ---------------------------------------------------------------------------


@pytest.fixture()
def _yaml_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Write a temp config with both local and aurora envs."""
    cfg_file = tmp_path / "tapdb-config.yaml"
    domain_registry = tmp_path / "domain_code_registry.json"
    prefix_registry = tmp_path / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test-localhost"}}}\n',
        encoding="utf-8",
    )
    prefix_registry.write_text(
        (
            '{"version":"0.4.0","ownership":{"Z":{"TPX":{"issuer_app_code":"daylily-tapdb"},'
            '"EDG":{"issuer_app_code":"daylily-tapdb"},'
            '"ADT":{"issuer_app_code":"daylily-tapdb"},'
            '"SYS":{"issuer_app_code":"daylily-tapdb"},'
            '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
        ),
        encoding="utf-8",
    )
    cfg_file.write_text(
        textwrap.dedent("""\
        meta:
          config_version: 3
          client_id: clientx
          database_name: dbx
          owner_repo_name: daylily-tapdb
          domain_registry_path: {domain_registry}
          prefix_ownership_registry_path: {prefix_registry}
        environments:
          dev:
            host: localhost
            port: 5432
            ui_port: 8911
            domain_code: Z
            user: daylily
            password: ""
            database: tapdb_dev
            schema_name: tapdb_dbx_dev
          aurora_dev:
            engine_type: aurora
            host: my-cluster.us-west-2.rds.amazonaws.com
            port: 5432
            domain_code: Z
            user: tapdb_admin
            password: ""
            database: tapdb_dev
            schema_name: tapdb_dbx_aurora_dev
            region: us-west-2
            cluster_identifier: my-cluster
            iam_auth: true
            ssl: true
        """).format(
            domain_registry=domain_registry,
            prefix_registry=prefix_registry,
        )
    )
    set_cli_context(config_path=cfg_file)
    return cfg_file


class TestGetDbConfigEngineType:
    @pytest.mark.usefixtures("_yaml_config")
    def test_local_env_has_engine_type_local(self):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg = get_db_config_for_env("dev")
        assert cfg["engine_type"] == "local"
        assert cfg["schema_name"] == "tapdb_dbx_dev"

    @pytest.mark.usefixtures("_yaml_config")
    def test_aurora_env_has_engine_type_aurora(self):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg = get_db_config_for_env("aurora_dev")
        assert cfg["engine_type"] == "aurora"
        assert cfg["schema_name"] == "tapdb_dbx_aurora_dev"

    @pytest.mark.usefixtures("_yaml_config")
    def test_aurora_env_has_aurora_fields(self):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg = get_db_config_for_env("aurora_dev")
        assert cfg["region"] == "us-west-2"
        assert cfg["cluster_identifier"] == "my-cluster"
        # YAML parses `true` as Python bool True; str(True) == "True"
        assert cfg["iam_auth"] == "True"
        assert cfg["ssl"] == "True"

    @pytest.mark.usefixtures("_yaml_config")
    def test_local_env_no_aurora_fields(self):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg = get_db_config_for_env("dev")
        assert "region" not in cfg
        assert "cluster_identifier" not in cfg

    @pytest.mark.usefixtures("_yaml_config")
    def test_local_env_backward_compat(self):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg = get_db_config_for_env("dev")
        # All original keys still present
        for key in ("host", "port", "user", "password", "database"):
            assert key in cfg
        assert cfg["host"] == "localhost"
        assert cfg["database"] == "tapdb_dev"

    def test_local_env_derives_namespaced_unix_socket_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg_file = tmp_path / "tapdb-config.yaml"
        domain_registry = tmp_path / "domain_code_registry.json"
        prefix_registry = tmp_path / "prefix_ownership_registry.json"
        domain_registry.write_text(
            '{"version":"0.4.0","domains":{"Z":{"name":"test-localhost"}}}\n',
            encoding="utf-8",
        )
        prefix_registry.write_text(
            (
                '{"version":"0.4.0","ownership":{"Z":{"TPX":{"issuer_app_code":"daylily-tapdb"},'
                '"EDG":{"issuer_app_code":"daylily-tapdb"},'
                '"ADT":{"issuer_app_code":"daylily-tapdb"},'
                '"SYS":{"issuer_app_code":"daylily-tapdb"},'
                '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
            ),
            encoding="utf-8",
        )
        cfg_file.write_text(
            textwrap.dedent("""\
            meta:
              config_version: 3
              client_id: clientx
              database_name: dbx
              owner_repo_name: daylily-tapdb
              domain_registry_path: {domain_registry}
              prefix_ownership_registry_path: {prefix_registry}
            environments:
              dev:
                host: localhost
                port: 5432
                ui_port: 8911
                domain_code: Z
                user: daylily
                password: ""
                database: tapdb_dev
                schema_name: tapdb_dbx_dev
            """).format(
                domain_registry=domain_registry,
                prefix_registry=prefix_registry,
            )
        )
        monkeypatch.setenv("HOME", str(tmp_path))
        set_cli_context(config_path=cfg_file)

        cfg = get_db_config_for_env("dev")
        context = resolve_context(config_path=cfg_file)
        assert context is not None

        assert cfg["unix_socket_dir"] == str(context.postgres_socket_dir("dev"))

    def test_local_env_unix_socket_dir_env_override(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg_file = tmp_path / "tapdb-config.yaml"
        domain_registry = tmp_path / "domain_code_registry.json"
        prefix_registry = tmp_path / "prefix_ownership_registry.json"
        domain_registry.write_text(
            '{"version":"0.4.0","domains":{"Z":{"name":"test-localhost"}}}\n',
            encoding="utf-8",
        )
        prefix_registry.write_text(
            (
                '{"version":"0.4.0","ownership":{"Z":{"TPX":{"issuer_app_code":"daylily-tapdb"},'
                '"EDG":{"issuer_app_code":"daylily-tapdb"},'
                '"ADT":{"issuer_app_code":"daylily-tapdb"},'
                '"SYS":{"issuer_app_code":"daylily-tapdb"},'
                '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
            ),
            encoding="utf-8",
        )
        cfg_file.write_text(
            textwrap.dedent("""\
            meta:
              config_version: 3
              client_id: clientx
              database_name: dbx
              owner_repo_name: daylily-tapdb
              domain_registry_path: {domain_registry}
              prefix_ownership_registry_path: {prefix_registry}
            environments:
              dev:
                host: localhost
                port: 5432
                ui_port: 8911
                domain_code: Z
                user: daylily
                password: ""
                database: tapdb_dev
                schema_name: tapdb_dbx_dev
                unix_socket_dir: /tmp/from-config
            """).format(
                domain_registry=domain_registry,
                prefix_registry=prefix_registry,
            )
        )
        set_cli_context(config_path=cfg_file)

        cfg = get_db_config_for_env("dev")

        assert cfg["unix_socket_dir"] == "/tmp/from-config"

    @pytest.mark.usefixtures("_yaml_config")
    def test_aurora_env_does_not_expose_unix_socket_dir(self):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg = get_db_config_for_env("aurora_dev")
        assert "unix_socket_dir" not in cfg


class TestConfigPathScoping:
    def test_explicit_config_path_used_directly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from daylily_tapdb.cli.db_config import get_config_paths

        override = tmp_path / "custom.yaml"
        paths = get_config_paths(config_path=override)
        assert paths == [override]

    def test_missing_config_raises(self, monkeypatch: pytest.MonkeyPatch):
        """Without a config path, get_config_paths must error — no silent fallback."""
        from daylily_tapdb.cli.context import set_cli_context
        from daylily_tapdb.cli.db_config import get_config_paths

        # Clear any ambient config
        set_cli_context(config_path="")

        with pytest.raises(RuntimeError, match="config path is required"):
            get_config_paths()


class TestDatabaseNameNormalization:
    def test_default_database_name_for_namespace_normalizes_hyphens(self):
        from daylily_tapdb.cli.db_config import default_database_name_for_namespace

        assert (
            default_database_name_for_namespace("auruse1-daylily-tapdb", "dev")
            == "tapdb_auruse1_daylily_tapdb_dev"
        )

    def test_default_schema_name_for_database_normalizes_hyphens(self):
        from daylily_tapdb.cli.db_config import default_schema_name_for_database

        assert (
            default_schema_name_for_database("auruse1-daylily-tapdb", "dev")
            == "tapdb_auruse1_daylily_tapdb_dev"
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

    def test_get_db_config_requires_explicit_schema_name(self, _yaml_config: Path):
        import yaml

        from daylily_tapdb.cli.db_config import get_db_config_for_env

        payload = yaml.safe_load(_yaml_config.read_text(encoding="utf-8"))
        del payload["environments"]["dev"]["schema_name"]
        _yaml_config.write_text(yaml.safe_dump(payload), encoding="utf-8")

        with pytest.raises(RuntimeError, match="environments.dev.schema_name"):
            get_db_config_for_env("dev")

    def test_get_db_config_rejects_unsafe_schema_name(self, _yaml_config: Path):
        import yaml

        from daylily_tapdb.cli.db_config import get_db_config_for_env

        payload = yaml.safe_load(_yaml_config.read_text(encoding="utf-8"))
        payload["environments"]["dev"]["schema_name"] = "tapdb-dev"
        _yaml_config.write_text(yaml.safe_dump(payload), encoding="utf-8")

        with pytest.raises(RuntimeError, match="environments.dev.schema_name"):
            get_db_config_for_env("dev")
