"""Tests for Aurora configuration support (T1).

Covers:
- AuroraConfig dataclass defaults and tag enforcement
- AuroraConfig.from_dict() construction
- get_db_config_for_env() engine_type field for local and aurora envs
- Aurora-specific fields in get_db_config_for_env()
- Backward compatibility: existing local envs still return the same shape
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from daylily_tapdb.aurora.config import AuroraConfig


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
    cfg_file.write_text(
        textwrap.dedent("""\
        environments:
          dev:
            host: localhost
            port: 5432
            user: daylily
            password: ""
            database: tapdb_dev
          aurora_dev:
            engine_type: aurora
            host: my-cluster.us-west-2.rds.amazonaws.com
            port: 5432
            user: tapdb_admin
            password: ""
            database: tapdb_dev
            region: us-west-2
            cluster_identifier: my-cluster
            iam_auth: true
            ssl: true
        """)
    )
    monkeypatch.setenv("TAPDB_CONFIG_PATH", str(cfg_file))
    return cfg_file


class TestGetDbConfigEngineType:
    @pytest.mark.usefixtures("_yaml_config")
    def test_local_env_has_engine_type_local(self):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg = get_db_config_for_env("dev")
        assert cfg["engine_type"] == "local"

    @pytest.mark.usefixtures("_yaml_config")
    def test_aurora_env_has_engine_type_aurora(self):
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        cfg = get_db_config_for_env("aurora_dev")
        assert cfg["engine_type"] == "aurora"

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

