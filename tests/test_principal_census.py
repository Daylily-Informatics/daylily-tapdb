"""Read-only census scope and failure classification, without a database deploy."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from daylily_tapdb import principal_census as census
from daylily_tapdb.cli import census as cli
from tests.test_identity_inventory import snapshot


@pytest.fixture
def source(monkeypatch):
    cfg = dict(
        snapshot()["target"],
        config_path="/tmp/test-config.yaml",
        operator_user="operator",
    )
    calls = []
    data = {
        "identity": [
            dict(
                operator="operator",
                current_role="operator",
                read_only="on",
                activity_complete=True,
            )
        ],
        "databases": [
            dict(name="test", oid=1, owner="operator", owner_oid=2, acl=None)
        ],
        "memberships": [dict(role="rds_iam", member="runtime", grantor="operator")],
        "binding_catalog": [],
    }
    physical = {
        "database": "test",
        "database_oid": 1,
        "server_address": "127.0.0.1",
        "server_port": 15438,
    }

    class Connection:
        def execute(self, statement, parameters=None):
            sql = str(statement)
            calls.append((sql, parameters))
            assert sql.lstrip().startswith(("SELECT", "SET LOCAL", "/* tapdb_census:"))
            if "tapdb_census:" not in sql:
                return None
            section = sql.split("tapdb_census:")[1].split(" ")[0]
            return SimpleNamespace(mappings=lambda: data.get(section, []))

    @contextmanager
    def connection(config, **kwargs):
        assert config == cfg
        assert kwargs == {"isolation_level": "REPEATABLE READ", "read_only": True}
        yield Connection()

    monkeypatch.setattr(census, "operator_connection", connection)
    monkeypatch.setattr(census, "physical_target", lambda *a: physical)
    return cfg, calls, data


def test_historical_census_reports_absence_not_connection_failure(source):
    cfg, calls, _ = source
    result = census.capture_principal_census(
        cfg, database_names=["destination", "control"]
    )
    assert [item["exists"] for item in result["database_checks"]] == [
        True,
        False,
        False,
    ]
    assert result["scope_bindings"]["status"] == "unavailable_historical_structure"
    assert result["iam_memberships"][0]["role"] == "rds_iam"
    assert result["activity"]["complete_visibility"]
    assert all(
        "query" not in sql.lower() and "rolpassword" not in sql for sql, _ in calls
    )
    assert all(
        params["census_limit"] == 100001
        for sql, params in calls
        if "tapdb_census:" in sql
    )


def test_census_reports_unfiltered_binding_and_activity_limits(source):
    cfg, _, data = source
    data["identity"][0]["activity_complete"] = False
    data["binding_catalog"] = [
        {
            "readable": True,
            "row_security_active": True,
            "columns": [
                "role_name",
                "config_identity",
                "schema_name",
                "domain_code",
                "issuer_app_code",
                "tenant_id",
                "allow_global_rows",
            ],
        }
    ]
    result = census.capture_principal_census(cfg)
    assert not result["activity"]["complete_visibility"]
    assert result["scope_bindings"]["status"] == "unavailable_unfiltered_access"
    data["binding_catalog"][0]["row_security_active"] = False
    data["bindings"] = [{"role_name": "runtime", "tenant_id": None}]
    assert (
        census.capture_principal_census(cfg)["scope_bindings"]["rows"]
        == data["bindings"]
    )


def test_census_refuses_overflow_and_wrong_authenticated_role(source):
    cfg, _, data = source
    data["roles"] = [{"name": "one"}, {"name": "two"}]
    with pytest.raises(census.PrincipalCensusError, match="roles exceeds"):
        census.capture_principal_census(cfg, max_catalog_rows=1)
    data["identity"][0]["operator"] = "wrong"
    with pytest.raises(census.PrincipalCensusError, match="authenticated"):
        census.capture_principal_census(cfg)


def test_census_cli_does_not_publish_absence_on_connection_error(monkeypatch, tmp_path):
    errors = []
    monkeypatch.setattr(cli, "get_db_config", lambda: {})
    monkeypatch.setattr(
        cli,
        "capture_principal_census",
        lambda *a, **k: (_ for _ in ()).throw(ConnectionError("PRIVATE credential")),
    )
    monkeypatch.setattr(cli.ccyo_out, "emit_error_json", lambda *a: errors.append(a))
    with pytest.raises(SystemExit) as caught:
        cli.census(
            database=["target"],
            receipt=tmp_path / "census.json",
            statement_timeout_ms=10,
            max_catalog_rows=10,
        )
    assert caught.value.code == 2
    assert errors == [("principal_census_error", "ConnectionError")]
    assert not (tmp_path / "census.json").exists()
