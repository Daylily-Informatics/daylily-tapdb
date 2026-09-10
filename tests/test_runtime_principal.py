"""Offline plans, exact principals, receipt rejection, and thin CLI contracts."""

from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from daylily_tapdb import runtime_principal as rp
from daylily_tapdb.cli import runtime_principal as cli


@pytest.fixture
def cfg(tmp_path):
    return {
        "config_path": str(tmp_path / "target.yaml"),
        "engine_type": "local",
        "host": "localhost",
        "port": 15438,
        "database": "principal_test",
        "schema_name": "principal_schema",
        "user": "principal_runtime",
        "operator_user": "principal_operator",
        "operator_configured": True,
        "operator_password": "",
        "password": "",
        "iam_auth": False,
        "operator_iam_auth": False,
        "client_id": "test",
        "database_name": "test",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "tenant_id": "",
        "allow_global_claims": True,
    }


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def mappings(self):
        return self

    def one(self):
        return self.rows[0]

    def one_or_none(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)

    def scalar_one(self):
        return self.rows[0]


class Connection:
    def __init__(self, cfg, *, exists=True):
        self.cfg = cfg
        self.exists = exists
        self.commands = []
        self.memberships = []
        self.unsafe = {}
        self.role_changes = {}
        self.identity_changes = {}

    def execute(self, statement, params=None):
        sql = str(statement)
        self.commands.append((sql, params))
        if "tapdb_principal:search_path" in sql:
            return Result(["pg_catalog"])
        if "tapdb_principal:identity" in sql:
            return Result(
                [
                    {
                        "database": self.cfg["database"],
                        "database_oid": 42,
                        "session_user": self.cfg["operator_user"],
                        "current_user": self.cfg["operator_user"],
                        **self.identity_changes,
                    }
                ]
            )
        if "tapdb_principal:role" in sql:
            if not self.exists:
                return Result()
            return Result(
                [
                    {
                        "oid": 51,
                        "name": self.cfg["user"],
                        "login": True,
                        "superuser": False,
                        "bypassrls": False,
                        "createdb": False,
                        "createrole": False,
                        "replication": False,
                        "inherit": False,
                        **self.role_changes,
                    }
                ]
            )
        if "tapdb_principal:memberships" in sql:
            return Result(self.memberships)
        if "tapdb_principal:unsafe" in sql:
            return Result([self.unsafe])
        if sql.startswith("CREATE ROLE"):
            self.exists = True
        if sql.startswith("GRANT rds_iam"):
            self.memberships = [
                {"name": "rds_iam", "admin": False, "inherit": False, "set": False}
            ]
        if "tapdb_principal:effective_permissions" in sql:
            return Result([False])
        return Result()


def install_connection(monkeypatch, connection):
    @contextmanager
    def connect(config, **kwargs):
        assert config is connection.cfg
        yield connection

    monkeypatch.setattr(rp, "operator_connection", connect)


def test_bootstrap_default_is_offline_and_secret_free(cfg, monkeypatch):
    monkeypatch.setattr(rp, "operator_connection", lambda *_: pytest.fail("connected"))
    monkeypatch.setattr(rp, "_credential", lambda *_: pytest.fail("resolved secret"))
    cfg["password"] = "unit-only-credential-value"
    plan = rp.bootstrap_runtime_principal(cfg)
    assert plan["status"] == "planned"
    assert plan["grants"] == [{"database": cfg["database"], "privileges": ["CONNECT"]}]
    assert plan["schema_changes"] == []
    assert cfg["password"] not in json.dumps(plan)
    assert rp.build_runtime_principal_bootstrap_plan(cfg) == plan


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("config_path", "relative.yaml"),
        ("host", ""),
        ("host", " localhost"),
        ("port", "bad"),
        ("port", 70000),
        ("server_port", 0),
        ("server_port", 65536),
        ("server_port", "invalid"),
        ("schema_name", "x" * 64),
        ("database", "bad\nname"),
        ("operator_configured", False),
        ("operator_user", "principal_runtime"),
        ("engine_type", "missing"),
        ("user", "rds_superuser"),
        ("iam_auth", "invalid"),
        ("iam_auth", True),
        ("operator_iam_auth", "invalid"),
        ("tenant_id", "not-a-uuid"),
        ("domain_code", "z"),
        ("owner_repo_name", "UpperCase"),
        ("allow_global_claims", "yes"),
    ],
)
def test_plan_rejects_incomplete_or_unsafe_configuration(cfg, field, value):
    cfg[field] = value
    with pytest.raises(rp.RuntimePrincipalError):
        rp.build_runtime_principal_bootstrap_plan(cfg)


def test_iam_plan_requires_explicit_aurora_target_and_cert(cfg):
    cfg.update(
        engine_type="aurora",
        iam_auth="true",
        operator_iam_auth=True,
        region="us-west-2",
        cluster_identifier="isolated-test-cluster",
        sslrootcert="/explicit/test-ca.pem",
    )
    assert rp.build_runtime_principal_bootstrap_plan(cfg)["membership"] == ["rds_iam"]
    cfg["sslrootcert"] = "relative.pem"
    with pytest.raises(rp.RuntimePrincipalError, match="absolute"):
        rp.build_runtime_principal_bootstrap_plan(cfg)


def test_bootstrap_apply_creates_only_exact_login_and_connect(cfg, monkeypatch):
    connection = Connection(cfg, exists=False)
    install_connection(monkeypatch, connection)
    result = rp.bootstrap_runtime_principal(cfg, apply=True)
    writes = [
        sql
        for sql, _ in connection.commands
        if sql.startswith(("CREATE", "GRANT", "ALTER", "INSERT"))
    ]
    assert result["created"] is True
    assert len(writes) == 2
    assert writes[0].startswith(
        'CREATE ROLE "principal_runtime" LOGIN NOSUPERUSER NOBYPASSRLS'
    )
    assert "NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT" in writes[0]
    assert (
        writes[1] == 'GRANT CONNECT ON DATABASE "principal_test" TO "principal_runtime"'
    )
    assert "SCHEMA" not in " ".join(writes)
    connection.commands.clear()
    assert rp.bootstrap_runtime_principal(cfg, apply=True)["created"] is False
    assert not any(sql.startswith("CREATE") for sql, _ in connection.commands)


@pytest.mark.parametrize(
    "field", ["superuser", "bypassrls", "createdb", "createrole", "replication"]
)
def test_rejects_elevated_roles_before_any_grants(cfg, monkeypatch, field):
    connection = Connection(cfg)
    connection.role_changes[field] = True
    install_connection(monkeypatch, connection)
    with pytest.raises(rp.RuntimePrincipalError, match="elevated"):
        rp.bootstrap_runtime_principal(cfg, apply=True)
    assert not any(sql.startswith("GRANT") for sql, _ in connection.commands)


@pytest.mark.parametrize(
    "field",
    ["ownership", "settings", "database_create", "indirect_membership", "shared_role"],
)
def test_rejects_unsafe_principal_catalogs(cfg, monkeypatch, field):
    connection = Connection(cfg)
    connection.unsafe[field] = True
    install_connection(monkeypatch, connection)
    with pytest.raises(rp.RuntimePrincipalError, match=field):
        rp.bootstrap_runtime_principal(cfg, apply=True)


@pytest.mark.parametrize(
    "membership",
    [
        {"name": "unrelated", "admin": False},
        {"name": "rds_iam", "admin": True},
        {"name": "rds_iam", "admin": False},
    ],
)
def test_rejects_unconfigured_memberships(cfg, monkeypatch, membership):
    connection = Connection(cfg)
    connection.memberships = [membership]
    install_connection(monkeypatch, connection)
    with pytest.raises(rp.RuntimePrincipalError, match="membership"):
        rp.bootstrap_runtime_principal(cfg, apply=True)


@pytest.mark.parametrize("field", ["database", "session_user", "current_user"])
def test_rejects_connection_identity_mismatch(cfg, monkeypatch, field):
    connection = Connection(cfg)
    connection.identity_changes[field] = "different"
    install_connection(monkeypatch, connection)
    with pytest.raises(rp.RuntimePrincipalError, match="exact configured"):
        rp.bootstrap_runtime_principal(cfg, apply=True)
    assert len(connection.commands) == 1


def test_scope_sql_pins_all_immutable_fields_and_rejects_schema_substitution(cfg):
    sql = rp.runtime_scope_binding_sql(cfg["schema_name"], cfg)
    assert "ON CONFLICT (role_name) DO NOTHING" in sql
    for field in (
        "config_identity",
        "schema_name",
        "domain_code",
        "issuer_app_code",
        "tenant_id",
        "allow_global_rows",
    ):
        assert f"{field} IS NOT DISTINCT FROM" in sql
    assert "UPDATE" not in sql
    with pytest.raises(rp.RuntimePrincipalError, match="differs"):
        rp.runtime_scope_binding_sql("other_schema", cfg)


def test_binding_tampering_and_wrong_target_fail_before_connection(
    cfg, monkeypatch, tmp_path
):
    plan = rp._seal(
        {
            "format": rp._FORMAT,
            "operation": "bind",
            "status": "planned",
            "target": rp._target(cfg),
        }
    )
    receipt = tmp_path / "bind.json"
    monkeypatch.setattr(rp, "operator_connection", lambda *_: pytest.fail("connected"))
    receipt.write_text(json.dumps({**plan, "sha256": "invalid"}))
    with pytest.raises(rp.RuntimePrincipalError, match="digest"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    wrong = copy.deepcopy(plan)
    wrong["target"]["database"] = "other"
    receipt.write_text(json.dumps(rp._seal(wrong)))
    with pytest.raises(rp.RuntimePrincipalError, match="configured target"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)


def test_binding_stale_receipt_has_no_writes(cfg, monkeypatch, tmp_path):
    receipt = tmp_path / "bind.json"
    plan = rp._seal(
        {
            "format": rp._FORMAT,
            "operation": "bind",
            "status": "planned",
            "target": rp._target(cfg),
        }
    )
    receipt.write_text(json.dumps(plan))
    connection = Connection(cfg)
    install_connection(monkeypatch, connection)
    monkeypatch.setattr(
        rp,
        "build_runtime_principal_binding_plan",
        lambda *_: {**plan, "schema": "changed"},
    )
    with pytest.raises(rp.RuntimePrincipalError, match="stale"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert connection.commands == []


def test_binding_receipt_cannot_overwrite_existing_evidence(cfg, tmp_path):
    receipt = tmp_path / "bind.json"
    receipt.write_text("{}")
    with pytest.raises(rp.RuntimePrincipalError, match="already exists"):
        rp.bind_runtime_principal(cfg, receipt_path=receipt)
    with pytest.raises(rp.RuntimePrincipalError, match="absolute"):
        rp.bind_runtime_principal(cfg, receipt_path=Path("relative"))


def test_cli_honors_framework_dry_run_veto(cfg, monkeypatch):
    import cli_core_yo.runtime

    monkeypatch.setattr(cli, "get_db_config", lambda: cfg)
    monkeypatch.setattr(
        cli_core_yo.runtime, "get_context", lambda: SimpleNamespace(dry_run=True)
    )
    monkeypatch.setattr(rp, "operator_connection", lambda *_: pytest.fail("connected"))
    result = CliRunner().invoke(cli.runtime_principal_app, ["bootstrap", "--apply"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["status"] == "planned"
    help_result = CliRunner().invoke(cli.runtime_principal_app, ["bootstrap", "--help"])
    assert "CONNECT" in help_result.output


def test_cli_redacts_driver_failures(cfg, monkeypatch):
    monkeypatch.setattr(cli, "get_db_config", lambda: cfg)

    def broken(*args, **kwargs):
        raise ValueError("driver statement contained a secret")

    monkeypatch.setattr(cli, "bootstrap_runtime_principal", broken)
    result = CliRunner().invoke(cli.runtime_principal_app, ["bootstrap"])
    assert result.exit_code != 0
    assert "secret" not in result.output


class CatalogConnection(Connection):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.schema = {
            "oid": 50,
            "owner_oid": 60,
            "owner": cfg["operator_user"],
            "acl": None,
            "runtime_create": False,
            "sql_name": cfg["schema_name"],
        }
        self.objects = [
            {
                "oid": i + 100,
                "name": name,
                "kind": "r",
                "owner": cfg["operator_user"],
                "rls": name != "_tapdb_migrations",
                "force_rls": name != "_tapdb_migrations",
                "acl": None,
            }
            for i, name in enumerate(sorted(rp._WRITABLE | rp._READABLE | {rp._SCOPE}))
        ] + [
            {
                "oid": 200,
                "name": "wx_instance_seq",
                "kind": "S",
                "owner": cfg["operator_user"],
                "rls": False,
                "force_rls": False,
                "acl": None,
            }
        ]
        contract = rp.canonical_security_contract(
            cfg["schema_name"], cfg["schema_name"]
        )
        self.policies = [
            {
                "relation": row["name"],
                "name": name,
                "command": "*",
                "permissive": True,
                "roles": "{60}" if name == "tapdb_operator_access" else "{0}",
                "using": "true"
                if name == "tapdb_operator_access"
                else contract["policies"][row["name"]][0],
                "with_check": "true"
                if name == "tapdb_operator_access"
                else contract["policies"][row["name"]][1],
            }
            for row in self.objects
            if row["rls"]
            for name in (
                ["tapdb_operator_access"]
                if row["name"] == rp._SCOPE
                else ["tapdb_operator_access", row["name"] + "_scope_isolation"]
            )
        ]
        self.functions = [
            {
                "oid": i + 300,
                **expected,
                "owner": cfg["operator_user"],
                "acl": None,
                "definition_hash": str(i),
            }
            for i, expected in enumerate(contract["functions"].values())
        ]
        self.triggers = [
            {
                **expected,
                "definition": "canonical trigger",
            }
            for expected in contract["triggers"].values()
        ]
        self.scope = None
        self.leaked_permissions = False
        self.database_access = {"acl": None, "runtime_connect": True}
        self.connect_grant_effective = True

    def execute(self, statement, params=None):
        sql = str(statement)
        self.commands.append((sql, params))
        if sql.startswith("GRANT CONNECT") and self.connect_grant_effective:
            self.database_access["runtime_connect"] = True
        for marker, rows in (
            ("database_access", [self.database_access]),
            ("schema", [self.schema] if self.schema else []),
            ("objects", self.objects),
            ("policies", self.policies),
            ("functions", self.functions),
            ("triggers", self.triggers),
            ("effective_permissions", [self.leaked_permissions]),
        ):
            if "tapdb_principal:" + marker + " */" in sql:
                return Result(rows)
        if sql.startswith("SELECT config_identity"):
            return Result([self.scope] if self.scope else [])
        self.commands.pop()
        return super().execute(statement, params)


@pytest.fixture
def catalog(cfg, monkeypatch):
    from daylily_tapdb import sequences

    monkeypatch.setattr(
        sequences,
        "capture_runtime_sequence_bindings",
        lambda *args, **kwargs: {
            "wx_instance_seq": {
                "name": "wx_instance_seq",
                "owner": cfg["operator_user"],
                "mapping": {
                    "kind": "prefix",
                    "prefix": "WX",
                    "evidence": [
                        {
                            "source": "catalog_annotation",
                            "annotation": "tapdb-prefix-binding/v1:WX",
                        }
                    ],
                },
            }
        },
    )
    return CatalogConnection(cfg)


def test_binding_plan_and_apply_exact_grants(cfg, catalog, monkeypatch, tmp_path):
    install_connection(monkeypatch, catalog)
    path = tmp_path / "bind.json"
    planned = rp.bind_runtime_principal(cfg, receipt_path=path)
    assert path.stat().st_mode & 0o777 == 0o600
    assert planned == rp._seal(planned)
    assert planned["database_grants"] == [
        {"database": cfg["database"], "privileges": ["CONNECT"]}
    ]
    assert not any(
        sql.startswith(("GRANT", "REVOKE", "INSERT")) for sql, _ in catalog.commands
    )
    applied = rp.bind_runtime_principal(cfg, apply=True, receipt_path=path)
    assert applied["privileges_verified"] is True
    grants = [sql for sql, _ in catalog.commands if sql.startswith("GRANT")]
    assert any("GRANT USAGE, SELECT ON SEQUENCE" in sql for sql in grants)
    assert 'GRANT CONNECT ON DATABASE "principal_test" TO "principal_runtime"' in grants
    assert not any("UPDATE ON SEQUENCE" in sql for sql in grants)
    assert any(
        'GRANT SELECT ON TABLE "principal_schema"."_tapdb_migrations"' in sql
        for sql in grants
    )
    assert not any(rp._SCOPE in sql for sql in grants)
    assert all('TO "principal_runtime"' in sql for sql in grants)
    assert path.with_name("bind.result.json").exists()
    with pytest.raises(rp.RuntimePrincipalError, match="result already exists"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=path)


@pytest.mark.parametrize(
    "field,value", [("acl", "changed"), ("runtime_connect", False)]
)
def test_binding_database_privilege_evidence_must_not_change(
    cfg, catalog, monkeypatch, tmp_path, field, value
):
    install_connection(monkeypatch, catalog)
    receipt = tmp_path / "database-stale.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    catalog.database_access[field] = value
    with pytest.raises(rp.RuntimePrincipalError, match="stale"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert not any(sql.startswith("GRANT") for sql, _ in catalog.commands)


def test_binding_requires_connect_grant_to_be_effective(
    cfg, catalog, monkeypatch, tmp_path
):
    install_connection(monkeypatch, catalog)
    catalog.database_access["runtime_connect"] = False
    catalog.connect_grant_effective = False
    receipt = tmp_path / "database-denied.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    with pytest.raises(rp.RuntimePrincipalError, match="CONNECT.*not established"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert not receipt.with_name("database-denied.result.json").exists()


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("missing_schema", "Existing managed schema"),
        ("wrong_schema_owner", "Existing managed schema"),
        ("runtime_create", "effective CREATE"),
        ("missing_table", "table inventory"),
        ("foreign_object_owner", "operator owner"),
        ("rls_disabled", "forced RLS"),
        ("force_disabled", "forced RLS"),
        ("missing_function", "RLS migration"),
        ("missing_policy", "missing runtime/operator"),
        ("extra_policy", "missing runtime/operator"),
        ("operator_public", "policy authority"),
        ("restrictive_policy", "policy authority"),
        ("missing_with_check", "policy authority"),
        ("operator_filter", "policy authority"),
        ("disabled_trigger", "immutable runtime-scope trigger"),
        ("wrong_trigger_function", "immutable runtime-scope trigger"),
        ("changed_scope", "conflicting immutable"),
    ],
)
def test_binding_rejects_unsafe_catalog(cfg, catalog, mutation, match):
    if mutation == "missing_schema":
        catalog.schema = None
    elif mutation == "wrong_schema_owner":
        catalog.schema["owner"] = "other"
    elif mutation == "runtime_create":
        catalog.schema["runtime_create"] = True
    elif mutation == "missing_table":
        catalog.objects.pop(0)
    elif mutation == "foreign_object_owner":
        catalog.objects[0]["owner"] = "other"
    elif mutation == "view":
        catalog.objects.append(
            {**catalog.objects[0], "name": "unsafe_view", "kind": "v"}
        )
    elif mutation in ("rls_disabled", "force_disabled"):
        next(row for row in catalog.objects if row["name"] == "generic_instance")[
            "rls" if mutation == "rls_disabled" else "force_rls"
        ] = False
    elif mutation == "missing_function":
        catalog.functions = []
    elif mutation == "missing_policy":
        catalog.policies.pop()
    elif mutation == "extra_policy":
        catalog.policies.append({**catalog.policies[0], "name": "unexpected_policy"})
    elif mutation == "operator_public":
        catalog.policies[0]["roles"] = "{0,60}"
    elif mutation == "restrictive_policy":
        catalog.policies[0]["permissive"] = False
    elif mutation == "missing_with_check":
        catalog.policies[0]["with_check"] = None
    elif mutation == "operator_filter":
        catalog.policies[0]["using"] = "false"
    elif mutation == "disabled_trigger":
        catalog.triggers[0]["enabled"] = "D"
    elif mutation == "wrong_trigger_function":
        catalog.triggers[0]["function"] = "different"
    elif mutation == "changed_scope":
        catalog.scope = {"tenant_id": "changed"}
    with pytest.raises(rp.RuntimePrincipalError, match=match):
        rp.build_runtime_principal_binding_plan(catalog, cfg)
    assert not any(
        sql.startswith(("GRANT", "REVOKE", "INSERT")) for sql, _ in catalog.commands
    )


def test_existing_identical_scope_can_be_planned(cfg, catalog):
    catalog.scope = rp.build_runtime_principal_binding_plan(catalog, cfg)["scope"]
    assert (
        rp.build_runtime_principal_binding_plan(catalog, cfg)["existing_scope"]
        == catalog.scope
    )


def test_public_permission_leak_rejects_binding(cfg, catalog, monkeypatch, tmp_path):
    install_connection(monkeypatch, catalog)
    receipt = tmp_path / "leak.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    catalog.leaked_permissions = True
    with pytest.raises(rp.RuntimePrincipalError, match="forbidden effective object"):
        rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert not receipt.with_name("leak.result.json").exists()


def test_cli_bind_and_runtime_errors(cfg, catalog, monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "get_db_config", lambda: cfg)
    install_connection(monkeypatch, catalog)
    receipt = tmp_path / "cli-bind.json"
    result = CliRunner().invoke(
        cli.runtime_principal_app, ["bind", "--receipt", str(receipt)]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["operation"] == "bind"
    result = CliRunner().invoke(
        cli.runtime_principal_app, ["bind", "--receipt", str(receipt)]
    )
    assert result.exit_code == 2
    assert "already" in result.output and "exists" in result.output
    cfg["operator_user"] = cfg["user"]
    result = CliRunner().invoke(cli.runtime_principal_app, ["bootstrap"])
    assert "collision" in result.output


@pytest.mark.parametrize("profile", [None, "explicit-test-profile"])
def test_secret_lookup_selects_exact_profile(monkeypatch, profile):
    from daylily_tapdb.aurora import connection as aurora

    calls = []
    client = SimpleNamespace(
        get_secret_value=lambda **kwargs: {
            "SecretString": json.dumps({"password": "unit-test-value"})
        }
    )

    def make_client(service, **kwargs):
        calls.append((service, kwargs))
        return client

    def make_session(**kwargs):
        calls.append(("session", kwargs))
        return SimpleNamespace(client=make_client)

    monkeypatch.setattr(
        aurora,
        "_ensure_boto3",
        lambda: SimpleNamespace(
            client=make_client, session=SimpleNamespace(Session=make_session)
        ),
    )
    result = aurora.AuroraConnectionBuilder.get_secret_password(
        "arn:aws:secretsmanager:us-west-2:000000000000:secret:test",
        "us-west-2",
        profile=profile,
    )
    assert result == "unit-test-value"
    if profile:
        assert calls[0] == ("session", {"profile_name": profile})
    else:
        assert not any(name == "session" for name, _ in calls)
    assert calls[-1] == ("secretsmanager", {"region_name": "us-west-2"})


def test_principal_credentials_propagate_exact_aws_profile(cfg, monkeypatch):
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    calls = []
    cfg.update(
        region="us-west-2",
        aws_profile="explicit-test-profile",
        operator_secret_arn="arn:aws:secretsmanager:us-west-2:000000000000:secret:test",
    )
    monkeypatch.setattr(
        AuroraConnectionBuilder,
        "get_secret_password",
        lambda *args, **kwargs: calls.append((args, kwargs)) or "unit-test-value",
    )
    assert rp._credential(cfg, operator=True) == "unit-test-value"
    assert calls[0][1] == {"profile": "explicit-test-profile"}
    cfg["operator_iam_auth"] = True
    cfg["engine_type"] = "aurora"
    monkeypatch.setattr(
        AuroraConnectionBuilder,
        "get_iam_auth_token",
        lambda *args, **kwargs: calls.append((args, kwargs)) or "unit-iam-token",
    )
    assert rp._credential(cfg, operator=True) == "unit-iam-token"
    assert calls[-1][1] == {"profile": "explicit-test-profile"}


@pytest.mark.parametrize("operator", [True, False])
@pytest.mark.parametrize("server_port", [None, 5432])
def test_principal_iam_signing_uses_explicit_server_port(
    cfg, monkeypatch, operator, server_port
):
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    cfg.update(
        engine_type="aurora",
        region="us-west-2",
        cluster_identifier="isolated-test-cluster",
        sslrootcert="/explicit/test-ca.pem",
        aws_profile="reviewed-profile",
        port=55434,
        iam_auth=True,
        operator_iam_auth=True,
    )
    if server_port is not None:
        cfg["server_port"] = server_port
        assert rp._target(cfg)["server_port"] == server_port
    calls = []
    monkeypatch.setattr(
        AuroraConnectionBuilder,
        "get_iam_auth_token",
        lambda *args, **kwargs: calls.append((args, kwargs)) or "unit-iam-token",
    )
    assert rp._credential(cfg, operator=operator) == "unit-iam-token"
    assert calls == [
        (
            (
                cfg["region"],
                cfg["host"],
                55434 if server_port is None else server_port,
                cfg["operator_user" if operator else "user"],
            ),
            {"profile": "reviewed-profile"},
        )
    ]


def test_credentials_require_explicit_auth(cfg):
    cfg.pop("password")
    with pytest.raises(rp.RuntimePrincipalError, match="Explicit password"):
        rp._credential(cfg, operator=False)
    cfg.update(engine_type="aurora", password="")
    with pytest.raises(rp.RuntimePrincipalError, match="requires a credential"):
        rp._credential(cfg, operator=False)


def test_iam_apply_requires_and_verifies_only_configured_membership(cfg, monkeypatch):
    cfg.update(
        engine_type="aurora",
        iam_auth=True,
        region="us-west-2",
        cluster_identifier="qualification",
        sslrootcert="/explicit/ca.pem",
    )
    connection = Connection(cfg, exists=False)
    install_connection(monkeypatch, connection)
    monkeypatch.setattr(
        rp,
        "_credential",
        lambda *args, **kwargs: pytest.fail(
            "IAM role creation must not retrieve a runtime password"
        ),
    )
    result = rp.bootstrap_runtime_principal(cfg, apply=True)
    assert result["principal"]["memberships"] == [
        {"name": "rds_iam", "admin": False, "inherit": False, "set": False}
    ]
    writes = [
        sql for sql, _ in connection.commands if sql.startswith(("CREATE", "GRANT"))
    ]
    assert len(writes) == 3
    assert "PASSWORD" not in writes[0]
    assert (
        writes[1]
        == 'GRANT rds_iam TO "principal_runtime" WITH ADMIN FALSE, INHERIT FALSE, SET FALSE'
    )
    original_execute = connection.execute

    def missing_membership(sql, params=None):
        result = original_execute(sql, params)
        if str(sql).startswith("GRANT rds_iam"):
            connection.memberships = []
        return result

    connection.execute = missing_membership
    with pytest.raises(rp.RuntimePrincipalError, match="was not established"):
        rp.bootstrap_runtime_principal(cfg, apply=True)


def test_operator_connection_pins_tls_profile_identity_and_isolation(
    cfg, monkeypatch, tmp_path
):
    cert = tmp_path / "explicit-ca.pem"
    cert.write_text("unit-test-certificate-placeholder")
    cfg.update(
        engine_type="aurora",
        region="us-west-2",
        cluster_identifier="qualification",
        sslrootcert=str(cert),
        host="qualified.example.invalid",
        hostaddr="127.0.0.1",
    )
    captured = []
    connection = Connection(cfg)

    @contextmanager
    def transaction():
        yield connection

    engine = SimpleNamespace(
        begin=transaction, dispose=lambda: captured.append("disposed")
    )

    def build(url, **kwargs):
        captured.append((url, kwargs))
        return engine

    monkeypatch.setattr(rp, "create_engine", build)
    monkeypatch.setattr(
        rp, "_credential", lambda *args, **kwargs: "unit-only-operator-credential"
    )
    with rp.operator_connection(cfg, isolation_level="REPEATABLE READ", read_only=True):
        pass
    url, options = captured[0]
    assert url.username == cfg["operator_user"] and url.database == cfg["database"]
    assert url.query["sslmode"] == "verify-full"
    assert url.query["sslrootcert"] == str(cert)
    assert url.query["hostaddr"] == "127.0.0.1"
    assert url.query["options"] == "-csearch_path=pg_catalog"
    assert options["isolation_level"] == "REPEATABLE READ"
    assert options["hide_parameters"] is True
    assert str(connection.commands[0][0]) == "SET TRANSACTION READ ONLY"
    assert captured[-1] == "disposed"
    cfg["sslrootcert"] = str(tmp_path / "absent-ca.pem")
    with pytest.raises(rp.RuntimePrincipalError, match="does not exist"):
        with rp.operator_connection(cfg):
            pytest.fail("connected without explicit CA")


def test_narrow_fresh_schema_grants_share_binding_contract(cfg):
    sql = rp.runtime_schema_grants_sql(
        cfg["schema_name"], cfg["user"], operator_user=cfg["operator_user"]
    )
    assert "GRANT USAGE, SELECT ON ALL SEQUENCES" not in sql
    assert "GRANT USAGE, SELECT, UPDATE" not in sql
    assert 'GRANT SELECT ON TABLE "principal_schema"."_tapdb_migrations"' in sql
    assert "REVOKE ALL ON TABLES" in sql and "REVOKE ALL ON SEQUENCES" in sql
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES" not in sql
    assert "Global or PUBLIC default privileges conflict" in sql
    assert f'REVOKE ALL ON TABLE "principal_schema"."{rp._SCOPE}"' in sql
    with pytest.raises(rp.RuntimePrincipalError, match="collision"):
        rp.runtime_schema_grants_sql(
            cfg["schema_name"], cfg["user"], operator_user=cfg["user"]
        )


def test_missing_role_or_noncanonical_scope_fail_closed(cfg):
    with pytest.raises(rp.RuntimePrincipalError, match="does not exist"):
        rp._role_state(Connection(cfg, exists=False), rp._target(cfg))
    cfg["tenant_id"] = "00000000000000000000000000000001"
    with pytest.raises(rp.RuntimePrincipalError, match="canonical UUID"):
        rp.build_runtime_principal_bootstrap_plan(cfg)
    cfg["host"] = None
    with pytest.raises(rp.RuntimePrincipalError, match="host is required"):
        rp.build_runtime_principal_bootstrap_plan(cfg)


@pytest.mark.parametrize("engine", ["local", "compose"])
@pytest.mark.parametrize("value", [None, "", False, "false"])
def test_non_aurora_engine_keeps_iam_inapplicable_without_new_config_fields(
    cfg, engine, value
):
    cfg.update(engine_type=engine, iam_auth=value, operator_iam_auth=value)
    plan = rp.build_runtime_principal_bootstrap_plan(cfg)
    assert plan["target"]["iam_auth"] is False
    assert plan["target"]["operator_iam_auth"] is False
    assert rp._credential(cfg, operator=True) == ""


def test_aurora_still_requires_explicit_iam_mode(cfg):
    cfg.update(
        engine_type="aurora",
        region="us-west-2",
        cluster_identifier="qualification",
        sslrootcert="/explicit/ca.pem",
    )
    cfg.pop("iam_auth")
    with pytest.raises(rp.RuntimePrincipalError, match="explicitly true or false"):
        rp.build_runtime_principal_bootstrap_plan(cfg)
