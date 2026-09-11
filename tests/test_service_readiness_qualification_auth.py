"""Independent, offline principal-binding counterexamples.

These catalog doubles exercise the public planner/apply boundary, not PostgreSQL
semantics. No database, role, allocator, or AWS resource is created. The positive
control deliberately supplies a catalog accepted by the reviewed implementation;
each adversarial case changes only the boundary named by its test.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager

import pytest

from daylily_tapdb import runtime_principal as rp
from daylily_tapdb import sequences
from daylily_tapdb.runtime_catalog_contract import canonical_security_contract

CORE_TABLES = (
    "_tapdb_migrations",
    "audit_log",
    "generic_instance",
    "generic_instance_lineage",
    "generic_template",
    "inbox_message",
    "outbox_event",
    "outbox_event_attempt",
    "tapdb_identity_prefix_config",
    "tapdb_legacy_outbox_mapping",
    "tapdb_runtime_principal_scope",
)


class _Rows:
    def __init__(self, rows=()):
        self.rows = rows

    def mappings(self):
        return self

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]

    def one_or_none(self):
        assert len(self.rows) <= 1
        return self.rows[0] if self.rows else None

    def scalar_one(self):
        return self.one()

    def __iter__(self):
        return iter(self.rows)


class _OfflineCatalog:
    """Small independent SQL-boundary double; never opens a connection."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.commands = []
        self.objects = [
            self.relation(name, oid=100 + i) for i, name in enumerate(CORE_TABLES)
        ]
        self.objects.append(self.relation("wx_instance_seq", oid=200, kind="S"))
        contract = canonical_security_contract(cfg["schema_name"], cfg["schema_name"])
        self.policies = []
        for obj in self.objects:
            if not obj["rls"]:
                continue
            self.policies.append(
                self.policy(obj["name"], "tapdb_operator_access", "{71}", "true")
            )
            if obj["name"] != "tapdb_runtime_principal_scope":
                self.policies.append(
                    self.policy(
                        obj["name"],
                        obj["name"] + "_scope_isolation",
                        "{0}",
                        contract["policies"][obj["name"]][0],
                    )
                )
                self.policies[-1]["with_check"] = contract["policies"][obj["name"]][1]
        self.functions = [
            {
                **copy.deepcopy(expected),
                "oid": 300 + index,
                "owner": cfg["operator_user"],
                "acl": "{qualification_operator=X/qualification_operator}",
                "definition_hash": "1" * 32,
            }
            for index, expected in enumerate(contract["functions"].values())
        ]
        self.triggers = [
            {
                **copy.deepcopy(expected),
                "definition": "Offline display field; firing metadata is authoritative",
            }
            for expected in contract["triggers"].values()
        ]

    def relation(self, name, *, oid=900, kind="r", owner=None, protected=None):
        if protected is None:
            protected = name in CORE_TABLES and name != "_tapdb_migrations"
        return {
            "oid": oid,
            "name": name,
            "kind": kind,
            "owner": owner or self.cfg["operator_user"],
            "rls": protected,
            "force_rls": protected,
            "acl": "{qualification_operator=arwdDxt/qualification_operator}",
            "column_acls": [],
        }

    @staticmethod
    def policy(relation, name, roles, expression):
        return {
            "relation": relation,
            "name": name,
            "command": "*",
            "permissive": True,
            "roles": roles,
            "using": expression,
            "with_check": expression,
        }

    def function(self, name, *, oid=902, security_definer=False):
        return {
            "oid": oid,
            "name": name,
            "arguments": "",
            "owner": self.cfg["operator_user"],
            "security_definer": security_definer,
            "settings": '{"search_path=qualification_scope, pg_catalog, pg_temp"}',
            "acl": "{qualification_operator=X/qualification_operator}",
            "definition_hash": "1" * 32,
        }

    def execute(self, statement, params=None):
        sql = str(statement)
        self.commands.append((sql, params))
        rows = {
            "search_path": ["qualification_scope"],
            "database_access": [{"acl": None, "runtime_connect": True}],
            "identity": [
                {
                    "database": self.cfg["database"],
                    "database_oid": 70,
                    "session_user": self.cfg["operator_user"],
                    "current_user": self.cfg["operator_user"],
                }
            ],
            "role": [
                {
                    "oid": 72,
                    "name": self.cfg["user"],
                    "login": True,
                    "superuser": False,
                    "bypassrls": False,
                    "createdb": False,
                    "createrole": False,
                    "replication": False,
                    "inherit": False,
                }
            ],
            "memberships": [],
            "unsafe": [
                {
                    "ownership": False,
                    "settings": False,
                    "database_create": False,
                    "indirect_membership": False,
                    "shared_role": False,
                }
            ],
            "schema": [
                {
                    "oid": 73,
                    "owner_oid": 71,
                    "owner": self.cfg["operator_user"],
                    "acl": None,
                    "runtime_create": False,
                    "sql_name": self.cfg["schema_name"],
                }
            ],
            "objects": self.objects,
            "policies": self.policies,
            "functions": self.functions,
            "triggers": self.triggers,
            "default_acls": [],
            "effective_permissions": [False],
        }
        for marker, result in rows.items():
            if f"/* tapdb_principal:{marker} */" in sql:
                return _Rows(copy.deepcopy(result))
        if sql.startswith("SELECT config_identity"):
            return _Rows()
        if sql.startswith("SELECT pg_catalog.set_config('search_path'"):
            return _Rows([params["path"] if params else "pg_catalog"])
        # Operator/ACL assertion blocks and write statements are recorded only.
        assert sql.lstrip().startswith(
            ("DO ", "INSERT ", "REVOKE ", "GRANT ", "ALTER ")
        ), sql
        return _Rows()


@pytest.fixture
def offline_binding(tmp_path, monkeypatch):
    cfg = {
        "config_path": str(tmp_path / "qualification.yaml"),
        "engine_type": "local",
        "host": "offline.invalid",
        "port": 1,
        "database": "qualification_database",
        "schema_name": "qualification_scope",
        "user": "qualification_runtime",
        "operator_user": "qualification_operator",
        "operator_configured": True,
        "iam_auth": False,
        "operator_iam_auth": False,
        "client_id": "qualification",
        "database_name": "qualification",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "tenant_id": "",
        "allow_global_claims": False,
    }
    catalog = _OfflineCatalog(cfg)

    @contextmanager
    def connection(config, **kwargs):
        assert config is cfg
        yield catalog

    def forbid_network(*args, **kwargs):
        pytest.fail("Independent offline qualification must not open a database")

    monkeypatch.setattr(rp, "operator_connection", connection)
    monkeypatch.setattr(rp, "create_engine", forbid_network)
    # The classifier has its own real-PG coverage. This catalog-boundary double
    # supplies one explicit canonical annotation, never inferred unknown access.
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
    return cfg, catalog


def test_offline_binding_positive_control(offline_binding, tmp_path):
    cfg, catalog = offline_binding
    receipt = tmp_path / "positive.json"
    planned = rp.bind_runtime_principal(cfg, receipt_path=receipt)
    assert planned["status"] == "planned"
    assert all(
        not sql.lstrip().startswith(("GRANT ", "REVOKE ", "INSERT ", "ALTER "))
        for sql, _ in catalog.commands
    )
    applied = rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert applied["privileges_verified"] is True


@pytest.mark.parametrize(
    "owner", ["qualification_operator", "historical_application_owner"]
)
def test_preserved_historical_table_does_not_preclude_core_binding(
    offline_binding, owner
):
    cfg, catalog = offline_binding
    extra = catalog.relation("historical_assertion", owner=owner)
    catalog.objects.append(extra)
    planned = rp.build_runtime_principal_binding_plan(catalog, cfg)
    assert extra in planned["objects"]
    assert "historical_assertion" not in {grant["name"] for grant in planned["grants"]}


def test_named_runtime_policy_cannot_be_unconditional(offline_binding):
    cfg, catalog = offline_binding
    policy = next(
        row
        for row in catalog.policies
        if row["name"] == "generic_instance_scope_isolation"
    )
    policy.update(using="true", with_check="true")
    with pytest.raises(rp.RuntimePrincipalError):
        rp.build_runtime_principal_binding_plan(catalog, cfg)


def test_named_immutable_trigger_must_reject_actual_mutations(offline_binding):
    cfg, catalog = offline_binding
    trigger = next(
        row
        for row in catalog.triggers
        if row["name"] == "tapdb_runtime_scope_immutable"
    )
    trigger["when"] = "false"
    trigger["definition"] = (
        "CREATE TRIGGER tapdb_runtime_scope_immutable BEFORE UPDATE OR DELETE ON qualification_scope.tapdb_runtime_principal_scope FOR EACH ROW WHEN (false) EXECUTE FUNCTION qualification_scope.tapdb_reject_runtime_scope_mutation()"
    )
    with pytest.raises(rp.RuntimePrincipalError):
        rp.build_runtime_principal_binding_plan(catalog, cfg)


def test_binding_does_not_grant_unknown_operator_only_routine(
    offline_binding, tmp_path
):
    cfg, catalog = offline_binding
    catalog.functions.append(
        catalog.function("historical_operator_export", security_definer=True)
    )
    receipt = tmp_path / "operator-routine.json"
    rp.bind_runtime_principal(cfg, receipt_path=receipt)
    rp.bind_runtime_principal(cfg, apply=True, receipt_path=receipt)
    assert not any(
        sql.startswith("GRANT EXECUTE") and '"historical_operator_export"' in sql
        for sql, _ in catalog.commands
    ), "Core runtime binding must not expand access to a retained operator-only routine"


def test_binding_does_not_infer_unknown_allocator_access(offline_binding):
    cfg, catalog = offline_binding
    catalog.objects.append(
        catalog.relation("historical_allocator", kind="S", protected=False)
    )
    planned = rp.build_runtime_principal_binding_plan(catalog, cfg)
    assert "historical_allocator" not in {
        grant["name"] for grant in planned["grants"]
    }, "An unrelated preserved sequence is not automatically a core runtime allocator"
