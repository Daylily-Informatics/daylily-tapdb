"""Pure checks only; no DB fixtures, connection, network or container startup."""

import copy
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from daylily_tapdb.security_context import configured_graph_tenant_scope
from daylily_tapdb.services.scope_corrections import (
    CONTRACT, _sha, _validate_inventory, _validate_plan, validate_request,
)


def request():
    # EUID read from the sanitized, owning Atlas closure receipt; never generated.
    return {
        "target": {"database": "review", "schema_name": "review", "domain_code": "M",
                   "owner_repo_name": "atlas", "operator_user": "review_operator",
                   "config_path": "/explicit/operator.yaml"},
        "operation_id": "review-scope-1", "actor": "review-operator",
        "reason": "Reviewed native scope correction", "approval_ref": "review-evidence",
        "migration_receipt_sha256": "a" * 64, "tenant_authority_sha256": "b" * 64,
        "tenant_id": "00000000-0000-0000-0000-000000000000",
        "source_euids": ["M-AGX-ANG5"],
        "allowed_templates": ["ordering/order/generic/1.0/"],
        "retire_lineage_euids": [],
    }


def inventory():
    common = {"is_deleted": False, "tenant_id": None, "domain_code": "M",
              "issuer_app_code": "atlas", "category": "ordering", "type": "order",
              "subtype": "generic", "version": "1.0", "euid_prefix": "AGX"}
    return {
        "sources": [{**common, "uid": 1, "euid": "M-AGX-ANG5", "template_uid": 2,
                     "has_identity_key": False, "has_identity_claim": False}],
        "templates": [{**common, "uid": 2, "instance_prefix": "AGX"}],
        "neighbors": [], "lineages": [],
    }


def test_request_binds_explicit_tenant_and_exact_source_without_inference():
    value = validate_request(request())
    assert value["tenant_id"] == "00000000-0000-0000-0000-000000000000"
    assert value["source_euids"] == ["M-AGX-ANG5"]
    for change in ({"source_euids": []}, {"source_euids": ["M-AGX-ANG5"] * 2},
                   {"tenant_authority_sha256": ""}, {"scope_default": "global"}):
        with pytest.raises(ValueError):
            validate_request({**request(), **change})


@pytest.mark.parametrize("field", ["has_identity_key", "has_identity_claim", "is_deleted"])
def test_claimed_or_inactive_sources_are_not_scope_migrated(field):
    before = inventory()
    before["sources"][0][field] = True
    with pytest.raises(ValueError):
        _validate_inventory(request(), before)


def test_cross_owner_neighbor_and_missing_retirement_are_rejected():
    before = inventory()
    before["neighbors"] = [{"domain_code": "M", "issuer_app_code": "other-owner"}]
    with pytest.raises(ValueError, match="owner/domain"):
        _validate_inventory(request(), before)
    value = request()
    # Real persisted lineage identity from the same retained closure evidence.
    value["retire_lineage_euids"] = ["M-EDG-A9KV"]
    with pytest.raises(ValueError, match="active incident"):
        _validate_inventory(value, inventory())


def test_exact_plan_hash_cannot_be_reused_for_a_different_tenant():
    plan = {"contract": CONTRACT, "request": request(), "before": inventory()}
    plan["sha256"] = _sha(plan)
    assert _validate_plan(plan, plan["sha256"])["sha256"] == plan["sha256"]
    altered = copy.deepcopy(plan)
    altered["request"]["tenant_id"] = "11111111-1111-1111-1111-111111111111"
    with pytest.raises(ValueError, match="checksum"):
        _validate_plan(altered, plan["sha256"])


def test_single_tenant_global_read_scope_is_explicit_not_an_extra_tenant():
    tenant = request()["tenant_id"]
    assert configured_graph_tenant_scope({"tenant_id": tenant, "allow_global_claims": True}) == frozenset({tenant, None})
    assert configured_graph_tenant_scope({"tenant_id": tenant, "allow_global_claims": False}) is None
    assert configured_graph_tenant_scope({"tenant_id": tenant}) is None


def test_migration_replaces_only_the_same_native_function_body():
    root = Path(__file__).resolve().parents[1]
    fresh = (root / "schema/rls.sql").read_text()
    upgrade = (root / "schema/migrations/20260926_220000_reviewed_scope_correction.sql").read_text()
    start = "CREATE OR REPLACE FUNCTION tapdb_validate_lineage_endpoint_scope()"
    end = "$$ LANGUAGE plpgsql;"
    def body(source):
        return source[source.index(start):source.index(end, source.index(start)) + len(end)]
    assert body(fresh) == body(upgrade)
    assert "tapdb-allow-" not in upgrade
    assert "tapdb-transformation:" not in upgrade
    for unchanged in ("generic_instance.tenant_id", "generic_instance_lineage.tenant_id"):
        table, column = unchanged.split(".")
        from daylily_tapdb.migration_identity import _IMMUTABLE_COLUMNS
        assert column in _IMMUTABLE_COLUMNS[table]


def test_added_unrelated_lineage_is_rejected(monkeypatch):
    from daylily_tapdb.services import scope_corrections as scope
    before = {group: [] for group in ("sources", "lineages", "neighbors", "templates")}
    after = copy.deepcopy(before)
    after["lineages"] = [{"euid": "M-EDG-A9KV", "parent_instance_uid": 42,
        "child_instance_uid": 43, "relationship_type": "unrelated", "is_deleted": False,
        "tenant_id": request()["tenant_id"], "domain_code": "M", "issuer_app_code": "atlas"}]
    monkeypatch.setattr(scope, "_inventory", lambda *_: after)
    monkeypatch.setattr(scope, "_receipt_object", lambda *_: None)
    with pytest.raises(RuntimeError, match="unexpected added lineage"):
        scope._verify_after(object(), {"request": request()}, before)


def test_changed_function_is_accepted_by_native_catalog_parser(monkeypatch):
    from daylily_tapdb import runtime_catalog_contract as contract
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(contract, "security_assets", lambda: {
        name: (root / "schema" / name).read_text()
        for name in ("tapdb_schema.sql", "rls.sql", "allocator_functions.sql", "runtime_identity_authorization.sql")
    })
    parsed = contract.canonical_security_contract("review", "review")
    function = parsed["functions"][("tapdb_validate_lineage_endpoint_scope", "")]
    assert function["settings"] == ["search_path=review, pg_catalog, pg_temp"]
    assert function["security_definer"] is False
    assert parsed["triggers"][("generic_instance_lineage", "zz_tapdb_validate_lineage_endpoint_scope")]["type"] == 23


@pytest.mark.parametrize("parent_scope,child_scope,expected_scope", [
    (None, "tenant", "tenant"), ("tenant", None, "tenant"),
    (None, None, None), ("tenant", "tenant", "tenant"),
    ("tenant", "other", "tenant"),
])
def test_factory_keeps_shared_relationship_in_scoped_endpoint(parent_scope, child_scope, expected_scope):
    from daylily_tapdb.factory.instance import InstanceFactory
    values = {None: None, "tenant": UUID(int=0), "other": UUID(int=1)}
    # Non-persisting factory check: plain labels, no fabricated Meridian EUIDs.
    parent = SimpleNamespace(euid="parent-fixture", uid=1,
        tenant_id=values[parent_scope], polymorphic_discriminator="generic_instance")
    child = SimpleNamespace(euid="child-fixture", uid=2,
        tenant_id=values[child_scope], polymorphic_discriminator="generic_instance")
    added = []
    session = SimpleNamespace(add=added.append, flush=lambda: None)
    factory = object.__new__(InstanceFactory)
    edge = factory.link_instances(session, parent, child, "catalog_relation")
    assert added == [edge]
    assert edge.tenant_id == values[expected_scope]
    assert (edge.parent_instance_uid, edge.child_instance_uid) == (1, 2)
    # A different scoped child is never silently adopted. PostgreSQL retains
    # authority to reject the edge or apply the existing explicit multitenant rule.


def test_shared_domain_guard_retains_bound_scope_and_xrf_boundary():
    root = Path(__file__).resolve().parents[1]
    source = (root / "schema/rls.sql").read_text()
    branch = source.split("-- A single authorized tenant", 1)[1].split(
        "-- An explicit service allowlist", 1)[0]
    for clause in (
        "NOT tapdb_session_role_is_operator()",
        "tapdb_allow_global_rows()",
        "((parent_tenant IS NULL) <> (child_tenant IS NULL))",
        "NEW.tenant_id IS NOT DISTINCT FROM COALESCE(parent_tenant, child_tenant)",
        "NEW.tenant_id = ANY(tapdb_allowed_tenant_ids())",
        "(parent_category, parent_type) <> ('reference', 'external_identifier')",
        "(child_category, child_type) <> ('reference', 'external_identifier')",
    ):
        assert clause in branch
    assert "TG_OP" not in branch  # Applies to both INSERT and UPDATE.
    assert "cardinality(array_remove(" in source  # Existing multitenant rule retained.
