"""Complete packaged security metadata and failure-path contract tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from daylily_tapdb import runtime_catalog_contract as rc
from daylily_tapdb import runtime_principal as rp


def _catalog():
    contract = rc.canonical_security_contract("canonical", "canonical")
    tables = rp._WRITABLE | rp._READABLE | {rp._SCOPE}
    policies = []
    for table in tables - {"_tapdb_migrations"}:
        policies.append(
            {
                "relation": table,
                "name": "tapdb_operator_access",
                "command": "*",
                "permissive": True,
                "roles": "{10}",
                "using": "true",
                "with_check": "true",
            }
        )
        if table != rp._SCOPE:
            using, check = contract["policies"][table]
            policies.append(
                {
                    "relation": table,
                    "name": table + "_scope_isolation",
                    "command": "*",
                    "permissive": True,
                    "roles": "{0}",
                    "using": using,
                    "with_check": check,
                }
            )
    return contract, {
        "owner": "operator",
        "owner_oid": 10,
        "managed_tables": tables,
        "policies": policies,
        "functions": [
            {**copy.deepcopy(row), "owner": "operator"}
            for row in contract["functions"].values()
        ],
        "triggers": [copy.deepcopy(row) for row in contract["triggers"].values()],
    }


def test_complete_canonical_authority_and_preserved_extra_metadata():
    contract, catalog = _catalog()
    catalog["functions"].append(
        {"name": "historical", "arguments": "", "owner": "another"}
    )
    catalog["policies"].append({"relation": "historical", "name": "arbitrary"})
    catalog["triggers"].append({"relation": "historical", "name": "arbitrary"})
    granted = rc.validate_managed_security(contract, **catalog)
    assert len(granted) == 30
    assert all(row["name"] != "historical" for row in granted)


def test_allocator_api_and_resolution_are_explicitly_schema_bound():
    contract = rc.canonical_security_contract("canonical", "canonical")
    allocators = {
        "set_generic_template_euid",
        "set_generic_instance_euid",
        "set_generic_instance_lineage_euid",
        "set_audit_log_euid",
    }
    for name in allocators:
        function = contract["functions"][(name, "")]
        assert function["settings"] == ["search_path=canonical, pg_catalog, pg_temp"]
        assert function["security_definer"] is False
        assert "TG_TABLE_SCHEMA" in function["source"]
        assert "pg_catalog.nextval(seq_name::pg_catalog.regclass)" in function["source"]
        assert "SELECT nextval" not in function["source"]
    helper = contract["functions"][("tapdb_get_identity_prefix", "entity_name text")]
    assert helper["settings"] == ["search_path=canonical, pg_catalog, pg_temp"]
    assert helper["security_definer"] is False
    assert "FROM %I.tapdb_identity_prefix_config" in helper["source"]
    assert "pg_catalog.current_schema()" in helper["source"]
    assert (
        "FROM %I.generic_template"
        in contract["functions"][("set_generic_instance_euid", "")]["source"]
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "inline_drift",
        "asset_drift",
        "duplicate_inline",
        "missing_start",
        "missing_end",
        "signature",
        "missing_function",
        "duplicate_function",
        "extra_base_definition",
        "extra_rls_definition",
    ],
)
def test_shared_allocator_copy_cannot_drift_or_hide_duplicates(mutation, monkeypatch):
    assets = rc.security_assets()
    original = assets["allocator_functions.sql"]
    function = next(rc._FUNCTION.finditer(original)).group()
    if mutation == "inline_drift":
        assets["tapdb_schema.sql"] = assets["tapdb_schema.sql"].replace(
            "pg_catalog.nextval(", "nextval(", 1
        )
    elif mutation == "asset_drift":
        assets["allocator_functions.sql"] = original.replace(
            "pg_catalog.nextval(", "nextval(", 1
        )
    elif mutation == "duplicate_inline":
        assets["tapdb_schema.sql"] += original
    elif mutation in {"extra_base_definition", "extra_rls_definition"}:
        name = "tapdb_schema.sql" if mutation == "extra_base_definition" else "rls.sql"
        assets[name] += "\n" + function
    else:
        changed = original
        if mutation == "missing_start":
            changed = changed.replace(
                "-- tapdb-managed-allocator-functions:start", "-- start"
            )
        elif mutation == "missing_end":
            changed = changed.replace(
                "-- tapdb-managed-allocator-functions:end", "-- end"
            )
        elif mutation == "signature":
            changed = changed.replace("entity_name TEXT", "entity_name UUID")
        elif mutation == "missing_function":
            changed = changed.replace(function, "")
        elif mutation == "duplicate_function":
            changed = changed.replace(function, function + "\n" + function)
        assets["allocator_functions.sql"] = changed
        assets["tapdb_schema.sql"] = assets["tapdb_schema.sql"].replace(
            original, changed
        )
    monkeypatch.setattr(rc, "security_assets", lambda: assets)
    with pytest.raises(rc.RuntimeCatalogContractError, match="allocator"):
        rc.canonical_security_contract("canonical", "canonical")


def test_allocator_migration_is_only_the_shared_function_asset():
    from daylily_tapdb.migration_identity import _expand_migration_source

    schema = Path(__file__).resolve().parents[1] / "schema"
    migration = (
        schema / "migrations/20260910_233000_pin_managed_allocator_resolution.sql"
    )
    source = migration.read_text()
    assert set(
        line.removeprefix("-- tapdb-allow-schema: ")
        for line in source.splitlines()
        if line.startswith("-- tapdb-allow-schema: ")
    ) == {
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
    }
    assert "-- tapdb-include: ../allocator_functions.sql" in source
    assert "tapdb_schema.sql" not in source
    assert (schema / "allocator_functions.sql").read_text() in _expand_migration_source(
        migration, schema_root=schema
    )


@pytest.mark.parametrize(
    "field",
    list(
        rc.canonical_security_contract("canonical", "canonical")["functions"].values()
    )[0].keys(),
)
def test_every_canonical_routine_metadata_field_is_required(field):
    contract, catalog = _catalog()
    catalog["functions"][0].pop(field)
    with pytest.raises(rc.RuntimeCatalogContractError):
        rc.validate_managed_security(contract, **catalog)


@pytest.mark.parametrize(
    "mutation",
    [
        "owner",
        "source",
        "missing",
        "extra_trigger",
        "missing_trigger",
        "trigger_when",
        "missing_trigger_when",
        "policy_missing",
        "policy_role",
        "policy_using",
        "policy_check",
    ],
)
def test_security_contract_adversarial_metadata(mutation):
    contract, catalog = _catalog()
    if mutation in {"owner", "source"}:
        catalog["functions"][0][mutation] = "different"
    elif mutation == "missing":
        catalog["functions"].pop()
    elif mutation == "extra_trigger":
        catalog["triggers"].append(
            {"relation": "generic_instance", "name": "unexpected"}
        )
    elif mutation == "missing_trigger":
        catalog["triggers"].pop()
    elif mutation == "trigger_when":
        catalog["triggers"][0]["when"] = "false"
    elif mutation == "missing_trigger_when":
        catalog["triggers"][0].pop("when")
    elif mutation == "policy_missing":
        catalog["policies"].pop()
    elif mutation == "policy_role":
        catalog["policies"][0]["roles"] = "{0}"
    elif mutation == "policy_using":
        catalog["policies"][0]["using"] = "false"
    else:
        catalog["policies"][0]["with_check"] = None
    with pytest.raises(rc.RuntimeCatalogContractError):
        rc.validate_managed_security(contract, **catalog)


def test_expression_tokens_preserve_semantics_and_quoted_whitespace():
    assert rc.expression_tokens("(a AND (b OR c))") != rc.expression_tokens(
        "((a AND b) OR c)"
    )
    assert rc.expression_tokens("'a b'") != rc.expression_tokens("'ab'")
    assert rc.expression_tokens('"a b"') != rc.expression_tokens('"ab"')
    assert rc.expression_tokens("a \n =\t b") == rc.expression_tokens("a=b")
    for expression in ("a; b", "a;", "'unterminated"):
        with pytest.raises(rc.RuntimeCatalogContractError, match="token"):
            rc.expression_tokens(expression)


def _distribution(*, direct=None, files=(), located=None):
    return SimpleNamespace(
        read_text=lambda _: json.dumps(direct) if direct is not None else None,
        files=files,
        locate_file=lambda _: located,
    )


def test_installed_asset_uses_distribution_file_not_cwd(tmp_path, monkeypatch):
    asset = tmp_path / "rls.sql"
    asset.write_text("reviewed")
    package = _distribution(files=["../../../schema/rls.sql"], located=asset)
    monkeypatch.setattr(rc, "distribution", lambda _: package)
    assert rc._asset_path("rls.sql") == asset
    package.files = ["schema/rls.sql"]
    assert rc._asset_path("rls.sql") == asset
    package.files = []
    with pytest.raises(rc.RuntimeCatalogContractError, match="one exact"):
        rc._asset_path("rls.sql")
    package.files = ["schema/rls.sql"]
    package.locate_file = lambda _: tmp_path / "missing"
    with pytest.raises(rc.RuntimeCatalogContractError, match="missing"):
        rc._asset_path("rls.sql")


def test_editable_asset_matches_the_imported_source(monkeypatch):
    root = Path(rc.__file__).resolve().parents[1]
    monkeypatch.setattr(
        rc,
        "distribution",
        lambda _: _distribution(
            direct={"dir_info": {"editable": True}, "url": root.as_uri()}
        ),
    )
    assert rc._asset_path("rls.sql") == root / "schema" / "rls.sql"


@pytest.mark.parametrize(
    "url",
    [
        "https://example.invalid/source",
        "file://remote/source",
        "file:///wrong/distribution",
    ],
)
def test_editable_asset_identity_must_match_imported_distribution(url, monkeypatch):
    monkeypatch.setattr(
        rc,
        "distribution",
        lambda _: _distribution(direct={"dir_info": {"editable": True}, "url": url}),
    )
    with pytest.raises(rc.RuntimeCatalogContractError):
        rc._asset_path("rls.sql")


@pytest.mark.parametrize("mutation", ["function", "language", "trigger", "pin"])
def test_unrecognized_packaged_contract_fails_closed(mutation, monkeypatch):
    assets = rc.security_assets()
    if mutation == "function":
        assets["rls.sql"] += "\nCREATE OR REPLACE FUNCTION unsupported"
    elif mutation == "language":
        assets["rls.sql"] = assets["rls.sql"].replace(
            "LANGUAGE sql", "LANGUAGE unsupported"
        )
    elif mutation == "trigger":
        assets["rls.sql"] += "\nCREATE TRIGGER unsupported"
    else:
        assets["rls.sql"] = assets["rls.sql"].replace(
            "FOREACH function_name IN ARRAY ARRAY[",
            "FOREACH unsupported IN ARRAY ARRAY[",
        )
    monkeypatch.setattr(rc, "security_assets", lambda: assets)
    with pytest.raises(rc.RuntimeCatalogContractError):
        rc.canonical_security_contract("canonical", "canonical")


@pytest.mark.parametrize(
    "mapping, allowed",
    [
        ({"kind": "unmapped"}, False),
        ({"kind": "owned_column", "columns": []}, False),
        (
            {
                "kind": "owned_column",
                "columns": [
                    {"schema_name": "canonical", "table_name": "generic_instance"}
                ],
            },
            True,
        ),
        (
            {
                "kind": "owned_column",
                "columns": [
                    {
                        "schema_name": "canonical",
                        "table_name": "tapdb_identity_prefix_config",
                    }
                ],
            },
            False,
        ),
        (
            {
                "kind": "prefix",
                "prefix": "ZZ",
                "evidence": [{"source": "catalog_annotation"}],
            },
            False,
        ),
        (
            {
                "kind": "prefix",
                "prefix": "WX",
                "evidence": [{"source": "catalog_annotation"}],
            },
            True,
        ),
        (
            {
                "kind": "prefix",
                "prefix": "ZZ",
                "evidence": [
                    {
                        "source": "stored_rows",
                        "table": "generic_instance",
                        "scope": {"domain_code": "Z", "owner_repo_name": "different"},
                    }
                ],
            },
            False,
        ),
        (
            {
                "kind": "prefix",
                "prefix": "ZZ",
                "evidence": [
                    {
                        "source": "stored_rows",
                        "table": "generic_instance",
                        "scope": {"domain_code": "Z", "owner_repo_name": "owner"},
                    }
                ],
            },
            True,
        ),
    ],
)
def test_mapping_evidence_is_not_automatically_runtime_authority(
    mapping, allowed, monkeypatch
):
    from daylily_tapdb import sequences

    target = {
        "schema_name": "canonical",
        "operator_user": "operator",
        "domain_code": "Z",
        "owner_repo_name": "owner",
        "config_path": "/explicit/config",
    }
    state = {"name": "allocator", "owner": "operator", "mapping": mapping}
    monkeypatch.setattr(
        sequences,
        "capture_runtime_sequence_bindings",
        lambda *args, **kwargs: {"allocator": state},
    )
    result = rp._managed_sequence_bindings(
        object(), target, [{"name": "allocator", "owner": "operator", "kind": "S"}]
    )
    assert bool(result) is allowed


def test_mapping_errors_and_owner_mismatch_fail_closed(monkeypatch):
    from daylily_tapdb import sequences

    target = {
        "schema_name": "canonical",
        "operator_user": "operator",
        "config_path": "/explicit/config",
    }

    def invalid(*args, **kwargs):
        raise sequences.SequenceProtectionError("unproven")

    monkeypatch.setattr(sequences, "capture_runtime_sequence_bindings", invalid)
    with pytest.raises(rp.RuntimePrincipalError, match="cannot be proven"):
        rp._managed_sequence_bindings(object(), target, [])
    monkeypatch.setattr(
        sequences,
        "capture_runtime_sequence_bindings",
        lambda *args, **kwargs: {
            "allocator": {
                "owner": "other",
                "mapping": {
                    "kind": "owned_column",
                    "columns": [
                        {"schema_name": "canonical", "table_name": "generic_instance"}
                    ],
                },
            }
        },
    )
    with pytest.raises(rp.RuntimePrincipalError, match="operator owner"):
        rp._managed_sequence_bindings(
            object(), target, [{"name": "allocator", "owner": "other", "kind": "S"}]
        )
