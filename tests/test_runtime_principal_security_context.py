"""Fail-closed transaction inputs and purpose-specific operator authority."""

from types import SimpleNamespace

import pytest

from daylily_tapdb.security_context import (
    TapdbTransactionContext,
    apply_transaction_context,
    assert_operator_role,
    operator_role_assertion_sql,
    transaction_context_pgoptions,
)


def context(**changes):
    return TapdbTransactionContext(
        **{
            "config_identity": "/explicit path/target.yaml",
            "schema_name": "target_schema",
            "domain_code": "Z",
            "owner_repo_name": "daylily-tapdb",
            "tenant_id": None,
            "actor": "unit:operator",
            "allow_global_rows": False,
            **changes,
        }
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"config_identity": ""},
        {"schema_name": " trailing "},
        {"domain_code": "\x00"},
        {"owner_repo_name": None},
        {"actor": "\n"},
        {"tenant_id": "invalid"},
        {"allow_global_rows": "true"},
        {"additional_tenant_ids": "*"},
        {"additional_tenant_ids": [None]},
        {"additional_tenant_ids": ["invalid"]},
        {"additional_tenant_ids": ["00000000-0000-0000-0000-000000000001"] * 2},
    ],
)
def test_context_rejects_missing_or_malformed_authority(changes):
    with pytest.raises(ValueError):
        context(**changes)


def test_context_is_atomic_explicit_and_transaction_local():
    calls = []
    session = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        execute=lambda sql, params=None: calls.append((str(sql), params)),
    )
    apply_transaction_context(session, context())
    settings = {params["name"]: params["value"] for _, params in calls if params}
    assert settings["session.current_config_identity"] == "/explicit path/target.yaml"
    assert settings["session.current_tenant_id"] == ""
    assert settings["session.additional_tenant_ids"] == "{}"
    assert settings["session.allow_global_rows"] == "false"
    assert all(
        "set_config(:name, :value, true)" in sql for sql, params in calls if params
    )
    assert calls[-1][0] == "SELECT tapdb_assert_runtime_role()"
    calls.clear()
    apply_transaction_context(
        session,
        context(
            tenant_id="00000000-0000-0000-0000-000000000001", allow_global_rows=True
        ),
        assert_runtime_role=False,
    )
    assert all("tapdb_assert_runtime_role" not in sql for sql, _ in calls)
    apply_transaction_context(
        SimpleNamespace(dialect=SimpleNamespace(name="sqlite")), context()
    )


def test_pgoptions_escape_spaces_and_install_explicit_global_scope():
    options = transaction_context_pgoptions(context(actor="operator\\with space"))
    assert "current_config_identity=/explicit\\ path/target.yaml" in options
    assert "current_username=operator\\\\with\\ space" in options
    assert "current_tenant_id= " in options
    assert "allow_global_rows=false" in options
    assert "allow_global_rows=true" in transaction_context_pgoptions(
        context(allow_global_rows=True)
    )


def test_operator_assertion_purposes_and_exact_targets():
    strict = operator_role_assertion_sql(
        schema_name="managed_schema", operator_user="exact'operator"
    )
    assert "session_user = 'exact''operator'" in strict
    assert "n.nspname = 'managed_schema'" in strict
    assert "p.polroles = ARRAY[r.oid]" in strict
    assert "NOT restriction.polpermissive" in strict
    assert "OR r.rolcreatedb" not in strict
    assert "d.datdba = r.oid" not in strict
    assert "OR r.rolcreatedb" in operator_role_assertion_sql(allow_create_database=True)
    assert "d.datdba = r.oid" in operator_role_assertion_sql(allow_database_owner=True)
    for keyword in ("schema_name", "operator_user"):
        with pytest.raises(ValueError):
            operator_role_assertion_sql(**{keyword: ""})


def test_operator_assertion_never_infers_aurora_privileges_from_role_name():
    queries = []
    row = ("rds_superuser", False, False)
    session = SimpleNamespace(
        execute=lambda sql: (
            queries.append(str(sql)) or SimpleNamespace(one_or_none=lambda: row)
        )
    )
    assert_operator_role(
        session, schema_name="managed_schema", operator_user="rds_superuser"
    )
    assert len(queries) == 2
    assert "tapdb_operator_access" in queries[1]
    row = None
    with pytest.raises(RuntimeError, match="no authenticated"):
        assert_operator_role(session)
