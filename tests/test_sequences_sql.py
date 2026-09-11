import pytest

from daylily_tapdb.sequences import (
    _build_ensure_instance_prefix_sequence_sql,
    _normalize_instance_prefix,
)


def test_provisioning_state_sql_has_no_fixed_table_floor_or_repair():
    sql = _build_ensure_instance_prefix_sequence_sql(
        "agx_instance_seq", schema_name="explicit_schema"
    ).lower()
    assert '"explicit_schema"."agx_instance_seq"' in sql
    for field in (
        "seqincrement",
        "seqmin",
        "seqmax",
        "seqstart",
        "seqcache",
        "seqcycle",
        "last_value",
        "is_called",
    ):
        assert field in sql
    assert "generic_template" not in sql
    assert "audit_log" not in sql
    assert "max(euid_seq)" not in sql
    assert "regexp_replace(" not in sql
    assert "euid like" not in sql
    assert "setval(" not in sql
    assert "to_regclass(:qualified)" in sql


def test_provisioning_state_sql_quotes_both_identifiers():
    sql = _build_ensure_instance_prefix_sequence_sql(
        'seq"quoted', schema_name='schema"quoted'
    )
    assert '"schema""quoted"."seq""quoted"' in sql


@pytest.mark.parametrize("prefix", ["G-X", "UQ", "", "   "])
def test_normalize_instance_prefix_rejects_invalid(prefix: str):
    with pytest.raises(ValueError):
        _normalize_instance_prefix(prefix)
