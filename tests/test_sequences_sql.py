import pytest

from daylily_tapdb.sequences import (
    _build_ensure_instance_prefix_sequence_sql,
    _normalize_instance_prefix,
)


def test_ensure_instance_prefix_sequence_sql_uses_euid_seq_and_prefix():
    sql = _build_ensure_instance_prefix_sequence_sql("agx_instance_seq").lower()
    assert "max(euid_seq)" in sql
    assert "where euid_prefix = :prefix" in sql
    assert "generic_template" in sql
    assert "audit_log" in sql
    assert "regexp_replace(" not in sql
    assert "euid like" not in sql


@pytest.mark.parametrize("prefix", ["G-X", "UQ", "", "   "])
def test_normalize_instance_prefix_rejects_invalid(prefix: str):
    with pytest.raises(ValueError):
        _normalize_instance_prefix(prefix)
