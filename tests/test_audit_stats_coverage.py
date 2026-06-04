from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from daylily_tapdb import audit as audit_mod
from daylily_tapdb import stats as stats_mod
from daylily_tapdb.web import runtime as runtime_mod


class _MappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def one(self):
        return self._rows[0]


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), dict(params or {})))
        return _MappingsResult(self.rows)


def test_query_audit_trail_builds_filters_and_entries():
    changed_at = datetime(2026, 6, 4, tzinfo=UTC)
    session = _Session(
        [
            {
                "euid": "Z-SMP-1Q",
                "changed_by": "admin@example.com",
                "operation_type": "UPDATE",
                "changed_at": changed_at,
                "name": "Sample",
                "polymorphic_discriminator": "generic_instance",
                "category": "SMP",
                "type": "sample",
                "subtype": "tube",
                "bstatus": "active",
                "old_value": "{}",
                "new_value": '{"ok": true}',
            }
        ]
    )

    rows = audit_mod.query_audit_trail(
        session,
        changed_by="admin@example.com",
        euid="Z-SMP-1Q",
        since=changed_at,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        limit=7,
        order="asc",
    )

    sql, params = session.calls[0]
    assert "al.changed_by = :changed_by" in sql
    assert "al.rel_table_euid_fk = :euid" in sql
    assert "ORDER BY al.changed_at ASC" in sql
    assert params["limit"] == 7
    assert rows == [
        audit_mod.AuditEntry(
            euid="Z-SMP-1Q",
            changed_by="admin@example.com",
            operation_type="UPDATE",
            changed_at=changed_at,
            name="Sample",
            polymorphic_discriminator="generic_instance",
            category="SMP",
            type="sample",
            subtype="tube",
            bstatus="active",
            old_value="{}",
            new_value='{"ok": true}',
        )
    ]


def test_query_audit_trail_defaults_to_desc_order_without_filters():
    session = _Session([])

    assert audit_mod.query_audit_trail(session) == []
    sql, params = session.calls[0]
    assert "WHERE" not in sql
    assert "ORDER BY al.changed_at DESC" in sql
    assert params == {"limit": 500}


def test_template_instance_and_lineage_stats_build_scoped_queries():
    age = timedelta(days=2)
    created = datetime(2026, 6, 4, tzinfo=UTC)

    template_session = _Session(
        [
            {
                "total": 3,
                "distinct_types": 2,
                "distinct_subtypes": 2,
                "distinct_categories": 1,
                "latest_created": created,
                "earliest_created": created,
                "average_age": age,
                "singleton_count": 1,
            }
        ]
    )
    instance_session = _Session(
        [
            {
                "total": 4,
                "distinct_types": 2,
                "distinct_poly": 1,
                "distinct_categories": 2,
                "distinct_subtypes": 3,
                "latest_created": created,
                "earliest_created": created,
                "average_age": age,
            }
        ]
    )
    lineage_session = _Session(
        [
            {
                "total": 5,
                "distinct_parent_types": 2,
                "distinct_child_types": 2,
                "distinct_poly": 1,
                "distinct_categories": 1,
                "latest_created": created,
                "earliest_created": created,
                "average_age": age,
            }
        ]
    )

    template_stats = stats_mod.get_template_stats(
        template_session,
        include_deleted=True,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
    )
    instance_stats = stats_mod.get_instance_stats(
        instance_session,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
    )
    lineage_stats = stats_mod.get_lineage_stats(
        lineage_session,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
    )

    assert template_stats.total == 3
    assert template_stats.singleton_count == 1
    assert instance_stats.distinct_polymorphic_discriminators == 1
    assert lineage_stats.distinct_parent_types == 2
    for session in (template_session, instance_session, lineage_session):
        sql, params = session.calls[0]
        assert "domain_code = :domain_code" in sql
        assert "issuer_app_code = :issuer_app_code" in sql
        assert params["domain_code"] == "Z"
        assert params["issuer_app_code"] == "daylily-tapdb"


def test_stats_default_filters_exclude_deleted():
    session = _Session(
        [
            {
                "total": 0,
                "distinct_types": 0,
                "distinct_subtypes": 0,
                "distinct_categories": 0,
                "latest_created": None,
                "earliest_created": None,
                "average_age": None,
                "singleton_count": 0,
            }
        ]
    )

    stats = stats_mod.get_template_stats(session)

    assert stats.total == 0
    assert session.calls[0][1] == {"is_deleted": False}


def test_runtime_helper_branches_and_cache_cleanup(caplog):
    assert runtime_mod._parse_bool("yes", default=False) is True
    assert runtime_mod._parse_bool("off", default=True) is False
    assert runtime_mod._parse_bool("maybe", default=True) is True
    assert runtime_mod._audit_username_for_session("  alice  ") == "alice"
    assert runtime_mod._audit_username_for_session("") == "unknown"
    with pytest.raises(RuntimeError, match="schema_name"):
        runtime_mod._require_schema_name({})

    class _SessionNoPostgres:
        bind = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

        def __init__(self):
            self.executed = False

        def execute(self, stmt, params=None):
            self.executed = True

    sqlite_session = _SessionNoPostgres()
    runtime_mod._set_search_path(sqlite_session, "tapdb")
    assert sqlite_session.executed is False

    class _Engine:
        def dispose(self):
            raise RuntimeError("dispose failed")

    runtime_mod._bundles[("cfg", "schema")] = runtime_mod.RuntimeBundle(
        config_path="cfg",
        target_name="target",
        engine=_Engine(),
        SessionFactory=lambda: None,
        cfg={},
        schema_name="schema",
    )
    runtime_mod._clear_runtime_cache_for_tests()
    assert "Error disposing DAG runtime engine" in caplog.text
