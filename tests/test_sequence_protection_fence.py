"""Strict provider, visibility, worker and inherited-ACL boundary predicates."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import make_url

from daylily_tapdb import sequence_fence as fence
from daylily_tapdb.sequences import SequenceProtectionError


class Result:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value

    def mappings(self):
        return self

    def scalars(self):
        return self

    def all(self):
        return self.value

    def one(self):
        return self.value

    def one_or_none(self):
        return self.value

    def __iter__(self):
        return iter(self.value)


def role(name, oid, *, superuser=False):
    return dict(
        oid=oid,
        rolname=name,
        rolsuper=superuser,
        rolinherit=True,
        rolcreaterole=superuser,
        rolcreatedb=superuser,
        rolcanlogin=True,
        rolreplication=superuser,
        rolbypassrls=superuser,
        rolconnlimit=-1,
        rolvaliduntil=None,
    )


@pytest.fixture
def provider(monkeypatch):
    contract = {
        "schema_version": fence.PROVIDER_VERSION,
        "engine": "aurora-postgresql",
        "engine_version": "16.13",
        "aws_profile": "explicit-test-profile",
        "region": "us-west-2",
        "cluster_identifier": "owned-test-cluster",
        "cluster_arn": "arn:aws:rds:us-west-2:000000000000:cluster:owned-test-cluster",
        "cluster_resource_id": "cluster-provider-resource",
        "writer_endpoint": "writer.example.invalid",
        "sslmode": "verify-full",
    }
    catalog = [role("operator", 20), role("rdsadmin", 10, superuser=True)]
    memberships = []
    metadata = dict(
        Engine=contract["engine"],
        EngineVersion=contract["engine_version"],
        DBClusterIdentifier=contract["cluster_identifier"],
        DBClusterArn=contract["cluster_arn"],
        DbClusterResourceId=contract["cluster_resource_id"],
        Endpoint=contract["writer_endpoint"],
    )
    values = {
        "version": 160013,
        "aurora_version": "16.13.provider-build",
    }
    calls = []

    def execute(sql, params=None):
        query = str(sql)
        calls.append(query)
        if "server_version_num" in query:
            return Result(values["version"])
        if query == "SELECT pg_catalog.aurora_version()":
            return Result(values["aurora_version"])
        raise AssertionError(query)

    connection = SimpleNamespace(
        engine=SimpleNamespace(
            url=make_url(
                "postgresql://operator@writer.example.invalid:55434/control?sslmode=verify-full&sslrootcert=/explicit/provider-ca.pem"
            )
        ),
        execute=execute,
    )
    monkeypatch.setattr(
        fence, "role_catalog", lambda connection: (catalog, memberships)
    )
    import boto3

    class Session:
        def __init__(self, *, profile_name):
            calls.append(("profile", profile_name))

        def client(self, service, *, region_name):
            calls.append(("client", service, region_name))
            return self

        def describe_db_clusters(self, **kwargs):
            calls.append(("describe", kwargs))
            return {"DBClusters": values.get("clusters", [metadata])}

    monkeypatch.setattr(boto3, "Session", Session)
    return SimpleNamespace(
        connection=connection,
        contract=contract,
        catalog=catalog,
        memberships=memberships,
        metadata=metadata,
        values=values,
        calls=calls,
        target={"engine_type": "aurora", "host": contract["writer_endpoint"]},
    )


def prove(provider):
    return fence.provider_evidence(
        provider.connection,
        target=provider.target,
        operator_role="operator",
        provider_contract=provider.contract,
    )


def test_aurora_exception_requires_authenticated_exact_resource_and_catalog(provider):
    result = prove(provider)
    assert result["provider_role"] == provider.catalog[1]
    assert result["provider_metadata"] == provider.metadata
    assert ("profile", "explicit-test-profile") in provider.calls
    assert ("describe", {"DBClusterIdentifier": "owned-test-cluster"}) in provider.calls


@pytest.mark.parametrize(
    "field",
    [
        "rolsuper",
        "rolcanlogin",
        "rolcreatedb",
        "rolcreaterole",
        "rolreplication",
        "rolbypassrls",
    ],
)
def test_provider_attribute_drift_is_not_exempted(provider, field):
    provider.catalog[1][field] = False
    with pytest.raises(SequenceProtectionError, match="attributes"):
        prove(provider)


@pytest.mark.parametrize(
    "field",
    [
        "Engine",
        "EngineVersion",
        "DBClusterIdentifier",
        "DBClusterArn",
        "DbClusterResourceId",
        "Endpoint",
    ],
)
def test_authenticated_provider_metadata_must_match_every_pinned_field(provider, field):
    provider.metadata[field] = "different-provider-value"
    with pytest.raises(SequenceProtectionError, match="metadata"):
        prove(provider)


@pytest.mark.parametrize(
    "change",
    ["none", "extra", "missing", "version", "engine", "endpoint", "empty", "tls", "ca"],
)
def test_name_or_config_alone_never_proves_provider_identity(provider, change):
    if change == "none":
        provider.contract = None
    elif change == "extra":
        provider.contract["unreviewed"] = "value"
    elif change == "missing":
        provider.contract.pop("cluster_resource_id")
    elif change == "version":
        provider.contract["engine_version"] = "16.12"
    elif change == "engine":
        provider.contract["engine"] = "postgres"
    elif change == "endpoint":
        provider.contract["writer_endpoint"] = "reader.example.invalid"
    elif change == "empty":
        provider.contract["aws_profile"] = ""
    elif change == "tls":
        provider.connection.engine.url = (
            provider.connection.engine.url.update_query_dict({"sslmode": "require"})
        )
    else:
        provider.connection.engine.url = (
            provider.connection.engine.url.difference_update_query(["sslrootcert"])
        )
    with pytest.raises(SequenceProtectionError, match="pinned provider"):
        prove(provider)


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "unknown_super",
        "unknown_nologin_super",
        "operator_super",
        "provider_member",
        "provider_group",
    ],
)
def test_privilege_or_membership_unknowns_fail_closed(provider, change):
    if change == "missing":
        provider.catalog.pop()
    elif change in {"unknown_super", "unknown_nologin_super"}:
        provider.catalog.append(role("unverified_admin", 99, superuser=True))
        provider.catalog[-1]["rolcanlogin"] = change == "unknown_super"
    elif change == "operator_super":
        provider.catalog[0]["rolsuper"] = True
    else:
        provider.memberships.append(
            {
                "member": 10 if change == "provider_member" else 20,
                "roleid": 10 if change == "provider_group" else 30,
                "grantor": 10,
            }
        )
    with pytest.raises(SequenceProtectionError):
        prove(provider)


def test_provider_as_membership_grantor_is_not_a_membership_path(provider):
    provider.memberships.append(
        dict(
            roleid=30,
            member=20,
            grantor=10,
            admin_option=False,
            inherit_option=True,
            set_option=True,
        )
    )
    assert prove(provider)["provider_role"]["oid"] == 10


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", 160012),
        ("aurora_version", ""),
        ("clusters", []),
    ],
)
def test_live_server_must_corroborate_aurora_provider(provider, field, value):
    provider.values[field] = value
    with pytest.raises(SequenceProtectionError):
        prove(provider)


def test_community_has_no_provider_exemption_and_requires_exact_login(provider):
    provider.target = {"engine_type": "local"}
    with pytest.raises(SequenceProtectionError, match="must not receive"):
        prove(provider)
    provider.contract = None
    with pytest.raises(SequenceProtectionError, match="unknown privileged"):
        prove(provider)
    provider.catalog[:] = [role("operator", 20, superuser=True)]
    assert prove(provider)["provider_role"] is None
    provider.catalog[0]["rolcanlogin"] = False
    with pytest.raises(SequenceProtectionError, match="LOGIN"):
        prove(provider)


def census_connection(*, rows=(), visible=True, prepared=0, subscriptions=0):
    calls = []

    def execute(sql, params=None):
        query = str(sql)
        calls.append(query)
        if "rolsuper OR" in query:
            return Result(visible)
        if "pg_stat_clear_snapshot" in query:
            return Result(None)
        if "FROM pg_stat_activity" in query:
            return Result(rows)
        if "pg_prepared_xacts" in query:
            return Result(prepared)
        if "pg_subscription" in query:
            return Result(subscriptions)
        raise AssertionError(query)

    return SimpleNamespace(execute=execute), calls


def test_activity_census_refreshes_snapshot_before_every_check_and_never_reads_secrets():
    connection, calls = census_connection()
    for _ in range(2):
        fence.census(connection, database="target", database_oid=42)
    assert sum("pg_stat_clear_snapshot" in sql for sql in calls) == 2
    assert all("query," not in sql and "subconninfo" not in sql for sql in calls)


@pytest.mark.parametrize(
    "backend_type",
    [
        "client backend",
        "autovacuum worker",
        "parallel worker",
        "logical replication worker",
        "custom scheduler",
        None,
    ],
)
def test_every_other_target_backend_type_is_excluded(backend_type):
    connection, _ = census_connection(
        rows=[
            {
                "pid": 4,
                "backend_start": datetime(2026, 1, 1),
                "backend_type": backend_type,
                "usename": "operator",
            }
        ]
    )
    with pytest.raises(SequenceProtectionError, match="sessions|workers"):
        fence.census(connection, database="target", database_oid=42)


@pytest.mark.parametrize(
    "change",
    [
        "pid_reused",
        "obscured_start",
        "obscured_user",
        "visibility",
        "prepared",
        "subscription",
    ],
)
def test_retained_backend_cannot_mask_obscured_or_different_session(change):
    row = {
        "pid": 7,
        "backend_start": datetime(2026, 1, 1),
        "backend_type": "client backend",
        "usename": "operator",
    }
    retained = {"pid": 7, "backend_start": row["backend_start"].isoformat()}
    if change == "pid_reused":
        row["backend_start"] = datetime(2026, 2, 1)
    elif change == "obscured_start":
        row["backend_start"] = None
    elif change == "obscured_user":
        row["usename"] = None
    connection, _ = census_connection(
        rows=[row],
        visible=change != "visibility",
        prepared=int(change == "prepared"),
        subscriptions=int(change == "subscription"),
    )
    with pytest.raises(SequenceProtectionError):
        fence.census(
            connection, database="target", database_oid=42, retained_backend=retained
        )


def test_exact_retained_backend_is_the_only_permitted_activity():
    row = {
        "pid": 7,
        "backend_start": datetime(2026, 1, 1),
        "backend_type": "client backend",
        "usename": "operator",
    }
    connection, _ = census_connection(rows=[row])
    fence.census(
        connection,
        database="target",
        database_oid=42,
        retained_backend={"pid": 7, "backend_start": row["backend_start"].isoformat()},
    )


@pytest.mark.parametrize(
    "setting,aurora,accepted",
    [
        ("", False, True),
        ("pg_stat_statements", False, True),
        (
            "rdsutils,rds_casts,pg_stat_statements,writeforward,aws_s3_native,rds_blue_green",
            True,
            True,
        ),
        (
            "rdsutils,rds_casts,pg_stat_statements,writeforward,aws_s3_native,rds_blue_green",
            False,
            False,
        ),
        ("pg_cron", True, False),
        ("unknown_extension", False, False),
    ],
)
def test_startup_code_requires_explicit_supported_baseline(setting, aurora, accepted):
    connection = SimpleNamespace(
        execute=lambda sql, params=None: Result(
            setting if "current_setting" in str(sql) else []
        )
    )
    if accepted:
        assert (
            fence.worker_baseline(
                connection, database_oid=1, operator_role="operator", aurora=aurora
            )["settings"]["shared_preload_libraries"]
            == setting
        )
    else:
        with pytest.raises(SequenceProtectionError, match="startup-loaded"):
            fence.worker_baseline(
                connection, database_oid=1, operator_role="operator", aurora=aurora
            )


def test_target_role_setting_overrides_and_unknown_extensions_are_not_ignored():
    connection = SimpleNamespace(
        execute=lambda sql, params=None: Result(
            ""
            if "current_setting" in str(sql)
            else [
                {
                    "database_oid": 1,
                    "role_oid": 2,
                    "setting": "session_preload_libraries=pg_cron",
                }
            ]
        )
    )
    with pytest.raises(SequenceProtectionError, match="scheduler"):
        fence.worker_baseline(
            connection, database_oid=1, operator_role="operator", aurora=False
        )
    connection.execute = lambda *args: Result(
        [{"extname": "plpgsql", "extversion": "1"}]
    )
    assert fence.target_extensions(connection)[0]["extname"] == "plpgsql"
    connection.execute = lambda *args: Result(
        [{"extname": "unknown-code", "extversion": "1"}]
    )
    with pytest.raises(SequenceProtectionError, match="extension"):
        fence.target_extensions(connection)


def test_inherited_operator_membership_is_rejected_even_without_current_connect():
    provider = {
        "provider_role": None,
        "roles": [role("operator", 1), role("customer", 2)],
        "memberships": [{"roleid": 1, "member": 3}, {"roleid": 3, "member": 2}],
    }
    connection = SimpleNamespace(execute=lambda *args: Result(["operator"]))
    with pytest.raises(SequenceProtectionError, match="membership"):
        fence.exclusive_acl(
            connection,
            state={"operator_role": "operator", "database_oid": 1},
            provider=provider,
        )

    provider["memberships"] = []
    fence.exclusive_acl(
        connection,
        state={"operator_role": "operator", "database_oid": 1},
        provider=provider,
    )
    connection.execute = lambda *args: Result(["operator", "customer"])
    with pytest.raises(SequenceProtectionError, match="CONNECT"):
        fence.exclusive_acl(
            connection,
            state={"operator_role": "operator", "database_oid": 1},
            provider=provider,
        )


def test_fence_control_identity_is_exact_and_uses_explicit_server_port():
    from tests.test_sequence_protection import inventory

    target = dict(inventory()["target"], port=55434, server_port=5432)
    state = dict(
        database=target["database"],
        database_oid=12345,
        datallowconn=False,
        owner_oid=20,
        owner_role="operator",
        operator_role="operator",
        current_role="operator",
        control_database="control",
        server_address="127.0.0.1",
        server_port=5432,
        server_version_num=160013,
    )
    connection = SimpleNamespace(
        engine=SimpleNamespace(
            url=make_url("postgresql://operator@localhost:55434/control")
        ),
        execute=lambda *args: Result(state),
    )
    assert fence.control_state(connection, target)["server_port"] == 5432
    for changed in [
        connection.engine.url.set(database=target["database"]),
        connection.engine.url.set(host="other.example.invalid"),
        connection.engine.url.set(port=55435),
        connection.engine.url.set(database=""),
    ]:
        fake = SimpleNamespace(
            engine=SimpleNamespace(url=changed), execute=connection.execute
        )
        with pytest.raises(SequenceProtectionError, match="different control"):
            fence.control_state(fake, target)
    for field, value in [
        ("operator_role", "other"),
        ("current_role", "other"),
        ("server_port", 5433),
        ("server_version_num", 150000),
        ("control_database", "other"),
    ]:
        changed = dict(state, **{field: value})
        connection.execute = lambda *args: Result(changed)
        with pytest.raises(SequenceProtectionError, match="operator/server"):
            fence.control_state(connection, target)
    connection.execute = lambda *args: Result(None)
    with pytest.raises(SequenceProtectionError, match="does not exist"):
        fence.control_state(connection, target)


def test_external_fence_journal_missing_or_corrupt_is_not_empty(tmp_path):
    from pathlib import Path

    with pytest.raises(SequenceProtectionError, match="absolute"):
        fence.read_fence_history(Path("relative"))
    (tmp_path / "000001.json").write_text('{"broken":"not a valid operation receipt"}')
    with pytest.raises(SequenceProtectionError, match="corrupt"):
        fence.read_fence_history(tmp_path)


def test_session_epoch_lock_is_retained_and_other_epoch_or_contender_rejected():
    calls = []
    connection = SimpleNamespace(
        info={}, execute=lambda *args: calls.append(args) or Result(True)
    )
    fence.lock_session(connection, 42)
    fence.lock_session(connection, 42)
    assert len(calls) == 1
    with pytest.raises(SequenceProtectionError, match="different fence"):
        fence.lock_session(connection, 43)
    fence.unlock_session(connection)
    assert connection.info == {}
    fence.unlock_session(connection)
    assert len(calls) == 2
    connection.execute = lambda *args: Result(False)
    with pytest.raises(SequenceProtectionError, match="Another maintenance"):
        fence.lock_session(connection, 42)


def test_original_acl_must_have_exact_owner_and_supported_grantor_chain():
    state = {"owner_role": "operator", "operator_role": "operator", "owner_oid": 1}
    fence._restorable_acl({"entries": []}, state)
    with pytest.raises(SequenceProtectionError, match="database owner"):
        fence._restorable_acl({"entries": []}, {**state, "owner_role": "other"})
    with pytest.raises(SequenceProtectionError, match="grantor"):
        fence._restorable_acl(
            {"entries": [{"privilege_type": "CONNECT", "grantor": 2}]}, state
        )


def test_origin_requires_physical_identity_operator_control_and_backend_start():
    state = {
        "database_oid": 42,
        "server_address": "127.0.0.1",
        "server_port": 5432,
        "operator_role": "operator",
        "control_database": "control",
    }
    origin = {
        "physical_target": {
            key: state[key] for key in ("database_oid", "server_address", "server_port")
        },
        "operator_role": "operator",
        "control_database": "control",
        "connection_backend": {"pid": 7, "backend_start": "2026-01-01T00:00:00"},
    }
    fence._verify_origin(state, origin)
    with pytest.raises(SequenceProtectionError, match="physical"):
        fence._verify_origin({**state, "database_oid": 43}, origin)
    with pytest.raises(SequenceProtectionError, match="operator/control"):
        fence._verify_origin({**state, "control_database": "other"}, origin)
    with pytest.raises(SequenceProtectionError, match="backend_start"):
        fence._verify_origin(state, {**origin, "connection_backend": {"pid": 7}})


def test_malformed_original_reservation_fails_closed():
    target = {"explicit": "target"}
    receipt = SimpleNamespace(
        detail={
            "target": target,
            "floors": [{"name": "sequence", "value": True, "source": "old"}],
        }
    )
    with pytest.raises(SequenceProtectionError, match="Malformed"):
        fence.retained_floors([receipt], target)
