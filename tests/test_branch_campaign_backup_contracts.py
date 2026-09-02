from __future__ import annotations

import socket
from types import SimpleNamespace

import pytest

from daylily_tapdb.backup import introspect, service, verify
from daylily_tapdb.backup.errors import BackupNotFoundError, BackupVerificationError
from daylily_tapdb.backup.manifest import AssetRef, BackupManifest, SequenceState
from daylily_tapdb.security_context import TapdbTransactionContext


def _manifest(**changes):
    values = {
        "backup_id": "full-branch-campaign",
        "backup_class": "full",
        "target_identity": {
            "client_id": "client",
            "database_name": "database",
            "schema_name": "tapdb_unit",
            "domain_code": "A",
            "owner_repo_name": "owner-a",
            "target_label": "client/database/tapdb_unit@local",
            "data_scope": {
                "mode": "physical_schema",
                "tenant_id": None,
                "row_security": "bypassed",
                "physical_schema_complete": True,
                "restore_mode": "isolated_or_in_place",
            },
        },
        "postgres": {"server_version": "17.2"},
        "migrations": {"asset_checksums": []},
        "row_counts": {"generic_instance": 1},
        "sequences": [SequenceState(name="instance_seq", last_value=1, is_called=True)],
        "content_inventory": {
            "counts_by_kind": {},
            "schema_names_seen": ["tapdb_unit"],
            "dumped_by_version": "17.2",
        },
        "governance": {},
        "included_assets": [AssetRef(name="tapdb.dump", bytes=8, sha256="ab" * 32)],
    }
    values.update(changes)
    return BackupManifest(**values)


class _Rows:
    def __init__(self, rows=(), scalar_value=None, first_value=None):
        self.rows = list(rows)
        self.scalar_value = scalar_value
        self.first_value = first_value

    def __iter__(self):
        return iter(self.rows)

    def scalar(self):
        return self.scalar_value

    def first(self):
        return self.first_value


class _SequenceSession:
    def __init__(self, first, second=None):
        self.results = [first] if second is None else [first, second]

    def execute(self, *_args, **_kwargs):
        return self.results.pop(0)


def test_branch_campaign_introspection_empty_catalogues_and_missing_migrations():
    assert introspect.capture_sequences(_SequenceSession(_Rows()), "unit") == []
    assert introspect.capture_migrations(_SequenceSession(_Rows()), "unit") == []


class _Transaction:
    def __init__(self):
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


class _SnapshotConnection:
    def __init__(self):
        self.transactions = []
        self.closed = False

    def execution_options(self, **_kwargs):
        return self

    def begin(self):
        transaction = _Transaction()
        self.transactions.append(transaction)
        return transaction

    def execute(self, *_args):
        raise RuntimeError("snapshot unavailable")

    def close(self):
        self.closed = True


def test_branch_campaign_snapshot_export_degrades_and_reinstalls_context():
    connection = _SnapshotConnection()
    manager = SimpleNamespace(
        engine=SimpleNamespace(connect=lambda: connection),
        install_transaction_context=lambda conn: setattr(
            manager, "installs", getattr(manager, "installs", 0) + 1
        ),
    )
    with introspect.snapshot_transaction(manager) as (active, snapshot):
        assert active is connection
        assert snapshot is None
    assert manager.installs == 2
    assert [item.rollbacks for item in connection.transactions] == [1, 1]
    assert connection.closed is True


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (RuntimeError("no permission"), {"address": None, "port": None}),
        (_Rows(first_value=None), {"address": None, "port": None}),
        (_Rows(first_value=("not-an-ip", 5432)), {"address": None, "port": 5432}),
    ],
)
def test_branch_campaign_backend_address_handles_unavailable_or_invalid_values(
    result, expected
):
    class _Session:
        def execute(self, *_args):
            if isinstance(result, Exception):
                raise result
            return result

    assert introspect.resolved_backend_address(_Session()) == expected


def test_branch_campaign_governance_and_capacity_missing_inputs(monkeypatch, tmp_path):
    block = service._governance_block(
        {"domain_code": "A", "owner_repo_name": "owner-a"}
    )
    assert block["prefix_ownership_registry_path"] is None
    assert block["domain_registry_path"] is None

    remote = SimpleNamespace(describe=lambda: {"backend": "s3"})
    assert (
        service._free_space_check(remote, {"config_dir": str(tmp_path)}).status
        == "skip"
    )
    local = SimpleNamespace(describe=lambda: {"backend": "local"})
    monkeypatch.setattr(
        service.shutil,
        "disk_usage",
        lambda _path: (_ for _ in ()).throw(OSError("disk unavailable")),
    )
    assert (
        service._free_space_check(local, {"config_dir": str(tmp_path)}).status == "warn"
    )


def test_branch_campaign_client_address_resolution_branches(monkeypatch):
    assert service._client_resolved_address("") is None
    monkeypatch.setattr(
        socket, "getaddrinfo", lambda *_args: (_ for _ in ()).throw(OSError("dns"))
    )
    assert service._client_resolved_address("db.example") is None
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args: [
            (socket.AF_INET6, 0, 0, "", ("2001:db8::1", 0, 0, 0)),
            (socket.AF_INET, 0, 0, "", ("192.0.2.10", 0)),
        ],
    )
    assert service._client_resolved_address("db.example") == "192.0.2.10"
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args: [(socket.AF_INET6, 0, 0, "", ("2001:db8::2", 0, 0, 0))],
    )
    assert service._client_resolved_address("db.example") == "2001:db8::2"


def _command(*, ok=True, output="", stdout="", returncode=0):
    return SimpleNamespace(ok=ok, output=output, stdout=stdout, returncode=returncode)


def test_branch_campaign_dump_reports_command_and_listing_failures(
    monkeypatch, tmp_path
):
    cfg = {"host": "db.example", "engine_type": "aurora"}
    monkeypatch.setattr(service.engine, "client_env", lambda _cfg: {})
    monkeypatch.setattr(
        service.engine, "build_pg_dump_command", lambda *_a, **_k: ["pg_dump"]
    )
    monkeypatch.setattr(
        service.engine,
        "build_pg_restore_list_command",
        lambda *_a: ["pg_restore", "-l"],
    )
    monkeypatch.setattr(service, "_client_resolved_address", lambda _host: "192.0.2.10")
    commands = iter([_command(ok=False, output="dump failed", returncode=2)])
    monkeypatch.setattr(service.engine, "run_command", lambda *_a, **_k: next(commands))
    with pytest.raises(BackupVerificationError, match="pg_dump failed"):
        service._run_dump(
            cfg,
            schema_name="tapdb_unit",
            artifact=tmp_path / "dump",
            snapshot="snapshot-id",
            transaction_context=TapdbTransactionContext(
                config_identity="config",
                schema_name="tapdb_unit",
                domain_code="A",
                owner_repo_name="owner-a",
                tenant_id=None,
                actor="operator",
                allow_global_rows=False,
            ),
        )

    commands = iter([_command(), _command(ok=False, output="listing failed")])
    monkeypatch.setattr(service.engine, "run_command", lambda *_a, **_k: next(commands))
    with pytest.raises(BackupVerificationError, match="table of contents"):
        service._run_dump(
            cfg,
            schema_name="tapdb_unit",
            artifact=tmp_path / "dump",
            snapshot="snapshot-id",
            transaction_context=TapdbTransactionContext(
                config_identity="config",
                schema_name="tapdb_unit",
                domain_code="A",
                owner_repo_name="owner-a",
                tenant_id=None,
                actor="operator",
                allow_global_rows=False,
            ),
        )


def test_branch_campaign_manifest_lookup_and_archive_toc_failures(
    monkeypatch, tmp_path
):
    storage = SimpleNamespace(
        get_bytes=lambda _key: (_ for _ in ()).throw(OSError("missing"))
    )
    with pytest.raises(BackupNotFoundError):
        service._load_manifest(storage, "backups/missing")
    monkeypatch.setattr(service, "storage_for", lambda _settings: object())
    with pytest.raises(ValueError, match="either backup_id or path"):
        service.verify_backup({}, {}, record_receipt=False)

    monkeypatch.setattr(
        service.engine, "build_pg_restore_list_command", lambda *_a: ["pg_restore"]
    )
    monkeypatch.setattr(
        service.engine,
        "run_command",
        lambda *_a, **_k: _command(ok=False, output="bad archive"),
    )
    assert service._toc_only_check(tmp_path / "dump").failed

    inventory = SimpleNamespace(
        schema_names_seen=lambda: ["other_schema"],
        entries=[],
        to_payload=lambda: {},
    )
    monkeypatch.setattr(
        service.engine, "run_command", lambda *_a, **_k: _command(stdout="toc")
    )
    monkeypatch.setattr(service.engine, "parse_toc", lambda _text: inventory)
    assert service._toc_check(tmp_path / "dump", _manifest()).failed


def test_branch_campaign_health_helper_failure_modes(monkeypatch):
    assert service._health_settings_checks(
        {"invalid_fields": ["expected_interval_hours"]}
    )[0].failed

    listing = service.BackupListing(storage={"backend": "s3"})
    raising = SimpleNamespace(
        deletion_capability=lambda: (_ for _ in ()).throw(RuntimeError("denied"))
    )
    assert service._health_storage_safety_checks(raising, listing)[0].status == "warn"
    unknown = SimpleNamespace(
        deletion_capability=lambda: {"reclaims": None, "reason": "access denied"}
    )
    assert service._health_storage_safety_checks(unknown, listing)[0].status == "warn"

    monkeypatch.setattr(
        service,
        "_health_settings_checks",
        lambda _settings: (_ for _ in ()).throw(RuntimeError("bad settings")),
    )
    monkeypatch.setattr(service, "storage_for", lambda _settings: object())
    monkeypatch.setattr(service, "list_backups", lambda *_args: service.BackupListing())
    monkeypatch.setattr(
        service,
        "_health_storage_checks",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("bad store")),
    )
    report = service.health_report(
        {
            "client_id": "client",
            "database_name": "database",
            "schema_name": "unit",
            "engine_type": "local",
        },
        {"config_dir": "/tmp"},
    )
    assert service.SOURCE_SETTINGS in report.unavailable
    assert service.SOURCE_STORAGE in report.unavailable


def test_branch_campaign_restore_client_prefix_and_migration_checks(monkeypatch):
    manifest = _manifest()
    monkeypatch.setattr(verify.engine, "client_version", lambda _name: None)
    assert verify._check_restore_client_version(manifest).failed
    monkeypatch.setattr(verify.engine, "client_version", lambda _name: "unknown")
    monkeypatch.setattr(verify.engine, "parse_version_major", lambda _value: None)
    assert verify._check_restore_client_version(manifest).status == "pass"

    assert verify._check_prefix_claimability({}, manifest).status == "skip"
    prefixed = _manifest(representative_objects=[{"euid_prefix": "ABC"}])
    assert verify._check_prefix_claimability({}, prefixed).status == "warn"

    from daylily_tapdb import schema_inventory

    monkeypatch.setattr(
        schema_inventory,
        "find_schema_root",
        lambda _path: (_ for _ in ()).throw(FileNotFoundError("missing assets")),
    )
    assert (
        verify._check_migration_inventory(manifest, allow_unknown=False).status
        == "warn"
    )


def test_branch_campaign_governance_disk_and_scope_checks(monkeypatch, tmp_path):
    manifest = _manifest()
    assert verify._check_governance({}, manifest).status == "skip"
    remote = SimpleNamespace(describe=lambda: {"backend": "s3"})
    assert (
        verify._check_disk_space(manifest, {"config_dir": str(tmp_path)}, remote).status
        == "skip"
    )
    local = SimpleNamespace(describe=lambda: {"backend": "local"})
    monkeypatch.setattr(
        "shutil.disk_usage",
        lambda _path: (_ for _ in ()).throw(OSError("disk unavailable")),
    )
    assert (
        verify._check_disk_space(manifest, {"config_dir": str(tmp_path)}, local).status
        == "warn"
    )

    missing = _manifest(target_identity={"schema_name": "tapdb_unit"})
    assert verify._check_restore_scope({}, missing, mode=verify.MODE_ISOLATED).failed
    mismatched = _manifest()
    mismatched.target_identity["data_scope"] = {
        **mismatched.target_identity["data_scope"],
        "mode": "tenant_and_global",
        "tenant_id": "tenant-b",
    }
    assert verify._check_restore_scope({}, mismatched, mode=verify.MODE_ISOLATED).failed
    incomplete = _manifest()
    incomplete.target_identity["data_scope"]["physical_schema_complete"] = False
    assert verify._check_restore_scope({}, incomplete, mode=verify.MODE_ISOLATED).failed
    assert verify._check_restore_scope(
        {}, manifest, mode=verify.MODE_IN_PLACE
    ).status == ("pass")
    assert (
        verify._check_restore_scope({}, manifest, mode=verify.MODE_ISOLATED).status
        == "pass"
    )


class _RoleScalars:
    def __init__(self, names):
        self.names = names

    def scalars(self):
        return self.names


def test_branch_campaign_rls_role_checks_cover_all_preflight_states(
    monkeypatch, tmp_path
):
    no_policies = _manifest()
    assert verify._check_rls_roles(object(), no_policies).status == "skip"
    policies = _manifest(content_inventory={"counts_by_kind": {"POLICY": 2}})
    assert verify._check_rls_roles(object(), policies).status == "warn"

    monkeypatch.setattr(
        verify.engine, "build_pg_restore_sql_command", lambda *_a, **_k: ["pg_restore"]
    )
    monkeypatch.setattr(
        verify.engine,
        "run_command",
        lambda *_a, **_k: _command(ok=False, output="render failed"),
    )
    assert (
        verify._check_rls_roles(
            object(), policies, archive_path=tmp_path / "dump"
        ).status
        == "warn"
    )

    monkeypatch.setattr(
        verify.engine, "run_command", lambda *_a, **_k: _command(stdout="policy sql")
    )
    monkeypatch.setattr(verify.engine, "policy_roles", lambda _sql: {"runtime_role"})
    session = SimpleNamespace(execute=lambda *_a: _RoleScalars([]))
    check = verify._check_rls_roles(session, policies, archive_path=tmp_path / "dump")
    assert check.failed
    assert check.data["missing"] == ["runtime_role"]


def test_branch_campaign_target_probe_and_maintenance_failures(monkeypatch, tmp_path):
    cfg = {
        "operator_configured": True,
        "operator_user": "operator",
        "operator_password": "",
        "user": "runtime",
    }
    monkeypatch.setattr(verify, "_database_exists", lambda *_args: True)
    monkeypatch.setattr(verify.engine, "build_psql_command", lambda *_a, **_k: ["psql"])
    monkeypatch.setattr(verify.engine, "client_env", lambda _cfg: {})
    monkeypatch.setattr(
        verify.engine,
        "run_command",
        lambda *_a, **_k: _command(ok=False, output="probe failed"),
    )
    assert (
        verify._check_target_emptiness(
            cfg,
            mode=verify.MODE_ISOLATED,
            target_database="restore_database",
            target_schema="tapdb_unit",
        ).status
        == "warn"
    )
    with pytest.raises(BackupVerificationError, match="Maintenance statement failed"):
        verify._admin_sql(cfg, "SELECT 1")

    wrong_class = _manifest(backup_class="template-pack")
    storage = SimpleNamespace()
    monkeypatch.setattr(service, "storage_for", lambda _settings: storage)
    monkeypatch.setattr(service, "find_backup_prefix", lambda *_args: "backups/item")
    monkeypatch.setattr(service, "_load_manifest", lambda *_args: wrong_class)
    with pytest.raises(BackupVerificationError, match="only full logical"):
        verify._restore_archive({}, {}, backup_id="item", database="restore_database")


def test_branch_campaign_rollback_attempts_rename_after_drop_failure(monkeypatch):
    calls = []

    def _admin(_cfg, sql, **_kwargs):
        calls.append(sql)
        if sql.startswith("DROP SCHEMA"):
            raise RuntimeError("drop failed")

    monkeypatch.setattr(verify, "_admin_sql", _admin)
    verify._rollback_in_place({}, database="database", schema="unit", superseded="old")
    assert len(calls) == 2

    monkeypatch.setattr(
        verify,
        "_admin_sql",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("rename failed")),
    )
    with pytest.raises(BackupVerificationError, match="rollback could not restore"):
        verify._rollback_in_place(
            {}, database="database", schema="unit", superseded="old"
        )


def test_restore_runtime_access_retains_hardened_function_search_path(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        service,
        "connection_config_for_role",
        lambda _cfg, _role: {"user": "tapdb_operator"},
    )
    monkeypatch.setattr(
        verify,
        "_admin_sql",
        lambda _cfg, sql, **kwargs: captured.update(sql=sql, kwargs=kwargs),
    )

    verify._restore_runtime_access(
        {"user": "tapdb_runtime"},
        database="restored_database",
        schema="restored_schema",
    )

    sql = captured["sql"]
    assert "SET search_path TO %I, pg_catalog, pg_temp', fn.nspname, fn.proname" in sql
    assert "SET search_path TO %I', fn.nspname" not in sql
    assert captured["kwargs"] == {"database": "restored_database"}


def test_branch_campaign_restore_step_description_and_confirmation_bypass():
    steps = verify._describe_steps(
        mode=verify.MODE_IN_PLACE,
        target_database="database",
        target_schema="unit",
        keep_superseded=True,
    )
    assert any("keep the superseded schema" in step for step in steps)
    cfg = {"destructive_operations": verify.POLICY_ALLOWED}
    assert verify.confirmation_required(cfg, verify.MODE_IN_PLACE) is False
    verify._require_confirmation(cfg, None)
