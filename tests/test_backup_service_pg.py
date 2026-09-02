"""Backup service against a real PostgreSQL.

Uses the ``pg_instance`` session fixture -- an ephemeral cluster created with
``initdb`` under pytest's tmp dir and torn down afterwards. Auto-skips when
PostgreSQL binaries are absent.

These are the tests that prove the capture is real: that a dump contains every
table rather than a hardcoded five, that plan and verify never mutate, and that
a corrupted artifact is rejected.
"""

from __future__ import annotations

import json
import shutil

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.backup import service, verify
from daylily_tapdb.backup.manifest import (
    BACKUP_CLASS_FULL,
    BACKUP_CLASS_PROVIDER_SNAPSHOT,
    BACKUP_CLASS_TEMPLATE_PACK,
    PROVENANCE_OPERATOR,
    canonical_bytes,
    sha256_hex,
    sign_manifest,
)
from daylily_tapdb.backup.receipts import read_receipts, verify_receipt_chain
from daylily_tapdb.backup.storage import MANIFEST_CHECKSUM_KEY, MANIFEST_KEY
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db_config import get_backup_settings, get_db_config

runner = CliRunner()

pytestmark = pytest.mark.skipif(
    not shutil.which("pg_dump") or not shutil.which("pg_restore"),
    reason="pg_dump/pg_restore not on PATH",
)


@pytest.fixture(autouse=True)
def _context(pg_instance, monkeypatch):
    monkeypatch.setenv("HOME", str(pg_instance["base"]))
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    monkeypatch.setattr(cli_mod, "PID_FILE", pg_instance["base"] / "ui.pid")
    monkeypatch.setattr(cli_mod, "LOG_FILE", pg_instance["base"] / "ui.log")
    yield
    clear_cli_context()


@pytest.fixture(scope="module")
def _schema_applied(pg_instance):
    """Apply the TAPDB schema and seed core templates once for this module.

    Seeding matters: without it ``generic_template`` is empty, and a
    template-pack export test would pass vacuously against a pack containing
    no templates at all.
    """
    config = str(pg_instance["config_path"])
    applied = runner.invoke(app, ["--config", config, "db", "schema", "apply"])
    assert applied.exit_code == 0, applied.output

    seeded = runner.invoke(
        app, ["--config", config, "db", "data", "seed", "--skip-existing"]
    )
    assert seeded.exit_code == 0, seeded.output
    return True


@pytest.fixture
def env(pg_instance, _schema_applied, tmp_path):
    """Resolved cfg + settings pointing at an isolated storage root."""
    cfg = get_db_config()
    settings = dict(get_backup_settings())
    settings["config_dir"] = str(tmp_path)
    settings["storage_uri"] = f"file://{tmp_path / 'store'}"
    return cfg, settings


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def test_plan_reports_a_reachable_target(env):
    cfg, settings = env

    plan = service.plan_backup(cfg, settings)

    assert plan.ok, [c.to_payload() for c in plan.blocking]
    assert plan.target_label.endswith(f"@{cfg['database']}")
    assert plan.would_capture["table_count"] >= 9


def test_plan_never_writes_anything(env, tmp_path):
    cfg, settings = env

    service.plan_backup(cfg, settings)

    # Read-only means read-only: no storage tree, no receipts.
    assert not (tmp_path / "store").exists()
    assert read_receipts(service.receipts_directory(settings)) == []


def test_plan_discloses_that_full_capture_is_physically_complete(env):
    cfg, settings = env

    scope = service.plan_backup(cfg, settings).would_capture["data_scope"]

    assert scope["mode"] == "physical_schema"
    assert scope["physical_schema_complete"] is True
    assert scope["restore_mode"] == "isolated_or_in_place"
    assert all(
        item["key"] != "rows_outside_rls_scope"
        for item in service.plan_backup(cfg, settings).would_capture["state_inventory"]
    )


def test_plan_enumerates_every_table_not_a_fixed_list(env):
    cfg, settings = env

    tables = service.plan_backup(cfg, settings).would_capture["tables"]

    # The legacy command captured five. Enumeration must find all nine.
    for table in (
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
        "tapdb_identity_prefix_config",
        "outbox_event",
        "outbox_event_attempt",
        "inbox_message",
        "_tapdb_migrations",
    ):
        assert table in tables


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


def test_create_produces_a_verifiable_backup(env):
    cfg, settings = env

    result = service.create_backup(cfg, settings)

    assert result.manifest is not None
    assert result.verify is not None and result.verify.ok
    assert result.receipt_id
    manifest = result.manifest
    assert manifest.backup_class == BACKUP_CLASS_FULL
    assert manifest.target_identity["schema_name"] == cfg["schema_name"]


def test_manifest_records_every_table_and_sequence(env):
    cfg, settings = env

    manifest = service.create_backup(cfg, settings).manifest

    assert len(manifest.row_counts) >= 9
    assert "_tapdb_migrations" in manifest.row_counts
    sequence_names = {seq.name for seq in manifest.sequences}
    # All three sequence classes: static library, per-prefix, and IDENTITY.
    assert "wx_instance_seq" in sequence_names
    assert any(name.endswith("_uid_seq") for name in sequence_names)


def test_archive_proves_its_own_schema_scope(env):
    cfg, settings = env

    manifest = service.create_backup(cfg, settings).manifest

    assert manifest.content_inventory["schema_names_seen"] == [cfg["schema_name"]]
    counts = manifest.content_inventory["counts_by_kind"]
    assert counts["TABLE"] >= 10
    assert counts["FUNCTION"] >= 20
    assert counts["TRIGGER"] >= 16


def test_manifest_carries_no_secrets(env):
    cfg, settings = env

    payload = service.create_backup(cfg, settings).manifest.to_payload()
    blob = str(payload).lower()

    assert "password" not in blob
    assert str(cfg.get("password") or "no-password-set") not in str(payload)


def test_manifest_declares_the_complete_operator_data_scope(env):
    cfg, settings = env

    scope = service.create_backup(cfg, settings).manifest.target_identity["data_scope"]

    assert scope == {
        "mode": "physical_schema",
        "tenant_id": None,
        "row_security": "bypassed",
        "physical_schema_complete": True,
        "restore_mode": "isolated_or_in_place",
    }


def test_full_backup_and_isolated_restore_include_rows_from_multiple_tenants(env):
    """The signed completeness claim is backed by archive and restore evidence."""
    cfg, settings = env
    schema = str(cfg["schema_name"])
    tenant_a = "00000000-0000-4000-8000-000000000101"
    tenant_b = "00000000-0000-4000-8000-000000000102"
    selected: list[tuple[int, str | None]] = []
    restored_database: str | None = None

    with service.open_session(
        cfg, app_username="pytest_multi_tenant_setup", connection_role="operator"
    ) as conn:
        with conn.session_scope(commit=True) as session:
            rows = session.execute(
                text(
                    f'SELECT uid, tenant_id::text FROM "{schema}".generic_template '
                    "ORDER BY uid LIMIT 2"
                )
            ).all()
            assert len(rows) == 2
            selected = [(int(row[0]), row[1]) for row in rows]
            for (uid, _original), tenant_id in zip(
                selected, (tenant_a, tenant_b), strict=True
            ):
                session.execute(
                    text(
                        f'UPDATE "{schema}".generic_template '
                        "SET tenant_id = CAST(:tenant_id AS uuid) WHERE uid = :uid"
                    ),
                    {"tenant_id": tenant_id, "uid": uid},
                )

    try:
        created = service.create_backup(cfg, settings)
        operator_count = created.manifest.row_counts["generic_template"]

        with service.open_session(
            cfg, app_username="pytest_runtime_count", connection_role="runtime"
        ) as conn:
            with conn.session_scope(commit=False) as session:
                runtime_count = int(
                    session.execute(
                        text(f'SELECT count(*) FROM "{schema}".generic_template')
                    ).scalar_one()
                )

        assert operator_count >= runtime_count + 2

        restored = verify.restore_backup(
            cfg,
            settings,
            backup_id=created.backup_id,
            options=verify.RestoreOptions(mode=verify.MODE_ISOLATED),
        )
        restored_database = restored.target_database
        restored_cfg = {
            **cfg,
            "database": restored.target_database,
            "schema_name": restored.target_schema,
        }
        with service.open_session(
            restored_cfg,
            app_username="pytest_multi_tenant_verify",
            connection_role="operator",
        ) as conn:
            with conn.session_scope(commit=False) as session:
                tenant_ids = set(
                    session.execute(
                        text(
                            f'SELECT tenant_id::text FROM "{restored.target_schema}".'
                            "generic_template WHERE tenant_id IS NOT NULL"
                        )
                    ).scalars()
                )
        assert {tenant_a, tenant_b}.issubset(tenant_ids)
    finally:
        if restored_database:
            verify._drop_database(cfg, restored_database)
        with service.open_session(
            cfg,
            app_username="pytest_multi_tenant_cleanup",
            connection_role="operator",
        ) as conn:
            with conn.session_scope(commit=True) as session:
                for uid, original in selected:
                    session.execute(
                        text(
                            f'UPDATE "{schema}".generic_template '
                            "SET tenant_id = CAST(:tenant_id AS uuid) WHERE uid = :uid"
                        ),
                        {"tenant_id": original, "uid": uid},
                    )


def test_full_target_identity_never_claims_a_tenant_filtered_scope(env):
    cfg, _settings = env
    tenant_id = "00000000-0000-4000-8000-000000000001"
    tenant_cfg = dict(cfg, tenant_id=tenant_id)

    scope = service._target_identity(tenant_cfg)["data_scope"]

    assert scope["mode"] == "physical_schema"
    assert scope["tenant_id"] is None
    assert scope["physical_schema_complete"] is True


def test_backup_class_role_and_signed_scope_contracts_are_exact(env):
    cfg, _settings = env
    tenant_cfg = {
        **cfg,
        "tenant_id": "00000000-0000-4000-8000-000000000201",
    }

    assert service._connection_role_for_backup_class(BACKUP_CLASS_FULL) == "operator"
    assert (
        service._connection_role_for_backup_class(BACKUP_CLASS_TEMPLATE_PACK)
        == "runtime"
    )
    assert (
        service._connection_role_for_backup_class(BACKUP_CLASS_PROVIDER_SNAPSHOT)
        is None
    )
    assert service._target_identity(
        tenant_cfg, backup_class=BACKUP_CLASS_TEMPLATE_PACK
    )["data_scope"] == {
        "mode": "tenant_and_global",
        "tenant_id": tenant_cfg["tenant_id"],
        "row_security": "enforced",
        "physical_schema_complete": False,
        "restore_mode": "not_applicable",
    }
    assert service._target_identity(cfg, backup_class=BACKUP_CLASS_PROVIDER_SNAPSHOT)[
        "data_scope"
    ] == {
        "mode": "provider_cluster_snapshot",
        "tenant_id": None,
        "row_security": "not_applicable",
        "physical_schema_complete": False,
        "restore_mode": "provider_cutover",
    }


def test_full_backup_operator_config_fails_closed_without_a_distinct_identity(env):
    cfg, _settings = env

    with pytest.raises(RuntimeError, match="require target.operator credentials"):
        service.connection_config_for_role(
            {**cfg, "operator_configured": False}, "operator"
        )
    with pytest.raises(RuntimeError, match="non-empty and distinct"):
        service.connection_config_for_role(
            {**cfg, "operator_user": cfg["user"]}, "operator"
        )

    selected = service.connection_config_for_role(cfg, "operator")
    assert selected["user"] == cfg["operator_user"]
    assert selected["user"] != cfg["user"]
    assert selected["tenant_id"] is None
    assert selected["allow_global_claims"] is True


def test_template_pack_uses_runtime_rls_scope_without_operator_credentials(
    env, tmp_path
):
    cfg, settings = env
    schema = str(cfg["schema_name"])
    tenant_id = "00000000-0000-4000-8000-000000000202"
    selected: list[tuple[int, str | None]] = []

    with service.open_session(
        cfg, app_username="pytest_template_scope_setup", connection_role="operator"
    ) as conn:
        with conn.session_scope(commit=True) as session:
            rows = session.execute(
                text(
                    f'SELECT uid, tenant_id::text FROM "{schema}".generic_template '
                    "ORDER BY uid LIMIT 2"
                )
            ).all()
            assert len(rows) == 2
            selected = [(int(row[0]), row[1]) for row in rows]
            session.execute(
                text(
                    f'UPDATE "{schema}".generic_template '
                    "SET tenant_id = CAST(:tenant_id AS uuid) "
                    "WHERE uid IN (:first_uid, :second_uid)"
                ),
                {
                    "tenant_id": tenant_id,
                    "first_uid": selected[0][0],
                    "second_uid": selected[1][0],
                },
            )

    try:
        runtime_cfg = {**cfg, "operator_configured": False, "operator_user": ""}
        with service.open_session(
            runtime_cfg,
            app_username="pytest_template_scope_runtime_count",
            connection_role="runtime",
        ) as conn:
            with conn.session_scope(commit=False) as session:
                runtime_visible = int(
                    session.execute(
                        text(f'SELECT count(*) FROM "{schema}".generic_template')
                    ).scalar_one()
                )
        with service.open_session(
            cfg,
            app_username="pytest_template_scope_operator_count",
            connection_role="operator",
        ) as conn:
            with conn.session_scope(commit=False) as session:
                operator_visible = int(
                    session.execute(
                        text(f'SELECT count(*) FROM "{schema}".generic_template')
                    ).scalar_one()
                )
        assert operator_visible == runtime_visible + 2

        plan = service.plan_backup(
            runtime_cfg, settings, backup_class=BACKUP_CLASS_TEMPLATE_PACK
        )
        assert plan.ok, [check.to_payload() for check in plan.blocking]
        assert plan.would_capture["data_scope"]["mode"] == "global_only"
        assert plan.would_capture["state_inventory"] == []
        assert {item["key"] for item in plan.would_capture["excluded_state"]} >= {
            "non_template_database_state",
            "rows_outside_rls_scope",
        }

        result = service.create_backup(
            runtime_cfg, settings, backup_class=BACKUP_CLASS_TEMPLATE_PACK
        )
        artifact = tmp_path / "store" / result.storage_prefix / "template-pack.json"
        pack = json.loads(artifact.read_text(encoding="utf-8"))
        scope = result.manifest.target_identity["data_scope"]

        assert scope["row_security"] == "enforced"
        assert scope["physical_schema_complete"] is False
        assert result.manifest.schema_drift == {
            "status": "not_applicable",
            "has_drift": None,
        }
        assert result.manifest.row_counts == {
            "generic_template": len(pack["templates"])
        }
        assert len(pack["templates"]) == runtime_visible
        assert result.manifest.sequences == []
        assert result.manifest.content_inventory["visibility_scope"] == scope
    finally:
        with service.open_session(
            cfg,
            app_username="pytest_template_scope_cleanup",
            connection_role="operator",
        ) as conn:
            with conn.session_scope(commit=True) as session:
                for uid, original in selected:
                    session.execute(
                        text(
                            f'UPDATE "{schema}".generic_template '
                            "SET tenant_id = CAST(:tenant_id AS uuid) WHERE uid = :uid"
                        ),
                        {"tenant_id": original, "uid": uid},
                    )


def test_create_writes_manifest_checksum_and_artifact(env, tmp_path):
    cfg, settings = env

    result = service.create_backup(cfg, settings)

    root = tmp_path / "store" / result.storage_prefix
    assert (root / "manifest.json").is_file()
    assert (root / "manifest.sha256").is_file()
    assert (root / "tapdb.dump").is_file()


def test_create_emits_a_chained_receipt(env):
    cfg, settings = env

    service.create_backup(cfg, settings)
    service.create_backup(cfg, settings)

    stored = read_receipts(service.receipts_directory(settings))
    creates = [r for r in stored if r.operation == "backup_create"]

    assert len(creates) == 2
    assert verify_receipt_chain(stored).ok


def test_dry_run_captures_nothing(env, tmp_path):
    cfg, settings = env

    result = service.create_backup(cfg, settings, dry_run=True)

    assert result.dry_run
    assert result.manifest is None
    assert not (tmp_path / "store").exists()


def test_dump_is_actually_snapshot_consistent(env):
    cfg, settings = env

    consistency = service.create_backup(cfg, settings).manifest.consistency

    # A plain local cluster supports pg_export_snapshot, so the strong
    # guarantee must actually be obtained here. Asserting only "one of the two
    # modes" would let a silent regression to best_effort pass unnoticed --
    # and best_effort means manifest counts may not match the dump.
    assert consistency["mode"] == "snapshot", consistency
    assert consistency["snapshot"], "snapshot name was not recorded"


def test_backend_address_is_recorded_for_snapshot_pinning(env):
    cfg, settings = env

    consistency = service.create_backup(cfg, settings).manifest.consistency

    # Aurora reader/writer endpoints are different hosts; pg_dump --snapshot is
    # only valid against the backend that exported it, so which one answered
    # has to be on the record.
    assert "backend" in consistency


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


def test_verify_passes_on_a_good_backup(env):
    cfg, settings = env
    created = service.create_backup(cfg, settings)

    report = service.verify_backup(cfg, settings, backup_id=created.backup_id)

    assert report.ok, [c.to_payload() for c in report.checks if c.failed]
    assert any(c.id == "archive.deep_read" for c in report.checks)


def test_verify_detects_a_corrupted_artifact(env, tmp_path):
    cfg, settings = env
    created = service.create_backup(cfg, settings)
    artifact = tmp_path / "store" / created.storage_prefix / "tapdb.dump"

    raw = bytearray(artifact.read_bytes())
    midpoint = len(raw) // 2
    for offset in range(midpoint, min(midpoint + 512, len(raw))):
        raw[offset] ^= 0xFF
    artifact.write_bytes(bytes(raw))

    report = service.verify_backup(cfg, settings, backup_id=created.backup_id)

    assert not report.ok
    assert any(c.id.startswith("asset.checksum") and c.failed for c in report.checks)


def test_verify_detects_a_falsified_manifest(env, tmp_path):
    cfg, settings = env
    created = service.create_backup(cfg, settings)
    manifest_file = tmp_path / "store" / created.storage_prefix / "manifest.json"

    payload = manifest_file.read_text().replace('"status": "complete"', '"status": "x"')
    manifest_file.write_text(payload)

    report = service.verify_backup(cfg, settings, backup_id=created.backup_id)

    assert not report.ok
    assert any(c.id == "manifest.checksum" and c.failed for c in report.checks)


def test_verify_does_not_mutate_the_database(env):
    cfg, settings = env
    created = service.create_backup(cfg, settings)
    before = service.create_backup(cfg, settings).manifest.row_counts

    service.verify_backup(cfg, settings, backup_id=created.backup_id)
    after = service.create_backup(cfg, settings).manifest.row_counts

    assert before == after


def test_verify_of_an_unknown_backup_raises(env):
    cfg, settings = env

    from daylily_tapdb.backup.errors import BackupNotFoundError

    with pytest.raises(BackupNotFoundError):
        service.verify_backup(cfg, settings, backup_id="full-does-not-exist")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_is_empty_before_any_backup(env):
    cfg, settings = env

    assert service.list_backups(cfg, settings).entries == []


def test_list_finds_backups_by_scanning_for_manifests(env):
    cfg, settings = env
    first = service.create_backup(cfg, settings)
    second = service.create_backup(cfg, settings)

    listing = service.list_backups(cfg, settings)
    ids = [entry.backup_id for entry in listing.entries]

    assert {first.backup_id, second.backup_id} <= set(ids)
    assert listing.to_payload()["count"] == len(listing.entries)


def test_list_filters_by_class(env):
    cfg, settings = env
    service.create_backup(cfg, settings, backup_class=BACKUP_CLASS_FULL)
    service.create_backup(cfg, settings, backup_class=BACKUP_CLASS_TEMPLATE_PACK)

    full = service.list_backups(cfg, settings, backup_class=BACKUP_CLASS_FULL)
    packs = service.list_backups(cfg, settings, backup_class=BACKUP_CLASS_TEMPLATE_PACK)

    assert all(e.backup_class == BACKUP_CLASS_FULL for e in full.entries)
    assert all(e.backup_class == BACKUP_CLASS_TEMPLATE_PACK for e in packs.entries)
    assert packs.entries


# ---------------------------------------------------------------------------
# template pack
# ---------------------------------------------------------------------------


def test_template_pack_export_round_trips(env, tmp_path):
    cfg, settings = env

    result = service.create_backup(
        cfg, settings, backup_class=BACKUP_CLASS_TEMPLATE_PACK
    )
    artifact = tmp_path / "store" / result.storage_prefix / "template-pack.json"
    pack = json.loads(artifact.read_text())

    # Non-empty is load-bearing: iterating an empty list asserts nothing, and
    # an export that silently produced no templates would look like a pass.
    assert pack["templates"], "export produced no templates"
    for entry in pack["templates"]:
        for key in (
            "name",
            "polymorphic_discriminator",
            "category",
            "type",
            "subtype",
            "version",
            "instance_prefix",
        ):
            assert entry.get(key), f"missing {key} in {entry}"
        # Destination-owned identity must never ride along.
        for forbidden in ("uid", "euid", "euid_seq", "created_dt"):
            assert forbidden not in entry


def test_template_pack_summary_lands_in_the_manifest(env):
    cfg, settings = env

    result = service.create_backup(
        cfg, settings, backup_class=BACKUP_CLASS_TEMPLATE_PACK
    )

    inventory = result.manifest.content_inventory
    assert inventory["template_count"] > 0
    assert inventory["categories"]
    assert inventory["instance_prefixes"]


def test_provider_snapshot_class_produces_a_receipt_backup(env, tmp_path, monkeypatch):
    """Backup class (c) end to end, with AWS faked out.

    The RDS call is stubbed, but everything around it is real: the manifest,
    the artifact, storage layout, and the post-write verification.
    """
    from daylily_tapdb.backup import snapshots

    cfg, settings = env
    cfg = {**cfg, "operator_configured": False, "operator_user": ""}

    class _FakeRds:
        def create_db_cluster_snapshot(self, **kwargs):
            return {
                "DBClusterSnapshot": {
                    "DBClusterSnapshotIdentifier": kwargs[
                        "DBClusterSnapshotIdentifier"
                    ],
                    "DBClusterIdentifier": kwargs["DBClusterIdentifier"],
                    "Status": "creating",
                    "EngineVersion": "16.4",
                    "StorageEncrypted": True,
                    "KmsKeyId": "arn:aws:kms:us-west-2:1:key/abc",
                }
            }

    # The target here is a local cluster, so the Aurora guard is bypassed; the
    # guard itself has its own tests.
    monkeypatch.setattr(snapshots, "require_enabled", lambda cfg, settings: None)
    monkeypatch.setattr(snapshots, "_rds_client", lambda region: _FakeRds())
    monkeypatch.setattr(
        service,
        "open_session",
        lambda *_args, **_kwargs: pytest.fail(
            "provider snapshots must not open a PostgreSQL session"
        ),
    )
    settings = {
        **settings,
        "provider_snapshots_enabled": True,
        "provider_snapshots_cluster_identifier": "tapdb-test-cluster",
    }

    plan = service.plan_backup(
        cfg, settings, backup_class=BACKUP_CLASS_PROVIDER_SNAPSHOT
    )
    assert plan.ok, [check.to_payload() for check in plan.blocking]
    assert plan.would_capture["state_inventory"] == []
    assert plan.would_capture["provider_snapshot"]["inventory"] == (
        "opaque_until_provider_restore"
    )

    result = service.create_backup(
        cfg, settings, backup_class=BACKUP_CLASS_PROVIDER_SNAPSHOT
    )

    assert result.verify.ok
    assert result.manifest.backup_class == BACKUP_CLASS_PROVIDER_SNAPSHOT
    assert result.manifest.target_identity["data_scope"] == {
        "mode": "provider_cluster_snapshot",
        "tenant_id": None,
        "row_security": "not_applicable",
        "physical_schema_complete": False,
        "restore_mode": "provider_cutover",
    }
    assert result.manifest.row_counts == {}
    assert result.manifest.sequences == []
    assert result.manifest.schema_drift == {
        "status": "not_applicable",
        "has_drift": None,
    }
    inventory = result.manifest.content_inventory
    assert inventory["cluster_identifier"] == "tapdb-test-cluster"
    assert inventory["encrypted"] is True

    artifact = tmp_path / "store" / result.storage_prefix / "snapshot-receipt.json"
    receipt = json.loads(artifact.read_text())
    assert receipt["engine_version"] == "16.4"
    assert receipt["kms_key_id"].startswith("arn:aws:kms:")


def test_a_provider_snapshot_backup_is_listed_and_filterable(env, monkeypatch):
    from daylily_tapdb.backup import snapshots

    cfg, settings = env

    class _FakeRds:
        def create_db_cluster_snapshot(self, **kwargs):
            return {"DBClusterSnapshot": {"DBClusterIdentifier": "c"}}

    monkeypatch.setattr(snapshots, "require_enabled", lambda cfg, settings: None)
    monkeypatch.setattr(snapshots, "_rds_client", lambda region: _FakeRds())
    settings = {**settings, "provider_snapshots_cluster_identifier": "c"}

    created = service.create_backup(
        cfg, settings, backup_class=BACKUP_CLASS_PROVIDER_SNAPSHOT
    )
    listing = service.list_backups(
        cfg, settings, backup_class=BACKUP_CLASS_PROVIDER_SNAPSHOT
    )

    assert created.backup_id in [e.backup_id for e in listing.entries]
    assert all(
        e.backup_class == BACKUP_CLASS_PROVIDER_SNAPSHOT for e in listing.entries
    )


def test_unknown_backup_class_is_rejected(env):
    cfg, settings = env

    with pytest.raises(ValueError, match="Unknown backup class"):
        service.create_backup(cfg, settings, backup_class="nonsense")


def test_a_routine_backup_records_operator_provenance(env):
    """An ordinary backup says so positively; it never stores an empty block.

    Absence of ``provenance`` has exactly one meaning -- the manifest was
    written before the field existed. If routine backups also stored ``{}``, a
    reader could not tell a new ordinary backup from a legacy one, so the
    "unknown provenance" case would never shrink as the store turns over. It
    would accumulate, and every rule built on it would grow more conservative
    forever rather than less.
    """
    cfg, settings = env
    result = service.create_backup(cfg, settings)

    assert result.manifest.provenance == {"created_by": PROVENANCE_OPERATOR}

    # And it survives the round-trip through storage, not just in memory.
    stored = service._load_manifest(
        service.storage_for(settings),
        service.find_backup_prefix(
            cfg, service.storage_for(settings), result.backup_id
        ),
    )
    assert stored.provenance == {"created_by": PROVENANCE_OPERATOR}


def test_provenance_is_signed_on_a_real_backup(env):
    """The ordering guarantee, asserted end to end rather than in a unit test."""
    cfg, settings = env
    result = service.create_backup(cfg, settings)

    report = service.verify_backup(
        cfg, settings, backup_id=result.backup_id, level=service.VERIFY_DEEP
    )

    assert report.ok, [c.to_payload() for c in report.checks if c.failed]
    signature_checks = [c for c in report.checks if "signature" in c.id]
    assert signature_checks, [c.id for c in report.checks]
    assert all(not c.failed for c in signature_checks)


def test_a_manifest_written_before_provenance_existed_still_verifies(env):
    """Adding a manifest field must not invalidate backups already on disk.

    Today this is belt-and-braces: no released TapDB any service pins contains
    the backup subsystem, so no pre-provenance manifest exists anywhere. The
    guarantee matters for the *next* field, and it only holds because
    verification reads the raw stored bytes -- ``sha256_hex(manifest_bytes)``
    for the checksum and ``json.loads(manifest_bytes)`` for the signature.

    Route either one through ``manifest.to_payload()`` instead and every
    manifest predating the newest field fails verification, which fails restore
    preflight, which turns an additive change into unrestorable backups.
    """
    import json

    cfg, settings = env
    result = service.create_backup(cfg, settings)

    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, result.backup_id)

    # Rewrite the stored manifest as an older TapDB would have written it:
    # no provenance key at all, and signed over that payload.
    payload = json.loads(storage.get_bytes(f"{prefix}/{MANIFEST_KEY}").decode("utf-8"))
    del payload["provenance"]
    payload["signature"] = sign_manifest(payload, mode="none")
    legacy_bytes = canonical_bytes(payload)
    storage.put_bytes(f"{prefix}/{MANIFEST_KEY}", legacy_bytes)
    storage.put_bytes(
        f"{prefix}/{MANIFEST_CHECKSUM_KEY}",
        sha256_hex(legacy_bytes).encode("utf-8"),
    )

    report = service.verify_backup(
        cfg, settings, backup_id=result.backup_id, level=service.VERIFY_DEEP
    )

    failed = [c.to_payload() for c in report.checks if c.failed]
    assert report.ok, failed

    # Specifically the two checks that a round-trip would have broken.
    by_id = {c.id: c for c in report.checks}
    assert by_id["manifest.checksum"].status == service.STATUS_PASS
    assert by_id["manifest.signature"].status != service.STATUS_FAIL

    # And it loads with an empty block, which must read as "unknown", never as
    # a routine operator backup.
    loaded = service._load_manifest(storage, prefix)
    assert loaded.provenance == {}
