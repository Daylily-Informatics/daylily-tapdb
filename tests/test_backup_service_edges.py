"""Edge cases for the backup service against a real PostgreSQL.

Covers the boundaries the happy-path tests do not: a neighbouring schema in the
same database, damaged or missing storage objects, and the non-``full`` backup
class taking a different verification route.
"""

from __future__ import annotations

import shutil

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.backup import service
from daylily_tapdb.backup.errors import BackupNotFoundError
from daylily_tapdb.backup.manifest import BACKUP_CLASS_TEMPLATE_PACK
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
    cfg = get_db_config()
    settings = dict(get_backup_settings())
    settings["config_dir"] = str(tmp_path)
    settings["storage_uri"] = f"file://{tmp_path / 'store'}"
    return cfg, settings


@pytest.fixture
def neighbour_schema(env):
    """Create a second schema holding a table with a colliding-ish name."""
    cfg, _ = env
    with service.open_session(cfg, app_username="pytest") as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(text("CREATE SCHEMA IF NOT EXISTS stranger"))
            session.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS stranger.generic_instance "
                    "(uid bigint primary key, secret text)"
                )
            )
            session.execute(
                text(
                    "INSERT INTO stranger.generic_instance (uid, secret) "
                    "VALUES (1, 'do-not-capture') ON CONFLICT DO NOTHING"
                )
            )
    yield "stranger"
    with service.open_session(cfg, app_username="pytest") as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(text("DROP SCHEMA IF EXISTS stranger CASCADE"))


# ---------------------------------------------------------------------------
# schema isolation
# ---------------------------------------------------------------------------


def test_a_neighbouring_schema_is_not_captured(env, neighbour_schema):
    cfg, settings = env

    manifest = service.create_backup(cfg, settings).manifest

    # A table with the *same name* exists in the other schema holding exactly
    # one row; scoping must be by schema, not by name.
    assert manifest.content_inventory["schema_names_seen"] == [cfg["schema_name"]]
    assert manifest.target_identity["schema_name"] == cfg["schema_name"]
    assert set(manifest.row_counts) == {
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
        "tapdb_identity_prefix_config",
        "outbox_event",
        "outbox_event_attempt",
        "inbox_message",
        "_tapdb_migrations",
    }


def test_the_neighbours_data_is_absent_from_the_artifact(
    env, neighbour_schema, tmp_path
):
    cfg, settings = env

    result = service.create_backup(cfg, settings)
    artifact = tmp_path / "store" / result.storage_prefix / "tapdb.dump"

    # The marker row lives only in the other schema, so it must not appear
    # anywhere in the archive bytes.
    assert b"do-not-capture" not in artifact.read_bytes()


def test_plan_counts_only_the_configured_schema(env, neighbour_schema):
    cfg, settings = env

    tables = service.plan_backup(cfg, settings).would_capture["tables"]

    # Nine TAPDB tables; the neighbour's table must not inflate the list.
    assert len(tables) == 9


# ---------------------------------------------------------------------------
# damaged or missing storage objects
# ---------------------------------------------------------------------------


def test_verify_fails_cleanly_when_the_artifact_is_missing(env, tmp_path):
    cfg, settings = env
    created = service.create_backup(cfg, settings)
    (tmp_path / "store" / created.storage_prefix / "tapdb.dump").unlink()

    # A missing artifact must be a reported failure, not a traceback.
    with pytest.raises((BackupNotFoundError, FileNotFoundError, OSError)):
        service.verify_backup(cfg, settings, backup_id=created.backup_id)


def test_verify_fails_when_the_detached_checksum_is_missing(env, tmp_path):
    cfg, settings = env
    created = service.create_backup(cfg, settings)
    (tmp_path / "store" / created.storage_prefix / "manifest.sha256").unlink()

    report = service.verify_backup(cfg, settings, backup_id=created.backup_id)

    assert not report.ok
    assert any(c.id == "manifest.checksum" and c.failed for c in report.checks)


def test_a_backup_whose_manifest_is_gone_is_not_listed(env, tmp_path):
    cfg, settings = env
    created = service.create_backup(cfg, settings)
    (tmp_path / "store" / created.storage_prefix / "manifest.json").unlink()

    listing = service.list_backups(cfg, settings)

    # Discovery is manifest-driven, so an artifact without one is invisible
    # rather than half-listed.
    assert created.backup_id not in [entry.backup_id for entry in listing.entries]


def test_listing_survives_an_unparseable_manifest(env, tmp_path):
    cfg, settings = env
    good = service.create_backup(cfg, settings)
    broken = service.create_backup(cfg, settings)
    (tmp_path / "store" / broken.storage_prefix / "manifest.json").write_text("{{{")

    listing = service.list_backups(cfg, settings)

    # One damaged backup must not hide every good one -- the whole point of
    # scanning for manifests instead of trusting an index.
    assert good.backup_id in [entry.backup_id for entry in listing.entries]
    assert broken.backup_id not in [entry.backup_id for entry in listing.entries]


def test_a_damaged_manifest_is_reported_not_silently_dropped(env, tmp_path):
    cfg, settings = env
    service.create_backup(cfg, settings)
    broken = service.create_backup(cfg, settings)
    (tmp_path / "store" / broken.storage_prefix / "manifest.json").write_text("{{{")

    listing = service.list_backups(cfg, settings)

    # A shorter list with no explanation would read as "this is everything you
    # have", which is exactly wrong when a backup is damaged.
    assert broken.storage_prefix in listing.damaged
    assert listing.to_payload()["damaged"] == listing.damaged


# ---------------------------------------------------------------------------
# non-full backup classes
# ---------------------------------------------------------------------------


def test_verify_of_a_template_pack_skips_archive_checks(env):
    cfg, settings = env
    created = service.create_backup(
        cfg, settings, backup_class=BACKUP_CLASS_TEMPLATE_PACK
    )

    report = service.verify_backup(cfg, settings, backup_id=created.backup_id)

    assert report.ok, [c.to_payload() for c in report.checks if c.failed]
    # A JSON pack has no pg_restore table of contents to read.
    assert not any(c.id == "archive.toc" for c in report.checks)
    assert not any(c.id == "archive.deep_read" for c in report.checks)


def test_a_corrupted_template_pack_is_detected(env, tmp_path):
    cfg, settings = env
    created = service.create_backup(
        cfg, settings, backup_class=BACKUP_CLASS_TEMPLATE_PACK
    )
    artifact = tmp_path / "store" / created.storage_prefix / "template-pack.json"
    artifact.write_text(artifact.read_text() + " ")

    report = service.verify_backup(cfg, settings, backup_id=created.backup_id)

    assert not report.ok


# ---------------------------------------------------------------------------
# id and listing behaviour
# ---------------------------------------------------------------------------


def test_backup_ids_do_not_collide_within_the_same_second(env):
    cfg, settings = env

    ids = {service.new_backup_id("full") for _ in range(200)}

    assert len(ids) == 200


def test_listing_is_newest_first(env):
    cfg, settings = env
    first = service.create_backup(cfg, settings)
    second = service.create_backup(cfg, settings)

    entries = service.list_backups(cfg, settings).entries

    assert entries[0].created_at >= entries[-1].created_at
    assert {first.backup_id, second.backup_id} <= {e.backup_id for e in entries}


def test_listing_honours_a_limit(env):
    cfg, settings = env
    for _ in range(3):
        service.create_backup(cfg, settings)

    assert len(service.list_backups(cfg, settings, limit=2).entries) == 2
    assert service.list_backups(cfg, settings, limit=0).entries == []
