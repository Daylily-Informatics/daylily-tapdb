"""``tapdb backup`` driven end to end against a real PostgreSQL.

Exercises the CLI the way an operator and a monitoring job would: real exit
codes, real ``--json`` payloads, real artifacts on disk. The registry and
argument tests live in ``test_backup_cli.py``; this file is about what actually
happens when the commands run.

Uses the ``pg_instance`` fixture -- an ephemeral cluster created with ``initdb``
under pytest's tmp dir on port 15438, torn down afterwards.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.backup import service
from daylily_tapdb.cli import framework_app
from daylily_tapdb.cli.backup import EXIT_FINDINGS, EXIT_OK
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context

runner = CliRunner()

pytestmark = pytest.mark.skipif(
    not shutil.which("pg_dump") or not shutil.which("pg_restore"),
    reason="pg_dump/pg_restore not on PATH",
)


@pytest.fixture(autouse=True)
def _context(pg_instance, monkeypatch, tmp_path):
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
    # Module-scoped fixtures are set up before function-scoped ones, so this
    # cannot rely on `_context` above having run. Establish the context here.
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    config = str(pg_instance["config_path"])
    applied = runner.invoke(
        framework_app, ["--config", config, "db", "schema", "apply"]
    )
    assert applied.exit_code == 0, applied.output
    seeded = runner.invoke(
        framework_app, ["--config", config, "db", "data", "seed", "--skip-existing"]
    )
    assert seeded.exit_code == 0, seeded.output
    return True


@pytest.fixture
def store(pg_instance, _schema_applied, tmp_path, monkeypatch):
    """Point backup storage at an isolated directory for each test."""
    from daylily_tapdb.cli import db_config as db_config_mod

    real = db_config_mod.get_backup_settings

    def _patched(**kwargs):
        settings = dict(real(**kwargs))
        settings["config_dir"] = str(tmp_path)
        settings["storage_uri"] = f"file://{tmp_path / 'store'}"
        return settings

    monkeypatch.setattr(db_config_mod, "get_backup_settings", _patched)
    import daylily_tapdb.cli.backup as cli_backup

    monkeypatch.setattr(
        cli_backup, "_resolve", lambda: (db_config_mod.get_db_config(), _patched())
    )
    return tmp_path


def _run(pg_instance, *args, json_mode: bool = False):
    argv = ["--config", str(pg_instance["config_path"])]
    if json_mode:
        argv.append("--json")
    argv.extend(args)
    return runner.invoke(framework_app, argv)


def _payload(result):
    return json.loads(result.output)


# ---------------------------------------------------------------------------
# read-only commands
# ---------------------------------------------------------------------------


def test_plan_succeeds_and_reports_what_it_would_capture(pg_instance, store):
    result = _run(pg_instance, "backup", "plan", json_mode=True)

    assert result.exit_code == EXIT_OK, result.output
    payload = _payload(result)
    assert payload["ok"] is True
    assert payload["would_capture"]["table_count"] >= 9
    assert payload["checks"]


def test_plan_writes_nothing(pg_instance, store):
    _run(pg_instance, "backup", "plan")

    assert not (store / "store").exists()


def test_list_is_empty_but_successful_before_any_backup(pg_instance, store):
    result = _run(pg_instance, "backup", "list", json_mode=True)

    # An empty inventory is a valid answer, not an error.
    assert result.exit_code == EXIT_OK
    assert _payload(result)["count"] == 0


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


def test_create_produces_a_real_artifact(pg_instance, store):
    result = _run(pg_instance, "backup", "create", json_mode=True)

    assert result.exit_code == EXIT_OK, result.output
    payload = _payload(result)
    backup_id = payload["backup_id"]
    root = store / "store" / payload["storage_prefix"]

    assert (root / "manifest.json").is_file()
    assert (root / "manifest.sha256").is_file()
    assert (root / "tapdb.dump").is_file()
    assert payload["manifest"]["row_counts"]
    assert backup_id


def test_create_dry_run_writes_nothing(pg_instance, store):
    result = _run(pg_instance, "--dry-run", "backup", "create", json_mode=True)

    assert result.exit_code == EXIT_OK, result.output
    assert _payload(result)["dry_run"] is True
    assert not (store / "store").exists()


def test_create_then_list_shows_the_backup(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))

    listed = _payload(_run(pg_instance, "backup", "list", json_mode=True))

    assert created["backup_id"] in [b["backup_id"] for b in listed["backups"]]


def test_create_records_a_note(pg_instance, store):
    result = _run(
        pg_instance, "backup", "create", "--note", "before migration", json_mode=True
    )

    assert _payload(result)["manifest"]["timestamps"]["note"] == "before migration"


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


def test_verify_passes_on_a_fresh_backup(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))

    result = _run(
        pg_instance,
        "backup",
        "verify",
        "--backup-id",
        created["backup_id"],
        json_mode=True,
    )

    assert result.exit_code == EXIT_OK, result.output
    assert _payload(result)["ok"] is True


def test_verify_exits_one_on_a_corrupted_artifact(pg_instance, store):
    """Exit 1, not 2: the command ran and found a problem.

    A monitoring job needs to distinguish "this backup is corrupt" from "the
    runner is misconfigured".
    """
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))
    artifact = store / "store" / created["storage_prefix"] / "tapdb.dump"
    raw = bytearray(artifact.read_bytes())
    for offset in range(len(raw) // 2, len(raw) // 2 + 512):
        raw[offset] ^= 0xFF
    artifact.write_bytes(bytes(raw))

    result = _run(
        pg_instance,
        "backup",
        "verify",
        "--backup-id",
        created["backup_id"],
        json_mode=True,
    )

    assert result.exit_code == EXIT_FINDINGS
    assert _payload(result)["ok"] is False


def test_verify_of_an_unknown_backup_is_a_finding(pg_instance, store):
    result = _run(
        pg_instance, "backup", "verify", "--backup-id", "full-nope", json_mode=True
    )

    assert result.exit_code == EXIT_FINDINGS


# ---------------------------------------------------------------------------
# restore-plan and restore
# ---------------------------------------------------------------------------


def test_restore_plan_prints_steps_and_a_fingerprint(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))

    result = _run(
        pg_instance,
        "backup",
        "restore-plan",
        "--backup-id",
        created["backup_id"],
        json_mode=True,
    )

    assert result.exit_code == EXIT_OK, result.output
    payload = _payload(result)
    assert payload["steps"]
    assert payload["plan_fingerprint"]
    assert payload["required_confirm_target"]


def test_in_place_restore_without_confirmation_is_a_finding(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))

    result = _run(
        pg_instance,
        "backup",
        "restore",
        "--backup-id",
        created["backup_id"],
        "--mode",
        "in-place",
        json_mode=True,
    )

    assert result.exit_code == EXIT_FINDINGS
    assert "confirm" in result.output.lower()


def test_a_stale_fingerprint_is_refused(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))
    label = _payload(
        _run(
            pg_instance,
            "backup",
            "restore-plan",
            "--backup-id",
            created["backup_id"],
            json_mode=True,
        )
    )["required_confirm_target"]

    result = _run(
        pg_instance,
        "backup",
        "restore",
        "--backup-id",
        created["backup_id"],
        "--mode",
        "in-place",
        "--confirm-target",
        label,
        "--plan-fingerprint",
        "not-the-real-one",
        json_mode=True,
    )

    assert result.exit_code == EXIT_FINDINGS
    assert "stale" in result.output.lower()


def test_isolated_restore_round_trips_through_the_cli(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))

    result = _run(
        pg_instance,
        "backup",
        "restore",
        "--backup-id",
        created["backup_id"],
        json_mode=True,
    )

    assert result.exit_code == EXIT_OK, result.output
    payload = _payload(result)
    assert payload["ok"] is True
    assert payload["mode"] == "isolated"

    # Clean up the database the restore created.
    from daylily_tapdb.backup import engine as eng
    from daylily_tapdb.cli.db_config import get_db_config

    cfg = get_db_config()
    eng.run_command(
        eng.build_psql_command(
            cfg,
            sql=f'DROP DATABASE IF EXISTS "{payload["target_database"]}"',
            database="postgres",
        ),
        env=eng.client_env(cfg),
    )


def test_restore_dry_run_mutates_nothing(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))

    result = _run(
        pg_instance,
        "--dry-run",
        "backup",
        "restore",
        "--backup-id",
        created["backup_id"],
        json_mode=True,
    )

    assert result.exit_code == EXIT_OK, result.output
    assert _payload(result)["dry_run"] is True


# ---------------------------------------------------------------------------
# rehearse
# ---------------------------------------------------------------------------


def test_rehearse_verifies_and_writes_evidence(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))

    result = _run(
        pg_instance,
        "backup",
        "rehearse",
        "--backup-id",
        created["backup_id"],
        json_mode=True,
    )

    assert result.exit_code == EXIT_OK, result.output
    payload = _payload(result)
    assert payload["ok"] is True
    assert payload["evidence_key"]
    assert (store / "store" / payload["evidence_key"]).is_file()


def test_rehearse_records_a_rehearsal_receipt(pg_instance, store):
    from daylily_tapdb.backup.receipts import read_receipts

    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))
    _run(
        pg_instance,
        "backup",
        "rehearse",
        "--backup-id",
        created["backup_id"],
        json_mode=True,
    )

    operations = [r.operation for r in read_receipts(store / "backups" / "receipts")]

    assert "backup_rehearse" in operations
    assert "backup_restore" not in operations


# ---------------------------------------------------------------------------
# json contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args",
    [
        ("backup", "plan"),
        ("backup", "list"),
        ("backup", "create"),
    ],
)
def test_json_mode_emits_only_parseable_json(pg_instance, store, args):
    result = _run(pg_instance, *args, json_mode=True)

    # Automation parses stdout; a stray human-readable line would break it.
    payload = json.loads(result.output)
    assert isinstance(payload, dict)


def test_human_list_output_names_each_backup(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))

    result = _run(pg_instance, "backup", "list")

    assert result.exit_code == EXIT_OK
    assert created["backup_id"] in result.output


def test_human_list_says_so_when_empty(pg_instance, store):
    result = _run(pg_instance, "backup", "list")

    assert result.exit_code == EXIT_OK
    assert "No backups found" in result.output


def test_human_list_warns_about_a_damaged_backup(pg_instance, store):
    good = _payload(_run(pg_instance, "backup", "create", json_mode=True))
    broken = _payload(_run(pg_instance, "backup", "create", json_mode=True))
    (store / "store" / broken["storage_prefix"] / "manifest.json").write_text("{{{")

    result = _run(pg_instance, "backup", "list")

    # A shorter list with no explanation would read as "this is everything".
    assert result.exit_code == EXIT_OK
    assert good["backup_id"] in result.output
    assert "unreadable manifest" in result.output


def test_human_plan_output_is_readable(pg_instance, store):
    result = _run(pg_instance, "backup", "plan")

    assert result.exit_code == EXIT_OK
    assert "Backup plan for" in result.output
    assert "storage:" in result.output


def test_verify_of_a_loose_path_warns_about_its_limits(pg_instance, store):
    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))
    artifact = store / "store" / created["storage_prefix"] / "tapdb.dump"

    result = _run(pg_instance, "backup", "verify", "--path", str(artifact))

    # Without a manifest there is nothing to compare against; the command must
    # say so rather than implying a full verification.
    assert result.exit_code == EXIT_OK
    assert "readability only" in result.output


def test_human_mode_is_not_json(pg_instance, store):
    result = _run(pg_instance, "backup", "plan")

    with pytest.raises(json.JSONDecodeError):
        json.loads(result.output)


def test_operations_are_written_to_the_shared_cli_log(pg_instance, store):
    _run(pg_instance, "backup", "create")

    log = Path(pg_instance["config_path"]).parent / "logs" / "db_operations.log"
    assert log.is_file()
    assert "BACKUP_CREATE" in log.read_text()


def test_create_emits_a_receipt_readable_by_the_service(pg_instance, store):
    from daylily_tapdb.backup.receipts import read_receipts, verify_receipt_chain

    _run(pg_instance, "backup", "create")
    _run(pg_instance, "backup", "create")

    receipts = read_receipts(store / "backups" / "receipts")
    creates = [r for r in receipts if r.operation == "backup_create"]

    assert len(creates) == 2
    assert all(r.actor.surface == "cli" for r in creates)
    assert verify_receipt_chain(receipts).ok


def test_the_cli_and_the_service_agree_on_what_exists(pg_instance, store):
    """The surfaces-contract property, checked at the CLI boundary."""
    from daylily_tapdb.cli.db_config import get_backup_settings, get_db_config

    created = _payload(_run(pg_instance, "backup", "create", json_mode=True))
    cli_ids = {
        b["backup_id"]
        for b in _payload(_run(pg_instance, "backup", "list", json_mode=True))[
            "backups"
        ]
    }

    settings = dict(get_backup_settings())
    settings["config_dir"] = str(store)
    settings["storage_uri"] = f"file://{store / 'store'}"
    service_ids = {
        e.backup_id for e in service.list_backups(get_db_config(), settings).entries
    }

    assert created["backup_id"] in cli_ids
    assert cli_ids == service_ids
