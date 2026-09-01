"""``tapdb backup health`` -- the alerting contract, one test per decision row.

Two properties of this file are deliberate and load-bearing.

**No PostgreSQL, anywhere.** There is no ``pg_dump`` skip marker and no
``pg_instance`` fixture. Health's defining guarantee is that it needs no
database -- it must still answer when the database is down, which is exactly
when someone is asking. Copying the ``skipif`` from
``test_backup_cli_exit_codes.py`` would make this entire suite skip green on
any machine without PostgreSQL, deleting coverage of the one command whose
whole point is not needing it. The config below points at a port with nothing
listening, so if health ever opens a connection the failure is a non-
``BackupError``, ``_handle`` returns 2, and the assertions here catch it.

**The real entry point, as a subprocess.** ``CliRunner`` normalises exit codes
-- that is how ``typer.Exit`` silently reported 0 from the shell while four
tests said otherwise. Since the exit code *is* the contract here, these spawn
the CLI the way cron does.

Fixtures are seeded through the real ``write_receipt`` and real
``BackupManifest``, never hand-written JSON. Literal dicts drifted from reality
four separate times in the GUI tests before that lesson took.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from daylily_tapdb.backup.manifest import (
    AssetRef,
    BackupManifest,
    canonical_bytes,
    sha256_hex,
    sign_manifest,
)
from daylily_tapdb.backup.receipts import (
    OPERATION_CREATE,
    OPERATION_REHEARSE,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    SURFACE_CLI,
    Actor,
    write_receipt,
)
from daylily_tapdb.backup.storage import (
    MANIFEST_CHECKSUM_KEY,
    MANIFEST_KEY,
    backup_prefix,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_UNAVAILABLE = 2

CLIENT = "healthclient"
DATABASE_NAME = "healthdb"
SCHEMA = "tapdb_healthdb"
ACTOR = Actor(surface=SURFACE_CLI, username="pytest")

#: Nothing listens here. Any attempt to connect fails fast, which is what makes
#: "health needs no database" a falsifiable claim rather than an aspiration.
DEAD_PORT = 1


def _now() -> datetime:
    return datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def target(tmp_path: Path):
    """A configured target with storage and receipts, and no database."""
    home = tmp_path / "home"
    cfg_dir = home / ".config" / "tapdb" / CLIENT / DATABASE_NAME
    cfg_dir.mkdir(parents=True)

    domain_registry = cfg_dir / "domain_code_registry.json"
    domain_registry.write_text(
        json.dumps(
            {"version": "0.4.0", "domains": {"Z": {"name": "test-health"}}}, indent=2
        )
        + "\n",
        encoding="utf-8",
    )
    prefix_registry = cfg_dir / "prefix_ownership_registry.json"
    prefix_registry.write_text(
        json.dumps(
            {
                "version": "0.4.0",
                "ownership": {
                    "Z": {
                        "TPX": {"issuer_app_code": "daylily-tapdb"},
                        "ADT": {"issuer_app_code": "daylily-tapdb"},
                        "SYS": {"issuer_app_code": "daylily-tapdb"},
                    }
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    storage_dir = tmp_path / "store"
    config_path = cfg_dir / "tapdb-config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "meta": {
                    "config_version": 4,
                    "client_id": CLIENT,
                    "database_name": DATABASE_NAME,
                    "owner_repo_name": "daylily-tapdb",
                    "domain_registry_path": str(domain_registry),
                    "prefix_ownership_registry_path": str(prefix_registry),
                },
                "target": {
                    "engine_type": "local",
                    "host": "localhost",
                    "port": DEAD_PORT,
                    "ui_port": 18911,
                    "domain_code": "Z",
                    "user": "nobody",
                    "password": "",
                    "database": "nodb",
                    "schema_name": SCHEMA,
                },
                "safety": {
                    "safety_tier": "local",
                    "destructive_operations": "confirm_required",
                },
                "backup": {"storage": {"uri": f"file://{storage_dir}"}},
            }
        ),
        encoding="utf-8",
    )
    os.chmod(config_path, 0o600)

    return {
        "home": home,
        "config_path": config_path,
        "config_dir": cfg_dir,
        "storage_dir": storage_dir,
        "receipts_dir": cfg_dir / "backups" / "receipts",
    }


def _set_backup_config(target, **backup_keys) -> None:
    """Merge keys into the ``backup:`` block of the target's config."""
    path = target["config_path"]
    data = yaml.safe_load(path.read_text())
    data.setdefault("backup", {}).update(backup_keys)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def _add_backup(
    target,
    *,
    backup_id: str,
    artifacts: bool = True,
    backup_class: str = "provider-snapshot",
    corrupt: bool = False,
) -> str:
    """Publish a real manifest (and optionally its artifact) into storage.

    Defaults to ``provider-snapshot``: it is a real recovery point (the runbook
    lists snapshot cutover as one of the three recovery methods) *and*
    ``verify_backup`` only reads an archive's table of contents for ``full``
    backups, which shells out to ``pg_restore``. A ``full`` default would make
    this suite need the PostgreSQL client tools -- the dependency the whole
    file exists to avoid.

    It is deliberately **not** ``template-pack``. That was the original default
    and it made ``test_a_healthy_target_exits_zero`` assert exit 0 for a store
    with no way back to the data at all, since ``restore_backup`` refuses every
    class but ``full`` and a template pack carries no instance data. The test
    suite was pinning the exact failure the command exists to prevent.
    """
    prefix = backup_prefix(CLIENT, DATABASE_NAME, backup_class, backup_id)
    root = target["storage_dir"] / prefix
    root.mkdir(parents=True, exist_ok=True)

    payload = b'{"snapshot": "recorded"}'
    asset_name = {
        "full": "tapdb.dump",
        "template-pack": "template-pack.json",
        "provider-snapshot": "snapshot-receipt.json",
    }[backup_class]
    if artifacts:
        (root / asset_name).write_bytes(
            b"corrupted-after-the-fact" if corrupt else payload
        )

    manifest = BackupManifest(
        backup_id=backup_id,
        backup_class=backup_class,
        target_identity={
            "client_id": CLIENT,
            "database_name": DATABASE_NAME,
            "schema_name": SCHEMA,
            "target_label": f"{CLIENT}/{DATABASE_NAME}/{SCHEMA}@nodb",
        },
        included_assets=[
            AssetRef(name=asset_name, bytes=len(payload), sha256=sha256_hex(payload))
        ],
        provenance={"created_by": "operator"},
        timestamps={"started_at": _now().isoformat()},
    )
    manifest.signature = sign_manifest(manifest.to_payload(), mode="none")

    raw = canonical_bytes(manifest.to_payload())
    (root / MANIFEST_KEY).write_bytes(raw)
    (root / MANIFEST_CHECKSUM_KEY).write_bytes(sha256_hex(raw).encode("utf-8"))
    return prefix


def _receipt(
    target,
    *,
    operation,
    status,
    when=None,
    backup_id="full-x",
    detail=None,
    mirror=None,
):
    return write_receipt(
        target["receipts_dir"],
        operation=operation,
        status=status,
        actor=ACTOR,
        backup_id=backup_id,
        target_label=f"{CLIENT}/{DATABASE_NAME}/{SCHEMA}@nodb",
        detail=detail or {},
        receipt_mirror=mirror or {},
        now=when or _now(),
    )


def _health(target, *args) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "daylily_tapdb.cli",
            "--config",
            str(target["config_path"]),
            *args,
        ],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env={
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(target["home"]),
            "USER": "pytest",
        },
        timeout=300,
    )


def _run(target, *extra) -> tuple[int, dict]:
    """Run health and return (exit code, parsed stdout).

    Parsing stdout is not incidental. A subprocess that dies on an import error
    or a click parse failure also returns 1, which would satisfy every
    "expect exit 1" assertion below on its own. Requiring real JSON forces the
    process to have actually produced a verdict.
    """
    result = _health(target, "backup", "health", *extra)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:  # pragma: no cover - failure path
        pytest.fail(
            f"stdout was not JSON (exit {result.returncode}):\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
        )
    return result.returncode, payload


def _status_of(payload: dict, check_id: str) -> str:
    for check in payload["checks"]:
        if check["id"] == check_id:
            return check["status"]
    raise AssertionError(
        f"{check_id} missing from {[c['id'] for c in payload['checks']]}"
    )


# ---------------------------------------------------------------------------
# harness
# ---------------------------------------------------------------------------


def test_the_harness_reaches_the_cli_at_all(target):
    """Guards the guard: if the subprocess never ran, nothing below means anything."""
    result = _health(target, "backup", "--help")

    assert result.returncode == EXIT_OK, result.stdout + result.stderr
    assert "health" in result.stdout


def test_health_emits_json_on_stdout_with_no_json_flag(target):
    """JSON by default is the contract -- `backup health --json` is rejected.

    ``--json`` is a global option, so it must precede the subcommand. A caller
    that appends args after it gets ``Error: No such option``, which is why
    health does not have one.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_OK, payload
    assert payload["target_label"].startswith(CLIENT)
    assert payload["checks"], payload

    rejected = _health(target, "backup", "health", "--json")
    assert rejected.returncode != EXIT_OK
    assert "No such option" in (rejected.stderr + rejected.stdout)


def test_global_json_flag_produces_exactly_one_document(target):
    """`tapdb --json backup health` must not emit two JSON documents."""
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    result = _health(target, "--json", "backup", "health")

    # json.loads rejects trailing content, so a second document fails here.
    payload = json.loads(result.stdout)
    assert payload["status"] in {"ok", "warn"}


# ---------------------------------------------------------------------------
# the decision table
# ---------------------------------------------------------------------------


def test_a_healthy_target_exits_zero(target):
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_OK, payload["failing"]
    assert payload["failing"] == []


def test_every_backup_failing_and_none_ever_succeeding_exits_one(target):
    """The `never_run` masks `failing` hole.

    ``derive_backup_status`` evaluates ``never_run`` before ``failing`` and
    returns one scalar, so a brand-new production target whose nightly backup
    has failed for a month reports ``never_run``. Keying this check on
    ``status == "failing"`` would grade that as a warning and exit 0 while zero
    backups exist. It keys on ``last_attempt_status`` instead.
    """
    for day in range(3):
        _receipt(
            target,
            operation=OPERATION_CREATE,
            status=STATUS_FAILED,
            when=_now() - timedelta(days=3 - day),
        )

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.last_attempt") == "fail"


def test_last_attempt_failed_after_prior_success_exits_one(target):
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(
        target,
        operation=OPERATION_CREATE,
        status=STATUS_SUCCEEDED,
        when=_now() - timedelta(hours=5),
    )
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_FAILED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.last_attempt") == "fail"


def test_a_success_older_than_the_cadence_exits_one(target):
    _set_backup_config(target, expected_interval_hours=24)
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(
        target,
        operation=OPERATION_CREATE,
        status=STATUS_SUCCEEDED,
        when=_now() - timedelta(days=5),
    )

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.cadence") == "fail"


def test_an_empty_store_exits_one(target):
    """Receipts record attempts, not inventory.

    Retarget ``backup.storage.uri`` by one character and every receipt-derived
    check still passes against a store with nothing in it.
    """
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.inventory") == "fail"


def test_deleting_the_receipts_directory_still_exits_one(target):
    """Total erasure must not report healthier than partial truncation.

    ``verify_receipt_chain([], head=None)`` returns ok with count 0, so the
    chain check is vacuously green on a wiped store -- and inventory passes
    too, because the backups themselves are untouched. Corrupting *one*
    receipt fails; deleting *all* of them would otherwise pass, rewarding the
    more destructive act. ``health.receipt_coverage`` is what closes that.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    for path in target["receipts_dir"].iterdir():
        path.unlink()

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.receipt_chain") == "pass"  # vacuously
    assert _status_of(payload, "health.inventory") == "pass"  # backups intact
    assert _status_of(payload, "health.receipt_coverage") == "fail"
    assert exit_code == EXIT_FINDINGS


def test_manifests_present_but_artifacts_gone_exits_one(target):
    """A Glacier transition or partial sync leaves a tidy, unrestorable list."""
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa", artifacts=False)
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.hollow_backup") == "fail"


def test_a_tampered_receipt_exits_one(target):
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    newest = sorted(target["receipts_dir"].glob("*.json"))[-1]
    payload_json = json.loads(newest.read_text())
    payload_json["status"] = "succeeded-but-edited"
    newest.chmod(0o600)
    newest.write_text(json.dumps(payload_json, indent=2))

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.receipt_chain") == "fail"


def test_an_unparseable_receipt_file_warns_rather_than_failing(target):
    """Local corruption is not tampering, and must not page forever.

    ``write_receipt`` deliberately lets a corrupt file keep its sequence number
    rather than block writes, so one partial write leaves a permanent sequence
    gap with no repair command. Failing hard would mean the only way to silence
    the alert is deleting the audit trail.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    oldest = sorted(target["receipts_dir"].glob("*.json"))[0]
    oldest.chmod(0o600)
    oldest.write_text("{ this is not json")

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.receipt_chain") == "warn"
    assert exit_code == EXIT_OK, payload["failing"]


def test_a_rehearsal_that_fails_on_schedule_exits_one(target):
    """Age alone would keep a nightly rehearsal that always fails green."""
    _set_backup_config(target, expected_rehearsal_interval_days=90)
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(
        target,
        operation=OPERATION_REHEARSE,
        status=STATUS_FAILED,
        when=_now() - timedelta(days=1),
    )

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.rehearsal_age") == "fail"


def test_a_rehearsal_older_than_the_interval_exits_one(target):
    _set_backup_config(target, expected_rehearsal_interval_days=30)
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(
        target,
        operation=OPERATION_REHEARSE,
        status=STATUS_SUCCEEDED,
        when=_now() - timedelta(days=200),
    )

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.rehearsal_age") == "fail"


def test_an_unparseable_cadence_exits_one(target):
    """A typo in the field that arms the alarm must not read as "no alarm wanted".

    ``_float`` returns the default on a parse error, and that default is 0,
    meaning no cadence -- so ``"24h"`` silently disarms the only
    scheduler-stopped detector there is.
    """
    _set_backup_config(target, expected_interval_hours="24h")
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.cadence_configured") == "fail"


def test_an_interrupted_prune_warns_but_does_not_page(target):
    """Warn, because a failure here could never be cleared.

    Receipts are immutable, so a dangling intent cannot be resolved by writing
    anything. As a failure it would page forever and the only way to silence it
    would be deleting the audit trail -- training operators to do the most
    destructive thing available.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(
        target,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={"phase": "intent", "prune_id": "prune-abc"},
    )

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.interrupted_prune") == "warn"
    assert exit_code == EXIT_OK, payload["failing"]


def test_an_interrupted_prune_that_did_damage_still_pages(target):
    """What makes the warn above safe.

    The consequence of an interrupted prune -- a half-deleted prefix -- is
    detected independently and *fails*. Downgrading the bookkeeping signal
    loses nothing, because the damage itself still alerts.
    """
    prefix = _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(
        target,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={"phase": "intent", "prune_id": "prune-abc"},
    )
    # Exactly what an interruption between artifact-delete and manifest-delete
    # leaves behind: manifest intact, artifact gone.
    (target["storage_dir"] / prefix / "snapshot-receipt.json").unlink()

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.interrupted_prune") == "warn"
    assert _status_of(payload, "health.hollow_backup") == "fail"
    assert exit_code == EXIT_FINDINGS


def test_a_completed_prune_does_not_flag(target):
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(
        target,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={"phase": "intent", "prune_id": "prune-abc"},
    )
    _receipt(
        target,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={"phase": "outcome", "prune_id": "prune-abc"},
    )

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.interrupted_prune") == "pass"
    assert exit_code == EXIT_OK, payload["failing"]


# ---------------------------------------------------------------------------
# warn, never page
# ---------------------------------------------------------------------------


def test_a_target_with_no_backups_at_all_exits_one(target):
    """A freshly provisioned target *does* page, and that is correct.

    The plan justified grading ``never_run`` as a warning so a new target would
    not alert the moment it exists. That rationale does not survive contact
    with ``health.inventory``: a target with no backups cannot be recovered,
    and reporting "healthy" for it is precisely the failure this command
    exists to prevent. Inventory wins, deliberately. The way to stop the alert
    is to take a backup, not to grade "you have none" as noise.
    """
    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.inventory") == "fail"
    assert _status_of(payload, "health.never_run") == "warn"


def test_never_run_annotates_but_never_drives_the_exit_code(target):
    """``health.never_run`` is informational, and cannot be reached alone.

    Every state that makes it true is already failed by another check --
    no backups (inventory), backups with no create receipt at all (receipt
    coverage), or a create receipt that failed (last attempt). It is kept
    because "this target has never had a successful backup" is worth reading
    in the JSON, not because it changes any verdict.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.never_run") == "warn"
    # Reached only alongside a real failure -- here, the missing audit trail.
    assert exit_code == EXIT_FINDINGS
    assert "health.receipt_coverage" in payload["failing"]


def test_no_cadence_configured_warns_but_exits_zero(target):
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_OK
    assert _status_of(payload, "health.cadence_configured") == "warn"


# ---------------------------------------------------------------------------
# exit 2 -- could not answer
# ---------------------------------------------------------------------------


def test_unreadable_storage_exits_two(target):
    """Storage unreachable is "I could not look", not "there are no backups"."""
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _set_backup_config(target, storage={"uri": "s3://"})

    exit_code, payload = _run(target)

    assert exit_code == EXIT_UNAVAILABLE, payload


def test_a_reached_failure_outranks_an_unreachable_source(target):
    """Tampering plus an unreachable bucket is exit 1, not 2.

    Health *did* reach a verdict; burying it under "could not answer" would
    suppress the alert exactly when someone has broken the audit trail.
    """
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    newest = sorted(target["receipts_dir"].glob("*.json"))[-1]
    edited = json.loads(newest.read_text())
    edited["status"] = "edited"
    newest.chmod(0o600)
    newest.write_text(json.dumps(edited, indent=2))

    _set_backup_config(target, storage={"uri": "s3://"})

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS, payload
    assert _status_of(payload, "health.receipt_chain") == "fail"


def test_stdout_is_json_even_when_health_cannot_answer(target):
    """Kahlo does json.loads(stdout) on every exit code, including 2."""
    _set_backup_config(target, storage={"uri": "s3://"})

    exit_code, payload = _run(target)

    assert exit_code == EXIT_UNAVAILABLE
    assert payload["status"] == "unavailable"


# ---------------------------------------------------------------------------
# the no-database guarantee
# ---------------------------------------------------------------------------


def test_health_answers_with_no_database_listening(target):
    """The config points at a dead port; health must still reach a verdict.

    If health ever opened a connection, the failure would surface as a
    non-``BackupError`` and ``_handle`` would return 2 -- so this asserting a
    real verdict is what keeps the no-database property honest.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_OK, payload
    assert payload["unavailable"] == []


def test_health_writes_nothing(target):
    """Read-only by intent, asserted rather than assumed.

    ``verify_backup`` writes a receipt by default, and health calls it for
    ``health.newest_verifies``.
    """

    def digest(root: Path) -> list[tuple[str, str]]:
        return sorted(
            (str(p.relative_to(root)), sha256_hex(p.read_bytes()))
            for p in root.rglob("*")
            if p.is_file()
        )

    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    before = digest(target["config_dir"]) + digest(target["storage_dir"])
    _run(target)
    after = digest(target["config_dir"]) + digest(target["storage_dir"])

    assert before == after


# ---------------------------------------------------------------------------
# human output
# ---------------------------------------------------------------------------


def test_human_output_renders_every_check(target):
    """``_render_checks`` does a bare ``check['detail']`` lookup."""
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    result = _health(target, "backup", "health", "--human")

    assert result.returncode == EXIT_OK, result.stdout + result.stderr
    assert "health.inventory" in result.stdout
    assert "{" not in result.stdout.split("\n")[0]


# ---------------------------------------------------------------------------
# toolchain independence
# ---------------------------------------------------------------------------


def test_a_corrupted_artifact_fails_verification(target):
    """Checksum mismatch is a real integrity failure, not an environment one."""
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa", corrupt=True)
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.newest_verifies") == "fail"


def test_a_missing_pg_restore_warns_rather_than_failing(target, tmp_path):
    """A slim monitoring container must not report the fleet as unrestorable.

    Quick verification reads the archive's table of contents for ``full``
    backups, which shells out to ``pg_restore``. ``_toc_check`` reports a
    missing binary exactly as it reports a corrupt archive, so without an
    explicit toolchain probe health would fail every backup anywhere the
    PostgreSQL client tools are not installed -- which is most places a
    monitoring job runs.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa", backup_class="full")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    empty_bin = tmp_path / "emptybin"
    empty_bin.mkdir()
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "daylily_tapdb.cli",
            "--config",
            str(target["config_path"]),
            "backup",
            "health",
        ],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env={
            "PATH": str(empty_bin),  # no pg_restore anywhere
            "HOME": str(target["home"]),
            "USER": "pytest",
        },
        timeout=300,
    )
    payload = json.loads(result.stdout)

    assert _status_of(payload, "health.newest_verifies") == "warn"
    assert result.returncode == EXIT_OK, payload["failing"]


# ---------------------------------------------------------------------------
# receipt mirror freshness
# ---------------------------------------------------------------------------


def test_a_current_mirror_passes(target, tmp_path):
    mirror = tmp_path / "mirror"
    _set_backup_config(target, receipt_mirror={"uri": f"file://{mirror}"})
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(
        target,
        operation=OPERATION_CREATE,
        status=STATUS_SUCCEEDED,
        mirror={"uri": f"file://{mirror}"},
    )

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.receipt_mirror") == "pass"
    assert exit_code == EXIT_OK, payload["failing"]


def test_a_mirror_that_stopped_receiving_warns(target, tmp_path):
    """A write-only, best-effort mirror is otherwise indistinguishable from a no-op.

    Receipts written before the mirror was configured leave it behind, which is
    the same shape as a mirror that has been quietly failing.
    """
    mirror = tmp_path / "mirror"
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    # Configure the mirror only now, so it is empty while the chain is at 2.
    _set_backup_config(target, receipt_mirror={"uri": f"file://{mirror}"})

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.receipt_mirror") == "warn"
    # Warn, not fail: a stale mirror is an evidence gap, not lost recoverability.
    assert exit_code == EXIT_OK, payload["failing"]


def test_no_mirror_configured_is_skipped_not_warned(target):
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    _, payload = _run(target)

    assert _status_of(payload, "health.receipt_mirror") == "skip"


# ---------------------------------------------------------------------------
# damaged manifests, and storage safety
# ---------------------------------------------------------------------------


def test_an_unreadable_manifest_exits_one(target):
    """A prefix that lists but whose manifest will not parse.

    Distinct from `hollow_backup`, where the manifest is fine and the artifacts
    are gone. Here the backup is undescribable, so nothing can be said about
    whether it would restore.
    """
    prefix = _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    (target["storage_dir"] / prefix / MANIFEST_KEY).write_text("{ not json")

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.damaged") == "fail"


def test_local_storage_does_not_warn_about_protections_it_cannot_have(target):
    """Object Lock and versioning are S3 concepts; local storage has neither.

    Warning about their absence would fire on every developer machine and
    every local deployment, forever, with no action available -- and an alert
    nobody can act on is the kind that trains people to ignore the rest.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    _, payload = _run(target)

    assert _status_of(payload, "health.storage_safety") == "pass"


def test_a_backend_without_the_probe_is_skipped(tmp_path, monkeypatch):
    """A host-supplied backend need not implement `deletion_capability`."""
    from daylily_tapdb.backup import service

    class _NoProbe:
        def list_keys(self, prefix=""):
            return []

        def list_sizes(self, prefix=""):
            return {}

        def describe(self):
            return {"backend": "custom"}

    monkeypatch.setattr(service, "storage_for", lambda _s: _NoProbe())
    report = service.health_report(
        {"client_id": "a", "database_name": "b", "schema_name": "c", "database": "d"},
        {"config_dir": str(tmp_path), "storage_uri": "file://x"},
    )
    by_id = {c.id: c for c in report.checks}

    assert by_id["health.storage_safety"].status == "skip"
    # Distinguish "probe absent, correctly skipped" from "the storage block
    # blew up and every row was skipped wholesale" -- `health_report` turns any
    # exception from the storage checks into a SKIP for every id, so the
    # status alone cannot tell the two apart.
    assert report.unavailable == []
    assert by_id["health.inventory"].status != "skip"


def test_storage_safety_warns_on_an_unprotected_bucket(tmp_path, monkeypatch):
    """The probe path, exercised in-process because no backend implements it yet.

    Runs through the real `health_report` rather than the check helper, so the
    verdict still has to survive aggregation and the exit-code rules -- an
    unprotected bucket is infrastructure's to fix and must never page.
    """
    from daylily_tapdb.backup import service

    class _Probing:
        """Minimal backend that reports deletion capability."""

        def __init__(self, capability):
            self._capability = capability

        def list_keys(self, prefix=""):
            return []

        def describe(self):
            return {"backend": "fake", "uri": "s3://fake"}

        def deletion_capability(self):
            return self._capability

    cfg = {
        "client_id": "acme",
        "database_name": "prod",
        "schema_name": "tapdb_prod",
        "database": "tapdb",
    }
    settings = {"config_dir": str(tmp_path), "storage_uri": "s3://fake"}

    backend = _Probing({"reclaims": True, "versioning": None, "object_lock": False})
    monkeypatch.setattr(service, "storage_for", lambda _s: backend)

    report = service.health_report(cfg, settings)
    by_id = {c.id: c for c in report.checks}

    assert by_id["health.storage_safety"].status == "warn"
    assert "Object Lock" in by_id["health.storage_safety"].detail
    # Warn only -- it must not be the reason a target pages.
    assert "health.storage_safety" not in [c.id for c in report.failing]


def test_storage_safety_passes_on_a_protected_bucket(tmp_path, monkeypatch):
    from daylily_tapdb.backup import service

    class _Probing:
        def list_keys(self, prefix=""):
            return []

        def describe(self):
            return {"backend": "fake", "uri": "s3://fake"}

        def deletion_capability(self):
            return {"reclaims": False, "versioning": "Enabled", "object_lock": True}

    cfg = {
        "client_id": "acme",
        "database_name": "prod",
        "schema_name": "tapdb_prod",
        "database": "tapdb",
    }
    settings = {"config_dir": str(tmp_path), "storage_uri": "s3://fake"}
    monkeypatch.setattr(service, "storage_for", lambda _s: _Probing())

    report = service.health_report(cfg, settings)
    by_id = {c.id: c for c in report.checks}

    assert by_id["health.storage_safety"].status == "pass"


# ---------------------------------------------------------------------------
# anti-vacuity
# ---------------------------------------------------------------------------

#: Every check `health_report` can emit. Adding a check without adding a case
#: to this file fails the closure test below.
KNOWN_CHECKS = {
    "health.cadence",
    "health.cadence_configured",
    "health.damaged",
    "health.hollow_backup",
    "health.interrupted_prune",
    "health.inventory",
    "health.last_attempt",
    "health.recovery_point",
    "health.never_run",
    "health.newest_verifies",
    "health.receipt_chain",
    "health.receipt_coverage",
    "health.receipt_mirror",
    "health.rehearsal_age",
    "health.storage_safety",
}


def test_every_emitted_check_is_known_and_covered(target):
    """A new check cannot be added without a test that names it.

    The failure this prevents is quiet: a check appears, nobody writes a case
    for it, and it sits in the output for months without anyone knowing whether
    it can actually fire. Comparing what the implementation emits against what
    this file asserts on is the only thing that makes that visible.
    """
    import re
    from pathlib import Path

    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    _, payload = _run(target)
    emitted = {check["id"] for check in payload["checks"]}

    assert emitted == KNOWN_CHECKS, (
        f"unexpected={sorted(emitted - KNOWN_CHECKS)} "
        f"missing={sorted(KNOWN_CHECKS - emitted)}"
    )

    # And each one is actually asserted on somewhere in this file, rather than
    # merely appearing in the list above.
    source = Path(__file__).read_text()
    asserted = set(re.findall(r'_status_of\(payload, "(health\.[a-z_]+)"\)', source))
    asserted |= set(re.findall(r'by_id\["(health\.[a-z_]+)"\]', source))
    uncovered = KNOWN_CHECKS - asserted
    assert not uncovered, f"checks with no assertion in this file: {sorted(uncovered)}"


def test_a_healthy_target_emits_no_failures_and_real_verdicts(target):
    """Guards against the whole suite passing on an all-`skip` report.

    Every "exit 0" assertion elsewhere would also hold if health silently
    degraded to skipping everything, which is the shape a broken source or an
    over-broad `except` produces.
    """
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)
    statuses = [check["status"] for check in payload["checks"]]

    assert exit_code == EXIT_OK
    assert statuses.count("pass") >= 8, payload["checks"]


# ---------------------------------------------------------------------------
# reachable states the branch coverage showed were untested
# ---------------------------------------------------------------------------


def test_a_rehearsal_cadence_with_no_rehearsal_yet_warns(target):
    """Setting the cadence before ever rehearsing is the normal first step."""
    _set_backup_config(target, expected_rehearsal_interval_days=90)
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.rehearsal_age") == "warn"
    assert exit_code == EXIT_OK, payload["failing"]


def test_a_mirror_configured_before_any_receipt_is_skipped(target, tmp_path):
    """Nothing to be behind on yet -- must not warn on a fresh target."""
    _set_backup_config(target, receipt_mirror={"uri": f"file://{tmp_path / 'mirror'}"})
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")

    _, payload = _run(target)

    assert _status_of(payload, "health.receipt_mirror") == "skip"


def test_an_unreadable_mirror_warns_rather_than_failing(target):
    """A mirror that cannot be reached is an evidence gap, not lost recovery."""
    _set_backup_config(target, receipt_mirror={"uri": "s3://"})
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.receipt_mirror") == "warn"
    assert exit_code == EXIT_OK, payload["failing"]


# ---------------------------------------------------------------------------
# recovery points -- counting rows is not the contract
# ---------------------------------------------------------------------------


def test_a_store_of_only_template_packs_exits_one(target):
    """The defect this check exists for, and the one the fixture used to hide.

    A target backed up nightly as ``template-pack`` lists dozens of healthy
    rows: inventory passes, checksums verify, receipts line up. And it cannot
    be restored -- ``restore_backup`` refuses every class but ``full``, and the
    runbook is explicit that a template pack is "a configuration export, not a
    recovery tool". Exit 0 here would mean "healthy" for a target with no way
    back to its data.
    """
    for index in range(3):
        _add_backup(
            target,
            backup_id=f"tpk-20260801T00000{index}Z-aaaaaa",
            backup_class="template-pack",
        )
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.inventory") == "pass"  # rows exist
    assert _status_of(payload, "health.recovery_point") == "fail"  # none usable


def test_a_full_backup_is_a_recovery_point(target):
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa", backup_class="full")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    _, payload = _run(target)

    assert _status_of(payload, "health.recovery_point") == "pass"


def test_a_provider_snapshot_is_a_recovery_point(target):
    """Snapshot cutover is one of the three documented recovery methods."""
    _add_backup(
        target,
        backup_id="snp-20260801T000000Z-aaaaaa",
        backup_class="provider-snapshot",
    )
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    _, payload = _run(target)

    assert _status_of(payload, "health.recovery_point") == "pass"


def test_verification_targets_a_recovery_point_not_merely_the_newest(target):
    """A recent template pack must not stand in for a real recovery point."""
    _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa", backup_class="full")
    _add_backup(
        target,
        backup_id="tpk-20260801T235959Z-bbbbbb",  # sorts newest
        backup_class="template-pack",
    )
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    _, payload = _run(target)

    verified = next(c for c in payload["checks"] if c["id"] == "health.newest_verifies")
    assert verified["data"].get("backup_id", "").startswith("full-"), verified


# ---------------------------------------------------------------------------
# degraded sources must not read as evidence of health
# ---------------------------------------------------------------------------


def test_unreadable_prefixes_do_not_pass_the_hollow_check(tmp_path, monkeypatch):
    """Checking nothing is not the same as finding nothing wrong.

    S3 throttling at scale, or a policy allowing LIST at the bucket root but
    denying it per-prefix, makes every per-entry read fail while the store
    itself is plainly reachable. Skipping those entries silently reported
    "every listed backup's artifacts are present in storage" having inspected
    none of them.
    """
    from daylily_tapdb.backup import service

    listing = service.BackupListing(
        entries=[
            service.BackupSummary(
                backup_id="full-1",
                backup_class="full",
                created_at="2026-08-01T00:00:00+00:00",
                status="complete",
                storage_prefix="acme/prod/full/full-1",
                target_label="acme/prod/tapdb_prod@tapdb",
                row_totals=1,
                bytes=10,
            )
        ],
        storage={"backend": "fake"},
    )

    class _Denied:
        def list_keys(self, prefix=""):
            raise PermissionError("AccessDenied on sub-prefix")

        def describe(self):
            return {"backend": "fake"}

    checks = service._health_storage_checks(
        {"client_id": "a", "database_name": "b", "schema_name": "c", "database": "d"},
        {"config_dir": str(tmp_path), "storage_uri": "s3://fake"},
        _Denied(),
        listing,
        1,
    )
    by_id = {c.id: c for c in checks}

    assert by_id["health.hollow_backup"].status != "pass"
    assert "inspected" in by_id["health.hollow_backup"].detail
    assert by_id["health.hollow_backup"].data["unchecked"]


def test_an_oversized_artifact_is_skipped_not_downloaded(tmp_path, monkeypatch):
    """Health is polled; `backup verify` is the audit.

    Quick verification downloads and hashes every asset. On a 50 GB dump that
    is 50 GB of egress per poll -- roughly 14 TB/day at a five-minute cadence
    -- for a liveness check.
    """
    from daylily_tapdb.backup import service

    listing = service.BackupListing(
        entries=[
            service.BackupSummary(
                backup_id="full-huge",
                backup_class="full",
                created_at="2026-08-01T00:00:00+00:00",
                status="complete",
                storage_prefix="acme/prod/full/full-huge",
                target_label="acme/prod/tapdb_prod@tapdb",
                row_totals=1,
                bytes=service.HEALTH_VERIFY_MAX_BYTES + 1,
            )
        ],
        storage={"backend": "fake"},
    )

    class _Storage:
        def list_keys(self, prefix=""):
            return [f"{prefix}/tapdb.dump"]

        def describe(self):
            return {"backend": "fake"}

    def _explode(*args, **kwargs):
        raise AssertionError("health must not download an oversized artifact")

    monkeypatch.setattr(service, "verify_backup", _explode)
    monkeypatch.setattr(
        service,
        "_load_manifest",
        lambda *a, **k: service.BackupManifest(
            backup_id="full-huge", backup_class="full"
        ),
    )

    checks = service._health_storage_checks(
        {"client_id": "a", "database_name": "b", "schema_name": "c", "database": "d"},
        {"config_dir": str(tmp_path), "storage_uri": "s3://fake"},
        _Storage(),
        listing,
        1,
    )
    by_id = {c.id: c for c in checks}

    assert by_id["health.newest_verifies"].status == "skip"
    assert "backup verify" in by_id["health.newest_verifies"].detail

    # The completeness guarantee holds on *this* path too. An early return here
    # dropped `health.storage_safety` on any target whose newest recovery point
    # exceeds the read limit -- i.e. most real ones -- and the closure test
    # could not catch it because it only ever runs the healthy fixture.
    assert set(service.STORAGE_CHECK_IDS) <= set(by_id), sorted(
        set(service.STORAGE_CHECK_IDS) - set(by_id)
    )


def test_unreadable_receipts_still_emit_every_receipt_check(tmp_path):
    """An unavailable source must emit a complete, predictable set of rows.

    Emitting one SKIP where the healthy path emits seven leaves a consumer
    keying on check ids with a KeyError, and makes "not checked this run"
    indistinguishable from "this check does not exist".
    """
    from daylily_tapdb.backup import service

    cfg = {
        "client_id": "a",
        "database_name": "b",
        "schema_name": "c",
        "database": "d",
    }
    # No `config_dir`, so `receipts_directory` raises rather than returning an
    # empty list -- the difference between "no receipts" and "cannot look".
    report = service.health_report(cfg, {"storage_uri": f"file://{tmp_path}"})
    emitted = {check.id for check in report.checks}

    assert set(service.RECEIPT_CHECK_IDS) <= emitted, sorted(
        set(service.RECEIPT_CHECK_IDS) - emitted
    )
    assert service.SOURCE_RECEIPTS in report.unavailable
    for check_id in service.RECEIPT_CHECK_IDS:
        status = next(c.status for c in report.checks if c.id == check_id)
        assert status == "skip", (check_id, status)


# ---------------------------------------------------------------------------
# artifact size, and the configurable read limit
# ---------------------------------------------------------------------------


def test_a_truncated_artifact_is_caught_without_reading_it(target):
    """Presence is not integrity.

    A partial sync or an interrupted upload leaves a file that exists and is
    the wrong length. Comparing names alone -- the original check -- passed it,
    and on S3 the size is already in the listing response, so checking it costs
    nothing.
    """
    prefix = _add_backup(target, backup_id="full-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    (target["storage_dir"] / prefix / "snapshot-receipt.json").write_bytes(b"")

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    assert _status_of(payload, "health.hollow_backup") == "fail"
    hollow = next(c for c in payload["checks"] if c["id"] == "health.hollow_backup")
    assert hollow["data"]["hollow"][0]["truncated"], hollow


def test_size_is_checked_on_every_backup_not_just_the_newest(target):
    """`newest_verifies` only ever looks at one backup; this looks at all."""
    _add_backup(target, backup_id="snp-20260801T235959Z-newest")
    old = _add_backup(target, backup_id="snp-20260801T000000Z-oldest")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    (target["storage_dir"] / old / "snapshot-receipt.json").write_bytes(b"x")

    exit_code, payload = _run(target)

    assert exit_code == EXIT_FINDINGS
    hollow = next(c for c in payload["checks"] if c["id"] == "health.hollow_backup")
    assert hollow["data"]["hollow"][0]["backup_id"].endswith("oldest")


def test_the_read_limit_is_configurable_and_zero_means_no_limit(target):
    """A small store can opt into always checksumming."""
    _add_backup(target, backup_id="snp-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    # A limit of 1 byte skips even this tiny artifact.
    _set_backup_config(target, health_verify_max_bytes=1)
    _, payload = _run(target)
    assert _status_of(payload, "health.newest_verifies") == "skip"

    # 0 disables the limit entirely.
    _set_backup_config(target, health_verify_max_bytes=0)
    exit_code, payload = _run(target)
    assert _status_of(payload, "health.newest_verifies") == "pass"
    assert exit_code == EXIT_OK, payload["failing"]


def test_a_backend_that_cannot_report_sizes_does_not_fake_agreement(tmp_path):
    """Unknown size must never read as "matches".

    TapDB is embedded by several hosts, and a host-supplied storage backend
    need not implement `list_sizes`. Falling back to presence-only is correct;
    defaulting the unknown size to 0 -- or to the expected value -- would turn
    every artifact into a false mismatch, or every truncation into a pass.
    """
    from daylily_tapdb.backup import service

    listing = service.BackupListing(
        entries=[
            service.BackupSummary(
                backup_id="full-1",
                backup_class="full",
                created_at="2026-08-01T00:00:00+00:00",
                status="complete",
                storage_prefix="acme/prod/full/full-1",
                target_label="acme/prod/tapdb_prod@tapdb",
                row_totals=1,
                bytes=999,
            )
        ],
        storage={"backend": "legacy"},
    )

    class _NoSizes:
        """A third-party backend predating `list_sizes`."""

        def list_keys(self, prefix=""):
            return [f"{prefix}/tapdb.dump"]

        def describe(self):
            return {"backend": "legacy"}

    monkey_manifest = service.BackupManifest(
        backup_id="full-1",
        backup_class="full",
        included_assets=[
            service.AssetRef(name="tapdb.dump", bytes=999, sha256="ab" * 32)
        ],
    )

    import unittest.mock as mock

    with mock.patch.object(service, "_load_manifest", return_value=monkey_manifest):
        checks = service._health_storage_checks(
            {
                "client_id": "a",
                "database_name": "b",
                "schema_name": "c",
                "database": "d",
            },
            {"config_dir": str(tmp_path), "storage_uri": "file://x"},
            _NoSizes(),
            listing,
            1,
        )

    by_id = {c.id: c for c in checks}
    # Present but unmeasurable: not a failure, and not silently "verified".
    assert by_id["health.hollow_backup"].status == "pass"
    assert by_id["health.hollow_backup"].data.get("hollow", []) == []


def test_a_failed_reconcile_does_not_clear_the_interrupted_prune_warning(target):
    """An outcome that still lists work is not a resolution.

    Reconciliation writes an outcome even when it could not finish, so keying
    only on the phase retired the "go and look" signal while the prefix was
    still half-deleted -- and nothing raises it again.
    """
    _add_backup(target, backup_id="snp-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(
        target,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={"phase": "intent", "prune_id": "p-half"},
    )
    _receipt(
        target,
        operation="backup_prune",
        status=STATUS_FAILED,
        detail={
            "phase": "outcome",
            "prune_id": "p-half",
            "reconciled": True,
            "finished": [],
            "remaining": ["acme/prod/full/stuck"],
        },
    )

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.interrupted_prune") == "warn"
    assert exit_code == EXIT_OK


def test_health_reads_prunes_own_receipt_vocabulary(target):
    """Both sides must not spell the constants independently.

    Health and prune each hardcoded `"backup_prune"` / `"intent"` /
    `"outcome"`, and so did both test suites -- so a rename would have left
    everything green with the detector dead.
    """
    from daylily_tapdb.backup import receipts as receipts_mod

    _add_backup(target, backup_id="snp-20260801T000000Z-aaaaaa")
    _receipt(target, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(
        target,
        operation=receipts_mod.OPERATION_PRUNE,
        status=STATUS_SUCCEEDED,
        detail={
            "phase": receipts_mod.PRUNE_PHASE_INTENT,
            "prune_id": "p-vocab",
        },
    )

    exit_code, payload = _run(target)

    assert _status_of(payload, "health.interrupted_prune") == "warn"
    assert exit_code == EXIT_OK
