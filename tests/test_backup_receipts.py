"""Receipts: immutability, hash-chain tamper detection, and status derivation."""

from __future__ import annotations

import json
import os
import stat
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from daylily_tapdb.backup.manifest import canonical_bytes
from daylily_tapdb.backup.receipts import (
    BACKUP_STATUS_FAILING,
    BACKUP_STATUS_NEVER_RUN,
    BACKUP_STATUS_OK,
    BACKUP_STATUS_STALE,
    OPERATION_CREATE,
    OPERATION_RESTORE,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    SURFACE_CLI,
    SURFACE_GUI,
    Actor,
    derive_backup_status,
    next_due_at,
    read_receipts,
    receipts_dir,
    verify_receipt_chain,
    write_receipt,
)

BASE = datetime(2026, 7, 27, 12, 0, 0, tzinfo=UTC)


def _create(directory: Path, *, offset_hours: float = 0, status=STATUS_SUCCEEDED):
    return write_receipt(
        directory,
        operation=OPERATION_CREATE,
        status=status,
        actor=Actor(surface=SURFACE_CLI, username="alice"),
        backup_id=f"full-{int(offset_hours)}",
        backup_class="full",
        target_label="acme/prod/tapdb_prod@tapdb",
        now=BASE + timedelta(hours=offset_hours),
    )


def test_receipts_dir_sits_under_the_config_dir(tmp_path: Path):
    assert receipts_dir(tmp_path) == tmp_path / "backups" / "receipts"


def test_receipt_is_written_read_only(tmp_path: Path):
    receipt = _create(tmp_path)

    assert receipt.path is not None
    mode = stat.S_IMODE(os.stat(receipt.path).st_mode)
    assert mode == 0o400


def test_receipt_write_is_atomic_leaving_no_temp_file(tmp_path: Path):
    from daylily_tapdb.backup.receipts import HEAD_FILENAME

    _create(tmp_path)

    # Assert the actual property -- no staged temp file survived -- rather than
    # "no dotfiles", which was a proxy that also forbade the head anchor.
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
    assert leftovers == [], leftovers
    assert (tmp_path / HEAD_FILENAME).exists(), "head anchor was not written"


def test_first_receipt_has_no_predecessor(tmp_path: Path):
    receipt = _create(tmp_path)

    assert receipt.sequence == 1
    assert receipt.prev_receipt_sha256 is None


def test_receipts_chain_to_their_predecessor(tmp_path: Path):
    first = _create(tmp_path, offset_hours=0)
    second = _create(tmp_path, offset_hours=1)

    assert second.sequence == 2
    assert second.prev_receipt_sha256 == first.checksum()
    assert verify_receipt_chain(read_receipts(tmp_path)).ok


def test_chain_verifies_across_many_receipts(tmp_path: Path):
    for hour in range(5):
        _create(tmp_path, offset_hours=hour)

    verification = verify_receipt_chain(read_receipts(tmp_path))

    assert verification.ok
    assert verification.count == 5
    assert verification.findings == []


def test_tampering_with_a_receipt_breaks_the_chain(tmp_path: Path):
    _create(tmp_path, offset_hours=0)
    _create(tmp_path, offset_hours=1)
    _create(tmp_path, offset_hours=2)

    victim = sorted(tmp_path.glob("*.json"))[0]
    os.chmod(victim, 0o600)  # an owner can always do this; the chain still tells
    payload = json.loads(victim.read_text())
    payload["backup_id"] = "tampered"
    victim.write_text(json.dumps(payload, indent=2, sort_keys=True))

    verification = verify_receipt_chain(read_receipts(tmp_path))

    assert not verification.ok
    assert any("prev_receipt_sha256 does not match" in f for f in verification.findings)


def test_removing_a_receipt_is_detected(tmp_path: Path):
    _create(tmp_path, offset_hours=0)
    _create(tmp_path, offset_hours=1)
    _create(tmp_path, offset_hours=2)

    victim = sorted(tmp_path.glob("*.json"))[1]
    os.chmod(victim, 0o600)
    victim.unlink()

    verification = verify_receipt_chain(read_receipts(tmp_path))

    assert not verification.ok
    assert any("expected sequence" in f for f in verification.findings)


def test_receipt_records_actor_surface_and_username(tmp_path: Path):
    write_receipt(
        tmp_path,
        operation=OPERATION_RESTORE,
        status=STATUS_SUCCEEDED,
        actor=Actor(surface=SURFACE_GUI, username="bob"),
        backup_id="full-1",
        now=BASE,
    )

    receipt = read_receipts(tmp_path)[0]

    assert receipt.actor.surface == SURFACE_GUI
    assert receipt.actor.username == "bob"


def test_a_configured_mirror_receives_the_receipt_and_the_anchor(tmp_path: Path):
    """The mirror is written, byte-identical, with the anchor after it.

    This replaces a test that asserted mirroring was *never* executed. That was
    an accurate description of the old behaviour and is now the bug it would
    have hidden: the config was plumbed end to end, recorded inside the hashed
    payload, and acted on by nothing.
    """
    mirror_dir = tmp_path / "mirror"

    receipt = write_receipt(
        tmp_path / "receipts",
        operation=OPERATION_CREATE,
        status=STATUS_SUCCEEDED,
        actor=Actor(surface=SURFACE_CLI, username="alice"),
        receipt_mirror={"uri": f"file://{mirror_dir}"},
        now=BASE,
    )

    mirrored = mirror_dir / "000001.json"
    assert mirrored.exists(), sorted(p.name for p in mirror_dir.rglob("*"))

    # Byte-identical, not merely "a file appeared". A truncated or re-serialised
    # copy would still satisfy existence and would still be worthless as
    # evidence.
    assert mirrored.read_bytes() == canonical_bytes(receipt.to_payload())

    head = json.loads((mirror_dir / "head.json").read_text())
    assert head["sequence"] == receipt.sequence
    assert head["sha256"] == receipt.checksum()


def test_the_anchor_never_runs_ahead_of_the_receipt(tmp_path: Path):
    """A head describing a receipt the mirror does not have reads as truncated.

    Final state cannot show this: on a local mirror both orderings finish
    identically. The receipt write has to *fail* for the ordering to matter,
    which is exactly the production case -- one object write rejected while the
    next succeeds.
    """
    from daylily_tapdb.backup import receipts as receipts_mod

    mirror_dir = tmp_path / "mirror"
    real = receipts_mod._mirror_backend

    class _RefusesReceipts:
        """Accepts the anchor, rejects the receipt -- a partial outage."""

        def __init__(self, inner):
            self._inner = inner

        def put_bytes(self, key, data):
            if key.endswith(".json") and key != "head.json":
                raise OSError("object write rejected")
            return self._inner.put_bytes(key, data)

        def get_bytes(self, key):
            return self._inner.get_bytes(key)

    receipts_mod._mirror_backend = lambda uri: _RefusesReceipts(real(uri))
    try:
        write_receipt(
            tmp_path / "receipts",
            operation=OPERATION_CREATE,
            status=STATUS_SUCCEEDED,
            actor=Actor(surface=SURFACE_CLI),
            receipt_mirror={"uri": f"file://{mirror_dir}"},
            now=BASE,
        )
    finally:
        receipts_mod._mirror_backend = real

    # No anchor at all is correct. An anchor written before the receipt would
    # describe a receipt the mirror does not hold -- which makes a genuinely
    # truncated mirror look consistent, the one state the anchor exists to
    # expose.
    assert not (mirror_dir / "head.json").exists()

    # And with a working mirror the anchor does land, so this is not passing
    # because mirroring never happens.
    for _ in range(3):
        write_receipt(
            tmp_path / "receipts",
            operation=OPERATION_CREATE,
            status=STATUS_SUCCEEDED,
            actor=Actor(surface=SURFACE_CLI),
            receipt_mirror={"uri": f"file://{mirror_dir}"},
            now=BASE,
        )
    head = json.loads((mirror_dir / "head.json").read_text())
    assert (mirror_dir / f"{head['sequence']:06d}.json").exists()


def test_an_unreachable_mirror_never_fails_the_receipt(tmp_path: Path):
    """A receipt that published locally must not be reported as failed."""
    receipt = write_receipt(
        tmp_path / "receipts",
        operation=OPERATION_CREATE,
        status=STATUS_SUCCEEDED,
        actor=Actor(surface=SURFACE_CLI),
        receipt_mirror={"uri": "s3://"},  # unbuildable on purpose
        now=BASE,
    )

    assert receipt.sequence == 1
    assert (tmp_path / "receipts" / "000001.json").exists()


def test_a_failing_mirror_stops_being_retried(tmp_path: Path):
    """Bounded, not merely non-fatal.

    Without a breaker an unreachable mirror pays a full connect timeout on
    every receipt, silently multiplying the wall time of every backup.
    """
    from daylily_tapdb.backup import receipts as receipts_mod

    attempts = {"n": 0}

    def _explode(uri):
        attempts["n"] += 1
        raise RuntimeError("mirror is down")

    receipts_mod._mirror_failures.clear()
    original = receipts_mod._mirror_backend
    receipts_mod._mirror_backend = _explode
    try:
        for _ in range(10):
            write_receipt(
                tmp_path / "receipts",
                operation=OPERATION_CREATE,
                status=STATUS_SUCCEEDED,
                actor=Actor(surface=SURFACE_CLI),
                receipt_mirror={"uri": "s3://audit/receipts"},
                now=BASE,
            )
    finally:
        receipts_mod._mirror_backend = original
        receipts_mod._mirror_failures.clear()

    assert attempts["n"] == receipts_mod._MIRROR_FAILURE_LIMIT
    assert len(read_receipts(tmp_path / "receipts")) == 10


def test_no_mirror_configured_ships_nothing(tmp_path: Path):
    """The default path must not construct a backend or import an AWS SDK.

    Asserting only "boto3 was not imported" proves the weaker half: without the
    early return, `_mirror_backend("")` raises inside the broad `except` and no
    SDK is imported either. Spying on the constructor is what pins the claim.
    """
    from daylily_tapdb.backup import receipts as receipts_mod

    sys.modules.pop("boto3", None)
    built: list[str] = []
    real = receipts_mod._mirror_backend

    def _spy(uri):
        built.append(uri)
        return real(uri)

    receipts_mod._mirror_backend = _spy
    try:
        write_receipt(
            tmp_path,
            operation=OPERATION_CREATE,
            status=STATUS_SUCCEEDED,
            actor=Actor(surface=SURFACE_CLI, username="alice"),
            now=BASE,
        )
        # Paired with a positive call below, so "zero calls" cannot pass on a
        # spy the code never received.
        assert built == []

        write_receipt(
            tmp_path,
            operation=OPERATION_CREATE,
            status=STATUS_SUCCEEDED,
            actor=Actor(surface=SURFACE_CLI, username="alice"),
            receipt_mirror={"uri": f"file://{tmp_path / 'mirror'}"},
            now=BASE,
        )
        assert len(built) == 1
    finally:
        receipts_mod._mirror_backend = real

    assert "boto3" not in sys.modules


def test_mirroring_leaves_no_temp_files_in_the_receipts_directory(tmp_path: Path):
    """Staging beside the receipts would break the 0o400 and no-.tmp contracts."""
    receipts_dir = tmp_path / "receipts"
    write_receipt(
        receipts_dir,
        operation=OPERATION_CREATE,
        status=STATUS_SUCCEEDED,
        actor=Actor(surface=SURFACE_CLI),
        receipt_mirror={"uri": f"file://{tmp_path / 'mirror'}"},
        now=BASE,
    )

    leftovers = [p.name for p in receipts_dir.iterdir() if p.name.endswith(".tmp")]
    assert leftovers == []


def test_unparseable_files_are_skipped_not_fatal(tmp_path: Path):
    _create(tmp_path)
    (tmp_path / "999999-garbage-backup_create.json").write_text("not json")

    assert len(read_receipts(tmp_path)) == 1


def test_read_receipts_on_missing_directory_is_empty(tmp_path: Path):
    assert read_receipts(tmp_path / "absent") == []


def test_empty_chain_verifies_trivially():
    verification = verify_receipt_chain([])

    assert verification.ok
    assert verification.count == 0


def test_status_never_run_without_any_create(tmp_path: Path):
    block = derive_backup_status([], now=BASE)

    assert block["status"] == BACKUP_STATUS_NEVER_RUN
    assert block["last_success_at"] is None


def test_status_ok_after_a_recent_success(tmp_path: Path):
    _create(tmp_path, offset_hours=0)

    block = derive_backup_status(
        read_receipts(tmp_path),
        expected_interval_hours=24,
        now=BASE + timedelta(hours=1),
    )

    assert block["status"] == BACKUP_STATUS_OK
    assert block["age_hours"] == 1.0


def test_status_stale_when_older_than_the_configured_cadence(tmp_path: Path):
    _create(tmp_path, offset_hours=0)

    block = derive_backup_status(
        read_receipts(tmp_path),
        expected_interval_hours=24,
        now=BASE + timedelta(hours=30),
    )

    assert block["status"] == BACKUP_STATUS_STALE
    assert block["cadence_configured"] is True


def test_status_is_never_stale_when_no_cadence_configured(tmp_path: Path):
    _create(tmp_path, offset_hours=0)

    block = derive_backup_status(
        read_receipts(tmp_path),
        expected_interval_hours=0,
        now=BASE + timedelta(days=400),
    )

    assert block["status"] == BACKUP_STATUS_OK
    assert block["cadence_configured"] is False


def test_status_failing_when_the_latest_attempt_failed(tmp_path: Path):
    _create(tmp_path, offset_hours=0)
    _create(tmp_path, offset_hours=1, status=STATUS_FAILED)

    block = derive_backup_status(
        read_receipts(tmp_path),
        expected_interval_hours=24,
        now=BASE + timedelta(hours=2),
    )

    assert block["status"] == BACKUP_STATUS_FAILING
    assert block["last_attempt_status"] == STATUS_FAILED
    # The earlier success is still reported, so operators know what they have.
    assert block["last_success_backup_id"] == "full-0"


def test_failing_outranks_stale(tmp_path: Path):
    _create(tmp_path, offset_hours=0)
    _create(tmp_path, offset_hours=100, status=STATUS_FAILED)

    block = derive_backup_status(
        read_receipts(tmp_path),
        expected_interval_hours=24,
        now=BASE + timedelta(hours=101),
    )

    assert block["status"] == BACKUP_STATUS_FAILING


def test_restore_receipts_do_not_affect_backup_status(tmp_path: Path):
    write_receipt(
        tmp_path,
        operation=OPERATION_RESTORE,
        status=STATUS_SUCCEEDED,
        actor=Actor(surface=SURFACE_CLI),
        now=BASE,
    )

    block = derive_backup_status(read_receipts(tmp_path), now=BASE)

    assert block["status"] == BACKUP_STATUS_NEVER_RUN
    assert block["receipt_count"] == 1


def test_next_due_at_follows_the_cadence(tmp_path: Path):
    _create(tmp_path, offset_hours=0)
    block = derive_backup_status(
        read_receipts(tmp_path), expected_interval_hours=24, now=BASE
    )

    due = next_due_at(block, expected_interval_hours=24)

    assert due is not None
    assert datetime.fromisoformat(due) == BASE + timedelta(hours=24)


def test_next_due_at_is_none_without_a_cadence(tmp_path: Path):
    _create(tmp_path, offset_hours=0)
    block = derive_backup_status(read_receipts(tmp_path), now=BASE)

    assert next_due_at(block, expected_interval_hours=0) is None


# ---------------------------------------------------------------------------
# truncation
# ---------------------------------------------------------------------------


def _write_some(directory, count=4):
    from daylily_tapdb.backup.receipts import Actor, write_receipt

    return [
        write_receipt(
            directory,
            operation="backup_create",
            status="succeeded",
            actor=Actor(surface="cli", username="t"),
            backup_id=f"b{i}",
        )
        for i in range(count)
    ]


def _chain_of(directory):
    from daylily_tapdb.backup.receipts import (
        read_head,
        read_receipts,
        verify_receipt_chain,
    )

    return verify_receipt_chain(read_receipts(directory), head=read_head(directory))


def test_removing_the_newest_receipts_is_detected(tmp_path):
    """Truncation, which the forward-only walk could never see.

    1..N-k is a perfectly valid chain, so deleting the newest receipts left
    `ok == True` -- and because `derive_backup_status` reads the most recent
    create receipt, dropping a failed one flipped the status page from
    `failing` to `ok`. Someone deleting the receipt for a restore they would
    rather not explain left the audit trail reporting itself intact.

    Unlike editing the tip, this is not inherent to hash chaining: the
    sequence numbers already existed, they were simply not anchored.
    """
    import os

    _write_some(tmp_path, 4)
    assert _chain_of(tmp_path).ok

    newest = sorted(tmp_path.glob("0*.json"))[-1]
    os.chmod(newest, 0o600)
    newest.unlink()

    result = _chain_of(tmp_path)
    assert not result.ok, "truncation went undetected"
    assert any("removed from the end" in f for f in result.findings), result.findings


def test_forging_the_newest_receipt_is_detected(tmp_path):
    """The tip is anchored by hash too, not only by sequence number."""
    import json
    import os

    _write_some(tmp_path, 3)
    newest = sorted(tmp_path.glob("0*.json"))[-1]
    os.chmod(newest, 0o600)
    payload = json.loads(newest.read_text())
    payload["status"] = "failed"
    newest.write_text(json.dumps(payload, indent=2, sort_keys=True))

    result = _chain_of(tmp_path)
    assert not result.ok, "an edited tip went undetected"
    assert any("recorded head" in f for f in result.findings), result.findings


def test_an_intact_chain_still_passes_with_the_anchor(tmp_path):
    """Guards the guard: the anchor must not fail honest chains."""
    _write_some(tmp_path, 5)

    result = _chain_of(tmp_path)

    assert result.ok, result.findings
    assert result.count == 5


def test_a_missing_anchor_does_not_break_verification(tmp_path):
    """The anchor is advisory; losing it degrades, never breaks.

    Older receipt directories have no anchor at all, and a write that could
    not update it must still be a valid receipt.
    """
    import os

    from daylily_tapdb.backup.receipts import HEAD_FILENAME

    _write_some(tmp_path, 3)
    head = tmp_path / HEAD_FILENAME
    os.chmod(head, 0o600)
    head.unlink()

    result = _chain_of(tmp_path)

    assert result.ok, result.findings
