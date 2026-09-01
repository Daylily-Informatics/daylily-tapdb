"""Receipt write path: tail-only reads, exclusive sequence claiming, races."""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from daylily_tapdb.backup import receipts as receipts_mod
from daylily_tapdb.backup.receipts import (
    OPERATION_CREATE,
    STATUS_SUCCEEDED,
    Actor,
    Receipt,
    last_receipt,
    read_receipts,
    verify_receipt_chain,
    write_receipt,
)

BASE = datetime(2026, 7, 27, 12, 0, 0, tzinfo=UTC)


def _write(directory: Path, index: int = 0):
    return write_receipt(
        directory,
        operation=OPERATION_CREATE,
        status=STATUS_SUCCEEDED,
        actor=Actor(surface="cli", username="alice"),
        backup_id=f"full-{index}",
        now=BASE + timedelta(seconds=index),
    )


def test_filename_is_the_sequence_number_only(tmp_path: Path):
    receipt = _write(tmp_path, 0)

    assert receipt.path is not None
    assert receipt.path.name == "000001.json"


def test_last_receipt_returns_the_newest(tmp_path: Path):
    for index in range(4):
        _write(tmp_path, index)

    tail = last_receipt(tmp_path)

    assert tail is not None
    assert tail.sequence == 4
    assert tail.backup_id == "full-3"


def test_last_receipt_is_none_on_an_empty_directory(tmp_path: Path):
    assert last_receipt(tmp_path) is None
    assert last_receipt(tmp_path / "absent") is None


def test_write_parses_only_the_tail_not_every_receipt(tmp_path: Path, monkeypatch):
    for index in range(6):
        _write(tmp_path, index)

    parsed: list[str] = []
    real_parse = receipts_mod._try_parse

    def _counting_parse(path: Path):
        parsed.append(path.name)
        return real_parse(path)

    monkeypatch.setattr(receipts_mod, "_try_parse", _counting_parse)
    _write(tmp_path, 6)

    # The whole point: appending must not re-parse the history.
    assert parsed == ["000006.json"]


def test_sequence_ordering_survives_padding_overflow(tmp_path: Path):
    # Past 999999 the zero padding overflows, and raw string sorting would put
    # "1000000" before "999999". Sorting on the parsed integer must not.
    for name in ("000999.json", "999999.json", "1000000.json"):
        (tmp_path / name).write_text(
            json.dumps(
                {
                    "sequence": int(Path(name).stem),
                    "receipt_id": Path(name).stem,
                    "created_at": BASE.isoformat(),
                    "operation": OPERATION_CREATE,
                    "status": STATUS_SUCCEEDED,
                    "actor": {"surface": "cli", "username": "alice"},
                }
            )
        )

    tail = last_receipt(tmp_path)

    assert tail is not None
    assert tail.sequence == 1000000


def test_a_corrupt_tail_falls_back_to_the_previous_receipt(tmp_path: Path):
    _write(tmp_path, 0)
    _write(tmp_path, 1)
    victim = tmp_path / "000002.json"
    os.chmod(victim, 0o600)
    victim.write_text("not json")

    tail = last_receipt(tmp_path)

    assert tail is not None
    assert tail.sequence == 1


def test_a_corrupt_receipt_does_not_wedge_the_write_path(tmp_path: Path):
    # A damaged receipt must not stop the system from recording new backups.
    _write(tmp_path, 0)
    _write(tmp_path, 1)
    victim = tmp_path / "000002.json"
    os.chmod(victim, 0o600)
    victim.write_text("not json")

    receipt = _write(tmp_path, 2)

    assert receipt.sequence == 3  # the damaged number is skipped, not reused
    assert victim.read_text() == "not json"  # and never overwritten


def test_a_corrupt_receipt_surfaces_as_a_chain_finding(tmp_path: Path):
    # Skipping the number keeps writes working; the damage must still show up.
    _write(tmp_path, 0)
    _write(tmp_path, 1)
    victim = tmp_path / "000002.json"
    os.chmod(victim, 0o600)
    victim.write_text("not json")
    _write(tmp_path, 2)

    verification = verify_receipt_chain(read_receipts(tmp_path))

    assert not verification.ok
    assert any("expected sequence" in finding for finding in verification.findings)


def test_concurrent_writers_never_share_a_sequence(tmp_path: Path):
    workers = 16

    with ThreadPoolExecutor(max_workers=workers) as pool:
        written = list(pool.map(lambda i: _write(tmp_path, i), range(workers)))

    sequences = sorted(receipt.sequence for receipt in written)

    assert sequences == list(range(1, workers + 1))
    assert len({r.path for r in written}) == workers


def test_concurrent_writers_leave_a_verifiable_chain(tmp_path: Path):
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: _write(tmp_path, i), range(8)))

    stored = read_receipts(tmp_path)
    verification = verify_receipt_chain(stored)

    assert verification.count == 8
    assert verification.ok, verification.findings


def test_concurrent_writers_leave_no_temp_files(tmp_path: Path):
    from daylily_tapdb.backup.receipts import HEAD_FILENAME

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: _write(tmp_path, i), range(8)))

    leftovers = [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
    assert leftovers == [], leftovers
    # Eight racing writers must leave exactly one anchor, not eight.
    anchors = [p.name for p in tmp_path.iterdir() if p.name == HEAD_FILENAME]
    assert anchors == [HEAD_FILENAME], anchors


def test_a_taken_sequence_is_retried_not_clobbered(tmp_path: Path):
    first = _write(tmp_path, 0)
    original = first.path.read_bytes()

    second = _write(tmp_path, 1)

    assert second.sequence == 2
    # The loser of a race must never overwrite the winner's receipt.
    assert first.path.read_bytes() == original


def test_write_gives_up_after_bounded_retries(tmp_path: Path, monkeypatch):
    # Simulate a writer that always loses the race for the next sequence.
    def _always_taken(src, dst):
        raise FileExistsError(dst)

    monkeypatch.setattr(receipts_mod.os, "link", _always_taken)

    with pytest.raises(RuntimeError, match="Could not claim a receipt sequence"):
        _write(tmp_path, 0)


def test_published_receipts_are_read_only_after_a_race(tmp_path: Path):
    with ThreadPoolExecutor(max_workers=4) as pool:
        written = list(pool.map(lambda i: _write(tmp_path, i), range(4)))

    import stat

    # Pin the count so the loop below cannot pass by iterating over nothing.
    assert len(written) == 4

    for receipt in written:
        assert stat.S_IMODE(os.stat(receipt.path).st_mode) == 0o400


def test_status_derivation_stops_at_the_latest_success(tmp_path: Path):
    # Only the two most recent create receipts can change the answer.
    history = [
        Receipt(
            sequence=index + 1,
            receipt_id=f"{index:06d}",
            created_at=(BASE + timedelta(hours=index)).isoformat(),
            operation=OPERATION_CREATE,
            status=STATUS_SUCCEEDED,
            actor=Actor(surface="cli"),
            backup_id=f"full-{index}",
        )
        for index in range(50)
    ]

    block = receipts_mod.derive_backup_status(history, now=BASE + timedelta(hours=50))

    assert block["last_success_backup_id"] == "full-49"
    assert block["receipt_count"] == 50
