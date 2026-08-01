"""Immutable, hash-chained receipts for every backup lifecycle run.

Receipts are files under ``<config_dir>/backups/receipts/``, not database rows:
a restore replaces the database that would otherwise hold the record of that
very restore. Files survive it.

Each receipt records the SHA-256 of its predecessor, so the sequence is
tamper-*evident*: editing or removing any receipt breaks the chain at that
point and ``verify_receipt_chain`` reports where. ``chmod 0o400`` discourages
casual edits but is not the integrity mechanism -- an owner can always chmod
back. The chain is what detects it.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from daylily_tapdb.backup.manifest import canonical_bytes, sha256_hex

RECEIPTS_DIRNAME = "receipts"

OPERATION_CREATE = "backup_create"
OPERATION_RESTORE = "backup_restore"
OPERATION_REHEARSE = "backup_rehearse"
OPERATION_VERIFY = "backup_verify"
#: Retention. Uniquely among operations this writes *two* receipts per applied
#: run -- an `intent` before the first delete and an `outcome` after -- linked
#: by `prune_id`. Every other mutating operation is self-cleaning on failure;
#: a prune interrupted after three of ten deletions is not, and an intent with
#: no matching outcome is precisely the interrupted-prune detector that
#: `health.interrupted_prune` reads.
OPERATION_PRUNE = "backup_prune"

#: `detail["phase"]` values on a prune receipt.
PRUNE_PHASE_INTENT = "intent"
PRUNE_PHASE_OUTCOME = "outcome"

STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"

SURFACE_CLI = "cli"
SURFACE_API = "api"
SURFACE_GUI = "gui"

#: Backup status values surfaced by the GUI status page and the API.
BACKUP_STATUS_OK = "ok"
BACKUP_STATUS_STALE = "stale"
BACKUP_STATUS_FAILING = "failing"
BACKUP_STATUS_NEVER_RUN = "never_run"

#: Bounded retries when racing another writer for the next sequence number.
#: Every attempt re-reads the tail, and each round at least one writer wins, so
#: a writer needs at most one attempt per competing writer. This bound is set
#: well above any plausible concurrency purely as a livelock backstop -- it is
#: not a throttle, and attempts are cheap (one listdir plus one parse).
_MAX_SEQUENCE_ATTEMPTS = 64


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _stamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class Actor:
    """Who initiated a lifecycle run, and through which surface."""

    surface: str
    username: Optional[str] = None

    def to_payload(self) -> dict[str, Any]:
        return {"surface": self.surface, "username": self.username}

    @classmethod
    def from_payload(cls, payload: Optional[dict[str, Any]]) -> "Actor":
        data = payload or {}
        return cls(
            surface=str(data.get("surface") or "unknown"),
            username=data.get("username"),
        )


@dataclass
class Receipt:
    """One immutable record of a lifecycle run."""

    sequence: int
    receipt_id: str
    created_at: str
    operation: str
    status: str
    actor: Actor
    backup_id: Optional[str] = None
    backup_class: Optional[str] = None
    target_label: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)
    receipt_mirror: dict[str, Any] = field(default_factory=dict)
    prev_receipt_sha256: Optional[str] = None

    #: Populated on read; not part of the hashed payload.
    path: Optional[Path] = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "receipt_id": self.receipt_id,
            "created_at": self.created_at,
            "operation": self.operation,
            "status": self.status,
            "actor": self.actor.to_payload(),
            "backup_id": self.backup_id,
            "backup_class": self.backup_class,
            "target_label": self.target_label,
            "detail": self.detail,
            "receipt_mirror": self.receipt_mirror,
            "prev_receipt_sha256": self.prev_receipt_sha256,
        }

    def checksum(self) -> str:
        """Return this receipt's SHA-256, which its successor records."""
        return sha256_hex(canonical_bytes(self.to_payload()))

    @property
    def succeeded(self) -> bool:
        return self.status == STATUS_SUCCEEDED

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, Any],
        *,
        path: Optional[Path] = None,
    ) -> "Receipt":
        return cls(
            sequence=int(payload["sequence"]),
            receipt_id=str(payload["receipt_id"]),
            created_at=str(payload["created_at"]),
            operation=str(payload["operation"]),
            status=str(payload["status"]),
            actor=Actor.from_payload(payload.get("actor")),
            backup_id=payload.get("backup_id"),
            backup_class=payload.get("backup_class"),
            target_label=payload.get("target_label"),
            detail=dict(payload.get("detail") or {}),
            receipt_mirror=dict(payload.get("receipt_mirror") or {}),
            prev_receipt_sha256=payload.get("prev_receipt_sha256"),
            path=path,
        )


def receipts_dir(config_dir: Path) -> Path:
    """Return the receipts directory for a config directory."""
    return Path(config_dir).expanduser() / "backups" / RECEIPTS_DIRNAME


def _sequence_from_name(path: Path) -> int:
    """Return the sequence encoded in a receipt filename.

    Sorting on the parsed integer rather than the raw string matters past
    sequence 999999, where zero-padding overflows and ``"1000000"`` sorts
    *before* ``"999999"`` lexicographically.
    """
    try:
        return int(path.stem)
    except ValueError:
        return -1


def _try_parse(path: Path) -> Optional[Receipt]:
    """Parse one receipt file, or return None if it is not a valid receipt."""
    try:
        payload = json.loads(path.read_bytes().decode("utf-8"))
        return Receipt.from_payload(payload, path=path)
    except (ValueError, KeyError, TypeError, OSError):
        return None


def _receipt_paths(directory: Path) -> list[Path]:
    """Return receipt file paths in sequence order."""
    base = Path(directory)
    if not base.is_dir():
        return []
    return sorted(base.glob("*.json"), key=_sequence_from_name)


def read_receipts(directory: Path) -> list[Receipt]:
    """Read every receipt in order. Unparseable files are skipped.

    This walks the whole directory, which is what chain verification and
    listing genuinely need. The write path deliberately does not use it -- see
    ``last_receipt``.
    """
    receipts = [
        receipt
        for receipt in (_try_parse(path) for path in _receipt_paths(directory))
        if receipt is not None
    ]
    receipts.sort(key=lambda item: item.sequence)
    return receipts


def _next_sequence(directory: Path) -> int:
    """Return the next unclaimed sequence number, from filenames alone.

    Unparseable files still occupy their number, so this looks at names rather
    than content -- a damaged receipt costs one skipped sequence instead of
    wedging the write path.
    """
    highest = max(
        (_sequence_from_name(path) for path in _receipt_paths(directory)),
        default=0,
    )
    return max(highest, 0) + 1


def last_receipt(directory: Path) -> Optional[Receipt]:
    """Return the newest receipt, parsing only that one file.

    A chained write needs exactly two facts -- the last sequence number and the
    last checksum -- and both live in a single file. Reading only the tail
    keeps appending a receipt O(1) in parses rather than O(n), so a long-lived
    receipt directory does not make every backup progressively slower.
    """
    for path in reversed(_receipt_paths(directory)):
        receipt = _try_parse(path)
        if receipt is not None:
            return receipt
    return None


def write_receipt(
    directory: Path,
    *,
    operation: str,
    status: str,
    actor: Actor,
    backup_id: Optional[str] = None,
    backup_class: Optional[str] = None,
    target_label: Optional[str] = None,
    detail: Optional[dict[str, Any]] = None,
    receipt_mirror: Optional[dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Receipt:
    """Append one immutable receipt, chained to its predecessor.

    Concurrency is handled by the filesystem rather than a lock. The filename
    is the zero-padded sequence number and nothing else, so the sequence *is*
    the exclusive resource: ``os.link`` publishes the receipt atomically and
    fails with ``FileExistsError`` if another writer already claimed that
    number, in which case this writer re-reads the tail and retries. That makes
    duplicate sequences impossible rather than merely detectable.

    ``os.link`` -- not ``os.replace`` -- is the primitive that makes this work.
    ``os.replace`` overwrites unconditionally and so cannot detect the race.
    (``os.link`` is reliable on local filesystems; the receipts directory lives
    under the local config directory. It is historically unreliable on NFS.)

    ``receipt_mirror`` is *recorded* in the receipt, never acted on -- shipping
    receipts to a remote mirror is standing-infrastructure work owned by the
    dayhoff CDK companion issue. Nothing here imports an AWS SDK.
    """
    base = Path(directory)
    base.mkdir(parents=True, exist_ok=True)

    moment = now or _utcnow()
    stamp = _stamp(moment)

    for _ in range(_MAX_SEQUENCE_ATTEMPTS):
        # The next number comes from filenames, the chain link from the last
        # *parseable* receipt. Deriving both from parsed content would let one
        # corrupt file block every future write: its number would be recomputed
        # forever while os.link kept refusing the occupied name. Skipping the
        # number instead leaves the damage visible to verify_receipt_chain
        # rather than silently blocking backups.
        sequence = _next_sequence(base)
        previous = last_receipt(base)
        prev_hash = previous.checksum() if previous else None

        receipt = Receipt(
            sequence=sequence,
            receipt_id=f"{sequence:06d}-{stamp}",
            created_at=moment.astimezone(UTC).isoformat(),
            operation=operation,
            status=status,
            actor=actor,
            backup_id=backup_id,
            backup_class=backup_class,
            target_label=target_label,
            detail=dict(detail or {}),
            receipt_mirror=dict(receipt_mirror or {}),
            prev_receipt_sha256=prev_hash,
        )

        target = base / f"{sequence:06d}.json"
        # mkstemp guarantees a name unique across both processes and threads.
        # Deriving the temp name from the sequence would collide between
        # threads racing for the same number, and the loser's cleanup would
        # delete the winner's staged bytes out from under it.
        handle, tmp_name = tempfile.mkstemp(dir=base, prefix=".receipt-", suffix=".tmp")
        tmp = Path(tmp_name)
        try:
            with os.fdopen(handle, "wb") as stream:
                stream.write(canonical_bytes(receipt.to_payload()))
            try:
                # Atomic publish that fails rather than clobbers.
                os.link(tmp, target)
            except FileExistsError:
                continue
        finally:
            tmp.unlink(missing_ok=True)

        # Read-only once published: the content is final at this point.
        os.chmod(target, 0o400)
        receipt.path = target
        _write_head(base, receipt)
        # After the local anchor, not before: at the chmod above the local head
        # still records sequence N-1, so mirroring there would publish a stale
        # anchor -- or, computed from `receipt` directly, one running ahead of
        # local, since `_write_head` swallows its own failures.
        mirror_receipt(receipt, receipt_mirror or {})
        return receipt

    raise RuntimeError(
        f"Could not claim a receipt sequence in {directory} after "
        f"{_MAX_SEQUENCE_ATTEMPTS} attempts. This should be unreachable at any "
        "realistic level of concurrency; suspect a stuck writer or a "
        "filesystem where os.link does not fail on an existing target."
    )


# ---------------------------------------------------------------------------
# Receipt mirroring
#
# A restore wipes the database that would otherwise hold the audit trail, and
# receipts live next to the config on one host. Losing that host loses the
# record of every backup and restore ever performed. The mirror is a second
# copy somewhere that survives the host.
#
# Three properties, each of which the obvious implementation gets wrong:
#
# 1. **Follower, never allocator.** Sequence claiming depends on ``os.link``
#    failing on an existing target; S3 has no such primitive. The mirror copies
#    what the local writer already committed and never decides a sequence.
# 2. **Best-effort, never raises.** A receipt that published successfully must
#    not be reported as failed because a bucket was unreachable.
# 3. **Bounded.** "Never raises" is not "never costs". Without a circuit
#    breaker an unreachable mirror pays a full connect timeout on *every*
#    receipt -- create, verify, restore -- silently multiplying the wall time
#    of every backup with no signal that it is happening.
#
# Write-only in v1: `verify_receipt_chain` and `read_head` stay local-only, so
# the mirror is evidence a human or auditor reads, not a path the code falls
# back to. `health.receipt_mirror` is what notices it has fallen behind.
# ---------------------------------------------------------------------------

#: Consecutive mirror failures after which this process stops trying. Reset on
#: any success. Process-lifetime only -- deliberately not persisted, so a
#: restart always re-attempts.
_MIRROR_FAILURE_LIMIT = 3

_mirror_failures: dict[str, int] = {}


def _mirror_backend(uri: str) -> Any:
    """Build the mirror storage backend.

    Imported here rather than at module scope. ``backup/__init__.py`` already
    imports ``storage``, so there is no cycle either way, but keeping it local
    preserves this module's property of pulling in nothing heavier than
    ``manifest`` when a caller only wants to read receipts.
    """
    from daylily_tapdb.backup.storage import build_storage_backend

    return build_storage_backend(uri)


def mirror_receipt(receipt: "Receipt", mirror: dict[str, Any]) -> bool:
    """Copy one published receipt, and the head anchor, to the mirror.

    Returns whether the mirror was written, for tests and for health. Never
    raises.

    The anchor goes *after* the receipt and only if the receipt landed. A head
    that runs ahead of the receipts it describes would make the mirror look
    truncated; a mirror with no head at all can be silently truncated and still
    look complete, which is worse than having no mirror -- it is a false
    audit trail.
    """
    uri = str((mirror or {}).get("uri") or "").strip()
    if not uri:
        return False
    if _mirror_failures.get(uri, 0) >= _MIRROR_FAILURE_LIMIT:
        return False

    try:
        backend = _mirror_backend(uri)
        backend.put_bytes(
            f"{receipt.sequence:06d}.json", canonical_bytes(receipt.to_payload())
        )
        # Never move the anchor backwards. Two concurrent writers claim
        # sequences 5 and 6; if 6 mirrors first and 5 then overwrites the
        # anchor, the mirror holds 000006.json while head says 5 -- which
        # `health.receipt_mirror` reads as "receipts are not reaching the
        # mirror" forever, and which would make a mirror genuinely truncated
        # to 5 look consistent. Read-then-write is not atomic, but the failure
        # it leaves is a stale anchor that the next receipt corrects, rather
        # than a permanently wrong one.
        try:
            current = json.loads(backend.get_bytes("head.json").decode("utf-8"))
            recorded = int(current.get("sequence") or 0)
        except Exception:
            recorded = 0
        if receipt.sequence >= recorded:
            backend.put_bytes(
                "head.json",
                canonical_bytes(
                    {
                        "sequence": receipt.sequence,
                        "receipt_id": receipt.receipt_id,
                        "sha256": receipt.checksum(),
                    }
                ),
            )
    except Exception:
        _mirror_failures[uri] = _mirror_failures.get(uri, 0) + 1
        return False

    _mirror_failures.pop(uri, None)
    return True


@dataclass(frozen=True)
class ChainVerification:
    """Result of walking the receipt hash chain."""

    ok: bool
    count: int
    findings: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {"ok": self.ok, "count": self.count, "findings": self.findings}


#: Records the newest receipt ever written. Without it the chain is only
#: verifiable *forwards*: 1..N-k is a perfectly valid chain, so deleting the
#: newest receipts leaves `ok == True` and can flip the status page from
#: `failing` to `ok`. Truncation is not inherent to hash chaining the way
#: editing the tip is -- the sequence numbers already exist, they just were
#: not anchored anywhere.
HEAD_FILENAME = ".head"


def _write_head(directory: Path, receipt: Receipt) -> None:
    """Record the newest receipt, atomically and best-effort.

    Never raises: a receipt that was published successfully must not be
    reported as failed because the anchor could not be updated. A missing or
    stale anchor degrades verification to a warning, which is the honest
    outcome -- silently losing the receipt would be worse.
    """
    try:
        payload = canonical_bytes(
            {
                "sequence": receipt.sequence,
                "receipt_id": receipt.receipt_id,
                "sha256": receipt.checksum(),
            }
        )
        handle, tmp_name = tempfile.mkstemp(
            dir=directory, prefix=".head-", suffix=".tmp"
        )
        tmp = Path(tmp_name)
        try:
            with os.fdopen(handle, "wb") as stream:
                stream.write(payload)
            os.replace(tmp, directory / HEAD_FILENAME)
        finally:
            tmp.unlink(missing_ok=True)
    except Exception:  # pragma: no cover - anchor is advisory, never fatal
        pass


def read_head(directory: Path) -> Optional[dict[str, Any]]:
    """Return the recorded newest-receipt anchor, or None if absent."""
    path = Path(directory) / HEAD_FILENAME
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def verify_receipt_chain(
    receipts: list[Receipt],
    *,
    head: Optional[dict[str, Any]] = None,
) -> ChainVerification:
    """Verify hash-chain continuity across an ordered receipt list.

    Pass ``head`` (from ``read_head``) to also detect **truncation**. Without
    it this walks forwards only, and a truncated chain is indistinguishable
    from a shorter honest one.
    """
    findings: list[str] = []
    previous: Optional[Receipt] = None

    for index, receipt in enumerate(receipts):
        expected_sequence = index + 1
        if receipt.sequence != expected_sequence:
            findings.append(
                f"{receipt.receipt_id}: expected sequence {expected_sequence}, "
                f"found {receipt.sequence} (a receipt may have been removed)"
            )
        if previous is None:
            if receipt.prev_receipt_sha256 is not None:
                findings.append(
                    f"{receipt.receipt_id}: first receipt must not chain to a "
                    "predecessor"
                )
        else:
            expected_hash = previous.checksum()
            if receipt.prev_receipt_sha256 != expected_hash:
                findings.append(
                    f"{receipt.receipt_id}: prev_receipt_sha256 does not match "
                    f"{previous.receipt_id} (tampering or truncation)"
                )
        previous = receipt

    if head:
        recorded = int(head.get("sequence") or 0)
        newest = receipts[-1].sequence if receipts else 0
        if newest < recorded:
            findings.append(
                f"recorded head is sequence {recorded} ({head.get('receipt_id')}) "
                f"but the newest receipt on disk is {newest or 'none'} -- "
                f"{recorded - newest} receipt(s) removed from the end"
            )
        elif receipts and newest == recorded:
            expected = str(head.get("sha256") or "")
            if expected and receipts[-1].checksum() != expected:
                findings.append(
                    f"{receipts[-1].receipt_id}: newest receipt does not match the "
                    "recorded head (the tip was edited)"
                )

    return ChainVerification(ok=not findings, count=len(receipts), findings=findings)


def _parse_created_at(value: str) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def derive_backup_status(
    receipts: list[Receipt],
    *,
    expected_interval_hours: float = 0,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Derive the operator-facing status block from receipts.

    ``never_run`` no successful create has ever been recorded.
    ``failing``   the most recent create attempt failed.
    ``stale``     the last success is older than the configured cadence.
    ``ok``        otherwise.

    A cadence of 0 means none is configured, so ``stale`` is unreachable and
    the status page says so rather than implying a schedule that does not
    exist.
    """
    moment = now or _utcnow()

    # Scan backwards and stop as soon as both facts are known. The status page
    # renders on every GUI load, and only the two most recent create receipts
    # can affect the answer -- older ones cannot change it.
    last_attempt: Optional[Receipt] = None
    last_success: Optional[Receipt] = None
    for item in reversed(receipts):
        if item.operation != OPERATION_CREATE:
            continue
        if last_attempt is None:
            last_attempt = item
        if item.succeeded:
            last_success = item
            break

    last_success_at = (
        _parse_created_at(last_success.created_at) if last_success else None
    )

    age_hours: Optional[float] = None
    if last_success_at is not None:
        age_hours = (moment - last_success_at).total_seconds() / 3600.0

    if last_success is None:
        status = BACKUP_STATUS_NEVER_RUN
    elif last_attempt is not None and not last_attempt.succeeded:
        status = BACKUP_STATUS_FAILING
    elif (
        expected_interval_hours
        and expected_interval_hours > 0
        and age_hours is not None
        and age_hours > float(expected_interval_hours)
    ):
        status = BACKUP_STATUS_STALE
    else:
        status = BACKUP_STATUS_OK

    return {
        "status": status,
        "cadence_configured": bool(
            expected_interval_hours and expected_interval_hours > 0
        ),
        "expected_interval_hours": expected_interval_hours,
        "last_success_at": last_success.created_at if last_success else None,
        "last_success_backup_id": last_success.backup_id if last_success else None,
        "last_attempt_at": last_attempt.created_at if last_attempt else None,
        "last_attempt_status": last_attempt.status if last_attempt else None,
        "age_hours": None if age_hours is None else round(age_hours, 2),
        "receipt_count": len(receipts),
    }


def next_due_at(
    status_block: dict[str, Any],
    *,
    expected_interval_hours: float,
) -> Optional[str]:
    """Return when the next backup is due, or None when no cadence is set."""
    if not expected_interval_hours or expected_interval_hours <= 0:
        return None
    last = status_block.get("last_success_at")
    if not last:
        return None
    parsed = _parse_created_at(str(last))
    if parsed is None:
        return None
    return (parsed + timedelta(hours=float(expected_interval_hours))).isoformat()


__all__ = [
    "BACKUP_STATUS_FAILING",
    "BACKUP_STATUS_NEVER_RUN",
    "BACKUP_STATUS_OK",
    "BACKUP_STATUS_STALE",
    "OPERATION_CREATE",
    "OPERATION_REHEARSE",
    "OPERATION_RESTORE",
    "OPERATION_PRUNE",
    "PRUNE_PHASE_INTENT",
    "PRUNE_PHASE_OUTCOME",
    "OPERATION_VERIFY",
    "RECEIPTS_DIRNAME",
    "STATUS_FAILED",
    "STATUS_SUCCEEDED",
    "SURFACE_API",
    "SURFACE_CLI",
    "SURFACE_GUI",
    "Actor",
    "ChainVerification",
    "HEAD_FILENAME",
    "Receipt",
    "derive_backup_status",
    "last_receipt",
    "mirror_receipt",
    "next_due_at",
    "read_head",
    "read_receipts",
    "receipts_dir",
    "verify_receipt_chain",
    "write_receipt",
]
