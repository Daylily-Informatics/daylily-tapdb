"""Transaction-scoped PostgreSQL advisory locks with redacted receipts."""

from __future__ import annotations

import hashlib
import math
import time
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class AdvisoryLockReceipt:
    """Non-sensitive evidence for one transaction-scoped lock acquisition."""

    algorithm: str
    lock_fingerprint: str
    acquired: bool
    wait_ms: int
    timeout_ms: int | None

    def to_payload(self) -> dict[str, str | bool | int | None]:
        """Return a stable receipt without raw advisory-lock inputs."""
        return {
            "algorithm": self.algorithm,
            "lock_fingerprint": self.lock_fingerprint,
            "acquired": self.acquired,
            "wait_ms": self.wait_ms,
            "timeout_ms": self.timeout_ms,
        }


class AdvisoryLockTimeoutError(TimeoutError):
    """A bounded advisory-lock attempt reached its deadline."""

    def __init__(self, receipt: AdvisoryLockReceipt) -> None:
        self.receipt = receipt
        self.diagnostic = receipt.to_payload()
        super().__init__(
            "advisory lock "
            f"{receipt.lock_fingerprint[:16]} timed out after "
            f"{receipt.wait_ms} ms (limit {receipt.timeout_ms} ms)"
        )


def _frame_lock_parts(namespace: str, parts: Iterable[object]) -> bytes:
    if any(part is None for part in parts):
        raise ValueError("advisory lock namespace and parts must be non-empty")
    values = [namespace, *(str(part) for part in parts)]
    framed = bytearray(b"tapdb-advisory-lock\x00v1\x00")
    for value in values:
        if not isinstance(value, str) or not value:
            raise ValueError("advisory lock namespace and parts must be non-empty")
        encoded = value.encode("utf-8")
        framed.extend(len(encoded).to_bytes(8, byteorder="big", signed=False))
        framed.extend(encoded)
    return bytes(framed)


def derive_advisory_lock_key(namespace: str, *parts: object) -> int:
    """Derive PostgreSQL's signed-int64 key from framed SHA-256 input."""
    digest = hashlib.sha256(_frame_lock_parts(namespace, parts)).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def advisory_lock_fingerprint(namespace: str, *parts: object) -> str:
    """Return a receipt-safe fingerprint without exposing raw lock inputs."""
    return hashlib.sha256(_frame_lock_parts(namespace, parts)).hexdigest()


def _require_transaction(session: Session) -> None:
    bind = session.get_bind()
    dialect = str(getattr(getattr(bind, "dialect", None), "name", "") or "")
    if dialect != "postgresql":
        raise RuntimeError("transaction advisory locks require PostgreSQL")
    if not session.in_transaction():
        raise RuntimeError("transaction advisory locks require an active transaction")


def acquire_transaction_advisory_lock(
    session: Session,
    namespace: str,
    *parts: object,
    timeout_seconds: float | None = None,
    poll_interval_seconds: float = 0.05,
) -> AdvisoryLockReceipt:
    """Acquire a lock held until the caller's current transaction ends.

    ``timeout_seconds=None`` uses PostgreSQL's blocking lock primitive. A
    finite timeout polls ``pg_try_advisory_xact_lock`` without committing or
    rolling back the caller's transaction.
    """
    _require_transaction(session)
    if timeout_seconds is not None and (
        not math.isfinite(timeout_seconds) or timeout_seconds < 0
    ):
        raise ValueError("timeout_seconds must be finite and non-negative")
    if not math.isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be finite and positive")

    key = derive_advisory_lock_key(namespace, *parts)
    fingerprint = advisory_lock_fingerprint(namespace, *parts)
    started = time.monotonic()
    timeout_ms = None if timeout_seconds is None else int(round(timeout_seconds * 1000))

    if timeout_seconds is None:
        session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": key}
        )
        waited = max(0, int(round((time.monotonic() - started) * 1000)))
        return AdvisoryLockReceipt(
            "sha256-framed-signed-int64-v1", fingerprint, True, waited, None
        )

    deadline = started + timeout_seconds
    while True:
        acquired = bool(
            session.execute(
                text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
                {"lock_key": key},
            ).scalar_one()
        )
        if acquired:
            waited = max(0, int(round((time.monotonic() - started) * 1000)))
            return AdvisoryLockReceipt(
                "sha256-framed-signed-int64-v1",
                fingerprint,
                True,
                waited,
                timeout_ms,
            )
        now = time.monotonic()
        if now >= deadline:
            waited = max(0, int(round((now - started) * 1000)))
            raise AdvisoryLockTimeoutError(
                AdvisoryLockReceipt(
                    "sha256-framed-signed-int64-v1",
                    fingerprint,
                    False,
                    waited,
                    timeout_ms,
                )
            )
        time.sleep(min(poll_interval_seconds, deadline - now))


__all__ = [
    "AdvisoryLockReceipt",
    "AdvisoryLockTimeoutError",
    "acquire_transaction_advisory_lock",
    "advisory_lock_fingerprint",
    "derive_advisory_lock_key",
]
