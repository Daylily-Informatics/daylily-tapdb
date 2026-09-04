"""Typed errors for the TAPDB backup and recovery lifecycle.

Every error carries a stable machine-readable ``code``. The CLI maps codes to
exit statuses, canonical GUI JSON maps them to HTTP statuses, and GUI HTML
renders them as operator-facing notices -- so the code, not the message text,
is the contract shared across every presentation.
"""

from __future__ import annotations

from typing import Any, Optional


class BackupError(Exception):
    """Base class for every backup/restore lifecycle failure."""

    code = "backup_error"

    def __init__(
        self,
        message: str,
        *,
        detail: Optional[dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.detail: dict[str, Any] = dict(detail or {})

    def to_payload(self) -> dict[str, Any]:
        """Render the error for JSON output on any surface."""
        return {
            "error": self.code,
            "message": str(self),
            "detail": self.detail,
        }


class BackupNotFoundError(BackupError):
    """No backup artifact matches the requested id or path."""

    code = "backup_not_found"


class BackupVerificationError(BackupError):
    """A backup failed integrity verification.

    Raised for checksum mismatches, an unreadable table of contents, and
    corrupt data blocks. Restore raises this during preflight -- that is,
    strictly before any target mutation.
    """

    code = "backup_verification_failed"


class BackupVersionMismatchError(BackupError):
    """PostgreSQL major-version compatibility gate refused the operation."""

    code = "version_mismatch"


class RestoreConfirmationError(BackupError):
    """The typed confirmation label was missing or did not match the target."""

    code = "confirm_target_mismatch"


class RestoreStageStaleError(BackupError):
    """The staged restore plan no longer matches current state.

    ``plan_restore`` returns a ``plan_fingerprint``; apply re-stages and
    compares. A mismatch means the backup, target, or options changed between
    staging and applying, so the operator confirmed something other than what
    would now happen.
    """

    code = "stale_stage"


class BackupPolicyBlockedError(BackupError):
    """Configured safety policy forbids the requested destructive operation."""

    code = "destructive_operations_blocked"


__all__ = [
    "BackupError",
    "BackupNotFoundError",
    "BackupPolicyBlockedError",
    "BackupVerificationError",
    "BackupVersionMismatchError",
    "RestoreConfirmationError",
    "RestoreStageStaleError",
]
