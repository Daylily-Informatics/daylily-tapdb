"""Presentation-neutral context and the shared staged-restore apply flow.

The admin API and the embedded GUI both need the same three things: a status
block, a review context for a staged restore, and a way to apply that restore.
Building them here rather than in each surface is what makes "the API and the
GUI do the same thing" a fact rather than a hope -- ``apply_restore_from_review``
*is* the code path both call, so they cannot diverge in what they check.

Nothing here imports FastAPI, Jinja, or typer. The functions return plain
dictionaries and dataclasses; turning those into JSON or HTML is the surface's
job and the only thing a surface should be doing.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

from daylily_tapdb.backup import service, verify
from daylily_tapdb.backup.errors import RestoreConfirmationError, RestoreStageStaleError
from daylily_tapdb.backup.receipts import (
    Actor,
    derive_backup_status,
    next_due_at,
    read_head,
    read_receipts,
    verify_receipt_chain,
)

#: How many receipts a status block carries. Enough to see recent history
#: without turning a status page into a log viewer.
RECENT_RECEIPTS = 20

#: Backup references arrive in URL paths on both surfaces. At least one
#: alphanumeric character is required, which rejects ``.``, ``..`` and other
#: pure-punctuation refs.
_REF_PATTERN = re.compile(r"^(?=.*[A-Za-z0-9])[A-Za-z0-9._-]+$")


def validate_backup_ref(ref: str) -> str:
    """Validate a backup reference, raising ``ValueError`` if malformed.

    Shared by the API and the GUI so the two cannot disagree about what a
    valid reference is. Each surface translates the ValueError into its own
    idiom -- a 400 for the API, a re-rendered page for the GUI.
    """
    text = str(ref or "").strip()
    if not _REF_PATTERN.match(text):
        raise ValueError(f"Malformed backup reference: {ref!r}")
    return text


def status_context(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Build the operator-facing status block.

    This is the item-10 scheduled-status view: is there a recent backup, did
    the last attempt succeed, and is the receipt chain intact. The chain
    verification belongs here rather than on a separate page -- a status that
    says "healthy" while its own audit trail is broken is worse than no status
    at all.
    """
    receipts_dir = service.receipts_directory(settings)
    receipts = read_receipts(receipts_dir)
    interval = float(settings.get("expected_interval_hours") or 0)
    status = derive_backup_status(receipts, expected_interval_hours=interval, now=now)
    chain = verify_receipt_chain(receipts, head=read_head(receipts_dir))

    return {
        "target_label": service.target_label(cfg),
        "status": status["status"],
        "cadence": {
            "configured": status["cadence_configured"],
            "expected_interval_hours": interval,
            "next_due_at": next_due_at(status, expected_interval_hours=interval),
        },
        "last_success_at": status["last_success_at"],
        "last_success_backup_id": status["last_success_backup_id"],
        "last_attempt_at": status["last_attempt_at"],
        "last_attempt_status": status["last_attempt_status"],
        "age_hours": status["age_hours"],
        "receipt_count": status["receipt_count"],
        "receipt_chain": chain.to_payload(),
        "recent_receipts": [
            receipt.to_payload() for receipt in receipts[-RECENT_RECEIPTS:][::-1]
        ],
        "storage": service.storage_for(settings).describe(),
    }


def inventory_context(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_class: Optional[str] = None,
    limit: Optional[int] = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Build the backups-page context: the listing plus its status block."""
    listing = service.list_backups(
        cfg, settings, backup_class=backup_class, limit=limit
    )
    return {
        "status": status_context(cfg, settings, now=now),
        **listing.to_payload(),
    }


def restore_review_context(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_id: str,
    options: Optional[verify.RestoreOptions] = None,
) -> dict[str, Any]:
    """Stage a restore and build the review context.

    Read-only. The returned ``plan_fingerprint`` and ``required_confirm_target``
    are what the apply step checks against, so a review page renders exactly
    the operation the operator would be authorising.
    """
    plan = verify.plan_restore(
        cfg, settings, backup_id=backup_id, options=options or verify.RestoreOptions()
    )
    payload = plan.to_payload()
    payload["blocking"] = [check.to_payload() for check in plan.blocking]
    payload["target_label"] = service.target_label(cfg)
    return payload


def apply_restore_from_review(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_id: str,
    plan_fingerprint: Optional[str],
    confirm_target: Optional[str],
    options: Optional[verify.RestoreOptions] = None,
    actor: Optional[Actor] = None,
) -> verify.RestoreResult:
    """Apply a staged restore. **The one code path both surfaces use.**

    The order matters and is deliberate:

    1. the fingerprint must be present, and the typed label must be present
       *when the service will actually check it* -- a missing one is a client
       error, not a mismatch, and saying so plainly beats a confusing 409;
    2. the plan is re-staged inside ``restore_backup``, which compares the
       fingerprint and refuses a stale stage;
    3. the typed label is re-checked by the service itself.

    Steps 2 and 3 are *not* performed here even though this function could do
    them, because then a caller reaching ``restore_backup`` directly -- the
    CLI, a script, a future surface -- would bypass them. Defence lives in the
    service; this function only supplies what the service needs.

    The presence check defers to ``verify.confirmation_required`` rather than
    demanding a label unconditionally: an isolated restore never has its label
    checked, so requiring one here would reject a correct request over a field
    that is about to be ignored.
    """
    resolved = options or verify.RestoreOptions()

    if not plan_fingerprint:
        raise RestoreStageStaleError(
            "A staged plan fingerprint is required to apply a restore.",
            detail={"missing": "plan_fingerprint"},
        )
    if not confirm_target and verify.confirmation_required(
        cfg, resolved.normalized_mode()
    ):
        raise RestoreConfirmationError(
            "The typed target label is required to apply a restore.",
            detail={"required_confirm_target": service.target_label(cfg)},
        )

    return verify.restore_backup(
        cfg,
        settings,
        backup_id=backup_id,
        options=resolved,
        confirm_target=confirm_target,
        plan_fingerprint=plan_fingerprint,
        actor=actor,
    )


__all__ = [
    "RECENT_RECEIPTS",
    "apply_restore_from_review",
    "inventory_context",
    "restore_review_context",
    "status_context",
]
