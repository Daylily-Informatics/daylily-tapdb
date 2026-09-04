"""Canonical GUI management-API adapter for the backup lifecycle.

Two jobs only: parse requests, and map the service's typed errors onto HTTP
status codes. All behaviour lives in ``daylily_tapdb.backup`` -- if this module
ever grows a check or a `pg_dump`, the API has started to diverge from the CLI
and the GUI.

The status mapping is the interesting part, because the codes carry meaning to
a client:

* **403** the policy forbids the operation outright -- retrying will not help
* **409** the request conflicts with current state (wrong label, stale stage,
  incompatible version) -- re-stage and try again
* **422** the artifact itself failed verification -- and, for restore, this is
  raised *before* the target is touched
* **404** no such backup
"""

from __future__ import annotations

import re
from typing import Any, Optional

from fastapi import HTTPException

from daylily_tapdb.backup import service, verify, views
from daylily_tapdb.backup.errors import (
    BackupError,
    BackupNotFoundError,
    BackupPolicyBlockedError,
    BackupVerificationError,
    BackupVersionMismatchError,
    RestoreConfirmationError,
    RestoreStageStaleError,
)
from daylily_tapdb.backup.manifest import BACKUP_CLASSES
from daylily_tapdb.backup.receipts import SURFACE_API, Actor

#: Backup references appear in URL paths. Constrain them rather than trusting
#: the router: a reference is an identifier we minted, never free text.
#:
#: At least one alphanumeric character is required, which rejects ``.``, ``..``
#: and other pure-punctuation refs. Nothing downstream joins a ref into a
#: filesystem path -- lookup compares against discovered prefixes -- but
#: relying on that property instead of rejecting nonsense at the boundary is
#: the wrong place to put the guarantee.
REF_PATTERN = re.compile(r"^(?=.*[A-Za-z0-9])[A-Za-z0-9._-]+$")

#: Service error -> HTTP status. Anything unmapped is a 500, which is correct:
#: an error we did not anticipate is not a client's fault.
ERROR_STATUS: dict[type[BackupError], int] = {
    BackupNotFoundError: 404,
    BackupPolicyBlockedError: 403,
    RestoreConfirmationError: 409,
    RestoreStageStaleError: 409,
    BackupVersionMismatchError: 409,
    BackupVerificationError: 422,
}


def validate_ref(ref: str) -> str:
    """Validate a backup reference from a URL path."""
    try:
        return views.validate_backup_ref(ref)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_backup_ref", "message": str(exc)},
        ) from exc


def validate_class(value: Optional[str]) -> Optional[str]:
    """Validate an optional backup class from a query or body field."""
    if value is None or value == "":
        return None
    normalized = str(value).strip().lower()
    if normalized not in BACKUP_CLASSES:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_backup_class",
                "message": f"Expected one of: {', '.join(BACKUP_CLASSES)}",
            },
        )
    return normalized


def http_error(exc: BackupError) -> HTTPException:
    """Translate a typed service error into an HTTPException."""
    status = ERROR_STATUS.get(type(exc), 500)
    return HTTPException(status_code=status, detail=exc.to_payload())


def as_http(exc: Exception) -> HTTPException:
    """Translate any exception, typed or not, into an HTTPException."""
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, BackupError):
        return http_error(exc)
    return HTTPException(
        status_code=500,
        detail={"error": "backup_error", "message": str(exc)},
    )


def api_actor(request: Any) -> Actor:
    """Build the receipt actor for an API caller."""
    user = getattr(getattr(request, "state", None), "user", None) or {}
    return Actor(
        surface=SURFACE_API,
        username=user.get("email") or user.get("username"),
    )


def restore_options_from(payload: dict[str, Any]) -> verify.RestoreOptions:
    """Build restore options from a request body, rejecting unsafe names."""
    options = verify.RestoreOptions(
        mode=str(payload.get("mode") or verify.MODE_ISOLATED),
        target_database=payload.get("target_database") or None,
        target_schema=payload.get("target_schema") or None,
        allow_identity_mismatch=bool(payload.get("allow_identity_mismatch")),
        allow_unknown_migrations=bool(payload.get("allow_unknown_migrations")),
        allow_unclaimable_prefixes=bool(payload.get("allow_unclaimable_prefixes")),
        keep_superseded=bool(payload.get("keep_superseded")),
    )
    try:
        options.normalized_mode()
        options.validated_target_database()
        options.validated_target_schema()
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_restore_options", "message": str(exc)},
        ) from exc
    return options


# ---------------------------------------------------------------------------
# Handlers -- each is a thin call into the service or the shared views
# ---------------------------------------------------------------------------


def list_payload(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_class: Optional[str] = None,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    """GET /api/admin/backups -- listing, receipts, and the status block."""
    try:
        return views.inventory_context(
            cfg, settings, backup_class=validate_class(backup_class), limit=limit
        )
    except Exception as exc:
        raise as_http(exc) from exc


def status_payload(cfg: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    """GET /api/admin/backups/status."""
    try:
        return views.status_context(cfg, settings)
    except Exception as exc:
        raise as_http(exc) from exc


def health_payload(cfg: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    """GET /api/admin/backups/health -- the alerting contract in HTTP form.

    The same ``service.health_report`` the CLI runs, so the two surfaces cannot
    disagree about whether a target is recoverable. What differs is only how
    the verdict is signalled, because HTTP has no exit code:

    ==================  ============  ===========================================
    CLI exit            HTTP          meaning
    ==================  ============  ===========================================
    ``0``               ``200``       health answered; nothing failing
    ``1``               ``200``       health answered; something is wrong
    ``2``               ``503``       health could not answer
    ==================  ============  ===========================================

    **A failing backup is a successful health report, so it is 200, not 5xx.**
    This is the one decision worth defending. Returning 503 for "backups are
    broken" would put a correctly-functioning detector behind every proxy,
    retry layer and uptime monitor between the caller and this service -- each
    of which treats 5xx as "the service is sick, try again". The finding would
    be retried, rate-limited, and eventually reported as a TapDB outage rather
    than as the backup problem it is. Callers read ``status`` from the body.

    503 is reserved for the state where health genuinely produced no verdict,
    which is the HTTP analogue of exit 2.
    """
    try:
        report = service.health_report(cfg, settings)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "health_unavailable",
                "message": str(exc),
                "status": "unavailable",
            },
        ) from exc

    payload = report.to_payload()
    if report.exit_code == service.HEALTH_UNAVAILABLE:
        # The 503 body carries the *same* keys as the 200 body, plus `error`.
        # FastAPI serialises `detail` as `{"detail": {...}}`, so nesting the
        # report under a key would make a caller following "read `status` from
        # the body" hit a KeyError on precisely the path where the CLI goes to
        # some trouble to stay parseable.
        raise HTTPException(
            status_code=503,
            detail={**payload, "error": "health_unavailable"},
        )
    return payload


def plan_payload(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_class: Optional[str] = None,
    strict: bool = False,
) -> dict[str, Any]:
    """GET /api/admin/backups/plan -- read-only."""
    try:
        plan = service.plan_backup(
            cfg,
            settings,
            backup_class=validate_class(backup_class) or "full",
            strict_drift=strict,
        )
    except Exception as exc:
        raise as_http(exc) from exc
    return plan.to_payload()


def create_payload(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    body: dict[str, Any],
    actor: Actor,
) -> dict[str, Any]:
    """POST /api/admin/backups -- create, returning 201 with the manifest."""
    try:
        result = service.create_backup(
            cfg,
            settings,
            backup_class=validate_class(body.get("backup_class")) or "full",
            allow_drift=bool(body.get("allow_drift")),
            note=body.get("note") or None,
            actor=actor,
            existing_snapshot=body.get("existing_snapshot") or None,
        )
    except Exception as exc:
        raise as_http(exc) from exc
    return result.to_payload()


def verify_payload(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    ref: str,
    level: str = service.VERIFY_DEEP,
    actor: Actor,
) -> dict[str, Any]:
    """POST /api/admin/backups/{ref}/verify -- 422 for a corrupt artifact."""
    backup_id = validate_ref(ref)
    if level not in (service.VERIFY_QUICK, service.VERIFY_DEEP):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_level", "message": "Expected quick or deep"},
        )
    try:
        report = service.verify_backup(
            cfg, settings, backup_id=backup_id, level=level, actor=actor
        )
    except Exception as exc:
        raise as_http(exc) from exc

    payload = report.to_payload()
    if not report.ok:
        # A corrupt backup is an unprocessable *entity*, not a server fault.
        raise HTTPException(
            status_code=422,
            detail={
                "error": "backup_verification_failed",
                "message": "Backup failed verification",
                "report": payload,
            },
        )
    return payload


def stage_payload(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    ref: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    """POST /api/admin/backups/{ref}/restore/stage -- read-only."""
    backup_id = validate_ref(ref)
    options = restore_options_from(body or {})
    try:
        return views.restore_review_context(
            cfg, settings, backup_id=backup_id, options=options
        )
    except Exception as exc:
        raise as_http(exc) from exc


def apply_payload(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    ref: str,
    body: dict[str, Any],
    actor: Actor,
) -> dict[str, Any]:
    """POST /api/admin/backups/{ref}/restore/apply.

    Goes through ``views.apply_restore_from_review`` -- literally the same
    function the GUI form posts to, so the two surfaces cannot enforce
    different rules.
    """
    backup_id = validate_ref(ref)
    payload = body or {}
    options = restore_options_from(payload)

    # ``confirm_target`` is only mandatory when the service will check it --
    # an isolated restore ignores it, so demanding it would 400 a correct
    # request over a field about to be discarded.
    required_fields = ["plan_fingerprint"]
    if verify.confirmation_required(cfg, options.normalized_mode()):
        required_fields.append("confirm_target")

    missing = [field for field in required_fields if not payload.get(field)]
    if missing:
        # A missing field is a malformed request (400), distinct from a field
        # that is present and wrong (409).
        raise HTTPException(
            status_code=400,
            detail={
                "error": "missing_fields",
                "message": f"Required for this restore: {', '.join(required_fields)}",
                "missing": missing,
            },
        )

    try:
        result = views.apply_restore_from_review(
            cfg,
            settings,
            backup_id=backup_id,
            plan_fingerprint=payload.get("plan_fingerprint"),
            confirm_target=payload.get("confirm_target"),
            options=options,
            actor=actor,
        )
    except Exception as exc:
        raise as_http(exc) from exc
    return result.to_payload()


def rehearse_payload(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    ref: str,
    body: dict[str, Any],
    actor: Actor,
) -> dict[str, Any]:
    """POST /api/admin/backups/{ref}/rehearse -- return the evidence pointer."""
    backup_id = validate_ref(ref)
    try:
        evidence = verify.rehearse_restore(
            cfg,
            settings,
            backup_id=backup_id,
            keep=bool((body or {}).get("keep")),
            actor=actor,
        )
    except Exception as exc:
        raise as_http(exc) from exc
    return evidence.to_payload()


__all__ = [
    "ERROR_STATUS",
    "REF_PATTERN",
    "api_actor",
    "apply_payload",
    "as_http",
    "create_payload",
    "health_payload",
    "http_error",
    "list_payload",
    "plan_payload",
    "rehearse_payload",
    "restore_options_from",
    "stage_payload",
    "status_payload",
    "validate_class",
    "validate_ref",
]
