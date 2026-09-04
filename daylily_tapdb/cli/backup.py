"""``tapdb backup`` -- the operator-facing surface of the backup lifecycle.

Every command here is a thin adapter over ``daylily_tapdb.backup``. No command
builds a ``pg_dump`` invocation, reimplements a check, or writes a receipt: the
service does all of that, so the CLI and canonical GUI's HTML/JSON surfaces
cannot drift apart in what they actually do.

Exit codes follow the drift-check precedent:

* ``0`` the operation succeeded
* ``1`` the operation ran and reported findings (a failed check, a refused
  confirmation, a corrupt artifact)
* ``2`` the command could not run at all (no config, unreachable target)

That split matters for automation: ``1`` means "we looked and found a problem",
``2`` means "we never got far enough to look".
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, NoReturn, Optional

import typer
from cli_core_yo import ccyo_out

from daylily_tapdb.backup import service, verify
from daylily_tapdb.backup.errors import BackupError
from daylily_tapdb.backup.manifest import (
    BACKUP_CLASS_FULL,
    BACKUP_CLASS_PROVIDER_SNAPSHOT,
    BACKUP_CLASSES,
)
from daylily_tapdb.backup.receipts import SURFACE_CLI, Actor

backup_app = typer.Typer(help="Backup and recovery lifecycle")

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_ERROR = 2


# ---------------------------------------------------------------------------
# Shared plumbing
# ---------------------------------------------------------------------------


def _json_mode() -> bool:
    """Return whether the framework put this invocation in JSON mode."""
    try:
        from cli_core_yo.runtime import get_context

        return bool(get_context().json_mode)
    except Exception:
        return False


def _dry_run_requested() -> bool:
    """Return the framework-level ``--dry-run`` flag."""
    try:
        from cli_core_yo.runtime import get_context

        return bool(get_context().dry_run)
    except Exception:
        return False


def _resolve() -> tuple[dict[str, Any], dict[str, Any]]:
    """Resolve target config and backup settings, or exit 2.

    A missing or unusable config is an "could not run" condition, not a
    finding -- hence exit 2.
    """
    from daylily_tapdb.cli.db_config import get_backup_settings, get_db_config

    try:
        return get_db_config(), get_backup_settings()
    except Exception as exc:
        _fail(f"Cannot resolve TapDB target configuration: {exc}", code=EXIT_ERROR)
        raise  # pragma: no cover - _fail always raises


def _exit(code: int) -> NoReturn:
    """Leave a command with ``code``, in a way the real entry point preserves.

    **Do not replace this with ``typer.Exit``.** The two are not equivalent
    here, and the difference is invisible under ``CliRunner``:

    ``cli_core_yo.app.run`` -- what ``main()`` calls, and therefore what the
    ``tapdb`` binary does -- invokes the app with ``standalone_mode=False``.
    In that mode click *returns* the code of a ``click.exceptions.Exit``
    (which ``typer.Exit`` is) instead of raising it, and ``run`` discards that
    return value and reports 0. Every non-zero exit is silently lost, so
    ``tapdb backup verify`` on a corrupt archive would tell a monitoring job
    it succeeded.

    A plain ``SystemExit`` is not intercepted by click, so it reaches ``run``'s
    own ``except SystemExit`` handler, which returns ``exc.code`` correctly.

    ``CliRunner`` handles both identically, which is exactly why this needs
    the end-to-end coverage in ``tests/test_backup_cli_exit_codes.py`` rather
    than another ``CliRunner`` assertion.
    """
    raise SystemExit(code)


def _fail(message: str, *, code: int, detail: Optional[dict] = None) -> None:
    if _json_mode():
        ccyo_out.emit_error_json("backup_error", message, detail)
    else:
        ccyo_out.error(message)
    _exit(code)


def _emit(payload: dict[str, Any], *, human: Optional[str] = None) -> None:
    if _json_mode():
        ccyo_out.emit_json(payload)
    elif human:
        ccyo_out.print_text(human)


def _render_checks(checks: list[dict[str, Any]]) -> str:
    symbols = {"pass": "✓", "fail": "✗", "warn": "!", "skip": "-"}
    return "\n".join(
        f"  {symbols.get(check['status'], '?')} {check['id']}: {check['detail']}"
        for check in checks
    )


def _actor() -> Actor:
    import os

    return Actor(surface=SURFACE_CLI, username=os.environ.get("USER") or None)


def _log(operation: str, details: str = "") -> None:
    """Append to the shared CLI operation log, best effort."""
    try:
        from daylily_tapdb.cli.db import _log_operation

        _log_operation("target", operation, details)
    except Exception:  # pragma: no cover - logging must never break a command
        pass


def _handle(exc: Exception) -> None:
    """Translate a service error into the right exit code.

    A ``BackupError`` means the subsystem looked and found a problem -- that is
    a finding (1). Anything else is an operational failure (2).
    """
    if isinstance(exc, BackupError):
        _fail(str(exc), code=EXIT_FINDINGS, detail=exc.to_payload())
    _fail(str(exc), code=EXIT_ERROR)


def _validate_class(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in BACKUP_CLASSES:
        raise typer.BadParameter(f"--class must be one of: {', '.join(BACKUP_CLASSES)}")
    return normalized


# ---------------------------------------------------------------------------
# Read-only commands
# ---------------------------------------------------------------------------


@backup_app.command("plan")
def backup_plan(
    backup_class: str = typer.Option(
        BACKUP_CLASS_FULL, "--class", help="Backup class to plan"
    ),
    strict: bool = typer.Option(
        False, "--strict", help="Treat schema drift as a blocking finding"
    ),
) -> None:
    """Report what a backup would capture. Never writes anything."""
    resolved_class = _validate_class(backup_class)
    cfg, settings = _resolve()
    try:
        plan = service.plan_backup(
            cfg, settings, backup_class=resolved_class, strict_drift=strict
        )
    except Exception as exc:
        _handle(exc)
        return

    payload = plan.to_payload()
    _emit(
        payload,
        human=(
            f"\nBackup plan for [bold]{plan.target_label}[/bold]\n"
            f"  class:   {plan.backup_class}\n"
            f"  storage: {plan.storage.get('uri')}\n"
            f"  tables:  {plan.would_capture.get('table_count', '?')}\n"
            + _render_checks(payload["checks"])
        ),
    )
    _exit(EXIT_OK if plan.ok else EXIT_FINDINGS)


@backup_app.command("list")
def backup_list(
    backup_class: Optional[str] = typer.Option(
        None, "--class", help="Only list this backup class"
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", help="Maximum number of backups to list"
    ),
) -> None:
    """List backups discoverable for this target.

    An empty inventory is a valid answer, not an error -- exit 0.
    """
    resolved_class = _validate_class(backup_class) if backup_class else None
    cfg, settings = _resolve()
    try:
        listing = service.list_backups(
            cfg, settings, backup_class=resolved_class, limit=limit
        )
    except Exception as exc:
        _handle(exc)
        return

    payload = listing.to_payload()
    if _json_mode():
        # Carry the same status block the canonical GUI exposes. Without it
        # the CLI is the one surface that cannot answer "is the backup schedule
        # healthy, and is the receipt chain intact" -- which is exactly what a
        # monitoring job asks. `status_context` is the shared implementation,
        # so the three surfaces cannot disagree about the answer.
        from daylily_tapdb.backup import views

        payload["status"] = views.status_context(cfg, settings)
        ccyo_out.emit_json(payload)
    elif not listing.entries:
        ccyo_out.print_text("No backups found for this target.")
    else:
        lines = [
            f"  {entry.backup_id}  {entry.backup_class:16} "
            f"{entry.created_at or '-'}  {entry.bytes} bytes"
            for entry in listing.entries
        ]
        ccyo_out.print_text("\n".join(lines))

    if listing.damaged and not _json_mode():
        ccyo_out.warning(
            f"{len(listing.damaged)} backup(s) have an unreadable manifest: "
            + ", ".join(listing.damaged)
        )
    _exit(EXIT_OK)


@backup_app.command("health")
def backup_health(
    human: bool = typer.Option(
        False, "--human", help="Print a readable summary instead of JSON"
    ),
) -> None:
    """Report whether this target is recoverable. Exit 0 means it is.

    **JSON on stdout by default, with no ``--json`` flag.** This is deliberate
    and load-bearing. ``--json`` is a *global* option, so it must precede the
    subcommand (``tapdb --json backup health``), but callers that append
    arguments after the subcommand -- Kahlo's ``run_tapdb_cli`` builds
    ``[..., "--config", path] + args`` -- would produce
    ``backup health --json`` and get ``Error: No such option``. Health is a
    machine-first command, so making JSON the default removes the trap
    entirely. ``--human`` opts into prose.

    **Needs no database.** Everything read is receipts, storage and config, so
    health still answers when the database is down -- which is exactly when
    someone is asking. Exit 2 therefore means config or storage could not be
    consulted; it never means "the database is down".

    Exit codes: ``0`` healthy (warnings allowed), ``1`` something is wrong,
    ``2`` health could not reach a verdict.
    """
    cfg, settings = _resolve()
    try:
        report = service.health_report(cfg, settings)
    except Exception as exc:
        # Even total failure emits parseable JSON on stdout. `_fail` would
        # write to stderr unless the global --json flag happened to be set,
        # and a consumer doing json.loads(stdout) would raise inside its own
        # integration rather than report a TapDB problem.
        ccyo_out.emit_json(
            {
                "target_label": service.target_label(cfg),
                "status": "unavailable",
                "exit_code": EXIT_ERROR,
                "ok": False,
                "error": str(exc),
                "checks": [],
            }
        )
        _exit(EXIT_ERROR)
        return

    payload = report.to_payload()
    if human:
        summary = f"{payload['target_label']}: {payload['status']}\n" + _render_checks(
            payload["checks"]
        )
        ccyo_out.print_text(summary)
    else:
        # Emitted directly rather than through `_emit`, which is a no-op unless
        # the global --json flag is set. Going through it would print nothing
        # at all for the default invocation.
        ccyo_out.emit_json(payload)
    _exit(report.exit_code)


@backup_app.command("verify")
def backup_verify(
    backup_id: Optional[str] = typer.Option(
        None, "--backup-id", help="Stored backup to verify"
    ),
    path: Optional[Path] = typer.Option(
        None, "--path", help="Verify a loose archive file instead"
    ),
    level: str = typer.Option(service.VERIFY_DEEP, "--level", help="quick or deep"),
) -> None:
    """Verify a backup's integrity. Reads only; never touches a database."""
    if not backup_id and not path:
        raise typer.BadParameter("Supply either --backup-id or --path")
    if level not in (service.VERIFY_QUICK, service.VERIFY_DEEP):
        raise typer.BadParameter("--level must be quick or deep")

    cfg, settings = _resolve()
    try:
        report = service.verify_backup(
            cfg,
            settings,
            backup_id=backup_id,
            path=path,
            level=level,
            actor=_actor(),
        )
    except Exception as exc:
        _handle(exc)
        return

    payload = report.to_payload()
    _emit(payload, human=_render_checks(payload["checks"]))
    if path is not None and not _json_mode():
        # Say plainly what a loose-file check cannot cover.
        ccyo_out.warning(
            "Verifying a loose file checks readability only; without its "
            "manifest there is nothing to compare checksums or scope against."
        )
    _exit(EXIT_OK if report.ok else EXIT_FINDINGS)


@backup_app.command("restore-plan")
def backup_restore_plan(
    backup_id: str = typer.Option(..., "--backup-id", help="Backup to stage"),
    mode: str = typer.Option(
        verify.MODE_ISOLATED, "--mode", help="isolated or in-place"
    ),
    target_database: Optional[str] = typer.Option(
        None, "--target-database", help="Restore into this database"
    ),
    target_schema: Optional[str] = typer.Option(
        None, "--target-schema", help="Rename the restored schema to this"
    ),
    allow_identity_mismatch: bool = typer.Option(
        False,
        "--allow-identity-mismatch",
        help="Proceed despite a domain/owner mismatch",
    ),
    allow_unknown_migrations: bool = typer.Option(
        False, "--allow-unknown-migrations", help="Proceed despite unknown migrations"
    ),
    allow_unclaimable_prefixes: bool = typer.Option(
        False,
        "--allow-unclaimable-prefixes",
        help="Proceed despite EUID prefixes this target cannot claim",
    ),
) -> None:
    """Stage a restore and print exactly what it would do. Never mutates."""
    cfg, settings = _resolve()
    options = _restore_options(
        mode=mode,
        target_database=target_database,
        target_schema=target_schema,
        allow_identity_mismatch=allow_identity_mismatch,
        allow_unknown_migrations=allow_unknown_migrations,
        allow_unclaimable_prefixes=allow_unclaimable_prefixes,
        keep_superseded=False,
    )
    try:
        plan = verify.plan_restore(cfg, settings, backup_id=backup_id, options=options)
    except Exception as exc:
        _handle(exc)
        return

    payload = plan.to_payload()
    steps = "\n".join(f"  {index}. {step}" for index, step in enumerate(plan.steps, 1))
    _emit(
        payload,
        human=(
            f"\nRestore plan for [bold]{plan.backup_id}[/bold]\n"
            f"  mode:     {plan.mode}\n"
            f"  database: {plan.target_database}\n"
            f"  schema:   {plan.target_schema}\n"
            f"  confirm:  {plan.required_confirm_target}\n"
            f"  finger:   {plan.plan_fingerprint}\n\nSteps:\n{steps}\n\nChecks:\n"
            + _render_checks(payload["checks"])
        ),
    )
    _exit(EXIT_OK if plan.ok else EXIT_FINDINGS)


# ---------------------------------------------------------------------------
# Mutating commands
# ---------------------------------------------------------------------------


@backup_app.command("create")
def backup_create(
    backup_class: str = typer.Option(
        BACKUP_CLASS_FULL, "--class", help="Backup class to create"
    ),
    allow_drift: bool = typer.Option(
        False, "--allow-drift", help="Capture even though the schema has drifted"
    ),
    note: Optional[str] = typer.Option(
        None, "--note", help="Free-text note recorded in the manifest and receipt"
    ),
    existing_snapshot: Optional[str] = typer.Option(
        None,
        "--existing-snapshot",
        help="Record an existing provider snapshot instead of creating one",
    ),
) -> None:
    """Capture a backup. The database is only ever read."""
    resolved_class = _validate_class(backup_class)
    if existing_snapshot and resolved_class != BACKUP_CLASS_PROVIDER_SNAPSHOT:
        # Silently ignoring it would hand back a full logical backup while the
        # operator believes they recorded an existing cluster snapshot.
        raise typer.BadParameter(
            "--existing-snapshot only applies to "
            f"--class {BACKUP_CLASS_PROVIDER_SNAPSHOT}"
        )
    cfg, settings = _resolve()
    dry_run = _dry_run_requested()

    try:
        result = service.create_backup(
            cfg,
            settings,
            backup_class=resolved_class,
            dry_run=dry_run,
            allow_drift=allow_drift,
            note=note,
            actor=_actor(),
            existing_snapshot=existing_snapshot,
        )
    except Exception as exc:
        _log("BACKUP_CREATE_FAILED", str(exc)[:200])
        _handle(exc)
        return

    if not dry_run:
        _log("BACKUP_CREATE", f"{result.backup_id} -> {result.storage_prefix}")

    payload = result.to_payload()
    _emit(
        payload,
        human=(
            f"{'Would create' if dry_run else 'Created'} backup "
            f"[bold]{result.backup_id}[/bold]\n"
            f"  class:   {result.backup_class}\n"
            f"  storage: {result.storage_prefix}"
        ),
    )
    ok = result.dry_run or (result.verify is None or result.verify.ok)
    _exit(EXIT_OK if ok else EXIT_FINDINGS)


def _restore_options(
    *,
    mode: str,
    target_database: Optional[str],
    target_schema: Optional[str],
    allow_identity_mismatch: bool,
    allow_unknown_migrations: bool,
    allow_unclaimable_prefixes: bool,
    keep_superseded: bool,
) -> verify.RestoreOptions:
    options = verify.RestoreOptions(
        mode=mode,
        target_database=target_database,
        target_schema=target_schema,
        allow_identity_mismatch=allow_identity_mismatch,
        allow_unknown_migrations=allow_unknown_migrations,
        allow_unclaimable_prefixes=allow_unclaimable_prefixes,
        keep_superseded=keep_superseded,
    )
    try:
        options.normalized_mode()
        options.validated_target_database()
        options.validated_target_schema()
    except Exception as exc:
        raise typer.BadParameter(str(exc)) from exc
    return options


@backup_app.command("restore")
def backup_restore(
    backup_id: str = typer.Option(..., "--backup-id", help="Backup to restore"),
    mode: str = typer.Option(
        verify.MODE_ISOLATED, "--mode", help="isolated or in-place"
    ),
    target_database: Optional[str] = typer.Option(
        None, "--target-database", help="Restore into this database"
    ),
    target_schema: Optional[str] = typer.Option(
        None, "--target-schema", help="Rename the restored schema to this"
    ),
    confirm_target: Optional[str] = typer.Option(
        None,
        "--confirm-target",
        help="Typed target label; required for an in-place restore",
    ),
    plan_fingerprint: Optional[str] = typer.Option(
        None,
        "--plan-fingerprint",
        help="Fingerprint from restore-plan; refuses if state has changed since",
    ),
    allow_identity_mismatch: bool = typer.Option(
        False,
        "--allow-identity-mismatch",
        help="Proceed despite a domain/owner mismatch",
    ),
    allow_unknown_migrations: bool = typer.Option(
        False, "--allow-unknown-migrations", help="Proceed despite unknown migrations"
    ),
    allow_unclaimable_prefixes: bool = typer.Option(
        False,
        "--allow-unclaimable-prefixes",
        help="Proceed despite EUID prefixes this target cannot claim",
    ),
    keep_superseded: bool = typer.Option(
        False,
        "--keep-superseded",
        help="Keep the replaced schema after an in-place restore",
    ),
) -> None:
    """Restore a backup.

    ``--mode isolated`` (the default) restores into a separate database and
    leaves live data untouched. ``--mode in-place`` replaces the configured
    schema and requires the typed confirmation label.
    """
    options = _restore_options(
        mode=mode,
        target_database=target_database,
        target_schema=target_schema,
        allow_identity_mismatch=allow_identity_mismatch,
        allow_unknown_migrations=allow_unknown_migrations,
        allow_unclaimable_prefixes=allow_unclaimable_prefixes,
        keep_superseded=keep_superseded,
    )
    if keep_superseded and options.normalized_mode() != verify.MODE_IN_PLACE:
        # Only in-place supersedes a schema; accepting this silently would
        # promise to keep something that is never created.
        raise typer.BadParameter("--keep-superseded only applies to --mode in-place")
    cfg, settings = _resolve()
    dry_run = _dry_run_requested()

    try:
        result = verify.restore_backup(
            cfg,
            settings,
            backup_id=backup_id,
            options=options,
            confirm_target=confirm_target,
            plan_fingerprint=plan_fingerprint,
            dry_run=dry_run,
            actor=_actor(),
        )
    except Exception as exc:
        _log("BACKUP_RESTORE_FAILED", f"{backup_id}: {str(exc)[:180]}")
        _handle(exc)
        return

    if not dry_run:
        _log(
            "BACKUP_RESTORE",
            f"{backup_id} -> {result.target_database}/{result.target_schema}",
        )

    payload = result.to_payload()
    _emit(
        payload,
        human=(
            f"{'Would restore' if dry_run else 'Restored'} "
            f"[bold]{backup_id}[/bold]\n"
            f"  mode:     {result.mode}\n"
            f"  database: {result.target_database}\n"
            f"  schema:   {result.target_schema}\n"
            + (
                f"  safety:   {result.safety_backup_id}\n"
                if result.safety_backup_id
                else ""
            )
            + _render_checks(payload["checks"])
        ),
    )
    if result.quarantined and not _json_mode():
        ccyo_out.warning(
            "Post-restore verification failed; the restored database has been "
            "retained and flagged quarantined for inspection."
        )
    _exit(EXIT_OK if (result.dry_run or result.ok) else EXIT_FINDINGS)


@backup_app.command("prune")
def backup_prune(
    apply_changes: bool = typer.Option(
        False, "--apply", help="Actually delete. Without this, nothing is removed."
    ),
    confirm_target: Optional[str] = typer.Option(
        None, "--confirm-target", help="Type the target label to confirm --apply"
    ),
    release: Optional[list[str]] = typer.Option(
        None, "--release", help="Disable one releasable hold (repeatable)"
    ),
    ignore_damaged: bool = typer.Option(
        False, "--ignore-damaged", help="Proceed despite unreadable manifests"
    ),
    allow_delete_markers: bool = typer.Option(
        False,
        "--allow-delete-markers",
        help="Proceed on a versioned bucket, where deleting reclaims nothing",
    ),
    allow_unknown_reclaim: bool = typer.Option(
        False,
        "--allow-unknown-reclaim",
        help="Proceed when deletion capability cannot be determined",
    ),
    allow_bulk: bool = typer.Option(
        False, "--allow-bulk", help="Proceed past the bulk-delete ceiling"
    ),
) -> None:
    """Delete backups that no retention rule protects.

    **Dry run is the default.** Without ``--apply`` this prints what would go
    and deletes nothing, and writes no receipt -- a plan is a read, and filling
    the audit trail with reads would bury the writes.

    Deleting requires ``--apply`` *and* the typed target label, the same one an
    in-place restore demands. The global ``--dry-run`` vetoes ``--apply``, so a
    scheduled run wrapped in it cannot delete whatever else is on the line.

    A backup is removed only when **no** rule protects it. Nothing is ever
    selected for deletion; things fail to be held. Human output lists every
    *retained* backup with the holds keeping it -- "why is this still here" is
    as operationally important as "what would go", and it is the only way to
    watch a rule actually working.

    Exit codes: ``0`` planned or applied cleanly, ``1`` a gate refused or a
    deletion failed, ``2`` could not run.
    """
    from daylily_tapdb.backup import prune as prune_mod

    released = tuple(release or ())
    for hold in released:
        if hold not in prune_mod.RELEASABLE_HOLDS:
            raise typer.BadParameter(
                f"--release must be one of: {', '.join(prune_mod.RELEASABLE_HOLDS)}"
            )

    cfg, settings = _resolve()

    # The framework's --dry-run is a veto, never merely advisory. A scheduled
    # invocation wrapped in it must not delete because someone also passed
    # --apply in the same command line.
    wants_apply = apply_changes and not _dry_run_requested()
    if apply_changes and _dry_run_requested():
        ccyo_out.warning("--dry-run is set; --apply ignored and nothing deleted.")

    try:
        result = prune_mod.prune_backups(
            cfg,
            settings,
            apply=wants_apply,
            confirm_target=confirm_target,
            released=released,
            ignore_damaged=ignore_damaged,
            allow_delete_markers=allow_delete_markers,
            allow_unknown_reclaim=allow_unknown_reclaim,
            allow_bulk=allow_bulk,
            actor=_actor(),
        )
    except Exception as exc:
        _handle(exc)
        return

    payload = result.to_payload()
    plan = result.plan

    if _json_mode():
        ccyo_out.emit_json(payload)
    else:
        lines: list[str] = []
        if result.reconciled:
            lines.append(
                f"Reconciled {len(result.reconciled)} interrupted prune(s): "
                + ", ".join(result.reconciled)
            )
        lines.append(f"Target: {plan.target_label}   keep_last: {plan.keep_last}")
        lines.append("")
        lines.append(f"Retained ({len(plan.retained)}):")
        for item in plan.retained:
            lines.append(
                f"  {item.backup_id}  {item.backup_class:16} "
                f"held by: {', '.join(sorted(item.holds))}"
            )
        lines.append("")
        verb = "Deleted" if not result.dry_run else "Would delete"
        lines.append(f"{verb} ({len(plan.deletable)}):")
        for item in plan.deletable:
            lines.append(
                f"  {item.backup_id}  {item.backup_class:16} {item.bytes} bytes"
            )
        if not plan.deletable:
            lines.append("  (nothing)")
        if plan.excluded:
            lines.append("")
            lines.append(f"Not this target's ({len(plan.excluded)}), never ranked:")
            for item in plan.excluded:
                lines.append(f"  {item.get('backup_id')}  {item.get('reason')}")
        if result.dry_run:
            lines.append("")
            lines.append(
                "Dry run -- nothing was deleted. Re-run with --apply "
                f'--confirm-target "{plan.target_label}" to proceed.'
            )
        ccyo_out.print_text("\n".join(lines))

    for gate in plan.blocking:
        if not _json_mode():
            ccyo_out.error(f"{gate.id}: {gate.detail}")

    _log(
        "backup-prune",
        f"{'applied' if not result.dry_run else 'planned'} "
        f"deletable={len(plan.deletable)}",
    )
    _exit(EXIT_OK if result.ok else EXIT_FINDINGS)


@backup_app.command("rehearse")
def backup_rehearse(
    backup_id: str = typer.Option(..., "--backup-id", help="Backup to rehearse"),
    keep: bool = typer.Option(
        False, "--keep", help="Keep the rehearsal database for inspection"
    ),
) -> None:
    """Restore into a throwaway database and prove the backup verifies.

    Live data is never touched. Evidence is written to storage even when the
    rehearsal fails -- that is the case worth having a record of.
    """
    cfg, settings = _resolve()
    dry_run = _dry_run_requested()

    try:
        evidence = verify.rehearse_restore(
            cfg,
            settings,
            backup_id=backup_id,
            keep=keep,
            dry_run=dry_run,
            actor=_actor(),
        )
    except Exception as exc:
        _log("BACKUP_REHEARSE_FAILED", f"{backup_id}: {str(exc)[:180]}")
        _handle(exc)
        return

    if not dry_run:
        _log(
            "BACKUP_REHEARSE",
            f"{backup_id} -> {evidence.database} ok={evidence.ok}",
        )

    payload = evidence.to_payload()
    _emit(
        payload,
        human=(
            f"Rehearsal {'planned' if dry_run else 'complete'} for "
            f"[bold]{backup_id}[/bold]\n"
            f"  database: {evidence.database}\n"
            f"  evidence: {evidence.evidence_key or '(not written)'}\n"
            + _render_checks(payload["checks"])
        ),
    )
    _exit(EXIT_OK if (evidence.dry_run or evidence.ok) else EXIT_FINDINGS)


__all__ = ["EXIT_ERROR", "EXIT_FINDINGS", "EXIT_OK", "backup_app"]
