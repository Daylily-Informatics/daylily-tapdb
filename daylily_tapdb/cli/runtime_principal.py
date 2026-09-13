"""Thin public CLI for explicit runtime principal bootstrap and binding."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.runtime_principal import (
    bind_runtime_principal,
    bootstrap_runtime_principal,
    set_runtime_identity_access,
)

runtime_principal_app = typer.Typer(
    help="Constrained runtime login preparation and receipt-bound access with runtime TEMP denial"
)


def _apply(requested: bool) -> bool:
    from cli_core_yo.runtime import get_context

    return requested and not bool(get_context().dry_run)


@runtime_principal_app.command("bootstrap")
def bootstrap(
    apply: bool = typer.Option(
        False,
        "--apply",
        help="Create/validate only target.user and grant CONNECT to the existing exact database",
    ),
) -> None:
    """Default: offline configuration-only plan with no connections or changes."""
    try:
        result = bootstrap_runtime_principal(get_db_config(), apply=_apply(apply))
    except Exception as exc:
        # Driver errors can include server-side statements and credentials.
        from daylily_tapdb.runtime_principal import RuntimePrincipalError

        message = (
            str(exc) if isinstance(exc, RuntimePrincipalError) else type(exc).__name__
        )
        raise typer.BadParameter(
            f"Runtime principal bootstrap failed: {message}"
        ) from None
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@runtime_principal_app.command("identity-access")
def identity_access(
    user_uid: int = typer.Option(..., "--user-uid", min=1),
    user_euid: str = typer.Option(..., "--user-euid"),
    enabled: bool = typer.Option(..., "--grant/--revoke"),
    reason: str = typer.Option(..., "--reason"),
    receipt: Path = typer.Option(..., "--receipt"),
    apply: bool = typer.Option(False, "--apply"),
) -> None:
    """Plan, then apply one exact authorization-only user grant for target.user.

    No user enumeration, ordinary object access, issuer reassignment or token
    changes. Apply requires the unchanged plan and existing operator context.
    """
    try:
        result = set_runtime_identity_access(
            get_db_config(), user_uid=user_uid, user_euid=user_euid,
            enabled=enabled, reason=reason, receipt_path=receipt, apply=_apply(apply),
        )
    except Exception as exc:
        from daylily_tapdb.runtime_principal import RuntimePrincipalError
        message = str(exc) if isinstance(exc, RuntimePrincipalError) else type(exc).__name__
        raise typer.BadParameter(f"Runtime identity access failed: {message}") from None
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@runtime_principal_app.command("bind")
def bind(
    receipt: Path = typer.Option(
        ...,
        "--receipt",
        help="Absolute plan receipt path: create in plan mode, validate and consume with --apply",
    ),
    apply: bool = typer.Option(
        False,
        "--apply",
        help="Apply reviewed CONNECT/schema grants, immutable scope, and database TEMP restriction; existing runtime sessions must be restarted",
    ),
) -> None:
    """Bind existing schema access and deny runtime temporary-object creation.

    The plan discloses database-wide PUBLIC TEMP revocation. Apply does not
    terminate existing sessions or apply/reset/seed the schema.
    """
    try:
        result = bind_runtime_principal(
            get_db_config(), apply=_apply(apply), receipt_path=receipt
        )
    except Exception as exc:
        from daylily_tapdb.runtime_principal import RuntimePrincipalError

        message = (
            str(exc) if isinstance(exc, RuntimePrincipalError) else type(exc).__name__
        )
        raise typer.BadParameter(
            f"Runtime principal binding failed: {message}"
        ) from None
    typer.echo(json.dumps(result, indent=2, sort_keys=True))
