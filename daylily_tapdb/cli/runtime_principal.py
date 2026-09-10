"""Thin public CLI for explicit runtime principal bootstrap and binding."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.runtime_principal import (
    bind_runtime_principal,
    bootstrap_runtime_principal,
)

runtime_principal_app = typer.Typer(
    help="Constrained runtime login preparation and separate receipt-bound CONNECT/schema grants"
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
        help="Apply exact database CONNECT, schema grants, and immutable scope from the unchanged reviewed receipt",
    ),
) -> None:
    """Activate reviewed access to an existing schema; never apply/reset/seed it."""
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
