"""Explicit native operator plan/apply for prospective issuance changes."""

from __future__ import annotations

import json
from pathlib import Path

import typer
from cli_core_yo import ccyo_out
from cli_core_yo.runtime import get_context

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.issuance_lifecycle import transition_issuance_profile
from daylily_tapdb.runtime_principal import RuntimePrincipalError
from daylily_tapdb.security_context import invocation_attribution

issuance_app = typer.Typer(help="Receipt-bound prospective native issuance profile transitions")


@issuance_app.command("transition")
def transition(
    expected_profile_json: str = typer.Option(
        ..., "--expected-profile-json", help="Exact existing native issuance profile JSON"
    ),
    new_profile_json: str = typer.Option(
        ..., "--new-profile-json", help="Exact requested profile preserving instance allocation"
    ),
    receipt: Path = typer.Option(
        ..., "--receipt", help="Absolute plan receipt: create first, consume unchanged with --apply"
    ),
    apply: bool = typer.Option(False, "--apply", help="Apply the reviewed prospective template allocator transition"),
) -> None:
    """Separate future template allocation without changing persisted identities.

    Requires an explicit root --attribution envelope and configured native
    operator. The config retains the expected old profile until apply succeeds.
    """
    try:
        attribution = invocation_attribution()
        if attribution is None:
            raise ValueError("Explicit root --attribution is required for this operator plan/apply")
        result = transition_issuance_profile(
            get_db_config(), expected_profile=json.loads(expected_profile_json),
            new_profile=json.loads(new_profile_json), attribution=attribution,
            receipt_path=receipt, apply=apply and not bool(get_context().dry_run),
        )
    except Exception as exc:
        message = str(exc) if isinstance(exc, (ValueError, RuntimePrincipalError)) else type(exc).__name__
        ccyo_out.emit_error_json("issuance_transition_error", message)
        raise SystemExit(2) from None
    ccyo_out.emit_json(result)
