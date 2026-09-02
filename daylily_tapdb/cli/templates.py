"""Repository-owned template pack CLI."""

from __future__ import annotations

import getpass
import json
from dataclasses import asdict
from pathlib import Path

import typer

from daylily_tapdb.cli.db import Environment, _tapdb_connection_for_env
from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.templates.repository import (
    export_repository_pack,
    import_repository_pack,
    repository_inventory,
)

templates_app = typer.Typer(help="Repository-owned template pack operations")


def _emit(payload: dict) -> None:
    typer.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _dry_run_requested() -> bool:
    from cli_core_yo.runtime import get_context

    return bool(get_context().dry_run)


@templates_app.command("export")
def templates_export(
    repository_pack: Path = typer.Option(
        ..., "--repository-pack", help="Explicit absolute .json repository pack path"
    ),
    euid: str = typer.Option("", "--euid", help="Export one exact template EUID"),
    actor: str = typer.Option("", "--actor", help="Provenance actor"),
) -> None:
    """Export canonical templates without database identities or secrets."""

    cfg = get_db_config()
    effective_actor = actor.strip() or getpass.getuser()
    with _tapdb_connection_for_env(
        Environment.target, app_username=effective_actor
    ) as conn:
        with conn.session_scope(commit=False) as session:
            receipt = export_repository_pack(
                session,
                repository_pack,
                domain_code=cfg["domain_code"],
                issuer_app_code=cfg["owner_repo_name"],
                prefix_registry_path=cfg["prefix_ownership_registry_path"],
                actor=effective_actor,
                template_euid=euid.strip() or None,
            )
    _emit(receipt)


@templates_app.command("import")
def templates_import(
    repository_pack: Path = typer.Option(
        ..., "--repository-pack", help="Explicit absolute .json repository pack path"
    ),
    apply: bool = typer.Option(
        False,
        "--apply/--dry-run",
        help="Persist validated missing templates; default is dry-run",
    ),
    actor: str = typer.Option("", "--actor", help="Audit actor"),
) -> None:
    """Validate by default; import only when ``--apply`` is explicit."""

    effective_apply = apply and not _dry_run_requested()
    cfg = get_db_config()
    effective_actor = actor.strip() or getpass.getuser()
    with _tapdb_connection_for_env(
        Environment.target, app_username=effective_actor
    ) as conn:
        with conn.session_scope(commit=effective_apply) as session:
            result = import_repository_pack(
                session,
                repository_pack,
                domain_code=cfg["domain_code"],
                owner_repo_name=cfg["owner_repo_name"],
                domain_registry_path=cfg["domain_registry_path"],
                prefix_registry_path=cfg["prefix_ownership_registry_path"],
                dry_run=not effective_apply,
            )
    _emit(asdict(result))


@templates_app.command("inventory")
def templates_inventory(
    repository_pack: Path = typer.Option(
        ..., "--repository-pack", help="Explicit absolute .json repository pack path"
    ),
    actor: str = typer.Option("", "--actor", help="Read audit actor"),
) -> None:
    """Show pending, backed-up, or failed repository status."""

    cfg = get_db_config()
    effective_actor = actor.strip() or getpass.getuser()
    with _tapdb_connection_for_env(
        Environment.target, app_username=effective_actor
    ) as conn:
        with conn.session_scope(commit=False) as session:
            result = repository_inventory(
                session,
                repository_pack,
                domain_code=cfg["domain_code"],
                issuer_app_code=cfg["owner_repo_name"],
            )
    _emit(result)


__all__ = ["templates_app"]
