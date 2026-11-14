"""Thin read-only historical database/principal census command."""

from pathlib import Path
from typing import Annotated

import typer
from cli_core_yo import ccyo_out

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.migration_identity import write_json_receipt
from daylily_tapdb.principal_census import (
    PrincipalCensusError,
    capture_principal_census,
)
from daylily_tapdb.runtime_principal import RuntimePrincipalError


def census(
    database: Annotated[
        list[str] | None,
        typer.Option(
            "--database",
            help="Exact database to check on the configured server; repeat for destination/control",
        ),
    ] = None,
    receipt: Annotated[
        Path | None,
        typer.Option("--receipt", help="New absolute census receipt; never overwrite"),
    ] = None,
    statement_timeout_ms: Annotated[
        int, typer.Option("--statement-timeout-ms", min=1)
    ] = 10000,
    max_catalog_rows: Annotated[
        int, typer.Option("--max-catalog-rows", min=1)
    ] = 100000,
) -> None:
    """Read original principals/ACLs/activity without initializing any schema."""
    try:
        if receipt is not None and (not receipt.is_absolute() or receipt.exists()):
            raise PrincipalCensusError("--receipt must be a new absolute path")
        result = capture_principal_census(
            get_db_config(),
            database_names=database or (),
            statement_timeout_ms=statement_timeout_ms,
            max_catalog_rows=max_catalog_rows,
        )
        if receipt is not None:
            write_json_receipt(receipt, result)
    except Exception as exc:
        # SQL/connection errors can contain credentials or statement content.
        message = (
            str(exc)
            if isinstance(exc, (PrincipalCensusError, RuntimePrincipalError))
            else type(exc).__name__
        )
        ccyo_out.emit_error_json("principal_census_error", message)
        raise SystemExit(2) from None
    ccyo_out.emit_json(result)
