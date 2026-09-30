"""Thin CLI for the shared native integrity operations."""

from __future__ import annotations
import json
from pathlib import Path
import typer
from daylily_tapdb.cli.db import Environment, _tapdb_connection_for_env
from daylily_tapdb.security_context import Attribution, invocation_attribution
from daylily_tapdb.services.integrity_operations import (
    execute_integrity_operation,
    WRITE_OPERATIONS,
)

history_app = typer.Typer(
    help="Read-only historical boundaries and guarded forward correction"
)
references_app = typer.Typer(
    help="Independent canonical references, annotations and typed associations"
)


def run(operation: str, payload: Path | None, attribution: Path | None, apply: bool):
    from cli_core_yo.runtime import get_context

    data = json.loads(payload.read_text()) if payload else {}
    ctx = (
        Attribution(**json.loads(attribution.read_text()))
        if attribution
        else invocation_attribution()
    )
    writing = operation in WRITE_OPERATIONS
    if writing and (not apply or get_context().dry_run):
        raise typer.BadParameter(
            "write operation requires explicit --apply; use resolve/plan-correction for read-only preview"
        )
    if writing and ctx is None:
        raise typer.BadParameter(
            "write operation requires --attribution with explicit authenticated actor/service context"
        )
    with _tapdb_connection_for_env(
        Environment.target,
        app_username=ctx.actor_subject if ctx else "tapdb-read",
        attribution=ctx,
    ) as conn:
        with conn.session_scope(commit=writing) as session:
            result = execute_integrity_operation(session, operation, data)
    typer.echo(json.dumps(result, indent=2, sort_keys=True, default=str))


def _command(operation):
    def invoke(
        payload: Path | None = typer.Option(
            None, "--payload", exists=True, dir_okay=False
        ),
        attribution: Path | None = typer.Option(
            None, "--attribution", exists=True, dir_okay=False
        ),
        apply: bool = typer.Option(False, "--apply"),
    ):
        run(operation, payload, attribution, apply)

    invoke.__name__ = operation.replace("-", "_")
    return invoke


for _op in (
    "status",
    "boundary",
    "object-at",
    "graph-at",
    "plan-correction",
    "apply-correction",
    "audit",
):
    history_app.command(_op)(_command(_op))
for _op in ("resolve", "register", "annotate", "attach", "detach", "reconcile"):
    references_app.command(_op)(_command(_op))

adoption_app = typer.Typer(
    help="Reviewed TapDB 11 adoption using the native physical writer fence"
)


@adoption_app.command("plan")
def adoption_plan(output: Path = typer.Option(..., "--output")):
    from daylily_tapdb.cli.db_config import get_db_config
    from daylily_tapdb.runtime_principal import operator_session
    from daylily_tapdb.integrity_lifecycle import plan_adoption

    if not output.is_absolute() or output.exists():
        raise typer.BadParameter("--output must be a new absolute receipt path")
    cfg = get_db_config()
    with operator_session(cfg, isolation_level="REPEATABLE READ") as conn, conn.begin():
        plan = plan_adoption(conn, cfg)
    with output.open("x") as f:
        json.dump(plan, f, indent=2, sort_keys=True, default=str)
    typer.echo(json.dumps({"plan": str(output), "sha256": plan["sha256"]}))


@adoption_app.command("apply")
def adoption_apply(
    plan: Path = typer.Option(..., "--plan", exists=True),
    control_config: Path = typer.Option(..., "--control-config", exists=True),
    receipts_dir: Path = typer.Option(..., "--receipts-dir"),
    receipt: Path = typer.Option(..., "--receipt"),
    provider_contract: Path | None = typer.Option(
        None, "--provider-contract", exists=True, dir_okay=False
    ),
    apply: bool = typer.Option(False, "--apply"),
):
    from cli_core_yo.runtime import get_context
    from daylily_tapdb.cli.db_config import get_db_config
    from daylily_tapdb.integrity_lifecycle import adopt_integrity

    if not apply or get_context().dry_run:
        raise typer.BadParameter(
            "explicit --apply required; inspect the separate plan first"
        )
    if not receipt.is_absolute() or receipt.exists() or not receipts_dir.is_absolute():
        raise typer.BadParameter(
            "--receipt must be a new absolute path; --receipts-dir must be absolute"
        )
    cfg = get_db_config()
    control = get_db_config(config_path=str(control_config))
    result = adopt_integrity(
        cfg,
        control,
        plan=json.loads(plan.read_text()),
        receipts_dir=receipts_dir,
        provider_contract=(
            json.loads(provider_contract.read_text()) if provider_contract else None
        ),
    )
    with receipt.open("x") as f:
        json.dump(result, f, indent=2, sort_keys=True, default=str)
    typer.echo(json.dumps(result, default=str))
