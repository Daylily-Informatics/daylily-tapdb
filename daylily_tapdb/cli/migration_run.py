"""Thin CLI for explicit migration composition and retained archive receipts."""

from pathlib import Path
from typing import Annotated

import typer
from cli_core_yo import ccyo_out
from cli_core_yo.runtime import get_context

from daylily_tapdb.cli.identity import _fail, _read, _resolve
from daylily_tapdb.migration_identity import write_json_receipt
from daylily_tapdb.migration_run import prepare_run, reconcile_run, run_migration
from daylily_tapdb.retained_archive import archive_restore_plan, retain_archive

migration_run_app = typer.Typer(help="Prepare and resume explicit native migration stages")
archive_app = typer.Typer(help="Retain closed databases and plan native recovery")


@archive_app.command("fence")
def fence_for_archive(
    source_contract: Path = typer.Option(...), control_config: Path = typer.Option(...),
    receipts_dir: Path = typer.Option(...), receipt: Path = typer.Option(...),
    provider_contract: Path | None = typer.Option(None),
    quarantine_receipt: Path | None = typer.Option(None),
    apply: bool = typer.Option(False, help="Close the native database gate and leave it closed for archival"),
) -> None:
    try:
        from daylily_tapdb.backup.source_contract import validate_source_contract
        from daylily_tapdb.cli.db_config import get_db_config
        from daylily_tapdb.runtime_principal import operator_session
        from daylily_tapdb.sequence_fence import acquire_database_writer_fence
        from daylily_tapdb.identity_inventory import validate_target

        cfg, target = _resolve()
        source = _read(source_contract)
        validate_source_contract(source)
        if source["sequence_inventory"]["target"] != validate_target(target, target["schema_name"]):
            raise ValueError("Source contract must match the explicitly selected config identity")
        if source.get("recovery_family") is None:
            raise ValueError("Archival requires an explicit recovery family")
        if not receipt.is_absolute() or receipt.exists():
            raise ValueError("--receipt must name a new absolute file")
        if not apply or get_context().dry_run:
            ccyo_out.emit_json({"phase": "planned", "target": target, "effect": "leave database gate closed",
                               "source_contract": str(source_contract), "control_config": str(control_config)})
            return
        control_cfg = get_db_config(config_path=control_config)
        with operator_session(cfg, isolation_level="REPEATABLE READ") as connection:
            with operator_session(control_cfg, isolation_level="REPEATABLE READ") as control:
                result = acquire_database_writer_fence(
                    connection, control_connection=control, inventory=source["sequence_inventory"],
                    receipts_dir=receipts_dir, recovery_family=source["recovery_family"],
                    provider_contract=_read(provider_contract) if provider_contract is not None else None,
                    quarantine_receipt=_read(quarantine_receipt) if quarantine_receipt is not None else None,
                )
                write_json_receipt(receipt, result)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(result)


@migration_run_app.command("prepare")
def prepare(definition: Path = typer.Option(...), receipt: Path = typer.Option(...)) -> None:
    try:
        _cfg, target = _resolve()
        result = prepare_run(_read(definition), target=target)
        write_json_receipt(receipt, result)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json({"receipt": str(receipt), "sha256": result["sha256"]})


@migration_run_app.command("resume")
def resume(plan: Path = typer.Option(...), receipts_dir: Path = typer.Option(...)) -> None:
    try:
        _cfg, target = _resolve()
        prepared = _read(plan)
        if get_context().dry_run:
            ccyo_out.emit_json({"dry_run": True, "plan": prepared})
            return
        result = run_migration(prepared, target=target, receipts_dir=receipts_dir)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(result)
    if result["phase"] != "succeeded":
        raise SystemExit(result.get("returncode", 1))


@migration_run_app.command("reconcile")
def reconcile(plan: Path = typer.Option(...), receipts_dir: Path = typer.Option(...), result: Path = typer.Option(...)) -> None:
    try:
        outcome = reconcile_run(_read(plan), receipts_dir=receipts_dir, result_path=result)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(outcome)


@archive_app.command("retain")
def retain(
    fence: Path = typer.Option(...), source_contract: Path = typer.Option(...),
    backup_result: Path = typer.Option(...), receipts_dir: Path = typer.Option(...),
    backup_receipts_dir: Path = typer.Option(...), receipt: Path = typer.Option(...),
) -> None:
    try:
        _cfg, target = _resolve()
        result = retain_archive(target=target, fence=_read(fence), source_contract=_read(source_contract),
                                backup_result=_read(backup_result), receipts_dir=receipts_dir,
                                backup_receipts_dir=backup_receipts_dir)
        write_json_receipt(receipt, result)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(result)


@archive_app.command("restore-plan")
def restore_plan(
    archive: Path = typer.Option(...), receipt: Path = typer.Option(...),
    generation_quarantine: Annotated[list[Path] | None, typer.Option("--generation-quarantine")] = None,
) -> None:
    try:
        result = archive_restore_plan(_read(archive), generation_quarantines=[_read(path) for path in generation_quarantine or []])
        write_json_receipt(receipt, result)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(result)
