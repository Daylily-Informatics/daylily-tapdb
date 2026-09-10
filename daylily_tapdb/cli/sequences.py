"""Receipt-bound sequence safety commands; arithmetic lives in sequences."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from cli_core_yo import ccyo_out
from cli_core_yo.runtime import get_context

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.cli.identity import _fail, _read, _resolve
from daylily_tapdb.identity_inventory import seal_receipt
from daylily_tapdb.migration_identity import write_json_receipt
from daylily_tapdb.runtime_principal import operator_connection, operator_session
from daylily_tapdb.sequences import (
    acquire_database_writer_fence,
    apply_sequence_advance_plan,
    apply_writer_fence_takeover,
    build_sequence_advance_plan,
    build_writer_fence_takeover_plan,
    capture_sequence_inventory,
    reconcile_writer_fence_release,
    record_sequence_advance_outcome,
    release_database_writer_fence,
    verify_sequence_floors,
)

sequences_app = typer.Typer(
    help="Fenced allocator advance and strict floor verification"
)


@sequences_app.command("advance")
def advance(
    floors: Path = typer.Option(
        ..., "--floors", help="Absolute JSON object containing floors array"
    ),
    receipt: Path = typer.Option(
        ..., "--receipt", help="New absolute dry-run or applied receipt path"
    ),
    apply: bool = typer.Option(
        False, "--apply", help="Apply exactly a reviewed advance plan"
    ),
    preflight_receipt: Path | None = typer.Option(
        None, "--preflight-receipt", help="Absolute reviewed dry-run plan"
    ),
    writer_fence: Path | None = typer.Option(
        None,
        "--writer-fence",
        help="Absolute fence declaration; verified against PostgreSQL",
    ),
    establish_writer_fence: bool = typer.Option(
        False,
        "--establish-writer-fence",
        help="Explicitly close the database connection gate through verified commit and release",
    ),
    control_config: Path | None = typer.Option(
        None,
        "--control-config",
        help="Absolute explicit config for a different control database; required to establish fence",
    ),
    receipts_dir: Path | None = typer.Option(
        None,
        "--receipts-dir",
        help="Absolute external hash-chained operation receipt directory",
    ),
    sequence_mappings: Path | None = typer.Option(
        None, "--sequence-mappings", help="Absolute verified allocator mappings JSON"
    ),
    provider_contract: Annotated[
        Path | None,
        typer.Option(
            "--provider-contract",
            help="Absolute pinned provider identity JSON; verified against AWS and PostgreSQL",
        ),
    ] = None,
    quarantine_receipt: Annotated[
        Path | None,
        typer.Option(
            "--quarantine-receipt",
            help="Absolute successful takeover receipt for explicit quarantine promotion",
        ),
    ] = None,
    recovery_family: Annotated[
        Path | None,
        typer.Option(
            "--recovery-family",
            help="Absolute sealed recovery family; retain all explicit family journal floors",
        ),
    ] = None,
) -> None:
    """Default to a dry-run receipt. Apply requires an unchanged target and fence."""
    applied = None
    try:
        if not receipt.is_absolute() or receipt.exists():
            raise ValueError("--receipt must name an absolute new file")
        cfg, target = _resolve(sequence_mappings)
        family = _read(recovery_family) if recovery_family is not None else None
        provider = _read(provider_contract) if provider_contract is not None else None
        quarantine = (
            _read(quarantine_receipt) if quarantine_receipt is not None else None
        )
        floor_document = _read(floors)
        if set(floor_document) != {"floors"}:
            raise ValueError("--floors must contain exactly a floors array")
        effective_apply = apply and not get_context().dry_run
        if establish_writer_fence and (not apply or writer_fence is not None):
            raise ValueError(
                "--establish-writer-fence requires --apply and is mutually exclusive with --writer-fence"
            )
        if control_config is not None and not establish_writer_fence:
            raise ValueError(
                "--control-config is valid only with --establish-writer-fence"
            )
        if (
            provider_contract is not None or quarantine_receipt is not None
        ) and not establish_writer_fence:
            raise ValueError(
                "Provider/quarantine declarations require --establish-writer-fence"
            )
        if effective_apply and (
            preflight_receipt is None
            or (writer_fence is None and not establish_writer_fence)
            or receipts_dir is None
        ):
            raise ValueError(
                "--apply requires --preflight-receipt, --writer-fence or --establish-writer-fence, and --receipts-dir"
            )
        if effective_apply and establish_writer_fence:
            if (
                preflight_receipt is None
                or receipts_dir is None
                or control_config is None
                or not control_config.is_absolute()
            ):
                raise ValueError(
                    "Established-fence apply requires explicit review, receipt, and absolute --control-config paths"
                )
            control_cfg = get_db_config(config_path=control_config)
            reviewed = _read(preflight_receipt)
            with operator_session(cfg, isolation_level="REPEATABLE READ") as connection:
                with connection.begin():
                    current = capture_sequence_inventory(
                        connection, schema_name=target["schema_name"], target=target
                    )
                    plan = build_sequence_advance_plan(
                        current, floors=floor_document["floors"], recovery_family=family
                    )
                    if reviewed != plan:
                        raise ValueError(
                            "Reviewed sequence plan no longer matches the explicit target, mappings, or floors"
                        )
                with operator_session(
                    control_cfg, isolation_level="REPEATABLE READ"
                ) as control_connection:
                    acquired = acquire_database_writer_fence(
                        connection,
                        control_connection=control_connection,
                        inventory=current,
                        receipts_dir=receipts_dir,
                        quarantine_receipt=quarantine,
                        provider_contract=provider,
                        recovery_family=family,
                    )
                    with connection.begin():
                        applied = apply_sequence_advance_plan(
                            connection,
                            reviewed,
                            writer_fence=acquired["fence"],
                            receipts_dir=receipts_dir,
                        )
                    payload = record_sequence_advance_outcome(
                        applied,
                        receipts_dir=receipts_dir,
                        outcome="committed",
                        actor=cfg["operator_user"],
                    )
                    released = release_database_writer_fence(
                        connection,
                        acquired,
                        result=payload,
                        receipts_dir=receipts_dir,
                        control_connection=control_connection,
                    )
                payload = seal_receipt({**payload, "writer_fence_release": released})
            write_json_receipt(receipt, payload)
            ccyo_out.emit_json(payload)
            return
        with operator_connection(
            cfg, isolation_level="REPEATABLE READ", read_only=not effective_apply
        ) as connection:
            current = capture_sequence_inventory(
                connection, schema_name=target["schema_name"], target=target
            )
            plan = build_sequence_advance_plan(
                current, floors=floor_document["floors"], recovery_family=family
            )
            if effective_apply:
                if (
                    preflight_receipt is None
                    or writer_fence is None
                    or receipts_dir is None
                ):
                    raise ValueError("Apply paths must be explicit")
                reviewed = _read(preflight_receipt)
                if reviewed != plan:
                    raise ValueError(
                        "Reviewed sequence plan no longer matches the explicit target, mappings, or floors"
                    )
                applied = apply_sequence_advance_plan(
                    connection,
                    reviewed,
                    writer_fence=_read(writer_fence),
                    receipts_dir=receipts_dir,
                )
            else:
                payload = plan
        if applied is not None:
            if receipts_dir is None:
                raise ValueError(
                    "Committed apply requires its external receipt directory"
                )
            payload = record_sequence_advance_outcome(
                applied,
                receipts_dir=receipts_dir,
                outcome="committed",
                actor=cfg["operator_user"],
            )
        write_json_receipt(receipt, payload)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(payload)


@sequences_app.command("verify")
def verify(
    floors: Path = typer.Option(
        ..., "--floors", help="Absolute JSON object containing floors array"
    ),
    sequence_mappings: Path | None = typer.Option(
        None, "--sequence-mappings", help="Absolute verified allocator mappings JSON"
    ),
    recovery_family: Annotated[
        Path | None,
        typer.Option(
            "--recovery-family",
            help="Absolute sealed family whose complete journal floors must also be exceeded",
        ),
    ] = None,
) -> None:
    """Read all allocators and prove their next values exceed every supplied floor."""
    try:
        cfg, target = _resolve(sequence_mappings)
        floor_document = _read(floors)
        if set(floor_document) != {"floors"}:
            raise ValueError("--floors must contain exactly a floors array")
        with operator_connection(
            cfg, isolation_level="REPEATABLE READ", read_only=True
        ) as connection:
            current = capture_sequence_inventory(
                connection, schema_name=target["schema_name"], target=target
            )
            result = verify_sequence_floors(
                current,
                floors=floor_document["floors"],
                recovery_family=_read(recovery_family)
                if recovery_family is not None
                else None,
            )
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(result)
    if not result["ok"]:
        raise SystemExit(1)


@sequences_app.command("reconcile")
def reconcile(
    control_config: Annotated[
        Path,
        typer.Option(
            "--control-config",
            help="Absolute explicit config for the original different control database",
        ),
    ],
    receipts_dir: Annotated[
        Path,
        typer.Option(
            "--receipts-dir",
            help="Absolute original external operation journal directory",
        ),
    ],
    receipt: Annotated[
        Path,
        typer.Option("--receipt", help="New absolute review or reconciliation receipt"),
    ],
    fence_intent_receipt_id: Annotated[
        str | None,
        typer.Option(
            "--fence-intent-receipt-id",
            help="Exact latest stalled fence intent to take over",
        ),
    ] = None,
    release_intent_receipt_id: Annotated[
        str | None,
        typer.Option(
            "--release-intent-receipt-id",
            help="Exact lost-acknowledgement release intent to observe",
        ),
    ] = None,
    apply: Annotated[
        bool,
        typer.Option("--apply", help="Apply the exact reviewed reconciliation receipt"),
    ] = False,
    preflight_receipt: Annotated[
        Path | None,
        typer.Option(
            "--preflight-receipt", help="Absolute reviewed reconciliation plan"
        ),
    ] = None,
    provider_contract: Annotated[
        Path | None,
        typer.Option(
            "--provider-contract", help="Absolute pinned provider identity JSON"
        ),
    ] = None,
    sequence_mappings: Annotated[
        Path | None,
        typer.Option(
            "--sequence-mappings", help="Absolute verified allocator mapping JSON"
        ),
    ] = None,
    recovery_family: Annotated[
        Path | None,
        typer.Option(
            "--recovery-family", help="Absolute identical original recovery family JSON"
        ),
    ] = None,
) -> None:
    """Explicit stalled-gate takeover; leave operator-only access for a NEW review."""
    try:
        if not receipt.is_absolute() or receipt.exists():
            raise ValueError("--receipt must name an absolute new file")
        if not control_config.is_absolute() or not control_config.is_file():
            raise ValueError("--control-config must name an existing absolute file")
        if not receipts_dir.is_absolute() or not receipts_dir.is_dir():
            raise ValueError(
                "--receipts-dir must name the existing absolute original journal"
            )
        if (fence_intent_receipt_id is None) == (release_intent_receipt_id is None):
            raise ValueError(
                "Supply exactly one explicit fence or release intent receipt ID"
            )
        effective_apply = apply and not get_context().dry_run
        if effective_apply and preflight_receipt is None:
            raise ValueError("--apply requires an unchanged --preflight-receipt")
        cfg, target = _resolve(sequence_mappings)
        control_cfg = get_db_config(config_path=control_config)
        provider = _read(provider_contract) if provider_contract is not None else None
        family = _read(recovery_family) if recovery_family is not None else None
        reviewed = (
            _read(preflight_receipt)
            if effective_apply and preflight_receipt is not None
            else None
        )
        with operator_session(
            control_cfg, isolation_level="REPEATABLE READ"
        ) as control:
            if release_intent_receipt_id is not None:
                payload = reconcile_writer_fence_release(
                    control,
                    release_intent_receipt_id=release_intent_receipt_id,
                    receipts_dir=receipts_dir,
                    provider_contract=provider,
                    recovery_family=family,
                    dry_run=not effective_apply,
                    preflight_receipt=reviewed,
                )
            else:
                if fence_intent_receipt_id is None:
                    raise ValueError("Fence intent receipt ID is required")
                planned = build_writer_fence_takeover_plan(
                    control,
                    target=target,
                    receipts_dir=receipts_dir,
                    fence_intent_receipt_id=fence_intent_receipt_id,
                    provider_contract=provider,
                    recovery_family=family,
                )
                if effective_apply:
                    if reviewed != planned:
                        raise ValueError(
                            "Reviewed reconciliation plan no longer matches the exact target and journal"
                        )
                    payload = apply_writer_fence_takeover(
                        control,
                        planned,
                        target_connection_factory=lambda: operator_session(
                            cfg, isolation_level="REPEATABLE READ"
                        ),
                        receipts_dir=receipts_dir,
                    )
                else:
                    payload = planned
        write_json_receipt(receipt, payload)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(payload)
