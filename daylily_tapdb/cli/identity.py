"""Thin, explicit-target identity capture and preservation commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any

import typer
from cli_core_yo import ccyo_out

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.identity_inventory import (
    InventoryLimitExceededError,
    capture_identity_inventory,
    validate_target,
    verify_identity_inventory,
    with_inventory_limits,
)
from daylily_tapdb.migration_identity import write_json_receipt
from daylily_tapdb.runtime_principal import RuntimePrincipalError, operator_connection

identity_app = typer.Typer(help="Catalog-complete identity preservation evidence")


def _read(path: Path) -> dict[str, Any]:
    """Read an explicit JSON object; its owning API validates its contract."""
    if not path.is_absolute() or not path.is_file():
        raise ValueError("Receipt and mapping paths must be existing absolute files")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Receipt and mapping files must contain a JSON object")
    return payload


def _resolve(
    sequence_mappings: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    cfg = dict(get_db_config())
    server = {"server_port": cfg["server_port"]} if "server_port" in cfg else {}
    target = validate_target(
        {
            **server,
            "engine_type": cfg["engine_type"],
            "host": cfg["host"],
            "port": cfg["port"],
            "database": cfg["database"],
            "schema_name": cfg["schema_name"],
            "config_identity": str(cfg["config_path"]),
            "domain_code": cfg["domain_code"],
            "owner_repo_name": cfg["owner_repo_name"],
        },
        cfg["schema_name"],
    )
    if "inventory_mode" in cfg:
        target["inventory_mode"] = cfg["inventory_mode"]
    if "inventory_limits" in cfg:
        target["inventory_limits"] = cfg["inventory_limits"]
    if sequence_mappings is not None:
        target["sequence_mappings"] = _read(sequence_mappings)
    return cfg, target


def _identity(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("schema_version") == "tapdb-source-contract/v1":
        from daylily_tapdb.backup.source_contract import validate_source_contract

        validate_source_contract(payload)
        return dict(payload["identity_inventory"])
    return payload


def _fail(exc: Exception) -> None:
    # Untrusted SQL/driver exceptions can contain row data. Keep those private.
    message = (
        str(exc)
        if isinstance(exc, (ValueError, OSError, RuntimePrincipalError))
        else type(exc).__name__
    )
    if isinstance(exc, InventoryLimitExceededError):
        ccyo_out.emit_error_json(
            "identity_inventory_limit", message, details=exc.diagnostics
        )
    else:
        ccyo_out.emit_error_json("identity_inventory_error", message)
    raise SystemExit(2)


@identity_app.command("prefix-from-evidence")
def prefix_from_evidence(
    evidence_kind: str = typer.Option(
        ..., "--evidence-kind", help="Explicit archived_source_contract or catalog_schema_absent evidence"
    ),
    evidence: Path = typer.Option(..., "--evidence", help="Absolute existing native source-contract or census JSON"),
    prefix: str = typer.Option(..., "--prefix", help="Exact Meridian prefix to project"),
    archive_result: Path | None = typer.Option(
        None, "--archive-result", help="Archive mode: absolute successful copy-SOP result JSON"
    ),
    recovery_family: Path | None = typer.Option(
        None, "--recovery-family", help="Archive mode: absolute existing family descriptor naming native journal roots"
    ),
    receipt: Path | None = typer.Option(None, "--receipt", help="New absolute projected-evidence JSON path"),
) -> None:
    """Consume existing evidence explicitly; never open or modify a database."""
    try:
        if receipt is not None and (not receipt.is_absolute() or receipt.exists()):
            raise ValueError("--receipt must name an absolute new file")
        from daylily_tapdb.identity_prefix_inventory import project_prefix_from_evidence

        payload = project_prefix_from_evidence(
            evidence_kind=evidence_kind, evidence=_read(evidence), evidence_path=evidence,
            prefix=prefix,
            archive_result=_read(archive_result) if archive_result is not None else None,
            archive_result_path=archive_result,
            recovery_family=_read(recovery_family) if recovery_family is not None else None,
            recovery_family_path=recovery_family,
        )
        if receipt is not None:
            write_json_receipt(receipt, payload)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(payload)


@identity_app.command("inventory")
def inventory(
    prefix: str | None = typer.Option(
        None, "--prefix", help="Read one exact Meridian prefix only; not a full preservation inventory"
    ),
    allocation_receipts_dirs: Annotated[
        list[Path] | None,
        typer.Option(
            "--allocation-receipts-dir",
            help="Explicit native allocation journal directory for --prefix; repeat for each known journal",
        ),
    ] = None,
    receipt: Path | None = typer.Option(
        None, "--receipt", help="New absolute JSON receipt path"
    ),
    receipt_only: bool = typer.Option(
        False, "--receipt-only", help="Write the complete receipt and print only its descriptor"
    ),
    source_version: str | None = typer.Option(
        None,
        "--source-version",
        help="Exact historical source version; capture source contract",
    ),
    sequence_mappings: Path | None = typer.Option(
        None,
        "--sequence-mappings",
        help="Absolute JSON file of explicit verified allocator mappings",
    ),
    recovery_family: Annotated[
        Path | None,
        typer.Option(
            "--recovery-family", help="Absolute sealed existing recovery family JSON"
        ),
    ] = None,
    new_recovery_family_id: Annotated[
        str | None,
        typer.Option(
            "--new-recovery-family-id",
            help="Explicit operator-chosen UUID for a new immutable recovery family",
        ),
    ] = None,
    family_receipts_dir: Annotated[
        list[Path] | None,
        typer.Option(
            "--family-receipts-dir",
            help="Explicit absolute existing journal root; repeat for each immutable family root",
        ),
    ] = None,
    family_receipt: Annotated[
        Path | None,
        typer.Option(
            "--family-receipt", help="New absolute recovery family receipt file"
        ),
    ] = None,
) -> None:
    """Read all physical tables without runtime schema initialization."""
    try:
        if receipt_only and receipt is None:
            raise ValueError("--receipt-only requires --receipt")
        if receipt is not None and (not receipt.is_absolute() or receipt.exists()):
            raise ValueError("--receipt must name an absolute new file")
        if allocation_receipts_dirs is not None and prefix is None:
            raise ValueError("--allocation-receipts-dir requires --prefix")
        if prefix is not None and any(
            value is not None
            for value in (
                source_version, sequence_mappings, recovery_family,
                new_recovery_family_id, family_receipts_dir, family_receipt,
            )
        ):
            raise ValueError("--prefix is scoped discovery and cannot be combined with preservation/recovery flags")
        creating_family = any(
            value is not None
            for value in (new_recovery_family_id, family_receipts_dir, family_receipt)
        )
        if creating_family and (
            not new_recovery_family_id
            or not family_receipts_dir
            or family_receipt is None
            or receipt is None
            or source_version is None
            or recovery_family is not None
        ):
            raise ValueError(
                "New recovery family requires all family flags, --source-version and --receipt; existing --recovery-family is mutually exclusive"
            )
        if family_receipt is not None and (
            not family_receipt.is_absolute()
            or family_receipt.exists()
            or family_receipt == receipt
        ):
            raise ValueError("--family-receipt must name a distinct absolute new file")
        if recovery_family is not None and source_version is None:
            raise ValueError(
                "--recovery-family requires an explicit --source-version contract"
            )
        if family_receipts_dir is not None and any(
            not path.is_absolute() or not path.is_dir() for path in family_receipts_dir
        ):
            raise ValueError(
                "Family receipt directories must explicitly name existing absolute directories"
            )
        family = _read(recovery_family) if recovery_family is not None else None
        cfg, target = _resolve(sequence_mappings)
        with operator_connection(
            cfg, isolation_level="REPEATABLE READ", read_only=True
        ) as connection:
            if prefix is not None:
                from daylily_tapdb.identity_prefix_inventory import capture_prefix_usage

                payload = capture_prefix_usage(
                    connection, schema_name=target["schema_name"], target=target, prefix=prefix,
                    allocation_receipts_dirs=allocation_receipts_dirs or (),
                )
            elif source_version is not None:
                from daylily_tapdb.backup.source_contract import (
                    capture_source_contract,
                    contract_hash,
                    validate_source_contract,
                )

                payload = capture_source_contract(
                    connection,
                    schema_name=target["schema_name"],
                    target=target,
                    source_version=source_version,
                    **({"recovery_family": family} if family is not None else {}),
                )
                if creating_family:
                    from daylily_tapdb.backup.recovery import build_recovery_family

                    if new_recovery_family_id is None or family_receipts_dir is None:
                        raise ValueError("New recovery family inputs must be explicit")
                    family = build_recovery_family(
                        family_id=new_recovery_family_id,
                        origin_inventory=payload["sequence_inventory"],
                        receipts_dirs=family_receipts_dir,
                    )
                    payload["recovery_family"] = family
                    payload["sha256"] = contract_hash(payload)
                    validate_source_contract(payload)
            else:
                payload = capture_identity_inventory(
                    connection, schema_name=target["schema_name"], target=target
                )
        if family_receipt is not None:
            if family is None:
                raise ValueError("Recovery family creation did not return a descriptor")
            write_json_receipt(family_receipt, family)
        if receipt is not None:
            write_json_receipt(receipt, payload)
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(
        {"receipt": str(receipt), "schema_version": payload["schema_version"],
         "sha256": payload["sha256"]}
        if receipt_only else payload
    )


@identity_app.command("verify")
def verify(
    before: Path = typer.Option(
        ..., "--before", help="Absolute original identity/source receipt"
    ),
    after: Path | None = typer.Option(
        None, "--after", help="Absolute final receipt; omit for live read-only capture"
    ),
    conversion_manifest: Path | None = typer.Option(
        None, "--conversion-manifest", help="Absolute declared conversion manifest"
    ),
    sequence_mappings: Path | None = typer.Option(
        None, "--sequence-mappings", help="Absolute explicit allocator mappings JSON"
    ),
) -> None:
    """Prove preserved identities and reject undeclared conversions."""
    try:
        cfg, target = _resolve(sequence_mappings)
        original = _identity(_read(before))
        target = with_inventory_limits(target, original)
        if after is None:
            with operator_connection(
                cfg, isolation_level="REPEATABLE READ", read_only=True
            ) as connection:
                final = capture_identity_inventory(
                    connection, schema_name=target["schema_name"], target=target
                )
        else:
            final = _identity(_read(after))
            if final["target"] != validate_target(target, target["schema_name"]):
                raise ValueError(
                    "Final receipt does not match the explicit config target"
                )
        conversion = (
            _read(conversion_manifest) if conversion_manifest is not None else None
        )
        result = verify_identity_inventory(
            original, final, conversion_manifest=conversion
        )
    except Exception as exc:
        _fail(exc)
        return
    ccyo_out.emit_json(result)
    if not result["ok"]:
        raise SystemExit(1)
