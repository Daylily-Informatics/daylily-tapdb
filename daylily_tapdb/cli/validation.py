"""Validation and repair CLI commands."""

from __future__ import annotations

import json
from typing import Optional

import typer
from cli_core_yo import ccyo_out

from daylily_tapdb.cli.db_config import get_config_path, get_db_config
from daylily_tapdb.validation import (
    assess_object,
    create_repair_record,
    editor_data_for_object,
)
from daylily_tapdb.web.runtime import get_db

validation_app = typer.Typer(help="Ephemeral evidence validation commands")
repair_app = typer.Typer(help="Explicit repair evidence commands")


def _print_payload(payload: dict) -> None:
    ccyo_out.print_text(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _read_json_object(raw: str, *, label: str) -> dict:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{label} invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise typer.BadParameter(f"{label} must be a JSON object")
    return payload


@validation_app.command("assess")
def assess(
    euid: str = typer.Argument(..., help="TapDB object EUID to assess"),
    validator_ref: Optional[str] = typer.Option(
        None,
        "--validator-ref",
        help="Override the object's configured validator reference",
    ),
) -> None:
    """Assess one object without persisting an assessment row."""

    config_path = str(get_config_path())
    with get_db(config_path) as conn:
        conn.app_username = "tapdb-cli"
        with conn.session_scope(commit=False) as session:
            try:
                assessment = assess_object(
                    session,
                    euid,
                    validator_ref=validator_ref,
                    context={"surface": "tapdb_cli"},
                )
            except LookupError as exc:
                ccyo_out.error(str(exc))
                raise typer.Exit(1) from exc
    _print_payload(assessment.to_dict())


@validation_app.command("revalidate")
def revalidate(
    euid: str = typer.Argument(..., help="TapDB object EUID to revalidate"),
    validator_ref: Optional[str] = typer.Option(
        None,
        "--validator-ref",
        help="Override the object's configured validator reference",
    ),
) -> None:
    """Re-run validation without persisting an assessment row."""

    config_path = str(get_config_path())
    with get_db(config_path) as conn:
        conn.app_username = "tapdb-cli"
        with conn.session_scope(commit=False) as session:
            try:
                assessment = assess_object(
                    session,
                    euid,
                    validator_ref=validator_ref,
                    context={"surface": "tapdb_cli", "operation": "revalidate"},
                )
            except LookupError as exc:
                ccyo_out.error(str(exc))
                raise typer.Exit(1) from exc
    _print_payload({"revalidated": True, "assessment": assessment.to_dict()})


@validation_app.command("editor-data")
def editor_data(
    euid: str = typer.Argument(..., help="TapDB object EUID to describe"),
    validator_ref: Optional[str] = typer.Option(
        None,
        "--validator-ref",
        help="Override the object's configured validator reference",
    ),
) -> None:
    """Emit editor metadata for raw, structured, and split views."""

    config_path = str(get_config_path())
    with get_db(config_path) as conn:
        conn.app_username = "tapdb-cli"
        with conn.session_scope(commit=False) as session:
            try:
                payload = editor_data_for_object(
                    session,
                    euid,
                    validator_ref=validator_ref,
                    context={"surface": "tapdb_cli"},
                )
            except LookupError as exc:
                ccyo_out.error(str(exc))
                raise typer.Exit(1) from exc
    _print_payload(payload)


@repair_app.command("create")
def create(
    euid: str = typer.Argument(..., help="Subject TapDB object EUID"),
    reason: str = typer.Option(..., "--reason", help="Human reason for repair"),
    payload_json: str = typer.Option(
        ...,
        "--payload-json",
        help="Repair payload JSON object; the subject is not mutated",
    ),
    actor: str = typer.Option(
        "tapdb-cli",
        "--actor",
        help="Actor identifier recorded on the repair evidence object",
    ),
) -> None:
    """Create an explicit repair evidence object without mutating the subject."""

    repair_payload = _read_json_object(payload_json, label="payload_json")
    config_path = str(get_config_path())
    cfg = get_db_config(config_path=config_path)
    with get_db(config_path) as conn:
        conn.app_username = actor
        with conn.session_scope(commit=True) as session:
            try:
                result = create_repair_record(
                    session,
                    domain_code=str(cfg.get("domain_code") or ""),
                    subject_euid=euid,
                    actor=actor,
                    reason=reason,
                    repair_payload=repair_payload,
                    governance_context={"surface": "tapdb_cli"},
                )
            except (LookupError, ValueError) as exc:
                ccyo_out.error(str(exc))
                raise typer.Exit(1) from exc
    _print_payload(result)
