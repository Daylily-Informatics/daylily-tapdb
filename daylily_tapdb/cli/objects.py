"""Governed TapDB object operator CLI."""

from __future__ import annotations

import getpass
import json
from typing import Any

import typer

from daylily_tapdb.cli.db import Environment, _tapdb_connection_for_env
from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.services.object_operations import (
    ObjectSelector,
    get_object,
    repair_object,
    soft_delete_object,
    update_object,
)
from daylily_tapdb.services.object_search import search_objects

objects_app = typer.Typer(help="Exact-selector object reads and governed mutations")


def _emit(payload: dict[str, Any]) -> None:
    typer.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _actor(value: str) -> str:
    return value.strip() or getpass.getuser()


def _dry_run_requested() -> bool:
    from cli_core_yo.runtime import get_context

    return bool(get_context().dry_run)


def _selector(
    *, euid: str, machine_uuid: str, uid: int | None, record_type: str
) -> ObjectSelector:
    return ObjectSelector(
        euid=euid.strip() or None,
        machine_uuid=machine_uuid.strip() or None,
        uid=uid,
        record_type=record_type.strip() or None,
    ).validated()


def _changes(values: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in values:
        if "=" not in item:
            raise typer.BadParameter("--set requires FIELD=JSON_VALUE")
        field, raw = item.split("=", 1)
        field = field.strip()
        if not field:
            raise typer.BadParameter("--set field may not be blank")
        try:
            result[field] = json.loads(raw)
        except json.JSONDecodeError:
            result[field] = raw
    return result


def _connection(actor: str):
    return _tapdb_connection_for_env(Environment.target, app_username=actor)


@objects_app.command("search")
def objects_search(
    q: str = typer.Option("", "--query", "-q"),
    euid: str = typer.Option("", "--euid"),
    record_type: str = typer.Option("all", "--record-type"),
    category: str = typer.Option("", "--category"),
    type_name: str = typer.Option("", "--type"),
    subtype: str = typer.Option("", "--subtype"),
    tenant_id: str = typer.Option("", "--tenant-id"),
    relationship_type: str = typer.Option("", "--relationship-type"),
    limit: int = typer.Option(25, "--limit", min=1, max=100),
    cursor: str = typer.Option("", "--cursor"),
    actor: str = typer.Option("", "--actor"),
) -> None:
    cfg = get_db_config()
    effective_actor = _actor(actor)
    with _connection(effective_actor) as conn:
        with conn.session_scope(commit=False) as session:
            payload = search_objects(
                session,
                service_name=cfg["client_id"],
                q=q,
                euid=euid,
                record_type=record_type,
                category=category,
                type_name=type_name,
                subtype=subtype,
                tenant_id=tenant_id,
                relationship_type=relationship_type,
                limit=limit,
                cursor=cursor,
            )
    _emit(payload)


def _selector_options(
    euid: str,
    machine_uuid: str,
    uid: int | None,
    record_type: str,
) -> ObjectSelector:
    return _selector(
        euid=euid, machine_uuid=machine_uuid, uid=uid, record_type=record_type
    )


@objects_app.command("get")
def objects_get(
    euid: str = typer.Option("", "--euid"),
    machine_uuid: str = typer.Option("", "--machine-uuid"),
    uid: int | None = typer.Option(None, "--uid", min=1),
    record_type: str = typer.Option("", "--record-type"),
    include_deleted: bool = typer.Option(False, "--include-deleted"),
    actor: str = typer.Option("", "--actor"),
) -> None:
    selector = _selector_options(euid, machine_uuid, uid, record_type)
    effective_actor = _actor(actor)
    with _connection(effective_actor) as conn:
        with conn.session_scope(commit=False) as session:
            payload = get_object(session, selector, include_deleted=include_deleted)
    _emit(payload)


@objects_app.command("update")
def objects_update(
    set_values: list[str] = typer.Option(..., "--set", help="FIELD=JSON_VALUE"),
    euid: str = typer.Option("", "--euid"),
    machine_uuid: str = typer.Option("", "--machine-uuid"),
    uid: int | None = typer.Option(None, "--uid", min=1),
    record_type: str = typer.Option("", "--record-type"),
    apply: bool = typer.Option(False, "--apply"),
    actor: str = typer.Option("", "--actor"),
) -> None:
    effective_apply = apply and not _dry_run_requested()
    selector = _selector_options(euid, machine_uuid, uid, record_type)
    effective_actor = _actor(actor)
    with _connection(effective_actor) as conn:
        with conn.session_scope(commit=effective_apply) as session:
            payload = update_object(
                session,
                selector,
                _changes(set_values),
                actor=effective_actor,
                dry_run=not effective_apply,
            )
    _emit(payload)


@objects_app.command("repair")
def objects_repair(
    reason: str = typer.Option(..., "--reason"),
    repair_json: str = typer.Option(..., "--repair-json"),
    euid: str = typer.Option("", "--euid"),
    machine_uuid: str = typer.Option("", "--machine-uuid"),
    uid: int | None = typer.Option(None, "--uid", min=1),
    record_type: str = typer.Option("", "--record-type"),
    apply: bool = typer.Option(False, "--apply"),
    actor: str = typer.Option("", "--actor"),
) -> None:
    effective_apply = apply and not _dry_run_requested()
    selector = _selector_options(euid, machine_uuid, uid, record_type)
    try:
        repair_payload = json.loads(repair_json)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter("--repair-json must be valid JSON") from exc
    if not isinstance(repair_payload, dict):
        raise typer.BadParameter("--repair-json must contain a JSON object")
    cfg = get_db_config()
    effective_actor = _actor(actor)
    with _connection(effective_actor) as conn:
        with conn.session_scope(commit=effective_apply) as session:
            payload = repair_object(
                session,
                selector,
                domain_code=cfg["domain_code"],
                actor=effective_actor,
                reason=reason,
                repair_payload=repair_payload,
                dry_run=not effective_apply,
            )
    _emit(payload)


@objects_app.command("delete")
def objects_delete(
    euid: str = typer.Option("", "--euid"),
    machine_uuid: str = typer.Option("", "--machine-uuid"),
    uid: int | None = typer.Option(None, "--uid", min=1),
    record_type: str = typer.Option("", "--record-type"),
    apply: bool = typer.Option(False, "--apply"),
    actor: str = typer.Option("", "--actor"),
) -> None:
    effective_apply = apply and not _dry_run_requested()
    selector = _selector_options(euid, machine_uuid, uid, record_type)
    effective_actor = _actor(actor)
    with _connection(effective_actor) as conn:
        with conn.session_scope(commit=effective_apply) as session:
            payload = soft_delete_object(
                session,
                selector,
                actor=effective_actor,
                dry_run=not effective_apply,
            )
    _emit(payload)


__all__ = ["objects_app"]
