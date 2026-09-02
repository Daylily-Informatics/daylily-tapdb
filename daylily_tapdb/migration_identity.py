"""Fail-closed, identity-preserving PostgreSQL migration execution."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, cast
from uuid import UUID

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from daylily_tapdb.advisory_locks import derive_advisory_lock_key
from daylily_tapdb.security_context import assert_operator_role

RECEIPT_VERSION = "tapdb-migration-receipt/v1"
_TRACKING_TABLE = "_tapdb_migrations"
_TABLES = (
    "generic_template",
    "generic_instance",
    "generic_instance_lineage",
    "audit_log",
    "outbox_event",
    "outbox_event_attempt",
    "inbox_message",
    "tapdb_identity_prefix_config",
    "tapdb_legacy_outbox_mapping",
)
_IMMUTABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "generic_template": (
        "uid",
        "euid",
        "euid_prefix",
        "euid_seq",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "polymorphic_discriminator",
        "category",
        "type",
        "subtype",
        "version",
        "instance_prefix",
        "created_dt",
    ),
    "generic_instance": (
        "uid",
        "euid",
        "euid_prefix",
        "euid_seq",
        "machine_uuid",
        "identity_key",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "polymorphic_discriminator",
        "category",
        "type",
        "subtype",
        "version",
        "template_uid",
        "created_dt",
    ),
    "generic_instance_lineage": (
        "uid",
        "euid",
        "euid_prefix",
        "euid_seq",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "parent_instance_uid",
        "child_instance_uid",
        "relationship_type",
        "created_dt",
    ),
    "audit_log": (
        "uid",
        "euid",
        "euid_prefix",
        "euid_seq",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "rel_table_name",
        "rel_table_uid_fk",
        "rel_table_euid_fk",
        "changed_at",
    ),
    "outbox_event": (
        "id",
        "event_id",
        "message_uid",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "destination",
        "dedupe_key",
        "created_dt",
        "receipt_machine_uuid",
        "claim_token",
    ),
    "outbox_event_attempt": (
        "uid",
        "outbox_event_id",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "attempt_no",
        "claim_token",
        "attempt_started_dt",
        "receipt_machine_uuid",
    ),
    "inbox_message": (
        "uid",
        "message_machine_uuid",
        "receipt_machine_uuid",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "source_domain_code",
        "source_issuer_app_code",
        "received_dt",
    ),
    "tapdb_identity_prefix_config": (
        "entity",
        "domain_code",
        "issuer_app_code",
        "prefix",
    ),
    "tapdb_legacy_outbox_mapping": (
        "old_outbox_id",
        "old_event_id",
        "message_uid",
        "message_euid",
        "message_euid_seq",
        "source_sha256",
        "mapped_dt",
    ),
}
_ALLOW_COLUMN_RE = re.compile(
    r"^\s*--\s*tapdb-allow-column:\s*([a-z0-9_]+\.[a-z0-9_]+)\s*$", re.M
)
_ALLOW_NEW_ROWS_RE = re.compile(
    r"^\s*--\s*tapdb-allow-new-rows:\s*([a-z0-9_]+)\s*$", re.M
)
_ALLOW_SEQUENCE_RE = re.compile(
    r"^\s*--\s*tapdb-allow-sequence:\s*([a-z0-9_]+)\s*$", re.M
)
_TRANSFORMATION_RE = re.compile(r"^\s*--\s*tapdb-transformation:\s*(\S.+?)\s*$", re.M)
_INCLUDE_RE = re.compile(r"^\s*--\s*tapdb-include:\s*(\S+)\s*$", re.M)
_TX_CONTROL_RE = re.compile(r"^\s*(?:BEGIN|COMMIT)\s*;\s*(?:--.*)?$", re.I | re.M)


@dataclass(frozen=True)
class _TransformationContract:
    column: str
    kind: str
    replacement: str | None = None
    requires_allow_column: bool = True


_TRANSFORMATION_CONTRACTS = {
    "generic_template.validator_ref:null_or_empty_to_universal_pass_v1": (
        _TransformationContract(
            column="generic_template.validator_ref",
            kind="null-or-empty-to-literal",
            replacement="UNIVERSAL_PASS@1",
        )
    ),
    "audit_log.changed_by:null_or_empty_to_pre92_unattributed_v1": (
        _TransformationContract(
            column="audit_log.changed_by",
            kind="null-or-empty-to-literal",
            replacement="migration:pre-9.2-unattributed",
        )
    ),
    "outbox_event.message_uid:null_to_legacy_mapping_v1": (
        _TransformationContract(
            column="outbox_event.message_uid",
            kind="null-to-legacy-outbox-mapping",
            requires_allow_column=False,
        )
    ),
}


class MigrationPreflightError(RuntimeError):
    """The target cannot be proven safe for migration."""


class MigrationReceiptMismatchError(MigrationPreflightError):
    """Live migration evidence no longer matches the approved preflight."""


@dataclass(frozen=True)
class MigrationResult:
    receipt: dict[str, Any]


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (UUID, Decimal, date, datetime)):
        return value.isoformat() if isinstance(value, (date, datetime)) else str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return str(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _validated_transformation_contracts(
    *,
    allowed_columns: list[str],
    transformations: list[str],
) -> dict[str, _TransformationContract]:
    immutable_columns = {
        f"{table_name}.{column}"
        for table_name, columns in _IMMUTABLE_COLUMNS.items()
        for column in columns
    }
    forbidden = sorted(set(allowed_columns) & immutable_columns)
    if forbidden:
        raise MigrationPreflightError(
            "immutable columns cannot be allowlisted: " + ", ".join(forbidden)
        )

    unknown = sorted(
        marker for marker in transformations if marker not in _TRANSFORMATION_CONTRACTS
    )
    if unknown:
        raise MigrationPreflightError(
            "unknown migration transformation contract: " + ", ".join(unknown)
        )

    contracts = {
        marker: _TRANSFORMATION_CONTRACTS[marker] for marker in transformations
    }
    paired_columns = {
        contract.column
        for contract in contracts.values()
        if contract.requires_allow_column
    }
    declared_columns = set(allowed_columns)
    missing_contracts = sorted(declared_columns - paired_columns)
    if missing_contracts:
        raise MigrationPreflightError(
            "allowed columns require an exact transformation contract: "
            + ", ".join(missing_contracts)
        )
    unused_contracts = sorted(paired_columns - declared_columns)
    if unused_contracts:
        raise MigrationPreflightError(
            "unused migration transformation contract: " + ", ".join(unused_contracts)
        )
    return contracts


def _expand_migration_source(
    path: Path, *, schema_root: Path, active_paths: tuple[Path, ...] = ()
) -> str:
    """Expand constrained SQL includes for runner-native migration execution."""
    resolved = path.resolve()
    if resolved in active_paths:
        raise MigrationPreflightError(f"cyclic migration include: {resolved}")
    try:
        resolved.relative_to(schema_root)
    except ValueError as exc:
        raise MigrationPreflightError(
            f"migration include escapes schema root: {resolved}"
        ) from exc
    if resolved.suffix != ".sql" or not resolved.is_file():
        raise MigrationPreflightError(
            f"migration include is not a SQL file: {resolved}"
        )
    source = resolved.read_text(encoding="utf-8")

    def expand(match: re.Match[str]) -> str:
        include_path = Path(match.group(1))
        if include_path.is_absolute():
            raise MigrationPreflightError(
                f"absolute migration include is forbidden: {include_path}"
            )
        return _expand_migration_source(
            resolved.parent / include_path,
            schema_root=schema_root,
            active_paths=(*active_paths, resolved),
        )

    return _INCLUDE_RE.sub(expand, source)


def _migration_assets(migrations_dir: Path) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    schema_root = migrations_dir.resolve().parent
    for path in sorted(migrations_dir.glob("*.sql")):
        source = path.read_text(encoding="utf-8")
        expanded_source = _expand_migration_source(path, schema_root=schema_root)
        allowed_columns = sorted(set(_ALLOW_COLUMN_RE.findall(source)))
        allowed_transformations = sorted(set(_TRANSFORMATION_RE.findall(source)))
        _validated_transformation_contracts(
            allowed_columns=allowed_columns,
            transformations=allowed_transformations,
        )
        assets.append(
            {
                "filename": path.name,
                "sha256": hashlib.sha256(expanded_source.encode("utf-8")).hexdigest(),
                "path": str(path.resolve()),
                "expanded_source": expanded_source,
                "allowed_columns": allowed_columns,
                "allowed_new_rows": sorted(set(_ALLOW_NEW_ROWS_RE.findall(source))),
                "allowed_sequences": sorted(set(_ALLOW_SEQUENCE_RE.findall(source))),
                "allowed_transformations": allowed_transformations,
            }
        )
    return assets


def _tracking_rows(connection: Connection) -> list[dict[str, Any]]:
    exists = connection.execute(
        text("SELECT to_regclass(:name) IS NOT NULL"), {"name": _TRACKING_TABLE}
    ).scalar_one()
    if not exists:
        raise MigrationPreflightError(
            "_tapdb_migrations is missing; apply the base schema first"
        )
    return [
        dict(row)
        for row in connection.execute(
            text("SELECT filename, applied_at FROM _tapdb_migrations ORDER BY filename")
        ).mappings()
    ]


def _apply_operator_context(connection: Connection, target: Mapping[str, Any]) -> None:
    if not connection.in_transaction():
        raise MigrationPreflightError(
            "migration inspection requires an active transaction"
        )
    settings = (
        ("TimeZone", "UTC"),
        ("search_path", target.get("schema_name")),
        ("session.current_config_identity", target.get("config_identity")),
        ("session.current_schema_name", target.get("schema_name")),
        ("session.current_domain_code", target.get("domain_code")),
        ("session.current_owner_repo_name", target.get("owner_repo_name")),
        ("session.current_tenant_id", ""),
        ("session.current_username", "migration:tapdb-9.2"),
        ("session.allow_global_rows", "true"),
    )
    for name, raw_value in settings:
        value = str(raw_value or "")
        if name not in {"session.current_tenant_id"} and not value.strip():
            raise MigrationPreflightError(
                f"operator transaction context is missing {name}"
            )
        connection.execute(
            text("SELECT set_config(:name, :value, true)"),
            {"name": name, "value": value},
        )
    try:
        assert_operator_role(connection)
    except RuntimeError as exc:
        raise MigrationPreflightError(str(exc)) from exc


def _table_snapshot(
    connection: Connection, schema_name: str, table_name: str
) -> dict[str, Any]:
    inspector = inspect(connection)
    columns = [
        str(item["name"])
        for item in inspector.get_columns(table_name, schema=schema_name)
    ]
    pk_columns = [
        str(item)
        for item in (
            inspector.get_pk_constraint(table_name, schema=schema_name).get(
                "constrained_columns"
            )
            or []
        )
    ]
    if not pk_columns:
        raise MigrationPreflightError(f"{table_name} has no primary key")
    selected = ", ".join(_quote_identifier(column) for column in columns)
    ordering = ", ".join(_quote_identifier(column) for column in pk_columns)
    qualified = f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"
    rows = (
        connection.execute(
            text(f"SELECT {selected} FROM {qualified} ORDER BY {ordering}")
        )
        .mappings()
        .all()
    )
    immutable_columns = [
        column for column in _IMMUTABLE_COLUMNS.get(table_name, ()) if column in columns
    ]
    evidence_rows: list[dict[str, Any]] = []
    for row in rows:
        key = [_jsonable(row[column]) for column in pk_columns]
        immutable = [_jsonable(row[column]) for column in immutable_columns]
        identity = {column: _jsonable(row[column]) for column in immutable_columns}
        column_sha256 = {column: _sha256(_jsonable(row[column])) for column in columns}
        evidence_rows.append(
            {
                "key": key,
                "immutable": immutable,
                "identity": identity,
                "column_sha256": column_sha256,
            }
        )
    active = None
    deleted = None
    if "is_deleted" in columns:
        active = sum(not bool(row["is_deleted"]) for row in rows)
        deleted = sum(bool(row["is_deleted"]) for row in rows)
    foreign_keys = []
    for fk in inspector.get_foreign_keys(table_name, schema=schema_name):
        constrained = [str(item) for item in fk.get("constrained_columns") or []]
        for row in rows:
            foreign_keys.append(
                {
                    "columns": constrained,
                    "values": [_jsonable(row[column]) for column in constrained],
                    "target_table": fk.get("referred_table"),
                    "target_columns": fk.get("referred_columns") or [],
                }
            )
    immutable_tuples = [row["immutable"] for row in evidence_rows]
    return {
        "columns": columns,
        "primary_key": pk_columns,
        "row_count": len(rows),
        "active_count": active,
        "soft_deleted_count": deleted,
        "immutable_columns": immutable_columns,
        "immutable_tuples": immutable_tuples,
        "immutable_sha256": _sha256(immutable_tuples),
        "full_rows_sha256": _sha256([row["column_sha256"] for row in evidence_rows]),
        "rows": evidence_rows,
        "foreign_keys": foreign_keys,
        "foreign_keys_sha256": _sha256(foreign_keys),
    }


def _sequence_snapshot(
    connection: Connection, schema_name: str
) -> list[dict[str, Any]]:
    rows = (
        connection.execute(
            text(
                "SELECT cls.relname AS sequence_name, seq.seqstart AS start_value, "
                "seq.seqmin AS minimum_value, seq.seqmax AS maximum_value, "
                "seq.seqincrement AS increment, seq.seqcycle AS cycle, "
                "seq.seqcache AS cache_size "
                "FROM pg_class cls "
                "JOIN pg_namespace ns ON ns.oid = cls.relnamespace "
                "JOIN pg_sequence seq ON seq.seqrelid = cls.oid "
                "WHERE ns.nspname = :schema AND cls.relkind = 'S' "
                "ORDER BY cls.relname"
            ),
            {"schema": schema_name},
        )
        .mappings()
        .all()
    )
    evidence = []
    for row in rows:
        sequence_name = str(row["sequence_name"])
        qualified = (
            f"{_quote_identifier(schema_name)}.{_quote_identifier(sequence_name)}"
        )
        state = (
            connection.execute(text(f"SELECT last_value, is_called FROM {qualified}"))
            .mappings()
            .one()
        )
        owner = (
            connection.execute(
                text(
                    "SELECT tbl.relname AS table_name, att.attname AS column_name "
                    "FROM pg_class seq JOIN pg_namespace ns ON ns.oid = seq.relnamespace "
                    "JOIN pg_depend dep ON dep.objid = seq.oid AND dep.deptype IN ('a','i') "
                    "JOIN pg_class tbl ON tbl.oid = dep.refobjid "
                    "JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = dep.refobjsubid "
                    "WHERE ns.nspname = :schema AND seq.relname = :sequence"
                ),
                {"schema": schema_name, "sequence": sequence_name},
            )
            .mappings()
            .first()
        )
        # PostgreSQL identity-column dependency metadata differs from serial
        # columns across supported server versions. Resolve the conventional
        # generated ``<table>_uid_seq`` name when the catalog join does not
        # expose an owner row.
        if owner is None and sequence_name.endswith("_uid_seq"):
            candidate_table = sequence_name[: -len("_uid_seq")]
            candidate_exists = connection.execute(
                text("SELECT to_regclass(:table_name) IS NOT NULL"),
                {"table_name": f"{schema_name}.{candidate_table}"},
            ).scalar_one()
            if candidate_exists:
                owner = {"table_name": candidate_table, "column_name": "uid"}
        evidence.append(
            {
                "name": sequence_name,
                "owner_table": owner["table_name"] if owner else None,
                "owner_column": owner["column_name"] if owner else None,
                "last_value": int(state["last_value"]),
                "is_called": bool(state["is_called"]),
                "start_value": int(row["start_value"]),
                "minimum_value": int(row["minimum_value"]),
                "maximum_value": int(row["maximum_value"]),
                "increment": int(row["increment"]),
                "cycle": bool(row["cycle"]),
                "cache_size": int(row["cache_size"]),
            }
        )
    return evidence


def _validate_scope_and_sequences(
    snapshot: dict[str, Any], *, validate_generators: bool = True
) -> None:
    for table_name in (
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
        "outbox_event",
        "outbox_event_attempt",
        "inbox_message",
        "tapdb_identity_prefix_config",
    ):
        table = snapshot["tables"].get(table_name)
        if not table:
            continue
        for row in table["rows"]:
            values = row["identity"]
            for column in ("domain_code", "issuer_app_code"):
                if column in values and not str(values[column] or "").strip():
                    raise MigrationPreflightError(
                        f"{table_name} row {row['key']} has missing {column}"
                    )

    if not validate_generators:
        return

    sequences = {item["name"]: item for item in snapshot["sequences"]}
    assigned_prefixes: set[str] = set()
    for identity_table in (
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
    ):
        for row in snapshot["tables"].get(identity_table, {}).get("rows", []):
            prefix = str(row["identity"].get("euid_prefix") or "").lower()
            if prefix:
                assigned_prefixes.add(prefix)
    missing_generators = sorted(
        f"{prefix}_instance_seq"
        for prefix in assigned_prefixes
        if f"{prefix}_instance_seq" not in sequences
    )
    if missing_generators:
        raise MigrationPreflightError(
            "missing identity sequence(s): " + ", ".join(missing_generators)
        )

    for sequence in sequences.values():
        table_name = sequence["owner_table"]
        column_name = sequence["owner_column"]
        table = snapshot["tables"].get(str(table_name))
        assigned: list[int] = []
        if table and column_name in table["columns"]:
            assigned = [
                int(row["identity"][column_name])
                for row in table["rows"]
                if row["identity"].get(column_name) is not None
            ]
        elif str(sequence["name"]).endswith("_instance_seq"):
            prefix = str(sequence["name"])[: -len("_instance_seq")].upper()
            for identity_table in (
                "generic_template",
                "generic_instance",
                "generic_instance_lineage",
                "audit_log",
            ):
                for row in snapshot["tables"].get(identity_table, {}).get("rows", []):
                    identity = row["identity"]
                    if (
                        str(identity.get("euid_prefix") or "").upper() == prefix
                        and identity.get("euid_seq") is not None
                    ):
                        assigned.append(int(identity["euid_seq"]))
        if not assigned:
            continue
        if int(sequence["increment"]) <= 0:
            raise MigrationPreflightError(
                f"identity sequence {sequence['name']} must have a positive increment"
            )
        if bool(sequence["cycle"]):
            raise MigrationPreflightError(
                f"identity sequence {sequence['name']} must not cycle"
            )
        if int(sequence["cache_size"]) != 1:
            raise MigrationPreflightError(
                f"identity sequence {sequence['name']} has ambiguous cached state"
            )
        next_value = sequence["last_value"] + (
            sequence["increment"] if sequence["is_called"] else 0
        )
        if next_value <= max(int(value) for value in assigned):
            raise MigrationPreflightError(
                f"sequence {sequence['name']} is behind assigned identity values"
            )


def build_migration_preflight(
    connection: Connection,
    *,
    migrations_dir: Path,
    target: Mapping[str, Any],
    _validate_generators: bool = True,
) -> dict[str, Any]:
    """Capture deterministic, sanitized preflight evidence without mutation."""
    schema_name = str(target.get("schema_name") or "").strip()
    if not schema_name:
        raise MigrationPreflightError("schema_name is required")
    _apply_operator_context(connection, target)
    live_schema = str(
        connection.execute(text("SELECT current_schema()")).scalar_one() or ""
    )
    if live_schema != schema_name:
        raise MigrationPreflightError(
            f"current schema {live_schema!r} does not match configured {schema_name!r}"
        )
    assets = _migration_assets(migrations_dir)
    tracking = _tracking_rows(connection)
    applied_names = {str(row["filename"]) for row in tracking}
    pending = [
        {
            key: value
            for key, value in item.items()
            if key not in {"path", "expanded_source"}
        }
        for item in assets
        if item["filename"] not in applied_names
    ]
    table_names = set(inspect(connection).get_table_names(schema=schema_name))
    tables = {
        name: _table_snapshot(connection, schema_name, name)
        for name in _TABLES
        if name in table_names
    }
    prefixes = tables.get("tapdb_identity_prefix_config", {}).get("rows", [])
    template_prefix_mapping = [
        {
            key: row["identity"].get(key)
            for key in (
                "uid",
                "domain_code",
                "issuer_app_code",
                "instance_prefix",
            )
        }
        for row in tables.get("generic_template", {}).get("rows", [])
    ]
    snapshot: dict[str, Any] = {
        "receipt_version": RECEIPT_VERSION,
        "target": {
            key: str(target[key])
            for key in (
                "engine_type",
                "host",
                "port",
                "database",
                "schema_name",
                "config_identity",
                "domain_code",
                "owner_repo_name",
            )
            if target.get(key) is not None
        },
        "pending_migrations": pending,
        "applied_migrations": [
            {
                "filename": row["filename"],
                "asset_sha256": next(
                    (
                        asset["sha256"]
                        for asset in assets
                        if asset["filename"] == row["filename"]
                    ),
                    None,
                ),
            }
            for row in tracking
        ],
        "tables": tables,
        "sequences": _sequence_snapshot(connection, schema_name),
        "prefix_configuration": [row["identity"] for row in prefixes],
        "template_instance_prefix_mapping": template_prefix_mapping,
    }
    _validate_scope_and_sequences(snapshot, validate_generators=_validate_generators)
    snapshot["evidence_sha256"] = _sha256(snapshot)
    return snapshot


def _receipt_comparable(receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: receipt[key]
        for key in (
            "receipt_version",
            "target",
            "pending_migrations",
            "applied_migrations",
            "tables",
            "sequences",
            "prefix_configuration",
            "template_instance_prefix_mapping",
        )
    }


def _strip_transaction_control(source: str) -> str:
    return _TX_CONTROL_RE.sub("", source)


def _verify_legacy_outbox_message_uid_transition(
    *,
    old_row: Mapping[str, Any],
    new_row: Mapping[str, Any],
    postflight: Mapping[str, Any],
) -> None:
    old_identity = old_row.get("identity", {})
    new_identity = new_row.get("identity", {})
    old_outbox_id = old_identity.get("id")
    if old_outbox_id is None:
        old_outbox_id = old_row["key"][0]
    old_message_uid = old_identity.get("message_uid")
    new_message_uid = new_identity.get("message_uid")

    if old_message_uid is not None:
        if new_message_uid != old_message_uid:
            raise MigrationReceiptMismatchError(
                "pre-existing outbox_event.message_uid changed for row "
                f"{old_row['key']}"
            )
        return
    if new_message_uid is None:
        raise MigrationReceiptMismatchError(
            f"legacy outbox_event.message_uid was not assigned for row {old_row['key']}"
        )

    mappings = (
        postflight["tables"].get("tapdb_legacy_outbox_mapping", {}).get("rows", [])
    )
    matches = [
        row
        for row in mappings
        if row.get("identity", {}).get("old_outbox_id") == old_outbox_id
    ]
    if len(matches) != 1 or (
        matches[0].get("identity", {}).get("message_uid") != new_message_uid
    ):
        raise MigrationReceiptMismatchError(
            "legacy outbox_event.message_uid does not match its preserved "
            f"old_outbox_id mapping for row {old_row['key']}"
        )


def _verify_preservation(
    preflight: Mapping[str, Any],
    postflight: Mapping[str, Any],
    *,
    allow_declared_sequence_advances: bool = True,
) -> None:
    allowed_columns = sorted(
        {
            value
            for item in preflight["pending_migrations"]
            for value in item.get("allowed_columns", [])
        }
    )
    transformation_markers = sorted(
        {
            value
            for item in preflight["pending_migrations"]
            for value in item.get("allowed_transformations", [])
        }
    )
    contracts = _validated_transformation_contracts(
        allowed_columns=allowed_columns,
        transformations=transformation_markers,
    )
    contracts_by_column = {contract.column: contract for contract in contracts.values()}
    immutable_columns = {
        f"{table_name}.{column}"
        for table_name, columns in _IMMUTABLE_COLUMNS.items()
        for column in columns
    }
    allowed_new_rows = {
        value
        for item in preflight["pending_migrations"]
        for value in item.get("allowed_new_rows", [])
    }
    for table_name, before in preflight["tables"].items():
        after = postflight["tables"].get(table_name)
        if after is None:
            raise MigrationReceiptMismatchError(
                f"table disappeared during migration: {table_name}"
            )
        before_rows = {_canonical_json(row["key"]): row for row in before["rows"]}
        after_rows = {_canonical_json(row["key"]): row for row in after["rows"]}
        if table_name not in allowed_new_rows and set(before_rows) != set(after_rows):
            raise MigrationReceiptMismatchError(f"row keys changed in {table_name}")
        if not set(before_rows).issubset(after_rows):
            raise MigrationReceiptMismatchError(
                f"pre-existing rows disappeared from {table_name}"
            )
        for key, old_row in before_rows.items():
            new_row = after_rows[key]
            for column, old_value in old_row["column_sha256"].items():
                qualified_column = f"{table_name}.{column}"
                new_value = new_row["column_sha256"].get(column)
                contract = contracts_by_column.get(qualified_column)
                if contract is not None:
                    if contract.kind == "null-or-empty-to-literal":
                        if old_value in {_sha256(None), _sha256("")}:
                            if new_value == _sha256(contract.replacement):
                                continue
                            raise MigrationReceiptMismatchError(
                                f"invalid declared transformation in {table_name} "
                                f"row {old_row['key']} column {column}"
                            )
                    if contract.kind == "null-to-legacy-outbox-mapping":
                        _verify_legacy_outbox_message_uid_transition(
                            old_row=old_row,
                            new_row=new_row,
                            postflight=postflight,
                        )
                        continue
                if new_value == old_value:
                    continue
                if column not in new_row["column_sha256"] or new_value != old_value:
                    raise MigrationReceiptMismatchError(
                        f"undeclared change in {table_name} row {old_row['key']} column {column}"
                    )
            added_columns = set(new_row["column_sha256"]) - set(
                old_row["column_sha256"]
            )
            for column in sorted(added_columns):
                qualified_column = f"{table_name}.{column}"
                new_value = new_row["column_sha256"][column]
                contract = contracts_by_column.get(qualified_column)
                if (
                    contract is not None
                    and contract.kind == "null-to-legacy-outbox-mapping"
                ):
                    _verify_legacy_outbox_message_uid_transition(
                        old_row=old_row,
                        new_row=new_row,
                        postflight=postflight,
                    )
                    continue
                if qualified_column in immutable_columns:
                    if new_value == _sha256(None):
                        continue
                    raise MigrationReceiptMismatchError(
                        f"new immutable column {qualified_column} must remain NULL "
                        f"on pre-existing row {old_row['key']}"
                    )
                if (
                    contract is not None
                    and contract.kind == "null-or-empty-to-literal"
                    and new_value == _sha256(contract.replacement)
                ):
                    continue
                if new_value != _sha256(None):
                    raise MigrationReceiptMismatchError(
                        f"newly added column {qualified_column} was populated on "
                        f"pre-existing row {old_row['key']} without its exact "
                        "transformation contract"
                    )
                if contract is not None:
                    raise MigrationReceiptMismatchError(
                        f"invalid declared transformation in {table_name} "
                        f"row {old_row['key']} newly added column {column}"
                    )
    outbox_contract = contracts_by_column.get("outbox_event.message_uid")
    if outbox_contract is not None:
        for old_row in preflight["tables"].get("outbox_event", {}).get("rows", []):
            new_row = next(
                (
                    row
                    for row in postflight["tables"]
                    .get("outbox_event", {})
                    .get("rows", [])
                    if row["key"] == old_row["key"]
                ),
                None,
            )
            if new_row is None:
                raise MigrationReceiptMismatchError(
                    f"pre-existing rows disappeared from outbox_event: {old_row['key']}"
                )
            _verify_legacy_outbox_message_uid_transition(
                old_row=old_row,
                new_row=new_row,
                postflight=postflight,
            )
    allowed_sequences = {
        value
        for item in preflight["pending_migrations"]
        for value in item.get("allowed_sequences", [])
    }
    before_sequences = {item["name"]: item for item in preflight["sequences"]}
    after_sequences = {item["name"]: item for item in postflight["sequences"]}
    if set(before_sequences) != set(after_sequences):
        raise MigrationReceiptMismatchError("identity sequence inventory changed")
    for name, old_state in before_sequences.items():
        new_state = after_sequences[name]
        if old_state == new_state:
            continue
        if not allow_declared_sequence_advances or name not in allowed_sequences:
            raise MigrationReceiptMismatchError(
                f"undeclared identity sequence state change: {name}"
            )
        stable_fields = set(old_state) - {"last_value", "is_called"}
        if any(old_state[field] != new_state[field] for field in stable_fields):
            raise MigrationReceiptMismatchError(
                f"identity sequence definition changed: {name}"
            )
        increment = int(old_state["increment"])
        old_next = int(old_state["last_value"]) + (
            increment if old_state["is_called"] else 0
        )
        new_next = int(new_state["last_value"]) + (
            increment if new_state["is_called"] else 0
        )
        if new_next <= old_next or (new_next - old_next) % increment:
            raise MigrationReceiptMismatchError(
                f"invalid identity sequence advance: {name}"
            )


def _advance_permitted_identity_sequences(
    connection: Connection,
    *,
    preflight: Mapping[str, Any],
    interim: Mapping[str, Any],
    schema_name: str,
) -> None:
    """Advance allowlisted generators only after all transactional checks pass.

    Legacy conversion supplies explicit identities so failed SQL cannot consume
    values through nontransactional nextval()/setval(). The final advancement
    uses transactional ALTER SEQUENCE RESTART and is deliberately the last
    mutating step before the result snapshot and commit.
    """
    allowed_sequences = {
        value
        for item in preflight["pending_migrations"]
        for value in item["allowed_sequences"]
    }
    for sequence in interim["sequences"]:
        name = str(sequence["name"])
        if name not in allowed_sequences:
            continue
        assigned: list[int] = []
        owner_table = sequence["owner_table"]
        owner_column = sequence["owner_column"]
        table = interim["tables"].get(str(owner_table))
        if table and owner_column in table["columns"]:
            assigned = [
                int(row["identity"][owner_column])
                for row in table["rows"]
                if row["identity"].get(owner_column) is not None
            ]
        elif name.endswith("_instance_seq"):
            prefix = name[: -len("_instance_seq")].upper()
            for identity_table in (
                "generic_template",
                "generic_instance",
                "generic_instance_lineage",
                "audit_log",
            ):
                for row in interim["tables"].get(identity_table, {}).get("rows", []):
                    identity = row["identity"]
                    if (
                        str(identity.get("euid_prefix") or "").upper() == prefix
                        and identity.get("euid_seq") is not None
                    ):
                        assigned.append(int(identity["euid_seq"]))
        if not assigned:
            continue
        increment = int(sequence["increment"])
        if increment <= 0:
            raise MigrationReceiptMismatchError(
                f"unsupported non-positive sequence increment: {name}"
            )
        current_next = int(sequence["last_value"]) + (
            increment if sequence["is_called"] else 0
        )
        assigned_max = max(assigned)
        if current_next <= assigned_max:
            if (assigned_max - current_next) % increment:
                raise MigrationReceiptMismatchError(
                    f"identity allocations are not aligned to sequence {name}"
                )
            restart_value = assigned_max + increment
            if restart_value > int(sequence["maximum_value"]):
                raise MigrationReceiptMismatchError(
                    f"identity sequence {name} cannot advance without wrapping"
                )
            qualified = f"{_quote_identifier(schema_name)}.{_quote_identifier(name)}"
            connection.exec_driver_sql(
                f"ALTER SEQUENCE {qualified} RESTART WITH {restart_value}"
            )


def apply_migration_preflight(
    connection: Connection,
    *,
    migrations_dir: Path,
    preflight: Mapping[str, Any],
    target: Mapping[str, Any],
) -> MigrationResult:
    """Apply exactly the receipt-bound migration set in the current transaction."""
    if not connection.in_transaction():
        raise MigrationPreflightError("migration apply requires an active transaction")
    schema_name = str(target.get("schema_name") or "").strip()
    lock_key = derive_advisory_lock_key(
        "tapdb.schema.migrate", target.get("database"), schema_name
    )
    connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key})
    existing_tables = [
        name
        for name in (*_TABLES, _TRACKING_TABLE)
        if connection.execute(
            text("SELECT to_regclass(:name) IS NOT NULL"), {"name": name}
        ).scalar_one()
    ]
    if existing_tables:
        qualified = ", ".join(
            f"{_quote_identifier(schema_name)}.{_quote_identifier(name)}"
            for name in existing_tables
        )
        connection.exec_driver_sql(f"LOCK TABLE {qualified} IN ACCESS EXCLUSIVE MODE")

    current = build_migration_preflight(
        connection, migrations_dir=migrations_dir, target=target
    )
    if _receipt_comparable(current) != _receipt_comparable(preflight):
        raise MigrationReceiptMismatchError(
            "live target no longer matches the preflight receipt"
        )

    asset_by_name = {
        item["filename"]: item for item in _migration_assets(migrations_dir)
    }
    for item in preflight["pending_migrations"]:
        filename = str(item["filename"])
        asset = asset_by_name.get(filename)
        if asset is None or asset["sha256"] != item["sha256"]:
            raise MigrationReceiptMismatchError(f"migration asset changed: {filename}")
        source = str(asset["expanded_source"])
        # SQLAlchemy's psycopg execution path supplies an empty parameter
        # mapping; escape DBAPI percent markers in migration literals first.
        migration_sql = _strip_transaction_control(source).replace("%", "%%")
        connection.exec_driver_sql(migration_sql)
        connection.execute(
            text("INSERT INTO _tapdb_migrations (filename) VALUES (:filename)"),
            {"filename": filename},
        )

    interim = build_migration_preflight(
        connection,
        migrations_dir=migrations_dir,
        target=target,
        _validate_generators=False,
    )
    _verify_preservation(preflight, interim, allow_declared_sequence_advances=False)
    _advance_permitted_identity_sequences(
        connection,
        preflight=preflight,
        interim=interim,
        schema_name=schema_name,
    )
    postflight = build_migration_preflight(
        connection, migrations_dir=migrations_dir, target=target
    )
    _verify_preservation(preflight, postflight)
    result = {
        "receipt_version": RECEIPT_VERSION,
        "status": "applied" if preflight["pending_migrations"] else "no-op",
        "preflight_evidence_sha256": preflight["evidence_sha256"],
        "postflight_evidence_sha256": postflight["evidence_sha256"],
        "target": preflight["target"],
        "applied_migrations": [
            item["filename"] for item in preflight["pending_migrations"]
        ],
        "sequence_pre_state": preflight["sequences"],
        "sequence_post_state": postflight["sequences"],
        "postflight": postflight,
    }
    result["result_sha256"] = _sha256(result)
    return MigrationResult(result)


def write_json_receipt(path: Path, receipt: Mapping[str, Any]) -> None:
    """Atomically publish one immutable deterministic receipt without overwrite."""
    if not path.is_absolute():
        raise MigrationPreflightError("receipt path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(_jsonable(receipt), indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    linked = False
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o444)
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, path, follow_symlinks=False)
            linked = True
        except FileExistsError as exc:
            raise MigrationPreflightError(
                f"receipt path already exists: {path}"
            ) from exc
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(path.parent, directory_flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except MigrationPreflightError:
        raise
    except OSError as exc:
        state = "published" if linked else "not published"
        raise MigrationPreflightError(
            f"receipt write failed ({state}) for {path}: {exc}"
        ) from exc
    finally:
        temporary_path.unlink(missing_ok=True)


def load_json_receipt(path: Path) -> dict[str, Any]:
    if not path.is_absolute() or not path.is_file():
        raise MigrationPreflightError(
            "preflight receipt must be an existing absolute file"
        )
    raw_payload: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw_payload, dict) or not all(
        isinstance(key, str) for key in raw_payload
    ):
        raise MigrationPreflightError("preflight receipt must be a JSON object")
    payload = cast(dict[str, Any], raw_payload)
    if payload.get("receipt_version") != RECEIPT_VERSION:
        raise MigrationPreflightError("unsupported migration receipt version")
    evidence_hash = payload.pop("evidence_sha256", None)
    if evidence_hash != _sha256(payload):
        raise MigrationPreflightError("preflight receipt hash mismatch")
    payload["evidence_sha256"] = evidence_hash
    return payload


__all__ = [
    "MigrationPreflightError",
    "MigrationReceiptMismatchError",
    "MigrationResult",
    "apply_migration_preflight",
    "build_migration_preflight",
    "load_json_receipt",
    "write_json_receipt",
]
