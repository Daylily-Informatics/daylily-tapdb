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

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from daylily_tapdb.advisory_locks import derive_advisory_lock_key
from daylily_tapdb.security_context import assert_operator_role

RECEIPT_VERSION = "tapdb-migration-receipt/v1"
_TRACKING_TABLE = "_tapdb_migrations"


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
_ALLOW_SCHEMA_RE = re.compile(r"^\s*--\s*tapdb-allow-schema:\s*([a-z0-9_]+)\s*$", re.M)
_ALLOW_NEW_TABLE_RE = re.compile(
    r"^\s*--\s*tapdb-allow-new-table:\s*([a-z0-9_]+)\s*$", re.M
)
_PREFIX_ANNOTATION_RE = re.compile(
    r"^\s*--\s*tapdb-add-prefix-binding:\s*([a-z0-9_]+):([0-9A-HJ-KMNP-TV-Z]{1,4})\s*$",
    re.M,
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
                "allowed_schema_tables": sorted(set(_ALLOW_SCHEMA_RE.findall(source))),
                "allowed_new_tables": sorted(set(_ALLOW_NEW_TABLE_RE.findall(source))),
                "allowed_prefix_annotations": [
                    {"name": name, "annotation": f"tapdb-prefix-binding/v1:{prefix}"}
                    for name, prefix in sorted(
                        set(_PREFIX_ANNOTATION_RE.findall(source))
                    )
                ],
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
    except SQLAlchemyError as exc:
        raise MigrationPreflightError(
            "migration inspection requires a verified operator connection"
        ) from exc


def _migration_tables(identity: Mapping[str, Any]) -> dict[str, Any]:
    """Adapt shared evidence for the existing exact transformation validators."""
    tables: dict[str, Any] = {}
    for name, table in identity["tables"].items():
        rows = []
        for key, row in table["rows"].items():
            values = row.get("identity", {})
            rows.append(
                {
                    "key": [values[column] for column in table["primary_key"]]
                    if table["primary_key"]
                    and all(column in values for column in table["primary_key"])
                    else [key],
                    "inventory_key": key,
                    "identity": values,
                    "column_sha256": row["columns"],
                    "count": row["count"],
                }
            )
        tables[name] = {
            "columns": [item["name"] for item in table["columns"]],
            "primary_key": table["primary_key"],
            "row_count": table["row_count"],
            "rows": rows,
            "immutable_columns": table["immutable_columns"],
            "active_count": sum(
                row["count"]
                for row in rows
                if row["column_sha256"].get("is_deleted") == _sha256(False)
            ),
            "soft_deleted_count": sum(
                row["count"]
                for row in rows
                if row["column_sha256"].get("is_deleted") == _sha256(True)
            ),
        }
    return tables


def _validate_scope_and_sequences(
    snapshot: dict[str, Any], *, validate_generators: bool = True
) -> None:
    from daylily_tapdb.sequences import verify_sequence_floors

    for table_name, table in snapshot["tables"].items():
        for row in table["rows"]:
            for column in ("domain_code", "issuer_app_code"):
                if (
                    column in row["identity"]
                    and not str(row["identity"][column] or "").strip()
                ):
                    raise MigrationPreflightError(
                        f"{table_name} row {row['key']} has missing {column}"
                    )
    if validate_generators:
        if not verify_sequence_floors(snapshot["sequence_inventory"], floors=[])["ok"]:
            raise MigrationPreflightError("generators are behind assigned identities")


def build_migration_preflight(
    connection: Connection,
    *,
    migrations_dir: Path,
    target: Mapping[str, Any],
    _validate_generators: bool = True,
    source_contract: Mapping[str, Any] | None = None,
    receipts_dir: Path | None = None,
    recovery_family: Mapping[str, Any] | None = None,
    _reviewed_recovery: Mapping[str, Any] | None = None,
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
    from daylily_tapdb.identity_inventory import capture_identity_inventory
    from daylily_tapdb.sequences import capture_sequence_inventory

    identity_inventory = capture_identity_inventory(
        connection, schema_name=schema_name, target=target
    )
    sequence_inventory = capture_sequence_inventory(
        connection, schema_name=schema_name, target=target
    )
    from daylily_tapdb.backup.recovery import (
        inventory_floors,
        recovery_family_state,
        require_family_member,
        require_retained_definitions,
        retained_recovery_state,
        validate_recovery_family,
    )
    from daylily_tapdb.sequences import build_sequence_advance_plan

    if (
        source_contract is not None
        and source_contract.get("recovery_family") != recovery_family
    ):
        raise MigrationPreflightError(
            "migration must explicitly retain its source recovery family"
        )
    if recovery_family is not None:
        if receipts_dir is None and _reviewed_recovery is None:
            raise MigrationPreflightError(
                "family migration requires explicit receipts_dir"
            )
        recovery_family = validate_recovery_family(
            recovery_family, required_directory=receipts_dir
        )
        require_family_member(recovery_family, sequence_inventory)
    if _reviewed_recovery is not None:
        allocator_recovery = dict(_reviewed_recovery)
    else:
        retained = (
            retained_recovery_state(receipts_dir, target=sequence_inventory["target"])
            if receipts_dir is not None
            else {"floors": [], "inventories": [], "pending": {}}
        )
        if recovery_family is not None:
            family_state = recovery_family_state(recovery_family)
            # Acquiring our own writer fence appends non-allocator events.
            # Validate the complete chains, but bind relevant floors, physical
            # members and inventory definitions rather than that changing head.
            family_state.pop("journal_heads")
            retained["floors"] += family_state["floors"]
            retained["inventories"] += family_state["inventories"]
            retained["family_state"] = family_state
        require_retained_definitions(sequence_inventory, retained["inventories"])
        allocator_recovery = {
            "receipts_dir": str(receipts_dir) if receipts_dir is not None else None,
            "state_sha256": _sha256(retained),
            "state": retained,
            "plan": build_sequence_advance_plan(
                sequence_inventory,
                floors=list(retained["floors"])
                + inventory_floors(sequence_inventory, source="migration-preflight"),
                recovery_family=recovery_family,
            ),
        }
    if source_contract is not None:
        from daylily_tapdb.backup.source_contract import (
            capture_source_contract,
            verify_source_contract,
        )

        actual = capture_source_contract(
            connection,
            schema_name=schema_name,
            target=target,
            source_version=str(source_contract["source_version"]),
            recovery_family=recovery_family,
        )
        verify_source_contract(source_contract, actual)
    tables = _migration_tables(identity_inventory)

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
        "target": identity_inventory["target"],
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
        "sequences": sequence_inventory["sequences"],
        "identity_inventory": identity_inventory,
        "sequence_inventory": sequence_inventory,
        "source_contract": dict(source_contract) if source_contract else None,
        "recovery_family": dict(recovery_family) if recovery_family else None,
        "allocator_recovery": allocator_recovery,
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
            "identity_inventory",
            "sequence_inventory",
            "source_contract",
            "recovery_family",
            "allocator_recovery",
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
    recovery_sequence_advances: Mapping[str, int] | None = None,
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
        if table_name == _TRACKING_TABLE:
            continue  # Tracking additions are verified by shared inventory below.
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
        from daylily_tapdb.sequences import sequence_definition, sequence_next_value

        if sequence_definition(old_state) != sequence_definition(new_state):
            raise MigrationReceiptMismatchError(
                f"identity sequence definition changed: {name}"
            )
        if any(
            old_state.get(key) != new_state.get(key)
            for key in ("owner", "dependencies")
        ):
            raise MigrationReceiptMismatchError(
                f"identity sequence ownership or mapping changed: {name}"
            )
        if old_state.get("mapping") != new_state.get("mapping"):
            _verify_added_prefix_annotation(
                preflight, old_state, new_state, postflight=postflight
            )
        # Assigned high-water may increase when exact migration contracts add
        # rows without touching sequence state; it is not itself an issuance.
        if all(old_state[key] == new_state[key] for key in ("last_value", "is_called")):
            continue
        reviewed_next = (recovery_sequence_advances or {}).get(name)
        if not allow_declared_sequence_advances or (
            name not in allowed_sequences and reviewed_next is None
        ):
            raise MigrationReceiptMismatchError(
                f"undeclared identity sequence state change: {name}"
            )
        if sequence_next_value(new_state) <= sequence_next_value(old_state):
            raise MigrationReceiptMismatchError(
                f"invalid identity sequence advance: {name}"
            )
        if (
            name not in allowed_sequences
            and sequence_next_value(new_state) != reviewed_next
        ):
            raise MigrationReceiptMismatchError(
                f"sequence advance differs from reviewed recovery plan: {name}"
            )

    if "identity_inventory" in preflight:
        _verify_physical_preservation(
            preflight, postflight, contracts_by_column, allowed_new_rows
        )


def _verify_added_prefix_annotation(
    preflight: Mapping[str, Any],
    old: Mapping[str, Any],
    new: Mapping[str, Any],
    *,
    postflight: Mapping[str, Any] | None = None,
) -> None:
    """Retain the exact prefix; only proven additive evidence may change.

    A declared new row can establish stored-row evidence for an already mapped
    prefix. A new catalog annotation additionally requires its exact SQL marker.
    Neither permits a new prefix, changed ownership, nor lost old evidence.
    """
    before = old["mapping"]
    after = new["mapping"]
    annotation = f"tapdb-prefix-binding/v1:{before.get('prefix')}"
    declaration = {"name": old["name"], "annotation": annotation}
    permitted = [
        item
        for migration in preflight["pending_migrations"]
        for item in migration.get("allowed_prefix_annotations", [])
    ]
    addition = {"source": "catalog_annotation", "annotation": annotation}
    old_evidence = before.get("evidence", [])
    new_evidence = after.get("evidence", [])
    additions = [item for item in new_evidence if item not in old_evidence]
    valid_additions = []
    for item in additions:
        if item == addition and declaration in permitted:
            valid_additions.append(item)
            continue
        if (
            postflight is None
            or set(item) != {"source", "table"}
            or item["source"] != "stored_rows"
        ):
            continue
        table_name = item["table"]
        allowed_rows = {
            name
            for migration in preflight["pending_migrations"]
            for name in migration.get("allowed_new_rows", [])
        }
        if table_name not in allowed_rows:
            continue
        old_rows = (
            preflight["identity_inventory"]["tables"]
            .get(table_name, {})
            .get("rows", {})
        )
        new_rows = (
            postflight["identity_inventory"]["tables"]
            .get(table_name, {})
            .get("rows", {})
        )
        if not any(
            row.get("identity", {}).get("euid_prefix") == before.get("prefix")
            for row in old_rows.values()
        ) and any(
            key not in old_rows
            and row.get("identity", {}).get("euid_prefix") == before.get("prefix")
            for key, row in new_rows.items()
        ):
            valid_additions.append(item)
    if (
        before.get("kind") != "prefix"
        or {key: value for key, value in before.items() if key != "evidence"}
        != {key: value for key, value in after.items() if key != "evidence"}
        or not additions
        or additions != valid_additions
        or any(new_evidence.count(item) != 1 for item in additions)
        or [item for item in new_evidence if item not in additions] != old_evidence
    ):
        raise MigrationReceiptMismatchError(
            f"identity sequence ownership or mapping changed: {old['name']}"
        )


def _verify_physical_preservation(
    preflight: Mapping[str, Any],
    postflight: Mapping[str, Any],
    contracts: Mapping[str, _TransformationContract],
    allowed_new_rows: set[str],
) -> None:
    from daylily_tapdb.identity_inventory import verify_identity_inventory

    before = preflight["identity_inventory"]
    after = postflight["identity_inventory"]
    schema_tables = {
        name
        for migration in preflight["pending_migrations"]
        for name in migration.get("allowed_schema_tables", [])
    }
    new_tables = {
        name
        for migration in preflight["pending_migrations"]
        for name in migration.get("allowed_new_tables", [])
    }
    declarations = {}
    for name, table in before["tables"].items():
        new_table = after["tables"].get(name, {})
        old_columns = {column["name"] for column in table["columns"]}
        new_columns = {column["name"] for column in new_table.get("columns", [])}
        changes = {}
        for qualified, contract in contracts.items():
            owner, column = qualified.split(".", 1)
            if owner != name or column not in old_columns:
                continue
            transition: dict[str, Any] = {
                "kind": "exact_value_hashes",
                "changes": {
                    key: {
                        "before": row["columns"][column],
                        "after": new_table["rows"][key]["columns"][column],
                    }
                    for key, row in table["rows"].items()
                    if key in new_table.get("rows", {})
                    and row["columns"][column]
                    != new_table["rows"][key]["columns"][column]
                },
            }
            if (
                transition["changes"]
                and contract.kind == "null-to-legacy-outbox-mapping"
            ):
                # The legacy conversion validator above proves the exact
                # outbox-to-message mapping. Shared inventory independently
                # verifies the nullable FK, newly added target/mapping rows,
                # original source key, and their complete recorded hashes.
                mapped_changes = {}
                for key, change in transition["changes"].items():
                    old_row = table["rows"][key]
                    new_row = new_table["rows"][key]
                    message_uid = new_row["identity"]["message_uid"]
                    target_matches = [
                        (row_key, row)
                        for row_key, row in after["tables"]["generic_instance"][
                            "rows"
                        ].items()
                        if row["identity"].get("uid") == message_uid
                    ]
                    mapping_matches = [
                        (row_key, row)
                        for row_key, row in after["tables"][
                            "tapdb_legacy_outbox_mapping"
                        ]["rows"].items()
                        if row["identity"].get("old_outbox_id")
                        == old_row["identity"]["id"]
                        and row["identity"].get("message_uid") == message_uid
                    ]
                    if len(target_matches) != 1 or len(mapping_matches) != 1:
                        raise MigrationReceiptMismatchError(
                            "legacy NULL reference lacks its exact target/mapping row"
                        )
                    mapped_changes[key] = {
                        **change,
                        "target_row_key": target_matches[0][0],
                        "mapping_row_key": mapping_matches[0][0],
                        "mapping_row_sha256": mapping_matches[0][1]["sha256"],
                    }
                transition = {
                    "kind": "null_reference_from_mapping/v1",
                    "target_table": "generic_instance",
                    "target_column": "uid",
                    "mapping_table": "tapdb_legacy_outbox_mapping",
                    "mapping_source_column": "old_outbox_id",
                    "mapping_target_column": "message_uid",
                    "changes": mapped_changes,
                }
            if transition["changes"]:
                changes[column] = transition
        declarations[name] = {
            "added_rows": name in allowed_new_rows or name == _TRACKING_TABLE,
            "added_columns": sorted(new_columns - old_columns)
            if name in schema_tables
            else [],
            "changed_columns": changes,
            "schema_changes": name in schema_tables,
        }
    conversion = {
        "schema_version": "tapdb-identity-conversion/v1",
        "tables": declarations,
        "added_tables": sorted(new_tables),
    }
    result = verify_identity_inventory(before, after, conversion_manifest=conversion)
    if not result["ok"]:
        raise MigrationReceiptMismatchError(
            "physical identity preservation failed: " + "; ".join(result["violations"])
        )


def _advance_permitted_identity_sequences(
    connection: Connection,
    *,
    preflight: Mapping[str, Any],
    interim: Mapping[str, Any],
    schema_name: str,
    writer_fence: Mapping[str, Any],
    receipts_dir: Path,
    retained_floors: list[dict[str, Any]],
) -> dict[str, Any]:
    """Use the one shared allocator, retaining reservations before final commit."""
    from daylily_tapdb.backup.recovery import inventory_floors
    from daylily_tapdb.sequences import (
        apply_sequence_advance_plan,
        build_sequence_advance_plan,
        sequence_next_value,
    )

    floors = list(retained_floors) + inventory_floors(
        preflight["sequence_inventory"], source="migration-preflight"
    )
    plan = build_sequence_advance_plan(
        interim["sequence_inventory"],
        floors=floors,
        recovery_family=preflight.get("recovery_family"),
    )
    allowed = {
        name
        for item in preflight["pending_migrations"]
        for name in item["allowed_sequences"]
    }
    current = {item["name"]: item for item in interim["sequences"]}
    changed = {
        item["name"]
        for item in plan["advances"]
        if item["next_value"] > sequence_next_value(current[item["name"]])
    }
    reviewed_plan = preflight["allocator_recovery"]["plan"]
    original = {item["name"]: item for item in preflight["sequences"]}
    reviewed = {
        item["name"]: item["next_value"]
        for item in reviewed_plan["advances"]
        if item["next_value"] > sequence_next_value(original[item["name"]])
    }
    undeclared = changed - allowed - set(reviewed)
    if undeclared:
        raise MigrationReceiptMismatchError(
            "undeclared sequence advance: " + ", ".join(sorted(undeclared))
        )
    for item in plan["advances"]:
        if (
            item["name"] in reviewed
            and item["name"] not in allowed
            and item["next_value"] != reviewed[item["name"]]
        ):
            raise MigrationReceiptMismatchError(
                "allocator state differs from reviewed recovery plan"
            )
    return apply_sequence_advance_plan(
        connection, plan, writer_fence=writer_fence, receipts_dir=receipts_dir
    )


def apply_migration_preflight(
    connection: Connection,
    *,
    migrations_dir: Path,
    preflight: Mapping[str, Any],
    target: Mapping[str, Any],
    writer_fence: Mapping[str, Any] | None = None,
    receipts_dir: Path | None = None,
) -> MigrationResult:
    """Apply exactly the receipt-bound migration set in the current transaction."""
    if not connection.in_transaction():
        raise MigrationPreflightError("migration apply requires an active transaction")
    schema_name = str(target.get("schema_name") or "").strip()
    lock_key = derive_advisory_lock_key(
        "tapdb.schema.migrate", target.get("database"), schema_name
    )
    connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key})
    from daylily_tapdb.backup.receipts import SURFACE_CLI, Actor
    from daylily_tapdb.backup.recovery import begin_recovery, observe_recovery
    from daylily_tapdb.identity_inventory import catalog_tables
    from daylily_tapdb.sequences import validate_writer_fence

    if writer_fence is None or receipts_dir is None:
        raise MigrationPreflightError(
            "migration apply requires a verified writer_fence and external receipts_dir"
        )
    if preflight.get("allocator_recovery", {}).get("receipts_dir") != str(receipts_dir):
        raise MigrationReceiptMismatchError(
            "preflight must bind the exact external receipts_dir"
        )
    validate_writer_fence(connection, preflight["sequence_inventory"], writer_fence)
    existing_tables = [item["name"] for item in catalog_tables(connection, schema_name)]

    if existing_tables:
        qualified = ", ".join(
            f"{_quote_identifier(schema_name)}.{_quote_identifier(name)}"
            for name in existing_tables
        )
        connection.exec_driver_sql(f"LOCK TABLE {qualified} IN ACCESS EXCLUSIVE MODE")

    current = build_migration_preflight(
        connection,
        migrations_dir=migrations_dir,
        target=target,
        source_contract=preflight.get("source_contract"),
        receipts_dir=receipts_dir,
        recovery_family=preflight.get("recovery_family"),
    )
    if _receipt_comparable(current) != _receipt_comparable(preflight):
        raise MigrationReceiptMismatchError(
            "live target no longer matches the preflight receipt"
        )

    recovery_intent = begin_recovery(
        receipts_dir,
        target=preflight["target"],
        inventories=[current["sequence_inventory"]],
        evidence={
            "purpose": "fenced_migration",
            "preflight_sha256": preflight["evidence_sha256"],
        },
        actor=Actor(surface=SURFACE_CLI, username="migration:tapdb"),
        recovery_family=preflight.get("recovery_family"),
    )
    observe_recovery(
        connection,
        receipts_dir,
        recovery_intent,
        actor=Actor(surface=SURFACE_CLI, username="migration:tapdb"),
        inventory=current["sequence_inventory"],
    )

    sequence_result: dict[str, Any] | None = None
    try:
        asset_by_name = {
            item["filename"]: item for item in _migration_assets(migrations_dir)
        }
        for item in preflight["pending_migrations"]:
            filename = str(item["filename"])
            asset = asset_by_name.get(filename)
            if asset is None or asset["sha256"] != item["sha256"]:
                raise MigrationReceiptMismatchError(
                    f"migration asset changed: {filename}"
                )
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
            _reviewed_recovery=preflight["allocator_recovery"],
            recovery_family=preflight.get("recovery_family"),
        )
        _verify_preservation(preflight, interim, allow_declared_sequence_advances=False)
        sequence_result = _advance_permitted_identity_sequences(
            connection,
            preflight=preflight,
            interim=interim,
            schema_name=schema_name,
            writer_fence=writer_fence,
            receipts_dir=receipts_dir,
            retained_floors=recovery_intent["floors"],
        )
        postflight = build_migration_preflight(
            connection,
            migrations_dir=migrations_dir,
            target=target,
            _reviewed_recovery=preflight["allocator_recovery"],
            recovery_family=preflight.get("recovery_family"),
        )
        from daylily_tapdb.sequences import sequence_next_value

        original_states = {item["name"]: item for item in preflight["sequences"]}
        reviewed_advances = {
            item["name"]: item["next_value"]
            for item in preflight["allocator_recovery"]["plan"]["advances"]
            if item["next_value"] > sequence_next_value(original_states[item["name"]])
        }
        _verify_preservation(
            preflight, postflight, recovery_sequence_advances=reviewed_advances
        )
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
            "recovery_intent": recovery_intent,
            "sequence_result": sequence_result,
            "commit_status": "pending_caller_commit",
            "allocator_changes": [
                {
                    "name": item["name"],
                    "from_next": sequence_next_value(original_states[item["name"]]),
                    "to_next": sequence_next_value(item),
                }
                for item in postflight["sequences"]
                if sequence_next_value(item)
                != sequence_next_value(original_states[item["name"]])
            ],
        }
        result["result_sha256"] = _sha256(result)
        return MigrationResult(result)
    except BaseException as exc:
        # The transaction owner must rollback before observing nontransactional
        # allocations. The durable intent and exception reference survive that.
        setattr(exc, "recovery_intent", recovery_intent)
        setattr(exc, "sequence_result", sequence_result)
        raise


def finalize_migration_recovery(
    connection: Connection,
    result: MigrationResult,
    *,
    receipts_dir: Path,
    writer_fence: Mapping[str, Any],
) -> dict[str, Any]:
    """Observe a separately committed migration and terminalize its intent.

    Call in a new read transaction after the owner committed the apply
    transaction. An ambiguous commit stays pending until this proof succeeds.
    """
    from daylily_tapdb.backup.receipts import SURFACE_CLI, Actor
    from daylily_tapdb.backup.recovery import finish_recovery
    from daylily_tapdb.identity_inventory import (
        capture_identity_inventory,
        verify_identity_inventory,
    )
    from daylily_tapdb.sequences import (
        capture_sequence_inventory,
        record_sequence_advance_outcome,
        validate_writer_fence,
    )

    payload = result.receipt
    target = dict(
        payload["target"],
        sequence_mappings=payload["postflight"]["sequence_inventory"][
            "sequence_mappings"
        ],
    )
    observed = capture_sequence_inventory(
        connection, schema_name=target["schema_name"], target=target
    )
    validate_writer_fence(connection, observed, writer_fence)
    identity = capture_identity_inventory(
        connection, schema_name=target["schema_name"], target=target
    )
    verified = verify_identity_inventory(
        payload["postflight"]["identity_inventory"], identity
    )
    if not verified["ok"] or observed != payload["postflight"]["sequence_inventory"]:
        raise MigrationReceiptMismatchError(
            "post-commit migration evidence differs from apply outcome"
        )
    allocator_result = record_sequence_advance_outcome(
        payload["sequence_result"],
        receipts_dir=receipts_dir,
        outcome="committed",
        actor="migration:tapdb",
    )
    receipt = finish_recovery(
        receipts_dir,
        payload["recovery_intent"],
        phase="committed",
        actor=Actor(surface=SURFACE_CLI, username="migration:tapdb"),
        inventory=observed,
        reservations=payload["sequence_result"].get("floors", []),
    )
    return {
        "status": "committed",
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.checksum(),
        "allocator_result": allocator_result,
    }


def finalize_migration_abort(
    connection: Connection,
    *,
    recovery_intent: Mapping[str, Any],
    receipts_dir: Path,
    writer_fence: Mapping[str, Any],
    sequence_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Retain actual consumed values after the caller has rolled back SQL."""
    from daylily_tapdb.backup.receipts import SURFACE_CLI, Actor
    from daylily_tapdb.backup.recovery import (
        finish_recovery,
        require_retained_definitions,
    )
    from daylily_tapdb.sequences import (
        capture_sequence_inventory,
        record_sequence_advance_outcome,
        validate_writer_fence,
    )

    target = dict(
        recovery_intent["target"],
        sequence_mappings=recovery_intent["inventories"][-1]["sequence_mappings"],
    )
    observed = capture_sequence_inventory(
        connection, schema_name=target["schema_name"], target=target
    )
    validate_writer_fence(connection, observed, writer_fence)
    require_retained_definitions(observed, recovery_intent["inventories"])
    if sequence_result is not None:
        record_sequence_advance_outcome(
            sequence_result,
            receipts_dir=receipts_dir,
            outcome="rolled_back",
            actor="migration:tapdb",
        )
    receipt = finish_recovery(
        receipts_dir,
        recovery_intent,
        phase="aborted",
        actor=Actor(surface=SURFACE_CLI, username="migration:tapdb"),
        inventory=observed,
    )
    return {
        "status": "aborted",
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.checksum(),
    }


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
