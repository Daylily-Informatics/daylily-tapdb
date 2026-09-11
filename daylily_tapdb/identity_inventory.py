"""Read-only, catalog-complete preservation evidence for explicit PostgreSQL targets.

Captures do not initialise TapDB or assume a current schema. The caller supplies
an already active REPEATABLE READ (or SERIALIZABLE) transaction. Content is
streamed and hashed; receipt size and individual row size have hard bounds.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

INVENTORY_VERSION = "tapdb-identity-inventory/v1"
CONVERSION_VERSION = "tapdb-identity-conversion/v1"
MAX_ROWS = 250_000
MAX_ROW_BYTES = 8 * 1024 * 1024
MAX_RECEIPT_BYTES = 128 * 1024 * 1024
_TARGET_FIELDS = (
    "engine_type",
    "host",
    "port",
    "database",
    "schema_name",
    "config_identity",
    "domain_code",
    "owner_repo_name",
)
_IDENTITY_COLUMNS = frozenset(
    {
        "uid",
        "id",
        "euid",
        "euid_prefix",
        "euid_seq",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "machine_uuid",
        "identity_key",
        "created_dt",
        "created_at",
        "changed_at",
        "received_dt",
        "mapped_dt",
        "instance_prefix",
        "event_id",
        "old_outbox_id",
        "old_event_id",
        "source_sha256",
        "message_euid_seq",
        "parent_instance_uid",
        "child_instance_uid",
        "relationship_type",
        "rel_table_name",
        "rel_table_uid_fk",
        "rel_table_euid_fk",
        "category",
        "type",
        "subtype",
        "version",
        "polymorphic_discriminator",
        "attempt_no",
        "attempt_started_dt",
        "claim_token",
        "destination",
        "dedupe_key",
    }
)
_DISPLAY_IDENTITIES = _IDENTITY_COLUMNS - {
    "identity_key",
    "destination",
    "dedupe_key",
    "claim_token",
}


class IdentityInventoryError(ValueError):
    """Identity evidence is incomplete, inconsistent, or unsafe to compare."""


def quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise IdentityInventoryError("An explicit nonempty SQL identifier is required")
    return '"' + value.replace('"', '""') + '"'


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def seal_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("sha256", None)
    result["sha256"] = content_hash(result)
    return result


def validate_receipt(payload: Mapping[str, Any], version: str) -> None:
    if not isinstance(payload, Mapping) or payload.get("schema_version") != version:
        raise IdentityInventoryError(f"Expected receipt format {version}")
    if payload.get("sha256") != seal_receipt(payload)["sha256"]:
        raise IdentityInventoryError("Receipt checksum mismatch")


def validate_target(target: Mapping[str, Any], schema_name: str) -> dict[str, Any]:
    if not isinstance(target, Mapping):
        raise IdentityInventoryError("An explicit physical/config target is required")
    for field in _TARGET_FIELDS:
        value = target.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, (str, int))
            or not str(value).strip()
        ):
            raise IdentityInventoryError(f"Explicit target.{field} is required")
    port = target["port"]
    if not str(port).isdigit() or not 1 <= int(port) <= 65535:
        raise IdentityInventoryError("target.port must be a valid TCP port")
    if target["schema_name"] != schema_name:
        raise IdentityInventoryError("Target schema does not match inventory schema")
    if not str(target["config_identity"]).startswith("/"):
        raise IdentityInventoryError(
            "target.config_identity must be an absolute config path"
        )
    quote_identifier(schema_name)
    result = {
        field: int(port) if field == "port" else str(target[field])
        for field in _TARGET_FIELDS
    }
    if "server_port" in target:
        server_port = target["server_port"]
        if (
            isinstance(server_port, bool)
            or not str(server_port).isdigit()
            or not 1 <= int(server_port) <= 65535
        ):
            raise IdentityInventoryError(
                "Explicit target.server_port must be a valid TCP port"
            )
        result["server_port"] = int(server_port)
    return result


def physical_target(
    connection: Connection, target: Mapping[str, Any]
) -> dict[str, Any]:
    """Bind configured identity to the authenticated physical connection."""
    url = connection.engine.url
    if (
        url.database != target["database"]
        or url.host != target["host"]
        or url.port != target["port"]
    ):
        raise IdentityInventoryError(
            "Connection does not match the explicit physical target"
        )
    row = (
        connection.execute(
            text(
                "SELECT current_database() AS database, d.oid::bigint AS database_oid, "
                "inet_server_addr()::text AS server_address, inet_server_port() AS server_port "
                "FROM pg_database d WHERE d.datname = current_database()"
            )
        )
        .mappings()
        .one()
    )
    if row["database"] != target["database"] or row["server_port"] != target.get(
        "server_port", target["port"]
    ):
        raise IdentityInventoryError(
            "Server does not match the explicit physical target"
        )
    return dict(row)


def require_snapshot(connection: Connection) -> None:
    if not connection.in_transaction() or connection.get_isolation_level() not in {
        "REPEATABLE READ",
        "SERIALIZABLE",
    }:
        raise IdentityInventoryError(
            "Capture requires an active REPEATABLE READ or SERIALIZABLE transaction"
        )
    connection.execute(text("SET LOCAL TimeZone = 'UTC'"))
    connection.execute(text("SET LOCAL DateStyle = 'ISO, YMD'"))
    connection.execute(text("SET LOCAL IntervalStyle = 'iso_8601'"))


@contextmanager
def catalog_capture_context(connection: Connection, *, schema_name: str):
    """Use stable catalog qualification, restoring the caller's search path."""
    from daylily_tapdb.security_context import operator_role_assertion_sql

    require_snapshot(connection)
    # An ordinary Aurora owner requires an unrestricted verified owner policy
    # and no applicable restrictive policy on every FORCE-RLS table.
    connection.execute(text(operator_role_assertion_sql(schema_name=schema_name)))
    connection.execute(text("SET LOCAL row_security = on"))
    previous = connection.execute(
        text("SELECT current_setting('search_path')")
    ).scalar_one()
    connection.execute(text("SET LOCAL search_path = pg_catalog"))
    try:
        yield
    except BaseException:
        try:
            connection.execute(
                text("SELECT set_config('search_path', :value, true)"),
                {"value": previous},
            )
        except DBAPIError:
            # A failed SQL statement may have aborted the caller's transaction;
            # retain that original error. PostgreSQL restores LOCAL settings on rollback.
            pass
        raise
    else:
        connection.execute(
            text("SELECT set_config('search_path', :value, true)"), {"value": previous}
        )


def catalog_tables(connection: Connection, schema_name: str) -> list[dict[str, Any]]:
    schema = connection.execute(
        text("SELECT oid FROM pg_namespace WHERE nspname=:schema"),
        {"schema": schema_name},
    ).scalar_one_or_none()
    if schema is None:
        raise IdentityInventoryError(f"Schema does not exist: {schema_name}")
    rows = connection.execute(
        text(
            "SELECT c.oid::bigint AS oid, c.relname AS name, c.relkind AS kind, "
            "pg_get_userbyid(c.relowner) AS owner, c.relrowsecurity AS rls_enabled, "
            "c.relforcerowsecurity AS rls_forced, c.relispartition AS is_partition "
            "FROM pg_class c WHERE c.relnamespace=:schema AND c.relkind IN ('r','p','f') "
            "ORDER BY c.relname"
        ),
        {"schema": schema},
    ).mappings()
    return [dict(row) for row in rows]


def catalog_columns(connection: Connection, oid: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT a.attname AS name, format_type(a.atttypid,a.atttypmod) AS data_type, "
                "NOT a.attnotnull AS nullable, pg_get_expr(d.adbin,d.adrelid) AS default, "
                "a.attidentity AS identity, a.attgenerated AS generated, a.attnum AS position, "
                "coll.collname AS collation FROM pg_attribute a "
                "LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
                "LEFT JOIN pg_collation coll ON coll.oid=a.attcollation "
                "WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
            ),
            {"oid": oid},
        ).mappings()
    ]


def catalog_dependencies(connection: Connection, oid: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT DISTINCT d.deptype AS kind, "
                "CASE WHEN depowner.oid IS NOT NULL THEN 'TOAST storage for ' || "
                "pg_describe_object('pg_class'::regclass,depowner.oid,0) ELSE "
                "pg_describe_object(d.classid,d.objid,d.objsubid) END AS dependent, "
                "CASE WHEN refowner.oid IS NOT NULL THEN 'TOAST storage for ' || "
                "pg_describe_object('pg_class'::regclass,refowner.oid,0) ELSE "
                "pg_describe_object(d.refclassid,d.refobjid,d.refobjsubid) END AS referenced "
                "FROM pg_depend d "
                "LEFT JOIN pg_class depowner ON d.classid='pg_class'::regclass AND depowner.reltoastrelid=d.objid "
                "LEFT JOIN pg_class refowner ON d.refclassid='pg_class'::regclass AND refowner.reltoastrelid=d.refobjid "
                "WHERE (d.classid='pg_class'::regclass AND d.objid=:oid) "
                "OR (d.refclassid='pg_class'::regclass AND d.refobjid=:oid) "
                "ORDER BY kind,dependent,referenced"
            ),
            {"oid": oid},
        ).mappings()
    ]


def _metadata(connection: Connection, table: Mapping[str, Any]) -> dict[str, Any]:
    oid = table["oid"]
    columns = catalog_columns(connection, oid)
    constraints = [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT c.conname AS name, c.contype AS kind, pg_get_constraintdef(c.oid,true) AS definition, "
                "c.convalidated AS validated, ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY "
                "k(num,ord) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num "
                "ORDER BY k.ord) AS columns, rn.nspname AS referenced_schema, rc.relname AS referenced_table, "
                "ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY k(num,ord) "
                "JOIN pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.num ORDER BY k.ord) AS referenced_columns "
                "FROM pg_constraint c LEFT JOIN pg_class rc ON rc.oid=c.confrelid "
                "LEFT JOIN pg_namespace rn ON rn.oid=rc.relnamespace "
                "WHERE c.conrelid=:oid ORDER BY c.conname"
            ),
            {"oid": oid},
        ).mappings()
    ]
    primary_key = [
        name
        for constraint in constraints
        if constraint["kind"] == "p"
        for name in constraint["columns"]
    ]
    immutable = set(primary_key)
    for constraint in constraints:
        if constraint["kind"] == "f":
            immutable.update(constraint["columns"])
    for column in columns:
        name = column["name"]
        if name in _IDENTITY_COLUMNS or name.endswith(("_uid", "_euid", "_uuid")):
            immutable.add(name)
    return {
        **{k: v for k, v in table.items() if k not in {"oid", "name"}},
        "columns": columns,
        "constraints": constraints,
        "indexes": [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT c.relname AS name, pg_get_indexdef(i.indexrelid) AS definition "
                    "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                    "WHERE i.indrelid=:oid ORDER BY c.relname"
                ),
                {"oid": oid},
            ).mappings()
        ],
        "triggers": [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT tgname AS name, pg_get_triggerdef(oid,true) AS definition, tgenabled AS enabled "
                    "FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal ORDER BY tgname"
                ),
                {"oid": oid},
            ).mappings()
        ],
        "policies": [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT polname AS name, polcmd AS command, polpermissive AS permissive, "
                    "pg_get_expr(polqual,polrelid) AS using, pg_get_expr(polwithcheck,polrelid) AS with_check "
                    "FROM pg_policy WHERE polrelid=:oid ORDER BY polname"
                ),
                {"oid": oid},
            ).mappings()
        ],
        "dependencies": catalog_dependencies(connection, oid),
        "primary_key": primary_key,
        "immutable_columns": sorted(immutable),
    }


def _capture_identity_inventory(
    connection: Connection, *, schema_name: str, target: Mapping[str, Any]
) -> dict[str, Any]:
    """Capture all physical table rows, identities, and catalog dependencies."""
    target = validate_target(target, schema_name)
    require_snapshot(connection)
    physical = physical_target(connection, target)
    tables: dict[str, Any] = {}
    receipt_bytes = 0
    total_rows = 0
    for table in catalog_tables(connection, schema_name):
        if table["kind"] == "f":
            raise IdentityInventoryError(
                "Foreign tables cannot provide a local consistent snapshot"
            )
        name = table["name"]
        entry = _metadata(connection, table)
        rows: dict[str, Any] = {}
        statement = text(
            f"SELECT to_jsonb(t)::text FROM ONLY {quote_identifier(schema_name)}.{quote_identifier(name)} t"
        )
        cursor = connection.execute(
            statement, execution_options={"stream_results": True, "yield_per": 256}
        )
        try:
            for (raw,) in cursor:
                total_rows += 1
                if total_rows > MAX_ROWS or len(raw.encode("utf-8")) > MAX_ROW_BYTES:
                    raise IdentityInventoryError(
                        "Identity capture exceeds the declared row/content bound"
                    )
                values = json.loads(raw, parse_float=str)
                row_hash = content_hash(values)
                key = (
                    content_hash([values[column] for column in entry["primary_key"]])
                    if entry["primary_key"]
                    else row_hash
                )
                if key in rows:
                    if entry["primary_key"]:
                        raise IdentityInventoryError(
                            f"Duplicate primary-key identity in {name}"
                        )
                    rows[key]["count"] += 1
                    continue
                row = {
                    "sha256": row_hash,
                    "count": 1,
                    "columns": {
                        column: content_hash(value) for column, value in values.items()
                    },
                    "identity": {
                        column: value
                        for column, value in values.items()
                        if column in _DISPLAY_IDENTITIES
                        or column.endswith(("_uid", "_euid", "_uuid"))
                    },
                }
                receipt_bytes += len(canonical_json(row).encode("utf-8")) + len(key)
                if receipt_bytes > MAX_RECEIPT_BYTES:
                    raise IdentityInventoryError(
                        "Identity receipt exceeds the declared size bound"
                    )
                rows[key] = row
        finally:
            cursor.close()
        entry.update(
            rows=rows,
            row_count=sum(row["count"] for row in rows.values()),
            content_sha256=content_hash(rows),
        )
        tables[name] = entry
    return seal_receipt(
        {
            "schema_version": INVENTORY_VERSION,
            "schema_name": schema_name,
            "target": target,
            "physical_target": physical,
            "tables": tables,
        }
    )


def capture_identity_inventory(
    connection: Connection, *, schema_name: str, target: Mapping[str, Any]
) -> dict[str, Any]:
    """Capture all physical tables in a stable read-only catalog context."""
    with catalog_capture_context(connection, schema_name=schema_name):
        return _capture_identity_inventory(
            connection, schema_name=schema_name, target=target
        )


def _column_change_allowed(
    old: str, new: str, key: str, contract: Mapping[str, Any]
) -> bool:
    kind = contract.get("kind")
    if kind == "null_or_empty_to_literal":
        if set(contract) != {"kind", "value"}:
            raise IdentityInventoryError(
                "Literal conversion requires exactly kind and value"
            )
        return old in {content_hash(None), content_hash("")} and new == content_hash(
            contract["value"]
        )
    if kind == "exact_value_hashes":
        if set(contract) != {"kind", "changes"} or not isinstance(
            contract["changes"], Mapping
        ):
            raise IdentityInventoryError("Exact conversion requires per-row changes")
        return contract["changes"].get(key) == {"before": old, "after": new}
    raise IdentityInventoryError("Unknown identity conversion contract")


def _null_reference_changes(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    conversion: Mapping[str, Any],
    *,
    table_name: str,
    column: str,
    contract: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Prove a NULL reference fill using new persisted rows and catalog FKs.

    This is not an identity rewrite exemption: only an originally nullable,
    non-key UID reference can be filled, and every cell is bound to an exact
    newly persisted mapping and newly persisted target primary-key identity.
    """
    label = f"{table_name}.{column}"
    if (
        set(contract)
        != {
            "kind",
            "target_table",
            "target_column",
            "mapping_table",
            "mapping_source_column",
            "mapping_target_column",
            "changes",
        }
        or not isinstance(contract["changes"], Mapping)
        or not contract["changes"]
    ):
        raise IdentityInventoryError(
            f"NULL reference fill requires exact per-row mapping evidence: {label}"
        )
    original = before["tables"][table_name]
    current = after["tables"][table_name]
    attributes = [item for item in original["columns"] if item["name"] == column]
    if (
        column in _IDENTITY_COLUMNS
        or not column.endswith("_uid")
        or column in original["primary_key"]
        or column in current["primary_key"]
        or len(original["primary_key"]) != 1
        or len(attributes) != 1
        or attributes[0].get("nullable") is not True
        or attributes[0].get("identity") != ""
        or attributes[0].get("generated") != ""
    ):
        raise IdentityInventoryError(
            f"Intrinsic identities cannot use NULL reference fill: {label}"
        )
    names = [
        contract[field]
        for field in (
            "target_table",
            "target_column",
            "mapping_table",
            "mapping_source_column",
            "mapping_target_column",
        )
    ]
    if not all(isinstance(name, str) and name for name in names):
        raise IdentityInventoryError(
            f"NULL reference fill needs explicit table/column names: {label}"
        )
    target_name, target_column, mapping_name, mapping_source, mapping_target = names
    target = after["tables"].get(target_name)
    mapping = after["tables"].get(mapping_name)
    if (
        target_name == table_name
        or mapping_name in {table_name, target_name}
        or target is None
        or mapping is None
        or target["primary_key"] != [target_column]
        or mapping["primary_key"] != [mapping_source]
    ):
        raise IdentityInventoryError(
            f"NULL reference fill needs distinct target/mapping primary-key tables: {label}"
        )
    for referenced_table, local_column in (
        (current, column),
        (mapping, mapping_target),
    ):
        dependencies = [
            item
            for item in referenced_table["constraints"]
            if item["kind"] == "f" and local_column in item["columns"]
        ]
        if len(dependencies) != 1 or any(
            dependencies[0].get(field) != expected
            for field, expected in {
                "columns": [local_column],
                "validated": True,
                "referenced_schema": after["schema_name"],
                "referenced_table": target_name,
                "referenced_columns": [target_column],
            }.items()
        ):
            raise IdentityInventoryError(
                f"NULL reference fill lacks an exact validated FK dependency: {label}"
            )
    for name in (target_name, mapping_name):
        allowed = (
            name in conversion.get("added_tables", [])
            if name not in before["tables"]
            else conversion.get("tables", {}).get(name, {}).get("added_rows") is True
        )
        if not allowed:
            raise IdentityInventoryError(
                f"NULL reference target/mapping additions are not approved: {label}"
            )
    for key, evidence in contract["changes"].items():
        if not isinstance(evidence, Mapping) or set(evidence) != {
            "before",
            "after",
            "target_row_key",
            "mapping_row_key",
            "mapping_row_sha256",
        }:
            raise IdentityInventoryError(
                f"NULL reference fill needs exact cell and row hashes: {label}"
            )
        old_row = original["rows"].get(key)
        new_row = current["rows"].get(key)
        target_row = target["rows"].get(evidence["target_row_key"])
        mapping_row = mapping["rows"].get(evidence["mapping_row_key"])
        if any(
            row is None or row["count"] != 1
            for row in (old_row, new_row, target_row, mapping_row)
        ):
            raise IdentityInventoryError(
                f"NULL reference fill names a missing/ambiguous persisted row: {label}"
            )
        if evidence["target_row_key"] in before["tables"].get(target_name, {}).get(
            "rows", {}
        ) or evidence["mapping_row_key"] in before["tables"].get(mapping_name, {}).get(
            "rows", {}
        ):
            raise IdentityInventoryError(
                f"NULL reference fill requires newly persisted target/mapping rows: {label}"
            )
        # Rows are proven non-None above; identity values are already safe
        # allowlisted inventory fields, never arbitrary mapping content.
        reference = new_row["identity"].get(column)
        if (
            column not in old_row["identity"]
            or old_row["identity"][column] is not None
            or old_row["columns"].get(column) != content_hash(None)
            or evidence["before"] != content_hash(None)
            or type(reference) is not int
            or reference <= 0
            or evidence["after"] != content_hash(reference)
            or new_row["columns"].get(column) != evidence["after"]
            or target_row["columns"].get(target_column) != evidence["after"]
            or mapping_row["columns"].get(mapping_target) != evidence["after"]
            or mapping_row["columns"].get(mapping_source)
            != old_row["columns"][original["primary_key"][0]]
            or mapping_row["sha256"] != evidence["mapping_row_sha256"]
        ):
            raise IdentityInventoryError(
                f"NULL reference fill does not match original NULL and exact persisted mapping: {label}"
            )
    return contract["changes"]


def verify_identity_inventory(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    conversion_manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare exhaustive rows; explicit conversions never waive immutable identity."""
    validate_receipt(before, INVENTORY_VERSION)
    validate_receipt(after, INVENTORY_VERSION)
    conversion = conversion_manifest or {
        "schema_version": CONVERSION_VERSION,
        "tables": {},
        "added_tables": [],
    }
    if conversion.get("schema_version") != CONVERSION_VERSION or set(conversion) - {
        "schema_version",
        "tables",
        "added_tables",
        "target",
    }:
        raise IdentityInventoryError("Unknown identity conversion manifest")
    declared = conversion.get("tables", {})
    if not isinstance(declared, Mapping) or set(declared) - set(before["tables"]):
        raise IdentityInventoryError("Conversion names an unknown original table")
    violations: list[str] = []
    allowed_target = conversion.get("target", before["target"])
    if (
        after["target"] != allowed_target
        or before["schema_name"] != after["schema_name"]
    ):
        violations.append("target_identity_changed")
    if (
        "target" not in conversion
        and before["physical_target"] != after["physical_target"]
    ):
        violations.append("physical_target_changed")
    for name in sorted(
        set(after["tables"])
        - set(before["tables"])
        - set(conversion.get("added_tables", []))
    ):
        violations.append(f"{name}:undeclared_table_added")
    for name, original in before["tables"].items():
        if name not in after["tables"]:
            violations.append(f"{name}:table_missing")
            continue
        current = after["tables"][name]
        contract = declared.get(name, {})
        if set(contract) - {
            "added_rows",
            "added_columns",
            "changed_columns",
            "schema_changes",
        }:
            raise IdentityInventoryError(f"Unknown conversion fields for {name}")
        changed = contract.get("changed_columns", {})
        if not isinstance(changed, Mapping) or any(
            not isinstance(change, Mapping) for change in changed.values()
        ):
            raise IdentityInventoryError(
                f"Conversion columns require explicit contracts: {name}"
            )
        immutable = set(original["immutable_columns"]) | set(
            current["immutable_columns"]
        )
        reference_changes = {
            column: _null_reference_changes(
                before,
                after,
                conversion,
                table_name=name,
                column=column,
                contract=change,
            )
            for column, change in changed.items()
            if change.get("kind") == "null_reference_from_mapping/v1"
        }
        if (set(changed) & immutable) - set(reference_changes):
            raise IdentityInventoryError(
                f"Immutable identities cannot be converted: {name}"
            )
        old_columns = {column["name"] for column in original["columns"]}
        new_columns = {column["name"] for column in current["columns"]}
        if old_columns - new_columns:
            violations.append(f"{name}:original_columns_missing")
        if new_columns - old_columns - set(contract.get("added_columns", [])):
            violations.append(f"{name}:undeclared_columns_added")
        if original["primary_key"] != current["primary_key"]:
            violations.append(f"{name}:primary_key_changed")
        metadata_fields = (
            "kind",
            "owner",
            "rls_enabled",
            "rls_forced",
            "columns",
            "constraints",
            "indexes",
            "triggers",
            "policies",
            "dependencies",
        )
        if not contract.get("schema_changes", False) and any(
            original[k] != current[k] for k in metadata_fields
        ):
            violations.append(f"{name}:undeclared_schema_change")
        old_rows, new_rows = original["rows"], current["rows"]
        if set(new_rows) - set(old_rows) and not contract.get("added_rows", False):
            violations.append(f"{name}:undeclared_rows_added")
        for key, old_row in old_rows.items():
            new_row = new_rows.get(key)
            if new_row is None or new_row["count"] < old_row["count"]:
                violations.append(f"{name}:{key}:original_row_missing")
                continue
            if new_row["count"] != old_row["count"] and not contract.get(
                "added_rows", False
            ):
                violations.append(f"{name}:{key}:row_multiplicity_changed")
            for column, old_hash in old_row["columns"].items():
                new_hash = new_row["columns"].get(column)
                if old_hash != new_hash and not (
                    (column in reference_changes and key in reference_changes[column])
                    or (
                        column in changed
                        and column not in reference_changes
                        and _column_change_allowed(
                            old_hash, new_hash, key, changed[column]
                        )
                    )
                ):
                    violations.append(f"{name}:{key}:{column}:value_changed")
    return seal_receipt(
        {
            "schema_version": "tapdb-identity-verification/v1",
            "ok": not violations,
            "violations": sorted(violations),
            "before_sha256": before["sha256"],
            "after_sha256": after["sha256"],
        }
    )
