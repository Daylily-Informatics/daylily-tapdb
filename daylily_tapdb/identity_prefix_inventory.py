"""Read-only discovery of one Meridian prefix in one explicit database.

This is a scoped inventory, not a complete backup or a global namespace claim.
It reports stored identities (including soft-deleted rows), audit references,
template bindings and the native sequence's reserved position. It never calls
nextval, provisions a generator, changes an owner, or reads object properties.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from sqlalchemy import text

from daylily_tapdb.euid import normalize_prefix
from daylily_tapdb.identity_inventory import (
    IdentityInventoryError,
    catalog_capture_context,
    catalog_columns,
    catalog_tables,
    physical_target,
    quote_identifier,
    validate_target,
)


def capture_prefix_usage(
    connection: Any,
    *,
    schema_name: str,
    target: Mapping[str, Any],
    prefix: str,
    allocation_receipts_dirs: Sequence[Path] = (),
) -> dict[str, Any]:
    """Read exactly the requested prefix without exporting unrelated rows.

    The caller supplies the existing native operator read-only transaction.
    Multiple databases require separate explicit invocations; an empty result
    from one database never asserts availability across an entire domain.
    """
    normalized = normalize_prefix(prefix)
    if normalized is None or normalized != prefix:
        raise IdentityInventoryError("prefix must be an explicit canonical Meridian token")
    exact_target = validate_target(target, schema_name)
    parameters = {
        "domain": exact_target["domain_code"],
        "prefix": prefix,
        "euid_pattern": f"{exact_target['domain_code']}-{prefix}-%",
    }
    identities: list[dict[str, Any]] = []
    bindings: list[dict[str, Any]] = []
    audit_references: list[dict[str, Any]] = []
    with catalog_capture_context(connection, schema_name=schema_name):
        physical = physical_target(connection, exact_target)
        for table in catalog_tables(connection, schema_name):
            columns = {item["name"] for item in catalog_columns(connection, table["oid"])}
            relevant = bool(
                {"euid", "euid_prefix", "instance_prefix", "rel_table_euid_fk"} & columns
                or table["name"] == "tapdb_identity_prefix_config"
            )
            if not relevant:
                continue
            if table["kind"] == "f":
                raise IdentityInventoryError("Prefix discovery cannot inspect foreign tables")
            name = table["name"]
            qualified = f"{quote_identifier(schema_name)}.{quote_identifier(name)}"
            if "euid" in columns:
                required = {"euid_prefix", "euid_seq", "domain_code", "issuer_app_code"}
                if not required <= columns:
                    raise IdentityInventoryError(
                        f"Prefix discovery requires canonical identity columns in {name}"
                    )
                rows = connection.execute(
                    text(
                        f"SELECT issuer_app_code AS owner, count(*) AS count, "
                        f"max(euid_seq) AS highest_assigned_sequence FROM ONLY {qualified} "
                        "WHERE domain_code=:domain AND euid_prefix=:prefix "
                        "GROUP BY issuer_app_code ORDER BY issuer_app_code"
                    ),
                    parameters,
                ).mappings()
                identities.extend({"table": name, **dict(row)} for row in rows)
            if name == "generic_template" and "instance_prefix" in columns:
                rows = connection.execute(
                    text(
                        f"SELECT issuer_app_code AS owner, count(*) AS count FROM ONLY {qualified} "
                        "WHERE domain_code=:domain AND instance_prefix=:prefix "
                        "GROUP BY issuer_app_code ORDER BY issuer_app_code"
                    ),
                    parameters,
                ).mappings()
                bindings.extend({"kind": "template", "table": name, **dict(row)} for row in rows)
            if name == "tapdb_identity_prefix_config":
                required = {"entity", "prefix", "domain_code", "issuer_app_code"}
                if not required <= columns:
                    raise IdentityInventoryError("Identity prefix bindings lack native owner scope")
                rows = connection.execute(
                    text(
                        f"SELECT entity, issuer_app_code AS owner FROM ONLY {qualified} "
                        "WHERE domain_code=:domain AND prefix=:prefix ORDER BY entity, issuer_app_code"
                    ),
                    parameters,
                ).mappings()
                bindings.extend({"kind": "identity", "table": name, **dict(row)} for row in rows)
            if "rel_table_euid_fk" in columns:
                count = connection.execute(
                    text(
                        f"SELECT count(*) FROM ONLY {qualified} "
                        "WHERE rel_table_euid_fk LIKE :euid_pattern"
                    ),
                    parameters,
                ).scalar_one()
                if count:
                    audit_references.append({"table": name, "count": count})

        # Include a matching native name even if its binding annotation is absent.
        # That is an existing generator requiring disposition, never proof of a
        # free prefix. Reading last_value includes PostgreSQL cached reservations.
        sequence_name = f"{prefix.lower()}_instance_seq"
        sequence_rows = connection.execute(
            text(
                "SELECT c.relname AS name, pg_get_userbyid(c.relowner) AS owner, "
                "s.seqstart AS start_value, s.seqincrement AS increment_by, "
                "s.seqcache AS cache_size, s.seqmin AS min_value, s.seqmax AS max_value, "
                "s.seqcycle AS cycle, obj_description(c.oid,'pg_class') AS binding_annotation "
                "FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND "
                "(c.relname=:sequence OR obj_description(c.oid,'pg_class')=:annotation) "
                "ORDER BY c.relname"
            ),
            {
                "schema": schema_name,
                "sequence": sequence_name,
                "annotation": f"tapdb-prefix-binding/v1:{prefix}",
            },
        ).mappings()
        sequences = []
        for row in sequence_rows:
            state = dict(row)
            qualified = f"{quote_identifier(schema_name)}.{quote_identifier(state['name'])}"
            position = connection.execute(
                text(f"SELECT last_value, is_called FROM {qualified}")
            ).mappings().one()
            sequences.append({**state, **dict(position)})

    journal_results = []
    for directory in allocation_receipts_dirs:
        from daylily_tapdb.backup.receipts import validate_receipts_directory
        from daylily_tapdb.sequence_fence import read_fence_history, retained_floors

        resolved = validate_receipts_directory(str(directory))
        receipts, _ = read_fence_history(resolved)
        matches = []
        for receipt in receipts:
            detail = receipt.detail
            named = detail.get(
                "target", detail.get("plan", {}).get(
                    "target", detail.get("result", {}).get("inventory", {}).get(
                        "target", detail.get("receipt", {}).get("target")
                    )
                )
            )
            physical_fields = ("engine_type", "host", "port", "database", "schema_name", "domain_code")
            if not isinstance(named, Mapping) or any(
                str(named.get(key, "")) != str(exact_target[key]) for key in physical_fields
            ):
                continue
            floors = retained_floors([receipt], named)
            reservation = detail.get("allocation_reservation")
            if isinstance(reservation, Mapping):
                floors.extend(reservation.get("floors", []))
            selected = [dict(floor) for floor in floors if floor.get("name") == sequence_name]
            if selected:
                matches.append({
                    "receipt_id": receipt.receipt_id,
                    "operation": receipt.operation,
                    "status": receipt.status,
                    "created_at": receipt.created_at,
                    "floors": selected,
                })
        journal_results.append({"directory": str(resolved), "reservations": matches})

    return {
        "schema_version": "tapdb-prefix-usage/v1",
        "target": exact_target,
        "physical_target": physical,
        "domain_code": exact_target["domain_code"],
        "prefix": prefix,
        "stored_identities": identities,
        "template_and_identity_bindings": bindings,
        "audit_references": audit_references,
        "native_sequences": sequences,
        "has_database_use": bool(identities or bindings or audit_references or sequences),
        "allocation_journals": journal_results,
        "has_journal_reservations": any(item["reservations"] for item in journal_results),
        "scope": "explicit_database_schema_only",
        "external_allocation_journals": "explicit_directories_only" if journal_results else "not_supplied",
    }
