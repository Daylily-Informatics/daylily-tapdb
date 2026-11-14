"""Read-only discovery of one Meridian prefix in one explicit database.

This is a scoped inventory, not a complete backup or a global namespace claim.
It reports stored identities (including soft-deleted rows), audit references,
template bindings and the native sequence's reserved position. It never calls
nextval, provisions a generator, changes an owner, or reads object properties.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from collections import defaultdict
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


def project_source_contract_prefix(
    source_contract: Mapping[str, Any], *, prefix: str,
) -> dict[str, Any]:
    """Query existing full native evidence; do not recapture or reverify it.

    This is an archived snapshot projection, not a live no-use receipt. The
    consumer separately supplies the native lifecycle evidence that identifies
    when this source was preserved/fenced. Existing receipt seals are retained
    as provenance identifiers, never recomputed here.
    """
    from daylily_tapdb.identity_inventory import content_hash

    if normalize_prefix(prefix) != prefix or not prefix:
        raise IdentityInventoryError("prefix must be an explicit canonical Meridian token")
    if source_contract.get("schema_version") != "tapdb-source-contract/v1":
        raise IdentityInventoryError("archived projection requires a native source contract")
    identity = source_contract.get("identity_inventory")
    sequences = source_contract.get("sequence_inventory")
    if not isinstance(identity, Mapping) or identity.get("schema_version") != "tapdb-identity-inventory/v1":
        raise IdentityInventoryError("archived prefix projection requires full row identity evidence, not audit digests")
    if not isinstance(sequences, Mapping) or sequences.get("schema_version") != "tapdb-sequence-inventory/v1":
        raise IdentityInventoryError("archived prefix projection requires native sequence inventory")
    target = identity["target"]
    physical = identity["physical_target"]
    if sequences.get("target") != target or sequences.get("physical_target") != physical:
        raise IdentityInventoryError("archived identity and sequence evidence name different targets")
    domain = target["domain_code"]
    euid_start = f"{domain}-{prefix}-"
    # The full inventory retains some scalar values as native per-column
    # hashes. This computes a lookup key for the requested value, not a receipt
    # checksum or a second verification of the successful native operation.
    prefix_value = content_hash(prefix)
    identities = defaultdict(lambda: {"count": 0, "highest_assigned_sequence": None})
    templates = defaultdict(int)
    bindings = []
    references = defaultdict(int)
    covered_tables = []
    tables = identity.get("tables")
    if not isinstance(tables, Mapping):
        raise IdentityInventoryError("archived identity evidence has no table coverage")
    for name, table in tables.items():
        columns = {column["name"] for column in table["columns"]}
        if not ({"euid", "euid_prefix", "instance_prefix", "rel_table_euid_fk"} & columns or name == "tapdb_identity_prefix_config"):
            continue
        rows = table.get("rows")
        if not isinstance(rows, Mapping):
            raise IdentityInventoryError(f"archived prefix evidence lacks full rows for {name}")
        covered_tables.append(name)
        for row in rows.values():
            values = row["identity"]
            if "euid" in columns:
                if not {"domain_code", "issuer_app_code", "euid_prefix", "euid_seq"} <= set(values):
                    raise IdentityInventoryError(f"archived canonical identity fields are unavailable in {name}")
                if values["domain_code"] == domain and values["euid_prefix"] == prefix:
                    group = identities[(name, values["issuer_app_code"])]
                    group["count"] += row["count"]
                    value = values["euid_seq"]
                    previous = group["highest_assigned_sequence"]
                    group["highest_assigned_sequence"] = value if previous is None else max(previous, value)
            if name == "generic_template" and "instance_prefix" in columns:
                if values["domain_code"] == domain and values["instance_prefix"] == prefix:
                    templates[(name, values["issuer_app_code"])] += row["count"]
            if name == "tapdb_identity_prefix_config":
                if values["domain_code"] == domain and row["columns"]["prefix"] == prefix_value:
                    bindings.append({
                        "kind": "identity", "table": name,
                        "owner": values["issuer_app_code"], "count": row["count"],
                        "entity_value_hash": row["columns"]["entity"],
                    })
            if "rel_table_euid_fk" in columns:
                reference = values["rel_table_euid_fk"]
                if isinstance(reference, str) and reference.startswith(euid_start):
                    references[name] += row["count"]
    stored = [{"table": name, "owner": owner, **values} for (name, owner), values in sorted(identities.items())]
    bindings.extend({"kind": "template", "table": name, "owner": owner, "count": count} for (name, owner), count in sorted(templates.items()))
    audit_references = [{"table": name, "count": count} for name, count in sorted(references.items())]
    sequence_name = f"{prefix.lower()}_instance_seq"
    selected_sequences = [
        dict(sequence) for sequence in sequences["sequences"]
        if sequence["name"] == sequence_name
        or (sequence["mapping"].get("kind") == "prefix" and sequence["mapping"].get("prefix") == prefix)
    ]
    missing = [name for name in sequences["missing_generators"] if name == sequence_name]
    return {
        "target": dict(target), "physical_target": dict(physical),
        "domain_code": domain, "prefix": prefix,
        "stored_identities": stored, "template_and_identity_bindings": bindings,
        "audit_references": audit_references, "native_sequences": selected_sequences,
        "missing_generators": missing,
        "has_database_use": bool(stored or bindings or audit_references or selected_sequences or missing),
        "covered_tables": sorted(covered_tables),
        "source_version": source_contract["source_version"],
        "source_version_evidence": source_contract["source_version_evidence"],
        "source_contract_sha256": source_contract["sha256"],
        "identity_inventory_sha256": identity["sha256"],
        "sequence_inventory_sha256": sequences["sha256"],
    }


def _evidence_path(value: str | Path, label: str) -> str:
    if not isinstance(value, (str, Path)) or not Path(value).is_absolute():
        raise IdentityInventoryError(f"{label} must be an explicit absolute path")
    return str(value)


def _archived_journals(family: Mapping[str, Any], target: Mapping[str, Any], prefix: str) -> list[dict[str, Any]]:
    from daylily_tapdb.backup.receipts import read_receipts
    from daylily_tapdb.sequence_fence import retained_floors

    if family.get("schema_version") != "tapdb-recovery-family/v1":
        raise IdentityInventoryError("archive evidence requires the explicit native recovery family")
    roots = family.get("receipts_dirs")
    if not isinstance(roots, list) or not roots:
        raise IdentityInventoryError("recovery family must declare its journal roots")
    sequence_name = f"{prefix.lower()}_instance_seq"
    results = []
    physical_fields = ("engine_type", "host", "port", "database", "schema_name", "domain_code")
    for root in roots:
        directory = Path(_evidence_path(root, "recovery-family journal"))
        if not directory.is_dir():
            raise IdentityInventoryError(f"declared recovery journal is unavailable: {directory}")
        # Read the existing owning service's receipts once. Do not recalculate
        # their checksums or rerun recovery-family/fence verification.
        receipts = read_receipts(directory)
        matches = []
        for receipt in receipts:
            detail = receipt.detail
            named = detail.get("target", detail.get("plan", {}).get(
                "target", detail.get("result", {}).get("inventory", {}).get(
                    "target", detail.get("receipt", {}).get("target")
                )
            ))
            if not isinstance(named, Mapping) or named.get("domain_code") != target["domain_code"]:
                continue
            declared_family = detail.get("recovery_family")
            same_family = isinstance(declared_family, Mapping) and declared_family.get("family_id") == family["family_id"]
            same_source = all(named.get(key) == target[key] for key in physical_fields)
            if not same_source and not same_family:
                continue
            floors = retained_floors([receipt], named)
            reservation = detail.get("allocation_reservation")
            if isinstance(reservation, Mapping):
                floors.extend(reservation.get("floors", []))
            selected = [dict(floor) for floor in floors if floor.get("name") == sequence_name]
            inventories = list(detail.get("inventories", []))
            for inventory in (
                detail.get("sequence_inventory"), detail.get("plan", {}).get("inventory"),
                detail.get("result", {}).get("inventory"),
                detail.get("receipt", {}).get("sequence_inventory"),
            ):
                if isinstance(inventory, Mapping):
                    inventories.append(inventory)
            generators = [
                dict(sequence) for inventory in inventories
                for sequence in inventory.get("sequences", [])
                if sequence["name"] == sequence_name or (
                    sequence["mapping"].get("kind") == "prefix"
                    and sequence["mapping"].get("prefix") == prefix
                )
            ]
            if selected or generators:
                matches.append({
                    "receipt_id": receipt.receipt_id, "operation": receipt.operation,
                    "status": receipt.status, "created_at": receipt.created_at,
                    "target": dict(named), "floors": selected, "native_sequences": generators,
                })
        results.append({
            "directory": str(directory), "reservations": matches,
            "consumed_receipts": [
                {"receipt_id": item.receipt_id, "sequence": item.sequence,
                 "created_at": item.created_at, "operation": item.operation, "status": item.status}
                for item in receipts
            ],
            "scope": "explicit_native_journal_root",
        })
    return results


def project_prefix_from_evidence(
    *, evidence_kind: str, evidence: Mapping[str, Any], evidence_path: str | Path,
    prefix: str, archive_result: Mapping[str, Any] | None = None,
    archive_result_path: str | Path | None = None,
    recovery_family: Mapping[str, Any] | None = None,
    recovery_family_path: str | Path | None = None,
) -> dict[str, Any]:
    """Project explicit existing native evidence without opening a database.

    The two evidence kinds are separate contracts, not alternate live-query
    paths. Existing successful outcomes are consumed; no receipt seal or chain
    is recomputed, and no global or timeless namespace absence is asserted.
    """
    source_path = _evidence_path(evidence_path, "evidence_path")
    if normalize_prefix(prefix) != prefix or not prefix:
        raise IdentityInventoryError("prefix must be an explicit canonical Meridian token")
    if evidence_kind == "catalog_schema_absent":
        if any(item is not None for item in (archive_result, archive_result_path, recovery_family, recovery_family_path)):
            raise IdentityInventoryError("catalog evidence cannot include archive/family inputs")
        if evidence.get("schema_version") != "tapdb-principal-census/v1":
            raise IdentityInventoryError("catalog_schema_absent requires a native principal census")
        if evidence.get("schemas") != [] or evidence.get("objects") != []:
            raise IdentityInventoryError("native census does not report the selected schema absent")
        target = evidence["target"]
        return {
            "schema_version": "tapdb-prefix-usage-from-evidence/v1",
            "observation_mode": evidence_kind,
            "scope": "explicit_database_schema_at_census",
            "target": dict(target), "physical_target": dict(evidence["physical_target"]),
            "domain_code": target["domain_code"], "prefix": prefix,
            "schema_present": False, "has_database_use": False,
            "allocation_journals": [], "has_journal_reservations": False,
            "external_allocation_journals": "not_supplied",
            "provenance": {
                "evidence_path": source_path, "evidence_sha256": evidence["sha256"],
                "observed_at": evidence["authenticated_operator"]["observed_at"],
                "time_basis": "native_catalog_observation",
                "authenticated_operator": dict(evidence["authenticated_operator"]),
            },
            "historical_allocation_claim": "not_established_by_schema_absence",
        }
    if evidence_kind != "archived_source_contract":
        raise IdentityInventoryError("evidence_kind must be archived_source_contract or catalog_schema_absent")
    if not isinstance(archive_result, Mapping) or not isinstance(recovery_family, Mapping):
        raise IdentityInventoryError("archive mode requires the successful copy-SOP result and explicit native recovery family")
    archive_path = _evidence_path(archive_result_path, "archive_result_path")
    family_path = _evidence_path(recovery_family_path, "recovery_family_path")
    projection = project_source_contract_prefix(evidence, prefix=prefix)
    physical = projection["physical_target"]
    fingerprint = archive_result.get("fingerprint", {})
    gate = archive_result.get("final_database_gates", {}).get(physical["database"], {})
    if (
        archive_result.get("status") != "copy_created_source_remains_fenced"
        or fingerprint.get("source") != physical["database"]
        or fingerprint.get("source_oid") != physical["database_oid"]
        or fingerprint.get("source_physical") != physical
        or fingerprint.get("source_contract_sha256") != evidence["sha256"]
        or gate.get("datname") != physical["database"]
        or gate.get("oid") != physical["database_oid"]
        or gate.get("datallowconn") is not False
    ):
        raise IdentityInventoryError("copy-SOP result does not bind this exact source contract to its fenced archive")
    completed_at = archive_result.get("completed_at")
    if not isinstance(completed_at, str) or not completed_at:
        raise IdentityInventoryError("copy-SOP result must retain its completion timestamp")
    copied_database = archive_result.get("copied_database", {})
    origin = recovery_family.get("origin", {})
    origin_target = origin.get("target", {})
    origin_physical = origin.get("physical_target", {})
    copied_name = fingerprint.get("target")
    # The copy-SOP's source contract and recorded copy identify this family's
    # origin. A family UUID or a journal directory alone does not establish
    # that relationship. Config paths may differ for the new copy; preserve
    # the explicit copied target rather than substituting the source config.
    if (
        not isinstance(copied_name, str) or not copied_name
        or copied_database.get("datname") != copied_name
        or type(copied_database.get("oid")) is not int
        or copied_database["oid"] <= 0
        or origin_target.get("database") != copied_name
        or any(origin_target.get(key) != projection["target"][key] for key in (
            "engine_type", "host", "port", "schema_name", "domain_code", "owner_repo_name"
        ))
        or origin_physical != {
            "database": copied_name, "database_oid": copied_database["oid"],
            "server_address": physical["server_address"],
            "server_port": physical["server_port"],
        }
    ):
        raise IdentityInventoryError("recovery family origin is not the exact copy recorded by this source's copy-SOP result")
    journals = _archived_journals(recovery_family, projection["target"], prefix)
    return {
        **projection,
        "schema_version": "tapdb-prefix-usage-from-evidence/v1",
        "observation_mode": evidence_kind,
        "scope": "explicit_archived_database_schema",
        "allocation_journals": journals,
        "has_journal_reservations": any(item["reservations"] for item in journals),
        "external_allocation_journals": "explicit_recovery_family_roots",
        "provenance": {
            "evidence_path": source_path, "archive_result_path": archive_path,
            "archive_evidence_kind": "copy_sop_source_remains_fenced",
            "archive_status": archive_result["status"], "completed_at": completed_at,
            "time_basis": "archive_operation_completion",
            "snapshot_capture_timestamp": None,
            "acceptance_reference": fingerprint["acceptance_reference"],
            "recovery_family_path": family_path,
            "recovery_family_id": recovery_family["family_id"],
            "recovery_family_sha256": recovery_family["sha256"],
            "recovery_family_origin": dict(origin),
            "recovery_family_linkage": {
                "relationship": "recorded_copy_of_archived_source",
                "source_physical_target": dict(physical),
                "copied_database": dict(copied_database),
                "copy_target": copied_name,
            },
        },
        "historical_allocation_claim": "explicit_source_contract_and_family_journals_only",
    }


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
