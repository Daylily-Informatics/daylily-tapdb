"""Native adoption's exact historical inputs and bounded allocation surface."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import re
from typing import Any, Mapping

from sqlalchemy import text

from daylily_tapdb.backup.receipts import validate_receipts_directory
from daylily_tapdb.backup.recovery import (
    recovery_family_state, require_family_member, require_retained_definitions,
    retained_recovery_state, validate_recovery_family,
)
from daylily_tapdb.identity_inventory import (
    catalog_capture_context, catalog_columns, quote_identifier, seal_receipt,
)
from daylily_tapdb.sequence_fence import active_epoch, read_fence_history
from daylily_tapdb.sequences import (
    SequenceProtectionError, build_sequence_advance_plan,
    build_sequence_allocation_reservation,
)


def _checkpoint(directory: Path) -> dict[str, Any]:
    rows, head = read_fence_history(directory)
    return {"head": head, "receipts": [
        {"receipt_id": row.receipt_id, "sha256": row.checksum()} for row in rows
    ]}


def adoption_history(inventory, receipts_dir, recovery_family):
    """Capture every original root, retaining definitions as well as floors."""
    directory = validate_receipts_directory(str(receipts_dir))
    family = (validate_recovery_family(recovery_family, required_directory=directory)
              if recovery_family is not None else None)
    roots = sorted(set([str(directory)] + (family["receipts_dirs"] if family else [])))
    checkpoints = {root: _checkpoint(Path(root)) for root in roots}
    for root in roots:
        rows, _ = read_fence_history(Path(root))
        if active_epoch(rows, inventory["target"]) is not None:
            raise SequenceProtectionError("An active writer fence requires native reconciliation")
    retained = retained_recovery_state(directory, target=inventory["target"], require_terminal=True)
    floors = list(retained["floors"])
    inventories = list(retained["inventories"])
    if family:
        require_family_member(family, inventory)
        state = recovery_family_state(family, require_terminal=True)
        floors.extend(state["floors"])
        inventories.extend(state["inventories"])
    require_retained_definitions(inventory, inventories)
    if checkpoints != {root: _checkpoint(Path(root)) for root in roots}:
        raise SequenceProtectionError("Original adoption history changed during planning")
    return {
        "receipts_dir": str(directory), "recovery_family": family,
        "checkpoints": checkpoints, "floors": floors,
        "inventory_sha256": sorted({item["sha256"] for item in inventories}),
    }


def require_adoption_history(history, inventory, *, fence_receipt=None):
    """Allow only this operation's two acquisition receipts after review."""
    directory = validate_receipts_directory(str(history["receipts_dir"]))
    family = history["recovery_family"]
    if family is not None:
        family = validate_recovery_family(family, required_directory=directory)
    roots = set([str(directory)] + (family["receipts_dirs"] if family else []))
    if set(history["checkpoints"]) != roots:
        raise SequenceProtectionError("Every original adoption/family root needs its exact checkpoint")
    for root, expected in history["checkpoints"].items():
        rows, head = read_fence_history(Path(root))
        prefix = [{"receipt_id": row.receipt_id, "sha256": row.checksum()}
                  for row in rows[:len(expected["receipts"])]]
        if prefix != expected["receipts"]:
            raise SequenceProtectionError("Original adoption journal prefix changed")
        suffix = rows[len(prefix):]
        if fence_receipt is None or root != history["receipts_dir"]:
            if suffix or head != expected["head"]:
                raise SequenceProtectionError("Adoption history changed after review")
            continue
        if (
            len(suffix) != 2
            or any(row.operation != "sequence_writer_fence" for row in suffix)
            or suffix[0].detail.get("phase") != "acquire_intent"
            or suffix[0].receipt_id != fence_receipt["intent_receipt_id"]
            or suffix[0].detail.get("target") != fence_receipt["target"]
            or suffix[0].detail.get("physical_target") != fence_receipt["physical_target"]
            or suffix[0].detail.get("recovery_family") != history["recovery_family"]
            or suffix[1].detail.get("phase") != "acquired"
            or suffix[1].detail.get("receipt") != fence_receipt
        ):
            raise SequenceProtectionError("Unrelated journal activity after adoption review")
    retained = retained_recovery_state(
        Path(history["receipts_dir"]), target=inventory["target"], require_terminal=True)
    floors, inventories = list(retained["floors"]), list(retained["inventories"])
    if family is not None:
        family = validate_recovery_family(family, required_directory=history["receipts_dir"])
        require_family_member(family, inventory)
        state = recovery_family_state(family, require_terminal=True)
        floors.extend(state["floors"])
        inventories.extend(state["inventories"])
    require_retained_definitions(inventory, inventories)
    if (floors != history["floors"] or sorted({item["sha256"] for item in inventories})
            != history["inventory_sha256"]):
        raise SequenceProtectionError("Reviewed adoption omitted or changed original historical evidence")


def allocation_paths(connection, cfg, inventory, templates):
    """Resolve actual identity dependencies; never infer a prefix from a name."""
    schema = cfg["schema_name"]
    states = inventory["sequences"]
    bindings = [dict(row) for row in connection.execute(text(
        f"SELECT entity,prefix FROM {quote_identifier(schema)}.tapdb_identity_prefix_config "
        "WHERE domain_code=:domain AND issuer_app_code=:owner "
        "AND entity IN ('generic_template','audit_log') ORDER BY entity"
    ), {"domain": cfg["domain_code"], "owner": cfg["owner_repo_name"]}).mappings()]
    if len(bindings) != 2 or {row["entity"] for row in bindings} != {"generic_template", "audit_log"}:
        raise SequenceProtectionError("Exact template and audit identity prefix bindings required")

    def prefix_sequence(prefix):
        found = [s["name"] for s in states
                 if s["mapping"].get("kind") == "prefix" and s["mapping"].get("prefix") == prefix]
        if len(found) != 1:
            raise SequenceProtectionError("Missing or ambiguous native prefix allocation path")
        return found[0]

    paths = []
    for row in bindings:
        table = row["entity"]
        owned = [s["name"] for s in states if s["mapping"].get("kind") == "owned_column"
                 and {"schema_name": schema, "table_name": table, "column_name": "uid",
                      "dependency_type": "i"} in s["mapping"]["columns"]]
        if len(owned) != 1:
            raise SequenceProtectionError("Exact native UID identity dependency required")
        paths.extend([
            {"table": table, "column": "uid", "sequence": owned[0]},
            {"table": table, "column": "euid", "prefix": row["prefix"],
             "sequence": prefix_sequence(row["prefix"])},
        ])
    # These five literal prefixes are the unchanged packaged schema's
    # CREATE SEQUENCE IF NOT EXISTS statements. Together with the two bundled
    # template prefixes, require them before any fenced DDL could create one.
    prefixes = ["WX", "WSX", "XX", "AY", "MSG"] + [item["instance_prefix"] for item in templates]
    for prefix in prefixes:
        name = prefix.lower() + "_instance_seq"
        if not any(s["name"] == name for s in states):
            raise SequenceProtectionError(
                "Combined adoption requires existing schema/bundled prefix generators; creation is unsupported")
        elif prefix_sequence(prefix) != name:
            raise SequenceProtectionError("Bundled template prefix lacks its native generator")
    counts = Counter()
    for path in paths:
        counts[path["sequence"]] += len(templates)
    return {"paths": paths, "maximum_template_inserts": len(templates),
            "allocation_counts": dict(sorted(counts.items()))}


def build_adoption_allocator_inputs(connection, cfg, inventory, templates, *,
                                   receipts_dir, recovery_family):
    history = adoption_history(inventory, receipts_dir, recovery_family)
    plan = build_sequence_advance_plan(
        inventory, floors=history["floors"], recovery_family=history["recovery_family"])
    paths = allocation_paths(connection, cfg, inventory, templates)
    reservation = build_sequence_allocation_reservation(
        plan, allocation_counts=paths["allocation_counts"])
    return {"history": history, "allocator_plan": plan,
            "allocation_paths": paths, "allocation_reservation": reservation,
            "allocation_surface": capture_allocation_surface(connection, cfg)}


def capture_allocation_surface(connection, cfg):
    """Read, validate and seal insert-time paths before any native mutation.

    Old trigger routines need not yet match TapDB11: the separate canonical
    verifier proves those after asset installation and before template writes.
    Noncanonical types/defaults/index hooks cannot hide unreserved allocations.
    """
    with catalog_capture_context(connection, schema_name=cfg["schema_name"]):
        return _capture_allocation_surface(connection, cfg)


def _capture_allocation_surface(connection, cfg):
    schema = cfg["schema_name"]
    relations = [dict(row) for row in connection.execute(text(
        "SELECT c.oid::bigint AS oid,c.relname AS name,c.relkind AS kind,c.relispartition AS partition, "
        "am.amname AS access_method,an.nspname AS handler_schema "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "LEFT JOIN pg_am am ON am.oid=c.relam LEFT JOIN pg_proc ap ON ap.oid=am.amhandler "
        "LEFT JOIN pg_namespace an ON an.oid=ap.pronamespace "
        "WHERE n.nspname=:schema AND c.relname IN ('generic_template','audit_log') ORDER BY c.relname"
    ), {"schema": schema}).mappings()]
    if len(relations) != 2 or any(row["kind"] != "r" or row["partition"]
                                or row["access_method"] != "heap" or row["handler_schema"] != "pg_catalog"
                                for row in relations):
        raise SequenceProtectionError("Bounded adoption requires ordinary template/audit tables")
    for relation in relations:
        types = [dict(row) for row in connection.execute(text(
            "SELECT a.attname AS column_name,t.typname AS type_name,n.nspname AS type_schema,t.typtype AS type_kind, "
            "cn.nspname AS collation_schema FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid "
            "JOIN pg_namespace n ON n.oid=t.typnamespace LEFT JOIN pg_collation coll ON coll.oid=a.attcollation "
            "LEFT JOIN pg_namespace cn ON cn.oid=coll.collnamespace "
            "WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
        ), {"oid": relation["oid"]}).mappings()]
        if any(row["type_schema"] != "pg_catalog" or row["type_kind"] != "b"
               or row["type_name"] not in {"int8", "int4", "text", "varchar", "bool", "jsonb",
                                         "timestamptz", "timestamp", "uuid", "numeric", "float8", "float4", "date"}
               or row["collation_schema"] not in {None, "pg_catalog"} for row in types):
            raise SequenceProtectionError("Custom/domain template or audit column types are unsupported")
        relation["types"] = types
        relation["columns"] = catalog_columns(connection, relation["oid"])
        if {column["name"] for column in relation["columns"]} != {row["column_name"] for row in types} or not any(
            column["name"] == "uid" for column in relation["columns"]
        ):
            raise SequenceProtectionError("Complete native template/audit column identity required")
        for column in relation["columns"]:
            if column["name"] == "uid":
                if column["identity"] not in {"a", "d"} or column["default"] is not None:
                    raise SequenceProtectionError("Canonical native UID identity required")
                continue
            default = column["default"]
            literal = default is None or default in {"CURRENT_TIMESTAMP", "now()", "true", "false"}
            literal = literal or bool(re.fullmatch(
                r"(?:[0-9]+|'(?:[^']|'')*')(?:::(?:text|character varying|jsonb|bigint|integer|boolean))?",
                default or ""))
            if column["identity"] or column["generated"] or not literal:
                raise SequenceProtectionError("Unbounded native template/audit column allocation")
        # Constraint expressions may call only immutable builtins.
        # Mutable/custom expression hooks are unsupported.
        unsafe = connection.execute(text(
            "WITH expressions AS ("
            "SELECT 'pg_constraint'::regclass AS classid,oid FROM pg_constraint WHERE conrelid=:oid "
            "UNION ALL SELECT 'pg_class'::regclass,indexrelid FROM pg_index WHERE indrelid=:oid) "
            "SELECT count(*) FROM expressions e JOIN pg_depend d ON d.classid=e.classid AND d.objid=e.oid "
            "LEFT JOIN pg_operator op ON d.refclassid='pg_operator'::regclass AND d.refobjid=op.oid "
            "JOIN pg_proc p ON (d.refclassid='pg_proc'::regclass AND d.refobjid=p.oid) OR p.oid=op.oprcode "
            "JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE p.provolatile <> 'i' OR n.nspname <> 'pg_catalog'"
        ), {"oid": relation["oid"]}).scalar_one()
        rules = connection.execute(text(
            "SELECT count(*) FROM pg_rewrite WHERE ev_class=:oid"
        ), {"oid": relation["oid"]}).scalar_one()
        custom_index = connection.execute(text(
            "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
            "JOIN pg_am am ON am.oid=c.relam JOIN pg_proc handler ON handler.oid=am.amhandler "
            "JOIN pg_namespace hn ON hn.oid=handler.pronamespace "
            "CROSS JOIN LATERAL unnest(i.indclass) AS opclass(oid) "
            "JOIN pg_opclass oc ON oc.oid=opclass.oid JOIN pg_namespace n ON n.oid=oc.opcnamespace "
            "WHERE i.indrelid=:oid AND (n.nspname <> 'pg_catalog' OR hn.nspname <> 'pg_catalog')"
        ), {"oid": relation["oid"]}).scalar_one()
        if unsafe or rules or custom_index:
            raise SequenceProtectionError("Custom template/audit constraint, index, operator or rule allocation is unsupported")
        relation["constraints"] = [dict(row) for row in connection.execute(text(
            "SELECT conname AS name,contype AS kind,convalidated AS validated, "
            "condeferrable AS deferrable,condeferred AS deferred,pg_get_constraintdef(oid) AS definition "
            "FROM pg_constraint WHERE conrelid=:oid ORDER BY conname"
        ), {"oid": relation["oid"]}).mappings()]
        relation["indexes"] = [dict(row) for row in connection.execute(text(
            "SELECT c.relname AS name,pg_get_indexdef(i.indexrelid) AS definition, "
            "i.indclass::text AS operator_classes,i.indcollation::text AS collations, "
            "i.indisvalid AS valid,i.indisready AS ready,i.indislive AS live,am.amname AS access_method "
            "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam "
            "WHERE i.indrelid=:oid ORDER BY c.relname"
        ), {"oid": relation["oid"]}).mappings()]
        relation["rules"] = []
    return seal_receipt({"schema_version": "tapdb-adoption-allocation-surface/v1",
                         "schema_name": schema, "relations": relations})


def require_bounded_allocation_surface(connection, cfg):
    """Prove the newly installed canonical trigger path before native seeding."""
    from daylily_tapdb.runtime_principal import build_runtime_principal_binding_plan

    binding = build_runtime_principal_binding_plan(connection, cfg)
    surface = capture_allocation_surface(connection, cfg)
    return seal_receipt({"binding_sha256": binding["sha256"],
                         "allocation_surface_sha256": surface["sha256"]})["sha256"]
