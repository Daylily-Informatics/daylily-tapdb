"""Sequence helpers for TapDB-backed EUID issuance.

TapDB uses shared per-prefix PostgreSQL sequences (for example:
``agx_instance_seq``) across every table that emits the same prefix. This
module centralizes the logic for creating and safely initializing these
sequences.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from daylily_tapdb.identity_inventory import (
    IdentityInventoryError,
    catalog_capture_context,
    catalog_columns,
    catalog_dependencies,
    catalog_tables,
    content_hash,
    physical_target,
    quote_identifier,
    require_snapshot,
    seal_receipt,
    validate_receipt,
    validate_target,
)
from daylily_tapdb.sequence_fence import (
    acquire_database_writer_fence as acquire_database_writer_fence,
)
from daylily_tapdb.sequence_fence import (
    apply_writer_fence_takeover as apply_writer_fence_takeover,
)
from daylily_tapdb.sequence_fence import (
    build_writer_fence_takeover_plan as build_writer_fence_takeover_plan,
)
from daylily_tapdb.sequence_fence import (
    reconcile_writer_fence_release as reconcile_writer_fence_release,
)
from daylily_tapdb.sequence_fence import (
    release_database_writer_fence as release_database_writer_fence,
)
from daylily_tapdb.sequence_fence import (
    validate_writer_quarantine as validate_writer_quarantine,
)

_INSTANCE_PREFIX_RE = re.compile(r"[0-9A-HJ-KMNP-TV-Z]{1,4}")

INVENTORY_VERSION = "tapdb-sequence-inventory/v1"
PLAN_VERSION = "tapdb-sequence-advance/v1"
_DEFINITION_FIELDS = (
    "name",
    "increment_by",
    "min_value",
    "max_value",
    "start_value",
    "cache_size",
    "cycle",
)


class SequenceProtectionError(IdentityInventoryError):
    """An allocator cannot be proven safe for issuance or recovery."""


def _integer(value: Any, field: str) -> int:
    if type(value) is not int:
        raise SequenceProtectionError(f"Sequence {field} must be an integer")
    return value


def sequence_definition(state: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return the complete portable generator definition."""
    if not isinstance(state, Mapping) or set(_DEFINITION_FIELDS) - set(state):
        raise SequenceProtectionError("Complete sequence definition is required")
    quote_identifier(state["name"])
    for field in _DEFINITION_FIELDS[1:-1]:
        _integer(state[field], field)
    if state["increment_by"] <= 0 or state["cycle"] is not False:
        raise SequenceProtectionError(
            "Only positive noncycling sequences are supported"
        )
    if state["cache_size"] < 1:
        raise SequenceProtectionError("Sequence cache_size must be positive")
    if not 1 <= state["min_value"] <= state["start_value"] <= state["max_value"]:
        raise SequenceProtectionError("Invalid positive sequence bounds/start")
    return {field: state[field] for field in _DEFINITION_FIELDS}


def sequence_next_value(state: Mapping[str, Any]) -> int:
    """Return the next unreserved value, rejecting exhaustion and ambiguity.

    For CACHE > 1, last_value includes all values reserved by prior sessions.
    Callers must close those sessions under a real fence before resuming.
    """
    definition = sequence_definition(state)
    value = _integer(state.get("last_value"), "last_value")
    if type(state.get("is_called")) is not bool:
        raise SequenceProtectionError("Sequence is_called must be explicit")
    if not definition["min_value"] <= value <= definition["max_value"]:
        raise SequenceProtectionError("Sequence last_value is outside its bounds")
    if (value - definition["start_value"]) % definition["increment_by"]:
        raise SequenceProtectionError(
            "Sequence state is not aligned to its start/increment"
        )
    result = value + definition["increment_by"] if state["is_called"] else value
    if result > definition["max_value"]:
        raise SequenceProtectionError(f"Sequence {state['name']} is exhausted")
    return result


def _prefix(value: Any) -> str:
    if not isinstance(value, str) or not _INSTANCE_PREFIX_RE.fullmatch(value):
        raise SequenceProtectionError("Stored/configured allocator prefix is malformed")
    return value


def _catalog_prefix_binding(annotation: Any, *, sequence_name: str) -> str | None:
    """Read only the versioned catalog contract; ordinary comments are not proof."""
    if annotation is None or not str(annotation).startswith("tapdb-prefix-binding"):
        return None
    marker = "tapdb-prefix-binding/v1:"
    if not isinstance(annotation, str) or not annotation.startswith(marker):
        raise SequenceProtectionError("Malformed catalog allocator prefix binding")
    prefix = _prefix(annotation[len(marker) :])
    if sequence_name != f"{prefix.lower()}_instance_seq":
        raise SequenceProtectionError(
            "Catalog allocator binding conflicts with generator"
        )
    return prefix


def _capture_sequence_inventory(
    connection: Any,
    *,
    schema_name: str,
    target: Mapping[str, Any],
    managed_tables: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Enumerate every generator and prove mappings using catalogs and bindings.

    Explicit ``target.sequence_mappings`` may retain mappings proven by a source
    contract for unreferenced/reserved generators. Merely matching a name is
    never evidence of ownership. Unknown mappings remain visible and block any
    advance or successful floor verification.
    """
    declarations = target.get("sequence_mappings", {})
    if not isinstance(declarations, Mapping):
        raise SequenceProtectionError(
            "target.sequence_mappings must be an explicit mapping"
        )
    exact_target = validate_target(target, schema_name)
    require_snapshot(connection)
    physical = physical_target(connection, exact_target)
    tables = catalog_tables(connection, schema_name)
    if managed_tables is not None:
        if managed_tables - {table["name"] for table in tables}:
            raise SequenceProtectionError(
                "Explicit managed sequence-binding tables are missing"
            )
        tables = [table for table in tables if table["name"] in managed_tables]
    table_columns = {
        table["name"]: catalog_columns(connection, table["oid"]) for table in tables
    }
    bindings: dict[str, list[dict[str, Any]]] = {}
    assigned: dict[str, int] = {}
    for table in tables:
        if table["kind"] == "f":
            raise SequenceProtectionError(
                "Foreign-table allocator mappings are unsupported"
            )
        name = table["name"]
        columns = {column["name"] for column in table_columns[name]}
        qualified = f"{quote_identifier(schema_name)}.{quote_identifier(name)}"
        scope: dict[str, Any] = {}
        predicate = ""
        parameters = {}
        if managed_tables is not None:
            if not {"domain_code", "issuer_app_code"} <= columns:
                # Legacy rows without configured owner scope cannot authorize
                # runtime prefix grants. Owned-column catalog proof is separate.
                continue
            predicate = (
                " WHERE domain_code=:runtime_domain AND issuer_app_code=:runtime_owner"
            )
            parameters = {
                "runtime_domain": target["domain_code"],
                "runtime_owner": target["owner_repo_name"],
            }
            scope = {
                "scope": {
                    "domain_code": target["domain_code"],
                    "owner_repo_name": target["owner_repo_name"],
                }
            }
        if {"euid_prefix", "euid_seq"} <= columns:
            for row in connection.execute(
                text(
                    f"SELECT euid_prefix, max(euid_seq) FROM {qualified}{predicate} GROUP BY euid_prefix ORDER BY euid_prefix"
                ),
                parameters,
            ):
                prefix = _prefix(row[0])
                value = _integer(row[1], "assigned_floor")
                bindings.setdefault(prefix, []).append(
                    {"source": "stored_rows", "table": name, **scope}
                )
                assigned[prefix] = max(assigned.get(prefix, 0), value)
        elif {"euid_prefix", "euid_seq"} & columns:
            raise SequenceProtectionError(
                f"Incomplete EUID allocator columns in {name}"
            )
        if name == "generic_template" and "instance_prefix" in columns:
            for (value,) in connection.execute(
                text(
                    f"SELECT DISTINCT instance_prefix FROM {qualified}{predicate} ORDER BY instance_prefix"
                ),
                parameters,
            ):
                bindings.setdefault(_prefix(value), []).append(
                    {"source": "template_binding", "table": name, **scope}
                )
        if name == "tapdb_identity_prefix_config" and "prefix" in columns:
            for (value,) in connection.execute(
                text(
                    f"SELECT DISTINCT prefix FROM {qualified}{predicate} ORDER BY prefix"
                ),
                parameters,
            ):
                bindings.setdefault(_prefix(value), []).append(
                    {"source": "identity_binding", "table": name, **scope}
                )

    rows = (
        connection.execute(
            text(
                "SELECT c.oid::bigint AS oid, c.relname AS name, pg_get_userbyid(c.relowner) AS owner, "
                "s.seqincrement AS increment_by,s.seqmin AS min_value,s.seqmax AS max_value, "
                "s.seqstart AS start_value,s.seqcache AS cache_size,s.seqcycle AS cycle, "
                "obj_description(c.oid,'pg_class') AS binding_annotation "
                "FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema ORDER BY c.relname"
            ),
            {"schema": schema_name},
        )
        .mappings()
        .all()
    )
    names = {row["name"] for row in rows}
    if set(declarations) - names:
        raise SequenceProtectionError(
            "Declared sequence mappings reference missing generators"
        )
    missing = sorted(
        f"{prefix.lower()}_instance_seq"
        for prefix in bindings
        if f"{prefix.lower()}_instance_seq" not in names
    )
    sequences = []
    for row in rows:
        state = dict(row)
        oid = state.pop("oid")
        name = state["name"]
        annotation = state.pop("binding_annotation")
        catalog_prefix = _catalog_prefix_binding(annotation, sequence_name=name)
        qualified = f"{quote_identifier(schema_name)}.{quote_identifier(name)}"
        state["dependencies"] = catalog_dependencies(connection, oid)
        ownership = [
            dict(item)
            for item in connection.execute(
                text(
                    "SELECT n.nspname AS schema_name,c.relname AS table_name,a.attname AS column_name, "
                    "d.deptype AS dependency_type FROM pg_depend d "
                    "JOIN pg_class c ON c.oid=d.refobjid JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=d.refobjsubid "
                    "WHERE d.classid='pg_class'::regclass AND d.objid=:oid "
                    "AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i') "
                    "ORDER BY n.nspname,c.relname,a.attname"
                ),
                {"oid": oid},
            ).mappings()
        ]
        matched_prefixes = [
            prefix for prefix in bindings if name == f"{prefix.lower()}_instance_seq"
        ]
        if catalog_prefix is not None and catalog_prefix not in matched_prefixes:
            matched_prefixes.append(catalog_prefix)
        declaration = declarations.get(name)
        if declaration is not None:
            if (
                not isinstance(declaration, Mapping)
                or set(declaration) != {"kind", "prefix", "evidence"}
                or declaration["kind"] != "prefix"
                or not str(declaration["evidence"]).strip()
            ):
                raise SequenceProtectionError(
                    "Explicit prefix mappings require kind, prefix, and evidence"
                )
            prefix = _prefix(declaration["prefix"])
            if name != f"{prefix.lower()}_instance_seq":
                raise SequenceProtectionError(
                    "Declared generator does not match the configured prefix protocol"
                )
            if prefix not in matched_prefixes:
                matched_prefixes.append(prefix)
        if ownership and matched_prefixes:
            raise SequenceProtectionError(f"Ambiguous UID/prefix ownership for {name}")
        if (
            managed_tables is not None
            and not matched_prefixes
            and not any(
                item["schema_name"] == schema_name
                and item["table_name"] in managed_tables
                for item in ownership
            )
        ):
            continue
        position = (
            connection.execute(text(f"SELECT last_value,is_called FROM {qualified}"))
            .mappings()
            .one()
        )
        state.update(position)
        floor = None
        if ownership:
            if len(ownership) != 1 or ownership[0]["schema_name"] != schema_name:
                raise SequenceProtectionError(
                    f"Cross-schema or ambiguous sequence ownership: {name}"
                )
            owner = ownership[0]
            floor = connection.execute(
                text(
                    f"SELECT max({quote_identifier(owner['column_name'])}) FROM "
                    f"{quote_identifier(schema_name)}.{quote_identifier(owner['table_name'])}"
                )
            ).scalar_one()
            state["mapping"] = {"kind": "owned_column", "columns": ownership}
        elif matched_prefixes:
            if len(matched_prefixes) != 1:
                raise SequenceProtectionError(f"Multiple prefixes map to {name}")
            prefix = matched_prefixes[0]
            evidence = list(bindings.get(prefix, []))
            if catalog_prefix is not None:
                evidence.append(
                    {"source": "catalog_annotation", "annotation": annotation}
                )
            if declaration is not None:
                evidence.append(
                    {
                        "source": "explicit_source_contract",
                        "evidence": declaration["evidence"],
                    }
                )
            state["mapping"] = {
                "kind": "prefix",
                "prefix": prefix,
                "evidence": evidence,
            }
            floor = assigned.get(prefix)
        else:
            state["mapping"] = {"kind": "unmapped"}
        state["assigned_floor"] = floor
        state["allocated_floor"] = state["last_value"] if state["is_called"] else None
        sequences.append(state)
    return seal_receipt(
        {
            "schema_version": INVENTORY_VERSION,
            "schema_name": schema_name,
            "target": exact_target,
            "physical_target": physical,
            "sequence_mappings": dict(declarations),
            "missing_generators": missing,
            "sequences": sequences,
        }
    )


def capture_sequence_inventory(
    connection: Any, *, schema_name: str, target: Mapping[str, Any]
) -> dict[str, Any]:
    """Capture every allocator with deterministic catalog qualifications."""
    with catalog_capture_context(connection, schema_name=schema_name):
        return _capture_sequence_inventory(
            connection, schema_name=schema_name, target=target
        )


def capture_runtime_sequence_bindings(
    connection: Any,
    *,
    schema_name: str,
    target: Mapping[str, Any],
    managed_tables: Sequence[str],
) -> dict[str, dict[str, Any]]:
    """Classify runtime-grant candidates using the shared mapping implementation.

    This is NOT a complete allocator inventory or an authorization decision.
    Stored prefix evidence is limited to the explicit domain/owner and managed
    tables; unknown extras are ungranted and need not be read. The principal
    authority must independently prove canonical tables and permitted bindings.
    """
    if isinstance(managed_tables, (str, bytes)) or not managed_tables:
        raise SequenceProtectionError("Explicit canonical managed tables are required")
    for name in managed_tables:
        quote_identifier(name)
    with catalog_capture_context(connection, schema_name=schema_name):
        inventory = _capture_sequence_inventory(
            connection,
            schema_name=schema_name,
            target=target,
            managed_tables=frozenset(managed_tables),
        )
    if inventory["missing_generators"]:
        raise SequenceProtectionError(
            "Configured core prefix bindings require missing generators"
        )
    result = {}
    for state in inventory["sequences"]:
        sequence_next_value(state)
        result[state["name"]] = state
    return result


def _validated_inventory(current: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    validate_receipt(current, INVENTORY_VERSION)
    validate_target(current["target"], current["schema_name"])
    if current.get("missing_generators"):
        raise SequenceProtectionError(
            "Missing generators: " + ", ".join(current["missing_generators"])
        )
    result = {}
    for state in current["sequences"]:
        name = state["name"]
        if name in result:
            raise SequenceProtectionError(f"Duplicate sequence in inventory: {name}")
        sequence_next_value(state)
        if state["mapping"]["kind"] not in {"owned_column", "prefix"}:
            raise SequenceProtectionError(f"Unknown allocator mapping: {name}")
        for field in ("allocated_floor", "assigned_floor"):
            if state[field] is not None:
                _integer(state[field], field)
        expected_allocated = state["last_value"] if state["is_called"] else None
        if state["allocated_floor"] != expected_allocated:
            raise SequenceProtectionError(
                "Recorded allocated floor disagrees with generator state"
            )
        result[name] = state
    return result


def _floors(
    floors: Sequence[Mapping[str, Any]], names: set[str]
) -> list[dict[str, Any]]:
    if not isinstance(floors, (list, tuple)):
        raise SequenceProtectionError(
            "floors must be an explicit list of named floor records"
        )
    normalized = []
    for floor in floors:
        if not isinstance(floor, Mapping) or set(floor) != {"name", "value", "source"}:
            raise SequenceProtectionError(
                "Each floor requires exactly name, value, and source"
            )
        if floor["name"] not in names:
            raise SequenceProtectionError(
                f"Floor requires a missing generator: {floor['name']}"
            )
        value = _integer(floor["value"], "floor")
        if (
            value < 0
            or not isinstance(floor["source"], str)
            or not floor["source"].strip()
        ):
            raise SequenceProtectionError(
                "Floor value must be nonnegative with explicit provenance"
            )
        normalized.append(dict(floor))
    return sorted(
        normalized, key=lambda item: (item["name"], item["value"], item["source"])
    )


def build_sequence_advance_plan(
    current: Mapping[str, Any],
    *,
    floors: Sequence[Mapping[str, Any]],
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Plan the smallest safe increment-aligned value without moving backwards."""
    states = _validated_inventory(current)
    retained = _floors(floors, set(states))
    family_fields: dict[str, Any] = {}
    if recovery_family is not None:
        from daylily_tapdb.backup.recovery import (
            recovery_family_state,
            require_family_member,
            validate_recovery_family,
        )

        family = validate_recovery_family(recovery_family)
        require_family_member(family, current)
        family_state = recovery_family_state(family, require_terminal=False)
        family_fields = {
            "recovery_family": family,
            "input_floors": retained,
            "family_state_sha256": content_hash(
                {
                    key: value
                    for key, value in family_state.items()
                    if key != "journal_heads"
                }
            ),
        }
        retained = _floors([*retained, *family_state["floors"]], set(states))
    advances = []
    for name, state in sorted(states.items()):
        applicable = [item["value"] for item in retained if item["name"] == name]
        applicable.extend(
            state[key]
            for key in ("allocated_floor", "assigned_floor")
            if state[key] is not None
        )
        floor = max(applicable) if applicable else state["min_value"] - 1
        increment, start = state["increment_by"], state["start_value"]
        steps = max(0, (floor + 1 - start + increment - 1) // increment)
        next_value = max(sequence_next_value(state), start + steps * increment)
        if next_value > state["max_value"]:
            raise SequenceProtectionError(
                f"Sequence {name} cannot advance beyond its maximum"
            )
        advances.append({"name": name, "floor": floor, "next_value": next_value})
    return seal_receipt(
        {
            "schema_version": PLAN_VERSION,
            "schema_name": current["schema_name"],
            "target": current["target"],
            "inventory": dict(current),
            "floors": retained,
            "advances": advances,
            **family_fields,
        }
    )


def verify_sequence_floors(
    current: Mapping[str, Any],
    *,
    floors: Sequence[Mapping[str, Any]],
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    plan = build_sequence_advance_plan(
        current, floors=floors, recovery_family=recovery_family
    )
    states = {state["name"]: state for state in current["sequences"]}
    violations = [
        item["name"]
        for item in plan["advances"]
        if sequence_next_value(states[item["name"]]) <= item["floor"]
    ]
    return seal_receipt(
        {
            "schema_version": "tapdb-sequence-verification/v1",
            "ok": not violations,
            "violations": violations,
            "inventory_sha256": current["sha256"],
            "floors": plan["floors"],
        }
    )


def validate_writer_fence(
    connection: Any, inventory: Mapping[str, Any], writer_fence: Mapping[str, Any]
) -> dict[str, Any]:
    """Prove complete, fresh session/worker exclusion and the exact physical gate."""
    from daylily_tapdb.sequence_fence import (
        backend_identity,
        census,
        provider_evidence,
        target_extensions,
        worker_baseline,
    )

    if not isinstance(writer_fence, Mapping) or writer_fence.get("mode") not in {
        "exclusive_database_connect",
        "database_connections_disabled",
    }:
        raise SequenceProtectionError(
            "An explicit physical database writer fence is required"
        )
    physical = physical_target(connection, inventory["target"])
    if (
        physical != inventory["physical_target"]
        or writer_fence.get("database_oid") != physical["database_oid"]
    ):
        raise SequenceProtectionError(
            "Writer fence is bound to a different physical database"
        )
    role = connection.execute(text("SELECT session_user")).scalar_one()
    allowed_keys = {"mode", "database_oid"}
    if writer_fence["mode"] == "exclusive_database_connect":
        allowed_keys.add("operator_role")
        if writer_fence.get("operator_role") != role:
            raise SequenceProtectionError(
                "Writer fence operator does not match authenticated session"
            )
        roles = (
            connection.execute(
                text(
                    "SELECT rolname FROM pg_roles WHERE rolcanlogin AND "
                    "has_database_privilege(oid, :database_oid, 'CONNECT') ORDER BY rolname"
                ),
                {"database_oid": physical["database_oid"]},
            )
            .scalars()
            .all()
        )
        if roles != [role]:
            raise SequenceProtectionError(
                "Database CONNECT is not exclusively fenced to this operator"
            )
    else:
        allowed_keys.update({"connection_backend", "provider_contract"})
        if writer_fence.get("connection_backend") != backend_identity(connection):
            raise SequenceProtectionError(
                "Writer fence requires the exact retained PID and backend_start"
            )
        allowed = connection.execute(
            text("SELECT datallowconn FROM pg_database WHERE oid=:oid"),
            {"oid": physical["database_oid"]},
        ).scalar_one()
        if allowed is not False:
            raise SequenceProtectionError("Database connections are not disabled")
    if set(writer_fence) != allowed_keys:
        raise SequenceProtectionError("Unknown writer fence fields")
    provider_evidence(
        connection,
        target=inventory["target"],
        operator_role=role,
        provider_contract=writer_fence.get("provider_contract"),
    )
    worker_baseline(
        connection,
        database_oid=physical["database_oid"],
        operator_role=role,
        aurora=inventory["target"]["engine_type"] == "aurora",
    )
    target_extensions(connection)
    census(
        connection,
        database=inventory["target"]["database"],
        database_oid=physical["database_oid"],
        retained_backend=backend_identity(connection),
    )
    return {
        "mode": writer_fence["mode"],
        "database_oid": physical["database_oid"],
        "operator_role": role,
        "other_sessions": [],
        "prepared_transactions": [],
    }


def _prior_sequence_intent_resolved(history: Sequence[Any], intent: Any) -> bool:
    """Require an exact terminal outcome or fresh, journal-bound observation.

    Larger numerical floors do not prove what an interrupted transaction did.
    Observed reconciliation preserves that unknown outcome, never calling it a
    successful commit merely because the current sequence appears advanced.
    """
    plan = intent.detail["plan"]
    outcomes = set()
    for row in history:
        if row.operation != "sequence_advance":
            continue
        detail = row.detail
        phase = detail.get("phase")
        if phase in {"committed", "rolled_back"}:
            result = detail.get("result", {})
            if result.get("intent_receipt_id") != intent.receipt_id:
                continue
            validate_receipt(result, "tapdb-sequence-apply/v1")
            if (
                result["phase"] != phase
                or result["plan_sha256"] != plan["sha256"]
                or result["inventory"]["target"] != plan["target"]
                or result["inventory"]["physical_target"]
                != plan["inventory"]["physical_target"]
                or result.get("recovery_family") != plan.get("recovery_family")
            ):
                raise SequenceProtectionError(
                    "Prior allocator outcome does not match its exact intent"
                )
            outcomes.add(phase)
        elif (
            phase == "reconciled_observed"
            and detail.get("intent_receipt_id") == intent.receipt_id
        ):
            quarantine = next(
                (
                    item.detail["receipt"]
                    for item in history
                    if item.operation == "sequence_writer_fence"
                    and item.detail.get("phase") == "quarantined"
                    and item.detail.get("receipt", {}).get("sha256")
                    == detail.get("reconciliation_receipt_sha256")
                ),
                None,
            )
            if quarantine is None:
                raise SequenceProtectionError(
                    "Prior allocator observation lacks its durable quarantine proof"
                )
            validate_receipt(quarantine, "tapdb-writer-fence-takeover/v1")
            if (
                detail.get("plan_sha256") != plan["sha256"]
                or detail.get("prior_transaction_outcome") != "unknown"
                or detail.get("observed_inventory") != quarantine["sequence_inventory"]
                or quarantine["target"] != plan["target"]
                or quarantine["physical_target"] != plan["inventory"]["physical_target"]
                or quarantine.get("recovery_family") != plan.get("recovery_family")
            ):
                raise SequenceProtectionError(
                    "Prior allocator observation does not match its exact intent and physical source"
                )
            outcomes.add(phase)
    if len(outcomes) > 1:
        raise SequenceProtectionError(
            "Prior allocator intent has contradictory terminal outcomes"
        )
    return bool(outcomes)


def apply_sequence_advance_plan(
    connection: Any,
    plan: Mapping[str, Any],
    *,
    writer_fence: Mapping[str, Any],
    receipts_dir: Path,
) -> dict[str, Any]:
    """Apply an unchanged reviewed plan inside the caller's transaction.

    The external intent survives database rollback. A returned pending-commit
    result is deliberately not a successful commit receipt; the caller owns
    commit/recovery and must retain the intent's conservative floors.

    ``verification.floors`` contains the already-applicable source and prior
    attempt floors that this operation proves its next values strictly exceed.
    ``floors`` additionally retains each exposed planned next value with source
    ``sequence_advance_intent`` for every future attempt or recovery. These are
    conservative future-attempt reservations, not persisted issued identities;
    the current next value may equal its own newly recorded reservation.
    """
    from daylily_tapdb.backup.receipts import (
        Actor,
        read_head,
        read_receipts,
        verify_receipt_chain,
        write_receipt,
    )

    validate_receipt(plan, PLAN_VERSION)
    expected = build_sequence_advance_plan(
        plan["inventory"],
        floors=plan.get("input_floors", plan["floors"]),
        recovery_family=plan.get("recovery_family"),
    )
    if expected != plan:
        raise SequenceProtectionError(
            "Reviewed advance plan is inconsistent with its inventory/floors"
        )
    directory = Path(receipts_dir)
    if not directory.is_absolute():
        raise SequenceProtectionError(
            "receipts_dir must be an explicit absolute external directory"
        )
    require_snapshot(connection)
    fence = validate_writer_fence(connection, plan["inventory"], writer_fence)
    # Clear this maintenance session's own pre-existing allocator cache too.
    connection.execute(text("DISCARD SEQUENCES"))
    target = dict(
        plan["target"], sequence_mappings=plan["inventory"]["sequence_mappings"]
    )
    current = capture_sequence_inventory(
        connection, schema_name=plan["schema_name"], target=target
    )
    if current != plan["inventory"]:
        raise SequenceProtectionError(
            "Stale sequence advance receipt: live inventory changed"
        )
    if directory.exists():
        prior = read_receipts(directory)
        head = read_head(directory)
        chain = verify_receipt_chain(prior, head=head)
        if (
            not chain.ok
            or (prior and head is None)
            or len(list(directory.glob("*.json"))) != len(prior)
        ):
            raise SequenceProtectionError("External receipt chain is not intact")
        next_values = {item["name"]: item["next_value"] for item in plan["advances"]}
        for old in prior:
            if (
                old.operation != "sequence_advance"
                or old.detail.get("phase") != "intent"
            ):
                continue
            old_plan = old.detail["plan"]
            if old_plan["target"] != plan["target"]:
                continue
            if old_plan["sha256"] == plan["sha256"]:
                raise SequenceProtectionError(
                    "This plan was already attempted; reconcile its external intent before retrying"
                )
            for floor in old.detail["floors"]:
                if (
                    floor["name"] not in next_values
                    or next_values[floor["name"]] <= floor["value"]
                ):
                    raise SequenceProtectionError(
                        "Plan does not retain the previous attempt's allocation floors"
                    )
            if not _prior_sequence_intent_resolved(prior, old):
                raise SequenceProtectionError(
                    "Unresolved prior allocator intent requires explicit outcome reconciliation before retry"
                )
    actor = Actor(surface="cli", username=fence["operator_role"])
    family_detail = (
        {"recovery_family": plan["recovery_family"]}
        if "recovery_family" in plan
        else {}
    )
    retained = plan["floors"] + [
        {
            "name": item["name"],
            "value": item["next_value"],
            "source": "sequence_advance_intent",
        }
        for item in plan["advances"]
    ]
    intent = write_receipt(
        directory,
        operation="sequence_advance",
        status="intent",
        actor=actor,
        detail={
            "phase": "intent",
            "plan": dict(plan),
            "writer_fence": fence,
            "floors": retained,
            **family_detail,
        },
    )
    savepoint = connection.begin_nested()
    try:
        current_states = {state["name"]: state for state in current["sequences"]}
        for item in plan["advances"]:
            if item["next_value"] == sequence_next_value(current_states[item["name"]]):
                continue
            connection.execute(
                text(
                    f"ALTER SEQUENCE {quote_identifier(plan['schema_name'])}.{quote_identifier(item['name'])} "
                    f"RESTART WITH {_integer(item['next_value'], 'next_value')}"
                )
            )
        validate_writer_fence(connection, plan["inventory"], writer_fence)
        after = capture_sequence_inventory(
            connection, schema_name=plan["schema_name"], target=target
        )
        verified = verify_sequence_floors(after, floors=plan["floors"])
        if not verified["ok"]:
            raise SequenceProtectionError(
                "Post-advance sequence floor verification failed"
            )
        actual = {
            state["name"]: sequence_next_value(state) for state in after["sequences"]
        }
        if actual != {item["name"]: item["next_value"] for item in plan["advances"]}:
            raise SequenceProtectionError(
                "Post-advance values do not match the reviewed plan"
            )
        result = seal_receipt(
            {
                "schema_version": "tapdb-sequence-apply/v1",
                "phase": "applied_pending_commit",
                "plan_sha256": plan["sha256"],
                "intent_receipt_id": intent.receipt_id,
                "inventory": after,
                "verification": verified,
                "floors": retained,
                **family_detail,
            }
        )
        write_receipt(
            directory,
            operation="sequence_advance",
            status="pending_commit",
            actor=actor,
            detail={
                "phase": "applied_pending_commit",
                "result": result,
                "floors": retained,
                **family_detail,
            },
        )
        savepoint.commit()
        return result
    except BaseException:
        savepoint.rollback()
        write_receipt(
            directory,
            operation="sequence_advance",
            status="failed",
            actor=actor,
            detail={
                "phase": "failed_or_ambiguous",
                "plan_sha256": plan["sha256"],
                "floors": retained,
                **family_detail,
            },
        )
        raise


def record_sequence_advance_outcome(
    result: Mapping[str, Any], *, receipts_dir: Path, outcome: str, actor: str
) -> dict[str, Any]:
    """Record the caller-owned transaction outcome outside the database."""
    from daylily_tapdb.backup.receipts import Actor, write_receipt

    validate_receipt(result, "tapdb-sequence-apply/v1")
    if result["phase"] != "applied_pending_commit" or outcome not in {
        "committed",
        "rolled_back",
        "ambiguous",
    }:
        raise SequenceProtectionError("Explicit transaction outcome is required")
    if not actor or not Path(receipts_dir).is_absolute():
        raise SequenceProtectionError(
            "Explicit actor and absolute receipts_dir are required"
        )
    payload = seal_receipt({**result, "phase": outcome})
    write_receipt(
        Path(receipts_dir),
        operation="sequence_advance",
        status=outcome,
        actor=Actor(surface="cli", username=actor),
        detail={
            "phase": outcome,
            "result": payload,
            "floors": result["floors"],
            **(
                {"recovery_family": result["recovery_family"]}
                if "recovery_family" in result
                else {}
            ),
        },
    )
    return payload


def _normalize_instance_prefix(prefix: str) -> str:
    normalized = prefix.strip().upper()
    if not _INSTANCE_PREFIX_RE.fullmatch(normalized):
        raise ValueError(f"Invalid TAPDB instance prefix: {prefix!r}")
    return normalized


def _build_ensure_instance_prefix_sequence_sql(
    seq_name: str, *, schema_name: str | None = None
) -> str:
    """Definition/state SQL only; assigned floors come from every catalog table."""
    quoted = quote_identifier(seq_name)
    if schema_name is not None:
        quoted = f"{quote_identifier(schema_name)}.{quoted}"
    return (
        f"SELECT :name AS name,s.seqincrement AS increment_by,s.seqmin AS min_value,"
        f"s.seqmax AS max_value,s.seqstart AS start_value,s.seqcache AS cache_size,"
        f"s.seqcycle AS cycle,position.last_value,position.is_called "
        f"FROM {quoted} AS position CROSS JOIN pg_sequence s "
        f"WHERE s.seqrelid=to_regclass(:qualified)"
    )


def ensure_instance_prefix_sequence(session: Any, prefix: str) -> None:
    """Provision/annotate an owned prefix generator, or verify an exact no-op.

    The caller supplies an already configured operator REPEATABLE READ session.
    Its actual current schema must prove the managed TapDB relation contract;
    public/implicit namespaces are refused. Every physical EUID-column relation
    is locked and inspected. Existing allocators are never repaired here.
    """
    normalized = _normalize_instance_prefix(prefix)
    connection = session.connection() if isinstance(session, Session) else session
    schema_name = connection.execute(text("SELECT current_schema()")).scalar_one()
    if (
        not isinstance(schema_name, str)
        or schema_name in {"public", "information_schema"}
        or schema_name.startswith("pg_")
    ):
        raise SequenceProtectionError(
            "Prefix provisioning requires an explicitly configured TapDB schema"
        )
    qualified_schema = quote_identifier(schema_name)
    seq_name = f"{normalized.lower()}_instance_seq"
    qualified = f"{qualified_schema}.{quote_identifier(seq_name)}"
    with (
        connection.begin_nested(),
        catalog_capture_context(connection, schema_name=schema_name),
    ):
        tables = catalog_tables(connection, schema_name)
        columns = {
            table["name"]: {
                column["name"] for column in catalog_columns(connection, table["oid"])
            }
            for table in tables
        }
        required = {
            "generic_template",
            "generic_instance",
            "generic_instance_lineage",
            "audit_log",
        }
        if (
            not required <= set(columns)
            or any(
                not {"euid_prefix", "euid_seq"} <= columns[name] for name in required
            )
            or "instance_prefix" not in columns["generic_template"]
        ):
            raise SequenceProtectionError(
                "Current schema does not prove the managed TapDB identity relations"
            )
        all_euid = []
        for table in tables:
            pair = {"euid_prefix", "euid_seq"} & columns[table["name"]]
            if pair and pair != {"euid_prefix", "euid_seq"}:
                raise SequenceProtectionError(
                    "Incomplete physical EUID allocator columns prevent provisioning"
                )
            if pair:
                if table["kind"] not in {"r", "p"}:
                    raise SequenceProtectionError(
                        "Unsupported physical EUID relation prevents complete provisioning proof"
                    )
                all_euid.append(f"{qualified_schema}.{quote_identifier(table['name'])}")
        connection.execute(
            text("LOCK TABLE " + ", ".join(all_euid) + " IN ACCESS EXCLUSIVE MODE")
        )
        # A schema-qualified, transaction-scoped lock serializes cooperative
        # creator calls too. It does not claim to fence arbitrary nextval users.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
            {"key": f"tapdb:prefix-provision:{schema_name}:{normalized}"},
        )
        found = connection.execute(
            text("SELECT to_regclass(:qualified)"), {"qualified": qualified}
        ).scalar_one()
        if found is None:
            connection.execute(
                text(f"CREATE SEQUENCE {qualified} AS bigint CACHE 1 NO CYCLE")
            )
        catalog = (
            connection.execute(
                text(
                    "SELECT c.oid::bigint AS oid,c.relkind,c.relpersistence,c.relowner=n.nspowner AS schema_owned,"
                    "s.seqtypid='bigint'::regtype AS bigint_type,pg_get_userbyid(n.nspowner)=session_user AS operator_owned,"
                    "obj_description(c.oid,'pg_class') AS annotation "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "LEFT JOIN pg_sequence s ON s.seqrelid=c.oid WHERE n.nspname=:schema AND c.relname=:name"
                ),
                {"schema": schema_name, "name": seq_name},
            )
            .mappings()
            .one()
        )
        if (
            catalog["relkind"] != "S"
            or catalog["relpersistence"] != "p"
            or catalog["schema_owned"] is not True
            or catalog["operator_owned"] is not True
            or catalog["bigint_type"] is not True
        ):
            raise SequenceProtectionError(
                "Prefix provisioning requires a permanent schema/operator-owned bigint sequence"
            )
        ownership = connection.execute(
            text(
                "SELECT count(*) FROM pg_depend WHERE classid='pg_class'::regclass AND objid=:oid "
                "AND refclassid='pg_class'::regclass AND deptype IN ('a','i')"
            ),
            {"oid": catalog["oid"]},
        ).scalar_one()
        if ownership:
            raise SequenceProtectionError(
                "An owned-column generator cannot be provisioned as a prefix generator"
            )
        state = dict(
            connection.execute(
                text(
                    _build_ensure_instance_prefix_sequence_sql(
                        seq_name, schema_name=schema_name
                    )
                ),
                {"name": seq_name, "qualified": qualified},
            )
            .mappings()
            .one()
        )
        next_value = sequence_next_value(state)
        assigned = [
            connection.execute(
                text(f"SELECT max(euid_seq) FROM {table} WHERE euid_prefix=:prefix"),
                {"prefix": normalized},
            ).scalar_one()
            for table in all_euid
        ]
        if any(
            value is not None and next_value <= _integer(value, "assigned_floor")
            for value in assigned
        ):
            raise SequenceProtectionError(
                f"Sequence {seq_name!r} is behind assigned identities; use an explicit fenced, receipt-bound 'tapdb db sequences advance' plan"
            )
        annotation = f"tapdb-prefix-binding/v1:{normalized}"
        if catalog["annotation"] not in {None, annotation}:
            raise SequenceProtectionError(
                "Existing allocator annotation conflicts with explicit prefix provisioning"
            )
        if catalog["annotation"] is None:
            # Prefix is validated before literal interpolation; PostgreSQL does
            # not accept bind parameters in COMMENT utility statements.
            connection.execute(
                text(f"COMMENT ON SEQUENCE {qualified} IS '{annotation}'")
            )
