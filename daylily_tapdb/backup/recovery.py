"""Durable recovery floors in the existing external, hash-chained receipt store.

An old dump alone cannot prove allocations made after its capture. A resumed
recovery requires a final fenced source inventory or an explicitly identified
isolated rehearsal. Every intent and observed outcome retains its floors,
including failures. An unresolved operation must be reconciled before retry.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence
from uuid import UUID, uuid4

from daylily_tapdb.backup.errors import BackupVerificationError
from daylily_tapdb.backup.receipts import (
    Actor,
    Receipt,
    read_head,
    read_receipts,
    verify_receipt_chain,
    write_receipt,
)

OPERATION_RECOVERY = "identity_recovery"
RECOVERY_VERSION = "tapdb-recovery-floor/v1"
RECOVERY_FAMILY_VERSION = "tapdb-recovery-family/v1"


def _history(directory: Path) -> list[Receipt]:
    if not directory.is_absolute():
        raise BackupVerificationError("recovery receipts directory must be absolute")
    receipts: list[Receipt] = read_receipts(directory)
    head = read_head(directory)
    paths = list(directory.glob("[0-9]*.json")) if directory.exists() else []
    if len(paths) != len(receipts) or (receipts and not head):
        raise BackupVerificationError(
            "recovery receipt chain is unreadable or lacks its head"
        )
    verification = verify_receipt_chain(receipts, head=head)
    if not verification.ok:
        raise BackupVerificationError(
            "recovery receipt chain is invalid",
            detail={"findings": verification.findings},
        )
    if receipts and head is not None and int(head["sequence"]) != receipts[-1].sequence:
        raise BackupVerificationError("recovery receipt head is stale")
    return receipts


def _member_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    from daylily_tapdb.identity_inventory import validate_target

    target = value.get("target")
    physical = value.get("physical_target")
    if not isinstance(target, Mapping) or not isinstance(physical, Mapping):
        raise BackupVerificationError(
            "family member needs exact target and physical_target"
        )
    normalized = validate_target(target, str(target.get("schema_name", "")))
    if dict(target) != normalized or set(physical) != {
        "database",
        "database_oid",
        "server_address",
        "server_port",
    }:
        raise BackupVerificationError("family member identity is not canonical")
    if (
        physical["database"] != normalized["database"]
        or type(physical["database_oid"]) is not int
        or physical["database_oid"] <= 0
        or type(physical["server_port"]) is not int
        or physical["server_port"] != normalized.get("server_port", normalized["port"])
        or not isinstance(physical["server_address"], str)
        or not physical["server_address"].strip()
    ):
        raise BackupVerificationError("family member physical identity is invalid")
    return {"target": normalized, "physical_target": dict(physical)}


def validate_recovery_family(
    family: Mapping[str, Any], *, required_directory: str | Path | None = None
) -> dict[str, Any]:
    """Validate an immutable, explicitly rooted recovery family descriptor.

    Roots are never discovered from names or related directories. Their declared
    completeness is an operator contract; every declared root must remain readable.
    """
    from daylily_tapdb.identity_inventory import seal_receipt

    if (
        not isinstance(family, Mapping)
        or set(family)
        != {"schema_version", "family_id", "origin", "receipts_dirs", "sha256"}
        or family.get("schema_version") != RECOVERY_FAMILY_VERSION
        or family.get("sha256") != seal_receipt(family)["sha256"]
    ):
        raise BackupVerificationError("invalid recovery family descriptor or checksum")
    try:
        if str(UUID(family["family_id"])) != family["family_id"]:
            raise ValueError("noncanonical UUID")
    except (ValueError, TypeError, AttributeError) as exc:
        raise BackupVerificationError(
            "family_id must be an explicit canonical UUID"
        ) from exc
    origin = family["origin"]
    if not isinstance(origin, Mapping) or set(origin) != {"target", "physical_target"}:
        raise BackupVerificationError("recovery family origin must be explicit")
    _member_identity(origin)
    roots = family["receipts_dirs"]
    if (
        not isinstance(roots, list)
        or not roots
        or any(not isinstance(item, str) for item in roots)
        or roots != sorted(set(roots))
    ):
        raise BackupVerificationError(
            "recovery family receipts_dirs must be sorted and unique"
        )
    for root in roots:
        path = Path(root)
        if not path.is_absolute() or not path.is_dir() or str(path.resolve()) != root:
            raise BackupVerificationError(
                "recovery family roots must be existing canonical absolute directories"
            )
        _history(path)
    if required_directory is not None and str(Path(required_directory)) not in roots:
        raise BackupVerificationError(
            "receipts directory is outside the explicit recovery family"
        )
    return dict(family)


def build_recovery_family(
    *,
    family_id: str,
    origin_inventory: Mapping[str, Any],
    receipts_dirs: Sequence[str | Path],
) -> dict[str, Any]:
    """Seal a user-chosen UUID, complete original inventory and explicit roots.

    This read-only builder does not invent an identity, create a directory, join
    a replacement, or assert that ordinary unjournaled writes are recoverable.
    """
    from daylily_tapdb.identity_inventory import seal_receipt

    inventory_floors(origin_inventory, source="family_origin")
    if isinstance(receipts_dirs, (str, bytes)):
        raise BackupVerificationError(
            "receipts_dirs must be an explicit list of directories"
        )
    family = seal_receipt(
        {
            "schema_version": RECOVERY_FAMILY_VERSION,
            "family_id": family_id,
            "origin": _member_identity(origin_inventory),
            "receipts_dirs": sorted(str(Path(path)) for path in receipts_dirs),
        }
    )
    return validate_recovery_family(family)


def _validated_floors(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        raise BackupVerificationError("retained allocator floors must be a list")
    for value in values:
        if (
            not isinstance(value, dict)
            or set(value) != {"name", "value", "source"}
            or not isinstance(value["name"], str)
            or not value["name"]
            or type(value["value"]) is not int
            or not isinstance(value["source"], str)
            or not value["source"].strip()
        ):
            raise BackupVerificationError("malformed retained allocator floor")
    return list(values)


def recovery_family_state(
    family: Mapping[str, Any], *, require_terminal: bool = True
) -> dict[str, Any]:
    """Union evidenced physical family members across every declared journal.

    Config names alone never identify a member. The immutable origin is the
    initial member; new members require an intent-linked, live physical
    observation written before import or allocator mutation.
    """
    from daylily_tapdb.identity_inventory import content_hash

    family = validate_recovery_family(family)
    members = {content_hash(family["origin"]): family["origin"]}
    rows: list[tuple[str, Receipt]] = []
    heads = {}
    # Receipt identifiers are unique within a journal, not across the family's
    # explicitly configured roots. Different roots may write the same sequence
    # number in the same second.
    tagged: set[tuple[str, str]] = set()
    intents: dict[str, Receipt] = {}
    pending: dict[str, str] = {}
    for root in family["receipts_dirs"]:
        history = _history(Path(root))
        heads[root] = read_head(Path(root))
        for receipt in history:
            rows.append((root, receipt))
            data = receipt.detail
            declared = data.get("recovery_family")
            if declared is None:
                continue
            declared = validate_recovery_family(declared, required_directory=root)
            if declared["family_id"] != family["family_id"]:
                continue
            if declared != family:
                raise BackupVerificationError(
                    "recovery family descriptor changed within its lineage"
                )
            tagged.add((root, receipt.receipt_id))
            if receipt.operation != OPERATION_RECOVERY:
                continue
            if data.get("schema_version") != RECOVERY_VERSION:
                raise BackupVerificationError("unsupported recovery family event")
            operation_id = str(data.get("operation_id", ""))
            key = f"{root}:{operation_id}"
            phase = data.get("phase")
            if phase == "intent":
                if key in intents:
                    raise BackupVerificationError("duplicate recovery family intent")
                intents[key] = receipt
                pending[key] = receipt.receipt_id
                continue
            original = intents.get(key)
            if (
                original is None
                or key not in pending
                or data.get("intent_sha256") != original.checksum()
                or data.get("target") != original.detail.get("target")
            ):
                raise BackupVerificationError(
                    "recovery family event has no matching pending intent"
                )
            if phase == "observed":
                member = _member_identity(data)
                members[content_hash(member)] = member
            elif phase in {"committed", "aborted", "reconciled", "reconciled_observed"}:
                pending.pop(key)
            elif phase != "ambiguous":
                raise BackupVerificationError("unknown recovery family phase")

    floors: list[dict[str, Any]] = []
    inventories: dict[str, dict[str, Any]] = {}
    sequence_intents: dict[tuple[str, str], str] = {}
    sequence_pending: set[tuple[str, str]] = set()
    legacy_intents: dict[str, Receipt] = {}
    quarantines = {
        row.detail["receipt"]["sha256"]: row.detail["receipt"]
        for _root, row in rows
        if row.operation == "sequence_writer_fence"
        and row.detail.get("receipt", {}).get("phase") == "quarantined"
    }

    def member_inventory(value: Any) -> bool:
        if not isinstance(value, dict):
            return False
        member = _member_identity(value)
        if content_hash(member) not in members:
            return False
        inventory_floors(value, source=f"family:{value.get('sha256')}")
        inventories[value["sha256"]] = value
        return True

    for root, receipt in rows:
        data = receipt.detail
        if receipt.operation == OPERATION_RECOVERY:
            if (root, receipt.receipt_id) not in tagged:
                # Retain pre-family origin history only through physically
                # evidenced inventory, never a matching database name alone.
                selected = any(
                    member_inventory(item) for item in data.get("inventories", [])
                )
                legacy_key = f"{root}:{data.get('operation_id', '')}"
                if not selected and legacy_key not in legacy_intents:
                    continue
                if data.get("phase") == "intent":
                    legacy_intents[legacy_key] = receipt
                    pending[legacy_key] = receipt.receipt_id
                else:
                    original = legacy_intents.get(legacy_key)
                    if (
                        original is None
                        or data.get("intent_sha256") != original.checksum()
                    ):
                        raise BackupVerificationError(
                            "pre-family outcome has no matching intent"
                        )
                    if data.get("phase") in {
                        "committed",
                        "aborted",
                        "reconciled",
                        "reconciled_observed",
                    }:
                        pending.pop(legacy_key, None)
            for value in data.get("inventories", []):
                if not member_inventory(value):
                    raise BackupVerificationError(
                        "recovery inventory belongs to an unproven family member"
                    )
            floors.extend(_validated_floors(data.get("floors", [])))
        elif receipt.operation == "sequence_advance":
            plan = data.get("plan", {})
            result = data.get("result", {})
            if data.get("phase") == "reconciled_observed":
                intent_id = data.get("intent_receipt_id")
                intent_key = (root, str(intent_id))
                if intent_key not in sequence_pending:
                    continue
                if (
                    sequence_intents.get((root, str(data.get("plan_sha256"))))
                    != intent_id
                    or data.get("prior_transaction_outcome") != "unknown"
                    or data.get("reconciliation_receipt_sha256") not in quarantines
                    or not member_inventory(data.get("observed_inventory"))
                ):
                    raise BackupVerificationError(
                        "invalid observed allocator reconciliation"
                    )
                observed_quarantine = quarantines[data["reconciliation_receipt_sha256"]]
                if _member_identity(observed_quarantine) != _member_identity(
                    data["observed_inventory"]
                ):
                    raise BackupVerificationError(
                        "allocator reconciliation references another physical quarantine"
                    )
                floors.extend(_validated_floors(data.get("floors", [])))
                sequence_pending.remove(intent_key)
            elif data.get("phase") == "intent" and member_inventory(
                plan.get("inventory")
            ):
                sequence_intents[(root, plan["sha256"])] = receipt.receipt_id
                sequence_pending.add((root, receipt.receipt_id))
                floors.extend(_validated_floors(data.get("floors", [])))
            elif member_inventory(result.get("inventory")):
                intent_id = result.get("intent_receipt_id")
                intent_key = (root, str(intent_id))
                if intent_key not in sequence_pending:
                    raise BackupVerificationError(
                        "family allocator outcome has no matching intent"
                    )
                floors.extend(_validated_floors(data.get("floors", [])))
                if data.get("phase") in {"committed", "rolled_back", "reconciled"}:
                    sequence_pending.remove(intent_key)
            elif (root, str(data.get("plan_sha256"))) in sequence_intents:
                floors.extend(_validated_floors(data.get("floors", [])))
        elif receipt.operation == "sequence_writer_fence":
            detail = data.get("receipt", data)
            if (root, receipt.receipt_id) in tagged:
                member = _member_identity(detail)
                if content_hash(member) not in members:
                    raise BackupVerificationError(
                        "writer fence belongs to an unproven family member"
                    )
                floors.extend(_validated_floors(detail.get("retained_floors", [])))
                floors.extend(_validated_floors(detail.get("floors", [])))
                if detail.get("sequence_inventory") is not None:
                    if not member_inventory(detail["sequence_inventory"]):
                        raise BackupVerificationError(
                            "quarantine inventory belongs to an unproven family member"
                        )
                    floors.extend(
                        inventory_floors(
                            detail["sequence_inventory"], source="family_quarantine"
                        )
                    )

    pending.update({f"{root}:allocator:{key}": key for root, key in sequence_pending})
    if require_terminal and pending:
        raise BackupVerificationError(
            "unresolved recovery family operations require reconciliation",
            detail={"pending": pending},
        )
    unique_floors = {
        (item["name"], item["value"], item["source"]): item for item in floors
    }
    return {
        "family_sha256": family["sha256"],
        "members": [members[key] for key in sorted(members)],
        "floors": [unique_floors[key] for key in sorted(unique_floors)],
        "inventories": [inventories[key] for key in sorted(inventories)],
        "pending": pending,
        "journal_heads": heads,
    }


def require_family_member(
    family: Mapping[str, Any], inventory: Mapping[str, Any]
) -> None:
    """Require an exact original or previously observed physical member."""
    member = _member_identity(inventory)
    if member not in recovery_family_state(family, require_terminal=False)["members"]:
        raise BackupVerificationError(
            "physical target is not an evidenced recovery family member"
        )


def inventory_floors(
    inventory: Mapping[str, Any], *, source: str
) -> list[dict[str, Any]]:
    """Project the shared inventory's already computed allocation boundaries."""
    from daylily_tapdb.sequences import verify_sequence_floors

    # Validate the receipt and definitions, but retain observed floors even
    # when a failed operation left the generator behind its assigned rows.
    # Terminal success is separately gated on strict verification.
    verify_sequence_floors(inventory, floors=[])
    floors = []
    for sequence in inventory["sequences"]:
        for field in ("allocated_floor", "assigned_floor"):
            if sequence[field] is not None:
                floors.append(
                    {
                        "name": sequence["name"],
                        "value": sequence[field],
                        "source": f"{source}:{field}",
                    }
                )
    return floors


def retained_recovery_state(
    directory: Path, *, target: Mapping[str, Any], require_terminal: bool = True
) -> dict[str, Any]:
    from daylily_tapdb.identity_inventory import validate_target

    target = validate_target(target, str(target["schema_name"]))
    floors: list[dict[str, Any]] = []
    inventories: list[dict[str, Any]] = []
    pending: dict[str, str] = {}
    intent_hashes: dict[str, str] = {}
    for receipt in _history(Path(directory)):
        data = receipt.detail
        if receipt.operation == "sequence_advance":
            plan = data.get("plan", {})
            result = data.get("result", {})
            sequence_target = plan.get("target") or result.get("inventory", {}).get(
                "target"
            )
            if sequence_target == dict(target):
                floors.extend(data.get("floors", []))
                for inventory in (plan.get("inventory"), result.get("inventory")):
                    if isinstance(inventory, dict):
                        inventories.append(inventory)
            continue
        if receipt.operation != OPERATION_RECOVERY or data.get("target") != dict(
            target
        ):
            continue
        if data.get("schema_version") != RECOVERY_VERSION:
            raise BackupVerificationError("unsupported recovery floor receipt")
        operation_id = str(data["operation_id"])
        if data["phase"] == "intent":
            if operation_id in intent_hashes:
                raise BackupVerificationError("duplicate recovery intent")
            pending[operation_id] = receipt.receipt_id
            intent_hashes[operation_id] = receipt.checksum()
        elif data["phase"] in {
            "committed",
            "aborted",
            "reconciled",
            "reconciled_observed",
        }:
            if operation_id not in pending:
                raise BackupVerificationError("recovery outcome has no matching intent")
            if data.get("intent_sha256") != intent_hashes[operation_id]:
                raise BackupVerificationError(
                    "recovery outcome has a mismatched intent"
                )
            pending.pop(operation_id)
        elif data["phase"] in {"ambiguous", "observed"}:
            if (
                operation_id not in pending
                or data.get("intent_sha256") != intent_hashes[operation_id]
            ):
                raise BackupVerificationError(
                    "recovery observation has no matching intent"
                )
        else:
            raise BackupVerificationError("unknown recovery operation phase")
        floors.extend(data.get("floors", []))
        inventories.extend(data.get("inventories", []))
    if require_terminal and pending:
        raise BackupVerificationError(
            "unresolved recovery requires reconciliation before retry",
            detail={"pending": pending},
        )
    return {"floors": floors, "inventories": inventories, "pending": pending}


def source_recovery_state(
    directory: str | Path, *, target: Mapping[str, Any]
) -> dict[str, Any]:
    """Bind the explicitly named source journal, separate from a replacement's.

    An absent journal is not equivalent to an intentionally empty journal.
    Pending source operations must be reconciled from fenced evidence first.
    """
    from daylily_tapdb.identity_inventory import content_hash

    path = Path(directory)
    if not path.is_absolute() or not path.is_dir():
        raise BackupVerificationError(
            "source_receipts_dir must name an existing absolute source journal directory"
        )
    state = retained_recovery_state(path, target=target)
    return {
        "source_receipts_dir": str(path),
        "state_sha256": content_hash(state),
        **state,
    }


def _append(directory: Path, *, actor: Actor, detail: dict[str, Any]) -> Receipt:
    _history(directory)
    receipt = write_receipt(
        directory,
        operation=OPERATION_RECOVERY,
        status=detail["phase"],
        actor=actor,
        detail=detail,
    )
    _history(directory)
    return receipt


def begin_recovery(
    directory: Path,
    *,
    target: Mapping[str, Any],
    inventories: list[dict[str, Any]],
    evidence: Mapping[str, Any],
    actor: Actor,
    floors: list[dict[str, Any]] | None = None,
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist intent and every known floor before a database write."""
    from daylily_tapdb.identity_inventory import validate_target

    target = validate_target(target, str(target["schema_name"]))
    retained = retained_recovery_state(directory, target=target)
    family = (
        validate_recovery_family(recovery_family, required_directory=directory)
        if recovery_family is not None
        else None
    )
    if evidence.get("purpose") == "fenced_source_recovery" and family is None:
        raise BackupVerificationError(
            "source recovery requires an explicit recovery family"
        )
    if family is not None:
        family_state = recovery_family_state(family)
        retained["floors"] += family_state["floors"]
        retained["inventories"] += family_state["inventories"]
        for inventory in inventories:
            require_family_member(family, inventory)
    if evidence.get("purpose") not in {
        "fenced_source_recovery",
        "isolated_rehearsal",
        "fenced_migration",
    }:
        raise BackupVerificationError(
            "recovery needs final fenced source evidence; an old dump alone is insufficient"
        )
    if not inventories:
        raise BackupVerificationError(
            "recovery requires complete generator inventories"
        )
    combined = list(retained["floors"]) + list(floors or [])
    for index, inventory in enumerate(inventories):
        combined.extend(
            inventory_floors(inventory, source=f"capture:{index}:{inventory['sha256']}")
        )
    detail: dict[str, Any] = {
        "schema_version": RECOVERY_VERSION,
        "operation_id": str(uuid4()),
        "phase": "intent",
        "target": dict(target),
        "evidence": dict(evidence),
        "floors": combined,
        "inventories": retained["inventories"] + inventories,
    }
    if family is not None:
        detail["recovery_family"] = family
    receipt = _append(Path(directory), actor=actor, detail=detail)
    return {
        **detail,
        "intent_receipt_id": receipt.receipt_id,
        "intent_sha256": receipt.checksum(),
    }


def observe_recovery(
    connection: Any,
    directory: Path,
    intent: Mapping[str, Any],
    *,
    actor: Actor,
    inventory: dict[str, Any] | None = None,
) -> Receipt:
    """Durably bind the live target OID before import or allocator mutation.

    An empty newly provisioned database can be observed without an inventory;
    finalization records its complete post-import generator inventory separately.
    """
    from daylily_tapdb.identity_inventory import physical_target

    state = retained_recovery_state(
        directory, target=intent["target"], require_terminal=False
    )
    saved = next(
        (
            row
            for row in _history(directory)
            if row.receipt_id == state["pending"].get(intent["operation_id"])
        ),
        None,
    )
    if saved is None or saved.checksum() != intent.get("intent_sha256"):
        raise BackupVerificationError(
            "physical observation requires the exact pending recovery intent"
        )
    physical = physical_target(connection, intent["target"])
    detail: dict[str, Any] = {
        "schema_version": RECOVERY_VERSION,
        "operation_id": intent["operation_id"],
        "phase": "observed",
        "target": dict(intent["target"]),
        "physical_target": physical,
        "intent_sha256": intent["intent_sha256"],
        "floors": [],
        "inventories": [],
    }
    if inventory is not None:
        if _member_identity(inventory) != _member_identity(detail):
            raise BackupVerificationError(
                "observed inventory does not match the live target"
            )
        detail["floors"] = inventory_floors(
            inventory, source=f"observed:{inventory['sha256']}"
        )
        detail["inventories"] = [inventory]
    family = intent.get("recovery_family")
    if family is not None:
        detail["recovery_family"] = validate_recovery_family(
            family, required_directory=directory
        )
    return _append(directory, actor=actor, detail=detail)


def finish_recovery(
    directory: Path,
    intent: Mapping[str, Any],
    *,
    phase: str,
    actor: Actor,
    inventory: dict[str, Any] | None = None,
    reservations: list[dict[str, Any]] | None = None,
    error: str | None = None,
) -> Receipt:
    if phase not in {"committed", "aborted", "ambiguous", "reconciled"}:
        raise BackupVerificationError("invalid recovery outcome phase")
    state = retained_recovery_state(
        directory, target=intent["target"], require_terminal=False
    )
    if intent["operation_id"] not in state["pending"]:
        raise BackupVerificationError("recovery intent is not pending")
    saved = next(
        (
            receipt
            for receipt in _history(directory)
            if receipt.receipt_id == state["pending"][intent["operation_id"]]
        ),
        None,
    )
    if saved is None or saved.checksum() != intent.get("intent_sha256"):
        raise BackupVerificationError(
            "recovery intent does not match its durable receipt"
        )
    if phase in {"committed", "reconciled"} and inventory is None:
        raise BackupVerificationError(
            "committed recovery requires observed generator inventory"
        )
    floors = list(reservations or [])
    if inventory is not None:
        floors.extend(
            inventory_floors(inventory, source=f"{phase}:{inventory['sha256']}")
        )
    detail: dict[str, Any] = {
        "schema_version": RECOVERY_VERSION,
        "operation_id": intent["operation_id"],
        "phase": phase,
        "target": dict(intent["target"]),
        "intent_sha256": intent["intent_sha256"],
        "floors": floors,
        "inventories": [inventory] if inventory else [],
        "error": error,
    }
    family = intent.get("recovery_family")
    if family is not None:
        detail["recovery_family"] = validate_recovery_family(
            family, required_directory=directory
        )
        if inventory is not None:
            require_family_member(family, inventory)
    return _append(Path(directory), actor=actor, detail=detail)


def reconcile_recovery(
    connection: Any,
    *,
    directory: Path,
    operation_id: str,
    writer_fence: Mapping[str, Any],
    actor: Actor,
) -> Receipt:
    """Reconcile an interrupted operation from current fenced database evidence.

    Unsafe current floors require an explicit shared sequence advance before
    this read-only database check can terminalize the external intent.
    """
    from daylily_tapdb.sequences import (
        capture_sequence_inventory,
        validate_writer_fence,
        verify_sequence_floors,
    )

    saved = next(
        (
            receipt
            for receipt in _history(directory)
            if receipt.operation == OPERATION_RECOVERY
            and receipt.detail.get("operation_id") == operation_id
            and receipt.detail.get("phase") == "intent"
        ),
        None,
    )
    if saved is None:
        raise BackupVerificationError("recovery intent was not found")
    intent = dict(saved.detail, intent_sha256=saved.checksum())
    retained = retained_recovery_state(
        directory, target=intent["target"], require_terminal=False
    )
    mappings = intent["inventories"][-1]["sequence_mappings"]
    target = dict(intent["target"], sequence_mappings=mappings)
    current = capture_sequence_inventory(
        connection, schema_name=target["schema_name"], target=target
    )
    validate_writer_fence(connection, current, writer_fence)
    require_retained_definitions(current, retained["inventories"])
    if not verify_sequence_floors(current, floors=retained["floors"])["ok"]:
        raise BackupVerificationError(
            "current generators have not passed every retained recovery floor"
        )
    return finish_recovery(
        directory, intent, phase="reconciled", actor=actor, inventory=current
    )


def reconcile_recovery_observed(
    connection: Any,
    *,
    directory: Path,
    operation_id: str,
    quarantine_proof: Mapping[str, Any],
    actor: Actor,
) -> Receipt:
    """Resolve observation ambiguity, not the lost transaction's outcome.

    Only A's fresh, same-session quarantine verification can authorize this
    event. Old reservations remain applicable to the next reviewed advance;
    this does not claim that a commit occurred or that issuance is safe now.
    """
    from daylily_tapdb.identity_inventory import physical_target, validate_receipt
    from daylily_tapdb.sequence_fence import backend_identity
    from daylily_tapdb.sequences import capture_sequence_inventory

    validate_receipt(quarantine_proof, "tapdb-writer-quarantine-verification/v1")
    if (
        quarantine_proof.get("ok") is not True
        or quarantine_proof.get("source_receipts_dir") != str(directory)
        or backend_identity(connection) != quarantine_proof.get("connection_backend")
        or physical_target(connection, quarantine_proof["target"])
        != quarantine_proof.get("physical_target")
    ):
        raise BackupVerificationError(
            "recovery observation requires a fresh same-session quarantine proof"
        )
    history = _history(directory)
    quarantine = next(
        (
            row.detail["receipt"]
            for row in history
            if row.operation == "sequence_writer_fence"
            and row.detail.get("receipt", {}).get("sha256")
            == quarantine_proof.get("quarantine_sha256")
        ),
        None,
    )
    if quarantine is None or quarantine.get("phase") != "quarantined":
        raise BackupVerificationError(
            "quarantine proof has no matching durable successful receipt"
        )
    state = retained_recovery_state(
        directory, target=quarantine_proof["target"], require_terminal=False
    )
    saved = next(
        (
            row
            for row in history
            if row.receipt_id == state["pending"].get(operation_id)
        ),
        None,
    )
    if saved is None or saved.detail.get("recovery_family") != quarantine_proof.get(
        "recovery_family"
    ):
        raise BackupVerificationError(
            "quarantine does not match the pending recovery lineage"
        )
    target = dict(
        saved.detail["target"], sequence_mappings=quarantine["sequence_mappings"]
    )
    current = capture_sequence_inventory(
        connection, schema_name=target["schema_name"], target=target
    )
    require_retained_definitions(current, state["inventories"])
    intent = dict(saved.detail, intent_sha256=saved.checksum())
    observe_recovery(connection, directory, intent, actor=actor, inventory=current)
    detail = {
        "schema_version": RECOVERY_VERSION,
        "operation_id": operation_id,
        "phase": "reconciled_observed",
        "target": saved.detail["target"],
        "physical_target": current["physical_target"],
        "intent_sha256": saved.checksum(),
        "floors": state["floors"]
        + inventory_floors(current, source="quarantine_observation"),
        "inventories": [current],
        "prior_transaction_outcome": "unknown",
        "quarantine_verification": dict(quarantine_proof),
    }
    if saved.detail.get("recovery_family") is not None:
        detail["recovery_family"] = saved.detail["recovery_family"]
    return _append(directory, actor=actor, detail=detail)


def require_retained_definitions(
    current: Mapping[str, Any], inventories: list[dict[str, Any]]
) -> None:
    """Missing post-backup generators block; definitions are never guessed."""
    from daylily_tapdb.sequences import sequence_definition

    present = {item["name"]: item for item in current["sequences"]}
    for inventory in inventories:
        for old in inventory["sequences"]:
            new = present.get(old["name"])
            if new is None:
                raise BackupVerificationError(
                    f"retained generator is missing: {old['name']}; verified recreation is required"
                )
            if sequence_definition(old) != sequence_definition(new):
                raise BackupVerificationError(
                    f"retained generator definition changed: {old['name']}"
                )


__all__ = [
    "build_recovery_family",
    "validate_recovery_family",
    "recovery_family_state",
    "require_family_member",
    "begin_recovery",
    "observe_recovery",
    "finish_recovery",
    "inventory_floors",
    "retained_recovery_state",
    "source_recovery_state",
    "reconcile_recovery_observed",
    "require_retained_definitions",
]
