"""Native archived-and-retained disposition; the closed writer gate stays closed."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from daylily_tapdb.backup.receipts import OPERATION_CREATE, read_receipts
from daylily_tapdb.backup.recovery import (
    inventory_floors, recovery_family_state, require_family_member, validate_recovery_family,
)
from daylily_tapdb.backup.source_contract import validate_source_contract
from daylily_tapdb.identity_inventory import content_hash, seal_receipt, validate_receipt, validate_target
from daylily_tapdb.sequence_fence import (
    FENCE_VERSION, TAKEOVER_VERSION, active_epoch, journal, read_fence_history, retained_floors,
)

VERSION = "tapdb-retained-archive/v1"
RESTORE_VERSION = "tapdb-archive-restore-plan/v1"


def _latest_fence(receipt: Mapping[str, Any], directory: Path) -> list[Any]:
    if receipt.get("phase") == "acquired":
        validate_receipt(receipt, FENCE_VERSION)
    elif receipt.get("phase") == "quarantined":
        validate_receipt(receipt, TAKEOVER_VERSION)
    else:
        raise ValueError("Archive evidence requires a native acquired or quarantined fence")
    history, _head = read_fence_history(directory)
    epoch = active_epoch(history, receipt["target"])
    if epoch is None or epoch.receipt_id != receipt["intent_receipt_id"]:
        raise ValueError("Fence was released or superseded; reconcile through native sequences controls")
    if not any(item.detail.get("receipt") == dict(receipt) and item.succeeded for item in history):
        raise ValueError("Fence must be the native successful result in its original journal")
    return history


def retain_archive(
    *, target: Mapping[str, Any], fence: Mapping[str, Any], source_contract: Mapping[str, Any],
    backup_result: Mapping[str, Any], receipts_dir: Path, backup_receipts_dir: Path,
) -> dict[str, Any]:
    """Record retained evidence without deleting data, releasing ACLs or probing backup bytes."""
    if fence.get("phase") != "acquired":
        raise ValueError("Archival requires the native closed database-connections gate")
    target = validate_target(target, str(target["schema_name"]))
    history = _latest_fence(fence, receipts_dir)
    validate_source_contract(source_contract)
    inventory = source_contract["sequence_inventory"]
    if (fence["target"] != dict(target) or inventory["target"] != dict(target)
        or fence["physical_target"] != inventory["physical_target"]
        or fence["inventory_sha256"] != inventory["sha256"]):
        raise ValueError("Archive source, allocation inventory and closed fence must identify the same database state")
    if source_contract.get("recovery_family") is None:
        raise ValueError("Archival requires an explicit recovery family")
    family = validate_recovery_family(source_contract["recovery_family"], required_directory=receipts_dir)
    if fence.get("recovery_family") != family:
        raise ValueError("Archive requires the fence's explicit recovery family")
    if backup_result.get("dry_run") is not False:
        raise ValueError("A completed native backup is required")
    successful = next((item for item in read_receipts(backup_receipts_dir)
                       if item.receipt_id == backup_result.get("receipt_id") and item.succeeded
                       and item.operation == OPERATION_CREATE and item.backup_class == "full"
                       and item.backup_id == backup_result.get("backup_id")), None)
    if successful is None:
        raise ValueError("Backup must have its native successful create receipt")
    manifest = backup_result.get("manifest", {})
    if manifest.get("source_contract", {}).get("sha256") != source_contract["sha256"]:
        raise ValueError("Backup source contract differs from the retained source")
    family_state = recovery_family_state(family)
    floors = retained_floors(history, target) + family_state["floors"] + inventory_floors(inventory, source="retained_archive")
    receipt = seal_receipt({
        "schema_version": VERSION, "phase": "archived_retained", "target": dict(target),
        "physical_target": inventory["physical_target"], "recovery_family": family,
        "source_contract_sha256": source_contract["sha256"],
        "sequence_inventory": inventory, "retained_floors": floors,
        "fence": dict(fence), "source_receipts_dir": str(receipts_dir),
        "backup_receipts_dir": str(backup_receipts_dir), "backup_receipt_id": successful.receipt_id,
        "backup_id": successful.backup_id, "backup_storage_prefix": backup_result["storage_prefix"],
        "restoration": "native_fence_reconciliation_and_all_generation_high_water",
    })
    journal(receipts_dir, phase="archived_retained", actor=str(fence["operator_role"]),
            detail={"target": dict(target), "receipt": receipt})
    return receipt


def archive_restore_plan(
    archive: Mapping[str, Any], *, generation_quarantines: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Union all known generations using latest native quarantine outcomes.

    This is a plan, never permission to open the old gate. Native takeover,
    recovery and sequence advance remain the apply/reconciliation authorities.
    """
    validate_receipt(archive, VERSION)
    directory = Path(archive["source_receipts_dir"])
    history, _head = read_fence_history(directory)
    if not any(item.detail.get("receipt") == dict(archive) for item in history):
        raise ValueError("Archive must be present in its original native journal")
    family = validate_recovery_family(archive["recovery_family"], required_directory=directory)
    state = recovery_family_state(family)
    covered: set[str] = set()
    floors = list(archive["retained_floors"]) + state["floors"]
    inventories = [archive["sequence_inventory"], *state["inventories"]]
    quarantines = []
    for fence in generation_quarantines:
        if fence.get("phase") != "quarantined" or fence.get("recovery_family") != family:
            raise ValueError("Each later generation requires its native family-bound quarantine result")
        _latest_fence(fence, Path(fence["source_receipts_dir"]))
        inventory = fence["sequence_inventory"]
        require_family_member(family, inventory)
        covered.add(content_hash({"target": inventory["target"], "physical_target": inventory["physical_target"]}))
        floors += inventory_floors(inventory, source="archive_restore_generation")
        floors += fence["retained_floors"]
        inventories.append(inventory)
        quarantines.append(fence["sha256"])
    if any(content_hash(member) not in covered for member in state["members"]):
        raise ValueError("Every physical generation, including the archived database, needs an explicit current native quarantine receipt")
    unique = {(item["name"], item["value"], item["source"]): item for item in floors}
    return seal_receipt({
        "schema_version": RESTORE_VERSION, "phase": "planned", "archive_sha256": archive["sha256"],
        "target": archive["target"], "physical_target": archive["physical_target"],
        "recovery_family": family, "family_state_sha256": content_hash(state),
        "floors": [unique[key] for key in sorted(unique)], "inventories": inventories,
        "generation_quarantines": quarantines,
        "required_apply": "native sequences reconcile, reviewed recovery/advance, then runtime binding",
        "opens_gate": False,
    })
