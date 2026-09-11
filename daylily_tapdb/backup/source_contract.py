"""Explicit, read-only historical source contracts for backup and migration.

The declared version describes the source; it is never inferred from an
application version or used to waive a physical inventory mismatch.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping

from daylily_tapdb.backup.errors import BackupVerificationError

SOURCE_CONTRACT_VERSION = "tapdb-source-contract/v1"
IDENTITY_ASSET = "identity-inventory.json"


def contract_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            {key: value for key, value in payload.items() if key != "sha256"},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def capture_source_contract(
    connection: Any,
    *,
    schema_name: str,
    target: Mapping[str, Any],
    source_version: str,
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Capture every physical table and generator without initializing runtime."""
    from daylily_tapdb.identity_inventory import capture_identity_inventory
    from daylily_tapdb.sequences import capture_sequence_inventory

    if not re.fullmatch(r"\d+\.\d+\.\d+(?:[.-][A-Za-z0-9.-]+)?", source_version):
        raise BackupVerificationError(
            "source_version must be an explicit exact version"
        )
    payload: dict[str, Any] = {
        "schema_version": SOURCE_CONTRACT_VERSION,
        "source_version": source_version,
        "source_version_evidence": "operator_declared",
        "identity_inventory": capture_identity_inventory(
            connection, schema_name=schema_name, target=target
        ),
        "sequence_inventory": capture_sequence_inventory(
            connection, schema_name=schema_name, target=target
        ),
    }
    if recovery_family is not None:
        from daylily_tapdb.backup.recovery import (
            require_family_member,
            validate_recovery_family,
        )

        family = validate_recovery_family(recovery_family)
        require_family_member(family, payload["sequence_inventory"])
        payload["recovery_family"] = family
    payload["sha256"] = contract_hash(payload)
    return payload


def validate_source_contract(contract: Mapping[str, Any]) -> None:
    from daylily_tapdb.identity_inventory import verify_identity_inventory
    from daylily_tapdb.sequences import verify_sequence_floors

    if contract.get("schema_version") != SOURCE_CONTRACT_VERSION:
        raise BackupVerificationError(
            "unsupported or missing historical source contract"
        )
    if contract.get("sha256") != contract_hash(contract):
        raise BackupVerificationError("historical source contract checksum mismatch")
    if contract.get("source_version_evidence") != "operator_declared":
        raise BackupVerificationError("source version provenance must be explicit")
    if not re.fullmatch(
        r"\d+\.\d+\.\d+(?:[.-][A-Za-z0-9.-]+)?", str(contract.get("source_version", ""))
    ):
        raise BackupVerificationError("source contract has no explicit exact version")
    identity = contract.get("identity_inventory")
    sequences = contract.get("sequence_inventory")
    if not isinstance(identity, dict) or not isinstance(sequences, dict):
        raise BackupVerificationError("source contract requires complete inventories")
    if not verify_identity_inventory(identity, identity)["ok"]:
        raise BackupVerificationError("source identity inventory does not verify")
    if not verify_sequence_floors(sequences, floors=[])["ok"]:
        raise BackupVerificationError(
            "source generators are behind assigned identities"
        )
    if identity["target"] != sequences["target"]:
        raise BackupVerificationError("source inventories identify different targets")
    if identity["physical_target"] != sequences["physical_target"]:
        raise BackupVerificationError(
            "source inventories identify different physical databases"
        )
    if contract.get("recovery_family") is not None:
        from daylily_tapdb.backup.recovery import require_family_member

        require_family_member(contract["recovery_family"], sequences)


def verify_source_contract(
    expected: Mapping[str, Any], actual: Mapping[str, Any]
) -> None:
    """Reject stale source contracts, including changed allocator positions."""
    validate_source_contract(expected)
    validate_source_contract(actual)
    if expected != actual:
        raise BackupVerificationError(
            "live source no longer matches its reviewed contract"
        )


def source_contract_descriptor(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Manifest-safe references; content hashes live in a separately verified asset."""
    validate_source_contract(contract)
    descriptor = {
        "schema_version": SOURCE_CONTRACT_VERSION,
        "source_version": contract["source_version"],
        "source_version_evidence": contract["source_version_evidence"],
        "sha256": contract["sha256"],
        "identity_inventory_sha256": contract["identity_inventory"]["sha256"],
        "sequence_inventory_sha256": contract["sequence_inventory"]["sha256"],
        "identity_asset": IDENTITY_ASSET,
    }
    if contract.get("recovery_family") is not None:
        descriptor["recovery_family"] = contract["recovery_family"]
    return descriptor
