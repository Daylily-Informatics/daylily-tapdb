"""Version labels never waive physical source or recovery-family evidence."""

from copy import deepcopy

import pytest

from daylily_tapdb.backup.errors import BackupVerificationError
from daylily_tapdb.backup.recovery import build_recovery_family
from daylily_tapdb.backup.source_contract import (
    capture_source_contract,
    contract_hash,
    source_contract_descriptor,
    validate_source_contract,
    verify_source_contract,
)
from daylily_tapdb.identity_inventory import seal_receipt
from tests.test_identity_inventory import snapshot
from tests.test_recovery_floor import TARGET, inventory


@pytest.fixture
def captures(monkeypatch):
    sequence = inventory()
    identity = seal_receipt(
        {
            **snapshot(),
            "schema_name": TARGET["schema_name"],
            "target": TARGET,
            "physical_target": dict(sequence["physical_target"]),
        }
    )
    monkeypatch.setattr(
        "daylily_tapdb.identity_inventory.capture_identity_inventory",
        lambda *_a, **_k: identity,
    )
    monkeypatch.setattr(
        "daylily_tapdb.sequences.capture_sequence_inventory", lambda *_a, **_k: sequence
    )
    return identity, sequence


def _capture():
    return capture_source_contract(
        object(),
        schema_name=TARGET["schema_name"],
        target=TARGET,
        source_version="9.0.9",
    )


def test_source_contract_binds_inventories_without_raw_rows_in_descriptor(captures):
    result = _capture()
    validate_source_contract(result)
    verify_source_contract(result, deepcopy(result))
    assert result["source_version_evidence"] == "operator_declared"
    descriptor = source_contract_descriptor(result)
    assert descriptor["identity_inventory_sha256"] == captures[0]["sha256"]
    assert descriptor["sequence_inventory_sha256"] == captures[1]["sha256"]
    assert descriptor["identity_asset"] == "identity-inventory.json"
    assert "identity_inventory" not in descriptor
    assert "sequence_inventory" not in descriptor


@pytest.mark.parametrize("version", ["", "latest", "9", "9.0", "Bloom9.0.0", "9.0.9 "])
def test_source_version_is_exact_and_never_inferred(version):
    with pytest.raises(BackupVerificationError, match="explicit exact version"):
        capture_source_contract(
            object(),
            schema_name=TARGET["schema_name"],
            target=TARGET,
            source_version=version,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "format",
        "checksum",
        "provenance",
        "version",
        "identity",
        "sequence",
        "target",
        "physical",
        "behind",
    ],
)
def test_source_contract_rejects_invalid_evidence(captures, mutation):
    result = deepcopy(_capture())
    if mutation == "format":
        result["schema_version"] = "unsupported"
    elif mutation == "checksum":
        result["sha256"] = "changed"
    elif mutation == "provenance":
        result["source_version_evidence"] = "inferred_from_application"
    elif mutation == "version":
        result["source_version"] = "unknown"
    elif mutation == "identity":
        result["identity_inventory"] = None
    elif mutation == "sequence":
        result["sequence_inventory"] = None
    elif mutation == "target":
        result["identity_inventory"]["target"] = dict(
            TARGET, config_identity="/different/explicit.yaml"
        )
        result["identity_inventory"] = seal_receipt(result["identity_inventory"])
    elif mutation == "physical":
        result["identity_inventory"]["physical_target"]["database_oid"] += 1
        result["identity_inventory"] = seal_receipt(result["identity_inventory"])
    else:
        result["sequence_inventory"]["sequences"][0]["assigned_floor"] = 50
        result["sequence_inventory"] = seal_receipt(result["sequence_inventory"])
    if mutation != "checksum":
        result["sha256"] = contract_hash(result)
    with pytest.raises(BackupVerificationError):
        validate_source_contract(result)


def test_shared_identity_verification_failure_is_blocking(captures, monkeypatch):
    result = _capture()
    monkeypatch.setattr(
        "daylily_tapdb.identity_inventory.verify_identity_inventory",
        lambda *_a: {"ok": False},
    )
    with pytest.raises(BackupVerificationError, match="does not verify"):
        validate_source_contract(result)


def test_changed_valid_source_contract_requires_fresh_review(captures):
    expected = _capture()
    actual = dict(expected, source_version="9.1.0")
    actual["sha256"] = contract_hash(actual)
    with pytest.raises(BackupVerificationError, match="no longer matches"):
        verify_source_contract(expected, actual)


def test_source_family_is_preserved_in_descriptor(captures, tmp_path):
    family = build_recovery_family(
        family_id="aaaaaaaa-2222-4333-8444-555555555555",
        origin_inventory=captures[1],
        receipts_dirs=[tmp_path],
    )
    result = capture_source_contract(
        object(),
        schema_name=TARGET["schema_name"],
        target=TARGET,
        source_version="9.0.9",
        recovery_family=family,
    )
    validate_source_contract(result)
    assert source_contract_descriptor(result)["recovery_family"] == family
