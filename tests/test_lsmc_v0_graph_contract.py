from __future__ import annotations

import pytest

from daylily_tapdb.graph_contracts import (
    attach_v0_edge_metadata,
    build_v0_edge_metadata,
    canonical_edge_type,
    describe_lineage_contract,
    is_strict_canonical_edge_type,
    metadata_location_label,
    v0_edge_metadata_from_lineage,
)
from daylily_tapdb.graph_contracts.lsmc_v0 import v0_edge_metadata_from_json_addl


class _Lineage:
    def __init__(self, *, relationship_type="contains", json_addl=None):
        self.relationship_type = relationship_type
        self.json_addl = json_addl or {}


def test_build_v0_edge_metadata_requires_evidence() -> None:
    with pytest.raises(ValueError, match="evidence_refs"):
        build_v0_edge_metadata(
            edge_type="HOLDS_MATERIAL",
            source_euid="Z-BCT-1",
            target_euid="Z-BNG-1",
            asserted_by_system="bloom",
            evidence_refs=[],
            correlation_id="corr-1",
            causation_id="cause-1",
        )


def test_build_v0_edge_metadata_normalizes_legacy_evidence_refs() -> None:
    metadata = build_v0_edge_metadata(
        edge_type="HOLDS_MATERIAL",
        source_euid="Z-BCT-1",
        target_euid="Z-BNG-1",
        asserted_by_system="bloom",
        evidence_refs=["Z-EVD-1"],
        correlation_id="corr-1",
        causation_id="cause-1",
    )

    assert metadata["contract"] == "LSMC_V0"
    assert metadata["edge_type"] == "HOLDS_MATERIAL"
    assert metadata["evidence_refs"] == [{"euid": "Z-EVD-1"}]
    assert metadata_location_label() == "json_addl.properties.v0_edge"
    assert is_strict_canonical_edge_type("HOLDS_MATERIAL")
    assert not is_strict_canonical_edge_type("holds_material")


def test_describe_lineage_contract_marks_legacy_aliases() -> None:
    payload = describe_lineage_contract(_Lineage(relationship_type="order_patient"))

    assert payload["compliance_status"] == "legacy_alias"
    assert payload["edge_type"] == "TEST_HAS_SUBJECT"
    assert canonical_edge_type("fulfillment_item") == "HAS_SLOT"


def test_describe_lineage_contract_marks_canonical_metadata() -> None:
    lineage = _Lineage(
        relationship_type="HOLDS_MATERIAL",
        json_addl={
            "properties": {
                "v0_edge": {
                    "edge_type": "HOLDS_MATERIAL",
                    "source_euid": "Z-BCT-1",
                    "target_euid": "Z-BNG-1",
                    "asserted_by_system": "bloom",
                    "evidence_refs": [{"euid": "Z-EVD-1"}],
                    "correlation_id": "corr-1",
                    "causation_id": "cause-1",
                    "edge_state": "active",
                }
            }
        },
    )

    payload = describe_lineage_contract(lineage)

    assert payload["compliance_status"] == "canonical"
    assert payload["source_euid"] == "Z-BCT-1"
    assert payload["target_euid"] == "Z-BNG-1"


def test_build_v0_edge_metadata_rejects_invalid_required_fields() -> None:
    base = {
        "edge_type": "HOLDS_MATERIAL",
        "source_euid": "Z-BCT-1",
        "target_euid": "Z-BNG-1",
        "asserted_by_system": "bloom",
        "evidence_refs": [{"euid": "Z-EVD-1"}],
        "correlation_id": "corr-1",
        "causation_id": "cause-1",
    }

    with pytest.raises(ValueError, match="Unsupported LSMC v0 edge type"):
        build_v0_edge_metadata(**{**base, "edge_type": "not-a-contract-edge"})
    with pytest.raises(ValueError, match="source_euid is required"):
        build_v0_edge_metadata(**{**base, "source_euid": " "})
    with pytest.raises(ValueError, match=r"evidence_refs\[0\] is empty"):
        build_v0_edge_metadata(**{**base, "evidence_refs": [" "]})
    with pytest.raises(ValueError, match=r"evidence_refs\[0\]\.euid is required"):
        build_v0_edge_metadata(**{**base, "evidence_refs": [{}]})
    with pytest.raises(ValueError, match=r"evidence_refs\[0\] must be"):
        build_v0_edge_metadata(**{**base, "evidence_refs": [123]})


def test_attach_and_describe_v0_metadata_edge_cases() -> None:
    lineage = _Lineage(relationship_type="unknown", json_addl=None)
    metadata = attach_v0_edge_metadata(
        lineage,
        {
            "edge_type": "contains",
            "source_euid": "Z-BCT-1",
            "target_euid": "Z-BNG-1",
            "asserted_by_system": "bloom",
            "evidence_refs": [{"root_euid": "Z-EVD-1", "kind": "source"}],
            "correlation_id": "corr-1",
            "causation_id": "cause-1",
            "valid_from": "2026-06-12T00:00:00+00:00",
            "valid_to": "2026-06-13T00:00:00+00:00",
        },
    )

    assert metadata["edge_type"] == "CONTAINS"
    assert metadata["evidence_refs"] == [{"root_euid": "Z-EVD-1", "kind": "source", "euid": "Z-EVD-1"}]
    assert v0_edge_metadata_from_lineage(lineage)["edge_type"] == "CONTAINS"
    assert v0_edge_metadata_from_json_addl([]) is None
    assert v0_edge_metadata_from_json_addl({"properties": []}) is None
    assert v0_edge_metadata_from_json_addl({"properties": {"v0_edge": []}}) is None

    invalid = _Lineage(
        json_addl={
            "properties": {
                "v0_edge": {
                    "edge_type": "BAD",
                    "source_euid": "Z-BCT-1",
                    "target_euid": "Z-BNG-1",
                    "asserted_by_system": "bloom",
                    "evidence_refs": [{"euid": "Z-EVD-1"}],
                    "correlation_id": "corr-1",
                    "causation_id": "cause-1",
                }
            }
        }
    )
    assert describe_lineage_contract(invalid)["compliance_status"] == "invalid"
    assert describe_lineage_contract(_Lineage(relationship_type="unmapped"))["compliance_status"] == "generic"
    assert canonical_edge_type(None) is None
