"""Every family-bearing gate phase proves an already evidenced physical member."""

from __future__ import annotations

from uuid import uuid4

import pytest

from daylily_tapdb.backup.errors import BackupVerificationError
from daylily_tapdb.backup.recovery import (
    build_recovery_family,
    recovery_family_state,
)
from daylily_tapdb.identity_inventory import seal_receipt
from daylily_tapdb.sequence_fence import journal, read_fence_history
from daylily_tapdb.sequences import SequenceProtectionError
from tests.test_sequence_protection import inventory


@pytest.fixture
def family_member(tmp_path):
    current = inventory()
    family = build_recovery_family(
        family_id=str(uuid4()), origin_inventory=current, receipts_dirs=[tmp_path]
    )
    return {
        "target": current["target"],
        "physical_target": current["physical_target"],
        "recovery_family": family,
    }


@pytest.mark.parametrize(
    "phase,nested",
    [
        ("acquire_intent", False),
        ("acquired", True),
        ("takeover_intent", False),
        ("acl_quarantined", False),
        ("quarantine_open_intent", True),
        ("release_intent", False),
        ("released", True),
        ("release_reconciled", True),
    ],
)
def test_every_tagged_phase_is_readable_by_authoritative_family_collector(
    family_member, tmp_path, phase, nested
):
    body = seal_receipt({"schema_version": "tapdb-writer-fence/v1", **family_member})
    detail = (
        {"target": body["target"], "receipt": body} if nested else dict(family_member)
    )
    row = journal(tmp_path, phase=phase, actor="operator", detail=detail)
    assert row.detail["recovery_family"] == family_member["recovery_family"]
    state = recovery_family_state(family_member["recovery_family"])
    assert state["members"] == [
        {key: family_member[key] for key in ("target", "physical_target")}
    ]


@pytest.mark.parametrize("change", ["missing", "other_oid", "other_target"])
def test_unbound_family_gate_event_is_refused_before_journal_write(
    family_member, tmp_path, change
):
    detail = dict(family_member)
    if change == "missing":
        detail.pop("physical_target")
    elif change == "other_oid":
        detail["physical_target"] = {**detail["physical_target"], "database_oid": 999}
    else:
        detail["target"] = {**detail["target"], "config_identity": "/other/config.yaml"}
    with pytest.raises(
        BackupVerificationError, match="physical_target|not an evidenced"
    ):
        journal(tmp_path, phase="released", actor="operator", detail=detail)
    assert read_fence_history(tmp_path) == ([], None)


@pytest.mark.parametrize("field", ["target", "recovery_family"])
def test_envelope_cannot_replace_nested_family_or_target(
    family_member, tmp_path, field
):
    detail = {"target": family_member["target"], "receipt": dict(family_member)}
    detail[field] = {"different": "explicit but conflicting"}
    with pytest.raises(SequenceProtectionError, match="different"):
        journal(tmp_path, phase="released", actor="operator", detail=detail)
    assert read_fence_history(tmp_path) == ([], None)
