"""Receipt and lifecycle refusals are enforced before any gate mutation."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from daylily_tapdb import identity_inventory as identity
from daylily_tapdb import sequence_fence as fence
from daylily_tapdb.identity_inventory import seal_receipt
from daylily_tapdb.sequences import SequenceProtectionError
from tests.test_sequence_protection import inventory
from tests.test_sequence_protection_fence import Result


def test_active_control_transactions_and_unknown_intents_cannot_reconcile(tmp_path):
    active = SimpleNamespace(in_transaction=lambda: True)
    for function, options in (
        (
            fence.build_writer_fence_takeover_plan,
            {"target": inventory()["target"], "fence_intent_receipt_id": "unknown"},
        ),
        (
            fence.reconcile_writer_fence_release,
            {"release_intent_receipt_id": "unknown"},
        ),
    ):
        with pytest.raises(SequenceProtectionError, match="transaction-free"):
            function(active, receipts_dir=tmp_path, **options)
        with pytest.raises(SequenceProtectionError, match="intent"):
            function(
                SimpleNamespace(in_transaction=lambda: False),
                receipts_dir=tmp_path,
                **options,
            )


def test_takeover_receipt_wrong_phase_never_opens_control_or_target(tmp_path):
    receipt = seal_receipt(
        {"schema_version": fence.TAKEOVER_VERSION, "phase": "quarantined"}
    )
    with pytest.raises(SequenceProtectionError, match="unchanged reviewed"):
        fence.apply_writer_fence_takeover(
            object(),
            receipt,
            receipts_dir=tmp_path,
            target_connection_factory=lambda: pytest.fail(
                "unreviewed target connection"
            ),
        )


@pytest.mark.parametrize(
    "change", ["active_control", "phase", "directory", "family", "physical", "operator"]
)
def test_quarantine_receipt_mismatch_is_rejected_before_control_queries(
    monkeypatch, tmp_path, change
):
    current = inventory()
    receipt = {
        "schema_version": fence.TAKEOVER_VERSION,
        "phase": "quarantined",
        "target": current["target"],
        "physical_target": current["physical_target"],
        "operator_role": "operator",
        "source_receipts_dir": str(tmp_path),
    }
    if change == "phase":
        receipt["phase"] = "planned"
    elif change == "directory":
        receipt["source_receipts_dir"] = str(tmp_path / "different-journal")
    elif change == "family":
        receipt["recovery_family"] = {"different": "unapproved lineage"}
    elif change == "physical":
        receipt["physical_target"] = {**current["physical_target"], "database_oid": 999}
    elif change == "operator":
        receipt["operator_role"] = "different-operator"
    monkeypatch.setattr(identity, "require_snapshot", lambda connection: None)
    monkeypatch.setattr(
        identity, "physical_target", lambda *a: current["physical_target"]
    )
    connection = SimpleNamespace(execute=lambda *a: Result("operator"))
    control = SimpleNamespace(in_transaction=lambda: change == "active_control")
    with pytest.raises(SequenceProtectionError):
        fence.validate_writer_quarantine(
            connection,
            control_connection=control,
            quarantine_receipt=seal_receipt(receipt),
            receipts_dir=tmp_path,
        )


@pytest.mark.parametrize("change", ["family", "not_committed"])
def test_lost_release_ack_requires_original_family_and_committed_evidence(
    monkeypatch, tmp_path, change
):
    target = inventory()["target"]
    detail = {
        "phase": "release_intent",
        "target": target,
        "committed_result": seal_receipt(
            {
                "schema_version": "tapdb-sequence-apply/v1",
                "phase": "ambiguous",
            }
        ),
    }
    if change == "family":
        detail["recovery_family"] = {"original": "explicit family"}
    row = SimpleNamespace(
        operation="sequence_writer_fence", receipt_id="exact-intent", detail=detail
    )
    monkeypatch.setattr(fence, "read_fence_history", lambda path: ([row], None))
    with pytest.raises(SequenceProtectionError, match="family|committed"):
        fence.reconcile_writer_fence_release(
            SimpleNamespace(in_transaction=lambda: False),
            receipts_dir=tmp_path,
            release_intent_receipt_id="exact-intent",
        )


def test_acl_readback_cannot_waive_mutation_of_nonconnect_privileges(monkeypatch):
    before = {"entries": [{"privilege_type": "TEMPORARY", "grantor": 1}]}
    after = {"entries": []}
    snapshots = iter([before, after])
    monkeypatch.setattr(fence, "acl_snapshot", lambda *a: next(snapshots))
    monkeypatch.setattr(fence, "_set_connect_acl", lambda *a: None)
    monkeypatch.setattr(fence, "exclusive_acl", lambda *a, **k: None)
    state = {
        "database_oid": 42,
        "owner_oid": 1,
        "operator_role": "operator",
        "owner_role": "operator",
    }
    with pytest.raises(SequenceProtectionError, match="non-CONNECT"):
        fence.quarantine_acl(object(), state=state, provider={})
    monkeypatch.setattr(fence, "acl_snapshot", lambda *a: after)
    with pytest.raises(SequenceProtectionError, match="restoration"):
        fence.restore_acl(object(), state=state, original_acl=before)
