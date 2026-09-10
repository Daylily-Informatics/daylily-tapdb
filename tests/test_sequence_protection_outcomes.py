"""An altered plan/floor cannot turn an unknown earlier attempt into a success."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from daylily_tapdb.identity_inventory import seal_receipt
from daylily_tapdb.sequences import (
    SequenceProtectionError,
    _prior_sequence_intent_resolved,
    build_sequence_advance_plan,
)
from tests.test_sequence_protection import inventory


@pytest.fixture
def prior_attempt():
    current = inventory()
    plan = build_sequence_advance_plan(current, floors=[])
    intent = SimpleNamespace(
        operation="sequence_advance",
        receipt_id="exact-intent",
        detail={"phase": "intent", "plan": plan},
    )
    result = {
        "schema_version": "tapdb-sequence-apply/v1",
        "phase": "committed",
        "intent_receipt_id": intent.receipt_id,
        "plan_sha256": plan["sha256"],
        "inventory": current,
    }
    return intent, result


@pytest.mark.parametrize("change", ["phase", "plan", "target", "physical", "family"])
def test_terminal_event_must_bind_exact_original_plan_and_physical_identity(
    prior_attempt, change
):
    intent, result = prior_attempt
    if change == "phase":
        result["phase"] = "rolled_back"
    elif change == "plan":
        result["plan_sha256"] = "different-plan"
    elif change == "family":
        result["recovery_family"] = {"different": "family"}
    else:
        key = "target" if change == "target" else "physical_target"
        result["inventory"] = {**result["inventory"], key: {"different": "database"}}
    row = SimpleNamespace(
        operation="sequence_advance",
        detail={"phase": "committed", "result": seal_receipt(result)},
    )
    with pytest.raises(SequenceProtectionError, match="does not match"):
        _prior_sequence_intent_resolved([intent, row], intent)


def test_ambiguous_unrelated_and_conflicting_outcomes_do_not_resolve(prior_attempt):
    intent, result = prior_attempt
    unrelated = SimpleNamespace(operation="unrelated_operation", detail={})
    other = SimpleNamespace(
        operation="sequence_advance",
        detail={"phase": "committed", "result": {"intent_receipt_id": "other-intent"}},
    )
    ambiguous = SimpleNamespace(
        operation="sequence_advance",
        detail={
            "phase": "ambiguous",
            "result": seal_receipt({**result, "phase": "ambiguous"}),
        },
    )
    assert not _prior_sequence_intent_resolved(
        [intent, unrelated, other, ambiguous], intent
    )
    terminal = [
        SimpleNamespace(
            operation="sequence_advance",
            detail={"phase": phase, "result": seal_receipt({**result, "phase": phase})},
        )
        for phase in ("committed", "rolled_back")
    ]
    with pytest.raises(SequenceProtectionError, match="contradictory"):
        _prior_sequence_intent_resolved([intent, *terminal], intent)


@pytest.mark.parametrize(
    "change",
    ["missing_proof", "claimed_commit", "different_plan", "different_observation"],
)
def test_observed_reconciliation_keeps_unknown_outcome_and_requires_durable_fresh_proof(
    prior_attempt, change
):
    intent, result = prior_attempt
    observed = result["inventory"]
    quarantine = seal_receipt(
        {
            "schema_version": "tapdb-writer-fence-takeover/v1",
            "phase": "quarantined",
            "target": observed["target"],
            "physical_target": observed["physical_target"],
            "sequence_inventory": observed,
        }
    )
    proof = SimpleNamespace(
        operation="sequence_writer_fence",
        detail={"phase": "quarantined", "receipt": quarantine},
    )
    detail = {
        "phase": "reconciled_observed",
        "intent_receipt_id": intent.receipt_id,
        "plan_sha256": result["plan_sha256"],
        "prior_transaction_outcome": "unknown",
        "observed_inventory": observed,
        "reconciliation_receipt_sha256": quarantine["sha256"],
    }
    if change == "claimed_commit":
        detail["prior_transaction_outcome"] = "committed"
    elif change == "different_plan":
        detail["plan_sha256"] = "different-plan"
    elif change == "different_observation":
        detail["observed_inventory"] = {"different": "source snapshot"}
    row = SimpleNamespace(operation="sequence_advance", detail=detail)
    history = [intent, row] if change == "missing_proof" else [intent, proof, row]
    with pytest.raises(SequenceProtectionError, match="observation"):
        _prior_sequence_intent_resolved(history, intent)
