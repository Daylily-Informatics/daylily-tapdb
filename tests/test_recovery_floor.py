"""Recovery history cannot forget reserved identifiers after failed attempts."""

from copy import deepcopy

import pytest

from daylily_tapdb.backup.errors import BackupVerificationError
from daylily_tapdb.backup.manifest import SequenceState
from daylily_tapdb.backup.receipts import Actor, read_receipts, write_receipt
from daylily_tapdb.backup.recovery import (
    begin_recovery,
    build_recovery_family,
    finish_recovery,
    require_retained_definitions,
    retained_recovery_state,
    source_recovery_state,
)
from daylily_tapdb.identity_inventory import seal_receipt

TARGET = {
    "engine_type": "local",
    "host": "localhost",
    "port": 5432,
    "database": "recovery_test",
    "schema_name": "history",
    "config_identity": "/test/tapdb.yaml",
    "domain_code": "Z",
    "owner_repo_name": "daylily-tapdb",
}
ACTOR = Actor(surface="cli", username="test-operator")


def inventory(*, name="history_uid_seq", last=22, called=True):
    return seal_receipt(
        {
            "schema_version": "tapdb-sequence-inventory/v1",
            "schema_name": "history",
            "target": TARGET,
            "physical_target": {
                "database": "recovery_test",
                "database_oid": 123,
                "server_address": "127.0.0.1",
                "server_port": 5432,
            },
            "sequence_mappings": {},
            "missing_generators": [],
            "sequences": [
                {
                    "name": name,
                    "last_value": last,
                    "is_called": called,
                    "increment_by": 7,
                    "min_value": 1,
                    "max_value": 1000,
                    "start_value": 1,
                    "cache_size": 1,
                    "cycle": False,
                    "owner": "test-operator",
                    "dependencies": [],
                    "mapping": {
                        "kind": "owned_column",
                        "columns": [
                            {
                                "schema_name": "history",
                                "table_name": "history",
                                "column_name": "uid",
                                "dependency_type": "i",
                            }
                        ],
                    },
                    "assigned_floor": 15,
                    "allocated_floor": last if called else None,
                }
            ],
        }
    )


@pytest.mark.parametrize(("called", "expected"), [(False, 22), (True, 29)])
def test_complete_manifest_sequence_uses_shared_non_unit_increment(called, expected):
    state = SequenceState.from_payload(inventory(called=called)["sequences"][0])
    assert state.next_value == expected
    assert SequenceState.from_payload(state.to_payload()) == state


def test_old_manifest_sequence_does_not_assume_increment_one():
    assert SequenceState("historical_generator", 100, True).next_value is None


def test_ambiguous_recovery_retains_floors_and_requires_reconciliation(tmp_path):
    intent = begin_recovery(
        tmp_path,
        target=TARGET,
        inventories=[inventory()],
        evidence={"purpose": "isolated_rehearsal"},
        actor=ACTOR,
    )
    finish_recovery(
        tmp_path,
        intent,
        phase="ambiguous",
        actor=ACTOR,
        reservations=[
            {"name": "history_uid_seq", "value": 50, "source": "aborted_probe"}
        ],
    )
    state = retained_recovery_state(tmp_path, target=TARGET, require_terminal=False)
    assert state["pending"]
    assert max(item["value"] for item in state["floors"]) == 50
    with pytest.raises(BackupVerificationError, match="reconciliation"):
        begin_recovery(
            tmp_path,
            target=TARGET,
            inventories=[inventory()],
            evidence={"purpose": "isolated_rehearsal"},
            actor=ACTOR,
        )


def test_failed_recovery_floors_survive_a_later_attempt(tmp_path):
    first = begin_recovery(
        tmp_path,
        target=TARGET,
        inventories=[inventory(last=50)],
        evidence={"purpose": "fenced_migration"},
        actor=ACTOR,
    )
    finish_recovery(tmp_path, first, phase="aborted", actor=ACTOR)
    second = begin_recovery(
        tmp_path,
        target=TARGET,
        inventories=[inventory(last=22)],
        evidence={"purpose": "fenced_migration"},
        actor=ACTOR,
    )
    assert max(item["value"] for item in second["floors"]) == 50
    assert len(second["inventories"]) == 2


def test_journal_also_retains_shared_allocator_reservations(tmp_path):
    current = inventory()
    write_receipt(
        tmp_path,
        operation="sequence_advance",
        status="intent",
        actor=ACTOR,
        detail={
            "plan": {"target": TARGET},
            "floors": [
                {"name": "history_uid_seq", "value": 99, "source": "reservation"}
            ],
        },
    )
    intent = begin_recovery(
        tmp_path,
        target=TARGET,
        inventories=[current],
        evidence={"purpose": "fenced_migration"},
        actor=ACTOR,
    )
    assert max(item["value"] for item in intent["floors"]) == 99


def test_generator_created_after_backup_cannot_be_silently_skipped():
    with pytest.raises(BackupVerificationError, match="missing"):
        require_retained_definitions(inventory(), [inventory(name="later_uid_seq")])


def test_retained_generator_definition_change_is_blocking():
    current = inventory()
    changed = deepcopy(current)
    changed["sequences"][0]["increment_by"] = 3
    with pytest.raises(BackupVerificationError, match="definition changed"):
        require_retained_definitions(changed, [current])


def test_corrupt_or_truncated_receipts_block_recovery(tmp_path):
    intent = begin_recovery(
        tmp_path,
        target=TARGET,
        inventories=[inventory()],
        evidence={"purpose": "isolated_rehearsal"},
        actor=ACTOR,
    )
    finish_recovery(tmp_path, intent, phase="aborted", actor=ACTOR)
    last = read_receipts(tmp_path)[-1].path
    last.unlink()
    with pytest.raises(BackupVerificationError, match="invalid"):
        retained_recovery_state(tmp_path, target=TARGET)


def test_outcome_must_match_its_persisted_intent(tmp_path):
    intent = begin_recovery(
        tmp_path,
        target=TARGET,
        inventories=[inventory()],
        evidence={"purpose": "isolated_rehearsal"},
        actor=ACTOR,
    )
    with pytest.raises(BackupVerificationError, match="durable receipt"):
        finish_recovery(
            tmp_path,
            dict(intent, intent_sha256="altered"),
            phase="aborted",
            actor=ACTOR,
        )


def test_old_dump_alone_does_not_prove_lost_database_recovery(tmp_path):
    with pytest.raises(BackupVerificationError, match="old dump"):
        begin_recovery(
            tmp_path,
            target=TARGET,
            inventories=[inventory()],
            evidence={"purpose": "lost_database"},
            actor=ACTOR,
        )


def test_source_journal_retains_attempt_reservations_and_new_generator_definitions(
    tmp_path,
):
    source_dir = tmp_path / "source-journal"
    source_dir.mkdir()
    later = inventory(name="later_uid_seq", last=22)
    from daylily_tapdb.sequences import (
        build_sequence_advance_plan,
        record_sequence_advance_outcome,
    )

    plan = build_sequence_advance_plan(
        later,
        floors=[{"name": "later_uid_seq", "value": 77, "source": "prior_attempt"}],
    )
    saved = write_receipt(
        source_dir,
        operation="sequence_advance",
        status="intent",
        actor=ACTOR,
        detail={
            "phase": "intent",
            "plan": plan,
            "floors": [
                {
                    "name": "later_uid_seq",
                    "value": 78,
                    "source": "sequence_advance_intent",
                }
            ],
        },
    )
    source = source_recovery_state(source_dir, target=TARGET)
    assert source["inventories"] == [later]
    assert source["floors"][0]["value"] == 78
    assert len(source["state_sha256"]) == 64
    with pytest.raises(BackupVerificationError, match="retained generator is missing"):
        require_retained_definitions(inventory(), source["inventories"])

    destination_dir = tmp_path / "replacement-journal"
    destination_dir.mkdir()
    family = build_recovery_family(
        family_id="01111111-2222-4333-8444-555555555555",
        origin_inventory=later,
        receipts_dirs=[source_dir, destination_dir],
    )
    record_sequence_advance_outcome(
        seal_receipt(
            {
                "schema_version": "tapdb-sequence-apply/v1",
                "phase": "applied_pending_commit",
                "plan_sha256": plan["sha256"],
                "intent_receipt_id": saved.receipt_id,
                "inventory": later,
                "verification": {},
                "floors": source["floors"],
            }
        ),
        receipts_dir=source_dir,
        outcome="rolled_back",
        actor="test-operator",
    )
    destination = dict(TARGET, database="replacement_test")
    intent = begin_recovery(
        destination_dir,
        target=destination,
        inventories=[later],
        floors=source["floors"],
        evidence={
            "purpose": "fenced_source_recovery",
            "source_receipts_dir": str(source_dir),
            "source_recovery_state_sha256": source["state_sha256"],
        },
        actor=ACTOR,
        recovery_family=family,
    )
    assert max(row["value"] for row in intent["floors"]) == 78


def test_missing_source_journal_is_not_an_empty_source_history(tmp_path):
    for path in ("relative-journal", tmp_path / "absent-journal"):
        with pytest.raises(BackupVerificationError, match="existing absolute"):
            source_recovery_state(path, target=TARGET)


def test_unreconciled_source_attempt_cannot_authorize_replacement(tmp_path):
    begin_recovery(
        tmp_path,
        target=TARGET,
        inventories=[inventory()],
        evidence={"purpose": "fenced_migration"},
        actor=ACTOR,
    )
    with pytest.raises(BackupVerificationError, match="unresolved recovery"):
        source_recovery_state(tmp_path, target=TARGET)
