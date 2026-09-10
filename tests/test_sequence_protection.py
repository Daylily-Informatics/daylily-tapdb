"""Pure strict-floor and fail-closed receipt contracts."""

from __future__ import annotations

from copy import deepcopy

import pytest

from daylily_tapdb.identity_inventory import seal_receipt
from daylily_tapdb.sequences import (
    INVENTORY_VERSION,
    SequenceProtectionError,
    build_sequence_advance_plan,
    sequence_definition,
    sequence_next_value,
    verify_sequence_floors,
)


def state(**changes):
    return {
        "name": "object_uid_seq",
        "increment_by": 3,
        "min_value": 2,
        "start_value": 2,
        "max_value": 101,
        "cache_size": 1,
        "cycle": False,
        "last_value": 8,
        "is_called": True,
        "allocated_floor": 8,
        "assigned_floor": None,
        "owner": "operator",
        "dependencies": [],
        "mapping": {"kind": "owned_column", "columns": []},
        **changes,
    }


def inventory(*states, **changes):
    return seal_receipt(
        {
            "schema_version": INVENTORY_VERSION,
            "schema_name": "objects",
            "target": {
                "engine_type": "local",
                "host": "localhost",
                "port": 15438,
                "database": "local_test",
                "schema_name": "objects",
                "config_identity": "/tmp/explicit-test.yaml",
                "domain_code": "Z",
                "owner_repo_name": "test-owner",
            },
            "physical_target": {
                "database": "local_test",
                "database_oid": 12345,
                "server_address": "127.0.0.1",
                "server_port": 15438,
            },
            "sequence_mappings": {},
            "missing_generators": [],
            "sequences": list(states) or [state()],
            **changes,
        }
    )


@pytest.mark.parametrize(
    ("called", "last", "expected"),
    [
        (True, 8, 11),
        (False, 8, 8),
        (False, 2, 2),
        (True, 2, 5),
    ],
)
def test_next_value_respects_increment_and_called(called, last, expected):
    assert sequence_next_value(state(is_called=called, last_value=last)) == expected


@pytest.mark.parametrize(
    "changes",
    [
        {"increment_by": 0},
        {"increment_by": -1},
        {"increment_by": True},
        {"cycle": True},
        {"cycle": 0},
        {"cache_size": 0},
        {"min_value": 0},
        {"start_value": 0},
        {"max_value": 1},
        {"last_value": None},
        {"last_value": 7},
        {"last_value": 200},
        {"last_value": True},
        {"is_called": 1},
        {"last_value": 101},
        {"name": ""},
    ],
)
def test_unsupported_or_exhausted_generators_fail(changes):
    with pytest.raises(ValueError):
        sequence_next_value(state(**changes))


def test_incomplete_definition_has_no_legacy_default():
    with pytest.raises(SequenceProtectionError, match="Complete"):
        sequence_definition({"name": "object_uid_seq", "last_value": 1})


@pytest.mark.parametrize(
    ("floor", "expected"), [(0, 11), (8, 11), (9, 11), (11, 14), (12, 14), (98, 101)]
)
def test_smallest_strictly_higher_increment_aligned_value(floor, expected):
    plan = build_sequence_advance_plan(
        inventory(),
        floors=[{"name": "object_uid_seq", "value": floor, "source": "reserved"}],
    )
    assert plan["advances"][0]["next_value"] == expected
    assert plan["advances"][0]["next_value"] > floor


def test_unused_sequence_not_advanced_without_applicable_floor():
    plan = build_sequence_advance_plan(
        inventory(state(last_value=2, is_called=False, allocated_floor=None)), floors=[]
    )
    assert plan["advances"] == [{"name": "object_uid_seq", "floor": 1, "next_value": 2}]


def test_every_floor_source_and_assigned_values_participate():
    floors = [
        {"name": "object_uid_seq", "source": source, "value": value}
        for source, value in [
            ("original_source", 10),
            ("final_fenced_source", 20),
            ("target_before_write", 30),
            ("reservation", 40),
            ("migration", 50),
            ("probe", 60),
            ("aborted", 70),
            ("recovery", 80),
        ]
    ]
    plan = build_sequence_advance_plan(
        inventory(state(assigned_floor=83)), floors=floors
    )
    assert plan["advances"][0] == {
        "name": "object_uid_seq",
        "floor": 83,
        "next_value": 86,
    }
    assert len(plan["floors"]) == 8


def test_cache_reservation_high_water_is_retained():
    plan = build_sequence_advance_plan(
        inventory(state(cache_size=8, last_value=23, allocated_floor=23)), floors=[]
    )
    assert plan["advances"][0]["next_value"] == 26


@pytest.mark.parametrize(
    "floors",
    [
        None,
        {},
        [{"name": "missing", "value": 1, "source": "history"}],
        [{"name": "object_uid_seq", "value": True, "source": "history"}],
        [{"name": "object_uid_seq", "value": -1, "source": "history"}],
        [{"name": "object_uid_seq", "value": 1, "source": ""}],
        [{"name": "object_uid_seq", "value": 1}],
    ],
)
def test_malformed_floors_fail_closed(floors):
    with pytest.raises(SequenceProtectionError):
        build_sequence_advance_plan(inventory(), floors=floors)


def test_floor_exhaustion_and_unknown_or_missing_generators_fail():
    with pytest.raises(SequenceProtectionError, match="maximum"):
        build_sequence_advance_plan(
            inventory(),
            floors=[{"name": "object_uid_seq", "value": 101, "source": "recovery"}],
        )
    with pytest.raises(SequenceProtectionError, match="Unknown"):
        build_sequence_advance_plan(
            inventory(state(mapping={"kind": "unmapped"})), floors=[]
        )
    with pytest.raises(SequenceProtectionError, match="Missing"):
        verify_sequence_floors(
            inventory(missing_generators=["reserved_instance_seq"]), floors=[]
        )


def test_tampered_receipt_and_duplicate_generator_fail():
    receipt = inventory()
    receipt["sequences"][0]["last_value"] = 98
    with pytest.raises(ValueError, match="checksum"):
        build_sequence_advance_plan(receipt, floors=[])
    with pytest.raises(SequenceProtectionError, match="Duplicate"):
        build_sequence_advance_plan(inventory(state(), state()), floors=[])


def test_inconsistent_allocated_floor_fails_even_with_recomputed_hash():
    with pytest.raises(SequenceProtectionError, match="disagrees"):
        build_sequence_advance_plan(inventory(state(allocated_floor=3)), floors=[])


def test_verify_reports_failure_on_equality_and_success_strictly_above():
    floor = {"name": "object_uid_seq", "value": 11, "source": "final"}
    current = inventory()
    assert verify_sequence_floors(current, floors=[floor])["ok"] is False
    floor["value"] = 10
    assert verify_sequence_floors(current, floors=[floor])["ok"] is True


def test_plan_is_deterministic_and_does_not_mutate_inputs():
    current = inventory()
    frozen = deepcopy(current)
    first = build_sequence_advance_plan(current, floors=[])
    assert first == build_sequence_advance_plan(current, floors=[])
    assert current == frozen


@pytest.mark.parametrize("prefix", ["WX", "WSX", "XX", "AY", "MSG"])
def test_catalog_prefix_binding_is_explicit_versioned_metadata(prefix):
    from daylily_tapdb.sequences import _catalog_prefix_binding

    assert (
        _catalog_prefix_binding(
            f"tapdb-prefix-binding/v1:{prefix}",
            sequence_name=f"{prefix.lower()}_instance_seq",
        )
        == prefix
    )


@pytest.mark.parametrize("annotation", [None, "ordinary unrelated operator comment"])
def test_catalog_comments_are_not_implicit_mapping_evidence(annotation):
    from daylily_tapdb.sequences import _catalog_prefix_binding

    assert _catalog_prefix_binding(annotation, sequence_name="ay_instance_seq") is None


@pytest.mark.parametrize(
    "annotation",
    [
        "tapdb-prefix-binding/v2:AY",
        "tapdb-prefix-binding/v1:",
        "tapdb-prefix-binding/v1:ay",
        "tapdb-prefix-binding/v1:WX",
        "tapdb-prefix-binding",
    ],
)
def test_malformed_or_conflicting_catalog_binding_fails(annotation):
    from daylily_tapdb.sequences import _catalog_prefix_binding

    with pytest.raises(SequenceProtectionError):
        _catalog_prefix_binding(annotation, sequence_name="ay_instance_seq")
