"""Fresh-process recovery across two different physical replacement databases."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from uuid import uuid4

import pytest

from daylily_tapdb.backup import service, verify
from daylily_tapdb.backup.receipts import read_receipts
from daylily_tapdb.backup.recovery import build_recovery_family, recovery_family_state
from daylily_tapdb.backup.source_contract import capture_source_contract
from daylily_tapdb.runtime_principal import operator_session
from daylily_tapdb.sequences import (
    apply_writer_fence_takeover,
    build_writer_fence_takeover_plan,
    sequence_next_value,
)
from tests.test_recovery_floor_pg import historical_source as source_fixture


@pytest.fixture(name="historical_source")
def source(pg_instance, tmp_path):
    yield from source_fixture.__wrapped__(pg_instance, tmp_path)


CHILD = """
import json, os, sys
from pathlib import Path
from sqlalchemy import text
from daylily_tapdb.backup import service
from daylily_tapdb.backup.receipts import Actor
from daylily_tapdb.backup.recovery import begin_recovery, observe_recovery
from daylily_tapdb.runtime_principal import operator_session
from daylily_tapdb.sequences import acquire_database_writer_fence, capture_sequence_inventory
p = json.load(sys.stdin)
cfg, family = p['cfg'], p['family']
directory = Path(p['directory'])
with operator_session(p['control'], isolation_level='REPEATABLE READ') as control, operator_session(cfg, isolation_level='REPEATABLE READ') as connection:
    with connection.begin():
        inventory = capture_sequence_inventory(connection, schema_name=cfg['schema_name'], target=service.inventory_target(cfg))
    acquire_database_writer_fence(connection, control_connection=control, inventory=inventory, receipts_dir=directory, recovery_family=family)
    transaction = connection.begin()
    actor = Actor(surface='cli', username=cfg['operator_user'])
    intent = begin_recovery(directory, target=inventory['target'], inventories=[inventory], evidence={'purpose':'fenced_migration'}, actor=actor, recovery_family=family)
    observe_recovery(connection, directory, intent, actor=actor, inventory=inventory)
    connection.execute(text('SELECT nextval(pg_get_serial_sequence(:table_name, :column_name))'), {'table_name':'"'+cfg['schema_name']+'".historical_record', 'column_name':'uid'})
    # Actual process death: no context exit, rollback hook, outcome or gate release.
    os._exit(73)
"""


def _capture(cfg, family):
    with operator_session(cfg, isolation_level="REPEATABLE READ") as connection:
        with connection.begin():
            return capture_source_contract(
                connection,
                schema_name=cfg["schema_name"],
                target=service.inventory_target(cfg),
                source_version="0.0.0-fixture",
                recovery_family=family,
            )


def _kill_and_take_over(cfg, control_cfg, family, directory):
    killed = subprocess.run(
        [sys.executable, "-c", CHILD],
        input=json.dumps(
            {
                "cfg": cfg,
                "control": control_cfg,
                "family": family,
                "directory": str(directory),
            }
        ),
        text=True,
        capture_output=True,
        timeout=30,
        env=os.environ.copy(),
        check=False,
    )
    assert killed.returncode == 73, killed.stderr
    origin = next(
        row
        for row in reversed(read_receipts(directory))
        if row.detail.get("phase") == "acquire_intent"
    )
    with operator_session(control_cfg, isolation_level="REPEATABLE READ") as control:
        plan = build_writer_fence_takeover_plan(
            control,
            target=service.inventory_target(cfg),
            receipts_dir=directory,
            fence_intent_receipt_id=origin.receipt_id,
            recovery_family=family,
        )
        assert plan["observed_connections_allowed"] is False
        quarantine = apply_writer_fence_takeover(
            control,
            plan,
            target_connection_factory=lambda: operator_session(
                cfg, isolation_level="REPEATABLE READ"
            ),
            receipts_dir=directory,
        )
    assert quarantine["phase"] == "quarantined"
    assert quarantine["old_operation_outcome"] == "unresolved_observed_only"
    events = read_receipts(directory)
    observed = [
        row
        for row in events
        if row.operation == "identity_recovery"
        and row.detail.get("phase") == "reconciled_observed"
    ]
    assert observed and observed[-1].detail["prior_transaction_outcome"] == "unknown"
    return quarantine


def test_subprocess_death_then_two_replacements_retain_every_family_floor(
    historical_source, tmp_path
):
    cfg, settings, engine = historical_source
    initial = _capture(cfg, None)
    engine.dispose()
    settings_a = dict(settings, config_dir=str(tmp_path / "replacement-a"))
    settings_b = dict(settings, config_dir=str(tmp_path / "replacement-b"))
    roots = [
        service.receipts_directory(item) for item in (settings, settings_a, settings_b)
    ]
    for root in roots:
        root.mkdir(parents=True)
    family = build_recovery_family(
        family_id=str(uuid4()),
        origin_inventory=initial["sequence_inventory"],
        receipts_dirs=roots,
    )
    source_contract = _capture(cfg, family)
    backup = service.create_backup(
        cfg, settings, source_contract=source_contract, recovery_family=family
    )
    # This explicitly named initdb-created control DB is part of the fixture.
    control_cfg = dict(cfg, database="postgres")
    current_cfg = cfg
    previous_floor = source_contract["sequence_inventory"]["sequences"][0][
        "allocated_floor"
    ]
    for index, destination_settings in enumerate((settings_a, settings_b)):
        quarantine = _kill_and_take_over(current_cfg, control_cfg, family, roots[index])
        actual = _capture(current_cfg, family)
        source_floor = actual["sequence_inventory"]["sequences"][0]["allocated_floor"]
        assert source_floor > previous_floor
        assert not recovery_family_state(family)["pending"]
        destination = "family_restore_" + uuid4().hex[:12]
        options = verify.RestoreOptions(target_database=destination)
        evidence = {
            "purpose": "fenced_source_recovery",
            "source_contract": actual,
            "source_receipts_dir": str(roots[index]),
            "recovery_family": family,
        }
        plan = verify.plan_restore(
            current_cfg,
            destination_settings,
            backup_id=backup.backup_id,
            options=options,
            recovery_source=evidence,
            control_cfg=control_cfg,
            quarantine_receipt=quarantine,
        )
        assert plan.ok, plan.to_payload()
        restored = verify.restore_backup(
            current_cfg,
            destination_settings,
            backup_id=backup.backup_id,
            options=options,
            recovery_source=evidence,
            control_cfg=control_cfg,
            quarantine_receipt=quarantine,
            plan_fingerprint=plan.plan_fingerprint,
        )
        assert restored.ok, restored.to_payload()
        assert restored.principal_binding_required
        current_cfg = dict(cfg, database=destination)
        after = _capture(current_cfg, family)
        assert (
            sequence_next_value(after["sequence_inventory"]["sequences"][0])
            > source_floor
        )
        assert (
            after["identity_inventory"]["tables"]
            == source_contract["identity_inventory"]["tables"]
        )
        previous_floor = source_floor
    state = recovery_family_state(family)
    assert len(state["members"]) == 3
    assert not state["pending"]
    assert max(item["value"] for item in state["floors"]) > previous_floor
