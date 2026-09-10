"""Thin CLI contracts, especially the framework dry-run veto."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from daylily_tapdb.backup.recovery import build_recovery_family
from daylily_tapdb.cli import identity as identity_cli
from daylily_tapdb.cli import sequences as cli
from daylily_tapdb.identity_inventory import seal_receipt, validate_receipt
from daylily_tapdb.migration_identity import write_json_receipt
from daylily_tapdb.sequences import (
    PLAN_VERSION,
    build_sequence_advance_plan,
    verify_sequence_floors,
)
from tests.test_sequence_protection import inventory, state


@pytest.fixture
def command(monkeypatch, tmp_path):
    current = inventory()
    cfg = {"operator_user": "operator"}
    files = {tmp_path / "floors.json": {"floors": []}}
    events = []
    output = []

    @contextmanager
    def connection(config, **options):
        events.append(("connect", options))
        yield object()
        events.append(("commit",))

    monkeypatch.setattr(cli, "_resolve", lambda mappings: (cfg, current["target"]))
    monkeypatch.setattr(cli, "_read", lambda path: files[path])
    monkeypatch.setattr(cli, "operator_connection", connection)
    monkeypatch.setattr(
        cli, "capture_sequence_inventory", lambda *args, **kwargs: current
    )
    monkeypatch.setattr(cli, "get_context", lambda: SimpleNamespace(dry_run=False))
    monkeypatch.setattr(cli.ccyo_out, "emit_json", output.append)
    monkeypatch.setattr(
        cli.ccyo_out, "emit_error_json", lambda *args: events.append(("error", *args))
    )
    monkeypatch.setattr(
        cli,
        "write_json_receipt",
        lambda path, payload: files.__setitem__(path, payload),
    )
    monkeypatch.setattr(
        cli,
        "apply_sequence_advance_plan",
        lambda *args, **kwargs: (
            events.append(("apply",)) or {"phase": "applied_pending_commit"}
        ),
    )
    monkeypatch.setattr(
        cli,
        "record_sequence_advance_outcome",
        lambda *args, **kwargs: (
            events.append(("outcome", kwargs["outcome"])) or {"phase": "committed"}
        ),
    )
    args = {
        "floors": tmp_path / "floors.json",
        "receipt": tmp_path / "result.json",
        "apply": False,
        "preflight_receipt": None,
        "writer_fence": None,
        "establish_writer_fence": False,
        "control_config": None,
        "receipts_dir": None,
        "sequence_mappings": None,
    }
    return current, files, events, output, args


def test_default_advance_emits_read_only_dry_plan(command):
    current, files, events, output, args = command
    cli.advance(**args)
    assert output == [build_sequence_advance_plan(current, floors=[])]
    assert files[args["receipt"]] == output[0]
    assert events[0] == (
        "connect",
        {"isolation_level": "REPEATABLE READ", "read_only": True},
    )
    assert ("apply",) not in events


def test_global_dry_run_vetoes_explicit_apply_without_extra_paths(command, monkeypatch):
    _, _, events, output, args = command
    monkeypatch.setattr(cli, "get_context", lambda: SimpleNamespace(dry_run=True))
    cli.advance(**dict(args, apply=True))
    assert output[0]["schema_version"] == "tapdb-sequence-advance/v1"
    assert ("apply",) not in events
    assert events[0][1]["read_only"] is True


def test_apply_requires_all_reviewed_paths(command):
    _, _, events, _, args = command
    with pytest.raises(SystemExit) as error:
        cli.advance(**dict(args, apply=True))
    assert error.value.code == 2
    assert not any(event[0] == "connect" for event in events)


def test_apply_records_commit_only_after_connection_commit(command, tmp_path):
    current, files, events, output, args = command
    files[tmp_path / "reviewed.json"] = build_sequence_advance_plan(current, floors=[])
    files[tmp_path / "fence.json"] = {"mode": "declared"}
    cli.advance(
        **dict(
            args,
            apply=True,
            preflight_receipt=tmp_path / "reviewed.json",
            writer_fence=tmp_path / "fence.json",
            receipts_dir=tmp_path,
        )
    )
    assert events == [
        ("connect", {"isolation_level": "REPEATABLE READ", "read_only": False}),
        ("apply",),
        ("commit",),
        ("outcome", "committed"),
    ]
    assert output == [{"phase": "committed"}]


def test_changed_reviewed_plan_never_reaches_apply(command, tmp_path):
    _, files, events, _, args = command
    files[tmp_path / "reviewed.json"] = {"tampered": True}
    files[tmp_path / "fence.json"] = {"mode": "declared"}
    with pytest.raises(SystemExit) as error:
        cli.advance(
            **dict(
                args,
                apply=True,
                preflight_receipt=tmp_path / "reviewed.json",
                writer_fence=tmp_path / "fence.json",
                receipts_dir=tmp_path,
            )
        )
    assert error.value.code == 2 and ("apply",) not in events


def test_existing_output_path_and_malformed_floors_rejected(command, tmp_path):
    _, files, _, _, args = command
    with pytest.raises(SystemExit):
        cli.advance(**dict(args, receipt=tmp_path))
    files[args["floors"]] = {"unknown": []}
    with pytest.raises(SystemExit):
        cli.advance(**args)


def test_verify_read_only_results_and_failed_floor_exit(command):
    _, files, events, output, args = command
    cli.verify(floors=args["floors"], sequence_mappings=None)
    assert output[-1]["ok"] and events[0][1]["read_only"]
    files[args["floors"]] = {
        "floors": [{"name": "object_uid_seq", "value": 11, "source": "retained"}]
    }
    with pytest.raises(SystemExit) as error:
        cli.verify(floors=args["floors"], sequence_mappings=None)
    assert error.value.code == 1 and not output[-1]["ok"]
    files[args["floors"]] = {"wrong": []}
    with pytest.raises(SystemExit) as error:
        cli.verify(floors=args["floors"], sequence_mappings=None)
    assert error.value.code == 2


def test_established_fence_cli_requires_explicit_control_and_mutual_exclusion(
    command, tmp_path
):
    _, _, _, _, args = command
    for changes in (
        {"establish_writer_fence": True},
        {"control_config": tmp_path / "control.yaml"},
        {
            "apply": True,
            "establish_writer_fence": True,
            "writer_fence": tmp_path / "fence.json",
        },
    ):
        with pytest.raises(SystemExit) as error:
            cli.advance(**dict(args, **changes))
        assert error.value.code == 2


def test_established_fence_cli_retains_target_through_release(
    command, monkeypatch, tmp_path
):
    current, files, events, output, args = command
    files[tmp_path / "reviewed.json"] = build_sequence_advance_plan(current, floors=[])

    class Retained:
        @contextmanager
        def begin(self):
            events.append(("begin",))
            yield self
            events.append(("commit",))

    @contextmanager
    def session(cfg, **kwargs):
        yield Retained()

    monkeypatch.setattr(cli, "operator_session", session)
    monkeypatch.setattr(
        cli, "get_db_config", lambda **kwargs: {"operator_user": "operator"}
    )
    monkeypatch.setattr(
        cli,
        "acquire_database_writer_fence",
        lambda *args, **kwargs: (
            events.append(("acquire",))
            or {"fence": {"mode": "database_connections_disabled"}}
        ),
    )
    monkeypatch.setattr(
        cli,
        "release_database_writer_fence",
        lambda *args, **kwargs: events.append(("release",)) or {"phase": "released"},
    )
    cli.advance(
        **dict(
            args,
            apply=True,
            establish_writer_fence=True,
            preflight_receipt=tmp_path / "reviewed.json",
            receipts_dir=tmp_path,
            control_config=tmp_path / "control.yaml",
        )
    )
    assert events == [
        ("begin",),
        ("commit",),
        ("acquire",),
        ("begin",),
        ("apply",),
        ("commit",),
        ("outcome", "committed"),
        ("release",),
    ]
    assert output[-1]["writer_fence_release"]["phase"] == "released"


def test_established_fence_apply_stale_plan_and_missing_control_fail(command, tmp_path):
    current, files, events, _, args = command
    files[tmp_path / "reviewed.json"] = build_sequence_advance_plan(current, floors=[])
    with pytest.raises(SystemExit) as error:
        cli.advance(
            **dict(
                args,
                apply=True,
                establish_writer_fence=True,
                preflight_receipt=tmp_path / "reviewed.json",
                receipts_dir=tmp_path,
            )
        )
    assert error.value.code == 2 and not events[:-1]


@pytest.mark.parametrize("declaration", ["provider_contract", "quarantine_receipt"])
def test_provider_or_quarantine_input_without_establishment_never_connects(
    command, tmp_path, declaration
):
    _, files, events, _, args = command
    declared = tmp_path / "explicit-declaration.json"
    files[declared] = {"explicit": "owner API validates only in establishment flow"}
    with pytest.raises(SystemExit) as error:
        cli.advance(**dict(args, **{declaration: declared}))
    assert error.value.code == 2
    assert not any(event[0] == "connect" for event in events)


def test_stale_established_plan_does_not_reach_control_or_close_gate(
    command, tmp_path, monkeypatch
):
    _, files, events, _, args = command
    reviewed = tmp_path / "stale.json"
    files[reviewed] = {"different": "reviewed source"}

    @contextmanager
    def begin():
        yield None

    @contextmanager
    def session(cfg, **kwargs):
        events.append(("target_session",))
        yield SimpleNamespace(begin=begin)

    monkeypatch.setattr(cli, "operator_session", session)
    monkeypatch.setattr(cli, "get_db_config", lambda **kwargs: {})
    monkeypatch.setattr(
        cli,
        "acquire_database_writer_fence",
        lambda *args, **kwargs: pytest.fail("stale plan must not close the database"),
    )
    with pytest.raises(SystemExit) as error:
        cli.advance(
            **dict(
                args,
                apply=True,
                establish_writer_fence=True,
                control_config=tmp_path / "explicit-control.yaml",
                receipts_dir=tmp_path,
                preflight_receipt=reviewed,
            )
        )
    assert error.value.code == 2
    assert events.count(("target_session",)) == 1


@pytest.fixture
def reconciliation_command(monkeypatch, tmp_path):
    control_path = tmp_path / "control.json"
    write_json_receipt(control_path, {"test": "explicit configured control"})
    events = []
    target = inventory()["target"]
    plan = seal_receipt(
        {
            "schema_version": "tapdb-writer-fence-takeover/v1",
            "phase": "planned",
            "target": target,
        }
    )
    cfg = {"operator_user": "operator", "kind": "target"}
    control_cfg = {"operator_user": "operator", "kind": "control"}

    @contextmanager
    def session(config, **kwargs):
        events.append(("session", config["kind"]))
        yield object()

    def build(control, **kwargs):
        events.append(("plan", kwargs))
        return plan

    def apply(control, reviewed, **kwargs):
        assert reviewed == plan
        with kwargs["target_connection_factory"]():
            events.append(("takeover",))
        return seal_receipt({**reviewed, "phase": "quarantined"})

    monkeypatch.setattr(cli, "_resolve", lambda mappings: (cfg, target))
    monkeypatch.setattr(cli, "get_db_config", lambda **kwargs: control_cfg)
    monkeypatch.setattr(cli, "operator_session", session)
    monkeypatch.setattr(cli, "build_writer_fence_takeover_plan", build)
    monkeypatch.setattr(cli, "apply_writer_fence_takeover", apply)
    monkeypatch.setattr(cli, "get_context", lambda: SimpleNamespace(dry_run=False))
    args = [
        "reconcile",
        "--control-config",
        str(control_path),
        "--receipts-dir",
        str(tmp_path),
        "--fence-intent-receipt-id",
        "explicit-intent-id",
    ]
    return args, plan, events


def test_reconcile_real_disk_review_then_apply_keeps_dry_run_control_only(
    reconciliation_command, tmp_path
):
    args, plan, events = reconciliation_command
    reviewed = tmp_path / "reviewed.json"
    dry = CliRunner().invoke(cli.sequences_app, [*args, "--receipt", str(reviewed)])
    assert dry.exit_code == 0, dry.output
    assert json.loads(reviewed.read_text()) == plan
    assert ("session", "target") not in events
    applied = tmp_path / "applied.json"
    result = CliRunner().invoke(
        cli.sequences_app,
        [
            *args,
            "--apply",
            "--preflight-receipt",
            str(reviewed),
            "--receipt",
            str(applied),
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(applied.read_text())["phase"] == "quarantined"
    assert events.count(("takeover",)) == 1


def test_reconcile_global_dry_veto_and_stale_disk_receipt(
    reconciliation_command, monkeypatch, tmp_path
):
    args, plan, events = reconciliation_command
    monkeypatch.setattr(cli, "get_context", lambda: SimpleNamespace(dry_run=True))
    result = CliRunner().invoke(
        cli.sequences_app,
        [*args, "--apply", "--receipt", str(tmp_path / "vetoed.json")],
    )
    assert result.exit_code == 0, result.output
    assert ("session", "target") not in events
    monkeypatch.setattr(cli, "get_context", lambda: SimpleNamespace(dry_run=False))
    stale = tmp_path / "stale.json"
    write_json_receipt(
        stale,
        seal_receipt(
            {**plan, "target": {**plan["target"], "database": "other-explicit-db"}}
        ),
    )
    result = CliRunner().invoke(
        cli.sequences_app,
        [
            *args,
            "--apply",
            "--preflight-receipt",
            str(stale),
            "--receipt",
            str(tmp_path / "rejected.json"),
        ],
    )
    assert result.exit_code == 2
    assert ("takeover",) not in events


def test_reconcile_reads_explicit_provider_family_files_and_release_mode(
    reconciliation_command, monkeypatch, tmp_path
):
    args, _, events = reconciliation_command
    provider, family = tmp_path / "provider.json", tmp_path / "family.json"
    write_json_receipt(
        provider, {"exact_provider_input": "owning API validates semantics"}
    )
    write_json_receipt(family, {"exact_family_input": "owning API validates semantics"})
    seen = []
    monkeypatch.setattr(
        cli,
        "reconcile_writer_fence_release",
        lambda *a, **k: seen.append(k) or {"phase": "release_reconciliation_planned"},
    )
    args = args[:-2] + [
        "--release-intent-receipt-id",
        "release-intent",
        "--provider-contract",
        str(provider),
        "--recovery-family",
        str(family),
    ]
    result = CliRunner().invoke(
        cli.sequences_app, [*args, "--receipt", str(tmp_path / "release-review.json")]
    )
    assert result.exit_code == 0, result.output
    assert seen[0]["provider_contract"] == json.loads(provider.read_text())
    assert seen[0]["recovery_family"] == json.loads(family.read_text())
    assert seen[0]["dry_run"] is True
    assert ("session", "target") not in events


@pytest.mark.parametrize(
    "change",
    [
        "relative_output",
        "existing_output",
        "relative_control",
        "missing_control",
        "relative_journal",
        "missing_journal",
        "no_intent",
        "two_intents",
        "unreviewed_apply",
    ],
)
def test_reconcile_rejects_invalid_disk_paths_and_modes_before_any_connection(
    reconciliation_command, tmp_path, change
):
    args, _, events = reconciliation_command
    args = [*args, "--receipt", str(tmp_path / "new-output.json")]
    if change == "relative_output":
        args[-1] = "relative-output.json"
    elif change == "existing_output":
        args[-1] = str(tmp_path)
    elif change in {"relative_control", "missing_control"}:
        args[args.index("--control-config") + 1] = str(
            Path("relative-control.yaml")
            if change == "relative_control"
            else tmp_path / "missing-control.yaml"
        )
    elif change in {"relative_journal", "missing_journal"}:
        args[args.index("--receipts-dir") + 1] = str(
            Path("relative-journal")
            if change == "relative_journal"
            else tmp_path / "missing-journal"
        )
    elif change == "no_intent":
        offset = args.index("--fence-intent-receipt-id")
        del args[offset : offset + 2]
    elif change == "two_intents":
        args += ["--release-intent-receipt-id", "other-reviewed-intent"]
    else:
        args.append("--apply")
    result = CliRunner().invoke(cli.sequences_app, args)
    assert result.exit_code == 2, result.output
    assert events == []


@pytest.fixture
def real_files_command(monkeypatch, tmp_path):
    """Replace database I/O; CLI parsing, file I/O, and pure validators are real."""
    mappings = {
        "abc_instance_seq": {
            "kind": "prefix",
            "prefix": "ABC",
            "evidence": "reviewed fixture source declaration",
        }
    }
    current = inventory(
        state(name="abc_instance_seq", mapping=mappings["abc_instance_seq"]),
        sequence_mappings=mappings,
    )
    cfg = dict(
        current["target"],
        config_path=current["target"]["config_identity"],
        operator_user="operator",
    )
    floors = [{"name": "abc_instance_seq", "value": 11, "source": "original_source"}]
    paths = {
        name: tmp_path / f"{name}.json"
        for name in ("floors", "mappings", "fence", "dry", "applied")
    }
    writer_fence = {
        "mode": "exclusive_database_connect",
        "operator_role": "operator",
        "database_oid": 12345,
    }
    for name, document in (
        ("floors", {"floors": floors}),
        ("mappings", mappings),
        ("fence", writer_fence),
    ):
        write_json_receipt(paths[name], document)
    events = []

    @contextmanager
    def connection(config, **options):
        assert config == cfg
        events.append(("connect", options))
        yield object()
        events.append(("commit",))

    def capture(connection, *, schema_name, target):
        assert schema_name == current["schema_name"]
        assert target["sequence_mappings"] == mappings
        return current

    monkeypatch.setattr(identity_cli, "get_db_config", lambda: cfg)
    monkeypatch.setattr(cli, "operator_connection", connection)
    monkeypatch.setattr(cli, "capture_sequence_inventory", capture)
    monkeypatch.setattr(cli, "get_context", lambda: SimpleNamespace(dry_run=False))
    return current, floors, paths, writer_fence, events


def test_runner_reads_actual_floor_mapping_and_saved_shared_plan_receipts(
    real_files_command, monkeypatch, tmp_path
):
    current, floors, paths, writer_fence, events = real_files_command
    runner = CliRunner()
    args = [
        "--floors",
        str(paths["floors"]),
        "--sequence-mappings",
        str(paths["mappings"]),
    ]
    dry = runner.invoke(
        cli.sequences_app, ["advance", *args, "--receipt", str(paths["dry"])]
    )
    assert dry.exit_code == 0, dry.output
    plan = build_sequence_advance_plan(current, floors=floors)
    assert json.loads(dry.stdout) == plan
    assert json.loads(paths["dry"].read_text()) == plan

    def apply(connection, reviewed, *, writer_fence, receipts_dir):
        validate_receipt(reviewed, PLAN_VERSION)
        assert reviewed == plan
        assert writer_fence == json.loads(paths["fence"].read_text())
        after = inventory(
            state(
                name="abc_instance_seq",
                last_value=14,
                is_called=False,
                allocated_floor=None,
                mapping=current["sequences"][0]["mapping"],
            ),
            sequence_mappings=current["sequence_mappings"],
        )
        events.append(("apply",))
        return seal_receipt(
            {
                "schema_version": "tapdb-sequence-apply/v1",
                "phase": "applied_pending_commit",
                "plan_sha256": reviewed["sha256"],
                "intent_receipt_id": "fixture-intent",
                "inventory": after,
                "verification": verify_sequence_floors(after, floors=floors),
                "floors": floors
                + [
                    {
                        "name": "abc_instance_seq",
                        "value": 14,
                        "source": "sequence_advance_intent",
                    }
                ],
            }
        )

    monkeypatch.setattr(cli, "apply_sequence_advance_plan", apply)
    applied = runner.invoke(
        cli.sequences_app,
        [
            "advance",
            *args,
            "--apply",
            "--receipt",
            str(paths["applied"]),
            "--preflight-receipt",
            str(paths["dry"]),
            "--writer-fence",
            str(paths["fence"]),
            "--receipts-dir",
            str(tmp_path / "journal"),
        ],
    )
    assert applied.exit_code == 0, applied.output
    payload = json.loads(applied.stdout)
    assert payload["phase"] == "committed" and payload["verification"]["ok"]
    assert json.loads(paths["applied"].read_text()) == payload
    assert events[-2:] == [("apply",), ("commit",)]
    assert writer_fence["mode"] == "exclusive_database_connect"


def test_runner_actual_files_preserve_plan_and_floor_semantic_checks(
    real_files_command, monkeypatch, tmp_path
):
    current, floors, paths, _, events = real_files_command
    monkeypatch.setattr(
        cli,
        "apply_sequence_advance_plan",
        lambda *args, **kwargs: pytest.fail("invalid review must never apply"),
    )
    write_json_receipt(
        paths["dry"],
        dict(build_sequence_advance_plan(current, floors=floors), sha256="invalid"),
    )
    result = CliRunner().invoke(
        cli.sequences_app,
        [
            "advance",
            "--floors",
            str(paths["floors"]),
            "--sequence-mappings",
            str(paths["mappings"]),
            "--receipt",
            str(paths["applied"]),
            "--apply",
            "--preflight-receipt",
            str(paths["dry"]),
            "--writer-fence",
            str(paths["fence"]),
            "--receipts-dir",
            str(tmp_path / "journal"),
        ],
    )
    assert result.exit_code == 2 and "no longer matches" in result.output
    assert not paths["applied"].exists()
    result = CliRunner().invoke(
        cli.sequences_app,
        [
            "verify",
            "--floors",
            str(paths["floors"]),
            "--sequence-mappings",
            str(paths["mappings"]),
        ],
    )
    assert result.exit_code == 1
    assert json.loads(result.stdout)["ok"] is False
    wrong = tmp_path / "wrong-floors.json"
    write_json_receipt(
        wrong,
        {"floors": [{"name": "missing_sequence", "value": 8, "source": "source"}]},
    )
    result = CliRunner().invoke(
        cli.sequences_app,
        [
            "verify",
            "--floors",
            str(wrong),
            "--sequence-mappings",
            str(paths["mappings"]),
        ],
    )
    assert result.exit_code == 2 and "missing generator" in result.output.lower()
    assert ("apply",) not in events


def test_runner_real_family_file_is_retained_in_plan_and_used_by_verification(
    real_files_command, tmp_path
):
    current, _, paths, _, _ = real_files_command
    journal = tmp_path / "family-journal"
    journal.mkdir()
    family = build_recovery_family(
        family_id=str(uuid4()), origin_inventory=current, receipts_dirs=[journal]
    )
    family_path = tmp_path / "family.json"
    write_json_receipt(family_path, family)
    args = [
        "--floors",
        str(paths["floors"]),
        "--sequence-mappings",
        str(paths["mappings"]),
        "--recovery-family",
        str(family_path),
    ]
    planned = CliRunner().invoke(
        cli.sequences_app, ["advance", *args, "--receipt", str(paths["dry"])]
    )
    assert planned.exit_code == 0, planned.output
    assert json.loads(paths["dry"].read_text())["recovery_family"] == family
    verified = CliRunner().invoke(cli.sequences_app, ["verify", *args])
    assert verified.exit_code == 1
    assert json.loads(verified.stdout)["ok"] is False
