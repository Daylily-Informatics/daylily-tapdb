"""Behavior coverage for the backup CLI adapter's remaining branches."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import typer

import daylily_tapdb.backup.prune as prune_service
import daylily_tapdb.cli.backup as cli
from daylily_tapdb.backup.errors import BackupError
from daylily_tapdb.backup.manifest import BACKUP_CLASS_FULL


class _ExitError(Exception):
    def __init__(self, code: int):
        self.code = code


def _raise_exit(code: int) -> None:
    raise _ExitError(code)


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch):
    emitted: list[tuple[str, object]] = []
    monkeypatch.setattr(cli, "_resolve", lambda: ({"database": "db"}, {"store": "s"}))
    monkeypatch.setattr(cli, "_actor", lambda: "actor")
    monkeypatch.setattr(cli, "_exit", _raise_exit)
    monkeypatch.setattr(
        cli, "_log", lambda op, detail="": emitted.append(("log", (op, detail)))
    )
    monkeypatch.setattr(
        cli.ccyo_out, "emit_json", lambda value: emitted.append(("json", value))
    )
    monkeypatch.setattr(
        cli.ccyo_out, "print_text", lambda value: emitted.append(("text", value))
    )
    monkeypatch.setattr(
        cli.ccyo_out, "warning", lambda value: emitted.append(("warning", value))
    )
    monkeypatch.setattr(
        cli.ccyo_out, "error", lambda value: emitted.append(("error", value))
    )
    return emitted


def test_framework_flags_resolution_and_shared_emitters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = SimpleNamespace(json_mode=True, dry_run=True)
    monkeypatch.setattr("cli_core_yo.runtime.get_context", lambda: context)
    assert cli._json_mode() is True
    assert cli._dry_run_requested() is True

    monkeypatch.setattr(
        "cli_core_yo.runtime.get_context",
        lambda: (_ for _ in ()).throw(RuntimeError("no context")),
    )
    assert cli._json_mode() is False
    assert cli._dry_run_requested() is False

    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config",
        lambda: (_ for _ in ()).throw(RuntimeError("bad config")),
    )
    monkeypatch.setattr(
        cli,
        "_fail",
        lambda message, **kwargs: (_ for _ in ()).throw(_ExitError(kwargs["code"])),
    )
    with pytest.raises(_ExitError) as exc:
        cli._resolve()
    assert exc.value.code == cli.EXIT_ERROR


def test_fail_emit_handle_actor_and_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(cli, "_exit", _raise_exit)
    monkeypatch.setattr(
        cli.ccyo_out, "emit_error_json", lambda *args: calls.append(("json", args))
    )
    monkeypatch.setattr(
        cli.ccyo_out, "error", lambda value: calls.append(("human", value))
    )
    monkeypatch.setattr(cli, "_json_mode", lambda: True)
    with pytest.raises(_ExitError) as exc:
        cli._fail("broken", code=2, detail={"why": "x"})
    assert exc.value.code == 2 and calls[0][0] == "json"
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    with pytest.raises(_ExitError):
        cli._fail("broken", code=1)
    assert calls[-1] == ("human", "broken")

    with pytest.raises(_ExitError) as exc:
        cli._handle(BackupError("finding"))
    assert exc.value.code == cli.EXIT_FINDINGS
    with pytest.raises(_ExitError) as exc:
        cli._handle(RuntimeError("operation"))
    assert exc.value.code == cli.EXIT_ERROR

    monkeypatch.setenv("USER", "operator")
    assert cli._actor().username == "operator"
    monkeypatch.delenv("USER")
    assert cli._actor().username is None
    assert cli._validate_class(" FULL ") == BACKUP_CLASS_FULL
    with pytest.raises(typer.BadParameter, match="--class"):
        cli._validate_class("unknown")


def test_emit_and_render_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        cli.ccyo_out, "emit_json", lambda payload: calls.append(("json", payload))
    )
    monkeypatch.setattr(
        cli.ccyo_out, "print_text", lambda value: calls.append(("text", value))
    )
    monkeypatch.setattr(cli, "_json_mode", lambda: True)
    cli._emit({"ok": True}, human="human")
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    cli._emit({"ok": True}, human="human")
    cli._emit({"ok": True}, human=None)
    assert calls == [("json", {"ok": True}), ("text", "human")]
    rendered = cli._render_checks(
        [
            {"status": "pass", "id": "one", "detail": "ok"},
            {"status": "unknown", "id": "two", "detail": "odd"},
        ]
    )
    assert "✓ one" in rendered and "? two" in rendered


def test_plan_success_findings_and_failure(
    harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = SimpleNamespace(
        target_label="target",
        backup_class="full",
        storage={"uri": "file:///backup"},
        would_capture={"table_count": 2},
        ok=False,
        to_payload=lambda: {
            "checks": [{"status": "warn", "id": "drift", "detail": "changed"}]
        },
    )
    monkeypatch.setattr(cli.service, "plan_backup", lambda *_a, **_k: plan)
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    with pytest.raises(_ExitError) as exc:
        cli.backup_plan(backup_class="full", strict=True)
    assert exc.value.code == cli.EXIT_FINDINGS
    assert any(kind == "text" and "Backup plan" in value for kind, value in harness)

    failure = RuntimeError("planning failed")
    handled: list[Exception] = []
    monkeypatch.setattr(cli, "_handle", handled.append)
    monkeypatch.setattr(
        cli.service, "plan_backup", lambda *_a, **_k: (_ for _ in ()).throw(failure)
    )
    cli.backup_plan(backup_class="full", strict=False)
    assert handled == [failure]


def test_list_json_empty_damaged_and_failure(
    harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = SimpleNamespace(
        backup_id="id", backup_class="full", created_at=None, bytes=12
    )
    listing = SimpleNamespace(
        entries=[entry], damaged=["bad"], to_payload=lambda: {"entries": ["id"]}
    )
    monkeypatch.setattr(cli.service, "list_backups", lambda *_a, **_k: listing)
    monkeypatch.setattr(
        "daylily_tapdb.backup.views.status_context", lambda *_a: {"ok": True}
    )
    monkeypatch.setattr(cli, "_json_mode", lambda: True)
    with pytest.raises(_ExitError):
        cli.backup_list(backup_class="full", limit=1)
    assert harness[-1][0] == "json" and harness[-1][1]["status"] == {"ok": True}

    listing.entries = []
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    with pytest.raises(_ExitError):
        cli.backup_list(backup_class=None, limit=None)
    assert any(
        item == ("text", "No backups found for this target.") for item in harness
    )
    assert any(kind == "warning" for kind, _ in harness)

    listing.entries = [entry]
    listing.damaged = []
    with pytest.raises(_ExitError):
        cli.backup_list(backup_class=None, limit=None)
    assert any(kind == "text" and "id" in value for kind, value in harness)

    failure = RuntimeError("list failed")
    handled: list[Exception] = []
    monkeypatch.setattr(cli, "_handle", handled.append)
    monkeypatch.setattr(
        cli.service, "list_backups", lambda *_a, **_k: (_ for _ in ()).throw(failure)
    )
    cli.backup_list(backup_class=None, limit=None)
    assert handled == [failure]


def test_health_human_json_and_failure(
    harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = SimpleNamespace(
        exit_code=1,
        to_payload=lambda: {
            "target_label": "target",
            "status": "degraded",
            "checks": [{"status": "fail", "id": "fresh", "detail": "old"}],
        },
    )
    monkeypatch.setattr(cli.service, "health_report", lambda *_a: report)
    with pytest.raises(_ExitError) as exc:
        cli.backup_health(human=True)
    assert exc.value.code == 1
    assert any(
        kind == "text" and "target: degraded" in value for kind, value in harness
    )
    with pytest.raises(_ExitError):
        cli.backup_health(human=False)
    assert harness[-1][0] == "json"

    monkeypatch.setattr(cli.service, "target_label", lambda _cfg: "target")
    monkeypatch.setattr(
        cli.service,
        "health_report",
        lambda *_a: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    with pytest.raises(_ExitError) as exc:
        cli.backup_health(human=False)
    assert exc.value.code == cli.EXIT_ERROR
    assert harness[-1][1]["status"] == "unavailable"


def test_verify_validation_success_warning_and_failure(
    harness, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(typer.BadParameter, match="either"):
        cli.backup_verify(backup_id=None, path=None, level="deep")
    with pytest.raises(typer.BadParameter, match="quick or deep"):
        cli.backup_verify(backup_id="id", path=None, level="invalid")

    report = SimpleNamespace(
        ok=False,
        to_payload=lambda: {
            "checks": [{"status": "fail", "id": "hash", "detail": "bad"}]
        },
    )
    monkeypatch.setattr(cli.service, "verify_backup", lambda *_a, **_k: report)
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    path = tmp_path / "loose.dump"
    with pytest.raises(_ExitError) as exc:
        cli.backup_verify(backup_id=None, path=path, level="quick")
    assert exc.value.code == cli.EXIT_FINDINGS
    assert any(kind == "warning" and "loose file" in value for kind, value in harness)

    failure = RuntimeError("verify failed")
    handled: list[Exception] = []
    monkeypatch.setattr(cli, "_handle", handled.append)
    monkeypatch.setattr(
        cli.service, "verify_backup", lambda *_a, **_k: (_ for _ in ()).throw(failure)
    )
    cli.backup_verify(backup_id="id", path=None, level="deep")
    assert handled == [failure]


def test_restore_plan_success_and_failure(
    harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = SimpleNamespace(
        backup_id="id",
        mode="isolated",
        target_database="restore",
        target_schema="schema",
        required_confirm_target=None,
        plan_fingerprint="finger",
        steps=["verify", "restore"],
        ok=True,
        to_payload=lambda: {"checks": []},
    )
    monkeypatch.setattr(cli.verify, "plan_restore", lambda *_a, **_k: plan)
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    with pytest.raises(_ExitError) as exc:
        cli.backup_restore_plan(
            backup_id="id",
            mode="isolated",
            target_database="restore",
            target_schema="schema",
            allow_identity_mismatch=True,
            allow_unknown_migrations=True,
            allow_unclaimable_prefixes=True,
        )
    assert exc.value.code == cli.EXIT_OK
    assert any(kind == "text" and "1. verify" in value for kind, value in harness)

    failure = RuntimeError("restore plan failed")
    handled: list[Exception] = []
    monkeypatch.setattr(cli, "_handle", handled.append)
    monkeypatch.setattr(
        cli.verify, "plan_restore", lambda *_a, **_k: (_ for _ in ()).throw(failure)
    )
    cli.backup_restore_plan(
        backup_id="id",
        mode="isolated",
        target_database=None,
        target_schema=None,
        allow_identity_mismatch=False,
        allow_unknown_migrations=False,
        allow_unclaimable_prefixes=False,
    )
    assert handled == [failure]


def test_create_success_dry_verify_and_failure(
    harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = SimpleNamespace(
        backup_id="id",
        backup_class="full",
        storage_prefix="prefix",
        dry_run=False,
        verify=SimpleNamespace(ok=False),
        to_payload=lambda: {"backup_id": "id"},
    )
    monkeypatch.setattr(cli.service, "create_backup", lambda *_a, **_k: result)
    monkeypatch.setattr(cli, "_dry_run_requested", lambda: False)
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    with pytest.raises(_ExitError) as exc:
        cli.backup_create(
            backup_class="full", allow_drift=True, note="note", existing_snapshot=None
        )
    assert exc.value.code == cli.EXIT_FINDINGS
    assert any(kind == "log" and value[0] == "BACKUP_CREATE" for kind, value in harness)

    result.dry_run = True
    result.verify = None
    monkeypatch.setattr(cli, "_dry_run_requested", lambda: True)
    with pytest.raises(_ExitError) as exc:
        cli.backup_create(
            backup_class="full", allow_drift=False, note=None, existing_snapshot=None
        )
    assert exc.value.code == cli.EXIT_OK

    failure = RuntimeError("create failed")
    handled: list[Exception] = []
    monkeypatch.setattr(cli, "_handle", handled.append)
    monkeypatch.setattr(
        cli.service, "create_backup", lambda *_a, **_k: (_ for _ in ()).throw(failure)
    )
    cli.backup_create(
        backup_class="full", allow_drift=False, note=None, existing_snapshot=None
    )
    assert handled == [failure]
    assert any(
        kind == "log" and value[0] == "BACKUP_CREATE_FAILED" for kind, value in harness
    )


def test_restore_options_and_restore_results(
    harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(typer.BadParameter):
        cli._restore_options(
            mode="invalid",
            target_database=None,
            target_schema=None,
            allow_identity_mismatch=False,
            allow_unknown_migrations=False,
            allow_unclaimable_prefixes=False,
            keep_superseded=False,
        )
    with pytest.raises(typer.BadParameter, match="keep-superseded"):
        cli.backup_restore(
            backup_id="id",
            mode="isolated",
            target_database=None,
            target_schema=None,
            confirm_target=None,
            plan_fingerprint=None,
            allow_identity_mismatch=False,
            allow_unknown_migrations=False,
            allow_unclaimable_prefixes=False,
            keep_superseded=True,
        )

    result = SimpleNamespace(
        target_database="db",
        target_schema="schema",
        mode="in-place",
        safety_backup_id="safety",
        quarantined=True,
        dry_run=False,
        ok=False,
        to_payload=lambda: {"checks": []},
    )
    monkeypatch.setattr(cli.verify, "restore_backup", lambda *_a, **_k: result)
    monkeypatch.setattr(cli, "_dry_run_requested", lambda: False)
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    with pytest.raises(_ExitError) as exc:
        cli.backup_restore(
            backup_id="id",
            mode="in-place",
            target_database="db",
            target_schema="schema",
            confirm_target="target",
            plan_fingerprint="f",
            allow_identity_mismatch=True,
            allow_unknown_migrations=True,
            allow_unclaimable_prefixes=True,
            keep_superseded=True,
        )
    assert exc.value.code == cli.EXIT_FINDINGS
    assert any(kind == "warning" and "quarantined" in value for kind, value in harness)

    result.safety_backup_id = None
    result.quarantined = False
    result.dry_run = True
    monkeypatch.setattr(cli, "_dry_run_requested", lambda: True)
    with pytest.raises(_ExitError) as exc:
        cli.backup_restore(
            backup_id="id",
            mode="isolated",
            target_database="db",
            target_schema="schema",
            confirm_target=None,
            plan_fingerprint=None,
            allow_identity_mismatch=False,
            allow_unknown_migrations=False,
            allow_unclaimable_prefixes=False,
            keep_superseded=False,
        )
    assert exc.value.code == cli.EXIT_OK

    failure = RuntimeError("restore failed")
    handled: list[Exception] = []
    monkeypatch.setattr(cli, "_handle", handled.append)
    monkeypatch.setattr(
        cli.verify, "restore_backup", lambda *_a, **_k: (_ for _ in ()).throw(failure)
    )
    cli.backup_restore(
        backup_id="id",
        mode="isolated",
        target_database=None,
        target_schema=None,
        confirm_target=None,
        plan_fingerprint=None,
        allow_identity_mismatch=False,
        allow_unknown_migrations=False,
        allow_unclaimable_prefixes=False,
        keep_superseded=False,
    )
    assert handled == [failure]


def test_prune_service_error(harness, monkeypatch: pytest.MonkeyPatch) -> None:
    failure = RuntimeError("prune failed")
    handled: list[Exception] = []
    monkeypatch.setattr(cli, "_handle", handled.append)
    monkeypatch.setattr(cli, "_dry_run_requested", lambda: False)
    monkeypatch.setattr(
        prune_service, "prune_backups", lambda *_a, **_k: (_ for _ in ()).throw(failure)
    )
    cli.backup_prune(
        apply_changes=False,
        confirm_target=None,
        release=None,
        ignore_damaged=False,
        allow_delete_markers=False,
        allow_unknown_reclaim=False,
        allow_bulk=False,
    )
    assert handled == [failure]


def test_rehearse_success_failure_and_dry_run(
    harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    evidence = SimpleNamespace(
        database="rehearsal",
        evidence_key=None,
        dry_run=False,
        ok=False,
        to_payload=lambda: {"checks": []},
    )
    monkeypatch.setattr(cli.verify, "rehearse_restore", lambda *_a, **_k: evidence)
    monkeypatch.setattr(cli, "_dry_run_requested", lambda: False)
    monkeypatch.setattr(cli, "_json_mode", lambda: False)
    with pytest.raises(_ExitError) as exc:
        cli.backup_rehearse(backup_id="id", keep=True)
    assert exc.value.code == cli.EXIT_FINDINGS
    assert any(
        kind == "log" and value[0] == "BACKUP_REHEARSE" for kind, value in harness
    )

    evidence.dry_run = True
    evidence.ok = True
    evidence.evidence_key = "receipt.json"
    monkeypatch.setattr(cli, "_dry_run_requested", lambda: True)
    with pytest.raises(_ExitError) as exc:
        cli.backup_rehearse(backup_id="id", keep=False)
    assert exc.value.code == cli.EXIT_OK

    failure = RuntimeError("rehearse failed")
    handled: list[Exception] = []
    monkeypatch.setattr(cli, "_handle", handled.append)
    monkeypatch.setattr(
        cli.verify, "rehearse_restore", lambda *_a, **_k: (_ for _ in ()).throw(failure)
    )
    cli.backup_rehearse(backup_id="id", keep=False)
    assert handled == [failure]
