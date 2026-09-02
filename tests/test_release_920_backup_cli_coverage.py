"""Behavior coverage for the prune CLI adapter and its safety gates."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import typer

import daylily_tapdb.backup.prune as prune_service
import daylily_tapdb.cli.backup as backup_cli


def _invoke_prune(**overrides) -> None:
    values = {
        "apply_changes": False,
        "confirm_target": None,
        "release": None,
        "ignore_damaged": False,
        "allow_delete_markers": False,
        "allow_unknown_reclaim": False,
        "allow_bulk": False,
    }
    values.update(overrides)
    backup_cli.backup_prune(**values)


def _result(*, dry_run: bool, ok: bool = True):
    retained = SimpleNamespace(
        backup_id="backup-retained",
        backup_class="full",
        holds={"keep-last", "legal-hold"},
    )
    deletable = SimpleNamespace(
        backup_id="backup-deletable",
        backup_class="full",
        bytes=123,
    )
    gate = SimpleNamespace(id="retention-gate", detail="operator action required")
    plan = SimpleNamespace(
        target_label="local/tapdb/schema",
        keep_last=3,
        retained=[retained],
        deletable=[deletable],
        excluded=[{"backup_id": "other", "reason": "different target"}],
        blocking=[gate],
    )
    return SimpleNamespace(
        dry_run=dry_run,
        ok=ok,
        reconciled=("old-prune",),
        plan=plan,
        to_payload=lambda: {"dry_run": dry_run, "ok": ok},
    )


def _patch_common(monkeypatch: pytest.MonkeyPatch, *, json_mode: bool):
    monkeypatch.setattr(backup_cli, "_resolve", lambda: ({"cfg": 1}, {"settings": 2}))
    monkeypatch.setattr(backup_cli, "_actor", lambda: "actor")
    monkeypatch.setattr(backup_cli, "_json_mode", lambda: json_mode)
    exits: list[int] = []
    logs: list[tuple[str, str]] = []
    monkeypatch.setattr(backup_cli, "_exit", exits.append)
    monkeypatch.setattr(
        backup_cli, "_log", lambda op, detail="": logs.append((op, detail))
    )
    return exits, logs


def test_prune_rejects_unknown_hold_before_resolution() -> None:
    with pytest.raises(typer.BadParameter, match="--release must be one of"):
        _invoke_prune(release=["not-a-hold"])


def test_prune_json_apply_passes_every_safety_option(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exits, logs = _patch_common(monkeypatch, json_mode=True)
    monkeypatch.setattr(backup_cli, "_dry_run_requested", lambda: False)
    emitted: list[dict] = []
    monkeypatch.setattr(backup_cli.ccyo_out, "emit_json", emitted.append)
    captured: dict = {}

    def _prune(cfg, settings, **kwargs):
        captured.update({"cfg": cfg, "settings": settings, **kwargs})
        return _result(dry_run=False)

    monkeypatch.setattr(prune_service, "prune_backups", _prune)
    hold = prune_service.RELEASABLE_HOLDS[0]
    _invoke_prune(
        apply_changes=True,
        confirm_target="local/tapdb/schema",
        release=[hold],
        ignore_damaged=True,
        allow_delete_markers=True,
        allow_unknown_reclaim=True,
        allow_bulk=True,
    )

    assert captured == {
        "cfg": {"cfg": 1},
        "settings": {"settings": 2},
        "apply": True,
        "confirm_target": "local/tapdb/schema",
        "released": (hold,),
        "ignore_damaged": True,
        "allow_delete_markers": True,
        "allow_unknown_reclaim": True,
        "allow_bulk": True,
        "actor": "actor",
    }
    assert emitted == [{"dry_run": False, "ok": True}]
    assert exits == [backup_cli.EXIT_OK]
    assert logs == [("backup-prune", "applied deletable=1")]


def test_prune_global_dry_run_veto_and_complete_human_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exits, logs = _patch_common(monkeypatch, json_mode=False)
    monkeypatch.setattr(backup_cli, "_dry_run_requested", lambda: True)
    warnings: list[str] = []
    printed: list[str] = []
    errors: list[str] = []
    monkeypatch.setattr(backup_cli.ccyo_out, "warning", warnings.append)
    monkeypatch.setattr(backup_cli.ccyo_out, "print_text", printed.append)
    monkeypatch.setattr(backup_cli.ccyo_out, "error", errors.append)
    captured: dict = {}

    def _prune(cfg, settings, **kwargs):
        captured.update(kwargs)
        return _result(dry_run=True, ok=False)

    monkeypatch.setattr(prune_service, "prune_backups", _prune)
    _invoke_prune(apply_changes=True)

    assert captured["apply"] is False
    assert warnings == ["--dry-run is set; --apply ignored and nothing deleted."]
    report = printed[0]
    assert "Reconciled 1 interrupted prune(s): old-prune" in report
    assert "held by: keep-last, legal-hold" in report
    assert "Would delete (1)" in report
    assert "backup-deletable" in report
    assert "Not this target's (1)" in report
    assert "Dry run -- nothing was deleted" in report
    assert errors == ["retention-gate: operator action required"]
    assert exits == [backup_cli.EXIT_FINDINGS]
    assert logs == [("backup-prune", "planned deletable=1")]


def test_prune_human_report_handles_empty_optional_sections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exits, _logs = _patch_common(monkeypatch, json_mode=False)
    monkeypatch.setattr(backup_cli, "_dry_run_requested", lambda: False)
    printed: list[str] = []
    monkeypatch.setattr(backup_cli.ccyo_out, "print_text", printed.append)
    plan = SimpleNamespace(
        target_label="target",
        keep_last=1,
        retained=[],
        deletable=[],
        excluded=[],
        blocking=[],
    )
    result = SimpleNamespace(
        reconciled=(),
        dry_run=False,
        ok=True,
        plan=plan,
        to_payload=lambda: {"ok": True},
    )
    monkeypatch.setattr(prune_service, "prune_backups", lambda *args, **kwargs: result)

    _invoke_prune()

    assert "Deleted (0):" in printed[0]
    assert "(nothing)" in printed[0]
    assert "Dry run" not in printed[0]
    assert exits == [backup_cli.EXIT_OK]


def test_prune_service_failure_uses_shared_error_translation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_common(monkeypatch, json_mode=False)
    monkeypatch.setattr(backup_cli, "_dry_run_requested", lambda: False)
    handled: list[Exception] = []
    monkeypatch.setattr(backup_cli, "_handle", handled.append)
    failure = RuntimeError("inventory unavailable")
    monkeypatch.setattr(
        prune_service,
        "prune_backups",
        lambda *args, **kwargs: (_ for _ in ()).throw(failure),
    )

    _invoke_prune()

    assert handled == [failure]
