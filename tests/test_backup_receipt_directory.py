"""Explicit retained journals: local files and mocks only, no database or AWS."""

import json
import pytest
import yaml
from fastapi import HTTPException

from admin import backups as api
from daylily_tapdb.backup import service
from daylily_tapdb.backup.errors import BackupVerificationError
from daylily_tapdb.backup.receipts import (
    read_head,
    read_receipts,
    verify_receipt_chain,
    write_receipt,
)
from daylily_tapdb.backup.recovery import build_recovery_family
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context
from daylily_tapdb.cli.db_config import get_backup_settings, get_db_config
from tests.test_backup_config import _init_config, _raw, _update, runner
from tests.test_recovery_floor import ACTOR, inventory


@pytest.fixture(autouse=True)
def isolated_context():
    clear_cli_context()
    yield
    clear_cli_context()


def configured(tmp_path):
    root = tmp_path.resolve()
    cfg_path = _init_config(root)
    journal = root / "retained-journal"
    journal.mkdir()
    result = _update(cfg_path, "--backup-receipts-directory", str(journal))
    assert result.exit_code == 0, result.exception
    return cfg_path, journal, get_backup_settings(config_path=cfg_path)


def family_for(journal):
    return build_recovery_family(
        family_id="01111111-2222-4333-8444-555555555555",
        origin_inventory=inventory(),
        receipts_dirs=[journal],
    )


def test_config_update_preserves_retained_receipts_and_default_is_unchanged(tmp_path):
    root = tmp_path.resolve()
    cfg_path = _init_config(root)
    settings = get_backup_settings(config_path=cfg_path)
    assert service.receipts_directory(settings) == root / "backups" / "receipts"
    assert not (root / "backups").exists()
    journal = root / "journal"
    journal.mkdir()
    receipt = write_receipt(
        journal, operation="operator_observation", status="succeeded", actor=ACTOR
    )
    before = receipt.path.read_bytes()
    result = _update(cfg_path, "--backup-receipts-directory", str(journal))
    assert result.exit_code == 0, result.exception
    assert _raw(cfg_path)["backup"]["receipts_directory"] == str(journal)
    assert (
        service.receipts_directory(get_backup_settings(config_path=cfg_path)) == journal
    )
    assert receipt.path.read_bytes() == before
    assert not (root / "backups").exists()


@pytest.mark.parametrize(
    "kind",
    [
        "blank",
        "relative",
        "missing",
        "file",
        "symlink",
        "parent",
        "slash",
        "whitespace",
    ],
)
def test_cli_rejects_bad_paths_without_changing_config(tmp_path, kind):
    cfg_path, journal, _ = configured(tmp_path)
    file_path = journal.parent / "file"
    file_path.write_text("not a directory")
    alias = journal.parent / "alias"
    alias.symlink_to(journal, target_is_directory=True)
    values = {
        "blank": "",
        "relative": "journal",
        "missing": str(journal / "missing"),
        "file": str(file_path),
        "symlink": str(alias),
        "parent": str(journal / ".." / journal.name),
        "slash": str(journal) + "/",
        "whitespace": " " + str(journal),
    }
    before = cfg_path.read_bytes()
    result = _update(cfg_path, "--backup-receipts-directory", values[kind])
    assert result.exit_code != 0
    assert "backup.receipts_directory" in str(result.exception)
    assert cfg_path.read_bytes() == before


@pytest.mark.parametrize("value", [None, "", 42, [], {}])
def test_malformed_explicit_config_never_uses_default(tmp_path, value):
    cfg_path = _init_config(tmp_path.resolve())
    root = _raw(cfg_path)
    root["backup"]["receipts_directory"] = value
    cfg_path.write_text(yaml.safe_dump(root))
    with pytest.raises(BackupVerificationError, match="backup.receipts_directory"):
        get_backup_settings(config_path=cfg_path)
    with pytest.raises(BackupVerificationError, match="backup.receipts_directory"):
        service.receipts_directory(
            {"config_dir": str(cfg_path.parent), "receipts_directory": value}
        )
    assert not (cfg_path.parent / "backups").exists()


def test_removed_explicit_journal_fails_instead_of_recreating(tmp_path):
    _, journal, settings = configured(tmp_path)
    journal.rmdir()
    with pytest.raises(BackupVerificationError):
        service.receipts_directory(settings)
    assert not journal.exists()


def test_native_plan_rejects_invalid_setting_before_storage_without_a_family(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        service, "storage_for", lambda _: pytest.fail("invalid setting reached storage")
    )
    with pytest.raises(BackupVerificationError, match="backup.receipts_directory"):
        service.plan_backup({}, {"config_dir": str(tmp_path), "receipts_directory": ""})


def test_family_gate_accepts_only_retained_directory_before_storage_or_database(
    tmp_path, monkeypatch
):
    _, journal, settings = configured(tmp_path)
    family = family_for(journal)
    calls = []

    def stop_at_storage(_settings):
        calls.append(True)
        raise RuntimeError("reached storage after retained-family validation")

    monkeypatch.setattr(service, "storage_for", stop_at_storage)
    with pytest.raises(RuntimeError, match="reached storage"):
        service.plan_backup({}, settings, recovery_family=family)
    assert calls == [True]
    calls.clear()
    other = journal.parent / "other"
    other.mkdir()
    with pytest.raises(BackupVerificationError, match="outside"):
        service.plan_backup(
            {}, {**settings, "receipts_directory": str(other)}, recovery_family=family
        )
    assert not calls
    assert list(journal.iterdir()) == []
    assert list(other.iterdir()) == []


def test_corrupt_retained_journal_is_not_bypassed(tmp_path, monkeypatch):
    _, journal, settings = configured(tmp_path)
    family = family_for(journal)
    (journal / "000001.json").write_text("corrupt")
    monkeypatch.setattr(
        service, "storage_for", lambda _: pytest.fail("family gate was bypassed")
    )
    with pytest.raises(BackupVerificationError, match="unreadable"):
        service.plan_backup({}, settings, recovery_family=family)


def test_cli_and_api_append_to_same_retained_chain_on_mock_capture_failure(
    tmp_path, monkeypatch
):
    cfg_path, journal, settings = configured(tmp_path)
    first = write_receipt(
        journal, operation="operator_observation", status="succeeded", actor=ACTOR
    )
    before = first.path.read_bytes()
    family = family_for(journal)
    family_path = journal.parent / "family.json"
    family_path.write_text(json.dumps(family))
    captures = []

    def fail_capture(*args, **kwargs):
        captures.append(kwargs["recovery_family"])
        raise BackupVerificationError("mock capture stopped before any database access")

    monkeypatch.setattr(service, "_capture", fail_capture)
    result = runner.invoke(
        app,
        [
            "--config",
            str(cfg_path),
            "backup",
            "create",
            "--recovery-family",
            str(family_path),
        ],
    )
    assert result.exit_code != 0
    assert "mock capture stopped" in result.output
    with pytest.raises(HTTPException) as error:
        api.create_payload(
            get_db_config(config_path=cfg_path),
            settings,
            body={"recovery_family": family},
            actor=ACTOR,
        )
    assert error.value.status_code == 422
    assert captures == [family, family]
    receipts = read_receipts(journal)
    assert len(receipts) == 3
    assert [item.status for item in receipts] == ["succeeded", "failed", "failed"]
    assert receipts[1].prev_receipt_sha256 == first.checksum()
    assert receipts[2].prev_receipt_sha256 == receipts[1].checksum()
    assert first.path.read_bytes() == before
    assert verify_receipt_chain(receipts, head=read_head(journal)).ok
    assert not (cfg_path.parent / "backups" / "receipts").exists()
