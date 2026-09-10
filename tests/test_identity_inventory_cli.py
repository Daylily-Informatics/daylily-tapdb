"""Explicit config, read-only capture, and identity/source receipt routing."""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from typer.testing import CliRunner

from daylily_tapdb.cli import identity as cli
from daylily_tapdb.migration_identity import write_json_receipt
from tests.test_identity_inventory import snapshot


@pytest.fixture
def command(monkeypatch, tmp_path):
    payload = snapshot()
    events, output, files = [], [], {}

    @contextmanager
    def connection(cfg, **kwargs):
        events.append(kwargs)
        yield object()

    monkeypatch.setattr(cli, "_resolve", lambda mappings: ({}, payload["target"]))
    monkeypatch.setattr(cli, "_read", lambda path: files[path])
    monkeypatch.setattr(cli, "operator_connection", connection)
    monkeypatch.setattr(
        cli, "capture_identity_inventory", lambda *args, **kwargs: payload
    )
    monkeypatch.setattr(
        cli, "write_json_receipt", lambda path, value: files.__setitem__(path, value)
    )
    monkeypatch.setattr(cli.ccyo_out, "emit_json", output.append)
    monkeypatch.setattr(
        cli.ccyo_out, "emit_error_json", lambda *args: events.append(args)
    )
    files[tmp_path / "before.json"] = payload
    return payload, events, output, files


def test_inventory_reads_without_current_runtime_and_emits_receipt(command, tmp_path):
    payload, events, output, files = command
    cli.inventory(
        receipt=tmp_path / "out.json", source_version=None, sequence_mappings=None
    )
    assert output == [payload] and files[tmp_path / "out.json"] == payload
    assert events == [{"isolation_level": "REPEATABLE READ", "read_only": True}]


def test_explicit_source_version_routes_to_source_contract(command, monkeypatch):
    from daylily_tapdb.backup import source_contract

    _, _, output, _ = command
    seen = []
    monkeypatch.setattr(
        source_contract,
        "capture_source_contract",
        lambda *args, **kwargs: (
            seen.append(kwargs) or {"schema_version": "tapdb-source-contract/v1"}
        ),
    )
    cli.inventory(receipt=None, source_version="9.0.9", sequence_mappings=None)
    assert seen[0]["source_version"] == "9.0.9"
    assert output[0]["schema_version"] == "tapdb-source-contract/v1"


def test_verify_live_and_offline_receipts(command, tmp_path):
    payload, events, output, files = command
    cli.verify(
        before=tmp_path / "before.json",
        after=None,
        conversion_manifest=None,
        sequence_mappings=None,
    )
    assert output[-1]["ok"] and events[0]["read_only"]
    files[tmp_path / "after.json"] = payload
    cli.verify(
        before=tmp_path / "before.json",
        after=tmp_path / "after.json",
        conversion_manifest=None,
        sequence_mappings=None,
    )
    assert output[-1]["ok"] and len(events) == 1


def test_verify_rejects_offline_receipt_for_different_config(command, tmp_path):
    payload, _, _, files = command
    files[tmp_path / "after.json"] = dict(payload, target={})
    with pytest.raises(SystemExit) as error:
        cli.verify(
            before=tmp_path / "before.json",
            after=tmp_path / "after.json",
            conversion_manifest=None,
            sequence_mappings=None,
        )
    assert error.value.code == 2


def test_inventory_error_sanitizes_driver_exception(command, monkeypatch):
    _, events, _, _ = command

    def fail(*args, **kwargs):
        raise RuntimeError("Driver exception carrying unsanitized row content")

    monkeypatch.setattr(cli, "capture_identity_inventory", fail)
    with pytest.raises(SystemExit):
        cli.inventory(receipt=None, source_version=None, sequence_mappings=None)
    assert events[-1][1] == "RuntimeError"


def test_explicit_file_read_and_target_resolution(monkeypatch, tmp_path):
    payload = snapshot()
    write_json_receipt(tmp_path / "input.json", {"mapping": {}})
    write_json_receipt(tmp_path / "mapping.json", {"mapping": {}})
    with pytest.raises(ValueError, match="absolute"):
        cli._read(type(tmp_path)("relative"))
    assert cli._read(tmp_path / "input.json") == {"mapping": {}}
    cfg = dict(
        payload["target"],
        config_path=payload["target"]["config_identity"],
        server_port=5432,
    )
    monkeypatch.setattr(cli, "get_db_config", lambda: cfg)
    actual, target = cli._resolve(tmp_path / "mapping.json")
    assert actual == cfg and target["server_port"] == 5432
    assert target["sequence_mappings"] == {"mapping": {}}


def test_runner_reads_actual_shared_identity_receipts_and_mapping_object(
    monkeypatch, tmp_path
):
    payload = snapshot()
    cfg = dict(payload["target"], config_path=payload["target"]["config_identity"])
    monkeypatch.setattr(cli, "get_db_config", lambda: cfg)
    for name, document in (
        ("before.json", payload),
        ("after.json", payload),
        ("mappings.json", {}),
        (
            "conversion.json",
            {"schema_version": "tapdb-identity-conversion/v1", "tables": {}},
        ),
    ):
        write_json_receipt(tmp_path / name, document)
    result = CliRunner().invoke(
        cli.identity_app,
        [
            "verify",
            "--before",
            str(tmp_path / "before.json"),
            "--after",
            str(tmp_path / "after.json"),
            "--sequence-mappings",
            str(tmp_path / "mappings.json"),
            "--conversion-manifest",
            str(tmp_path / "conversion.json"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["ok"] is True
    assert "receipt_version" not in payload and "evidence_sha256" not in payload


def test_runner_loaded_identity_receipt_still_requires_own_valid_hash(
    monkeypatch, tmp_path
):
    payload = snapshot()
    monkeypatch.setattr(
        cli,
        "get_db_config",
        lambda: dict(
            payload["target"], config_path=payload["target"]["config_identity"]
        ),
    )
    write_json_receipt(tmp_path / "before.json", dict(payload, sha256="invalid"))
    write_json_receipt(tmp_path / "after.json", payload)
    result = CliRunner().invoke(
        cli.identity_app,
        [
            "verify",
            "--before",
            str(tmp_path / "before.json"),
            "--after",
            str(tmp_path / "after.json"),
        ],
    )
    assert result.exit_code == 2
    assert "checksum" in result.output


@pytest.mark.parametrize("value", [[], "not an object", None, 17])
def test_generic_reader_rejects_non_object_json(tmp_path, value):
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="JSON object"):
        cli._read(path)


def test_generic_reader_rejects_missing_directory_and_malformed_json(tmp_path):
    for path in (tmp_path, tmp_path / "missing.json"):
        with pytest.raises(ValueError, match="existing absolute"):
            cli._read(path)
    path = tmp_path / "malformed.json"
    path.write_text("{not-json", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        cli._read(path)


def test_source_envelope_identity_is_validated(monkeypatch):
    from daylily_tapdb.backup import source_contract

    payload = snapshot()
    observed = []
    monkeypatch.setattr(source_contract, "validate_source_contract", observed.append)
    envelope = {
        "schema_version": "tapdb-source-contract/v1",
        "identity_inventory": payload,
    }
    assert cli._identity(envelope) == payload
    assert observed == [envelope]


def test_verify_finding_exit_and_explicit_conversion(command, tmp_path):
    from daylily_tapdb.identity_inventory import seal_receipt

    payload, _, output, files = command
    after = dict(payload, tables={})
    files[tmp_path / "after.json"] = seal_receipt(after)
    files[tmp_path / "conversion.json"] = {
        "schema_version": "tapdb-identity-conversion/v1",
        "tables": {},
    }
    with pytest.raises(SystemExit) as error:
        cli.verify(
            before=tmp_path / "before.json",
            after=tmp_path / "after.json",
            conversion_manifest=tmp_path / "conversion.json",
            sequence_mappings=None,
        )
    assert error.value.code == 1 and not output[-1]["ok"]
