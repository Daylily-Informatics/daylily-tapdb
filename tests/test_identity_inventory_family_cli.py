"""Real on-disk source/family CLI minting, with one authoritative C family builder."""

from __future__ import annotations

import json
from contextlib import contextmanager
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from daylily_tapdb import identity_inventory, sequences
from daylily_tapdb.backup.recovery import validate_recovery_family
from daylily_tapdb.backup.source_contract import validate_source_contract
from daylily_tapdb.cli import identity as cli
from daylily_tapdb.identity_inventory import seal_receipt
from tests.test_identity_inventory import snapshot
from tests.test_sequence_protection import inventory


@pytest.fixture
def family_cli(monkeypatch, tmp_path):
    seq = inventory()
    ident = seal_receipt(
        {
            **snapshot(),
            "schema_name": seq["schema_name"],
            "target": seq["target"],
            "physical_target": seq["physical_target"],
        }
    )
    events = []

    @contextmanager
    def connection(*args, **kwargs):
        events.append("database-read")
        yield object()

    monkeypatch.setattr(cli, "_resolve", lambda mappings: ({}, seq["target"]))
    monkeypatch.setattr(cli, "operator_connection", connection)
    monkeypatch.setattr(
        identity_inventory, "capture_identity_inventory", lambda *a, **k: ident
    )
    monkeypatch.setattr(sequences, "capture_sequence_inventory", lambda *a, **k: seq)
    journal = tmp_path / "journal"
    journal.mkdir()
    return seq, events, journal


def test_mint_and_reuse_exact_family_through_public_inventory_cli(family_cli, tmp_path):
    seq, events, journal = family_cli
    source_path, family_path = tmp_path / "source.json", tmp_path / "family.json"
    family_id = str(uuid4())
    result = CliRunner().invoke(
        cli.identity_app,
        [
            "inventory",
            "--source-version",
            "9.0.9",
            "--new-recovery-family-id",
            family_id,
            "--family-receipts-dir",
            str(journal),
            "--family-receipt",
            str(family_path),
            "--receipt",
            str(source_path),
        ],
    )
    assert result.exit_code == 0, result.output
    family = json.loads(family_path.read_text())
    source = json.loads(source_path.read_text())
    validate_recovery_family(family)
    validate_source_contract(source)
    assert family["origin"] == {
        "target": seq["target"],
        "physical_target": seq["physical_target"],
    }
    assert source["recovery_family"] == family and family["family_id"] == family_id
    second = tmp_path / "second-source.json"
    result = CliRunner().invoke(
        cli.identity_app,
        [
            "inventory",
            "--source-version",
            "9.0.9",
            "--recovery-family",
            str(family_path),
            "--receipt",
            str(second),
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(second.read_text())["recovery_family"] == family
    assert events == ["database-read", "database-read"]


@pytest.mark.parametrize(
    "case",
    [
        "missing_source_version",
        "missing_source_path",
        "missing_family_path",
        "missing_id",
        "missing_journal",
        "relative_source",
        "existing_source",
        "same_output",
        "relative_family",
        "missing_directory",
        "conflicting_existing",
    ],
)
def test_family_output_and_complete_flag_group_validate_before_database_read(
    family_cli, tmp_path, case
):
    _, events, journal = family_cli
    source_path, family_path = tmp_path / "source.json", tmp_path / "family.json"
    options = {
        "--source-version": "9.0.9",
        "--new-recovery-family-id": str(uuid4()),
        "--family-receipts-dir": str(journal),
        "--family-receipt": str(family_path),
        "--receipt": str(source_path),
    }
    if case.startswith("missing_") and case != "missing_directory":
        options.pop(
            {
                "missing_source_version": "--source-version",
                "missing_source_path": "--receipt",
                "missing_family_path": "--family-receipt",
                "missing_id": "--new-recovery-family-id",
                "missing_journal": "--family-receipts-dir",
            }[case]
        )
    elif case == "relative_source":
        options["--receipt"] = "relative-source.json"
    elif case == "existing_source":
        source_path.write_text("{}")
    elif case == "same_output":
        options["--family-receipt"] = str(source_path)
    elif case == "relative_family":
        options["--family-receipt"] = "relative-family.json"
    elif case == "missing_directory":
        options["--family-receipts-dir"] = str(tmp_path / "missing")
    else:
        options["--recovery-family"] = str(tmp_path / "explicit-family.json")
    args = ["inventory"]
    for key, value in options.items():
        args.extend([key, value])
    result = CliRunner().invoke(cli.identity_app, args)
    assert result.exit_code == 2, result.output
    assert events == []


def test_existing_family_requires_explicit_source_contract(family_cli, tmp_path):
    _, events, _ = family_cli
    result = CliRunner().invoke(
        cli.identity_app,
        ["inventory", "--recovery-family", str(tmp_path / "explicit.json")],
    )
    assert result.exit_code == 2, result.output
    assert events == []
