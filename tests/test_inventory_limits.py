"""Resource-policy regressions without database deployment or migration."""

from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from daylily_tapdb import identity_inventory as inv
from daylily_tapdb.backup.service import _source_contract_config, inventory_target
from daylily_tapdb.cli import identity as cli
from daylily_tapdb.migration_identity import write_json_receipt
from tests.test_identity_inventory import snapshot


@pytest.fixture
def capture(monkeypatch):
    original = snapshot()
    table = deepcopy(original["tables"]["history"])
    table["primary_key"] = ["id"]
    table["columns"] = [{"name": "id"}]
    table["immutable_columns"] = ["id"]
    for key in ("rows", "row_count", "content_sha256"):
        table.pop(key)
    monkeypatch.setattr(inv, "require_snapshot", lambda connection: None)
    monkeypatch.setattr(inv, "physical_target", lambda *a: original["physical_target"])
    monkeypatch.setattr(
        inv, "catalog_tables", lambda *a: [{"kind": "r", "name": "history"}]
    )
    monkeypatch.setattr(inv, "_metadata", lambda *a: deepcopy(table))

    def run(values, limits):
        class Cursor:
            closed = False

            def __iter__(self):
                for row in values:
                    yield (inv.canonical_json(row),)

            def close(self):
                self.closed = True

        cursor = Cursor()
        connection = SimpleNamespace(execute=lambda *a, **kw: cursor)
        return cursor, lambda: inv._capture_identity_inventory(
            connection,
            schema_name="inventory",
            target=original["target"],
            limits=limits,
        )

    return run


@pytest.mark.parametrize(
    "limits",
    [
        {"max_rows": 0},
        {"max_row_bytes": True},
        {"max_receipt_bytes": "512MiB"},
        {"unknown": 1},
        {"max_rows": 2**63},
    ],
)
def test_invalid_resource_policy_fails_before_capture(limits):
    with pytest.raises(inv.IdentityInventoryError):
        inv.InventoryLimits.parse(limits)


@pytest.mark.parametrize("name", ["max_rows", "max_row_bytes", "max_receipt_bytes"])
def test_selected_limit_failure_is_structured_and_closes_cursor(capture, name):
    cursor, run = capture(
        [{"id": "first", "private": "DO_NOT_DISCLOSE"}, {"id": "second"}], {name: 1}
    )
    with pytest.raises(inv.InventoryLimitExceededError) as caught:
        run()
    detail = caught.value.diagnostics
    assert detail["table"] == "history" and detail["phase"] == "identity.capture"
    assert detail["limit_name"] == name and detail["configured_value"] == 1
    assert detail["attempted_value"] > 1 and detail["processed_rows"] >= 0
    assert "DO_NOT_DISCLOSE" not in str(caught.value)
    assert cursor.closed


def test_complete_capture_above_original_128mib_roundtrips(capture, tmp_path):
    # Text primary keys are fixture data, not invented TapDB/Meridian EUIDs.
    rows = ({"id": ("x" * 65536) + str(index)} for index in range(2050))
    cursor, run = capture(rows, {"max_receipt_bytes": 160 * 1024 * 1024})
    receipt = run()
    assert cursor.closed and receipt["usage"]["rows"] == 2050
    assert receipt["usage"]["evidence_bytes"] > 128 * 1024 * 1024
    assert inv.verify_identity_inventory(receipt, receipt)["ok"]
    path = tmp_path / "large-inventory.json"
    write_json_receipt(path, receipt)
    import json

    with path.open() as handle:
        restored = json.load(handle)
    assert restored["sha256"] == receipt["sha256"]
    assert restored["limits"] == receipt["limits"]
    assert len(restored["tables"]["history"]["rows"]) == 2050
    path.unlink()


def test_receipt_policy_is_retained_and_conflicting_config_rejected(
    capture, monkeypatch
):
    _, run = capture([{"id": "source"}], {"max_receipt_bytes": 256 * 1024 * 1024})
    original = run()
    target = inv.with_inventory_limits(original["target"], original)
    assert target["inventory_limits"] == original["limits"]
    assert inv.validate_target(target, "inventory") == original["target"]
    with pytest.raises(inv.IdentityInventoryError, match="differ"):
        inv.with_inventory_limits(
            dict(target, inventory_limits={"max_rows": 1}), original
        )
    invalid = deepcopy(original)
    invalid["usage"]["evidence_bytes"] -= 1
    invalid = inv.seal_receipt(invalid)
    with pytest.raises(inv.IdentityInventoryError, match="usage"):
        inv.verify_identity_inventory(original, invalid)
    from daylily_tapdb.backup import source_contract

    monkeypatch.setattr(source_contract, "validate_source_contract", lambda *a: None)
    cfg = dict(original["target"], config_path=original["target"]["config_identity"])
    contract = {
        "identity_inventory": original,
        "sequence_inventory": {"sequence_mappings": {}},
    }
    resolved = _source_contract_config(cfg, contract)
    assert inventory_target(resolved)["inventory_limits"] == original["limits"]
    monkeypatch.setattr(cli, "get_db_config", lambda: resolved)
    assert cli._resolve()[1]["inventory_limits"] == original["limits"]


def test_selected_limits_preserve_default_v1_hash_contract(capture):
    _, run = capture([{"id": "one"}], None)
    receipt = run()
    assert "limits" not in receipt
    assert inv.with_inventory_limits(receipt["target"], receipt) == receipt["target"]
    assert asdict(inv.InventoryLimits.parse()) == {
        "max_rows": 250000,
        "max_row_bytes": 8388608,
        "max_receipt_bytes": 134217728,
    }


def test_native_config_update_sets_validated_inventory_policy(tmp_path):
    from typer.testing import CliRunner

    from daylily_tapdb.cli import framework_app
    from daylily_tapdb.cli.db_config import get_db_config
    from tests.test_cli_registry_v2 import _write_config

    config = _write_config(tmp_path / "tapdb.yaml")
    args = [
        "--config",
        str(config),
        "db-config",
        "update",
        "--inventory-max-receipt-bytes",
        "536870912",
        "--inventory-max-rows",
        "1000000",
    ]
    result = CliRunner().invoke(framework_app, args)
    assert result.exit_code == 0, result.output
    limits = get_db_config(config_path=config)["inventory_limits"]
    assert limits == {
        "max_rows": 1000000,
        "max_row_bytes": 8388608,
        "max_receipt_bytes": 536870912,
    }
    before = config.read_bytes()
    invalid = CliRunner().invoke(framework_app, args[:-1] + ["0"])
    assert invalid.exit_code == 2 and config.read_bytes() == before


def test_source_contract_accepts_exact_prerelease_version(monkeypatch):
    from daylily_tapdb import sequences
    from daylily_tapdb.backup import source_contract

    monkeypatch.setattr(
        inv, "capture_identity_inventory", lambda *a, **kw: {"target": kw["target"]}
    )
    monkeypatch.setattr(sequences, "capture_sequence_inventory", lambda *a, **kw: {})
    result = source_contract.capture_source_contract(
        object(),
        schema_name="inventory",
        target=snapshot()["target"],
        source_version="10.1.1rc1",
    )
    assert result["source_version"] == "10.1.1rc1"
