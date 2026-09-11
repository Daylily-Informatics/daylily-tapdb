"""Global and command-local drift JSON produce one payload and useful exits."""

import json

import pytest
from cli_core_yo.app import run

from daylily_tapdb.cli import db, spec
from tests.test_cli_registry_v2 import _write_config


@pytest.mark.parametrize("global_flag", [False, True])
@pytest.mark.parametrize(
    "state,code", [("clean", 0), ("drift", 1), ("missing", 2), ("error", 2)]
)
def test_drift_json_flags_and_exit_codes(
    monkeypatch, tmp_path, capsys, global_flag, state, code
):
    cfg = _write_config(tmp_path / "tapdb.yaml")
    monkeypatch.setattr(db, "_check_db_exists", lambda *a: state != "missing")

    def inspect(*a, **k):
        if state == "error":
            raise RuntimeError("diagnostic failure")
        return {"status": state, "strict": k["strict"]}, state == "drift"

    monkeypatch.setattr(db, "_run_schema_drift_check", inspect)
    args = (
        ["--config", str(cfg)]
        + (["--json"] if global_flag else [])
        + ["db", "schema", "drift-check", "--strict"]
        + ([] if global_flag else ["--json"])
    )
    exit_code = run(spec, args)
    output = capsys.readouterr().out
    assert exit_code == code, output
    payload = json.loads(output)
    assert payload["status"] == ("error" if code == 2 else state)
    assert payload["strict"] is True
