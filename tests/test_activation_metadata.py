"""Static contract tests for the TAPDB activation script."""

from pathlib import Path


def test_activate_uses_editable_metadata_check():
    script = Path(__file__).resolve().parents[1] / "activate"
    text = script.read_text(encoding="utf-8")

    assert "Editable project location" in text
    assert "daylily-tapdb" in text
    assert "_tapdb_module_is_from_repo" not in text
    assert "--smoke" in text
    assert 'python -m pip install -e ".[cli,admin,aurora,dev]"' in text
    assert "tapdb is not installed editable from" in text
