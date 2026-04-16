"""Static contract tests for the TAPDB activation script."""

from __future__ import annotations

import tomllib
from pathlib import Path


def test_pyproject_pins_published_cli_core_yo() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    dependencies = data["project"]["dependencies"]
    dev_dependencies = data["project"]["optional-dependencies"]["dev"]

    assert "cli-core-yo==2.1.0" in dependencies
    assert "cli-core-yo==2.1.0" in dev_dependencies


def test_activate_uses_published_cli_core_yo_metadata_check() -> None:
    script = Path(__file__).resolve().parents[1] / "activate"
    text = script.read_text(encoding="utf-8")

    assert "Editable project location" in text
    assert "daylily-tapdb" in text
    assert "_tapdb_distribution_is_published" in text
    assert "_tapdb_module_is_from_repo" not in text
    assert "--smoke" in text
    assert 'python -m pip install -e ".[cli,admin,aurora,dev]"' in text
    assert '_tapdb_cli_core_yo_version="2.1.0"' in text
    assert 'cli-core-yo==${_tapdb_cli_core_yo_version}' in text
    assert "cli-core-yo is not installed as published" in text
