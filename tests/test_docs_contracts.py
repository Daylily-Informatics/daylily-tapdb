"""Static contracts that keep README command maps aligned with example files."""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_exists_and_points_to_examples() -> None:
    readme = REPO_ROOT / "README.md"
    assert readme.exists(), "README.md should be present at the repo root."

    text = readme.read_text(encoding="utf-8")
    assert "examples/readme/00_smoke.sh" in text
    assert "examples/readme/10_bootstrap_local.sh" in text
    assert "examples/readme/20_python_api.py" in text
    assert "source ./activate" in text
    assert "tapdb --config <path> --env <name>" in text


def test_examples_contain_the_canonical_commands() -> None:
    smoke = (REPO_ROOT / "examples" / "readme" / "00_smoke.sh").read_text(
        encoding="utf-8"
    )
    bootstrap = (REPO_ROOT / "examples" / "readme" / "10_bootstrap_local.sh").read_text(
        encoding="utf-8"
    )
    python_api = (REPO_ROOT / "examples" / "readme" / "20_python_api.py").read_text(
        encoding="utf-8"
    )

    assert "tapdb --help" in smoke
    assert "config init" in bootstrap
    assert "bootstrap local --no-gui" in bootstrap
    assert "TAPDBConnection" in python_api
    assert "TemplateManager" in python_api
    assert "InstanceFactory" in python_api
