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
    assert "tapdb --config <path> ..." in text
    assert "--json info" in text
    assert "lsmc-bio/meridian-registry" in text
    assert "meridian-euid domain-check Q" in text


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
    assert "--env" not in bootstrap
    assert "bootstrap local --no-gui" in bootstrap
    assert "TAPDBConnection" in python_api
    assert "TemplateManager" in python_api
    assert "InstanceFactory" in python_api


def test_activate_banner_does_not_advertise_legacy_env_selectors() -> None:
    activate = (REPO_ROOT / "activate").read_text(encoding="utf-8")

    assert "--env" not in activate
    assert "dev | test | prod" not in activate
    assert "<env>" not in activate
    assert (
        "tapdb --config ~/.config/tapdb/<client>/<database>/tapdb-config.yaml"
        in activate
    )


def test_embeddable_gui_docs_expose_v1_mount_without_dayhoff_mutation() -> None:
    integration = (REPO_ROOT / "docs" / "integration-and-embedding.md").read_text(
        encoding="utf-8"
    )
    inclusion = (REPO_ROOT / "docs" / "tapdb_gui_inclusion.md").read_text(
        encoding="utf-8"
    )

    combined = integration + "\n" + inclusion
    assert "create_tapdb_gui_app" in combined
    assert 'app.mount(\n    "/tapdb"' in combined
    assert 'config_path="/abs/path/to/tapdb-config.yaml"' in combined
    assert "Dayhoff-Style Host Example" in integration
    assert "does not require mutating a Dayhoff repo" in integration
    assert "`create_tapdb_web_app(...)` remains available" in integration
    assert "/tapdb/api/create/{template_euid}" in combined
    assert "/tapdb/api/object/{euid}/status" in combined
    assert "/tapdb/api/object/{euid}/external-links" in combined
    assert "/tapdb/api/admin/readiness" in combined
