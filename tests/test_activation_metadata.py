"""Static contract tests for the TAPDB activation script."""

from __future__ import annotations

import builtins
import tomllib
from pathlib import Path

import pytest


def test_pyproject_pins_published_cli_core_yo() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    dependencies = data["project"]["dependencies"]
    dev_dependencies = data["project"]["optional-dependencies"]["dev"]
    admin_dependencies = data["project"]["optional-dependencies"]["admin"]

    assert "cli-core-yo==2.1.1" in dependencies
    assert "pyyaml" in dependencies
    assert "cli-core-yo==2.1.1" in dev_dependencies
    assert "daylily-auth-cognito==2.1.5" in admin_dependencies
    assert all("daylily-cognito" not in dependency for dependency in admin_dependencies)


def test_activate_uses_published_cli_core_yo_metadata_check() -> None:
    script = Path(__file__).resolve().parents[1] / "activate"
    text = script.read_text(encoding="utf-8")

    assert "Editable project location" in text
    assert "daylily-tapdb" in text
    assert "_tapdb_distribution_is_published" in text
    assert "_tapdb_module_is_from_repo" not in text
    assert "--smoke" in text
    assert 'python -m pip install -e ".[cli,admin,aurora,dev]"' in text
    assert '_tapdb_cli_core_yo_version="2.1.1"' in text
    assert "cli-core-yo==${_tapdb_cli_core_yo_version}" in text
    assert "cli-core-yo is not installed as published" in text


def test_package_init_has_source_checkout_version_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package_init = Path(__file__).resolve().parents[1] / "daylily_tapdb" / "__init__.py"
    real_import = builtins.__import__

    def _import_without_generated_version(
        name, globals=None, locals=None, fromlist=(), level=0
    ):
        if name == "daylily_tapdb._version":
            raise ImportError("setuptools-scm version module is absent")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _import_without_generated_version)
    namespace = {
        "__name__": "daylily_tapdb_init_fallback_probe",
        "__package__": "daylily_tapdb",
    }
    exec(
        compile(package_init.read_text(encoding="utf-8"), package_init, "exec"),
        namespace,
    )

    assert namespace["__version__"] == "0.0.0.dev0"
