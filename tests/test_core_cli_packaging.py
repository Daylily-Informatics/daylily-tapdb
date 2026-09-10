"""Core CLI import isolation and unchanged optional web export contracts."""

import importlib
import subprocess
import sys
from pathlib import Path

import pytest

_CORE_IMPORT_PRELUDE = """
import importlib.abc
import sys

class NoWebExtras(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'fastapi', 'starlette', 'jinja2', 'uvicorn'}:
            raise ModuleNotFoundError('Optional web dependency requested: ' + fullname)
        return None

sys.meta_path.insert(0, NoWebExtras())
"""


@pytest.mark.parametrize(
    "arguments",
    [
        ["--help"],
        ["db", "identity", "--help"],
        ["db", "sequences", "--help"],
        ["db", "runtime-principal", "--help"],
        ["validation", "--help"],
        ["repair", "--help"],
        ["backup", "--help"],
    ],
)
def test_core_cli_help_does_not_import_web_extras(arguments):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _CORE_IMPORT_PRELUDE + "\nfrom daylily_tapdb.cli import main\nmain()\n",
            *arguments,
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Usage:" in result.stdout


def test_missing_web_dependency_still_fails_when_web_surface_is_requested():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _CORE_IMPORT_PRELUDE
            + "\nfrom daylily_tapdb.web import mount_tapdb_dag_surfaces\n",
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "Optional web dependency requested: fastapi" in result.stderr


@pytest.mark.parametrize(
    ("module_name", "export"),
    [
        ("bridge", "TapdbHostBridge"),
        ("bridge", "TapdbHostNavLink"),
        ("dag_v2", "DAG_V2_CONTRACT"),
        ("dag_v2", "DAG_V2_EXTENSION"),
        ("dag_v2", "DagV2EligibilityReason"),
        ("dag_v2", "DagV2Limits"),
        ("dag_v2", "DagV2Manifest"),
        ("dag_v2", "DagV2MountResult"),
        ("dag_v2", "mount_tapdb_dag_surfaces"),
        ("dag_v2", "validate_dag_v2_manifest"),
        ("gui", "create_tapdb_gui_app"),
        ("gui", "create_tapdb_gui_router"),
    ],
)
def test_public_web_exports_are_the_owning_implementation(module_name, export):
    import daylily_tapdb.web as web

    path = (
        "daylily_tapdb.gui"
        if module_name == "gui"
        else f"daylily_tapdb.web.{module_name}"
    )
    assert export in web.__all__
    assert getattr(web, export) is getattr(importlib.import_module(path), export)


def test_unknown_web_export_fails_explicitly():
    import daylily_tapdb.web as web

    with pytest.raises(AttributeError, match="unknown_export"):
        getattr(web, "unknown_export")
