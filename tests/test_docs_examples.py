"""Runnable example checks for the documentation scripts."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = REPO_ROOT / "examples" / "readme"


def _run_bash(
    script: Path, *, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(script)],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _docs_local_enabled() -> bool:
    return os.environ.get("TAPDB_RUN_DOCS_LOCAL", "").strip() == "1"


def _pg_tools_available() -> bool:
    return all(
        shutil.which(tool)
        for tool in ("pg_ctl", "initdb", "createdb", "psql", "pg_isready")
    )


def test_readme_smoke_script_runs() -> None:
    result = _run_bash(EXAMPLES_DIR / "00_smoke.sh", env=os.environ.copy())
    assert result.returncode == 0, result.stderr or result.stdout
    assert "Smoke example completed." in result.stdout


@pytest.fixture(scope="module")
def docs_local_runtime(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    if not _docs_local_enabled():
        pytest.skip("Set TAPDB_RUN_DOCS_LOCAL=1 to run local docs runtime examples.")
    if not _pg_tools_available():
        pytest.skip("PostgreSQL binaries are required for local docs runtime examples.")

    workdir = tmp_path_factory.mktemp("tapdb_docs_runtime")
    env = os.environ.copy()
    env["TAPDB_DOCS_WORKDIR"] = str(workdir)
    env["TAPDB_DOCS_CLIENT_ID"] = "docs"
    env["TAPDB_DOCS_DATABASE_NAME"] = "demo"
    env["TAPDB_DOCS_EUID_CLIENT_CODE"] = "C"
    env["TAPDB_DOCS_DB_PORT"] = str(_free_port())
    env["TAPDB_DOCS_UI_PORT"] = str(_free_port())

    result = _run_bash(EXAMPLES_DIR / "10_bootstrap_local.sh", env=env)
    assert result.returncode == 0, result.stderr or result.stdout

    config_path = (
        Path(env["TAPDB_DOCS_WORKDIR"])
        / ".config"
        / "tapdb"
        / env["TAPDB_DOCS_CLIENT_ID"]
        / env["TAPDB_DOCS_DATABASE_NAME"]
        / "tapdb-config.yaml"
    )

    yield {
        "env": env,
        "config_path": config_path,
        "stdout": result.stdout,
    }

    stop_cmd = (
        "source ./activate >/dev/null 2>&1 && "
        f"tapdb --config '{config_path}' --env dev pg stop-local dev"
    )
    subprocess.run(
        ["bash", "-lc", stop_cmd],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_readme_bootstrap_local_script_runs(
    docs_local_runtime: dict[str, object],
) -> None:
    config_path = docs_local_runtime["config_path"]
    stdout = str(docs_local_runtime["stdout"])

    assert isinstance(config_path, Path)
    assert config_path.exists()
    assert "Local bootstrap complete" in stdout
    assert "Bootstrap example completed." in stdout


def test_readme_python_api_example_runs(docs_local_runtime: dict[str, object]) -> None:
    env = dict(docs_local_runtime["env"])
    result = subprocess.run(
        ["python", str(EXAMPLES_DIR / "20_python_api.py")],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout

    payload = json.loads(result.stdout)
    assert payload["template_code"] == "generic/generic/generic/1.0/"
    assert payload["instance_euid"]
    assert payload["domain_code"]
    assert payload["issuer_app_code"]
