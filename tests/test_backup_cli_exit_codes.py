"""Exit codes, asserted through the real ``tapdb`` entry point.

Every other CLI test drives the app with ``CliRunner``. That is fast and right
for behaviour, but it cannot see the bug this file exists for.

``main()`` calls ``cli_core_yo.app.run``, which invokes the app with
``standalone_mode=False``. In that mode click *returns* the code carried by a
``click.exceptions.Exit`` -- which is what ``typer.Exit`` raises -- rather than
raising it, and ``run`` discards that return value and reports ``0``. So the
commands below exited ``1`` under ``CliRunner`` and ``0`` from the shell, and
four passing tests said the contract held.

A monitoring job would have been told a corrupt backup was fine.

These tests therefore spawn the CLI as a subprocess -- the same path an
operator or a cron entry takes -- and assert the process exit status. Anything
that reintroduces ``typer.Exit`` in ``cli/backup.py``, or that changes how
``main()`` propagates codes, fails here.

The contract:

* ``0`` succeeded
* ``1`` ran and found a problem -- page on this
* ``2`` could not run at all -- a broken runner, not a broken backup
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from daylily_tapdb.cli import app
from daylily_tapdb.cli.backup import EXIT_ERROR, EXIT_FINDINGS, EXIT_OK
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context

REPO_ROOT = Path(__file__).resolve().parents[1]
runner = CliRunner()

pytestmark = pytest.mark.skipif(
    not shutil.which("pg_dump") or not shutil.which("pg_restore"),
    reason="pg_dump/pg_restore not on PATH",
)


def _cli(config_path, *args, home) -> subprocess.CompletedProcess:
    """Run the CLI the way a shell does, and return the completed process."""
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "daylily_tapdb.cli",
            "--config",
            str(config_path),
            *args,
        ],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env={
            "PATH": __import__("os").environ.get("PATH", ""),
            "HOME": str(home),
            "USER": __import__("os").environ.get("USER", "pytest"),
        },
        timeout=300,
    )


@pytest.fixture(scope="module")
def prepared(pg_instance):
    """A seeded target with one good backup, set up through the CLI itself."""
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    config = str(pg_instance["config_path"])
    applied = runner.invoke(app, ["--config", config, "db", "schema", "apply"])
    assert applied.exit_code == 0, applied.output
    seeded = runner.invoke(
        app, ["--config", config, "db", "data", "seed", "--skip-existing"]
    )
    assert seeded.exit_code == 0, seeded.output

    home = pg_instance["base"]
    created = _cli(config, "backup", "create", home=home)
    assert created.returncode == EXIT_OK, created.stdout + created.stderr

    listed = _cli(config, "backup", "list", home=home)
    assert listed.returncode == EXIT_OK, listed.stdout + listed.stderr
    backup_id = None
    for token in listed.stdout.split():
        if token.startswith("full-"):
            backup_id = token
            break
    assert backup_id, f"could not find a backup id in:\n{listed.stdout}"

    clear_cli_context()
    return {"config": config, "home": home, "backup_id": backup_id}


def test_the_harness_reaches_the_cli_at_all(prepared):
    """Guards the guard: if the subprocess never ran, everything below is
    meaningless."""
    result = _cli(prepared["config"], "backup", "--help", home=prepared["home"])

    assert result.returncode == EXIT_OK
    assert "rehearse" in result.stdout


def test_success_exits_zero(prepared):
    for args in (["backup", "plan"], ["backup", "list"]):
        result = _cli(prepared["config"], *args, home=prepared["home"])
        assert result.returncode == EXIT_OK, (
            f"tapdb {' '.join(args)} -> {result.returncode}\n"
            f"{result.stdout}{result.stderr}"
        )


def test_verifying_a_good_backup_exits_zero(prepared):
    result = _cli(
        prepared["config"],
        "backup",
        "verify",
        "--backup-id",
        prepared["backup_id"],
        home=prepared["home"],
    )

    assert result.returncode == EXIT_OK, result.stdout + result.stderr


def test_an_unknown_backup_exits_one_not_zero(prepared):
    """The regression that started this file.

    This reported 0 from the shell while `CliRunner` reported 1.
    """
    result = _cli(
        prepared["config"],
        "backup",
        "verify",
        "--backup-id",
        "does-not-exist",
        home=prepared["home"],
    )

    assert result.returncode == EXIT_FINDINGS, (
        f"expected 1 (a finding), got {result.returncode} -- a monitoring job "
        f"would treat this as success\n{result.stdout}{result.stderr}"
    )


def test_a_refused_in_place_restore_exits_one_not_zero(prepared):
    """A refused destructive operation must not look like a success."""
    result = _cli(
        prepared["config"],
        "backup",
        "restore",
        "--backup-id",
        prepared["backup_id"],
        "--mode",
        "in-place",
        home=prepared["home"],
    )

    assert result.returncode == EXIT_FINDINGS, (
        f"expected 1, got {result.returncode}\n{result.stdout}{result.stderr}"
    )


def test_a_wrong_confirmation_label_exits_one_not_zero(prepared):
    result = _cli(
        prepared["config"],
        "backup",
        "restore",
        "--backup-id",
        prepared["backup_id"],
        "--mode",
        "in-place",
        "--confirm-target",
        "definitely/not/the@label",
        home=prepared["home"],
    )

    assert result.returncode == EXIT_FINDINGS, (
        f"expected 1, got {result.returncode}\n{result.stdout}{result.stderr}"
    )


def test_a_bad_argument_exits_two(prepared):
    """Could-not-run is distinct from ran-and-found-a-problem."""
    result = _cli(
        prepared["config"],
        "backup",
        "list",
        "--class",
        "nonsense",
        home=prepared["home"],
    )

    assert result.returncode == EXIT_ERROR, (
        f"expected 2, got {result.returncode}\n{result.stdout}{result.stderr}"
    )


def test_a_missing_config_exits_two(tmp_path, prepared):
    result = _cli(tmp_path / "absent.yaml", "backup", "list", home=prepared["home"])

    assert result.returncode == EXIT_ERROR, (
        f"expected 2, got {result.returncode}\n{result.stdout}{result.stderr}"
    )


def test_the_backup_cli_never_uses_typer_exit():
    """The mechanism, pinned directly.

    ``typer.Exit`` is swallowed by the real entry point. Reintroducing it would
    make every assertion above fail, but it would fail with a confusing "got 0"
    rather than naming the cause -- so name it here too.
    """
    source = (REPO_ROOT / "daylily_tapdb" / "cli" / "backup.py").read_text()

    import ast

    offenders = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
            target = node.exc.func
            name = getattr(target, "attr", getattr(target, "id", ""))
            if name == "Exit":
                offenders.append(node.lineno)

    assert offenders == [], (
        f"typer.Exit at line(s) {offenders}: the real entry point discards its "
        "code. Use _exit() instead."
    )
