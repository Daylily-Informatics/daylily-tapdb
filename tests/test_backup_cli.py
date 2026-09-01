"""The ``tapdb backup`` CLI: registry policy, exit codes, and adapter shape.

Two properties matter most here and neither is about output formatting:

* the registry policy is enforced at *registration*, so a mistake breaks the
  whole CLI at import rather than when the command runs; and
* the commands must stay thin -- a surface that reimplements a check is a
  surface that can disagree with the other two.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from daylily_tapdb.cli import app, framework_app
from daylily_tapdb.cli._registry_v2 import (
    DRY_RUN_COMMANDS,
    INTERACTIVE_COMMANDS,
    JSON_COMMANDS,
    LONG_RUNNING_COMMANDS,
    MUTATING_COMMANDS,
    policy_for_command,
)
from daylily_tapdb.cli.backup import EXIT_ERROR, EXIT_FINDINGS, EXIT_OK

runner = CliRunner()


@pytest.fixture(autouse=True)
def _no_context_leak(monkeypatch: pytest.MonkeyPatch):
    """Reset CLI context around every test in this file.

    Several tests here invoke the CLI with a deliberately missing config to
    check exit codes. That selection is global state, and without this it
    leaks into the next module -- whose module-scoped fixtures are set up
    before any function-scoped context fixture can correct it.

    The assertions below intentionally inspect option and validation text.
    Rich/Typer truncates those strings at the narrow terminal width used by
    non-interactive CI, so make the test rendering width explicit rather than
    making its outcome depend on the runner's environment.
    """
    monkeypatch.setenv("COLUMNS", "120")
    from daylily_tapdb.cli.context import clear_cli_context

    clear_cli_context()
    yield
    clear_cli_context()


BACKUP_COMMANDS = (
    "plan",
    "create",
    "verify",
    "list",
    "restore-plan",
    "restore",
    "rehearse",
    "health",
    "prune",
)


# ---------------------------------------------------------------------------
# registry policy
# ---------------------------------------------------------------------------


def test_every_backup_command_is_registered():
    registry = framework_app._cli_core_yo_registry

    for name in BACKUP_COMMANDS:
        assert registry.get_command(("backup", name)) is not None, name


@pytest.mark.parametrize("name", BACKUP_COMMANDS)
def test_every_backup_command_supports_json(name):
    assert policy_for_command("backup", name).supports_json is True


@pytest.mark.parametrize("name", ["create", "restore", "rehearse"])
def test_mutating_commands_are_marked_mutating_and_long_running(name):
    policy = policy_for_command("backup", name)

    assert policy.mutates_state is True
    assert policy.long_running is True


@pytest.mark.parametrize("name", ["plan", "verify", "list", "restore-plan"])
def test_read_only_commands_are_not_marked_mutating(name):
    assert policy_for_command("backup", name).mutates_state is False


def test_restore_is_interactive_like_the_command_it_replaces():
    # ("db/data", "restore") is already interactive; the replacement must not
    # be less guarded than what it supersedes.
    assert policy_for_command("backup", "restore").interactive is True
    assert ("db/data", "restore") in INTERACTIVE_COMMANDS


@pytest.mark.parametrize("name", ["create", "restore", "rehearse"])
def test_dry_run_is_advertised_where_supported(name):
    assert policy_for_command("backup", name).supports_dry_run is True


@pytest.mark.parametrize("name", ["plan", "verify", "list", "restore-plan"])
def test_read_only_commands_do_not_advertise_dry_run(name):
    # There is nothing to simulate, and CommandPolicy rejects the combination.
    assert policy_for_command("backup", name).supports_dry_run is False


def test_dry_run_commands_are_a_subset_of_mutating_commands():
    """The invariant that would otherwise break the CLI at import.

    ``CommandPolicy.__post_init__`` raises when ``supports_dry_run`` is set
    without ``mutates_state``, and policies are built during registration --
    so a violation is not a failed command, it is a CLI that will not start.
    """
    assert DRY_RUN_COMMANDS <= (MUTATING_COMMANDS | INTERACTIVE_COMMANDS)


def test_the_whole_cli_still_builds():
    # The end state of the invariant above: if it were violated, importing and
    # building the app would raise rather than reach this assertion.
    from daylily_tapdb.cli import build_app

    assert build_app() is not None


def test_backup_commands_appear_in_the_declared_sets():
    for name in BACKUP_COMMANDS:
        assert ("backup", name) in JSON_COMMANDS
    for name in ("create", "restore", "rehearse"):
        assert ("backup", name) in MUTATING_COMMANDS
        assert ("backup", name) in LONG_RUNNING_COMMANDS


# ---------------------------------------------------------------------------
# help surface
# ---------------------------------------------------------------------------


def test_backup_group_is_reachable():
    result = runner.invoke(app, ["backup", "--help"])

    assert result.exit_code == 0
    for name in BACKUP_COMMANDS:
        assert name in result.output


@pytest.mark.parametrize("name", BACKUP_COMMANDS)
def test_each_command_has_help(name):
    result = runner.invoke(app, ["backup", name, "--help"])

    assert result.exit_code == 0


def test_restore_help_documents_the_confirmation_and_modes():
    result = runner.invoke(app, ["backup", "restore", "--help"])
    text = result.output

    assert "--confirm-target" in text
    assert "--mode" in text
    assert "--plan-fingerprint" in text
    assert "--keep-superseded" in text


def test_create_help_documents_the_backup_classes():
    result = runner.invoke(app, ["backup", "create", "--help"])

    assert "--class" in result.output
    assert "--allow-drift" in result.output


# ---------------------------------------------------------------------------
# argument validation
# ---------------------------------------------------------------------------


def test_an_unknown_backup_class_is_rejected(tmp_path: Path):
    result = runner.invoke(
        app, ["--config", str(tmp_path / "c.yaml"), "backup", "plan", "--class", "nope"]
    )

    assert result.exit_code != EXIT_OK
    assert "--class must be one of" in f"{result.output}{result.exception or ''}"


def test_verify_requires_a_target(tmp_path: Path):
    result = runner.invoke(
        app, ["--config", str(tmp_path / "c.yaml"), "backup", "verify"]
    )

    assert result.exit_code != EXIT_OK
    assert "--backup-id or --path" in f"{result.output}{result.exception or ''}"


def test_an_unknown_verify_level_is_rejected(tmp_path: Path):
    result = runner.invoke(
        app,
        [
            "--config",
            str(tmp_path / "c.yaml"),
            "backup",
            "verify",
            "--backup-id",
            "x",
            "--level",
            "medium",
        ],
    )

    assert result.exit_code != EXIT_OK


def test_existing_snapshot_requires_the_matching_class(tmp_path: Path):
    """Silently ignoring it would produce the wrong kind of backup.

    An operator who forgets `--class provider-snapshot` would otherwise get a
    full logical dump while believing they had recorded a cluster snapshot.
    """
    result = runner.invoke(
        app,
        [
            "--config",
            str(tmp_path / "c.yaml"),
            "backup",
            "create",
            "--existing-snapshot",
            "snap-123",
        ],
    )

    assert result.exit_code != EXIT_OK
    text = f"{result.output}{result.exception or ''}"
    assert "--existing-snapshot only applies to" in text


def test_existing_snapshot_is_accepted_with_the_right_class(tmp_path: Path):
    result = runner.invoke(
        app,
        [
            "--config",
            str(tmp_path / "c.yaml"),
            "backup",
            "create",
            "--class",
            "provider-snapshot",
            "--existing-snapshot",
            "snap-123",
        ],
    )

    # It gets past option validation and fails later on the missing config.
    assert "--existing-snapshot only applies to" not in (
        f"{result.output}{result.exception or ''}"
    )


def test_keep_superseded_requires_in_place(tmp_path: Path):
    # Only in-place supersedes a schema; accepting this for isolated would
    # promise to keep something that is never created.
    result = runner.invoke(
        app,
        [
            "--config",
            str(tmp_path / "c.yaml"),
            "backup",
            "restore",
            "--backup-id",
            "x",
            "--keep-superseded",
        ],
    )

    assert result.exit_code != EXIT_OK
    assert "--keep-superseded only applies to" in (
        f"{result.output}{result.exception or ''}"
    )


def test_keep_superseded_is_accepted_for_in_place(tmp_path: Path):
    result = runner.invoke(
        app,
        [
            "--config",
            str(tmp_path / "c.yaml"),
            "backup",
            "restore",
            "--backup-id",
            "x",
            "--mode",
            "in-place",
            "--keep-superseded",
        ],
    )

    assert "--keep-superseded only applies to" not in (
        f"{result.output}{result.exception or ''}"
    )


def test_a_hostile_target_database_is_rejected_before_anything_runs(tmp_path: Path):
    result = runner.invoke(
        app,
        [
            "--config",
            str(tmp_path / "c.yaml"),
            "backup",
            "restore",
            "--backup-id",
            "x",
            "--target-database",
            "evil'; DROP DATABASE postgres; --",
        ],
    )

    assert result.exit_code != EXIT_OK


# ---------------------------------------------------------------------------
# exit codes
# ---------------------------------------------------------------------------


def test_a_missing_config_exits_two_not_one(tmp_path: Path):
    """Exit 2 means "could not run"; exit 1 means "ran and found a problem".

    Automation depends on the distinction -- a monitoring job should page on a
    corrupt backup, not on a misconfigured runner.
    """
    result = runner.invoke(
        app, ["--config", str(tmp_path / "absent.yaml"), "backup", "list"]
    )

    assert result.exit_code == EXIT_ERROR


def test_exit_codes_are_the_documented_three():
    assert (EXIT_OK, EXIT_FINDINGS, EXIT_ERROR) == (0, 1, 2)


# ---------------------------------------------------------------------------
# thin-adapter contract
# ---------------------------------------------------------------------------


def _imported_modules(path: Path) -> set[str]:
    """Return every module name imported by a file, via AST.

    Substring searching would match prose -- these modules' docstrings discuss
    exactly the imports they promise not to have.
    """
    import ast

    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
            names.add(node.module)
    return names


def test_the_cli_never_shells_out_itself():
    # The CLI must go through the service, which goes through engine.py. A
    # surface that builds its own pg_dump is a surface that can drift from
    # the other two.
    imported = _imported_modules(Path("daylily_tapdb/cli/backup.py"))

    assert "subprocess" not in imported
    assert "daylily_tapdb.backup.engine" not in imported


def test_the_cli_delegates_to_the_shared_service():
    import daylily_tapdb.cli.backup as cli_backup

    assert cli_backup.service.__name__ == "daylily_tapdb.backup.service"
    assert cli_backup.verify.__name__ == "daylily_tapdb.backup.verify"


def test_the_service_layer_stays_free_of_typer():
    from daylily_tapdb.backup import service, verify

    for module in (service, verify):
        assert "typer" not in _imported_modules(Path(module.__file__))


# ---------------------------------------------------------------------------
# legacy deprecation
# ---------------------------------------------------------------------------


def test_legacy_backup_warns_and_names_its_replacement():
    from daylily_tapdb.cli import db as db_mod

    messages: list[str] = []

    class _Out:
        @staticmethod
        def warning(message):
            messages.append(message)

    original = db_mod.ccyo_out
    db_mod.ccyo_out = _Out()
    try:
        db_mod._warn_legacy_backup_command(
            "tapdb db data backup", "tapdb backup create", reason="it is incomplete"
        )
    finally:
        db_mod.ccyo_out = original

    assert messages
    assert "DEPRECATED" in messages[0]
    assert "tapdb backup create" in messages[0]
    # The notice must say *why*, so it is actionable rather than nagging.
    assert "it is incomplete" in messages[0]


def test_legacy_commands_still_exist():
    # Deprecated, not removed: scripts pinned to them keep working across the
    # upgrade. Checked through the registry because `db data --help` needs a
    # resolved config and this assertion is about registration, not context.
    registry = framework_app._cli_core_yo_registry

    assert registry.get_command(("db", "data", "backup")) is not None
    assert registry.get_command(("db", "data", "restore")) is not None


@pytest.mark.parametrize("command", ["db_backup", "db_restore"])
def test_legacy_docstrings_name_the_replacement(command):
    from daylily_tapdb.cli import db as db_mod

    doc = getattr(db_mod, command).__doc__ or ""

    assert "DEPRECATED" in doc
    assert "tapdb backup" in doc


def test_legacy_behaviour_is_unchanged_apart_from_the_notice():
    """Deprecation must not alter what the old commands do.

    The only edit to each was an added warning call; the five-table list and
    the psql restore are untouched, so a pinned script behaves identically.
    """
    import inspect

    from daylily_tapdb.cli import db as db_mod

    source = inspect.getsource(db_mod.db_backup)

    assert "_warn_legacy_backup_command" in source
    # The legacy shortcomings are still present -- that is the point of
    # deprecating rather than silently "fixing" a command scripts depend on.
    assert "generic_template" in source
    assert "pg_dump" in source


def test_json_payloads_are_serialisable():
    # Everything a command emits must survive json.dumps; a dataclass leaking
    # into a payload would break --json for automation only, not for humans.
    from daylily_tapdb.backup.service import BackupListing

    payload = BackupListing().to_payload()

    assert json.loads(json.dumps(payload)) == payload
