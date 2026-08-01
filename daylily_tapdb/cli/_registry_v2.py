"""Explicit cli-core-yo v2 registration policies for TapDB."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from cli_core_yo.spec import CommandPolicy

JSON_COMMANDS: set[tuple[str | None, str]] = {
    ("validation", "assess"),
    ("validation", "revalidate"),
    ("validation", "editor-data"),
    ("repair", "create"),
    ("backup", "plan"),
    ("backup", "create"),
    ("backup", "verify"),
    ("backup", "list"),
    ("backup", "restore-plan"),
    ("backup", "restore"),
    ("backup", "rehearse"),
    # `backup health` emits JSON on stdout unconditionally, so the global
    # --json flag changes nothing about its output. It is registered here
    # anyway: without it `tapdb --json backup health` -- the correct flag
    # position -- is rejected as a contract violation, which would hand a
    # monitoring caller an unparseable error for spelling the command right.
    ("backup", "health"),
    ("backup", "prune"),
}

MUTATING_COMMANDS = {
    ("bootstrap", "local"),
    ("bootstrap", "aurora"),
    ("ui", "start"),
    ("ui", "mkcert"),
    ("ui", "stop"),
    ("ui", "restart"),
    ("db-config", "init"),
    ("db-config", "update"),
    ("db", "create"),
    ("db", "delete"),
    ("db", "setup"),
    ("db/schema", "apply"),
    ("db/schema", "reset"),
    ("db/schema", "migrate"),
    ("db/data", "restore"),
    ("db/data", "seed"),
    ("repair", "create"),
    ("pg", "start"),
    ("pg", "stop"),
    ("pg", "restart"),
    ("pg", "init"),
    ("pg", "start-local"),
    ("pg", "stop-local"),
    ("users", "add"),
    ("users", "set-role"),
    ("users", "deactivate"),
    ("users", "activate"),
    ("users", "set-password"),
    ("users", "delete"),
    ("cognito", "setup"),
    ("cognito", "setup-with-google"),
    ("cognito", "bind"),
    ("cognito", "add-app"),
    ("cognito", "edit-app"),
    ("cognito", "remove-app"),
    ("cognito", "add-google-idp"),
    ("cognito", "fix-auth-flows"),
    ("cognito", "add-user"),
    ("cognito/config", "create"),
    ("cognito/config", "update"),
    ("aurora", "create"),
    ("aurora", "delete"),
    ("backup", "create"),
    ("backup", "restore"),
    ("backup", "rehearse"),
    ("backup", "prune"),
}

INTERACTIVE_COMMANDS = {
    ("db", "delete"),
    ("db/schema", "reset"),
    ("db/data", "restore"),
    ("users", "delete"),
    ("aurora", "delete"),
    # The replacement for `db data restore` must not be less guarded than the
    # command it supersedes.
    ("backup", "restore"),
    # `backup prune` is deliberately absent. Retention is a scheduled job, and
    # a confirmation prompt in a cron path gets defeated with `yes |` -- which
    # trains the reflex that defeats every other prompt too. Its guards are the
    # typed target label, the dry-run default, and the delete ceiling, none of
    # which a pipe can satisfy.
}

#: Commands that honour the framework-level ``--dry-run``.
#:
#: **Must stay a subset of MUTATING_COMMANDS.** ``CommandPolicy.__post_init__``
#: raises when ``supports_dry_run`` is set without ``mutates_state``, and
#: policies are built during registration -- so a violation breaks the whole
#: CLI at import time, not when the offending command is invoked. Read-only
#: commands (plan, restore-plan, verify, list) have nothing to simulate and
#: must never appear here.
DRY_RUN_COMMANDS = {
    ("backup", "create"),
    ("backup", "restore"),
    ("backup", "rehearse"),
    ("backup", "prune"),
}

LONG_RUNNING_COMMANDS = {
    ("bootstrap", "local"),
    ("bootstrap", "aurora"),
    ("ui", "start"),
    ("ui", "logs"),
    ("ui", "restart"),
    ("pg", "logs"),
    ("pg", "start"),
    ("pg", "stop"),
    ("pg", "restart"),
    ("pg", "start-local"),
    ("pg", "stop-local"),
    ("aurora", "create"),
    ("aurora", "delete"),
    ("aurora", "connect"),
    ("backup", "create"),
    ("backup", "restore"),
    ("backup", "rehearse"),
    ("backup", "prune"),
}


def help_text(callback: Callable[..., Any]) -> str:
    """Return deterministic CLI help text from the callback docstring."""
    return inspect.getdoc(callback) or ""


def policy_for_command(group_path: str | None, name: str) -> CommandPolicy:
    """Return the v2 command policy for one registered TapDB command."""
    key = (group_path, name)
    return CommandPolicy(
        supports_json=key in JSON_COMMANDS,
        mutates_state=key in MUTATING_COMMANDS or key in INTERACTIVE_COMMANDS,
        interactive=key in INTERACTIVE_COMMANDS,
        long_running=key in LONG_RUNNING_COMMANDS,
        supports_dry_run=key in DRY_RUN_COMMANDS,
    )
