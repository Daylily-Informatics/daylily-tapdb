"""Explicit cli-core-yo v2 registration policies for TapDB."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from cli_core_yo.spec import CommandPolicy

JSON_COMMANDS: set[tuple[str | None, str]] = set()

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
}

INTERACTIVE_COMMANDS = {
    ("db", "delete"),
    ("db/schema", "reset"),
    ("db/data", "restore"),
    ("users", "delete"),
    ("aurora", "delete"),
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
    )
