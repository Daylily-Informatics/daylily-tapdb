from __future__ import annotations

from pathlib import Path

from daylily_tapdb.cli import objects as objects_cli
from daylily_tapdb.cli import templates as templates_cli
from daylily_tapdb.cli._registry_v2 import policy_for_command
from daylily_tapdb.templates.repository import RepositoryImportResult


class _Scope:
    def __init__(self, session: object) -> None:
        self.session = session

    def __enter__(self) -> object:
        return self.session

    def __exit__(self, *_args: object) -> bool:
        return False


class _Connection:
    def __init__(self) -> None:
        self.commits: list[bool] = []

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def session_scope(self, *, commit: bool = False) -> _Scope:
        self.commits.append(commit)
        return _Scope(object())


def test_template_command_policies_match_file_and_database_effects() -> None:
    export = policy_for_command("templates", "export")
    import_ = policy_for_command("templates", "import")
    inventory = policy_for_command("templates", "inventory")

    assert export.supports_json is True
    assert export.mutates_state is True
    assert export.supports_dry_run is False
    assert import_.supports_json is True
    assert import_.mutates_state is True
    assert import_.supports_dry_run is True
    assert inventory.supports_json is True
    assert inventory.mutates_state is False
    assert inventory.supports_dry_run is False


def test_object_command_policies_match_read_and_dry_run_mutation_effects() -> None:
    for command in ("search", "get"):
        policy = policy_for_command("objects", command)
        assert policy.supports_json is True
        assert policy.mutates_state is False
        assert policy.supports_dry_run is False

    for command in ("update", "repair", "delete"):
        policy = policy_for_command("objects", command)
        assert policy.supports_json is True
        assert policy.mutates_state is True
        assert policy.supports_dry_run is True


def test_framework_dry_run_vetoes_explicit_apply_for_new_mutation_commands(
    monkeypatch,
) -> None:
    template_connection = _Connection()
    template_calls: list[bool] = []
    monkeypatch.setattr(templates_cli, "_dry_run_requested", lambda: True)
    monkeypatch.setattr(
        templates_cli,
        "get_db_config",
        lambda: {
            "domain_code": "Z",
            "owner_repo_name": "owner",
            "domain_registry_path": "/abs/domains.json",
            "prefix_ownership_registry_path": "/abs/prefixes.json",
        },
    )
    monkeypatch.setattr(
        templates_cli,
        "_tapdb_connection_for_env",
        lambda *_args, **_kwargs: template_connection,
    )
    monkeypatch.setattr(
        templates_cli,
        "import_repository_pack",
        lambda *_args, **kwargs: (
            template_calls.append(kwargs["dry_run"])
            or RepositoryImportResult(True, 1, 0, 0, ("ASY",), "checksum")
        ),
    )
    monkeypatch.setattr(templates_cli, "_emit", lambda _payload: None)

    templates_cli.templates_import(
        Path("/abs/templates.json"), apply=True, actor="operator"
    )

    object_connection = _Connection()
    object_calls: list[bool] = []
    monkeypatch.setattr(objects_cli, "_dry_run_requested", lambda: True)
    monkeypatch.setattr(objects_cli, "_connection", lambda _actor: object_connection)
    monkeypatch.setattr(
        objects_cli,
        "update_object",
        lambda *_args, **kwargs: (
            object_calls.append(kwargs["dry_run"]) or {"dry_run": True}
        ),
    )
    monkeypatch.setattr(objects_cli, "_emit", lambda _payload: None)

    objects_cli.objects_update(
        set_values=["name=updated"],
        euid="stored-object",
        machine_uuid="",
        uid=None,
        record_type="instance",
        apply=True,
        actor="operator",
    )

    assert template_connection.commits == [False]
    assert template_calls == [True]
    assert object_connection.commits == [False]
    assert object_calls == [True]
