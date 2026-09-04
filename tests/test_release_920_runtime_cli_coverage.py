"""Behavior coverage for small runtime CLI surfaces used by the 9.2 release."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer

import daylily_tapdb.cli.admin_server as admin_server
import daylily_tapdb.cli.validation as validation_cli


class _Connection:
    def __init__(self) -> None:
        self.app_username = ""
        self.commits: list[bool] = []
        self.session = object()

    @contextmanager
    def session_scope(self, *, commit: bool):
        self.commits.append(commit)
        yield self.session


class _DatabaseContext:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __enter__(self) -> _Connection:
        return self.connection

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False


def _install_validation_database(monkeypatch: pytest.MonkeyPatch) -> _Connection:
    connection = _Connection()
    monkeypatch.setattr(validation_cli, "get_config_path", lambda: Path("/cfg.yml"))
    monkeypatch.setattr(
        validation_cli,
        "get_db",
        lambda config_path: _DatabaseContext(connection),
    )
    return connection


def test_validation_json_object_contract() -> None:
    assert validation_cli._read_json_object('{"ok": true}', label="payload") == {
        "ok": True
    }
    with pytest.raises(typer.BadParameter, match="invalid JSON"):
        validation_cli._read_json_object("{", label="payload")
    with pytest.raises(typer.BadParameter, match="must be a JSON object"):
        validation_cli._read_json_object("[]", label="payload")


def test_validation_assess_revalidate_and_editor_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _install_validation_database(monkeypatch)
    emitted: list[dict] = []
    calls: list[tuple[str, str, object, dict]] = []

    class _Assessment:
        def to_dict(self) -> dict:
            return {"valid": True}

    def _assess(session, euid, *, validator_ref, context):
        calls.append(("assess", euid, validator_ref, context))
        assert session is connection.session
        return _Assessment()

    def _editor(session, euid, *, validator_ref, context):
        calls.append(("editor", euid, validator_ref, context))
        assert session is connection.session
        return {"euid": euid}

    monkeypatch.setattr(validation_cli, "assess_object", _assess)
    monkeypatch.setattr(validation_cli, "editor_data_for_object", _editor)
    monkeypatch.setattr(validation_cli, "_print_payload", emitted.append)

    validation_cli.assess("persisted-euid", validator_ref="validator/v1")
    validation_cli.revalidate("persisted-euid", validator_ref=None)
    validation_cli.editor_data("persisted-euid", validator_ref="validator/v2")

    assert connection.app_username == "tapdb-cli"
    assert connection.commits == [False, False, False]
    assert emitted == [
        {"valid": True},
        {"revalidated": True, "assessment": {"valid": True}},
        {"euid": "persisted-euid"},
    ]
    assert calls[0][-1] == {"surface": "tapdb_cli"}
    assert calls[1][-1] == {"surface": "tapdb_cli", "operation": "revalidate"}


@pytest.mark.parametrize("command_name", ["assess", "revalidate", "editor_data"])
def test_validation_lookup_failures_exit_one(
    monkeypatch: pytest.MonkeyPatch, command_name: str
) -> None:
    _install_validation_database(monkeypatch)
    errors: list[str] = []
    monkeypatch.setattr(validation_cli.ccyo_out, "error", errors.append)

    def _missing(*args, **kwargs):
        raise LookupError("object not found")

    if command_name == "editor_data":
        monkeypatch.setattr(validation_cli, "editor_data_for_object", _missing)
        command = validation_cli.editor_data
    else:
        monkeypatch.setattr(validation_cli, "assess_object", _missing)
        command = getattr(validation_cli, command_name)

    with pytest.raises(typer.Exit) as caught:
        command("persisted-euid", validator_ref=None)
    assert caught.value.exit_code == 1
    assert errors == ["object not found"]


def test_repair_create_success_and_governed_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _install_validation_database(monkeypatch)
    emitted: list[dict] = []
    captured: dict = {}
    monkeypatch.setattr(
        validation_cli,
        "get_db_config",
        lambda **kwargs: {"domain_code": "Z"},
    )
    monkeypatch.setattr(validation_cli, "_print_payload", emitted.append)

    def _create(session, **kwargs):
        assert session is connection.session
        captured.update(kwargs)
        return {"created": True}

    monkeypatch.setattr(validation_cli, "create_repair_record", _create)
    validation_cli.create(
        "persisted-euid",
        reason="operator correction",
        payload_json='{ "field": "value" }',
        actor="operator@example.test",
    )
    assert connection.app_username == "operator@example.test"
    assert connection.commits == [True]
    assert captured == {
        "domain_code": "Z",
        "subject_euid": "persisted-euid",
        "actor": "operator@example.test",
        "reason": "operator correction",
        "repair_payload": {"field": "value"},
        "governance_context": {"surface": "tapdb_cli"},
    }
    assert emitted == [{"created": True}]

    errors: list[str] = []
    monkeypatch.setattr(validation_cli.ccyo_out, "error", errors.append)
    monkeypatch.setattr(
        validation_cli,
        "create_repair_record",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("denied")),
    )
    with pytest.raises(typer.Exit) as caught:
        validation_cli.create(
            "persisted-euid",
            reason="bad",
            payload_json="{}",
            actor="operator@example.test",
        )
    assert caught.value.exit_code == 1
    assert errors == ["denied"]


def test_admin_context_reader_and_app_builder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    context_path = tmp_path / "context.json"
    monkeypatch.setattr(admin_server, "_context_file_path", lambda: context_path)

    with pytest.raises(RuntimeError, match="context is missing"):
        admin_server._read_context_file()

    context_path.write_text("[]\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Invalid TAPDB admin context"):
        admin_server._read_context_file()

    context_path.write_text(
        json.dumps({"config_path": "/absolute/tapdb-config.yaml"}) + "\n",
        encoding="utf-8",
    )
    assert admin_server._read_context_file()["config_path"] == (
        "/absolute/tapdb-config.yaml"
    )

    marker = object()
    monkeypatch.setattr(
        "daylily_tapdb.gui.create_tapdb_gui_app",
        lambda *, config_path: config_path == "/absolute/tapdb-config.yaml" and marker,
    )
    assert admin_server.build_app() is marker

    context_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="context file is incomplete"):
        admin_server.build_app()


def test_admin_server_run_passes_resolved_context_and_tls(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    context_path = tmp_path / "ui" / "context.json"
    context_path.parent.mkdir()
    calls: dict[str, object] = {}
    monkeypatch.setattr(admin_server, "_resolve_tls_mode", lambda mode: "https")
    monkeypatch.setattr(
        admin_server,
        "_write_context_file",
        lambda **kwargs: context_path,
    )
    monkeypatch.setattr(
        admin_server,
        "set_cli_context",
        lambda **kwargs: calls.setdefault("context", kwargs),
    )
    monkeypatch.setattr(
        admin_server,
        "_uvicorn_tls_kwargs",
        lambda **kwargs: {
            "ssl_keyfile": "/key.pem",
            "ssl_certfile": "/cert.pem",
        },
    )

    import uvicorn

    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: calls.update(kwargs))
    previous = Path.cwd()
    try:
        admin_server.run_admin_server(
            SimpleNamespace(
                config="/cfg.yml",
                host="127.0.0.1",
                port=8911,
                tls_mode="https",
                ssl_keyfile="/key.pem",
                ssl_certfile="/cert.pem",
                reload=True,
            )
        )
    finally:
        os.chdir(previous)

    assert calls["context"] == {"config_path": "/cfg.yml"}
    assert calls["factory"] is True
    assert calls["host"] == "127.0.0.1"
    assert calls["port"] == 8911
    assert calls["reload"] is True
    assert calls["ssl_keyfile"] == "/key.pem"


def test_admin_server_main_translates_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = SimpleNamespace()
    parser = SimpleNamespace(parse_args=lambda: args)
    monkeypatch.setattr(admin_server, "_build_parser", lambda: parser)
    seen: list[object] = []
    monkeypatch.setattr(admin_server, "run_admin_server", seen.append)
    admin_server.main()
    assert seen == [args]

    monkeypatch.setattr(
        admin_server,
        "run_admin_server",
        lambda value: (_ for _ in ()).throw(RuntimeError("cannot start")),
    )
    with pytest.raises(SystemExit, match="cannot start"):
        admin_server.main()
