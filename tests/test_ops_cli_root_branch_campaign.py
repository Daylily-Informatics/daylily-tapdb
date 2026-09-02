"""Behavior coverage for TapDB root CLI lifecycle and config branches."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer
import yaml

import daylily_tapdb.cli as cli
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context


def _command(group: str | None, name: str):
    app = cli.app
    assert app is not None
    target = app
    if group is not None:
        group_info = next(item for item in app.registered_groups if item.name == group)
        target = group_info.typer_instance
    return next(
        item.callback for item in target.registered_commands if item.name == name
    )


def _config_payload() -> dict[str, object]:
    return {
        "meta": {
            "config_version": 4,
            "client_id": "alpha",
            "database_name": "beta",
            "owner_repo_name": "daylily-tapdb",
            "domain_registry_path": "/tmp/domains.json",
            "prefix_ownership_registry_path": "/tmp/prefixes.json",
        },
        "admin": {
            "footer": {"repo_url": "https://example.com/repo"},
            "session": {"secret": "old-secret"},
            "auth": {
                "mode": "tapdb",
                "disabled_user": {"email": "admin@localhost", "role": "admin"},
                "shared_host": {
                    "session_secret": "",
                    "session_cookie": "session",
                    "session_max_age_seconds": 1209600,
                },
            },
            "cors": {"allowed_origins": []},
            "ui": {"tls": {"cert_path": "", "key_path": ""}},
            "metrics": {"enabled": True, "queue_max": 20000, "flush_seconds": 1.0},
        },
        "target": {
            "engine_type": "local",
            "host": "localhost",
            "port": "5533",
            "ui_port": "8911",
            "user": "tapdb",
            "password": "",
            "database": "tapdb_shared",
            "schema_name": "tapdb_beta",
            "domain_code": "Z",
            "support_email": "support@example.com",
        },
        "safety": {
            "safety_tier": "shared",
            "destructive_operations": "confirm_required",
        },
        "backup": {
            "storage": {"uri": ""},
            "retention": {"keep_last": 30},
            "encryption": {"mode": "none"},
            "signing": {"mode": "none", "kms_key_arn": ""},
            "provider_snapshots": {"enabled": False, "cluster_identifier": ""},
            "rehearsal": {"database_prefix": "tapdb_rehearsal"},
            "expected_interval_hours": 0,
            "receipt_mirror": {},
        },
    }


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "tapdb-config.yaml"
    path.write_text(
        yaml.safe_dump(_config_payload(), sort_keys=False), encoding="utf-8"
    )
    os.chmod(path, 0o600)
    clear_cli_context()
    set_cli_context(client_id="alpha", database_name="beta", config_path=path)
    monkeypatch.setenv("HOME", str(tmp_path))
    yield path
    clear_cli_context()


def test_tls_pid_port_and_admin_helpers(
    config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runtime = tmp_path / "runtime"
    monkeypatch.setattr(
        cli,
        "_ui_runtime_paths",
        lambda: (runtime / "ui.pid", runtime / "ui.log", runtime / "certs"),
    )
    import daylily_tapdb.cli.db_config as db_config

    monkeypatch.setattr(db_config, "get_admin_settings", lambda: {})
    cert, key = cli._resolve_tls_paths()
    assert cert == runtime / "certs" / "localhost.crt"
    assert key == runtime / "certs" / "localhost.key"
    monkeypatch.setattr(
        db_config,
        "get_admin_settings",
        lambda: {
            "tls_cert_path": "~/configured.crt",
            "tls_key_path": "~/configured.key",
        },
    )
    assert cli._resolve_tls_paths()[0].name == "configured.crt"
    explicit = cli._resolve_tls_paths(
        cert_file=Path("~/explicit.crt"), key_file=Path("~/explicit.key")
    )
    assert explicit[0].name == "explicit.crt"

    cert.parent.mkdir(parents=True)
    cert.write_text("cert", encoding="utf-8")
    key.write_text("key", encoding="utf-8")
    assert cli._ensure_tls_certificates("localhost", cert_file=cert, key_file=key) == (
        cert,
        key,
    )
    cert.unlink()
    key.unlink()
    monkeypatch.setattr(cli.shutil, "which", lambda _name: None)
    with pytest.raises(RuntimeError, match="openssl"):
        cli._ensure_tls_certificates("localhost", cert_file=cert, key_file=key)

    monkeypatch.setattr(cli.shutil, "which", lambda _name: "/usr/bin/openssl")
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 2, "", "bad cert"),
    )
    with pytest.raises(RuntimeError, match="bad cert"):
        cli._ensure_tls_certificates("example.com", cert_file=cert, key_file=key)

    def generate(command, **_kwargs):
        Path(command[command.index("-out") + 1]).write_text("cert", encoding="utf-8")
        Path(command[command.index("-keyout") + 1]).write_text("key", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(cli.subprocess, "run", generate)
    monkeypatch.setattr(
        cli.os, "chmod", lambda *_args: (_ for _ in ()).throw(OSError())
    )
    cli._ensure_tls_certificates("example.com", cert_file=cert, key_file=key)

    bad_pid = runtime / "bad.pid"
    bad_pid.write_text("bad", encoding="utf-8")
    assert cli._get_pid(bad_pid) is None
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError()),
    )
    assert "already in use" in cli._port_conflict_details(8911)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 1, "", ""),
    )
    assert "already in use" in cli._port_conflict_details(8911)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "header\n", ""),
    )
    assert "already in use" in cli._port_conflict_details(8911)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, "header\npython 1 x\n", ""
        ),
    )
    assert "python 1 x" in cli._port_conflict_details(8911)


class _Process:
    def __init__(self, *, pid: int = 321, returncode: int | None = None):
        self.pid = pid
        self.returncode = returncode

    def poll(self):
        return self.returncode


def test_ui_start_success_and_refusal_paths(
    config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    start = _command("ui", "start")
    pid_file = tmp_path / "ui" / "ui.pid"
    log_file = tmp_path / "ui" / "ui.log"
    cert = tmp_path / "cert.crt"
    key = tmp_path / "cert.key"
    monkeypatch.setattr(
        cli, "_ui_runtime_paths", lambda: (pid_file, log_file, tmp_path / "certs")
    )
    monkeypatch.setattr(cli, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli, "_get_pid", lambda _path: None)
    monkeypatch.setattr(cli, "_port_is_available", lambda *_args: True)
    monkeypatch.setattr(
        cli, "_ensure_tls_certificates", lambda *_args, **_kwargs: (cert, key)
    )
    monkeypatch.setattr(cli.time, "sleep", lambda *_args: None)
    import daylily_tapdb.cli.db_config as db_config

    monkeypatch.setattr(db_config, "get_db_config", lambda: {"ui_port": "8911"})
    monkeypatch.setattr(db_config, "get_config_path", lambda: config_path)
    commands: list[list[str]] = []
    monkeypatch.setattr(
        cli.subprocess,
        "Popen",
        lambda command, **_kwargs: commands.append(list(command)) or _Process(),
    )
    start(None, "localhost", True, True, None, None)
    assert pid_file.read_text() == "321"
    assert "--reload" in commands[-1]

    monkeypatch.setattr(cli, "_get_pid", lambda _path: 111)
    start(None, "localhost", False, True, None, None)
    monkeypatch.setattr(cli, "_get_pid", lambda _path: None)
    with pytest.raises(typer.Exit):
        start(9999, "localhost", False, True, None, None)

    monkeypatch.setattr(cli, "_port_is_available", lambda *_args: False)
    monkeypatch.setattr(cli, "_port_conflict_details", lambda _port: "busy")
    with pytest.raises(typer.Exit):
        start(None, "localhost", False, True, None, None)
    monkeypatch.setattr(cli, "_port_is_available", lambda *_args: True)
    monkeypatch.setattr(
        cli,
        "_ensure_tls_certificates",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("tls")),
    )
    with pytest.raises(typer.Exit):
        start(None, "localhost", False, True, None, None)

    monkeypatch.setattr(
        cli, "_ensure_tls_certificates", lambda *_args, **_kwargs: (cert, key)
    )
    monkeypatch.setattr(
        cli.subprocess, "Popen", lambda *_args, **_kwargs: _Process(returncode=1)
    )
    with pytest.raises(typer.Exit):
        start(None, "localhost", False, True, None, None)

    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    start(None, "localhost", False, False, None, None)


def test_ui_mkcert_stop_status_logs_and_restart(
    config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mkcert = _command("ui", "mkcert")
    stop = _command("ui", "stop")
    status = _command("ui", "status")
    logs = _command("ui", "logs")
    restart = _command("ui", "restart")
    pid_file = tmp_path / "ui.pid"
    log_file = tmp_path / "ui.log"
    monkeypatch.setattr(
        cli, "_ui_runtime_paths", lambda: (pid_file, log_file, tmp_path / "certs")
    )
    monkeypatch.setattr(cli.shutil, "which", lambda _name: None)
    with pytest.raises(typer.Exit):
        mkcert(None, None)

    monkeypatch.setattr(cli.shutil, "which", lambda _name: "/usr/bin/mkcert")
    monkeypatch.setattr(
        cli,
        "_resolve_tls_paths",
        lambda: (tmp_path / "default.crt", tmp_path / "default.key"),
    )
    results = iter(
        [
            subprocess.CompletedProcess([], 2, "", "install failed"),
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 2, "", "generate failed"),
        ]
    )
    monkeypatch.setattr(cli.subprocess, "run", lambda *_args, **_kwargs: next(results))
    with pytest.raises(typer.Exit):
        mkcert(None, None)
    with pytest.raises(typer.Exit):
        mkcert(None, None)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        cli.os, "chmod", lambda *_args: (_ for _ in ()).throw(OSError())
    )
    mkcert(tmp_path / "explicit.crt", tmp_path / "explicit.key")

    monkeypatch.setattr(cli, "_get_pid", lambda _path: 123)
    kills: list[int] = []

    def kill(_pid, signal_number):
        kills.append(signal_number)
        if signal_number == 0:
            raise ProcessLookupError

    monkeypatch.setattr(cli.os, "kill", kill)
    monkeypatch.setattr(cli.time, "sleep", lambda *_args: None)
    pid_file.write_text("123", encoding="utf-8")
    stop()
    assert not pid_file.exists()
    status()

    monkeypatch.setattr(
        cli.os, "kill", lambda *_args: (_ for _ in ()).throw(PermissionError())
    )
    with pytest.raises(typer.Exit):
        stop()
    monkeypatch.setattr(
        cli.os, "kill", lambda *_args: (_ for _ in ()).throw(ProcessLookupError())
    )
    stop()

    monkeypatch.setattr(cli, "_get_pid", lambda _path: None)
    status()
    logs(False, 2)
    log_file.write_text("one\ntwo\nthree\n", encoding="utf-8")
    logs(False, 2)
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    logs(True, 2)

    calls: list[str] = []
    stop_cell = dict(
        zip(restart.__code__.co_freevars, restart.__closure__, strict=True)
    )["ui_stop"]
    start_cell = dict(
        zip(restart.__code__.co_freevars, restart.__closure__, strict=True)
    )["ui_start"]
    original_stop, original_start = stop_cell.cell_contents, start_cell.cell_contents
    stop_cell.cell_contents = lambda: calls.append("stop")
    start_cell.cell_contents = lambda **_kwargs: calls.append("start")
    try:
        restart(None, "localhost")
    finally:
        stop_cell.cell_contents, start_cell.cell_contents = (
            original_stop,
            original_start,
        )
    assert calls == ["stop", "start"]


def test_local_bootstrap_and_ui_followup_branches(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = _command("bootstrap", "local")
    import daylily_tapdb.cli.db_config as db_config

    monkeypatch.setattr(db_config, "get_db_config", lambda: {"engine_type": "aurora"})
    with pytest.raises(typer.Exit):
        local(True, False, False)

    monkeypatch.setattr(
        db_config, "get_db_config", lambda: {"engine_type": "local", "ui_port": "8911"}
    )
    events: list[str] = []
    cells = dict(zip(local.__code__.co_freevars, local.__closure__, strict=True))
    replacements = {
        "pg_init": lambda **_kwargs: events.append("pg_init"),
        "pg_start_local": lambda **_kwargs: events.append("pg_start"),
        "create_database": lambda **_kwargs: events.append("create"),
        "apply_schema": lambda **_kwargs: events.append("schema"),
        "run_migrations": lambda **_kwargs: events.append("migrate"),
        "seed_templates": lambda **_kwargs: events.append("seed"),
        "_create_default_admin": lambda **_kwargs: events.append("admin"),
    }
    originals = {name: cells[name].cell_contents for name in replacements}
    for name, value in replacements.items():
        cells[name].cell_contents = value
    try:
        local(True, True, True)
    finally:
        for name, value in originals.items():
            cells[name].cell_contents = value
    assert events == [
        "pg_init",
        "pg_start",
        "create",
        "schema",
        "migrate",
        "seed",
        "admin",
    ]

    helper = dict(zip(local.__code__.co_freevars, local.__closure__, strict=True))[
        "_maybe_start_ui_after_bootstrap"
    ].cell_contents
    helper(True)
    start = dict(zip(helper.__code__.co_freevars, helper.__closure__, strict=True))[
        "ui_start"
    ]
    original = start.cell_contents
    start.cell_contents = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("ui"))
    try:
        helper(False)
    finally:
        start.cell_contents = original


def test_aurora_bootstrap_create_update_and_failure_paths(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    aurora = _command("bootstrap", "aurora")
    import daylily_tapdb.aurora.stack_manager as stack_module
    import daylily_tapdb.cli.aurora as aurora_cli

    monkeypatch.setattr(aurora_cli, "_ensure_boto3", lambda: None)
    monkeypatch.setattr(
        aurora_cli, "_resolve_ingress_cidr", lambda *_args: "10.0.0.0/8"
    )
    events: list[str] = []

    class Manager:
        def __init__(self, **_kwargs):
            pass

        def get_stack_status(self, _name):
            events.append("status")
            return {"outputs": {"ClusterEndpoint": "db.example", "ClusterPort": "5432"}}

        def update_stack(self, _desired):
            events.append("update")
            return {"outputs": {"ClusterEndpoint": "db.example", "ClusterPort": "5432"}}

        def create_stack(self, _desired):
            events.append("create")
            return {"outputs": {"ClusterEndpoint": "db.example", "ClusterPort": "5432"}}

    monkeypatch.setattr(stack_module, "AuroraStackManager", Manager)
    cells = dict(zip(aurora.__code__.co_freevars, aurora.__closure__, strict=True))
    replacements = {
        "config_update": lambda **_kwargs: events.append("config"),
        "create_database": lambda **_kwargs: events.append("database"),
        "apply_schema": lambda **_kwargs: events.append("schema"),
        "run_migrations": lambda **_kwargs: events.append("migration"),
        "seed_templates": lambda **_kwargs: events.append("seed"),
        "_create_default_admin": lambda **_kwargs: events.append("admin"),
        "_ensure_bootstrap_config_for_aurora": lambda: config_path,
        "_maybe_start_ui_after_bootstrap": lambda **_kwargs: events.append("ui"),
    }
    originals = {name: cells[name].cell_contents for name in replacements}
    for name, value in replacements.items():
        cells[name].cell_contents = value
    try:
        aurora("cluster", "us-west-2", "", None, False, True, True, False)
    finally:
        for name, value in originals.items():
            cells[name].cell_contents = value
    assert "update" in events and "config" in events and "seed" in events

    monkeypatch.setattr(
        aurora_cli,
        "_resolve_ingress_cidr",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("cidr")),
    )
    with pytest.raises(typer.Exit):
        aurora("cluster", "us-west-2", "", None, False, True, False, False)

    class CreateManager(Manager):
        def get_stack_status(self, _name):
            raise RuntimeError("stack not found")

    monkeypatch.setattr(stack_module, "AuroraStackManager", CreateManager)
    cells = dict(zip(aurora.__code__.co_freevars, aurora.__closure__, strict=True))
    replacements = {
        "config_update": lambda **_kwargs: events.append("config-create"),
        "create_database": lambda **_kwargs: None,
        "apply_schema": lambda **_kwargs: None,
        "run_migrations": lambda **_kwargs: None,
        "seed_templates": lambda **_kwargs: None,
        "_create_default_admin": lambda **_kwargs: None,
        "_ensure_bootstrap_config_for_aurora": lambda: config_path,
        "_maybe_start_ui_after_bootstrap": lambda **_kwargs: None,
    }
    originals = {name: cells[name].cell_contents for name in replacements}
    for name, value in replacements.items():
        cells[name].cell_contents = value
    monkeypatch.setattr(
        aurora_cli, "_resolve_ingress_cidr", lambda *_args: "10.0.0.0/8"
    )
    try:
        aurora("cluster", "us-west-2", "", None, False, True, False, False)
    finally:
        for name, value in originals.items():
            cells[name].cell_contents = value
    assert "create" in events

    class DelayedEndpointManager(Manager):
        calls = 0

        def get_stack_status(self, _name):
            type(self).calls += 1
            return {
                "outputs": (
                    {"ClusterEndpoint": "later.example", "ClusterPort": "5432"}
                    if type(self).calls > 1
                    else {}
                )
            }

        def update_stack(self, _desired):
            return {"outputs": {}}

    monkeypatch.setattr(stack_module, "AuroraStackManager", DelayedEndpointManager)
    cells = dict(zip(aurora.__code__.co_freevars, aurora.__closure__, strict=True))
    originals = {name: cells[name].cell_contents for name in replacements}
    for name, value in replacements.items():
        cells[name].cell_contents = value
    try:
        aurora("cluster", "us-west-2", "", None, False, True, False, False)
    finally:
        for name, value in originals.items():
            cells[name].cell_contents = value


def test_config_update_all_fields_and_validation_errors(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    update = _command("config", "update")
    kwargs = {
        "engine_type": "aurora",
        "host": "db.example",
        "hostaddr": "127.0.0.1",
        "port": 5432,
        "ui_port": 9443,
        "user": "operator",
        "password": "hidden",
        "tenant_id": "00000000-0000-4000-8000-000000000001",
        "allow_global_claims": True,
        "operator_user": "tapdb_operator",
        "operator_password": "operator-hidden",
        "operator_secret_arn": None,
        "operator_iam_auth": False,
        "database": "tapdb_new",
        "schema_name": "tapdb_new",
        "cognito_user_pool_id": "pool",
        "cognito_app_client_id": "client",
        "cognito_app_client_secret": "client-secret",
        "cognito_client_name": "name",
        "cognito_region": "us-west-2",
        "cognito_domain": "auth.example",
        "cognito_callback_url": "https://example/callback",
        "cognito_logout_url": "https://example/logout",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "domain_registry_path": "/new/domains.json",
        "prefix_ownership_registry_path": "/new/prefixes.json",
        "support_email": "new@example.com",
        "admin_repo_url": "https://example/new",
        "admin_session_secret": "new-secret",
        "admin_auth_mode": "shared_host",
        "admin_disabled_user_email": "ADMIN@EXAMPLE.COM",
        "admin_disabled_user_role": "ADMIN",
        "admin_shared_host_session_secret": "shared-secret",
        "admin_shared_host_session_cookie": "tapdb-session",
        "admin_shared_host_session_max_age_seconds": 60,
        "admin_allowed_origin": [" https://allowed.example ", ""],
        "admin_tls_cert_path": "/tls/cert",
        "admin_tls_key_path": "/tls/key",
        "admin_metrics_enabled": False,
        "admin_metrics_queue_max": 10,
        "admin_metrics_flush_seconds": 2.5,
        "clear": ["support_email"],
        "safety_tier": "production",
        "destructive_operations": "blocked",
        "backup_storage_uri": "s3://bucket/prefix",
        "backup_keep_last": 5,
        "backup_expected_interval_hours": 12.0,
        "backup_expected_rehearsal_interval_days": 7.0,
        "backup_rehearsal_database_prefix": "rehearsal_new",
        "backup_receipt_mirror_uri": "s3://mirror/receipts",
    }
    update(**kwargs)
    root = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert root["target"]["schema_name"] == "tapdb_new"
    assert root["target"]["tenant_id"] == "00000000-0000-4000-8000-000000000001"
    assert root["target"]["operator"]["user"] == "tapdb_operator"
    assert root["target"]["support_email"] == "new@example.com"
    assert root["admin"]["auth"]["disabled_user"]["email"] == "admin@example.com"
    assert root["backup"]["retention"]["keep_last"] == 5
    assert root["safety"]["destructive_operations"] == "blocked"

    empty = {key: None for key in kwargs}
    empty["admin_allowed_origin"] = []
    empty["clear"] = ["not_allowed"]
    with pytest.raises(RuntimeError, match="Unknown target"):
        update(**empty)
    empty["clear"] = []
    with pytest.raises(RuntimeError, match="No config changes"):
        update(**empty)

    for key, value, message in (
        ("backup_keep_last", 0, "at least 1"),
        ("backup_expected_interval_hours", -1, "zero or greater"),
        ("backup_expected_rehearsal_interval_days", -1, "zero or greater"),
        ("safety_tier", "unsafe", "local, shared, or production"),
        ("destructive_operations", "unsafe", "blocked"),
    ):
        case = dict(empty)
        case[key] = value
        with pytest.raises(RuntimeError, match=message):
            update(**case)

    credential_uri = dict(empty)
    credential_uri["backup_storage_uri"] = "s3://user:password@bucket/prefix"
    with pytest.raises(RuntimeError, match="credentials"):
        update(**credential_uri)


def test_config_update_rejects_missing_and_malformed_documents(
    config_path: Path,
) -> None:
    update = _command("config", "update")
    kwargs = {
        "engine_type": "local",
        "host": None,
        "hostaddr": None,
        "port": None,
        "ui_port": None,
        "user": None,
        "password": None,
        "database": None,
        "schema_name": None,
        "cognito_user_pool_id": None,
        "cognito_app_client_id": None,
        "cognito_app_client_secret": None,
        "cognito_client_name": None,
        "cognito_region": None,
        "cognito_domain": None,
        "cognito_callback_url": None,
        "cognito_logout_url": None,
        "domain_code": None,
        "owner_repo_name": None,
        "domain_registry_path": None,
        "prefix_ownership_registry_path": None,
        "support_email": None,
        "admin_repo_url": None,
        "admin_session_secret": None,
        "admin_auth_mode": None,
        "admin_disabled_user_email": None,
        "admin_disabled_user_role": None,
        "admin_shared_host_session_secret": None,
        "admin_shared_host_session_cookie": None,
        "admin_shared_host_session_max_age_seconds": None,
        "admin_allowed_origin": [],
        "admin_tls_cert_path": None,
        "admin_tls_key_path": None,
        "admin_metrics_enabled": None,
        "admin_metrics_queue_max": None,
        "admin_metrics_flush_seconds": None,
        "clear": [],
        "safety_tier": None,
        "destructive_operations": None,
        "backup_storage_uri": None,
        "backup_keep_last": None,
        "backup_expected_interval_hours": None,
        "backup_expected_rehearsal_interval_days": None,
        "backup_rehearsal_database_prefix": None,
        "backup_receipt_mirror_uri": None,
    }
    config_path.unlink()
    with pytest.raises(RuntimeError, match="required"):
        update(**kwargs)
    config_path.write_text("[]\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="YAML mapping"):
        update(**kwargs)
    config_path.write_text("target: {}\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="metadata"):
        update(**kwargs)


def test_framework_registration_and_main_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Registry:
        def __init__(self):
            self.calls = []

        def add_command(self, **kwargs):
            self.calls.append(kwargs)

    registry = Registry()
    cli._registry_add_command(
        registry=registry,
        group_path=None,
        name="sample",
        callback=lambda: None,
        help_text="sample",
    )
    assert registry.calls[0]["name"] == "sample"
    assert "policy" not in registry.calls[0]

    monkeypatch.setattr(cli, "clear_cli_context", lambda: None)
    import cli_core_yo.app as core_app

    monkeypatch.setattr(core_app, "run", lambda _spec, _args: 7)
    monkeypatch.setattr(cli.sys, "argv", ["tapdb", "info"])
    with pytest.raises(SystemExit) as raised:
        cli.main()
    assert raised.value.code == 7


def test_ui_start_rejects_missing_admin_extras(
    config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    start = _command("ui", "start")
    monkeypatch.setattr(
        cli,
        "_ui_runtime_paths",
        lambda: (tmp_path / "ui.pid", tmp_path / "ui.log", tmp_path / "certs"),
    )
    monkeypatch.setattr(
        cli,
        "_require_admin_extras",
        lambda: (_ for _ in ()).throw(SystemExit(1)),
    )
    import daylily_tapdb.cli.db_config as db_config

    monkeypatch.setattr(db_config, "get_db_config", lambda: {"ui_port": "8911"})
    with pytest.raises(typer.Exit):
        start(None, "localhost", False, True, None, None)


def test_build_app_without_boto3_exposes_failing_aurora_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_find_spec = cli.importlib.util.find_spec

    def find_spec(name: str):
        if name == "boto3":
            return None
        return original_find_spec(name)

    monkeypatch.setattr(cli.importlib.util, "find_spec", find_spec)
    isolated = cli.build_app()
    group = next(item for item in isolated.registered_groups if item.name == "aurora")
    callback = group.typer_instance.registered_callback.callback
    with pytest.raises(typer.Exit):
        callback(SimpleNamespace())


def test_admin_module_fallback_and_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    assert cli._find_admin_module() == "admin.main:app"
    monkeypatch.setattr(Path, "exists", lambda _path: False)
    with pytest.raises(ValueError, match="Cannot find admin module"):
        cli._find_admin_module()


def test_config_init_rejects_namespace_and_invalid_policy_fields(
    config_path: Path,
) -> None:
    initialize = _command("config", "init")
    base = {
        "client_id": "alpha",
        "database_name": "beta",
        "owner_repo_name": "daylily-tapdb",
        "domain_code": "Z",
        "domain_registry_path": "/tmp/domains.json",
        "prefix_ownership_registry_path": "/tmp/prefixes.json",
        "engine_type": "local",
        "host": "localhost",
        "hostaddr": None,
        "port": 5533,
        "ui_port": 8911,
        "user": "tapdb",
        "password": "",
        "database": "tapdb_shared",
        "schema_name": "tapdb_beta",
        "safety_tier": "shared",
        "destructive_operations": "confirm_required",
        "force": False,
    }

    mismatch = dict(base, client_id="other")
    with pytest.raises(RuntimeError, match="different namespace"):
        initialize(**mismatch)

    for key, value, message in (
        ("engine_type", "sqlite", "local or aurora"),
        ("safety_tier", "unsafe", "local, shared, or production"),
        ("destructive_operations", "unsafe", "blocked"),
        ("domain_code", "", "domain-code is required"),
    ):
        set_cli_context(
            client_id="alpha", database_name="beta", config_path=config_path
        )
        case = dict(base, force=True)
        case[key] = value
        with pytest.raises(RuntimeError, match=message):
            initialize(**case)


def test_ui_stop_escalates_after_grace_period(
    config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stop = _command("ui", "stop")
    pid_file = tmp_path / "ui.pid"
    pid_file.write_text("123", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "_ui_runtime_paths",
        lambda: (pid_file, tmp_path / "ui.log", tmp_path / "certs"),
    )
    monkeypatch.setattr(cli, "_get_pid", lambda _path: 123)
    signals: list[int] = []
    monkeypatch.setattr(
        cli.os, "kill", lambda _pid, signal_number: signals.append(signal_number)
    )
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)

    stop()

    assert signals[-1] == cli.signal.SIGKILL
