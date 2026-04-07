from __future__ import annotations

import builtins
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer
from typer.testing import CliRunner

import daylily_tapdb.cli.cognito as cognito_mod
from daylily_tapdb.cli.db import Environment

runner = CliRunner()


class _FakeConn:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def session_scope(self, commit: bool = False):
        _ = commit

        class _Scope:
            def __enter__(self_inner):
                return object()

            def __exit__(self_inner, exc_type, exc, tb):
                return False

        return _Scope()


def test_cognito_detect_running_ui_port_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pid_file = tmp_path / "ui.pid"
    monkeypatch.setattr(cognito_mod, "_ui_pid_file_for_env", lambda _env: pid_file)

    assert cognito_mod._detect_running_ui_port(Environment.dev) == (None, "ui not running")

    pid_file.write_text("1234\n", encoding="utf-8")
    monkeypatch.setattr(
        cognito_mod.os, "kill", lambda *_args, **_kwargs: (_ for _ in ()).throw(ProcessLookupError())
    )
    assert cognito_mod._detect_running_ui_port(Environment.dev) == (None, "ui pid missing/stale")

    monkeypatch.setattr(cognito_mod.os, "kill", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        cognito_mod.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ps unavailable")),
    )
    assert cognito_mod._detect_running_ui_port(Environment.dev) == (
        None,
        "could not inspect ui process",
    )

    monkeypatch.setattr(
        cognito_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr=""),
    )
    assert cognito_mod._detect_running_ui_port(Environment.dev) == (
        None,
        "could not inspect ui process",
    )

    monkeypatch.setattr(
        cognito_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="python -m uvicorn", stderr=""),
    )
    assert cognito_mod._detect_running_ui_port(Environment.dev) == (
        None,
        "ui process port not detected",
    )

    monkeypatch.setattr(
        cognito_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="uvicorn --port 9443", stderr=""),
    )
    assert cognito_mod._detect_running_ui_port(Environment.dev) == (9443, "running ui process")


def test_cognito_resolve_expected_ui_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cognito_mod, "get_db_config_for_env", lambda _env: {"ui_port": "9443"})
    assert cognito_mod._resolve_expected_ui_port(Environment.dev) == (
        9443,
        "config environments.dev.ui_port",
    )

    monkeypatch.setattr(cognito_mod, "get_db_config_for_env", lambda _env: {"ui_port": ""})
    monkeypatch.setattr(cognito_mod, "_detect_running_ui_port", lambda _env: (9555, "ui process"))
    assert cognito_mod._resolve_expected_ui_port(Environment.dev) == (9555, "ui process")

    monkeypatch.setattr(cognito_mod, "_detect_running_ui_port", lambda _env: (None, "ui not running"))
    assert cognito_mod._resolve_expected_ui_port(Environment.dev) == (
        cognito_mod.DEFAULT_COGNITO_CALLBACK_PORT,
        "default (ui not running)",
    )


def test_cognito_validate_bound_cognito_uris(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cognito_mod, "_resolve_expected_ui_port", lambda _env: (8911, "config"))

    expected_port, port_source, errors, notices = cognito_mod._validate_bound_cognito_uris(
        Environment.dev,
        {},
    )
    assert (expected_port, port_source, errors, notices) == (8911, "config", [], [])

    values = {
        "COGNITO_CALLBACK_URL": "not-a-uri",
        "COGNITO_LOGOUT_URL": "http://localhost:8911/logout",
        "COGNITO_REDIRECT_URIS": "https://localhost:9443/auth/callback https://example.com/ok",
    }
    _, _, errors, notices = cognito_mod._validate_bound_cognito_uris(Environment.dev, values)
    assert any("invalid URI" in msg for msg in errors)
    assert any("must use https" in msg for msg in errors)
    assert any("does not match TAPDB UI port 8911" in msg for msg in errors)
    assert notices == ["COGNITO_REDIRECT_URIS: https://example.com/ok"]


def test_cognito_filename_pool_and_context_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    assert cognito_mod._sanitize_filename_part("TapDB App!") == "TapDB-App"
    monkeypatch.setattr(cognito_mod, "get_db_config_for_env", lambda _env: {"database": "TapDB Dev"})
    assert cognito_mod._default_pool_name(Environment.dev) == "tapdb-tapdb-dev-users"
    assert cognito_mod._parse_daycog_context_name("pool.us-east-1.app") == (
        "pool",
        "us-east-1",
        "app",
    )
    assert cognito_mod._parse_daycog_context_name("invalid") == ("", "", "")


def test_cognito_load_contexts_and_match_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "daycog.yaml"
    monkeypatch.setattr(cognito_mod, "_daycog_config_path", lambda: path)

    with pytest.raises(RuntimeError, match="config store not found"):
        cognito_mod._load_daycog_contexts()

    path.write_text("- bad\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid daycog config store"):
        cognito_mod._load_daycog_contexts()

    path.write_text(
        "active_context: tapdb-dev.us-east-1.tapdb\n"
        "contexts:\n"
        "  tapdb-dev.us-east-1:\n"
        "    COGNITO_USER_POOL_ID: pool-1\n"
        "    AWS_REGION: us-east-1\n"
        "  tapdb-dev.us-east-1.tapdb:\n"
        "    COGNITO_USER_POOL_ID: pool-1\n"
        "    COGNITO_REGION: us-east-1\n"
        "    COGNITO_CLIENT_NAME: tapdb\n"
        "  ignored:\n"
        "    value: null\n"
        "  broken: nope\n",
        encoding="utf-8",
    )
    active_name, contexts = cognito_mod._load_daycog_contexts()
    assert active_name == "tapdb-dev.us-east-1.tapdb"
    assert contexts["ignored"] == {}

    score = cognito_mod._score_daycog_context_match(
        "tapdb-dev.us-east-1.tapdb",
        {"COGNITO_REGION": "us-east-1", "COGNITO_CLIENT_NAME": "tapdb"},
        active_name=active_name,
        prefer_region="us-east-1",
        prefer_client_name="tapdb",
    )
    assert score[0] > 0

    context_name, values = cognito_mod._find_pool_context_by_id(
        "pool-1",
        prefer_region="us-east-1",
        prefer_client_name="tapdb",
    )
    assert context_name == "tapdb-dev.us-east-1.tapdb"
    assert values["COGNITO_USER_POOL_ID"] == "pool-1"

    with pytest.raises(RuntimeError, match="No Daycog stored context maps"):
        cognito_mod._find_pool_context_by_id("missing-pool")


def test_cognito_validate_required_client_name_and_pool_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert (
        cognito_mod._validate_required_client_name(
            {"COGNITO_CLIENT_NAME": "tapdb"},
            context_label="env dev",
        )
        == "tapdb"
    )
    with pytest.raises(RuntimeError, match="must select Cognito app client name"):
        cognito_mod._validate_required_client_name(
            {"COGNITO_CLIENT_NAME": "wrong"},
            context_label="env dev",
        )

    path = tmp_path / "daycog.yaml"
    path.write_text(
        "active_context: tapdb-dev.us-east-1.tapdb\n"
        "contexts:\n"
        "  tapdb-dev.us-east-1:\n"
        "    COGNITO_USER_POOL_ID: pool-1\n"
        "  tapdb-dev.us-east-1.tapdb:\n"
        "    COGNITO_USER_POOL_ID: pool-2\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cognito_mod, "_daycog_config_path", lambda: path)
    assert cognito_mod._resolve_daycog_pool_id_after_setup(
        pool_name="tapdb-dev",
        region="us-east-1",
        client_name="tapdb",
    ) == ("pool-1", "tapdb-dev.us-east-1")

    path.write_text("active_context: missing\ncontexts: {}\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="pool ID was not found"):
        cognito_mod._resolve_daycog_pool_id_after_setup(
            pool_name="tapdb-dev",
            region="us-east-1",
            client_name="tapdb",
        )


def test_cognito_write_pool_id_to_tapdb_config_and_json_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "tapdb-config.yaml"
    config_path.write_text(
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: atlas\n"
        "  database_name: app\n"
        "environments:\n"
        "  dev:\n"
        "    host: localhost\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cognito_mod, "get_config_path", lambda: config_path)
    monkeypatch.setattr(
        cognito_mod,
        "resolve_context",
        lambda **_kwargs: SimpleNamespace(client_id="atlas", database_name="app"),
    )

    written = cognito_mod._write_pool_id_to_tapdb_config(Environment.dev, "pool-1")
    assert written == config_path
    assert "cognito_user_pool_id: pool-1" in config_path.read_text(encoding="utf-8")

    json_path = tmp_path / "tapdb-config.json"
    monkeypatch.setattr(cognito_mod, "get_config_path", lambda: json_path)
    monkeypatch.setattr(
        cognito_mod,
        "resolve_context",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("no context")),
    )

    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "yaml":
            raise ModuleNotFoundError("yaml unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    written = cognito_mod._write_pool_id_to_tapdb_config(Environment.test, "pool-2")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert written == json_path
    assert payload["environments"]["test"]["cognito_user_pool_id"] == "pool-2"


def test_cognito_run_daycog_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cognito_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="ok\n", stderr=""),
    )
    assert cognito_mod._run_daycog(["status"]) == "ok"

    monkeypatch.setattr(
        cognito_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=2, stdout="", stderr="boom"),
    )
    with pytest.raises(RuntimeError, match="boom"):
        cognito_mod._run_daycog(["status"])

    printed: list[str] = []
    monkeypatch.setattr(cognito_mod, "_run_daycog", lambda args, env=None: "printed")
    monkeypatch.setattr(cognito_mod.ccyo_out, "print_text", lambda text: printed.append(text))
    cognito_mod._run_daycog_printing(["status"])
    assert printed == ["printed"]


def test_cognito_build_setup_args_and_finalize_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = cognito_mod._build_daycog_setup_args(
        command="setup",
        selected_pool_name="tapdb-dev",
        region="us-east-1",
        domain_prefix="tapdb-dev",
        attach_domain=False,
        port=8911,
        callback_path="auth/callback",
        oauth_flows="code",
        scopes="openid,email",
        idps="COGNITO",
        password_min_length=12,
        mfa="off",
        profile="profile-1",
        client_name="tapdb",
        callback_url=None,
        logout_url=None,
        autoprovision=True,
        generate_secret=True,
        require_uppercase=False,
        require_lowercase=False,
        require_numbers=False,
        require_symbols=False,
        tags="team=platform",
    )
    assert "--domain-prefix" in args
    assert "--no-attach-domain" in args
    assert "--autoprovision" in args
    assert "--generate-secret" in args
    assert "--no-require-uppercase" in args
    assert "--no-require-lowercase" in args
    assert "--no-require-numbers" in args
    assert "--no-require-symbols" in args
    assert "--tags" in args

    monkeypatch.setattr(
        cognito_mod,
        "_resolve_daycog_pool_id_after_setup",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("missing pool")),
    )
    with pytest.raises(typer.Exit):
        cognito_mod._finalize_setup_binding(
            env=Environment.dev,
            selected_pool_name="tapdb-dev",
            selected_client_name="tapdb",
            region="us-east-1",
        )

    monkeypatch.setattr(
        cognito_mod,
        "_resolve_daycog_pool_id_after_setup",
        lambda **_kwargs: ("pool-1", "tapdb-dev.us-east-1"),
    )
    monkeypatch.setattr(
        cognito_mod,
        "_find_pool_context_by_id",
        lambda *args, **kwargs: (
            "tapdb-dev.us-east-1",
            {"COGNITO_CLIENT_NAME": "tapdb"},
        ),
    )
    monkeypatch.setattr(cognito_mod, "_write_pool_id_to_tapdb_config", lambda *_args: Path("/tmp/tapdb-config.yaml"))
    cognito_mod._finalize_setup_binding(
        env=Environment.dev,
        selected_pool_name="tapdb-dev",
        selected_client_name="tapdb",
        region="us-east-1",
    )


def test_cognito_bound_context_resolution_and_actor_user_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cognito_mod, "get_db_config_for_env", lambda _env: {})
    with pytest.raises(RuntimeError, match="No cognito_user_pool_id set"):
        cognito_mod._resolve_bound_daycog_context(Environment.dev)

    monkeypatch.setattr(
        cognito_mod,
        "get_db_config_for_env",
        lambda _env: {
            "cognito_user_pool_id": "pool-1",
            "host": "db.local",
            "port": "5432",
            "user": "tapdb",
            "password": "",
            "database": "tapdb_dev",
            "engine_type": "local",
        },
    )
    monkeypatch.setattr(
        cognito_mod,
        "_find_pool_context_by_id",
        lambda *args, **kwargs: (
            "tapdb-dev.us-east-1.tapdb",
            {"COGNITO_CLIENT_NAME": "tapdb", "COGNITO_REGION": "us-east-1"},
        ),
    )
    pool_id, context_name, values, proc_env = cognito_mod._resolve_bound_daycog_context(Environment.dev)
    assert pool_id == "pool-1"
    assert context_name == "tapdb-dev.us-east-1.tapdb"
    assert proc_env["COGNITO_CLIENT_NAME"] == "tapdb"
    assert values["COGNITO_REGION"] == "us-east-1"

    monkeypatch.setattr(
        cognito_mod,
        "_resolve_bound_daycog_context",
        lambda _env: (_ for _ in ()).throw(RuntimeError("not bound")),
    )
    selected_pool, proc_env, region, profile = cognito_mod._resolve_pool_command_context(
        Environment.dev,
        pool_name=None,
        region=None,
        profile=None,
    )
    assert selected_pool
    assert proc_env is None
    assert region == "us-east-1"
    assert profile is None

    monkeypatch.setattr(
        cognito_mod,
        "_resolve_bound_daycog_context",
        lambda _env: (
            "pool-1",
            "tapdb-dev.us-east-1.tapdb",
            {"COGNITO_REGION": "us-west-2", "AWS_PROFILE": "dev-profile"},
            {"COGNITO_REGION": "us-west-2", "AWS_PROFILE": "dev-profile"},
        ),
    )
    _, proc_env, region, profile = cognito_mod._resolve_pool_command_context(
        Environment.dev,
        pool_name="tapdb-dev",
        region=None,
        profile=None,
    )
    assert proc_env["AWS_PROFILE"] == "dev-profile"
    assert region == "us-west-2"
    assert profile == "dev-profile"

    with pytest.raises(RuntimeError, match="email is required"):
        cognito_mod._ensure_actor_user_row(Environment.dev, email="", role="user")
    with pytest.raises(RuntimeError, match="invalid role"):
        cognito_mod._ensure_actor_user_row(Environment.dev, email="alice@example.com", role="owner")

    monkeypatch.setattr(cognito_mod, "TAPDBConnection", _FakeConn)
    monkeypatch.setattr(
        cognito_mod,
        "create_or_get",
        lambda *_args, **_kwargs: (SimpleNamespace(is_active=False), True),
    )
    with pytest.raises(RuntimeError, match="is inactive"):
        cognito_mod._ensure_actor_user_row(
            Environment.dev,
            email="alice@example.com",
            role="admin",
        )

    monkeypatch.setattr(
        cognito_mod,
        "create_or_get",
        lambda *_args, **_kwargs: (SimpleNamespace(is_active=True), True),
    )
    cognito_mod._ensure_actor_user_row(
        Environment.dev,
        email="alice@example.com",
        role="admin",
        display_name="Alice",
    )


def test_cognito_setup_and_setup_with_google_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cognito_mod, "get_db_config_for_env", lambda _env: {"ui_port": "8911"})

    result = runner.invoke(cognito_mod.cognito_app, ["setup", "dev", "--port", "9000"])
    assert result.exit_code == 1

    result = runner.invoke(
        cognito_mod.cognito_app,
        ["setup", "dev", "--client-name", "custom"],
    )
    assert result.exit_code == 1

    captured: list[list[str]] = []
    monkeypatch.setattr(cognito_mod, "_run_daycog", lambda args, env=None: captured.append(list(args)) or "")
    monkeypatch.setattr(cognito_mod, "_finalize_setup_binding", lambda **_kwargs: None)
    result = runner.invoke(
        cognito_mod.cognito_app,
        [
            "setup-with-google",
            "dev",
            "--google-client-json",
            "/tmp/google.json",
            "--google-scopes",
            "openid profile",
        ],
    )
    assert result.exit_code == 0
    assert "--google-client-json" in captured[0]
    assert "--google-scopes" in captured[0]

    result = runner.invoke(cognito_mod.cognito_app, ["setup-with-google", "dev", "--port", "9000"])
    assert result.exit_code == 1
    result = runner.invoke(
        cognito_mod.cognito_app,
        ["setup-with-google", "dev", "--client-name", "custom"],
    )
    assert result.exit_code == 1


def test_cognito_status_and_management_command_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cognito_mod, "get_db_config_for_env", lambda _env: {})
    result = runner.invoke(cognito_mod.cognito_app, ["status", "dev"])
    assert result.exit_code == 1

    monkeypatch.setattr(
        cognito_mod,
        "get_db_config_for_env",
        lambda _env: {"cognito_user_pool_id": "pool-1"},
    )
    monkeypatch.setattr(
        cognito_mod,
        "_find_pool_context_by_id",
        lambda *_args, **_kwargs: ("ctx", {"COGNITO_CLIENT_NAME": "wrong"}),
    )
    result = runner.invoke(cognito_mod.cognito_app, ["status", "dev"])
    assert result.exit_code == 1

    monkeypatch.setattr(
        cognito_mod,
        "_find_pool_context_by_id",
        lambda *_args, **_kwargs: (
            "ctx",
            {
                "COGNITO_CLIENT_NAME": "tapdb",
                "COGNITO_CALLBACK_URL": "https://localhost:9999/auth/callback",
            },
        ),
    )
    monkeypatch.setattr(cognito_mod, "_validate_bound_cognito_uris", lambda *_args, **_kwargs: (8911, "config", ["bad uri"], []))
    monkeypatch.setattr(cognito_mod, "_ui_pid_file_for_env", lambda _env: Path("/tmp/ui.pid"))
    result = runner.invoke(cognito_mod.cognito_app, ["status", "dev"])
    assert result.exit_code == 1

    monkeypatch.setattr(cognito_mod, "_validate_bound_cognito_uris", lambda *_args, **_kwargs: (8911, "config", [], ["ok"]))
    result = runner.invoke(cognito_mod.cognito_app, ["status", "dev"])
    assert result.exit_code == 0

    captured: list[list[str]] = []
    monkeypatch.setattr(
        cognito_mod,
        "_resolve_pool_command_context",
        lambda *_args, **_kwargs: ("tapdb-dev", {"AWS_PROFILE": "dev-profile"}, "us-east-1", "dev-profile"),
    )
    monkeypatch.setattr(cognito_mod, "_run_daycog_printing", lambda args, env=None: captured.append(list(args)))
    runner.invoke(
        cognito_mod.cognito_app,
        [
            "add-app",
            "dev",
            "--app-name",
            "web",
            "--callback-url",
            "https://localhost:8911/auth/callback",
            "--logout-url",
            "https://localhost:8911/",
            "--generate-secret",
            "--set-default",
        ],
    )
    assert "--logout-url" in captured[-1]
    assert "--generate-secret" in captured[-1]
    assert "--set-default" in captured[-1]

    result = runner.invoke(cognito_mod.cognito_app, ["edit-app", "dev"])
    assert result.exit_code == 1
    captured.clear()
    result = runner.invoke(
        cognito_mod.cognito_app,
        [
            "edit-app",
            "dev",
            "--app-name",
            "web",
            "--client-id",
            "client-1",
            "--new-app-name",
            "web-next",
            "--callback-url",
            "https://localhost:8911/auth/callback",
            "--logout-url",
            "https://localhost:8911/",
            "--oauth-flows",
            "code",
            "--scopes",
            "openid,email",
            "--idp",
            "COGNITO",
            "--set-default",
        ],
    )
    assert result.exit_code == 0
    assert "--client-id" in captured[-1]
    assert "--new-app-name" in captured[-1]

    result = runner.invoke(cognito_mod.cognito_app, ["remove-app", "dev"])
    assert result.exit_code == 1
    captured.clear()
    result = runner.invoke(
        cognito_mod.cognito_app,
        [
            "remove-app",
            "dev",
            "--client-id",
            "client-1",
            "--force",
            "--keep-config",
        ],
    )
    assert result.exit_code == 0
    assert "--client-id" in captured[-1]
    assert "--force" in captured[-1]
    assert "--keep-config" in captured[-1]

    result = runner.invoke(cognito_mod.cognito_app, ["add-google-idp", "dev"])
    assert result.exit_code == 1
    captured.clear()
    result = runner.invoke(
        cognito_mod.cognito_app,
        [
            "add-google-idp",
            "dev",
            "--app-name",
            "web",
            "--google-client-id",
            "gid",
            "--google-client-secret",
            "gsecret",
            "--google-client-json",
            "/tmp/google.json",
        ],
    )
    assert result.exit_code == 0
    assert "--google-client-json" in captured[-1]


def test_cognito_fix_auth_flows_config_commands_and_add_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cognito_mod,
        "_resolve_bound_daycog_context",
        lambda _env: (_ for _ in ()).throw(RuntimeError("not bound")),
    )
    result = runner.invoke(cognito_mod.cognito_app, ["fix-auth-flows", "dev"])
    assert result.exit_code == 1

    printed: list[list[str]] = []
    monkeypatch.setattr(
        cognito_mod,
        "_resolve_bound_daycog_context",
        lambda _env: ("pool-1", "ctx", {"COGNITO_CLIENT_NAME": "tapdb"}, {"COGNITO_CLIENT_NAME": "tapdb"}),
    )
    monkeypatch.setattr(cognito_mod, "_run_daycog_printing", lambda args, env=None: printed.append(list(args)))
    result = runner.invoke(cognito_mod.cognito_app, ["fix-auth-flows", "dev"])
    assert result.exit_code == 0
    assert printed[-1] == ["fix-auth-flows"]

    monkeypatch.setattr(
        cognito_mod,
        "_resolve_pool_command_context",
        lambda *_args, **_kwargs: ("tapdb-dev", {"AWS_PROFILE": "dev-profile"}, "us-east-1", "dev-profile"),
    )
    printed.clear()
    assert runner.invoke(cognito_mod.cognito_app, ["config", "print", "dev"]).exit_code == 0
    assert printed[-1][:3] == ["config", "print", "--pool-name"]
    assert runner.invoke(cognito_mod.cognito_app, ["config", "create", "dev"]).exit_code == 0
    assert printed[-1][:3] == ["config", "create", "--pool-name"]
    assert "--profile" in printed[-1]
    assert runner.invoke(cognito_mod.cognito_app, ["config", "update", "dev"]).exit_code == 0
    assert printed[-1][:3] == ["config", "update", "--pool-name"]
    assert "--profile" in printed[-1]

    result = runner.invoke(
        cognito_mod.cognito_app,
        ["add-user", "dev", "alice@example.com", "--password", "secret", "--role", "owner"],
    )
    assert result.exit_code == 1

    monkeypatch.setattr(
        cognito_mod,
        "_resolve_bound_daycog_context",
        lambda _env: (_ for _ in ()).throw(RuntimeError("not bound")),
    )
    result = runner.invoke(
        cognito_mod.cognito_app,
        ["add-user", "dev", "alice@example.com", "--password", "secret"],
    )
    assert result.exit_code == 1

    monkeypatch.setattr(
        cognito_mod,
        "_resolve_bound_daycog_context",
        lambda _env: ("pool-1", "ctx", {"COGNITO_CLIENT_NAME": "tapdb"}, {"COGNITO_CLIENT_NAME": "tapdb"}),
    )
    monkeypatch.setattr(cognito_mod, "_run_daycog", lambda args, env=None: "")
    monkeypatch.setattr(
        cognito_mod,
        "_ensure_actor_user_row",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("actor sync failed")),
    )
    result = runner.invoke(
        cognito_mod.cognito_app,
        ["add-user", "dev", "alice@example.com", "--password", "secret"],
    )
    assert result.exit_code == 1
