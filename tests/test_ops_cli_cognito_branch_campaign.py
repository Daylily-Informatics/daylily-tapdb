"""Behavior coverage for legacy Cognito/Daycog CLI control flow."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import typer
import yaml

import daylily_tapdb.cli.cognito as cognito
from daylily_tapdb.cli.db import Environment


def _ctx_values(**updates: str) -> dict[str, str]:
    values = {
        "COGNITO_USER_POOL_ID": "pool-id",
        "COGNITO_REGION": "us-west-2",
        "COGNITO_CLIENT_NAME": "tapdb",
        "AWS_PROFILE": "profile",
    }
    values.update(updates)
    return values


def _setup_kwargs(**updates):
    values = dict(
        pool_name="pool",
        client_name="tapdb",
        profile="profile",
        region="us-west-2",
        domain_prefix=None,
        attach_domain=True,
        port=8911,
        callback_path="/auth/callback",
        callback_url=None,
        logout_url=None,
        autoprovision=True,
        generate_secret=False,
        oauth_flows="code",
        scopes="openid,email",
        idps="COGNITO",
        password_min_length=8,
        require_uppercase=True,
        require_lowercase=True,
        require_numbers=True,
        require_symbols=False,
        mfa="off",
        tags=None,
    )
    values.update(updates)
    return values


@pytest.fixture(autouse=True)
def _outputs(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("error", "warning", "success", "print_text"):
        monkeypatch.setattr(getattr(cognito, "ccyo_out"), name, lambda *_a, **_k: None)


def test_pid_path_split_and_process_inspection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_context = SimpleNamespace(ui_dir=lambda: tmp_path)
    monkeypatch.setattr(
        "daylily_tapdb.cli.context.resolve_context", lambda **_kwargs: fake_context
    )
    assert cognito._ui_pid_file() == tmp_path / "ui.pid"
    assert cognito._split_uri_values(" one, two\nthree ") == ["one", "two", "three"]
    assert cognito._iter_cognito_uri_values(
        {"COGNITO_CALLBACK_URLS": "https://a, https://b", "OTHER": "x"}
    ) == [
        ("COGNITO_CALLBACK_URLS", "https://a"),
        ("COGNITO_CALLBACK_URLS", "https://b"),
    ]

    pid = tmp_path / "ui.pid"
    pid.write_text("42", encoding="utf-8")
    monkeypatch.setattr(cognito, "_ui_pid_file", lambda: pid)
    monkeypatch.setattr(cognito.os, "kill", lambda *_a: None)
    monkeypatch.setattr(
        cognito.subprocess,
        "run",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("ps unavailable")),
    )
    assert cognito._detect_running_ui_port()[1] == "could not inspect ui process"
    monkeypatch.setattr(
        cognito.subprocess,
        "run",
        lambda *_a, **_k: SimpleNamespace(returncode=1, stdout="", stderr=""),
    )
    assert cognito._detect_running_ui_port()[1] == "could not inspect ui process"
    monkeypatch.setattr(
        cognito.subprocess,
        "run",
        lambda *_a, **_k: SimpleNamespace(returncode=0, stdout="tapdb ui", stderr=""),
    )
    assert cognito._detect_running_ui_port()[1] == "ui process port not detected"


def test_expected_port_default_and_uri_validation_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cognito, "get_db_config", lambda: {"ui_port": ""})
    monkeypatch.setattr(cognito, "_detect_running_ui_port", lambda: (None, "stopped"))
    assert cognito._resolve_expected_ui_port() == (8911, "default (stopped)")
    monkeypatch.setattr(cognito, "_resolve_expected_ui_port", lambda: (8911, "cfg"))
    assert cognito._validate_bound_cognito_uris({}) == (8911, "cfg", [], [])
    _, _, errors, notices = cognito._validate_bound_cognito_uris(
        {
            "COGNITO_CALLBACK_URLS": (
                "broken https://remote.example/callback "
                "https://remote.example:8443/callback "
                "https://localhost:8911/callback"
            ),
            "COGNITO_LOGOUT_URL": "http://remote.example/logout",
        }
    )
    assert any("invalid URI" in item for item in errors)
    assert any("must use https" in item for item in errors)
    assert len(notices) == 3


def test_context_parsing_loading_and_scoring(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert cognito._sanitize_filename_part(" ! ") == "app"
    assert cognito._sanitize_filename_part("hello world") == "hello-world"
    assert cognito._parse_daycog_context_name("not-a-context") == ("", "", "")
    assert cognito._parse_daycog_context_name("pool.us-west-2") == (
        "pool",
        "us-west-2",
        "",
    )
    assert cognito._parse_daycog_context_name("pool.us-west-2.tapdb") == (
        "pool",
        "us-west-2",
        "tapdb",
    )

    path = tmp_path / "daycog.yaml"
    monkeypatch.setattr(cognito, "_daycog_config_path", lambda: path)
    with pytest.raises(RuntimeError, match="not found"):
        cognito._load_daycog_contexts()
    path.write_text("- invalid\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid"):
        cognito._load_daycog_contexts()
    path.write_text(
        yaml.safe_dump(
            {
                "active_context": "active",
                "contexts": {
                    "active": {"A": 1, "NONE": None},
                    "skip": "not-a-map",
                },
            }
        ),
        encoding="utf-8",
    )
    assert cognito._load_daycog_contexts() == ("active", {"active": {"A": "1"}})
    path.write_text(yaml.safe_dump({"contexts": []}), encoding="utf-8")
    assert cognito._load_daycog_contexts() == ("", {})

    pool_score = cognito._score_daycog_context_match(
        "pool.us-west-2",
        _ctx_values(),
        active_name="pool.us-west-2",
        prefer_region="us-west-2",
        prefer_client_name="tapdb",
    )
    app_score = cognito._score_daycog_context_match(
        "pool.us-west-2.tapdb", _ctx_values()
    )
    mismatch = cognito._score_daycog_context_match(
        "pool.us-east-1.other", _ctx_values(COGNITO_CLIENT_NAME="tapdb")
    )
    assert pool_score[0] == 120
    assert app_score[0] == 60
    assert mismatch[0] == 0


def test_find_context_and_setup_pool_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contexts = {
        "pool.us-west-2.other": _ctx_values(COGNITO_CLIENT_NAME="other"),
        "pool.us-west-2.tapdb": _ctx_values(),
        "wrong.us-west-2": _ctx_values(COGNITO_USER_POOL_ID="other"),
    }
    monkeypatch.setattr(cognito, "_load_daycog_contexts", lambda: ("", contexts))
    name, values = cognito._find_pool_context_by_id(
        "pool-id", prefer_region="us-west-2", prefer_client_name="tapdb"
    )
    assert name == "pool.us-west-2.tapdb"
    assert values["COGNITO_CLIENT_NAME"] == "tapdb"
    with pytest.raises(RuntimeError, match="No Daycog"):
        cognito._find_pool_context_by_id("absent")

    monkeypatch.setattr(
        cognito,
        "_load_daycog_contexts",
        lambda: (
            "active",
            {
                "pool.us-west-2": {},
                "pool.us-west-2.tapdb": {},
                "active": {},
                "pool.us-west-2.other": _ctx_values(),
            },
        ),
    )
    assert cognito._resolve_daycog_pool_id_after_setup(
        pool_name="pool", region="us-west-2", client_name="tapdb"
    ) == ("pool-id", "pool.us-west-2.other")

    monkeypatch.setattr(cognito, "_load_daycog_contexts", lambda: ("", {"other": {}}))
    with pytest.raises(RuntimeError, match="pool ID was not found"):
        cognito._resolve_daycog_pool_id_after_setup(
            pool_name="pool", region="us-west-2", client_name="tapdb"
        )


def test_setup_pool_resolution_direct_and_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cognito,
        "_load_daycog_contexts",
        lambda: ("active", {"pool.us-west-2": _ctx_values(), "active": _ctx_values()}),
    )
    assert (
        cognito._resolve_daycog_pool_id_after_setup(
            pool_name="pool", region="us-west-2", client_name="tapdb"
        )[1]
        == "pool.us-west-2"
    )
    monkeypatch.setattr(
        cognito,
        "_load_daycog_contexts",
        lambda: ("active", {"active": _ctx_values()}),
    )
    assert (
        cognito._resolve_daycog_pool_id_after_setup(
            pool_name="pool", region="us-west-2", client_name="tapdb"
        )[1]
        == "active"
    )


@pytest.mark.parametrize(
    "root,message",
    [
        ([], "invalid TapDB config"),
        ({"meta": {}}, "explicit target config"),
        ({"target": {}}, "meta mapping"),
    ],
)
def test_write_pool_id_rejects_config_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    root: object,
    message: str,
) -> None:
    path = tmp_path / "tapdb.yaml"
    path.write_text(yaml.safe_dump(root), encoding="utf-8")
    monkeypatch.setattr(cognito, "get_config_path", lambda: path)
    with pytest.raises(RuntimeError, match=message):
        cognito._write_pool_id_to_tapdb_config("pool")


def test_write_pool_id_requires_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "missing.yaml"
    monkeypatch.setattr(cognito, "get_config_path", lambda: path)
    with pytest.raises(RuntimeError, match="explicit target config"):
        cognito._write_pool_id_to_tapdb_config("pool")


def test_daycog_subprocess_success_failure_and_printing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def _run(cmd, **_kwargs):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout=" output \n", stderr="")

    monkeypatch.setattr(cognito.subprocess, "run", _run)
    assert cognito._run_daycog(["status"], env={"A": "B"}) == "output"
    assert calls == [["daycog", "status"]]
    cognito._run_daycog_printing(["status"])

    monkeypatch.setattr(
        cognito.subprocess,
        "run",
        lambda *_a, **_k: SimpleNamespace(returncode=3, stdout="", stderr="bad"),
    )
    with pytest.raises(RuntimeError, match="bad"):
        cognito._run_daycog(["status"])
    monkeypatch.setattr(
        cognito.subprocess,
        "run",
        lambda *_a, **_k: SimpleNamespace(returncode=4, stdout="", stderr=""),
    )
    with pytest.raises(RuntimeError, match=r"daycog failed \(4\)"):
        cognito._run_daycog(["status"])
    monkeypatch.setattr(cognito, "_run_daycog", lambda *_a, **_k: "")
    cognito._run_daycog_printing(["quiet"])


def test_build_setup_args_all_false_options() -> None:
    args = cognito._build_daycog_setup_args(
        command="setup",
        selected_pool_name="pool",
        region="us-west-2",
        domain_prefix=None,
        attach_domain=False,
        port=8911,
        callback_path="callback",
        oauth_flows="code",
        scopes="openid",
        idps="COGNITO",
        password_min_length=8,
        mfa="off",
        profile=None,
        client_name=None,
        callback_url="https://remote/callback",
        logout_url="https://remote/logout",
        autoprovision=False,
        generate_secret=False,
        require_uppercase=False,
        require_lowercase=False,
        require_numbers=False,
        require_symbols=True,
        tags=None,
    )
    assert "--no-attach-domain" in args
    assert "--no-require-uppercase" in args
    assert "--no-require-lowercase" in args
    assert "--no-require-numbers" in args
    assert "--require-symbols" in args
    assert "--profile" not in args


def test_finalize_setup_binding_success_and_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        cognito,
        "_resolve_daycog_pool_id_after_setup",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("missing pool")),
    )
    with pytest.raises(typer.Exit):
        cognito._finalize_setup_binding(
            selected_pool_name="pool", selected_client_name="tapdb", region="r"
        )

    monkeypatch.setattr(
        cognito,
        "_resolve_daycog_pool_id_after_setup",
        lambda **_kwargs: ("pool-id", "context"),
    )
    monkeypatch.setattr(
        cognito,
        "_find_pool_context_by_id",
        lambda *_a, **_k: ("context", _ctx_values(COGNITO_CLIENT_NAME="wrong")),
    )
    with pytest.raises(typer.Exit):
        cognito._finalize_setup_binding(
            selected_pool_name="pool", selected_client_name="tapdb", region="r"
        )

    monkeypatch.setattr(
        cognito,
        "_find_pool_context_by_id",
        lambda *_a, **_k: ("context", _ctx_values()),
    )
    target = tmp_path / "tapdb.yaml"
    monkeypatch.setattr(cognito, "_write_pool_id_to_tapdb_config", lambda _pool: target)
    cognito._finalize_setup_binding(
        selected_pool_name="pool", selected_client_name="tapdb", region="r"
    )


def test_bound_and_pool_context_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cognito, "get_db_config", lambda: {})
    with pytest.raises(RuntimeError, match="No cognito"):
        cognito._resolve_bound_daycog_context(Environment.target)

    monkeypatch.setattr(
        cognito, "get_db_config", lambda: {"cognito_user_pool_id": "pool-id"}
    )
    monkeypatch.setattr(
        cognito,
        "_find_pool_context_by_id",
        lambda *_a, **_k: ("context", _ctx_values()),
    )
    result = cognito._resolve_bound_daycog_context(Environment.target)
    assert result[0:3] == ("pool-id", "context", _ctx_values())
    assert result[3]["AWS_PROFILE"] == "profile"

    monkeypatch.setattr(
        cognito,
        "_resolve_bound_daycog_context",
        lambda _env: (
            "pool-id",
            "context",
            _ctx_values(),
            {"AWS_PROFILE": "profile"},
        ),
    )
    assert cognito._resolve_pool_command_context(
        Environment.target, pool_name="pool", region=None, profile=None
    ) == ("pool", {"AWS_PROFILE": "profile"}, "us-west-2", "profile")

    monkeypatch.setattr(
        cognito,
        "_resolve_bound_daycog_context",
        lambda _env: ("pool-id", "context", {}, {}),
    )
    with pytest.raises(RuntimeError, match="region"):
        cognito._resolve_pool_command_context(
            Environment.target, pool_name="pool", region=None, profile="profile"
        )
    with pytest.raises(RuntimeError, match="profile"):
        cognito._resolve_pool_command_context(
            Environment.target, pool_name="pool", region="region", profile=None
        )


def test_required_client_and_actor_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    assert (
        cognito._validate_required_client_name(_ctx_values(), context_label="ctx")
        == "tapdb"
    )
    with pytest.raises(RuntimeError, match=r"got: \(missing\)"):
        cognito._validate_required_client_name({}, context_label="ctx")
    with pytest.raises(RuntimeError, match="email is required"):
        cognito._ensure_actor_user_row(Environment.target, email=" ")

    cfg = {
        "engine_type": "local",
        "iam_auth": "false",
        "region": "r",
        "host": "h",
        "port": "1",
        "hostaddr": "",
        "user": "u",
        "secret_arn": "",
        "database": "d",
        "domain_code": "Z",
        "owner_repo_name": "repo",
        "schema_name": "schema",
        "config_path": "/abs/tapdb-config.yaml",
    }
    monkeypatch.setattr(cognito, "get_db_config", lambda: cfg)

    class _Conn:
        def __init__(self, **kwargs):
            assert kwargs["db_pass"] is None
            assert kwargs["iam_auth"] is False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def session_scope(self, *, commit):
            assert commit is True
            return self

    monkeypatch.setattr(cognito, "TAPDBConnection", _Conn)
    monkeypatch.setattr(
        cognito,
        "create_or_get",
        lambda *_a, **_k: (SimpleNamespace(is_active=False), False),
    )
    with pytest.raises(RuntimeError, match="inactive"):
        cognito._ensure_actor_user_row(Environment.target, email="USER@example.com")


@pytest.mark.parametrize("command", ["setup", "setup-with-google"])
def test_setup_input_validation(command: str, monkeypatch: pytest.MonkeyPatch) -> None:
    function = (
        cognito.cognito_setup
        if command == "setup"
        else cognito.cognito_setup_with_google
    )
    monkeypatch.setattr(cognito, "get_db_config", lambda: {"ui_port": "8911"})
    kwargs = _setup_kwargs(region=None)
    if command == "setup-with-google":
        kwargs.update(
            google_client_id=None,
            google_client_secret=None,
            google_client_json=None,
            google_scopes="",
        )
    with pytest.raises(RuntimeError, match="region"):
        function(**kwargs)

    kwargs.update(region="r", port=9000)
    with pytest.raises(typer.Exit):
        function(**kwargs)

    kwargs.update(port=None, client_name="wrong")
    with pytest.raises(typer.Exit):
        function(**kwargs)


def test_setup_success_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    finalized: list[dict[str, str]] = []
    monkeypatch.setattr(cognito, "get_db_config", lambda: {"ui_port": "8911"})
    monkeypatch.setattr(
        cognito, "_run_daycog", lambda args, **_k: calls.append(args) or ""
    )
    monkeypatch.setattr(
        cognito, "_finalize_setup_binding", lambda **kwargs: finalized.append(kwargs)
    )
    monkeypatch.setattr(cognito, "_default_pool_name", lambda: "default-pool")
    cognito.cognito_setup(**_setup_kwargs(pool_name=None, client_name=None, port=None))
    assert calls[-1][0] == "setup"
    assert finalized[-1]["selected_pool_name"] == "default-pool"

    google = _setup_kwargs(
        pool_name=None,
        client_name=None,
        google_client_id="gid",
        google_client_secret="secret",
        google_client_json="client.json",
        google_scopes="openid",
    )
    cognito.cognito_setup_with_google(**google)
    assert calls[-1][0] == "setup-with-google"
    assert calls[-1][-8:] == [
        "--google-client-id",
        "gid",
        "--google-client-secret",
        "secret",
        "--google-client-json",
        "client.json",
        "--google-scopes",
        "openid",
    ]

    google.update(
        pool_name="pool",
        client_name="tapdb",
        port=8911,
        google_client_id=None,
        google_client_secret=None,
        google_client_json=None,
        google_scopes="",
    )
    cognito.cognito_setup_with_google(**google)
    assert "--google-scopes" not in calls[-1]


def test_status_variants(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cognito, "get_db_config", lambda: {})
    with pytest.raises(typer.Exit):
        cognito.cognito_status()

    monkeypatch.setattr(
        cognito, "get_db_config", lambda: {"cognito_user_pool_id": "pool-id"}
    )
    monkeypatch.setattr(cognito, "_ui_pid_file", lambda: tmp_path / "ui.pid")
    monkeypatch.setattr(
        cognito,
        "_find_pool_context_by_id",
        lambda *_a, **_k: ("context", _ctx_values(COGNITO_CLIENT_NAME="wrong")),
    )
    with pytest.raises(typer.Exit):
        cognito.cognito_status()

    monkeypatch.setattr(
        cognito,
        "_find_pool_context_by_id",
        lambda *_a, **_k: ("context", _ctx_values()),
    )
    monkeypatch.setattr(
        cognito,
        "_validate_bound_cognito_uris",
        lambda _values: (8911, "config", ["bad one", "bad two"], ["seen"]),
    )
    with pytest.raises(typer.Exit):
        cognito.cognito_status()
    monkeypatch.setattr(
        cognito,
        "_validate_bound_cognito_uris",
        lambda _values: (8911, "config", [], []),
    )
    cognito.cognito_status()


def test_pool_and_app_commands_render_all_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], object]] = []
    monkeypatch.setattr(
        cognito,
        "_resolve_pool_command_context",
        lambda *_a, **_k: ("pool", {"BOUND": "1"}, "region", "profile"),
    )
    monkeypatch.setattr(
        cognito,
        "_run_daycog_printing",
        lambda args, env=None: calls.append((args, env)),
    )
    cognito.cognito_list_pools(profile="profile", region="region")
    cognito.cognito_list_apps(pool_name="pool", profile="profile", region="region")
    cognito.cognito_add_app(
        app_name="app",
        callback_url="https://callback",
        pool_name="pool",
        profile="profile",
        region="region",
        logout_url="https://logout",
        generate_secret=True,
        oauth_flows="code",
        scopes="openid",
        idps="COGNITO",
        set_default=True,
    )
    cognito.cognito_edit_app(
        app_name="app",
        client_id="client",
        new_app_name="new",
        callback_url="https://callback",
        logout_url="https://logout",
        oauth_flows="code",
        scopes="openid",
        idps="COGNITO",
        set_default=True,
        pool_name="pool",
        profile="profile",
        region="region",
    )
    cognito.cognito_remove_app(
        app_name="app",
        client_id="client",
        pool_name="pool",
        profile="profile",
        region="region",
        force=True,
        delete_config=False,
    )
    cognito.cognito_add_google_idp(
        app_name="app",
        client_id="client",
        pool_name="pool",
        profile="profile",
        region="region",
        google_client_id="gid",
        google_client_secret="secret",
        google_client_json="json",
        scopes="openid",
    )
    assert len(calls) == 6
    assert "--generate-secret" in calls[2][0]
    assert "--new-app-name" in calls[3][0]
    assert "--keep-config" in calls[4][0]
    assert "--google-client-secret" in calls[5][0]


def test_app_commands_minimal_and_selector_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        cognito,
        "_resolve_pool_command_context",
        lambda *_a, **_k: ("pool", None, "region", None),
    )
    monkeypatch.setattr(
        cognito, "_run_daycog_printing", lambda args, env=None: calls.append(args)
    )
    cognito.cognito_list_pools(profile=None, region="region")
    cognito.cognito_list_apps(pool_name="pool", profile=None, region="region")
    cognito.cognito_add_app(
        app_name="app",
        callback_url="url",
        pool_name="pool",
        profile=None,
        region="region",
        logout_url=None,
        generate_secret=False,
        oauth_flows="code",
        scopes="openid",
        idps="COGNITO",
        set_default=False,
    )
    cognito.cognito_edit_app(
        app_name="app",
        client_id=None,
        new_app_name=None,
        callback_url=None,
        logout_url=None,
        oauth_flows=None,
        scopes=None,
        idps=None,
        set_default=False,
        pool_name="pool",
        profile=None,
        region="region",
    )
    cognito.cognito_remove_app(
        app_name=None,
        client_id="client",
        pool_name="pool",
        profile=None,
        region="region",
        force=False,
        delete_config=True,
    )
    cognito.cognito_add_google_idp(
        app_name=None,
        client_id="client",
        pool_name="pool",
        profile=None,
        region="region",
        google_client_id=None,
        google_client_secret=None,
        google_client_json=None,
        scopes="openid",
    )
    assert all("--profile" not in args for args in calls)

    for function, kwargs in [
        (
            cognito.cognito_edit_app,
            dict(
                app_name=None,
                client_id=None,
                new_app_name=None,
                callback_url=None,
                logout_url=None,
                oauth_flows=None,
                scopes=None,
                idps=None,
                set_default=False,
                pool_name="pool",
                profile=None,
                region="r",
            ),
        ),
        (
            cognito.cognito_remove_app,
            dict(
                app_name=None,
                client_id=None,
                pool_name="pool",
                profile=None,
                region="r",
                force=False,
                delete_config=True,
            ),
        ),
        (
            cognito.cognito_add_google_idp,
            dict(
                app_name=None,
                client_id=None,
                pool_name="pool",
                profile=None,
                region="r",
                google_client_id=None,
                google_client_secret=None,
                google_client_json=None,
                scopes="openid",
            ),
        ),
    ]:
        with pytest.raises(typer.Exit):
            function(**kwargs)


def test_config_fix_flow_and_add_user_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        cognito,
        "_resolve_pool_command_context",
        lambda *_a, **_k: ("pool", {"BOUND": "1"}, "region", "profile"),
    )
    monkeypatch.setattr(
        cognito, "_run_daycog_printing", lambda args, env=None: calls.append(args)
    )
    cognito.cognito_config_print(pool_name="pool", region="region")
    cognito.cognito_config_create(pool_name="pool", profile="profile", region="region")
    cognito.cognito_config_update(pool_name="pool", profile="profile", region="region")
    assert [args[1] for args in calls] == ["print", "create", "update"]

    monkeypatch.setattr(
        cognito,
        "_resolve_bound_daycog_context",
        lambda _env: (_ for _ in ()).throw(RuntimeError("not bound")),
    )
    with pytest.raises(typer.Exit):
        cognito.cognito_fix_auth_flows()
    monkeypatch.setattr(
        cognito,
        "_resolve_bound_daycog_context",
        lambda _env: ("pool-id", "ctx", _ctx_values(), {"A": "B"}),
    )
    cognito.cognito_fix_auth_flows()

    with pytest.raises(typer.Exit):
        cognito.cognito_add_user(
            email="user@example.com",
            password="pw",
            role="owner",
            display_name=None,
            no_verify=True,
        )

    monkeypatch.setattr(
        cognito,
        "_resolve_bound_daycog_context",
        lambda _env: (_ for _ in ()).throw(RuntimeError("not bound")),
    )
    with pytest.raises(typer.Exit):
        cognito.cognito_add_user(
            email="user@example.com",
            password="pw",
            role="user",
            display_name=None,
            no_verify=True,
        )

    monkeypatch.setattr(
        cognito,
        "_resolve_bound_daycog_context",
        lambda _env: ("pool-id", "ctx", _ctx_values(), {"A": "B"}),
    )
    daycog_calls: list[list[str]] = []
    monkeypatch.setattr(
        cognito, "_run_daycog", lambda args, env=None: daycog_calls.append(args) or ""
    )
    monkeypatch.setattr(cognito, "_ensure_actor_user_row", lambda *_a, **_k: None)
    monkeypatch.setattr(cognito, "_daycog_config_path", lambda: tmp_path / "daycog")
    cognito.cognito_add_user(
        email="user@example.com",
        password="pw",
        role="admin",
        display_name="User",
        no_verify=True,
    )
    cognito.cognito_add_user(
        email="user2@example.com",
        password="pw",
        role="user",
        display_name=None,
        no_verify=False,
    )
    assert "--no-verify" in daycog_calls[0]
    assert "--no-verify" not in daycog_calls[1]

    monkeypatch.setattr(
        cognito,
        "_ensure_actor_user_row",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db failed")),
    )
    with pytest.raises(typer.Exit):
        cognito.cognito_add_user(
            email="user@example.com",
            password="pw",
            role="user",
            display_name=None,
            no_verify=False,
        )


def test_config_commands_without_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        cognito,
        "_resolve_pool_command_context",
        lambda *_a, **_k: ("pool", None, "region", None),
    )
    monkeypatch.setattr(
        cognito, "_run_daycog_printing", lambda args, env=None: calls.append(args)
    )
    cognito.cognito_config_create(pool_name="pool", profile=None, region="region")
    cognito.cognito_config_update(pool_name="pool", profile=None, region="region")
    assert all("--profile" not in args for args in calls)
