"""TAPDB Cognito integration commands.

Operational lifecycle is delegated to the daylily-auth-cognito CLI (`daycog`).
TAPDB binds one explicitly selected flat auth-config file and its pool/client identities.
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import typer
import yaml
from cli_core_yo import ccyo_out
from rich.console import Console

from daylily_tapdb import TAPDBConnection
from daylily_tapdb.cli.db import Environment
from daylily_tapdb.cli.db_config import get_config_path, get_db_config
from daylily_tapdb.user_store import create_or_get

console = Console()
cognito_app = typer.Typer(help="Cognito auth integration commands (via daycog)")
config_app = typer.Typer(help="Explicit daycog flat auth-config utilities")
cognito_app.add_typer(config_app, name="config")
REQUIRED_COGNITO_CLIENT_NAME = "tapdb"


def _daycog_config_path() -> Path:
    """Use the selected current-format auth file; never infer a home context."""
    cfg = get_db_config()
    raw = str(cfg.get("cognito_auth_config_path") or "").strip()
    if not raw or not Path(raw).is_absolute():
        raise RuntimeError(
            "target.cognito_auth_config_path must name an explicit absolute "
            "daycog flat auth-config file. Set it with "
            "tapdb db-config update --cognito-auth-config-path."
        )
    return Path(raw)


def _load_daycog_config() -> dict[str, str]:
    from daylily_auth_cognito.cli.config import load_config_file

    return load_config_file(_daycog_config_path(), require_required_keys=True)


def _validate_required_client_name(values: dict[str, str], *, context_label: str) -> str:
    actual = str(values.get("COGNITO_CLIENT_NAME") or "").strip()
    if actual != REQUIRED_COGNITO_CLIENT_NAME:
        raise RuntimeError(
            f"{context_label} must select Cognito app client name "
            f"{REQUIRED_COGNITO_CLIENT_NAME!r} (got {actual!r})."
        )
    return actual


def _resolve_expected_ui_port() -> tuple[int, str]:
    cfg = get_db_config()
    raw = str(cfg.get("ui_port") or "").strip()
    if not raw.isdigit():
        raise RuntimeError("target.ui_port is required for Cognito callback configuration")
    return int(raw), "config target.ui_port"


def _validate_bound_cognito_uris(values: dict[str, str]) -> tuple[int, str, list[str], list[str]]:
    expected_port, source = _resolve_expected_ui_port()
    errors: list[str] = []
    notices: list[str] = []
    for key in ("COGNITO_CALLBACK_URL", "COGNITO_LOGOUT_URL"):
        uri = str(values.get(key) or "").strip()
        if not uri:
            continue
        parsed = urlparse(uri)
        if parsed.scheme != "https" or not parsed.netloc:
            errors.append(f"{key}: must be an absolute HTTPS URI: {uri}")
            continue
        port = parsed.port if parsed.port is not None else 443
        if parsed.hostname in {"localhost", "127.0.0.1", "::1"} and port != expected_port:
            errors.append(f"{key}: port {port} does not match target.ui_port {expected_port}")
            continue
        notices.append(f"{key}: {uri}")
    return expected_port, source, errors, notices


def _write_auth_binding_to_tapdb_config(values: dict[str, str]) -> Path:
    """Bind the native selected file's pool/client identities to the GUI target."""
    _validate_required_client_name(values, context_label="selected daycog auth config")
    for key in ("COGNITO_USER_POOL_ID", "COGNITO_APP_CLIENT_ID", "COGNITO_REGION"):
        if not str(values.get(key) or "").strip():
            raise RuntimeError(f"Selected auth config is missing {key}")
    _, _, errors, _ = _validate_bound_cognito_uris(values)
    if errors:
        raise RuntimeError("; ".join(errors))
    config_path = get_config_path()
    existing = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(existing, dict) or not isinstance(existing.get("target"), dict):
        raise RuntimeError(f"Invalid explicit TapDB target config: {config_path}")
    target = existing["target"]
    for key in (
        "COGNITO_USER_POOL_ID", "COGNITO_APP_CLIENT_ID", "COGNITO_CLIENT_NAME",
        "COGNITO_REGION", "COGNITO_DOMAIN", "COGNITO_CALLBACK_URL", "COGNITO_LOGOUT_URL",
    ):
        target[key.lower()] = str(values.get(key) or "")
    config_path.write_text(yaml.safe_dump(existing, sort_keys=False), encoding="utf-8")
    os.chmod(config_path, stat.S_IRUSR | stat.S_IWUSR)
    return config_path


def _run_daycog(args: list[str], env: Optional[dict[str, str]] = None) -> str:
    cmd = ["daycog", "--config", str(_daycog_config_path()), *args]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        msg = (result.stdout + "\n" + result.stderr).strip()
        raise RuntimeError(msg or f"daycog failed ({result.returncode})")
    return (result.stdout or "").strip()


def _run_daycog_printing(args: list[str], env: Optional[dict[str, str]] = None) -> None:
    out = _run_daycog(args, env=env)
    if out:
        ccyo_out.print_text(out)


def _build_daycog_setup_args(
    *,
    command: str,
    selected_pool_name: str,
    region: str,
    domain_prefix: Optional[str],
    attach_domain: bool,
    port: int,
    callback_path: str,
    oauth_flows: str,
    scopes: str,
    idps: str,
    password_min_length: int,
    mfa: str,
    profile: Optional[str],
    client_name: Optional[str],
    callback_url: Optional[str],
    logout_url: Optional[str],
    autoprovision: bool,
    generate_secret: bool,
    require_uppercase: bool,
    require_lowercase: bool,
    require_numbers: bool,
    require_symbols: bool,
    tags: Optional[str],
) -> list[str]:
    path = callback_path if callback_path.startswith("/") else f"/{callback_path}"
    resolved_callback_url = callback_url or f"https://localhost:{port}{path}"
    resolved_logout_url = logout_url or f"https://localhost:{port}/"

    args = [
        command,
        "--name",
        selected_pool_name,
        "--region",
        region,
        "--port",
        str(port),
        "--callback-path",
        path,
        "--callback-url",
        resolved_callback_url,
        "--logout-url",
        resolved_logout_url,
        "--oauth-flows",
        oauth_flows,
        "--scopes",
        scopes,
        "--idp",
        idps,
        "--password-min-length",
        str(password_min_length),
        "--mfa",
        mfa,
    ]
    if domain_prefix:
        args.extend(["--domain-prefix", domain_prefix])
    if attach_domain:
        args.append("--attach-domain")
    else:
        args.append("--no-attach-domain")
    if profile:
        args.extend(["--profile", profile])
    if client_name:
        args.extend(["--client-name", client_name])
    if autoprovision:
        args.append("--autoprovision")
    if generate_secret:
        args.append("--generate-secret")
    if require_uppercase:
        args.append("--require-uppercase")
    else:
        args.append("--no-require-uppercase")
    if require_lowercase:
        args.append("--require-lowercase")
    else:
        args.append("--no-require-lowercase")
    if require_numbers:
        args.append("--require-numbers")
    else:
        args.append("--no-require-numbers")
    if require_symbols:
        args.append("--require-symbols")
    else:
        args.append("--no-require-symbols")
    if tags:
        args.extend(["--tags", tags])
    return args


def _finalize_setup_binding(*, selected_client_name: str, region: str) -> None:
    values = _load_daycog_config()
    if values.get("COGNITO_CLIENT_NAME") != selected_client_name:
        raise RuntimeError("Native setup result selects a different Cognito client")
    if values.get("COGNITO_REGION") != region:
        raise RuntimeError("Native setup result selects a different Cognito region")
    cfg_path = _write_auth_binding_to_tapdb_config(values)
    ccyo_out.success(f"Bound native Cognito auth config to explicit TapDB target: {cfg_path}")
    ccyo_out.print_text(f"Auth config: {_daycog_config_path()}")


def _resolve_bound_daycog_config(
    env: Environment,
) -> tuple[str, Path, dict[str, str], dict[str, str]]:
    _ = env
    cfg = get_db_config()
    values = _load_daycog_config()
    _validate_required_client_name(values, context_label="selected daycog auth config")
    for key in ("cognito_user_pool_id", "cognito_app_client_id", "cognito_client_name", "cognito_region"):
        configured = str(cfg.get(key) or "").strip()
        if not configured or configured != str(values.get(key.upper()) or "").strip():
            raise RuntimeError(f"TapDB target.{key} must match the selected daycog auth config; use cognito bind")
    return values["COGNITO_USER_POOL_ID"], _daycog_config_path(), values, os.environ.copy()


def _resolve_pool_command_context(
    env: Environment,
    *,
    pool_name: Optional[str] = None,
    region: Optional[str] = None,
    profile: Optional[str] = None,
) -> tuple[str, Optional[dict[str, str]], str, str]:
    _ = env
    values = _load_daycog_config() if region is None or profile is None else {}
    selected_region = region if region is not None else values.get("COGNITO_REGION")
    selected_profile = profile if profile is not None else values.get("AWS_PROFILE")
    if not selected_region:
        raise RuntimeError("Explicit --region or selected auth-config COGNITO_REGION is required")
    if not selected_profile:
        raise RuntimeError("Explicit --profile or selected auth-config AWS_PROFILE is required")
    return pool_name or "", None, selected_region, selected_profile


def _ensure_actor_user_row(
    env: Environment,
    *,
    email: str,
    role: str = "user",
    display_name: Optional[str] = None,
) -> None:
    """Ensure TAPDB has an actor-backed user row for the Cognito identity."""
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        raise RuntimeError("email is required for TAPDB actor user creation")
    if role not in ("admin", "user"):
        raise RuntimeError(f"invalid role: {role}")
    _ = env
    cfg = get_db_config()
    engine_type = str(cfg["engine_type"]).strip().lower()
    iam_auth = str(cfg["iam_auth"]).strip().lower() in (
        "true",
        "1",
        "yes",
        "on",
    )
    region = str(cfg["region"]).strip()

    with TAPDBConnection(
        db_hostname=f"{cfg['host']}:{cfg['port']}",
        db_hostaddr=cfg.get("hostaddr") or None,
        db_user=cfg["user"],
        db_pass=cfg.get("password") if "password" in cfg else None,
        secret_arn=cfg.get("secret_arn") or None,
        db_name=cfg["database"],
        engine_type=engine_type,
        region=region,
        iam_auth=iam_auth,
        app_username=normalized_email,
        domain_code=str(cfg["domain_code"]),
        owner_repo_name=str(cfg["owner_repo_name"]),
        issuance_profile=cfg["issuance_profile"],
        schema_name=str(cfg["schema_name"]),
        tenant_id=str(cfg.get("tenant_id") or "") or None,
        additional_tenant_ids=tuple(cfg.get("additional_tenant_ids", ())),
        allow_global_rows=bool(cfg.get("allow_global_claims")),
        config_identity=str(cfg["config_path"]),
        aws_profile=str(cfg.get("aws_profile") or "") or None,
        sslrootcert=str(cfg.get("sslrootcert") or "") or None,
        server_port=int(cfg["server_port"]) if cfg.get("server_port") else None,
    ) as conn:
        with conn.session_scope(commit=True) as session:
            user, _ = create_or_get(
                session,
                login_identifier=normalized_email,
                email=normalized_email,
                display_name=display_name,
                role=role,
                is_active=True,
                require_password_change=False,
                password_hash=None,
                cognito_username=normalized_email,
            )
    if not user.is_active:
        raise RuntimeError(f"TAPDB user {normalized_email} is inactive")


@cognito_app.command("setup")
def cognito_setup(
    pool_name: str = typer.Option(..., "--pool-name", "-n", help="Explicit Cognito pool name"),
    client_name: Optional[str] = typer.Option(
        None,
        "--client-name",
        help="App client name (must be 'tapdb'; default: tapdb)",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile (required unless present in the selected native auth config)",
    ),
    region: Optional[str] = typer.Option(None, "--region", "-r", help="AWS region"),
    domain_prefix: Optional[str] = typer.Option(
        None,
        "--domain-prefix",
        help="Hosted UI domain prefix (default: pool name)",
    ),
    attach_domain: bool = typer.Option(
        True,
        "--attach-domain/--no-attach-domain",
        help="Attach/ensure Cognito Hosted UI domain",
    ),
    port: Optional[int] = typer.Option(
        None,
        "--port",
        "-p",
        help="Callback port for daycog (defaults to target.ui_port)",
    ),
    callback_path: str = typer.Option(
        "/auth/callback",
        "--callback-path",
        help="Callback path used with --port when --callback-url is not set",
    ),
    callback_url: Optional[str] = typer.Option(
        None, "--callback-url", help="Full callback URL override"
    ),
    logout_url: Optional[str] = typer.Option(
        None, "--logout-url", help="Optional logout URL for app client"
    ),
    autoprovision: bool = typer.Option(
        True,
        "--autoprovision/--no-autoprovision",
        help="Reuse existing app client by --client-name (default: enabled)",
    ),
    generate_secret: bool = typer.Option(
        False,
        "--generate-secret",
        help="Create app client with a generated secret",
    ),
    oauth_flows: str = typer.Option(
        "code", "--oauth-flows", help="Comma-separated OAuth flows"
    ),
    scopes: str = typer.Option(
        "openid,email,profile", "--scopes", help="Comma-separated OAuth scopes"
    ),
    idps: str = typer.Option(
        "COGNITO", "--idp", help="Comma-separated identity providers"
    ),
    password_min_length: int = typer.Option(
        8, "--password-min-length", help="Minimum password length"
    ),
    require_uppercase: bool = typer.Option(
        True,
        "--require-uppercase/--no-require-uppercase",
        help="Require uppercase characters in passwords",
    ),
    require_lowercase: bool = typer.Option(
        True,
        "--require-lowercase/--no-require-lowercase",
        help="Require lowercase characters in passwords",
    ),
    require_numbers: bool = typer.Option(
        True,
        "--require-numbers/--no-require-numbers",
        help="Require numbers in passwords",
    ),
    require_symbols: bool = typer.Option(
        False,
        "--require-symbols/--no-require-symbols",
        help="Require symbols in passwords",
    ),
    mfa: str = typer.Option("off", "--mfa", help="MFA mode: off|optional|required"),
    tags: Optional[str] = typer.Option(
        None, "--tags", help="Comma-separated tags in key=value format"
    ),
) -> None:
    """Create/reuse Cognito pool via daycog and bind pool ID into tapdb config."""
    cfg = get_db_config()
    if not region or not profile:
        raise RuntimeError("--region and --profile are required for TAPDB Cognito setup")
    configured_port, _ = _resolve_expected_ui_port()
    if port is None:
        selected_port = configured_port
    elif port != configured_port:
        ccyo_out.error(
            "--port does not match configured TAPDB UI port for explicit target: "
            f"{configured_port}"
        )
        raise typer.Exit(1)
    else:
        selected_port = port

    selected_pool_name = pool_name
    selected_client_name = client_name or REQUIRED_COGNITO_CLIENT_NAME
    if selected_client_name != REQUIRED_COGNITO_CLIENT_NAME:
        ccyo_out.error(
            f"TAPDB requires Cognito app client name '{REQUIRED_COGNITO_CLIENT_NAME}'."
        )
        raise typer.Exit(1)
    ccyo_out.print_text(
        f"[cyan]Setting up Cognito pool[/cyan] [bold]{selected_pool_name}[/bold] "
        "for explicit TapDB target"
    )

    args = _build_daycog_setup_args(
        command="setup",
        selected_pool_name=selected_pool_name,
        region=region,
        domain_prefix=domain_prefix,
        attach_domain=attach_domain,
        port=selected_port,
        callback_path=callback_path,
        oauth_flows=oauth_flows,
        scopes=scopes,
        idps=idps,
        password_min_length=password_min_length,
        mfa=mfa,
        profile=profile,
        client_name=selected_client_name,
        callback_url=callback_url,
        logout_url=logout_url,
        autoprovision=autoprovision,
        generate_secret=generate_secret,
        require_uppercase=require_uppercase,
        require_lowercase=require_lowercase,
        require_numbers=require_numbers,
        require_symbols=require_symbols,
        tags=tags,
    )

    _run_daycog(args)
    _finalize_setup_binding(
        selected_client_name=selected_client_name,
        region=region,
    )


@cognito_app.command("setup-with-google")
def cognito_setup_with_google(
    pool_name: str = typer.Option(..., "--pool-name", "-n", help="Explicit Cognito pool name"),
    client_name: Optional[str] = typer.Option(
        None,
        "--client-name",
        help="App client name (must be 'tapdb'; default: tapdb)",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile (required unless present in the selected native auth config)",
    ),
    region: Optional[str] = typer.Option(None, "--region", "-r", help="AWS region"),
    domain_prefix: Optional[str] = typer.Option(
        None,
        "--domain-prefix",
        help="Hosted UI domain prefix (default: pool name)",
    ),
    attach_domain: bool = typer.Option(
        True,
        "--attach-domain/--no-attach-domain",
        help="Attach/ensure Cognito Hosted UI domain",
    ),
    port: Optional[int] = typer.Option(
        None,
        "--port",
        "-p",
        help="Callback port for daycog (defaults to target.ui_port)",
    ),
    callback_path: str = typer.Option(
        "/auth/callback",
        "--callback-path",
        help="Callback path used with --port when --callback-url is not set",
    ),
    callback_url: Optional[str] = typer.Option(
        None, "--callback-url", help="Full callback URL override"
    ),
    logout_url: Optional[str] = typer.Option(
        None, "--logout-url", help="Optional logout URL for app client"
    ),
    autoprovision: bool = typer.Option(
        True,
        "--autoprovision/--no-autoprovision",
        help="Reuse existing app client by --client-name (default: enabled)",
    ),
    generate_secret: bool = typer.Option(
        False,
        "--generate-secret",
        help="Create app client with a generated secret",
    ),
    oauth_flows: str = typer.Option(
        "code", "--oauth-flows", help="Comma-separated OAuth flows"
    ),
    scopes: str = typer.Option(
        "openid,email,profile", "--scopes", help="Comma-separated OAuth scopes"
    ),
    idps: str = typer.Option(
        "COGNITO", "--idp", help="Comma-separated identity providers"
    ),
    password_min_length: int = typer.Option(
        8, "--password-min-length", help="Minimum password length"
    ),
    require_uppercase: bool = typer.Option(
        True,
        "--require-uppercase/--no-require-uppercase",
        help="Require uppercase characters in passwords",
    ),
    require_lowercase: bool = typer.Option(
        True,
        "--require-lowercase/--no-require-lowercase",
        help="Require lowercase characters in passwords",
    ),
    require_numbers: bool = typer.Option(
        True,
        "--require-numbers/--no-require-numbers",
        help="Require numbers in passwords",
    ),
    require_symbols: bool = typer.Option(
        False,
        "--require-symbols/--no-require-symbols",
        help="Require symbols in passwords",
    ),
    mfa: str = typer.Option("off", "--mfa", help="MFA mode: off|optional|required"),
    tags: Optional[str] = typer.Option(
        None, "--tags", help="Comma-separated tags in key=value format"
    ),
    google_client_id: Optional[str] = typer.Option(
        None, "--google-client-id", help="Google OAuth client ID"
    ),
    google_client_secret: Optional[str] = typer.Option(
        None, "--google-client-secret", help="Google OAuth client secret"
    ),
    google_client_json: Optional[str] = typer.Option(
        None, "--google-client-json", help="Path to Google OAuth client JSON"
    ),
    google_scopes: str = typer.Option(
        "openid email profile", "--google-scopes", help="Google authorize scopes"
    ),
) -> None:
    """Create/reuse Cognito pool+app and configure Google IdP; bind pool ID."""
    cfg = get_db_config()
    if not region or not profile:
        raise RuntimeError("--region and --profile are required for TAPDB Cognito setup")
    configured_port, _ = _resolve_expected_ui_port()
    if port is None:
        selected_port = configured_port
    elif port != configured_port:
        ccyo_out.error(
            "--port does not match configured TAPDB UI port for explicit target: "
            f"{configured_port}"
        )
        raise typer.Exit(1)
    else:
        selected_port = port

    selected_pool_name = pool_name
    selected_client_name = client_name or REQUIRED_COGNITO_CLIENT_NAME
    if selected_client_name != REQUIRED_COGNITO_CLIENT_NAME:
        ccyo_out.error(
            f"TAPDB requires Cognito app client name '{REQUIRED_COGNITO_CLIENT_NAME}'."
        )
        raise typer.Exit(1)
    ccyo_out.print_text(
        f"[cyan]Setting up Cognito (Google)[/cyan] [bold]{selected_pool_name}[/bold] "
        "for explicit TapDB target"
    )

    args = _build_daycog_setup_args(
        command="setup-with-google",
        selected_pool_name=selected_pool_name,
        region=region,
        domain_prefix=domain_prefix,
        attach_domain=attach_domain,
        port=selected_port,
        callback_path=callback_path,
        oauth_flows=oauth_flows,
        scopes=scopes,
        idps=idps,
        password_min_length=password_min_length,
        mfa=mfa,
        profile=profile,
        client_name=selected_client_name,
        callback_url=callback_url,
        logout_url=logout_url,
        autoprovision=autoprovision,
        generate_secret=generate_secret,
        require_uppercase=require_uppercase,
        require_lowercase=require_lowercase,
        require_numbers=require_numbers,
        require_symbols=require_symbols,
        tags=tags,
    )
    if google_client_id:
        args.extend(["--google-client-id", google_client_id])
    if google_client_secret:
        args.extend(["--google-client-secret", google_client_secret])
    if google_client_json:
        args.extend(["--google-client-json", google_client_json])
    if google_scopes:
        args.extend(["--google-scopes", google_scopes])

    _run_daycog(args)
    _finalize_setup_binding(
        selected_client_name=selected_client_name,
        region=region,
    )


@cognito_app.command("bind")
def cognito_bind(
    pool_id: str = typer.Option(..., "--pool-id", help="Exact selected Cognito pool ID"),
    client_id: str = typer.Option(..., "--client-id", help="Exact selected Cognito app client ID"),
) -> None:
    """Bind the selected native flat auth config to the explicit TapDB target."""
    values = _load_daycog_config()
    if values.get("COGNITO_USER_POOL_ID") != pool_id.strip():
        raise RuntimeError("--pool-id does not match the selected native auth config")
    if values.get("COGNITO_APP_CLIENT_ID") != client_id.strip():
        raise RuntimeError("--client-id does not match the selected native auth config")
    cfg_path = _write_auth_binding_to_tapdb_config(values)
    ccyo_out.success(f"Bound pool {pool_id} and client {client_id} to {cfg_path}")


@cognito_app.command("status")
def cognito_status() -> None:
    """Show the exact target and its selected native flat auth config."""
    pool_id, path, values, _ = _resolve_bound_daycog_config(Environment.target)
    for label, value in (
        ("Pool ID", pool_id),
        ("Auth config", str(path)),
        ("Region", values["COGNITO_REGION"]),
        ("Client ID", values["COGNITO_APP_CLIENT_ID"]),
        ("Client", values["COGNITO_CLIENT_NAME"]),
        ("Domain", values.get("COGNITO_DOMAIN", "(not set)")),
        ("Callback", values.get("COGNITO_CALLBACK_URL", "(not set)")),
        ("Logout", values.get("COGNITO_LOGOUT_URL", "(not set)")),
        ("AWS profile", values.get("AWS_PROFILE", "(not set)")),
    ):
        ccyo_out.print_text(f"{label}: {value}")
    expected_port, source, errors, _ = _validate_bound_cognito_uris(values)
    if errors:
        raise RuntimeError("; ".join(errors))
    ccyo_out.print_text(f"TapDB UI port: {expected_port} ({source})")


@cognito_app.command("list-pools")
def cognito_list_pools(
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile (required unless present in the selected native auth config)",
    ),
    region: Optional[str] = typer.Option(
        None,
        "--region",
        "-r",
        help="AWS region (required unless present in the selected native auth config)",
    ),
) -> None:
    """List Cognito pools in the selected region via daycog."""
    env = Environment.target
    _, proc_env, selected_region, selected_profile = _resolve_pool_command_context(
        env,
        region=region,
        profile=profile,
    )
    args = ["list-pools", "--region", selected_region]
    if selected_profile:
        args.extend(["--profile", selected_profile])
    _run_daycog_printing(args, env=proc_env)


@cognito_app.command("list-apps")
def cognito_list_apps(
    pool_name: str = typer.Option(..., "--pool-name", help="Explicit Cognito pool name"),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile (required unless present in the selected native auth config)",
    ),
    region: Optional[str] = typer.Option(
        None,
        "--region",
        "-r",
        help="AWS region (required unless present in the selected native auth config)",
    ),
) -> None:
    """List app clients for a Cognito pool via daycog."""
    env = Environment.target
    selected_pool, proc_env, selected_region, selected_profile = (
        _resolve_pool_command_context(
            env,
            pool_name=pool_name,
            region=region,
            profile=profile,
        )
    )
    args = ["list-apps", "--pool-name", selected_pool, "--region", selected_region]
    if selected_profile:
        args.extend(["--profile", selected_profile])
    _run_daycog_printing(args, env=proc_env)


@cognito_app.command("add-app")
def cognito_add_app(
    app_name: str = typer.Option(..., "--app-name", help="New app client name"),
    callback_url: str = typer.Option(..., "--callback-url", help="OAuth callback URL"),
    pool_name: str = typer.Option(..., "--pool-name", help="Explicit Cognito pool name"),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile (required unless present in the selected native auth config)",
    ),
    region: Optional[str] = typer.Option(
        None,
        "--region",
        "-r",
        help="AWS region (required unless present in the selected native auth config)",
    ),
    logout_url: Optional[str] = typer.Option(
        None, "--logout-url", help="Optional logout URL"
    ),
    generate_secret: bool = typer.Option(
        False, "--generate-secret", help="Create app client with secret"
    ),
    oauth_flows: str = typer.Option("code", "--oauth-flows", help="OAuth flows CSV"),
    scopes: str = typer.Option(
        "openid,email,profile", "--scopes", help="OAuth scopes CSV"
    ),
    idps: str = typer.Option("COGNITO", "--idp", help="Identity providers CSV"),
    read_attributes: Optional[str] = typer.Option(
        None, "--read-attributes", help="Explicit readable Cognito attributes CSV for the selected pool"
    ),
    set_default: bool = typer.Option(
        False,
        "--set-default",
        help="Print the native reminder to update the selected auth-config file",
    ),
) -> None:
    """Create a new app client in the pool via daycog."""
    env = Environment.target
    selected_pool, proc_env, selected_region, selected_profile = (
        _resolve_pool_command_context(
            env,
            pool_name=pool_name,
            region=region,
            profile=profile,
        )
    )
    args = [
        "add-app",
        "--pool-name",
        selected_pool,
        "--app-name",
        app_name,
        "--callback-url",
        callback_url,
        "--oauth-flows",
        oauth_flows,
        "--scopes",
        scopes,
        "--idp",
        idps,
        "--region",
        selected_region,
    ]
    if selected_profile:
        args.extend(["--profile", selected_profile])
    if read_attributes is not None:
        args.extend(["--read-attributes", read_attributes])
    if logout_url:
        args.extend(["--logout-url", logout_url])
    if generate_secret:
        args.append("--generate-secret")
    if set_default:
        args.append("--set-default")
    _run_daycog_printing(args, env=proc_env)


@cognito_app.command("edit-app")
def cognito_edit_app(
    app_name: Optional[str] = typer.Option(
        None, "--app-name", help="Existing app client name"
    ),
    client_id: Optional[str] = typer.Option(
        None, "--client-id", help="Existing app client ID"
    ),
    new_app_name: Optional[str] = typer.Option(
        None, "--new-app-name", help="Rename app client"
    ),
    callback_url: Optional[str] = typer.Option(
        None, "--callback-url", help="Override callback URL"
    ),
    logout_url: Optional[str] = typer.Option(
        None, "--logout-url", help="Override logout URL"
    ),
    oauth_flows: Optional[str] = typer.Option(
        None, "--oauth-flows", help="OAuth flows CSV"
    ),
    scopes: Optional[str] = typer.Option(None, "--scopes", help="OAuth scopes CSV"),
    idps: Optional[str] = typer.Option(None, "--idp", help="Identity providers CSV"),
    set_default: bool = typer.Option(
        False,
        "--set-default",
        help="Print the native reminder to update the selected auth-config file",
    ),
    pool_name: str = typer.Option(..., "--pool-name", help="Explicit Cognito pool name"),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile (required unless present in the selected native auth config)",
    ),
    region: Optional[str] = typer.Option(
        None,
        "--region",
        "-r",
        help="AWS region (required unless present in the selected native auth config)",
    ),
) -> None:
    """Edit an existing app client in the pool via daycog."""
    env = Environment.target
    if not app_name and not client_id:
        ccyo_out.error("Provide one of --app-name or --client-id")
        raise typer.Exit(1)

    selected_pool, proc_env, selected_region, selected_profile = (
        _resolve_pool_command_context(
            env,
            pool_name=pool_name,
            region=region,
            profile=profile,
        )
    )
    args = ["edit-app", "--pool-name", selected_pool, "--region", selected_region]
    if selected_profile:
        args.extend(["--profile", selected_profile])
    if app_name:
        args.extend(["--app-name", app_name])
    if client_id:
        args.extend(["--client-id", client_id])
    if new_app_name:
        args.extend(["--new-app-name", new_app_name])
    if callback_url:
        args.extend(["--callback-url", callback_url])
    if logout_url:
        args.extend(["--logout-url", logout_url])
    if oauth_flows:
        args.extend(["--oauth-flows", oauth_flows])
    if scopes:
        args.extend(["--scopes", scopes])
    if idps:
        args.extend(["--idp", idps])
    if set_default:
        args.append("--set-default")
    _run_daycog_printing(args, env=proc_env)


@cognito_app.command("remove-app")
def cognito_remove_app(
    app_name: Optional[str] = typer.Option(None, "--app-name", help="App client name"),
    client_id: Optional[str] = typer.Option(None, "--client-id", help="App client ID"),
    pool_name: str = typer.Option(..., "--pool-name", help="Explicit Cognito pool name"),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile (required unless present in the selected native auth config)",
    ),
    region: Optional[str] = typer.Option(
        None,
        "--region",
        "-r",
        help="AWS region (required unless present in the selected native auth config)",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
    delete_config: bool = typer.Option(
        True,
        "--delete-config/--keep-config",
        help="Native daycog compatibility flag; no config deletion in 2.1.5",
    ),
) -> None:
    """Remove an app client from the pool via daycog."""
    env = Environment.target
    if not app_name and not client_id:
        ccyo_out.error("Provide one of --app-name or --client-id")
        raise typer.Exit(1)

    selected_pool, proc_env, selected_region, selected_profile = (
        _resolve_pool_command_context(
            env,
            pool_name=pool_name,
            region=region,
            profile=profile,
        )
    )
    args = ["remove-app", "--pool-name", selected_pool, "--region", selected_region]
    if selected_profile:
        args.extend(["--profile", selected_profile])
    if app_name:
        args.extend(["--app-name", app_name])
    if client_id:
        args.extend(["--client-id", client_id])
    if force:
        args.append("--force")
    if not delete_config:
        args.append("--keep-config")
    _run_daycog_printing(args, env=proc_env)


@cognito_app.command("add-google-idp")
def cognito_add_google_idp(
    app_name: Optional[str] = typer.Option(None, "--app-name", help="App client name"),
    client_id: Optional[str] = typer.Option(None, "--client-id", help="App client ID"),
    pool_name: str = typer.Option(..., "--pool-name", help="Explicit Cognito pool name"),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile (required unless present in the selected native auth config)",
    ),
    region: Optional[str] = typer.Option(
        None,
        "--region",
        "-r",
        help="AWS region (required unless present in the selected native auth config)",
    ),
    google_client_id: Optional[str] = typer.Option(
        None, "--google-client-id", help="Google OAuth client ID"
    ),
    google_client_secret: Optional[str] = typer.Option(
        None, "--google-client-secret", help="Google OAuth client secret"
    ),
    google_client_json: Optional[str] = typer.Option(
        None, "--google-client-json", help="Path to Google OAuth client JSON"
    ),
    scopes: str = typer.Option(
        "openid email profile", "--scopes", help="Google authorize scopes"
    ),
) -> None:
    """Configure Google IdP for a pool/app via daycog."""
    env = Environment.target
    if not app_name and not client_id:
        ccyo_out.error("Provide one of --app-name or --client-id")
        raise typer.Exit(1)

    selected_pool, proc_env, selected_region, selected_profile = (
        _resolve_pool_command_context(
            env,
            pool_name=pool_name,
            region=region,
            profile=profile,
        )
    )
    args = [
        "add-google-idp",
        "--pool-name",
        selected_pool,
        "--region",
        selected_region,
        "--scopes",
        scopes,
    ]
    if selected_profile:
        args.extend(["--profile", selected_profile])
    if app_name:
        args.extend(["--app-name", app_name])
    if client_id:
        args.extend(["--client-id", client_id])
    if google_client_id:
        args.extend(["--google-client-id", google_client_id])
    if google_client_secret:
        args.extend(["--google-client-secret", google_client_secret])
    if google_client_json:
        args.extend(["--google-client-json", google_client_json])
    _run_daycog_printing(args, env=proc_env)


@cognito_app.command("fix-auth-flows")
def cognito_fix_auth_flows() -> None:
    """Enable required auth flows on the active daycog app client."""
    env = Environment.target
    try:
        _, _, _, proc_env = _resolve_bound_daycog_config(env)
    except RuntimeError as e:
        ccyo_out.error(f"{e}")
        raise typer.Exit(1)
    _run_daycog_printing(["fix-auth-flows"], env=proc_env)


@config_app.command("print")
def cognito_config_print() -> None:
    """Print the explicitly selected native daycog auth config."""
    _run_daycog_printing(["auth-config", "print"])


def _auth_config_command(
    operation: str,
    *,
    pool_name: Optional[str],
    pool_id: Optional[str],
    client_name: Optional[str],
    client_id: Optional[str],
    profile: str,
    region: str,
) -> None:
    if bool(pool_name) == bool(pool_id):
        raise RuntimeError("Select exactly one explicit --pool-name or --pool-id")
    if bool(client_name) == bool(client_id):
        raise RuntimeError("Select exactly one explicit --client-name or --client-id")
    args = ["auth-config", operation, "--profile", profile, "--region", region]
    for flag, value in (
        ("--pool-name", pool_name), ("--pool-id", pool_id),
        ("--client-name", client_name), ("--client-id", client_id),
    ):
        if value:
            args.extend([flag, value])
    _run_daycog_printing(args)


@config_app.command("create")
def cognito_config_create(
    pool_name: Optional[str] = typer.Option(None, "--pool-name", help="Exact existing pool name"),
    pool_id: Optional[str] = typer.Option(None, "--pool-id", help="Exact existing pool ID"),
    client_name: Optional[str] = typer.Option(None, "--client-name", help="Exact existing app client name"),
    client_id: Optional[str] = typer.Option(None, "--client-id", help="Exact existing app client ID"),
    profile: str = typer.Option(..., "--profile", help="Explicit existing AWS profile"),
    region: str = typer.Option(..., "--region", "-r", help="Explicit AWS region"),
) -> None:
    """Import existing pool/client state through native daycog auth-config create."""
    _auth_config_command("create", pool_name=pool_name, pool_id=pool_id,
                         client_name=client_name, client_id=client_id,
                         profile=profile, region=region)


@config_app.command("update")
def cognito_config_update(
    pool_name: Optional[str] = typer.Option(None, "--pool-name", help="Exact existing pool name"),
    pool_id: Optional[str] = typer.Option(None, "--pool-id", help="Exact existing pool ID"),
    client_name: Optional[str] = typer.Option(None, "--client-name", help="Exact existing app client name"),
    client_id: Optional[str] = typer.Option(None, "--client-id", help="Exact existing app client ID"),
    profile: str = typer.Option(..., "--profile", help="Explicit existing AWS profile"),
    region: str = typer.Option(..., "--region", "-r", help="Explicit AWS region"),
) -> None:
    """Update the selected file through native daycog auth-config update."""
    _auth_config_command("update", pool_name=pool_name, pool_id=pool_id,
                         client_name=client_name, client_id=client_id,
                         profile=profile, region=region)


@cognito_app.command("add-user")
def cognito_add_user(
    email: str = typer.Argument(..., help="User email"),
    password: str = typer.Option(..., "--password", "-p", help="Initial password"),
    role: str = typer.Option("user", "--role", "-r", help="tapdb role: admin|user"),
    display_name: Optional[str] = typer.Option(
        None,
        "--name",
        "-n",
        help="Optional display name for TAPDB actor user",
    ),
    no_verify: bool = typer.Option(
        True,
        "--no-verify/--verify-email",
        help="Set permanent password and mark email verified",
    ),
) -> None:
    """Create a Cognito user in the TAPDB-bound pool via daycog."""
    env = Environment.target
    if role not in ("admin", "user"):
        ccyo_out.error(f"Invalid role: {role}. Must be admin or user")
        raise typer.Exit(1)

    try:
        pool_id, auth_config_path, _, proc_env = _resolve_bound_daycog_config(env)
    except RuntimeError as e:
        ccyo_out.error(f"{e}")
        raise typer.Exit(1)

    args = ["add-user", email, "--password", password]
    if no_verify:
        args.append("--no-verify")
    _run_daycog(args, env=proc_env)

    try:
        _ensure_actor_user_row(
            env,
            email=email,
            role=role,
            display_name=display_name,
        )
    except Exception as e:
        ccyo_out.error(
            f"Cognito user created, but failed to provision TAPDB actor user: {e}"
        )
        raise typer.Exit(1)

    ccyo_out.success(f"Created Cognito user: {email}")
    ccyo_out.print_text(f"  Pool: [dim]{pool_id}[/dim]")
    ccyo_out.print_text(
        f"  Daycog auth config: [dim]{auth_config_path}[/dim]"
    )
    ccyo_out.print_text(f"  tapdb role: [dim]{role}[/dim]")
