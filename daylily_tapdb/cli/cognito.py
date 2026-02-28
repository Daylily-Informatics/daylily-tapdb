"""TAPDB Cognito integration commands.

Operational lifecycle is delegated to the daylily-cognito CLI (`daycog`).
TAPDB stores only Cognito pool ID in tapdb config.
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from daylily_tapdb.cli.db import Environment, _run_psql
from daylily_tapdb.cli.db_config import get_config_path, get_db_config_for_env

console = Console()
cognito_app = typer.Typer(help="Cognito auth integration commands (via daycog)")
config_app = typer.Typer(help="daycog config file utilities")
cognito_app.add_typer(config_app, name="config")
DEFAULT_COGNITO_CALLBACK_PORT = 8911


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key:
            values[key] = value
    return values


def _daycog_config_dir() -> Path:
    return Path.home() / ".config" / "daycog"


def _sanitize_filename_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "app"


def _default_pool_name(env: Environment) -> str:
    cfg = get_db_config_for_env(env.value)
    db_name = (cfg.get("database") or f"tapdb_{env.value}").strip()
    raw = f"tapdb-{db_name}-users"
    name = re.sub(r"[^a-zA-Z0-9-]+", "-", raw).strip("-").lower()
    return re.sub(r"-{2,}", "-", name)


def _score_daycog_env_match(
    path: Path,
    values: dict[str, str],
    *,
    prefer_region: Optional[str] = None,
    prefer_client_name: Optional[str] = None,
) -> tuple[int, str]:
    """Return sort key for daycog env preference (higher score is better)."""
    score = 0
    name = path.name
    region = (values.get("COGNITO_REGION") or values.get("AWS_REGION") or "").strip()
    client_name = (values.get("COGNITO_CLIENT_NAME") or "").strip()

    if name != "default.env":
        score += 10

    if region and name.endswith(f".{region}.env"):
        # Pool-scoped file (<pool>.<region>.env) is canonical selected context.
        score += 70
    elif region and client_name:
        safe_client = _sanitize_filename_part(client_name)
        if name.endswith(f".{region}.{safe_client}.env"):
            # App-scoped file (<pool>.<region>.<app>.env) is second-best.
            score += 60

    if prefer_region and region == prefer_region:
        score += 25
    if prefer_client_name and client_name == prefer_client_name:
        score += 15

    return (score, name)


def _find_pool_env_file_by_id(
    pool_id: str,
    *,
    prefer_region: Optional[str] = None,
    prefer_client_name: Optional[str] = None,
) -> tuple[Path, dict[str, str]]:
    cfg_dir = _daycog_config_dir()
    if not cfg_dir.exists():
        raise RuntimeError(f"daycog config dir not found: {cfg_dir}")

    files = sorted(cfg_dir.glob("*.env"))
    matches: list[tuple[Path, dict[str, str]]] = []
    for env_file in files:
        values = _read_env_file(env_file)
        if (values.get("COGNITO_USER_POOL_ID") or "").strip() == pool_id:
            matches.append((env_file, values))

    if matches:
        matches.sort(
            key=lambda item: _score_daycog_env_match(
                item[0],
                item[1],
                prefer_region=prefer_region,
                prefer_client_name=prefer_client_name,
            ),
            reverse=True,
        )
        return matches[0]

    raise RuntimeError(
        f"No daycog env file maps to pool ID {pool_id}. "
        "Expected a daycog env file in "
        f"{cfg_dir} (for example <pool>.<region>.env) with "
        f"COGNITO_USER_POOL_ID={pool_id}."
    )


def _resolve_daycog_pool_id_after_setup(
    *,
    pool_name: str,
    region: str,
    client_name: str,
) -> tuple[str, Path]:
    """Resolve pool ID from daycog 0.1.21+ config file naming."""
    cfg_dir = _daycog_config_dir()
    pool_env = cfg_dir / f"{pool_name}.{region}.env"
    app_env = cfg_dir / f"{pool_name}.{region}.{_sanitize_filename_part(client_name)}.env"
    default_env = cfg_dir / "default.env"

    checked: list[Path] = []
    for path in [pool_env, app_env, default_env]:
        checked.append(path)
        values = _read_env_file(path)
        pool_id = (values.get("COGNITO_USER_POOL_ID") or "").strip()
        if pool_id:
            return pool_id, path

    # Fallback for unusual setups: any env file matching pool+region prefix.
    for path in sorted(cfg_dir.glob(f"{pool_name}.{region}*.env")):
        if path in checked:
            continue
        values = _read_env_file(path)
        pool_id = (values.get("COGNITO_USER_POOL_ID") or "").strip()
        if pool_id:
            return pool_id, path

    raise RuntimeError(
        "daycog setup completed but pool ID was not found in expected files: "
        f"{pool_env}, {app_env}, {default_env}"
    )


def _write_pool_id_to_tapdb_config(env: Environment, pool_id: str) -> Path:
    config_path = get_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    existing: dict = {}
    if config_path.exists():
        raw = config_path.read_text(encoding="utf-8")
        try:
            import yaml  # type: ignore

            existing = yaml.safe_load(raw) or {}
        except ModuleNotFoundError:
            existing = json.loads(raw) if raw.strip() else {}

    envs = existing.setdefault("environments", {})
    env_cfg = envs.setdefault(env.value, {})
    env_cfg["cognito_user_pool_id"] = pool_id

    try:
        import yaml  # type: ignore

        config_path.write_text(
            yaml.dump(existing, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )
    except ModuleNotFoundError:
        config_path.write_text(json.dumps(existing, indent=2) + "\n", encoding="utf-8")

    os.chmod(config_path, stat.S_IRUSR | stat.S_IWUSR)  # 0600
    return config_path


def _run_daycog(args: list[str], env: Optional[dict[str, str]] = None) -> str:
    cmd = ["daycog", *args]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        msg = (result.stdout + "\n" + result.stderr).strip()
        raise RuntimeError(msg or f"daycog failed ({result.returncode})")
    return (result.stdout or "").strip()


def _run_daycog_printing(args: list[str], env: Optional[dict[str, str]] = None) -> None:
    out = _run_daycog(args, env=env)
    if out:
        console.print(out)


def _build_daycog_setup_args(
    *,
    command: str,
    selected_pool_name: str,
    region: str,
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
    args = [
        command,
        "--name",
        selected_pool_name,
        "--region",
        region,
        "--port",
        str(port),
        "--callback-path",
        callback_path,
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
    if profile:
        args.extend(["--profile", profile])
    if client_name:
        args.extend(["--client-name", client_name])
    if callback_url:
        args.extend(["--callback-url", callback_url])
    if logout_url:
        args.extend(["--logout-url", logout_url])
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


def _finalize_setup_binding(
    *,
    env: Environment,
    selected_pool_name: str,
    selected_client_name: str,
    region: str,
) -> None:
    try:
        pool_id, pool_env_path = _resolve_daycog_pool_id_after_setup(
            pool_name=selected_pool_name,
            region=region,
            client_name=selected_client_name,
        )
    except RuntimeError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    cfg_path = _write_pool_id_to_tapdb_config(env, pool_id)
    console.print(f"[green]✓[/green] Bound pool ID to tapdb config: {pool_id}")
    console.print(f"  TAPDB config: [dim]{cfg_path}[/dim]")
    console.print(f"  daycog env:   [dim]{pool_env_path}[/dim]")


def _resolve_bound_daycog_context(
    env: Environment,
) -> tuple[str, Path, dict[str, str], dict[str, str]]:
    cfg = get_db_config_for_env(env.value)
    pool_id = (cfg.get("cognito_user_pool_id") or "").strip()
    if not pool_id:
        raise RuntimeError(
            f"No cognito_user_pool_id set for env {env.value}. "
            f"Run: tapdb cognito setup {env.value}"
        )

    env_file, values = _find_pool_env_file_by_id(pool_id)
    proc_env = os.environ.copy()
    proc_env.update(values)
    return pool_id, env_file, values, proc_env


def _resolve_pool_command_context(
    env: Environment,
    *,
    pool_name: Optional[str] = None,
    region: Optional[str] = None,
    profile: Optional[str] = None,
) -> tuple[str, Optional[dict[str, str]], str, Optional[str]]:
    selected_pool = pool_name or _default_pool_name(env)
    selected_region = region
    selected_profile = profile
    proc_env: Optional[dict[str, str]] = None

    try:
        _, _, values, bound_proc_env = _resolve_bound_daycog_context(env)
        proc_env = bound_proc_env
        selected_region = selected_region or (
            values.get("COGNITO_REGION") or values.get("AWS_REGION") or None
        )
        selected_profile = selected_profile or values.get("AWS_PROFILE") or None
    except RuntimeError:
        # It's valid to manage apps before binding into TAPDB config.
        pass

    return selected_pool, proc_env, selected_region or "us-east-1", selected_profile


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _ensure_tapdb_user_row(
    env: Environment,
    *,
    email: str,
    role: str = "user",
    display_name: Optional[str] = None,
) -> None:
    """Ensure TAPDB has a local user row for the Cognito identity."""
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        raise RuntimeError("email is required for tapdb user creation")
    if role not in ("admin", "user"):
        raise RuntimeError(f"invalid role: {role}")

    em = _sql_quote(normalized_email)
    role_q = _sql_quote(role)
    display_name_sql = (
        f"'{_sql_quote(display_name.strip())}'"
        if display_name and display_name.strip()
        else "NULL"
    )
    insert_sql = f"""
        INSERT INTO tapdb_user (
            username, email, display_name, role,
            is_active, require_password_change, password_hash
        )
        VALUES (
            '{em}', '{em}', {display_name_sql}, '{role_q}',
            TRUE, FALSE, NULL
        )
    """
    ok, out = _run_psql(env, sql=insert_sql)
    if ok:
        return

    # If already present (username or email), treat as success.
    check_sql = f"""
        SELECT username
        FROM tapdb_user
        WHERE username = '{em}' OR email = '{em}'
        LIMIT 1
    """
    check_ok, check_out = _run_psql(env, sql=check_sql)
    if check_ok and check_out.strip():
        return
    raise RuntimeError(out.strip() or "Failed to create tapdb_user row")


@cognito_app.command("setup")
def cognito_setup(
    env: Environment = typer.Argument(..., help="Target environment"),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", "-n", help="Cognito pool name (defaults from DB name)"
    ),
    client_name: Optional[str] = typer.Option(
        None,
        "--client-name",
        help="App client name (default from daycog: <pool-name>-client)",
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: AWS_PROFILE env var)"
    ),
    region: str = typer.Option("us-east-1", "--region", "-r", help="AWS region"),
    port: int = typer.Option(
        DEFAULT_COGNITO_CALLBACK_PORT,
        "--port",
        "-p",
        help="Callback port for daycog",
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
    selected_pool_name = pool_name or _default_pool_name(env)
    selected_client_name = client_name or f"{selected_pool_name}-client"
    console.print(
        f"[cyan]Setting up Cognito pool[/cyan] [bold]{selected_pool_name}[/bold] "
        f"for env [bold]{env.value}[/bold]"
    )

    args = _build_daycog_setup_args(
        command="setup",
        selected_pool_name=selected_pool_name,
        region=region,
        port=port,
        callback_path=callback_path,
        oauth_flows=oauth_flows,
        scopes=scopes,
        idps=idps,
        password_min_length=password_min_length,
        mfa=mfa,
        profile=profile,
        client_name=client_name,
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
        env=env,
        selected_pool_name=selected_pool_name,
        selected_client_name=selected_client_name,
        region=region,
    )


@cognito_app.command("setup-with-google")
def cognito_setup_with_google(
    env: Environment = typer.Argument(..., help="Target environment"),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", "-n", help="Cognito pool name (defaults from DB name)"
    ),
    client_name: Optional[str] = typer.Option(
        None,
        "--client-name",
        help="App client name (default from daycog: <pool-name>-client)",
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: AWS_PROFILE env var)"
    ),
    region: str = typer.Option("us-east-1", "--region", "-r", help="AWS region"),
    port: int = typer.Option(
        DEFAULT_COGNITO_CALLBACK_PORT,
        "--port",
        "-p",
        help="Callback port for daycog",
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
    selected_pool_name = pool_name or _default_pool_name(env)
    selected_client_name = client_name or f"{selected_pool_name}-client"
    console.print(
        f"[cyan]Setting up Cognito (Google)[/cyan] [bold]{selected_pool_name}[/bold] "
        f"for env [bold]{env.value}[/bold]"
    )

    args = _build_daycog_setup_args(
        command="setup-with-google",
        selected_pool_name=selected_pool_name,
        region=region,
        port=port,
        callback_path=callback_path,
        oauth_flows=oauth_flows,
        scopes=scopes,
        idps=idps,
        password_min_length=password_min_length,
        mfa=mfa,
        profile=profile,
        client_name=client_name,
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
        env=env,
        selected_pool_name=selected_pool_name,
        selected_client_name=selected_client_name,
        region=region,
    )


@cognito_app.command("bind")
def cognito_bind(
    env: Environment = typer.Argument(..., help="Target environment"),
    pool_id: str = typer.Option(..., "--pool-id", help="Cognito user pool ID"),
) -> None:
    """Bind an existing Cognito pool ID into tapdb config."""
    cfg_path = _write_pool_id_to_tapdb_config(env, pool_id.strip())
    console.print(f"[green]✓[/green] Bound {pool_id.strip()} to env '{env.value}'")
    console.print(f"  TAPDB config: [dim]{cfg_path}[/dim]")


@cognito_app.command("status")
def cognito_status(
    env: Environment = typer.Argument(..., help="Target environment"),
) -> None:
    """Show TAPDB Cognito binding and mapped daycog pool env file."""
    cfg = get_db_config_for_env(env.value)
    pool_id = (cfg.get("cognito_user_pool_id") or "").strip()
    if not pool_id:
        console.print(f"[yellow]⚠[/yellow] No cognito_user_pool_id set for env {env.value}")
        raise typer.Exit(1)

    env_file, values = _find_pool_env_file_by_id(pool_id)
    region = values.get("COGNITO_REGION") or values.get("AWS_REGION") or "(missing)"
    client_id = values.get("COGNITO_APP_CLIENT_ID") or "(missing)"
    client_name = values.get("COGNITO_CLIENT_NAME") or "(missing)"
    callback_url = values.get("COGNITO_CALLBACK_URL") or "(missing)"
    logout_url = values.get("COGNITO_LOGOUT_URL") or "(not set)"
    profile = values.get("AWS_PROFILE") or "(missing)"
    console.print(f"[green]✓[/green] Env:        {env.value}")
    console.print(f"[green]✓[/green] Pool ID:    {pool_id}")
    console.print(f"[green]✓[/green] daycog env: {env_file}")
    console.print(f"[green]✓[/green] Region:     {region}")
    console.print(f"[green]✓[/green] Client ID:  {client_id}")
    console.print(f"[green]✓[/green] Client:     {client_name}")
    console.print(f"[green]✓[/green] Callback:   {callback_url}")
    console.print(f"[green]✓[/green] Logout:     {logout_url}")
    console.print(f"[green]✓[/green] Profile:    {profile}")


@cognito_app.command("list-pools")
def cognito_list_pools(
    env: Environment = typer.Argument(..., help="Target environment"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: daycog env context)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
    ),
) -> None:
    """List Cognito pools in the selected region via daycog."""
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
    env: Environment = typer.Argument(..., help="Target environment"),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", help="Cognito pool name (default from env DB name)"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: daycog env context)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
    ),
) -> None:
    """List app clients for a Cognito pool via daycog."""
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
    env: Environment = typer.Argument(..., help="Target environment"),
    app_name: str = typer.Option(..., "--app-name", help="New app client name"),
    callback_url: str = typer.Option(..., "--callback-url", help="OAuth callback URL"),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", help="Cognito pool name (default from env DB name)"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: daycog env context)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
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
    set_default: bool = typer.Option(
        False, "--set-default", help="Update pool/default env context to this app"
    ),
) -> None:
    """Create a new app client in the pool via daycog."""
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
    if logout_url:
        args.extend(["--logout-url", logout_url])
    if generate_secret:
        args.append("--generate-secret")
    if set_default:
        args.append("--set-default")
    _run_daycog_printing(args, env=proc_env)


@cognito_app.command("edit-app")
def cognito_edit_app(
    env: Environment = typer.Argument(..., help="Target environment"),
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
        False, "--set-default", help="Update pool/default env context to this app"
    ),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", help="Cognito pool name (default from env DB name)"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: daycog env context)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
    ),
) -> None:
    """Edit an existing app client in the pool via daycog."""
    if not app_name and not client_id:
        console.print("[red]✗[/red] Provide one of --app-name or --client-id")
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
    env: Environment = typer.Argument(..., help="Target environment"),
    app_name: Optional[str] = typer.Option(
        None, "--app-name", help="App client name"
    ),
    client_id: Optional[str] = typer.Option(
        None, "--client-id", help="App client ID"
    ),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", help="Cognito pool name (default from env DB name)"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: daycog env context)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
    delete_config: bool = typer.Option(
        True,
        "--delete-config/--keep-config",
        help="Delete per-app env file (default: true)",
    ),
) -> None:
    """Remove an app client from the pool via daycog."""
    if not app_name and not client_id:
        console.print("[red]✗[/red] Provide one of --app-name or --client-id")
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
    env: Environment = typer.Argument(..., help="Target environment"),
    app_name: Optional[str] = typer.Option(
        None, "--app-name", help="App client name"
    ),
    client_id: Optional[str] = typer.Option(
        None, "--client-id", help="App client ID"
    ),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", help="Cognito pool name (default from env DB name)"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: daycog env context)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
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
    if not app_name and not client_id:
        console.print("[red]✗[/red] Provide one of --app-name or --client-id")
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
def cognito_fix_auth_flows(
    env: Environment = typer.Argument(..., help="Target environment"),
) -> None:
    """Enable required auth flows on the active daycog app client."""
    try:
        _, _, _, proc_env = _resolve_bound_daycog_context(env)
    except RuntimeError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)
    _run_daycog_printing(["fix-auth-flows"], env=proc_env)


@config_app.command("print")
def cognito_config_print(
    env: Environment = typer.Argument(..., help="Target environment"),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", help="Cognito pool name (default from env DB name)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
    ),
) -> None:
    """Print daycog config file path + contents for the target pool context."""
    selected_pool, proc_env, selected_region, _ = _resolve_pool_command_context(
        env,
        pool_name=pool_name,
        region=region,
        profile=None,
    )
    args = ["config", "print", "--pool-name", selected_pool, "--region", selected_region]
    _run_daycog_printing(args, env=proc_env)


@config_app.command("create")
def cognito_config_create(
    env: Environment = typer.Argument(..., help="Target environment"),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", help="Cognito pool name (default from env DB name)"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: daycog env context)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
    ),
) -> None:
    """Create daycog pool config file from AWS and update default config."""
    selected_pool, proc_env, selected_region, selected_profile = (
        _resolve_pool_command_context(
            env,
            pool_name=pool_name,
            region=region,
            profile=profile,
        )
    )
    args = [
        "config",
        "create",
        "--pool-name",
        selected_pool,
        "--region",
        selected_region,
    ]
    if selected_profile:
        args.extend(["--profile", selected_profile])
    _run_daycog_printing(args, env=proc_env)


@config_app.command("update")
def cognito_config_update(
    env: Environment = typer.Argument(..., help="Target environment"),
    pool_name: Optional[str] = typer.Option(
        None, "--pool-name", help="Cognito pool name (default from env DB name)"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="AWS profile (fallback: daycog env context)"
    ),
    region: Optional[str] = typer.Option(
        None, "--region", "-r", help="AWS region (fallback: daycog env context)"
    ),
) -> None:
    """Update daycog pool config file from AWS and refresh default config."""
    selected_pool, proc_env, selected_region, selected_profile = (
        _resolve_pool_command_context(
            env,
            pool_name=pool_name,
            region=region,
            profile=profile,
        )
    )
    args = [
        "config",
        "update",
        "--pool-name",
        selected_pool,
        "--region",
        selected_region,
    ]
    if selected_profile:
        args.extend(["--profile", selected_profile])
    _run_daycog_printing(args, env=proc_env)


@cognito_app.command("add-user")
def cognito_add_user(
    env: Environment = typer.Argument(..., help="Target environment"),
    email: str = typer.Argument(..., help="User email"),
    password: str = typer.Option(..., "--password", "-p", help="Initial password"),
    role: str = typer.Option("user", "--role", "-r", help="tapdb role: admin|user"),
    display_name: Optional[str] = typer.Option(
        None,
        "--name",
        "-n",
        help="Optional display name for tapdb_user",
    ),
    no_verify: bool = typer.Option(
        True,
        "--no-verify/--verify-email",
        help="Set permanent password and mark email verified",
    ),
) -> None:
    """Create a Cognito user in the TAPDB-bound pool via daycog."""
    if role not in ("admin", "user"):
        console.print(f"[red]✗[/red] Invalid role: {role}. Must be admin or user")
        raise typer.Exit(1)

    try:
        pool_id, env_file, _, proc_env = _resolve_bound_daycog_context(env)
    except RuntimeError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    args = ["add-user", email, "--password", password]
    if no_verify:
        args.append("--no-verify")
    _run_daycog(args, env=proc_env)

    try:
        _ensure_tapdb_user_row(
            env,
            email=email,
            role=role,
            display_name=display_name,
        )
    except Exception as e:
        console.print(
            "[red]✗[/red] Cognito user created, but failed to create tapdb_user row: "
            f"{e}"
        )
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Created Cognito user: {email}")
    console.print(f"  Pool: [dim]{pool_id}[/dim]")
    console.print(f"  daycog env: [dim]{env_file}[/dim]")
    console.print(f"  tapdb role: [dim]{role}[/dim]")
