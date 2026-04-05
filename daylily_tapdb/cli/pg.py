"""PostgreSQL service management commands for TAPDB CLI."""

import json
import os
import platform
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import typer
from cli_core_yo import ccyo_out
from rich.console import Console

from daylily_tapdb.cli.context import active_env_name, resolve_context
from daylily_tapdb.cli.db import Environment
from daylily_tapdb.cli.db_config import get_db_config_for_env

console = Console()

pg_app = typer.Typer(help="PostgreSQL service management commands")


def _get_postgres_data_dir(env: "Environment") -> Path:
    """Get PostgreSQL data directory for environment.

    dev/test: ~/.config/tapdb/<client>/<database>/<env>/postgres/data
    prod: system default or PGDATA env var
    """
    if env.value == "prod":
        # Production uses system default
        return Path(os.environ.get("PGDATA", "/var/lib/postgresql/data"))
    ctx = resolve_context(
        require_keys=True,
        env_name=env.value,
    )
    return ctx.postgres_dir(env.value) / "data"


def _get_postgres_log_file(env: "Environment") -> Path:
    if env.value == "prod":
        return Path("/var/log/postgresql/postgresql.log")
    ctx = resolve_context(
        require_keys=True,
        env_name=env.value,
    )
    return ctx.postgres_dir(env.value) / "postgresql.log"


def _get_postgres_socket_dir(env: "Environment") -> Path:
    if env.value == "prod":
        return Path("/var/run/postgresql")
    cfg = get_db_config_for_env(env.value)
    configured = str(cfg.get("unix_socket_dir") or "").strip()
    if configured:
        return Path(configured).expanduser()
    ctx = resolve_context(
        require_keys=True,
        env_name=env.value,
    )
    return ctx.postgres_socket_dir(env.value)


def _get_instance_lock_file(env: "Environment") -> Path:
    if env.value == "prod":
        return Path(tempfile.gettempdir()) / "tapdb-prod-instance.lock"
    ctx = resolve_context(
        require_keys=True,
        env_name=env.value,
    )
    return ctx.lock_dir(env.value) / "instance.lock"


def _build_pg_ctl_options(port: int, socket_dir: Path) -> str:
    return " ".join(
        [
            f"-p {port}",
            f"-k {shlex.quote(str(socket_dir))}",
            "-h localhost",
        ]
    )


def _port_conflict_details(port: int) -> str:
    try:
        proc = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return f"port {port} is already in use"
    if proc.returncode != 0 or not (proc.stdout or "").strip():
        return f"port {port} is already in use"
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    if len(lines) < 2:
        return f"port {port} is already in use"
    return f"port {port} is in use ({lines[1]})"


def _is_port_available(port: int) -> bool:
    try:
        proc = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return True
    return proc.returncode != 0 or not (proc.stdout or "").strip()


def _active_env() -> Environment:
    raw = active_env_name("dev").strip()
    try:
        return Environment(raw)
    except ValueError:
        return Environment.dev


def _get_pg_service_cmd() -> tuple[str, list[str], list[str], Path]:
    """
    Get platform-specific system PostgreSQL service commands.

    This is intended for production environments where PostgreSQL is managed as a
    system service (e.g., systemd on Linux). Local dev/test should use the
    data-dir based commands: `tapdb --config <path> --env <name> pg init`
    + `tapdb --config <path> --env <name> pg start-local`.

    Returns: (method, start_cmd, stop_cmd, log_path)
    """
    system = platform.system()

    if system == "Linux":
        # Check for systemd
        if Path("/bin/systemctl").exists() or Path("/usr/bin/systemctl").exists():
            return (
                "systemd",
                ["sudo", "systemctl", "start", "postgresql"],
                ["sudo", "systemctl", "stop", "postgresql"],
                Path("/var/log/postgresql/postgresql-14-main.log"),
            )
        # Check for service command
        elif Path("/usr/sbin/service").exists():
            return (
                "sysvinit",
                ["sudo", "service", "postgresql", "start"],
                ["sudo", "service", "postgresql", "stop"],
                Path("/var/log/postgresql/postgresql-14-main.log"),
            )
        else:
            return ("unknown", [], [], Path())

    else:
        return ("unknown", [], [], Path())


def _is_pg_running() -> tuple[bool, str]:
    """Check if PostgreSQL is running. Returns (running, details)."""
    try:
        # Try to connect
        result = subprocess.run(
            ["pg_isready", "-q"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            # Get version
            ver_result = subprocess.run(
                ["psql", "-t", "-c", "SELECT version()"],
                capture_output=True,
                text=True,
                env={**os.environ, "PGDATABASE": "postgres"},
                timeout=5,
            )
            version = (
                ver_result.stdout.strip().split(",")[0]
                if ver_result.returncode == 0
                else "unknown"
            )
            return True, version
        return False, ""
    except FileNotFoundError:
        return False, "pg_isready not found"
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except Exception as e:
        return False, str(e)


@pg_app.command("start")
def pg_start():
    """Start system PostgreSQL service (production only).

    For local development, prefer:
    tapdb --config <path> --env <name> pg start-local <env>
    """
    running, details = _is_pg_running()
    if running:
        ccyo_out.success("PostgreSQL is already running")
        ccyo_out.print_text(f"  {details}")
        return

    method, start_cmd, _, _ = _get_pg_service_cmd()

    if method == "unknown":
        ccyo_out.error("No system PostgreSQL service found")
        ccyo_out.print_text("")
        ccyo_out.print_text("[bold]Recommended: Use TAPDB local dev/test commands[/bold]")
        ccyo_out.print_text("  [cyan]tapdb pg init dev[/cyan]         # Initialize data directory")
        ccyo_out.print_text("  [cyan]tapdb --config <path> --env dev pg start-local dev[/cyan]  # Start local instance")
        ccyo_out.print_text("")
        ccyo_out.print_text("[dim]Install PostgreSQL so initdb/pg_ctl"
            " are on PATH (conda recommended):[/dim]")
        ccyo_out.print_text("  [dim]conda install -c conda-forge postgresql[/dim]")
        raise typer.Exit(1)

    ccyo_out.warning(f"► Starting PostgreSQL ({method})...")

    try:
        result = subprocess.run(start_cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            # Wait and verify
            import time

            for _ in range(10):
                time.sleep(1)
                running, details = _is_pg_running()
                if running:
                    ccyo_out.success("PostgreSQL started")
                    ccyo_out.print_text(f"  {details}")
                    return

            ccyo_out.warning("Start command succeeded"
                " but PostgreSQL not responding")
            ccyo_out.print_text("  Check logs: [cyan]tapdb pg logs[/cyan]")
        else:
            ccyo_out.error("Failed to start PostgreSQL")
            ccyo_out.print_text(f"  {result.stderr}")
            raise typer.Exit(1)
    except subprocess.TimeoutExpired:
        ccyo_out.warning("Start command timed out")
    except Exception as e:
        ccyo_out.error(f"Error: {e}")
        raise typer.Exit(1)


@pg_app.command("stop")
def pg_stop():
    """Stop system PostgreSQL service (production only).

    For local development instances, use: tapdb pg stop-local <env>
    """
    running, _ = _is_pg_running()
    if not running:
        ccyo_out.print_text("PostgreSQL is not running")
        return

    method, _, stop_cmd, _ = _get_pg_service_cmd()

    if method == "unknown":
        ccyo_out.error("No system PostgreSQL service found")
        ccyo_out.print_text("  For local instances, use: [cyan]tapdb pg stop-local <env>[/cyan]")
        raise typer.Exit(1)

    ccyo_out.warning(f"► Stopping PostgreSQL ({method})...")

    try:
        result = subprocess.run(stop_cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            # Wait and verify
            import time

            for _ in range(10):
                time.sleep(1)
                running, _ = _is_pg_running()
                if not running:
                    ccyo_out.success("PostgreSQL stopped")
                    return

            ccyo_out.warning("Stop command succeeded but PostgreSQL still running")
        else:
            ccyo_out.error("Failed to stop PostgreSQL")
            ccyo_out.print_text(f"  {result.stderr}")
            raise typer.Exit(1)
    except Exception as e:
        ccyo_out.error(f"Error: {e}")
        raise typer.Exit(1)


@pg_app.command("status")
def pg_status():
    """Check if PostgreSQL is running and show connection info."""
    ccyo_out.print_text("\n[bold cyan]━━━ PostgreSQL Status ━━━[/bold cyan]")

    env = _active_env()
    if env == Environment.prod:
        running, details = _is_pg_running()
        if running:
            ccyo_out.success("PostgreSQL is running")
            ccyo_out.print_text(f"  Version: {details}")
        else:
            ccyo_out.error("○ PostgreSQL is not running")
            if details and details not in ("", "timeout"):
                ccyo_out.print_text(f"  Error: {details}")
            ccyo_out.print_text("\n  Start with: [cyan]tapdb pg start[/cyan]")
        return

    cfg = get_db_config_for_env(env.value)
    host = cfg["host"]
    port = cfg["port"]
    user = cfg["user"]
    data_dir = _get_postgres_data_dir(env)
    log_file = _get_postgres_log_file(env)
    socket_dir = _get_postgres_socket_dir(env)
    lock_file = _get_instance_lock_file(env)

    ready = subprocess.run(
        ["pg_isready", "-h", host, "-p", str(port), "-q"],
        capture_output=True,
        timeout=5,
    )
    if ready.returncode == 0:
        ccyo_out.success("Local PostgreSQL is running")
    else:
        ccyo_out.error("○ Local PostgreSQL is not running")
        ccyo_out.print_text(f"  Start with: [cyan]tapdb --config <path> --env {env.value} "
            f"pg start-local {env.value}[/cyan]")

    ccyo_out.print_text("\n[bold]Local Runtime:[/bold]")
    ctx = resolve_context(
        require_keys=True,
        env_name=env.value,
    )
    ccyo_out.print_text(f"  Namespace: {ctx.namespace_slug()}")
    ccyo_out.print_text(f"  Host:      {host}")
    ccyo_out.print_text(f"  Port:      {port}")
    ccyo_out.print_text(f"  User:      {user}")
    ccyo_out.print_text(f"  Data dir:  {data_dir}")
    ccyo_out.print_text(f"  Log file:  {log_file}")
    ccyo_out.print_text(f"  Socket dir: {socket_dir}")
    ccyo_out.print_text(f"  Lock file: {lock_file}")


@pg_app.command("logs")
def pg_logs(
    follow: bool = typer.Option(
        True, "--follow/--no-follow", "-f/-F", help="Follow log output (default: true)"
    ),
    lines: int = typer.Option(50, "--lines", "-n", help="Number of lines to show"),
):
    """View PostgreSQL logs (tails by default, Ctrl+C to stop)."""
    method, _, _, log_path = _get_pg_service_cmd()

    env = _active_env()
    local_logs: list[Path] = []
    try:
        if env != Environment.prod:
            local_logs.append(_get_postgres_log_file(env))
    except RuntimeError:
        # Namespace may be unresolved for non-local invocations.
        pass

    possible_logs = local_logs + [
        log_path,
        Path("/var/log/postgresql/postgresql-16-main.log"),
        Path("/var/log/postgresql/postgresql.log"),
    ]

    log_file = None
    for lf in possible_logs:
        if lf and lf.exists():
            log_file = lf
            break

    if not log_file:
        ccyo_out.warning("PostgreSQL log file not found")
        if local_logs:
            for path in local_logs:
                ccyo_out.print_text(f"  Checked local: {path}")

        # Try journalctl on Linux
        if platform.system() == "Linux":
            ccyo_out.print_text("\n[dim]Trying journalctl...[/dim]")
            cmd = ["sudo", "journalctl", "-u", "postgresql", "-n", str(lines)]
            if follow:
                cmd.append("-f")
            try:
                subprocess.run(cmd)
            except KeyboardInterrupt:
                ccyo_out.print_text("\n[dim]Stopped.[/dim]")
            except Exception as e:
                ccyo_out.error(f"{e}")
        return

    ccyo_out.print_text(f"[dim]Log file: {log_file}[/dim]\n")

    if follow:
        try:
            subprocess.run(["tail", "-f", "-n", str(lines), str(log_file)])
        except KeyboardInterrupt:
            ccyo_out.print_text("\n[dim]Stopped.[/dim]")
    else:
        # --no-follow / -F
        try:
            with open(log_file, "r") as f:
                all_lines = f.readlines()
                for line in all_lines[-lines:]:
                    ccyo_out.print_text(line.rstrip())
        except PermissionError:
            ccyo_out.error(f"Permission denied reading {log_file}")
            ccyo_out.print_text(f"  Try: [cyan]sudo tail -n {lines} {log_file}[/cyan]")
        except Exception as e:
            ccyo_out.error(f"Error reading logs: {e}")


@pg_app.command("restart")
def pg_restart():
    """Restart local PostgreSQL service."""
    pg_stop()
    import time

    time.sleep(2)
    pg_start()


@pg_app.command("init")
def pg_init(
    env: Environment = typer.Argument(..., help="Target environment (dev/test only)"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Reinitialize if already exists"
    ),
):
    """Initialize a PostgreSQL data directory for dev/test.

    Creates a local PostgreSQL data directory in:
    ~/.config/tapdb/<client-id>/<database-name>/<env>/postgres/data
    for development and testing. Production uses system PostgreSQL.

    After init, start with:
    tapdb --config <path> --env <name> pg start-local <env>
    """
    if env == Environment.prod:
        ccyo_out.error("Cannot init prod environment locally")
        ccyo_out.print_text("  Production should use system PostgreSQL installation")
        raise typer.Exit(1)

    data_dir = _get_postgres_data_dir(env)

    ccyo_out.print_text(f"\n[bold cyan]━━━ Initialize PostgreSQL ({env.value}) ━━━[/bold cyan]")
    ccyo_out.print_text(f"  Data directory: {data_dir}")

    # Check if initdb is available (must be in PATH)
    initdb_path = shutil.which("initdb")
    if not initdb_path:
        ccyo_out.error("initdb not found")
        ccyo_out.print_text("  Install PostgreSQL and ensure 'initdb' is on PATH")
        ccyo_out.print_text("  [cyan]conda install -c conda-forge postgresql[/cyan]")
        raise typer.Exit(1)

    # Check if already initialized
    if data_dir.exists() and (data_dir / "PG_VERSION").exists():
        if not force:
            ccyo_out.warning("Data directory already initialized")
            ccyo_out.print_text("  Use --force to reinitialize (will delete existing data)")
            return
        else:
            ccyo_out.warning("► Removing existing data directory...")
            shutil.rmtree(data_dir)

    # Create parent directory
    data_dir.parent.mkdir(parents=True, exist_ok=True)

    ccyo_out.warning("► Running initdb...")
    cfg = get_db_config_for_env(env.value)
    initdb_superuser = str(cfg.get("user") or "postgres").strip() or "postgres"

    # Run initdb
    try:
        result = subprocess.run(
            [
                initdb_path,
                "-D",
                str(data_dir),
                "--no-locale",
                "-E",
                "UTF8",
                "-U",
                initdb_superuser,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode == 0:
            ccyo_out.success("PostgreSQL data directory initialized")
            ccyo_out.print_text("\n[bold]Next steps:[/bold]")
            ccyo_out.print_text(f"  [cyan]tapdb --config <path> --env {env.value} "
                f"pg start-local {env.value}[/cyan]  # Start PostgreSQL")
            ccyo_out.print_text(f"  [cyan]tapdb --config <path> --env {env.value} "
                f"db setup {env.value}[/cyan]"
                "        # Create DB + schema + seed")
        else:
            ccyo_out.error("initdb failed")
            ccyo_out.print_text(f"  {result.stderr}")
            raise typer.Exit(1)
    except subprocess.TimeoutExpired:
        ccyo_out.error("initdb timed out")
        raise typer.Exit(1)
    except typer.Exit:
        raise
    except Exception as e:
        ccyo_out.error(f"Error: {e}")
        raise typer.Exit(1)


@pg_app.command("start-local")
def pg_start_local(
    env: Environment = typer.Argument(..., help="Target environment (dev/test only)"),
    port: Optional[int] = typer.Option(
        None,
        "--port",
        "-p",
        help="Port override (must match configured environments.<env>.port)",
    ),
):
    """Start a local PostgreSQL instance for dev/test.

    Uses the data directory created by 'tapdb pg init'.
    """
    if env == Environment.prod:
        ccyo_out.error("Use 'tapdb pg start' for production")
        raise typer.Exit(1)

    cfg = get_db_config_for_env(env.value)
    configured_port = int(str(cfg.get("port") or "0"))
    if configured_port < 1:
        ccyo_out.error(f"Missing/invalid configured port for env {env.value}.")
        ccyo_out.print_text(f"  Set environments.{env.value}.port in the namespaced TAPDB config.")
        raise typer.Exit(1)

    if port is None:
        port = configured_port
    elif port != configured_port:
        ccyo_out.error("--port override does not match configured TAPDB env port.")
        ccyo_out.print_text(f"  Configured environments.{env.value}.port = {configured_port}")
        raise typer.Exit(1)

    data_dir = _get_postgres_data_dir(env)
    lock_file = _get_instance_lock_file(env)
    socket_dir = _get_postgres_socket_dir(env)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    socket_dir.mkdir(parents=True, exist_ok=True)

    if not data_dir.exists() or not (data_dir / "PG_VERSION").exists():
        ccyo_out.error("Data directory not initialized")
        ccyo_out.print_text(f"  Run: [cyan]tapdb pg init {env.value}[/cyan]")
        raise typer.Exit(1)

    # Find pg_ctl (must be in PATH)
    pg_ctl_path = shutil.which("pg_ctl")
    if not pg_ctl_path:
        ccyo_out.error("pg_ctl not found")
        ccyo_out.print_text("  Install PostgreSQL and ensure 'pg_ctl' is on PATH")
        ccyo_out.print_text("  [cyan]conda install -c conda-forge postgresql[/cyan]")
        raise typer.Exit(1)

    # Check if already running
    pid_file = data_dir / "postmaster.pid"
    if pid_file.exists():
        ccyo_out.warning(f"PostgreSQL may already be running for {env.value}")
        ccyo_out.print_text(f"  PID file exists: {pid_file}")
        return

    if not _is_port_available(port):
        ccyo_out.error(f"{_port_conflict_details(port)}")
        ccyo_out.print_text("  Update environments."
            f"{env.value}.port in the namespaced TAPDB config to a free port.")
        raise typer.Exit(1)

    ccyo_out.warning(f"► Starting PostgreSQL ({env.value}) on port {port}...")

    log_file = _get_postgres_log_file(env)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    postgres_opts = _build_pg_ctl_options(port, socket_dir)

    try:
        result = subprocess.run(
            [
                pg_ctl_path,
                "start",
                "-D",
                str(data_dir),
                "-l",
                str(log_file),
                "-o",
                postgres_opts,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            ccyo_out.success("PostgreSQL started")
            ccyo_out.print_text(f"  Port: {port}")
            ccyo_out.print_text(f"  Data: {data_dir}")
            ccyo_out.print_text(f"  Log:  {log_file}")
            ccyo_out.print_text(f"  Socket dir: {socket_dir}")
            lock_payload = {
                "env": env.value,
                "port": port,
                "data_dir": str(data_dir),
                "log_file": str(log_file),
                "socket_dir": str(socket_dir),
            }
            lock_file.write_text(
                json.dumps(lock_payload, indent=2) + "\n",
                encoding="utf-8",
            )
            ccyo_out.print_text(f"  Lock: {lock_file}")
            ccyo_out.print_text("\n[bold]Next step:[/bold]")
            ccyo_out.print_text(f"  [cyan]tapdb --env {env.value} db setup {env.value}[/cyan]")
        else:
            ccyo_out.error("Failed to start PostgreSQL")
            ccyo_out.print_text(f"  {result.stderr}")
            ccyo_out.print_text(f"  Check log: {log_file}")
            raise typer.Exit(1)
    except typer.Exit:
        raise
    except Exception as e:
        ccyo_out.error(f"Error: {e}")
        raise typer.Exit(1)


@pg_app.command("stop-local")
def pg_stop_local(
    env: Environment = typer.Argument(..., help="Target environment (dev/test only)"),
):
    """Stop a local PostgreSQL instance for dev/test."""
    if env == Environment.prod:
        ccyo_out.error("Use 'tapdb pg stop' for production")
        raise typer.Exit(1)

    data_dir = _get_postgres_data_dir(env)
    lock_file = _get_instance_lock_file(env)

    if not data_dir.exists():
        ccyo_out.warning(f"Data directory doesn't exist: {data_dir}")
        return

    # Find pg_ctl (must be in PATH)
    pg_ctl_path = shutil.which("pg_ctl")
    if not pg_ctl_path:
        ccyo_out.error("pg_ctl not found")
        ccyo_out.print_text("  Install PostgreSQL and ensure 'pg_ctl' is on PATH")
        ccyo_out.print_text("  [cyan]conda install -c conda-forge postgresql[/cyan]")
        raise typer.Exit(1)

    ccyo_out.warning(f"► Stopping PostgreSQL ({env.value})...")

    try:
        result = subprocess.run(
            [pg_ctl_path, "stop", "-D", str(data_dir), "-m", "fast"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            ccyo_out.success("PostgreSQL stopped")
            lock_file.unlink(missing_ok=True)
        else:
            err = result.stderr.strip() or ("PostgreSQL may not be running")
            ccyo_out.warning(f"{err}")
    except Exception as e:
        ccyo_out.error(f"Error: {e}")
        raise typer.Exit(1)
