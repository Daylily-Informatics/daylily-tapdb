"""PostgreSQL service management commands for TAPDB CLI."""

import json
import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from daylily_tapdb.cli.context import resolve_context
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
    ctx = resolve_context(require_keys=True, env_name=env.value)
    return ctx.postgres_dir(env.value) / "data"


def _get_postgres_log_file(env: "Environment") -> Path:
    if env.value == "prod":
        return Path("/var/log/postgresql/postgresql.log")
    ctx = resolve_context(require_keys=True, env_name=env.value)
    return ctx.postgres_dir(env.value) / "postgresql.log"


def _get_instance_lock_file(env: "Environment") -> Path:
    if env.value == "prod":
        return Path("/tmp/tapdb-prod-instance.lock")
    ctx = resolve_context(require_keys=True, env_name=env.value)
    return ctx.lock_dir(env.value) / "instance.lock"


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
    raw = (os.environ.get("TAPDB_ENV") or "dev").strip().lower()
    try:
        return Environment(raw)
    except ValueError:
        return Environment.dev


def _get_pg_service_cmd() -> tuple[str, list[str], list[str], Path]:
    """
    Get platform-specific system PostgreSQL service commands.

    This is intended for production environments where PostgreSQL is managed as a
    system service (e.g., systemd on Linux). Local dev/test should use the
    data-dir based commands: `tapdb pg init` + `tapdb pg start-local`.

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

    For local development, prefer: tapdb pg start-local <env>
    """
    running, details = _is_pg_running()
    if running:
        console.print("[green]●[/green] PostgreSQL is already running")
        console.print(f"  {details}")
        return

    method, start_cmd, _, _ = _get_pg_service_cmd()

    if method == "unknown":
        console.print("[red]✗[/red] No system PostgreSQL service found")
        console.print("")
        console.print("[bold]Recommended: Use TAPDB local dev/test commands[/bold]")
        console.print(
            "  [cyan]tapdb pg init dev[/cyan]         # Initialize data directory"
        )
        console.print("  [cyan]tapdb pg start-local dev[/cyan]  # Start local instance")
        console.print("")
        console.print(
            "[dim]Install PostgreSQL so initdb/pg_ctl"
            " are on PATH (conda recommended):[/dim]"
        )
        console.print("  [dim]conda install -c conda-forge postgresql[/dim]")
        raise typer.Exit(1)

    console.print(f"[yellow]►[/yellow] Starting PostgreSQL ({method})...")

    try:
        result = subprocess.run(start_cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            # Wait and verify
            import time

            for _ in range(10):
                time.sleep(1)
                running, details = _is_pg_running()
                if running:
                    console.print("[green]✓[/green] PostgreSQL started")
                    console.print(f"  {details}")
                    return

            console.print(
                "[yellow]⚠[/yellow] Start command succeeded"
                " but PostgreSQL not responding"
            )
            console.print("  Check logs: [cyan]tapdb pg logs[/cyan]")
        else:
            console.print("[red]✗[/red] Failed to start PostgreSQL")
            console.print(f"  {result.stderr}")
            raise typer.Exit(1)
    except subprocess.TimeoutExpired:
        console.print("[yellow]⚠[/yellow] Start command timed out")
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(1)


@pg_app.command("stop")
def pg_stop():
    """Stop system PostgreSQL service (production only).

    For local development instances, use: tapdb pg stop-local <env>
    """
    running, _ = _is_pg_running()
    if not running:
        console.print("[dim]○[/dim] PostgreSQL is not running")
        return

    method, _, stop_cmd, _ = _get_pg_service_cmd()

    if method == "unknown":
        console.print("[red]✗[/red] No system PostgreSQL service found")
        console.print(
            "  For local instances, use: [cyan]tapdb pg stop-local <env>[/cyan]"
        )
        raise typer.Exit(1)

    console.print(f"[yellow]►[/yellow] Stopping PostgreSQL ({method})...")

    try:
        result = subprocess.run(stop_cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            # Wait and verify
            import time

            for _ in range(10):
                time.sleep(1)
                running, _ = _is_pg_running()
                if not running:
                    console.print("[green]✓[/green] PostgreSQL stopped")
                    return

            console.print(
                "[yellow]⚠[/yellow] Stop command succeeded but PostgreSQL still running"
            )
        else:
            console.print("[red]✗[/red] Failed to stop PostgreSQL")
            console.print(f"  {result.stderr}")
            raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(1)


@pg_app.command("status")
def pg_status():
    """Check if PostgreSQL is running and show connection info."""
    console.print("\n[bold cyan]━━━ PostgreSQL Status ━━━[/bold cyan]")

    env = _active_env()
    if env == Environment.prod:
        running, details = _is_pg_running()
        if running:
            console.print("[green]●[/green] PostgreSQL is [green]running[/green]")
            console.print(f"  Version: {details}")
        else:
            console.print("[red]○[/red] PostgreSQL is [red]not running[/red]")
            if details and details not in ("", "timeout"):
                console.print(f"  Error: {details}")
            console.print("\n  Start with: [cyan]tapdb pg start[/cyan]")
        return

    cfg = get_db_config_for_env(env.value)
    host = cfg["host"]
    port = cfg["port"]
    user = cfg["user"]
    data_dir = _get_postgres_data_dir(env)
    log_file = _get_postgres_log_file(env)
    lock_file = _get_instance_lock_file(env)

    ready = subprocess.run(
        ["pg_isready", "-h", host, "-p", str(port), "-q"],
        capture_output=True,
        timeout=5,
    )
    if ready.returncode == 0:
        console.print("[green]●[/green] Local PostgreSQL is [green]running[/green]")
    else:
        console.print("[red]○[/red] Local PostgreSQL is [red]not running[/red]")
        console.print(f"  Start with: [cyan]tapdb pg start-local {env.value}[/cyan]")

    console.print("\n[bold]Local Runtime:[/bold]")
    ctx = resolve_context(require_keys=True, env_name=env.value)
    console.print(f"  Namespace: {ctx.namespace_slug()}")
    console.print(f"  Host:      {host}")
    console.print(f"  Port:      {port}")
    console.print(f"  User:      {user}")
    console.print(f"  Data dir:  {data_dir}")
    console.print(f"  Log file:  {log_file}")
    console.print(f"  Lock file: {lock_file}")


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
        console.print("[yellow]⚠[/yellow] PostgreSQL log file not found")
        if local_logs:
            for path in local_logs:
                console.print(f"  Checked local: {path}")

        # Try journalctl on Linux
        if platform.system() == "Linux":
            console.print("\n[dim]Trying journalctl...[/dim]")
            cmd = ["sudo", "journalctl", "-u", "postgresql", "-n", str(lines)]
            if follow:
                cmd.append("-f")
            try:
                subprocess.run(cmd)
            except KeyboardInterrupt:
                console.print("\n[dim]Stopped.[/dim]")
            except Exception as e:
                console.print(f"[red]✗[/red] {e}")
        return

    console.print(f"[dim]Log file: {log_file}[/dim]\n")

    if follow:
        try:
            subprocess.run(["tail", "-f", "-n", str(lines), str(log_file)])
        except KeyboardInterrupt:
            console.print("\n[dim]Stopped.[/dim]")
    else:
        # --no-follow / -F
        try:
            with open(log_file, "r") as f:
                all_lines = f.readlines()
                for line in all_lines[-lines:]:
                    console.print(line.rstrip())
        except PermissionError:
            console.print(f"[red]✗[/red] Permission denied reading {log_file}")
            console.print(f"  Try: [cyan]sudo tail -n {lines} {log_file}[/cyan]")
        except Exception as e:
            console.print(f"[red]✗[/red] Error reading logs: {e}")


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

    After init, start with: tapdb pg start-local <env>
    """
    if env == Environment.prod:
        console.print("[red]✗[/red] Cannot init prod environment locally")
        console.print("  Production should use system PostgreSQL installation")
        raise typer.Exit(1)

    data_dir = _get_postgres_data_dir(env)

    console.print(
        f"\n[bold cyan]━━━ Initialize PostgreSQL ({env.value}) ━━━[/bold cyan]"
    )
    console.print(f"  Data directory: {data_dir}")

    # Check if initdb is available (must be in PATH)
    initdb_path = shutil.which("initdb")
    if not initdb_path:
        console.print("[red]✗[/red] initdb not found")
        console.print("  Install PostgreSQL and ensure 'initdb' is on PATH")
        console.print("  [cyan]conda install -c conda-forge postgresql[/cyan]")
        raise typer.Exit(1)

    # Check if already initialized
    if data_dir.exists() and (data_dir / "PG_VERSION").exists():
        if not force:
            console.print("[yellow]⚠[/yellow] Data directory already initialized")
            console.print("  Use --force to reinitialize (will delete existing data)")
            return
        else:
            console.print("[yellow]►[/yellow] Removing existing data directory...")
            shutil.rmtree(data_dir)

    # Create parent directory
    data_dir.parent.mkdir(parents=True, exist_ok=True)

    console.print("[yellow]►[/yellow] Running initdb...")

    # Run initdb
    try:
        result = subprocess.run(
            [initdb_path, "-D", str(data_dir), "--no-locale", "-E", "UTF8"],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode == 0:
            console.print("[green]✓[/green] PostgreSQL data directory initialized")
            console.print("\n[bold]Next steps:[/bold]")
            console.print(
                f"  [cyan]tapdb pg start-local {env.value}[/cyan]  # Start PostgreSQL"
            )
            console.print(
                f"  [cyan]tapdb db setup {env.value}[/cyan]"
                "        # Create DB + schema + seed"
            )
        else:
            console.print("[red]✗[/red] initdb failed")
            console.print(f"  {result.stderr}")
            raise typer.Exit(1)
    except subprocess.TimeoutExpired:
        console.print("[red]✗[/red] initdb timed out")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
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
        console.print("[red]✗[/red] Use 'tapdb pg start' for production")
        raise typer.Exit(1)

    cfg = get_db_config_for_env(env.value)
    configured_port = int(str(cfg.get("port") or "0"))
    if configured_port < 1:
        console.print(
            f"[red]✗[/red] Missing/invalid configured port for env {env.value}."
        )
        console.print(
            f"  Set environments.{env.value}.port in the namespaced TAPDB config."
        )
        raise typer.Exit(1)

    if port is None:
        port = configured_port
    elif port != configured_port:
        console.print(
            "[red]✗[/red] --port override does not match configured TAPDB env port."
        )
        console.print(f"  Configured environments.{env.value}.port = {configured_port}")
        raise typer.Exit(1)

    data_dir = _get_postgres_data_dir(env)
    lock_file = _get_instance_lock_file(env)
    lock_file.parent.mkdir(parents=True, exist_ok=True)

    if not data_dir.exists() or not (data_dir / "PG_VERSION").exists():
        console.print("[red]✗[/red] Data directory not initialized")
        console.print(f"  Run: [cyan]tapdb pg init {env.value}[/cyan]")
        raise typer.Exit(1)

    # Find pg_ctl (must be in PATH)
    pg_ctl_path = shutil.which("pg_ctl")
    if not pg_ctl_path:
        console.print("[red]✗[/red] pg_ctl not found")
        console.print("  Install PostgreSQL and ensure 'pg_ctl' is on PATH")
        console.print("  [cyan]conda install -c conda-forge postgresql[/cyan]")
        raise typer.Exit(1)

    # Check if already running
    pid_file = data_dir / "postmaster.pid"
    if pid_file.exists():
        console.print(
            f"[yellow]⚠[/yellow] PostgreSQL may already be running for {env.value}"
        )
        console.print(f"  PID file exists: {pid_file}")
        return

    if not _is_port_available(port):
        console.print(f"[red]✗[/red] {_port_conflict_details(port)}")
        console.print(
            "  Update environments."
            f"{env.value}.port in the namespaced TAPDB config to a free port."
        )
        raise typer.Exit(1)

    console.print(
        f"[yellow]►[/yellow] Starting PostgreSQL ({env.value}) on port {port}..."
    )

    log_file = _get_postgres_log_file(env)
    log_file.parent.mkdir(parents=True, exist_ok=True)

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
                f"-p {port}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            console.print("[green]✓[/green] PostgreSQL started")
            console.print(f"  Port: {port}")
            console.print(f"  Data: {data_dir}")
            console.print(f"  Log:  {log_file}")
            lock_payload = {
                "env": env.value,
                "port": port,
                "data_dir": str(data_dir),
                "log_file": str(log_file),
            }
            lock_file.write_text(
                json.dumps(lock_payload, indent=2) + "\n",
                encoding="utf-8",
            )
            console.print(f"  Lock: {lock_file}")

            # Set env vars hint
            console.print("\n[bold]Set environment:[/bold]")
            env_prefix = f"TAPDB_{env.value.upper()}_"
            console.print(f"  export {env_prefix}HOST=localhost")
            console.print(f"  export {env_prefix}PORT={port}")
        else:
            console.print("[red]✗[/red] Failed to start PostgreSQL")
            console.print(f"  {result.stderr}")
            console.print(f"  Check log: {log_file}")
            raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(1)


@pg_app.command("stop-local")
def pg_stop_local(
    env: Environment = typer.Argument(..., help="Target environment (dev/test only)"),
):
    """Stop a local PostgreSQL instance for dev/test."""
    if env == Environment.prod:
        console.print("[red]✗[/red] Use 'tapdb pg stop' for production")
        raise typer.Exit(1)

    data_dir = _get_postgres_data_dir(env)
    lock_file = _get_instance_lock_file(env)

    if not data_dir.exists():
        console.print(f"[yellow]⚠[/yellow] Data directory doesn't exist: {data_dir}")
        return

    # Find pg_ctl (must be in PATH)
    pg_ctl_path = shutil.which("pg_ctl")
    if not pg_ctl_path:
        console.print("[red]✗[/red] pg_ctl not found")
        console.print("  Install PostgreSQL and ensure 'pg_ctl' is on PATH")
        console.print("  [cyan]conda install -c conda-forge postgresql[/cyan]")
        raise typer.Exit(1)

    console.print(f"[yellow]►[/yellow] Stopping PostgreSQL ({env.value})...")

    try:
        result = subprocess.run(
            [pg_ctl_path, "stop", "-D", str(data_dir), "-m", "fast"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            console.print("[green]✓[/green] PostgreSQL stopped")
            lock_file.unlink(missing_ok=True)
        else:
            err = result.stderr.strip() or ("PostgreSQL may not be running")
            console.print(f"[yellow]⚠[/yellow] {err}")
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(1)
