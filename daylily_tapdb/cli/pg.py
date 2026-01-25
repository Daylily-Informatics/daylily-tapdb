"""PostgreSQL service management commands for TAPDB CLI."""

import os
import platform
import subprocess
import sys
from enum import Enum
from pathlib import Path
import shutil

import typer
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from daylily_tapdb.cli.db_config import get_db_config_for_env
console = Console()

pg_app = typer.Typer(help="PostgreSQL service management commands")


def _get_project_root() -> Path:
    """Get the project root directory."""
    # Try to find project root by looking for pyproject.toml
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    # Fall back to cwd
    return Path.cwd()


def _get_postgres_data_dir(env: "Environment") -> Path:
    """Get PostgreSQL data directory for environment.

    dev/test: ./postgres_data/<env>
    prod: system default or PGDATA env var
    """
    if env.value == "prod":
        # Production uses system default
        return Path(os.environ.get("PGDATA", "/var/lib/postgresql/data"))
    else:
        # dev/test use project-local data directory
        return _get_project_root() / "postgres_data" / env.value


# Environment enum (mirrors db.py)
class Environment(str, Enum):
    dev = "dev"
    test = "test"
    prod = "prod"


def _get_db_config(env: Environment) -> dict:
    """Get database configuration for environment."""
    return get_db_config_for_env(env.value)


def _run_psql(sql: str, database: str = "postgres", config: dict = None) -> tuple[bool, str]:
    """Run a SQL command via psql. Returns (success, output)."""
    if config is None:
        config = {"host": "localhost", "port": "5432", "user": os.environ.get("USER", "postgres")}

    env = os.environ.copy()
    if config.get("password"):
        env["PGPASSWORD"] = config["password"]

    cmd = [
        "psql",
        "-X",  # do not read ~/.psqlrc
        "-q",  # quiet
        "-t",  # tuples only
        "-A",  # unaligned
        "-h", config["host"],
        "-p", str(config["port"]),
        "-U", config["user"],
        "-d", database,
        "-v", "ON_ERROR_STOP=1",
        "-c", sql,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=30)
        return result.returncode == 0, result.stdout.strip() or result.stderr.strip()
    except FileNotFoundError:
        return False, "psql not found. Install PostgreSQL client tools."
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def _database_exists(db_name: str, config: dict) -> bool:
    """Check if a database exists."""
    sql = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"
    success, output = _run_psql(sql, "postgres", config)
    return success and output.strip() == "1"


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
                Path("/var/log/postgresql/postgresql-14-main.log")
            )
        # Check for service command
        elif Path("/usr/sbin/service").exists():
            return (
                "sysvinit",
                ["sudo", "service", "postgresql", "start"],
                ["sudo", "service", "postgresql", "stop"],
                Path("/var/log/postgresql/postgresql-14-main.log")
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
            version = ver_result.stdout.strip().split(",")[0] if ver_result.returncode == 0 else "unknown"
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
        console.print(f"[green]●[/green] PostgreSQL is already running")
        console.print(f"  {details}")
        return

    method, start_cmd, _, _ = _get_pg_service_cmd()

    if method == "unknown":
        console.print("[red]✗[/red] No system PostgreSQL service found")
        console.print("")
        console.print(
            "[bold]Recommended: Use TAPDB local dev/test commands[/bold]"
        )
        console.print("  [cyan]tapdb pg init dev[/cyan]         # Initialize data directory")
        console.print("  [cyan]tapdb pg start-local dev[/cyan]  # Start local instance")
        console.print("")
        console.print("[dim]Install PostgreSQL so initdb/pg_ctl are on PATH (conda recommended):[/dim]")
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
                    console.print(f"[green]✓[/green] PostgreSQL started")
                    console.print(f"  {details}")
                    return
            
            console.print("[yellow]⚠[/yellow] Start command succeeded but PostgreSQL not responding")
            console.print(f"  Check logs: [cyan]tapdb pg logs[/cyan]")
        else:
            console.print(f"[red]✗[/red] Failed to start PostgreSQL")
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
        console.print(f"[dim]○[/dim] PostgreSQL is not running")
        return

    method, _, stop_cmd, _ = _get_pg_service_cmd()

    if method == "unknown":
        console.print("[red]✗[/red] No system PostgreSQL service found")
        console.print("  For local instances, use: [cyan]tapdb pg stop-local <env>[/cyan]")
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
                    console.print(f"[green]✓[/green] PostgreSQL stopped")
                    return

            console.print("[yellow]⚠[/yellow] Stop command succeeded but PostgreSQL still running")
        else:
            console.print(f"[red]✗[/red] Failed to stop PostgreSQL")
            console.print(f"  {result.stderr}")
            raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(1)


@pg_app.command("status")
def pg_status():
    """Check if PostgreSQL is running and show connection info."""
    console.print(f"\n[bold cyan]━━━ PostgreSQL Status ━━━[/bold cyan]")

    running, details = _is_pg_running()

    if running:
        console.print(f"[green]●[/green] PostgreSQL is [green]running[/green]")
        console.print(f"  Version: {details}")
    else:
        console.print(f"[red]○[/red] PostgreSQL is [red]not running[/red]")
        if details and details not in ("", "timeout"):
            console.print(f"  Error: {details}")
        console.print(f"\n  Start with: [cyan]tapdb pg start[/cyan]")
        return

    # Connection info
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", os.environ.get("USER", "postgres"))

    console.print(f"\n[bold]Connection Info:[/bold]")
    console.print(f"  Host: {host}")
    console.print(f"  Port: {port}")
    console.print(f"  User: {user}")

    # List databases
    try:
        result = subprocess.run(
            ["psql", "-t", "-c", "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"],
            capture_output=True,
            text=True,
            env={**os.environ, "PGDATABASE": "postgres"},
            timeout=5,
        )
        if result.returncode == 0:
            databases = [db.strip() for db in result.stdout.strip().split("\n") if db.strip()]
            tapdb_dbs = [db for db in databases if db.startswith("tapdb")]

            console.print(f"\n[bold]TAPDB Databases:[/bold]")
            if tapdb_dbs:
                for db in tapdb_dbs:
                    console.print(f"  [green]●[/green] {db}")
            else:
                console.print(f"  [dim]None found (create with: tapdb db create dev)[/dim]")
    except Exception:
        pass


@pg_app.command("logs")
def pg_logs(
    follow: bool = typer.Option(True, "--follow/--no-follow", "-f/-F", help="Follow log output (default: true)"),
    lines: int = typer.Option(50, "--lines", "-n", help="Number of lines to show"),
):
    """View PostgreSQL logs (tails by default, Ctrl+C to stop)."""
    method, _, _, log_path = _get_pg_service_cmd()

    # Try to find log file - prioritize local postgres_data logs
    project_root = Path(__file__).parent.parent.parent
    local_logs = [
        project_root / "postgres_data" / "dev" / "postgresql.log",
        project_root / "postgres_data" / "test" / "postgresql.log",
    ]

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
        console.print(f"  Checked: ./postgres_data/dev/postgresql.log, ./postgres_data/test/postgresql.log")

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


@pg_app.command("create")
def pg_create(
    env: Environment = typer.Argument(..., help="Target environment"),
    owner: str = typer.Option(None, "--owner", "-o", help="Database owner (default: connection user)"),
):
    """Create the TAPDB database for the specified environment.

    Creates an empty PostgreSQL database named tapdb_<env> (e.g., tapdb_dev).
    Use 'tapdb db create <env>' to initialize the schema after creating the database.
    """
    config = _get_db_config(env)
    db_name = config["database"]
    db_owner = owner or config["user"]

    # Check connectivity to the configured server
    ok, out = _run_psql("SELECT 1", "postgres", config)
    if not ok:
        console.print("[red]✗[/red] Cannot connect to PostgreSQL for this environment")
        console.print(f"  {out}")
        if env in (Environment.dev, Environment.test):
            console.print(f"  If using local dev/test: [cyan]tapdb pg start-local {env.value}[/cyan]")
            console.print("  If tools are missing: [cyan]conda install -c conda-forge postgresql[/cyan]")
        else:
            console.print("  Verify TAPDB_PROD_* connection settings and network access")
        raise typer.Exit(1)

    # Check if database already exists
    if _database_exists(db_name, config):
        console.print(f"[yellow]⚠[/yellow] Database '{db_name}' already exists")
        return

    console.print(f"[yellow]►[/yellow] Creating database '{db_name}'...")

    # Create the database
    sql = f'CREATE DATABASE "{db_name}" OWNER "{db_owner}"'
    success, output = _run_psql(sql, "postgres", config)

    if success:
        console.print(f"[green]✓[/green] Database '{db_name}' created")
        console.print(f"  Owner: {db_owner}")
        console.print(f"\n  Next: [cyan]tapdb db create {env.value}[/cyan] to initialize schema")
    else:
        console.print(f"[red]✗[/red] Failed to create database")
        console.print(f"  {output}")
        raise typer.Exit(1)


@pg_app.command("delete")
def pg_delete(
    env: Environment = typer.Argument(..., help="Target environment"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Delete the TAPDB database for the specified environment.

    ⚠️  DESTRUCTIVE: This permanently deletes the database and all its data!
    """
    config = _get_db_config(env)
    db_name = config["database"]

    # Check connectivity to the configured server
    ok, out = _run_psql("SELECT 1", "postgres", config)
    if not ok:
        console.print("[red]✗[/red] Cannot connect to PostgreSQL for this environment")
        console.print(f"  {out}")
        if env in (Environment.dev, Environment.test):
            console.print(f"  If using local dev/test: [cyan]tapdb pg start-local {env.value}[/cyan]")
            console.print("  If tools are missing: [cyan]conda install -c conda-forge postgresql[/cyan]")
        else:
            console.print("  Verify TAPDB_PROD_* connection settings and network access")
        raise typer.Exit(1)

    # Check if database exists
    if not _database_exists(db_name, config):
        console.print(f"[yellow]⚠[/yellow] Database '{db_name}' does not exist")
        return

    # Safety confirmation for non-dev environments
    if not force:
        console.print(f"\n[bold red]⚠️  WARNING: DESTRUCTIVE OPERATION[/bold red]")
        console.print(f"This will permanently delete database: [bold]{db_name}[/bold]")
        console.print(f"All data will be lost!\n")

        if env == Environment.prod:
            console.print("[bold red]🚨 THIS IS A PRODUCTION DATABASE! 🚨[/bold red]\n")

        if not Confirm.ask(f"Delete database '{db_name}'?", default=False):
            console.print("[dim]Aborted.[/dim]")
            raise typer.Exit(0)

        # Second confirmation for prod
        if env == Environment.prod:
            typed = typer.prompt("Type the database name to confirm")
            if typed != db_name:
                console.print(f"[red]✗[/red] Name mismatch. Aborted.")
                raise typer.Exit(1)

    console.print(f"[yellow]►[/yellow] Deleting database '{db_name}'...")

    # Terminate existing connections
    term_sql = f"""
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '{db_name}' AND pid <> pg_backend_pid()
    """
    _run_psql(term_sql, "postgres", config)

    # Drop the database
    sql = f'DROP DATABASE "{db_name}"'
    success, output = _run_psql(sql, "postgres", config)

    if success:
        console.print(f"[green]✓[/green] Database '{db_name}' deleted")
    else:
        console.print(f"[red]✗[/red] Failed to delete database")
        console.print(f"  {output}")
        raise typer.Exit(1)


@pg_app.command("init")
def pg_init(
    env: Environment = typer.Argument(..., help="Target environment (dev/test only)"),
    force: bool = typer.Option(False, "--force", "-f", help="Reinitialize if already exists"),
):
    """Initialize a PostgreSQL data directory for dev/test.

    Creates a local PostgreSQL data directory in ./postgres_data/<env>
    for development and testing. Production uses system PostgreSQL.

    After init, start with: tapdb pg start-local <env>
    """
    if env == Environment.prod:
        console.print("[red]✗[/red] Cannot init prod environment locally")
        console.print("  Production should use system PostgreSQL installation")
        raise typer.Exit(1)

    data_dir = _get_postgres_data_dir(env)

    console.print(f"\n[bold cyan]━━━ Initialize PostgreSQL ({env.value}) ━━━[/bold cyan]")
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
            console.print(f"[yellow]⚠[/yellow] Data directory already initialized")
            console.print(f"  Use --force to reinitialize (will delete existing data)")
            return
        else:
            console.print(f"[yellow]►[/yellow] Removing existing data directory...")
            shutil.rmtree(data_dir)

    # Create parent directory
    data_dir.parent.mkdir(parents=True, exist_ok=True)

    console.print(f"[yellow]►[/yellow] Running initdb...")

    # Run initdb
    try:
        result = subprocess.run(
            [initdb_path, "-D", str(data_dir), "--no-locale", "-E", "UTF8"],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode == 0:
            console.print(f"[green]✓[/green] PostgreSQL data directory initialized")
            console.print(f"\n[bold]Next steps:[/bold]")
            console.print(f"  [cyan]tapdb pg start-local {env.value}[/cyan]  # Start PostgreSQL")
            console.print(f"  [cyan]tapdb db setup {env.value}[/cyan]        # Create DB + schema + seed")
        else:
            console.print(f"[red]✗[/red] initdb failed")
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
    port: int = typer.Option(None, "--port", "-p", help="Port (default: 5432 for dev, 5433 for test)"),
):
    """Start a local PostgreSQL instance for dev/test.

    Uses the data directory created by 'tapdb pg init'.
    """
    if env == Environment.prod:
        console.print("[red]✗[/red] Use 'tapdb pg start' for production")
        raise typer.Exit(1)

    data_dir = _get_postgres_data_dir(env)

    if not data_dir.exists() or not (data_dir / "PG_VERSION").exists():
        console.print(f"[red]✗[/red] Data directory not initialized")
        console.print(f"  Run: [cyan]tapdb pg init {env.value}[/cyan]")
        raise typer.Exit(1)

    # Default ports: dev=5432, test=5433
    if port is None:
        port = 5432 if env == Environment.dev else 5433

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
        console.print(f"[yellow]⚠[/yellow] PostgreSQL may already be running for {env.value}")
        console.print(f"  PID file exists: {pid_file}")
        return

    console.print(f"[yellow]►[/yellow] Starting PostgreSQL ({env.value}) on port {port}...")

    log_file = data_dir / "postgresql.log"

    try:
        result = subprocess.run(
            [
                pg_ctl_path, "start",
                "-D", str(data_dir),
                "-l", str(log_file),
                "-o", f"-p {port}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            console.print(f"[green]✓[/green] PostgreSQL started")
            console.print(f"  Port: {port}")
            console.print(f"  Data: {data_dir}")
            console.print(f"  Log:  {log_file}")

            # Set env vars hint
            console.print(f"\n[bold]Set environment:[/bold]")
            env_prefix = f"TAPDB_{env.value.upper()}_"
            console.print(f"  export {env_prefix}HOST=localhost")
            console.print(f"  export {env_prefix}PORT={port}")
        else:
            console.print(f"[red]✗[/red] Failed to start PostgreSQL")
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
            console.print(f"[green]✓[/green] PostgreSQL stopped")
        else:
            console.print(f"[yellow]⚠[/yellow] {result.stderr.strip() or 'PostgreSQL may not be running'}")
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(1)
