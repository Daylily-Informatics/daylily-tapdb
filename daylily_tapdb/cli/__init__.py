"""CLI entry point for daylily-tapdb."""

import os
import signal
import subprocess
import sys
import time
import importlib.util
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()


def _require_admin_extras() -> None:
    """Fail fast with a clear message if Admin UI extras aren't installed."""
    # We only need to ensure uvicorn is present here; FastAPI/etc are imported by admin.main.
    if importlib.util.find_spec("uvicorn") is None:
        console.print("[red]✗[/red] Admin UI dependencies are not installed.")
        console.print("  Install with: [cyan]pip install 'daylily-tapdb[admin]'[/cyan]")
        raise typer.Exit(1)


# Import subcommand modules
from daylily_tapdb.cli.db import db_app
from daylily_tapdb.cli.pg import pg_app
from daylily_tapdb.cli.user import user_app


app = typer.Typer(
    name="tapdb",
    help="TAPDB - Templated Abstract Polymorphic Database CLI",
    add_completion=True,
)
ui_app = typer.Typer(help="Admin UI management commands")
app.add_typer(ui_app, name="ui")
app.add_typer(db_app, name="db")
app.add_typer(pg_app, name="pg")
app.add_typer(user_app, name="user")

# PID file location
PID_FILE = Path.home() / ".tapdb" / "ui.pid"
LOG_FILE = Path.home() / ".tapdb" / "ui.log"


def _ensure_dir():
    """Ensure .tapdb directory exists."""
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)


def _get_pid() -> Optional[int]:
    """Get the running UI server PID if exists."""
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            # Check if process is running
            os.kill(pid, 0)
            return pid
        except (ValueError, ProcessLookupError, PermissionError):
            PID_FILE.unlink(missing_ok=True)
    return None


def _find_admin_module() -> str:
    """Find the admin module path."""
    # Check if running from repo with admin/ directory
    cwd_admin = Path.cwd() / "admin"
    if cwd_admin.exists() and (cwd_admin / "main.py").exists():
        return "admin.main:app"

    # Check relative to this file (installed package)
    pkg_admin = Path(__file__).parent.parent.parent / "admin"
    if pkg_admin.exists() and (pkg_admin / "main.py").exists():
        return "admin.main:app"

    raise typer.BadParameter(
        "Cannot find admin module. Run from the daylily-tapdb repo root, "
        "or ensure admin/ is installed."
    )


@ui_app.command("start")
def ui_start(
    port: int = typer.Option(8000, "--port", "-p", help="Port to run the server on"),
    host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host to bind to"),
    reload: bool = typer.Option(False, "--reload", "-r", help="Enable auto-reload"),
    background: bool = typer.Option(True, "--background/--foreground", "-b/-f", help="Run in background"),
):
    """Start the TAPDB Admin UI server."""
    _ensure_dir()

    _require_admin_extras()

    # Check if already running
    pid = _get_pid()
    if pid:
        console.print(f"[yellow]⚠[/yellow]  UI server already running (PID {pid})")
        console.print(f"   URL: [cyan]http://{host}:{port}[/cyan]")
        return

    try:
        admin_module = _find_admin_module()
    except typer.BadParameter as e:
        console.print(f"[red]✗[/red]  {e}")
        raise typer.Exit(1)

    cmd = [
        sys.executable, "-m", "uvicorn",
        admin_module,
        "--host", host,
        "--port", str(port),
    ]
    if reload:
        cmd.append("--reload")

    if background:
        # Start in background
        with open(LOG_FILE, "w") as log_f:
            proc = subprocess.Popen(
                cmd,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

        # Wait a moment and check if it started
        time.sleep(1)
        if proc.poll() is not None:
            console.print("[red]✗[/red]  Server failed to start. Check logs:")
            console.print(f"   [dim]{LOG_FILE}[/dim]")
            raise typer.Exit(1)

        PID_FILE.write_text(str(proc.pid))
        console.print(f"[green]✓[/green]  UI server started (PID {proc.pid})")
        console.print(f"   URL: [cyan]http://{host}:{port}[/cyan]")
        console.print(f"   Logs: [dim]{LOG_FILE}[/dim]")
    else:
        # Run in foreground
        console.print(f"[green]✓[/green]  Starting UI server on [cyan]http://{host}:{port}[/cyan]")
        console.print("   Press Ctrl+C to stop\n")
        try:
            subprocess.run(cmd)
        except KeyboardInterrupt:
            console.print("\n[yellow]⚠[/yellow]  Server stopped")


@ui_app.command("stop")
def ui_stop():
    """Stop the TAPDB Admin UI server."""
    pid = _get_pid()
    if not pid:
        console.print("[yellow]⚠[/yellow]  No UI server running")
        return

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait for graceful shutdown
        for _ in range(10):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            # Force kill if still running
            os.kill(pid, signal.SIGKILL)

        PID_FILE.unlink(missing_ok=True)
        console.print(f"[green]✓[/green]  UI server stopped (was PID {pid})")
    except ProcessLookupError:
        PID_FILE.unlink(missing_ok=True)
        console.print("[yellow]⚠[/yellow]  Server was not running")
    except PermissionError:
        console.print(f"[red]✗[/red]  Permission denied stopping PID {pid}")
        raise typer.Exit(1)


@ui_app.command("status")
def ui_status():
    """Check the status of the TAPDB Admin UI server."""
    pid = _get_pid()
    if pid:
        console.print(f"[green]●[/green]  UI server is [green]running[/green] (PID {pid})")
        console.print(f"   Logs: [dim]{LOG_FILE}[/dim]")
    else:
        console.print("[dim]○[/dim]  UI server is [dim]not running[/dim]")


@ui_app.command("logs")
def ui_logs(
    follow: bool = typer.Option(True, "--follow/--no-follow", "-f/-F", help="Follow log output (default: true)"),
    lines: int = typer.Option(50, "--lines", "-n", help="Number of lines to show"),
):
    """View TAPDB Admin UI server logs (tails by default, Ctrl+C to stop)."""
    if not LOG_FILE.exists():
        console.print("[yellow]⚠[/yellow]  No log file found. Start the server first.")
        return

    if follow:
        console.print(f"[dim]Following {LOG_FILE} (Ctrl+C to stop)[/dim]\n")
        try:
            subprocess.run(["tail", "-f", "-n", str(lines), str(LOG_FILE)])
        except KeyboardInterrupt:
            console.print("\n[dim]Stopped.[/dim]")
    else:
        # Read last N lines (--no-follow / -F)
        try:
            with open(LOG_FILE, "r") as f:
                all_lines = f.readlines()
                for line in all_lines[-lines:]:
                    console.print(line.rstrip())
        except Exception as e:
            console.print(f"[red]✗[/red]  Error reading logs: {e}")


@ui_app.command("restart")
def ui_restart(
    port: int = typer.Option(8000, "--port", "-p", help="Port to run the server on"),
    host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host to bind to"),
):
    """Restart the TAPDB Admin UI server."""
    ui_stop()
    time.sleep(1)
    ui_start(port=port, host=host, reload=False, background=True)


@app.command("version")
def version():
    """Show TAPDB version."""
    from daylily_tapdb import __version__
    console.print(f"daylily-tapdb [cyan]{__version__}[/cyan]")


@app.command("info")
def info():
    """Show TAPDB configuration and status."""
    from daylily_tapdb import __version__

    table = Table(title="TAPDB Info")
    table.add_column("Property", style="cyan")
    table.add_column("Value")

    table.add_row("Version", __version__)
    table.add_row("Python", sys.version.split()[0])
    table.add_row("Config Dir", str(PID_FILE.parent))

    pid = _get_pid()
    table.add_row("UI Server", f"Running (PID {pid})" if pid else "Stopped")

    console.print(table)


def main():
    """Main CLI entry point."""
    app()


if __name__ == "__main__":
    raise SystemExit(main())
