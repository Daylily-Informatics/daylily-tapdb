"""CLI entry point for daylily-tapdb."""

import importlib.util
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from daylily_tapdb.cli.context import TapdbContext, resolve_context

DEFAULT_UI_PORT = 8911
DEFAULT_UI_HOST = "localhost"
DEFAULT_UI_SCHEME = "https"
# Legacy module globals retained for compatibility with older tests/tools.
PID_FILE = Path.home() / ".tapdb" / "ui.pid"
LOG_FILE = Path.home() / ".tapdb" / "ui.log"
NAMESPACE_REQUIRED_TOPLEVEL = {
    "bootstrap",
    "db",
    "pg",
    "ui",
    "cognito",
    "user",
    "aurora",
    "info",
}


def _active_env_name() -> str:
    return (os.environ.get("TAPDB_ENV") or "dev").strip().lower()


def _require_context(*, env_name: Optional[str] = None) -> TapdbContext:
    return resolve_context(
        require_keys=True,
        env_name=env_name if env_name is not None else _active_env_name(),
    )


def _ui_runtime_paths(env_name: Optional[str] = None) -> tuple[Path, Path, Path]:
    ctx = _require_context(env_name=env_name)
    ui_dir = ctx.ui_dir(env_name or _active_env_name())
    return (
        ui_dir / "ui.pid",
        ui_dir / "ui.log",
        ui_dir / "certs",
    )


def _resolve_tls_paths(env_name: Optional[str] = None) -> tuple[Path, Path]:
    """Resolve TLS cert/key paths, allowing env overrides."""
    pid_file, _, certs_dir = _ui_runtime_paths(env_name)
    _ = pid_file  # path access validates context + env
    default_cert = certs_dir / "localhost.crt"
    default_key = certs_dir / "localhost.key"
    cert = Path(os.environ.get("TAPDB_UI_SSL_CERT", str(default_cert)))
    key = Path(os.environ.get("TAPDB_UI_SSL_KEY", str(default_key)))
    return cert, key


def _ensure_tls_certificates(
    host: str, *, env_name: Optional[str] = None
) -> tuple[Path, Path]:
    """Ensure TLS cert/key exist for HTTPS UI startup."""
    cert_path, key_path = _resolve_tls_paths(env_name)
    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    openssl = shutil.which("openssl")
    if not openssl:
        raise RuntimeError(
            "openssl is required to start the UI over HTTPS. "
            "Install openssl or set TAPDB_UI_SSL_CERT/TAPDB_UI_SSL_KEY."
        )

    san = "DNS:localhost"
    if host and host != "localhost":
        san = f"{san},DNS:{host}"
    cmd = [
        openssl,
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-sha256",
        "-days",
        "3650",
        "-nodes",
        "-subj",
        "/CN=localhost",
        "-addext",
        f"subjectAltName={san}",
        "-keyout",
        str(key_path),
        "-out",
        str(cert_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        # Fallback for older OpenSSL builds without -addext support.
        fallback_cmd = [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "3650",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
        ]
        fallback = subprocess.run(fallback_cmd, capture_output=True, text=True)
        if fallback.returncode != 0:
            msg = (fallback.stderr or fallback.stdout or "").strip()
            raise RuntimeError(
                f"Failed to generate TLS certificate with openssl: {msg}"
            )

    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return cert_path, key_path


def _get_pid(pid_file: Path) -> Optional[int]:
    """Get the running UI server PID if exists."""
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            # Check if process is running
            os.kill(pid, 0)
            return pid
        except (ValueError, ProcessLookupError, PermissionError):
            pid_file.unlink(missing_ok=True)
    return None


def _port_is_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((host, port)) != 0


def _port_conflict_details(port: int) -> str:
    try:
        proc = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return "port is already in use by another process"
    if proc.returncode != 0 or not (proc.stdout or "").strip():
        return "port is already in use by another process"
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    if len(lines) < 2:
        return "port is already in use by another process"
    first = lines[1]
    return f"port {port} is in use ({first})"


def _find_admin_module() -> str:
    """Find the admin module path."""
    cwd_admin = Path.cwd() / "admin"
    if cwd_admin.exists() and (cwd_admin / "main.py").exists():
        return "admin.main:app"

    pkg_admin = Path(__file__).parent.parent.parent / "admin"
    if pkg_admin.exists() and (pkg_admin / "main.py").exists():
        return "admin.main:app"

    raise ValueError(
        "Cannot find admin module. Run from the daylily-tapdb"
        " repo root, or ensure admin/ is installed."
    )


def _require_admin_extras() -> None:
    """Fail fast with a clear message if Admin UI extras aren't installed."""
    # Fail early with a clear, actionable list.
    # Note: python-multipart installs module name `multipart`.
    required = ["fastapi", "uvicorn", "jinja2", "multipart", "itsdangerous", "passlib"]
    missing = [m for m in required if importlib.util.find_spec(m) is None]
    if missing:
        print("Admin UI dependencies are not installed.", file=sys.stderr)
        print(f"Missing modules: {', '.join(missing)}", file=sys.stderr)
        print("Install with: pip install 'daylily-tapdb[admin]'", file=sys.stderr)
        raise SystemExit(1)


def build_app():
    """Build the Typer app.

    Lazy-imports CLI deps so core installs can import daylily_tapdb.
    """
    import typer
    from rich.console import Console
    from rich.table import Table

    console = Console()

    # Import subcommand modules (require Typer/Rich)
    from daylily_tapdb.cli.cognito import cognito_app
    from daylily_tapdb.cli.db import (
        Environment as DbEnvironment,
    )
    from daylily_tapdb.cli.db import (
        _create_default_admin,
        apply_schema,
        create_database,
        db_app,
        run_migrations,
        seed_templates,
    )
    from daylily_tapdb.cli.pg import pg_app, pg_init, pg_start_local
    from daylily_tapdb.cli.user import user_app

    app = typer.Typer(
        name="tapdb",
        help="TAPDB - Templated Abstract Polymorphic Database CLI",
        add_completion=True,
    )

    @app.callback()
    def _root_callback(
        ctx: typer.Context,
        client_id: Optional[str] = typer.Option(
            None,
            "--client-id",
            help=(
                "Client/application namespace key. Required for runtime/DB commands."
            ),
        ),
        database_name: Optional[str] = typer.Option(
            None,
            "--database-name",
            help=(
                "Database namespace key. Required for runtime/DB commands."
            ),
        ),
    ):
        """Set global CLI context options."""
        os.environ["TAPDB_STRICT_NAMESPACE"] = "0"
        if ctx.resilient_parsing:
            return
        if client_id:
            os.environ["TAPDB_CLIENT_ID"] = client_id
        if database_name:
            os.environ["TAPDB_DATABASE_NAME"] = database_name

        if any(arg in ("--help", "-h") for arg in sys.argv[1:]):
            return

        invoked = (ctx.invoked_subcommand or "").strip().lower()
        strict = invoked in NAMESPACE_REQUIRED_TOPLEVEL
        os.environ["TAPDB_STRICT_NAMESPACE"] = "1" if strict else "0"
        if not strict:
            return

        try:
            _require_context()
        except RuntimeError as exc:
            console.print(f"[red]✗[/red] {exc}")
            console.print(
                "  Example: [cyan]tapdb --client-id atlas --database-name app "
                "info[/cyan]"
            )
            raise typer.Exit(1)

    bootstrap_app = typer.Typer(help="One-command environment bootstrap")
    ui_app = typer.Typer(help="Admin UI management commands")
    config_root_app = typer.Typer(help="TAPDB config namespace commands")
    app.add_typer(bootstrap_app, name="bootstrap")
    app.add_typer(ui_app, name="ui")
    app.add_typer(config_root_app, name="config")
    app.add_typer(db_app, name="db")
    app.add_typer(pg_app, name="pg")
    app.add_typer(user_app, name="user")
    app.add_typer(cognito_app, name="cognito")

    # Aurora subcommand — always visible, but requires boto3
    _has_boto3 = False
    try:
        import importlib.util as _ilu

        _has_boto3 = _ilu.find_spec("boto3") is not None
    except Exception:
        pass

    if _has_boto3:
        from daylily_tapdb.cli.aurora import aurora_app

        app.add_typer(aurora_app, name="aurora")
    else:
        aurora_stub = typer.Typer(
            help="Aurora PostgreSQL management (requires boto3)",
        )

        @aurora_stub.callback(invoke_without_command=True)
        def _aurora_missing(ctx: typer.Context):
            console.print("[red]✗[/red] boto3 is required for Aurora commands.")
            console.print(
                "  Install with: [cyan]pip install 'daylily-tapdb[aurora]'[/cyan]"
            )
            raise typer.Exit(1)

        app.add_typer(aurora_stub, name="aurora")

    @ui_app.command("start")
    def ui_start(
        port: Optional[int] = typer.Option(
            None,
            "--port",
            "-p",
            help="Port to run the server on (defaults to environments.<env>.ui_port)",
        ),
        host: str = typer.Option(
            DEFAULT_UI_HOST, "--host", "-h", help="Host to bind to"
        ),
        reload: bool = typer.Option(False, "--reload", "-r", help="Enable auto-reload"),
        background: bool = typer.Option(
            True, "--background/--foreground", "-b/-f", help="Run in background"
        ),
    ):
        """Start the TAPDB Admin UI server."""
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        env_name = _active_env_name()
        pid_file, log_file, _ = _ui_runtime_paths(env_name)
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        cfg = get_db_config_for_env(env_name)
        configured_port = int(str(cfg.get("ui_port") or DEFAULT_UI_PORT))
        if port is None:
            port = configured_port
        elif port != configured_port:
            console.print(
                "[red]✗[/red] UI port override is not allowed in strict mode."
            )
            console.print(
                f"  Configured ui_port for env {env_name}: "
                f"[cyan]{configured_port}[/cyan]"
            )
            raise typer.Exit(1)

        try:
            _require_admin_extras()
        except SystemExit:
            console.print("[red]✗[/red] Admin UI dependencies are not installed.")
            console.print(
                "  Install with: [cyan]pip install 'daylily-tapdb[admin]'[/cyan]"
            )
            raise typer.Exit(1)

        pid = _get_pid(pid_file)
        if pid:
            console.print(f"[yellow]⚠[/yellow]  UI server already running (PID {pid})")
            console.print(f"   URL: [cyan]{DEFAULT_UI_SCHEME}://{host}:{port}[/cyan]")
            console.print(f"   PID file: [dim]{pid_file}[/dim]")
            return

        if not _port_is_available(host, port):
            console.print(f"[red]✗[/red] {_port_conflict_details(port)}")
            ns = _require_context(env_name=env_name).namespace_slug()
            console.print(f"  Namespace: [dim]{ns}[/dim]")
            console.print(
                "  Update environments."
                f"{env_name}.ui_port in the namespaced config to a free port."
            )
            raise typer.Exit(1)

        try:
            admin_module = _find_admin_module()
        except ValueError as e:
            console.print(f"[red]✗[/red]  {e}")
            raise typer.Exit(1)

        try:
            cert_path, key_path = _ensure_tls_certificates(host, env_name=env_name)
        except RuntimeError as e:
            console.print(f"[red]✗[/red]  {e}")
            raise typer.Exit(1)

        cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            admin_module,
            "--host",
            host,
            "--port",
            str(port),
            "--ssl-keyfile",
            str(key_path),
            "--ssl-certfile",
            str(cert_path),
        ]
        if reload:
            cmd.append("--reload")

        if background:
            with open(log_file, "w") as log_f:
                proc = subprocess.Popen(
                    cmd,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )

            time.sleep(1)
            if proc.poll() is not None:
                console.print("[red]✗[/red]  Server failed to start. Check logs:")
                console.print(f"   [dim]{log_file}[/dim]")
                raise typer.Exit(1)

            pid_file.write_text(str(proc.pid))
            console.print(f"[green]✓[/green]  UI server started (PID {proc.pid})")
            console.print(f"   URL: [cyan]{DEFAULT_UI_SCHEME}://{host}:{port}[/cyan]")
            console.print(f"   Logs: [dim]{log_file}[/dim]")
            console.print(f"   PID:  [dim]{pid_file}[/dim]")
        else:
            console.print(
                f"[green]✓[/green]  Starting UI server on "
                f"[cyan]{DEFAULT_UI_SCHEME}://{host}:{port}[/cyan]"
            )
            console.print("   Press Ctrl+C to stop\n")
            try:
                subprocess.run(cmd)
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠[/yellow]  Server stopped")

    @ui_app.command("mkcert")
    def ui_mkcert(
        cert_file: Optional[Path] = typer.Option(
            None,
            "--cert-file",
            help="Path to write mkcert-generated TLS certificate",
        ),
        key_file: Optional[Path] = typer.Option(
            None,
            "--key-file",
            help="Path to write mkcert-generated TLS private key",
        ),
    ):
        """Install mkcert local CA and generate localhost TLS certs for the UI."""
        env_name = _active_env_name()
        mkcert = shutil.which("mkcert")
        if not mkcert:
            console.print(
                "[red]✗[/red] mkcert is required for trusted local HTTPS certs."
            )
            console.print(
                "  Install mkcert first, then rerun [cyan]tapdb ui mkcert[/cyan]."
            )
            raise typer.Exit(1)

        default_cert, default_key = _resolve_tls_paths(env_name)
        cert_path = (cert_file or default_cert).expanduser()
        key_path = (key_file or default_key).expanduser()
        cert_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.parent.mkdir(parents=True, exist_ok=True)

        install_cmd = [mkcert, "-install"]
        install_result = subprocess.run(install_cmd, capture_output=True, text=True)
        if install_result.returncode != 0:
            msg = (install_result.stderr or install_result.stdout or "").strip()
            console.print("[red]✗[/red] Failed to install mkcert local CA.")
            if msg:
                console.print(f"  [dim]{msg}[/dim]")
            raise typer.Exit(1)

        generate_cmd = [
            mkcert,
            "-cert-file",
            str(cert_path),
            "-key-file",
            str(key_path),
            "localhost",
        ]
        generate_result = subprocess.run(generate_cmd, capture_output=True, text=True)
        if generate_result.returncode != 0:
            msg = (generate_result.stderr or generate_result.stdout or "").strip()
            console.print("[red]✗[/red] Failed to generate mkcert TLS files.")
            if msg:
                console.print(f"  [dim]{msg}[/dim]")
            raise typer.Exit(1)

        try:
            os.chmod(key_path, 0o600)
        except OSError:
            pass

        console.print("[green]✓[/green] mkcert certificate ready for TAPDB UI HTTPS")
        console.print(f"   Cert: [dim]{cert_path}[/dim]")
        console.print(f"   Key:  [dim]{key_path}[/dim]")
        if cert_file or key_file:
            console.print("   Set env overrides before start:")
            console.print(f"   [cyan]export TAPDB_UI_SSL_CERT={cert_path}[/cyan]")
            console.print(f"   [cyan]export TAPDB_UI_SSL_KEY={key_path}[/cyan]")
        console.print(
            "   Restart UI: [cyan]tapdb ui restart[/cyan]"
        )

    @ui_app.command("stop")
    def ui_stop():
        """Stop the TAPDB Admin UI server."""
        env_name = _active_env_name()
        pid_file, _, _ = _ui_runtime_paths(env_name)
        pid = _get_pid(pid_file)
        if not pid:
            console.print("[yellow]⚠[/yellow]  No UI server running")
            return

        try:
            os.kill(pid, signal.SIGTERM)
            for _ in range(10):
                time.sleep(0.5)
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break
            else:
                os.kill(pid, signal.SIGKILL)

            pid_file.unlink(missing_ok=True)
            console.print(f"[green]✓[/green]  UI server stopped (was PID {pid})")
        except ProcessLookupError:
            pid_file.unlink(missing_ok=True)
            console.print("[yellow]⚠[/yellow]  Server was not running")
        except PermissionError:
            console.print(f"[red]✗[/red]  Permission denied stopping PID {pid}")
            raise typer.Exit(1)

    @ui_app.command("status")
    def ui_status():
        """Check the status of the TAPDB Admin UI server."""
        env_name = _active_env_name()
        pid_file, log_file, _ = _ui_runtime_paths(env_name)
        pid = _get_pid(pid_file)
        if pid:
            console.print(
                f"[green]●[/green]  UI server is [green]running[/green] (PID {pid})"
            )
            console.print(f"   Logs: [dim]{log_file}[/dim]")
            console.print(f"   PID:  [dim]{pid_file}[/dim]")
        else:
            console.print("[dim]○[/dim]  UI server is [dim]not running[/dim]")

    @ui_app.command("logs")
    def ui_logs(
        follow: bool = typer.Option(
            True,
            "--follow/--no-follow",
            "-f/-F",
            help="Follow log output (default: true)",
        ),
        lines: int = typer.Option(50, "--lines", "-n", help="Number of lines to show"),
    ):
        """View TAPDB Admin UI server logs (tails by default, Ctrl+C to stop)."""
        env_name = _active_env_name()
        _, log_file, _ = _ui_runtime_paths(env_name)
        if not log_file.exists():
            console.print(
                "[yellow]⚠[/yellow]  No log file found. Start the server first."
            )
            return

        if follow:
            console.print(f"[dim]Following {log_file} (Ctrl+C to stop)[/dim]\n")
            try:
                subprocess.run(["tail", "-f", "-n", str(lines), str(log_file)])
            except KeyboardInterrupt:
                console.print("\n[dim]Stopped.[/dim]")
        else:
            try:
                with open(log_file, "r") as f:
                    all_lines = f.readlines()
                    for line in all_lines[-lines:]:
                        console.print(line.rstrip())
            except Exception as e:
                console.print(f"[red]✗[/red]  Error reading logs: {e}")

    @ui_app.command("restart")
    def ui_restart(
        port: Optional[int] = typer.Option(
            None, "--port", "-p", help="Port to run the server on"
        ),
        host: str = typer.Option(
            DEFAULT_UI_HOST, "--host", "-h", help="Host to bind to"
        ),
    ):
        """Restart the TAPDB Admin UI server."""
        ui_stop()
        time.sleep(1)
        ui_start(port=port, host=host, reload=False, background=True)

    def _resolve_bootstrap_env() -> DbEnvironment:
        raw = (os.environ.get("TAPDB_ENV") or "").strip().lower()
        if not raw:
            console.print("[red]✗[/red] TAPDB_ENV must be set for bootstrap")
            console.print("  Example: [cyan]export TAPDB_ENV=dev[/cyan]")
            raise typer.Exit(1)
        try:
            return DbEnvironment(raw)
        except ValueError:
            console.print(f"[red]✗[/red] Unsupported TAPDB_ENV '{raw}'")
            console.print("  Supported values: dev, test, prod")
            raise typer.Exit(1)

    def _maybe_start_ui_after_bootstrap(no_gui: bool) -> None:
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        if no_gui:
            console.print("  [dim]○[/dim] UI start skipped (--no-gui)")
            return
        env_name = _active_env_name()
        cfg = get_db_config_for_env(env_name)
        ui_port = int(str(cfg.get("ui_port") or DEFAULT_UI_PORT))
        try:
            ui_start(
                port=ui_port,
                host=DEFAULT_UI_HOST,
                reload=False,
                background=True,
            )
        except Exception as e:
            console.print(f"[yellow]⚠[/yellow] DB is ready, but UI start failed: {e}")
            console.print(
                "  Recover with: "
                f"[cyan]tapdb ui start --background --port {ui_port}[/cyan]"
            )

    @bootstrap_app.command("local")
    def bootstrap_local(
        no_gui: bool = typer.Option(
            False, "--no-gui", help="Skip starting TAPDB Admin UI"
        ),
        include_workflow: bool = typer.Option(
            False,
            "--include-workflow",
            "-w",
            help="Include workflow/action templates",
        ),
        insecure_dev_defaults: bool = typer.Option(
            False,
            "--insecure-dev-defaults",
            help="DEV ONLY: create default admin user (tapdb_admin/passw0rd)",
        ),
    ):
        """Bootstrap local TAPDB runtime, database, schema, and seed data."""
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        env = _resolve_bootstrap_env()
        cfg = get_db_config_for_env(env.value)
        if cfg.get("engine_type") == "aurora":
            console.print("[red]✗[/red] Active target is Aurora; use bootstrap aurora")
            raise typer.Exit(1)

        console.print(f"\n[bold cyan]━━━ Bootstrap Local ({env.value}) ━━━[/bold cyan]")

        if env in (DbEnvironment.dev, DbEnvironment.test):
            console.print("\n[bold]Step 1/6: Ensure local PostgreSQL runtime[/bold]")
            pg_init(env=env, force=False)
            pg_start_local(env=env, port=None)
        else:
            console.print("\n[bold]Step 1/6: Local runtime management[/bold]")
            console.print(
                "  [dim]○[/dim] Skipping local runtime management for prod target"
            )

        console.print("\n[bold]Step 2/6: Ensure database exists[/bold]")
        create_database(env=env, owner=None)

        console.print("\n[bold]Step 3/6: Apply schema[/bold]")
        apply_schema(env=env, reinitialize=False)

        console.print("\n[bold]Step 4/6: Run migrations[/bold]")
        run_migrations(env=env, dry_run=False)

        console.print("\n[bold]Step 5/6: Seed templates[/bold]")
        seed_templates(
            env=env,
            config_path=None,
            include_workflow=include_workflow,
            skip_existing=True,
            dry_run=False,
        )

        console.print("\n[bold]Step 6/6: Ensure admin user[/bold]")
        _create_default_admin(env=env, insecure_dev_defaults=insecure_dev_defaults)

        console.print("\n[bold]UI startup[/bold]")
        _maybe_start_ui_after_bootstrap(no_gui=no_gui)

        console.print("\n[bold green]✓ Local bootstrap complete[/bold green]")

    @bootstrap_app.command("aurora")
    def bootstrap_aurora(
        cluster: str = typer.Option(
            ...,
            "--cluster",
            help="Aurora cluster identifier to provision/reuse",
        ),
        region: str = typer.Option("us-west-2", "--region", "-r", help="AWS region"),
        no_gui: bool = typer.Option(
            False, "--no-gui", help="Skip starting TAPDB Admin UI"
        ),
        include_workflow: bool = typer.Option(
            False,
            "--include-workflow",
            "-w",
            help="Include workflow/action templates",
        ),
        insecure_dev_defaults: bool = typer.Option(
            False,
            "--insecure-dev-defaults",
            help="DEV ONLY: create default admin user (tapdb_admin/passw0rd)",
        ),
    ):
        """Bootstrap Aurora TAPDB stack (infra + DB + schema + seed + optional UI)."""
        from daylily_tapdb.aurora.config import AuroraConfig
        from daylily_tapdb.aurora.stack_manager import AuroraStackManager
        from daylily_tapdb.cli.aurora import (
            _ensure_boto3,
            _stack_name_for_env,
            _update_config_file,
        )

        _ensure_boto3()
        env = _resolve_bootstrap_env()
        stack_name = _stack_name_for_env(cluster)

        console.print(
            f"\n[bold cyan]━━━ Bootstrap Aurora ({env.value} -> {cluster})"
            " ━━━[/bold cyan]"
        )

        console.print("\n[bold]Step 1/7: Ensure Aurora cluster[/bold]")
        mgr = AuroraStackManager(region=region)
        try:
            info = mgr.get_stack_status(stack_name)
            console.print(f"  [green]✓[/green] Reusing existing stack {stack_name}")
        except RuntimeError:
            config = AuroraConfig(
                region=region,
                cluster_identifier=cluster,
                instance_class="db.r6g.large",
                engine_version="16.6",
                vpc_id="",
                iam_auth=True,
                publicly_accessible=False,
                deletion_protection=True,
                tags={
                    "lsmc-cost-center": "global",
                    "lsmc-project": f"tapdb-{region}",
                },
            )
            info = mgr.create_stack(config)
            console.print(f"  [green]✓[/green] Created Aurora stack {stack_name}")

        outputs = info.get("outputs", {})
        endpoint = outputs.get("ClusterEndpoint", "")
        port = str(outputs.get("ClusterPort", "5432"))
        if not endpoint:
            info = mgr.get_stack_status(stack_name)
            outputs = info.get("outputs", {})
            endpoint = outputs.get("ClusterEndpoint", "")
            port = str(outputs.get("ClusterPort", "5432"))
        if not endpoint:
            console.print(
                f"[red]✗[/red] Aurora endpoint not available for {stack_name}"
            )
            raise typer.Exit(1)

        console.print("\n[bold]Step 2/7: Update TAPDB target config[/bold]")
        _update_config_file(
            env.value,
            endpoint,
            port,
            region,
            cluster_identifier=cluster,
        )

        console.print("\n[bold]Step 3/7: Ensure database exists[/bold]")
        create_database(env=env, owner=None)

        console.print("\n[bold]Step 4/7: Apply schema[/bold]")
        apply_schema(env=env, reinitialize=False)

        console.print("\n[bold]Step 5/7: Run migrations[/bold]")
        run_migrations(env=env, dry_run=False)

        console.print("\n[bold]Step 6/7: Seed templates[/bold]")
        seed_templates(
            env=env,
            config_path=None,
            include_workflow=include_workflow,
            skip_existing=True,
            dry_run=False,
        )

        console.print("\n[bold]Step 7/7: Ensure admin user[/bold]")
        _create_default_admin(env=env, insecure_dev_defaults=insecure_dev_defaults)

        console.print("\n[bold]UI startup[/bold]")
        _maybe_start_ui_after_bootstrap(no_gui=no_gui)

        console.print("\n[bold green]✓ Aurora bootstrap complete[/bold green]")

    def _read_yaml_or_json_file(path: Path) -> dict:
        if not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8")
        try:
            import yaml  # type: ignore

            return yaml.safe_load(raw) or {}
        except ModuleNotFoundError:
            return json.loads(raw) if raw.strip() else {}

    def _write_yaml_or_json_file(path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            import yaml  # type: ignore

            path.write_text(
                yaml.dump(payload, default_flow_style=False, sort_keys=False),
                encoding="utf-8",
            )
        except ModuleNotFoundError:
            path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        os.chmod(path, 0o600)

    def _parse_env_port_pairs(values: list[str], *, flag: str) -> dict[str, int]:
        parsed: dict[str, int] = {}
        for raw in values:
            text = str(raw).strip()
            if not text:
                continue
            if "=" not in text:
                raise RuntimeError(f"{flag} must be provided as ENV=PORT (got {raw!r})")
            env_name, port_s = text.split("=", 1)
            env_name = env_name.strip().lower()
            port_s = port_s.strip()
            if not env_name:
                raise RuntimeError(f"{flag} missing environment in {raw!r}")
            if not port_s.isdigit():
                raise RuntimeError(f"{flag} has invalid port in {raw!r}")
            port = int(port_s)
            if port < 1 or port > 65535:
                raise RuntimeError(f"{flag} port out of range in {raw!r}")
            parsed[env_name] = port
        return parsed

    def _resolve_required_port(
        *,
        env_name: str,
        field: str,
        explicit_map: dict[str, int],
        existing_env_cfg: dict,
    ) -> int:
        if env_name in explicit_map:
            return explicit_map[env_name]

        existing_raw = str(existing_env_cfg.get(field) or "").strip()
        if existing_raw.isdigit():
            return int(existing_raw)

        if sys.stdin.isatty():
            while True:
                entered = typer.prompt(f"Enter {field} for {env_name}")
                if entered.strip().isdigit():
                    value = int(entered.strip())
                    if 1 <= value <= 65535:
                        return value
                console.print(f"[red]✗[/red] Invalid {field} for {env_name}")

        raise RuntimeError(
            f"Missing required {field} for env {env_name}. "
            f"Set via --db-port {env_name}=<port> or --ui-port {env_name}=<port>."
        )

    @config_root_app.command("init")
    def config_init(
        client_id: str = typer.Option(..., "--client-id", help="Client namespace key"),
        database_name: str = typer.Option(
            ..., "--database-name", help="Database namespace key"
        ),
        env: list[str] = typer.Option(
            ["dev"],
            "--env",
            help="Environment to configure (repeat for multiple)",
        ),
        db_port: list[str] = typer.Option(
            [],
            "--db-port",
            help="Per-env DB port mapping (ENV=PORT, repeatable)",
        ),
        ui_port: list[str] = typer.Option(
            [],
            "--ui-port",
            help="Per-env UI port mapping (ENV=PORT, repeatable)",
        ),
        force: bool = typer.Option(
            False, "--force", help="Overwrite existing config metadata if needed"
        ),
    ) -> None:
        """Initialize a namespaced TAPDB v2 config."""
        from daylily_tapdb.cli.db_config import get_config_path

        os.environ["TAPDB_CLIENT_ID"] = client_id
        os.environ["TAPDB_DATABASE_NAME"] = database_name
        ctx = _require_context()
        config_path = get_config_path()

        env_names = sorted({e.strip().lower() for e in env if str(e).strip()})
        if not env_names:
            raise RuntimeError("At least one --env must be provided")

        existing = _read_yaml_or_json_file(config_path)
        existing_meta = existing.get("meta") if isinstance(existing, dict) else None
        if isinstance(existing_meta, dict) and not force:
            if (
                str(existing_meta.get("client_id") or "") != client_id
                or str(existing_meta.get("database_name") or "") != database_name
            ):
                raise RuntimeError(
                    f"Config already exists for a different namespace: {config_path}. "
                    "Use --force to overwrite."
                )

        db_ports = _parse_env_port_pairs(db_port, flag="--db-port")
        ui_ports = _parse_env_port_pairs(ui_port, flag="--ui-port")

        root = existing if isinstance(existing, dict) else {}
        root["meta"] = {
            "config_version": 2,
            "client_id": client_id,
            "database_name": database_name,
        }
        envs = root.setdefault("environments", {})
        for env_name in env_names:
            prior = envs.get(env_name, {}) or {}
            resolved_db_port = _resolve_required_port(
                env_name=env_name,
                field="port",
                explicit_map=db_ports,
                existing_env_cfg=prior,
            )
            resolved_ui_port = _resolve_required_port(
                env_name=env_name,
                field="ui_port",
                explicit_map=ui_ports,
                existing_env_cfg=prior,
            )
            envs[env_name] = {
                **prior,
                "engine_type": str(prior.get("engine_type") or "local"),
                "host": "localhost",
                "port": str(resolved_db_port),
                "ui_port": str(resolved_ui_port),
                "user": str(prior.get("user") or os.environ.get("USER", "postgres")),
                "password": str(prior.get("password") or ""),
                "database": str(
                    prior.get("database") or f"tapdb_{database_name}_{env_name}"
                ),
                "cognito_user_pool_id": str(prior.get("cognito_user_pool_id") or ""),
                "audit_log_euid_prefix": str(prior.get("audit_log_euid_prefix") or ""),
                "support_email": str(prior.get("support_email") or ""),
            }

        _write_yaml_or_json_file(config_path, root)
        console.print("[green]✓[/green] TAPDB namespaced config initialized")
        console.print(f"  Namespace: [bold]{ctx.namespace_slug()}[/bold]")
        console.print(f"  Path:      [dim]{config_path}[/dim]")
        for env_name in env_names:
            env_cfg = root["environments"][env_name]
            console.print(
                "  "
                f"{env_name}: db_port={env_cfg['port']} ui_port={env_cfg['ui_port']}"
            )

    @config_root_app.command("migrate-legacy")
    def config_migrate_legacy(
        client_id: str = typer.Option(..., "--client-id", help="Client namespace key"),
        database_name: str = typer.Option(
            ..., "--database-name", help="Database namespace key"
        ),
        db_port: list[str] = typer.Option(
            [],
            "--db-port",
            help="Per-env DB port mapping (ENV=PORT, repeatable)",
        ),
        ui_port: list[str] = typer.Option(
            [],
            "--ui-port",
            help="Per-env UI port mapping (ENV=PORT, repeatable)",
        ),
        source: Optional[Path] = typer.Option(
            None,
            "--source",
            help="Optional legacy config source path",
        ),
        force: bool = typer.Option(
            False, "--force", help="Overwrite target if it already exists"
        ),
    ) -> None:
        """Migrate a legacy TAPDB config into namespaced v2 format."""
        from daylily_tapdb.cli.db_config import get_legacy_config_paths

        os.environ["TAPDB_CLIENT_ID"] = client_id
        os.environ["TAPDB_DATABASE_NAME"] = database_name
        ctx = _require_context()
        target_path = ctx.config_path()

        if target_path.exists() and not force:
            raise RuntimeError(
                f"Target config already exists: {target_path}. "
                "Use --force to overwrite."
            )

        if source:
            source_path = source.expanduser()
            if not source_path.exists():
                raise RuntimeError(f"Legacy source config not found: {source_path}")
        else:
            source_path = None
            for candidate in get_legacy_config_paths(database_name=database_name):
                if candidate.exists():
                    source_path = candidate
                    break
            if source_path is None:
                raise RuntimeError(
                    "No legacy TAPDB config found to migrate. "
                    "Use --source or run tapdb config init."
                )

        legacy = _read_yaml_or_json_file(source_path)
        legacy_envs = legacy.get("environments") if isinstance(legacy, dict) else None
        if not isinstance(legacy_envs, dict) or not legacy_envs:
            raise RuntimeError(
                f"Legacy config has no environments to migrate: {source_path}"
            )

        db_ports = _parse_env_port_pairs(db_port, flag="--db-port")
        ui_ports = _parse_env_port_pairs(ui_port, flag="--ui-port")

        migrated: dict = {
            "meta": {
                "config_version": 2,
                "client_id": client_id,
                "database_name": database_name,
            },
            "environments": {},
        }
        for env_name in sorted(legacy_envs.keys()):
            env_key = str(env_name).strip().lower()
            legacy_cfg = legacy_envs.get(env_name, {}) or {}
            engine_type = str(legacy_cfg.get("engine_type") or "local").strip().lower()

            resolved_db_port = _resolve_required_port(
                env_name=env_key,
                field="port",
                explicit_map=db_ports,
                existing_env_cfg=legacy_cfg,
            )
            resolved_ui_port = _resolve_required_port(
                env_name=env_key,
                field="ui_port",
                explicit_map=ui_ports,
                existing_env_cfg=legacy_cfg,
            )
            host = str(legacy_cfg.get("host") or "localhost")
            if engine_type != "aurora":
                host = "localhost"
            env_cfg = {
                **legacy_cfg,
                "engine_type": engine_type,
                "host": host,
                "port": str(resolved_db_port),
                "ui_port": str(resolved_ui_port),
            }
            # tapdb_user no longer exists; keep legacy configs readable but do not emit.
            env_cfg.pop("tapdb_user_euid_prefix", None)
            migrated["environments"][env_key] = env_cfg

        _write_yaml_or_json_file(target_path, migrated)
        console.print("[green]✓[/green] Legacy config migrated to namespaced v2 format")
        console.print(f"  Source: [dim]{source_path}[/dim]")
        console.print(f"  Target: [dim]{target_path}[/dim]")
        console.print(f"  Namespace: [bold]{ctx.namespace_slug()}[/bold]")
        for env_name in sorted(migrated["environments"].keys()):
            env_cfg = migrated["environments"][env_name]
            console.print(
                "  "
                f"{env_name}: db_port={env_cfg['port']} ui_port={env_cfg['ui_port']}"
            )

    @app.command("version")
    def version():
        """Show TAPDB version."""
        from daylily_tapdb import __version__

        console.print(f"daylily-tapdb [cyan]{__version__}[/cyan]")

    @app.command("info")
    def info(
        check_all_envs: bool = typer.Option(
            False,
            "--check-all-envs",
            help=(
                "Probe PostgreSQL status for dev/test/prod (may contact remote hosts). "
                "Default probes only TAPDB_ENV."
            ),
        ),
        as_json: bool = typer.Option(
            False, "--json", help="Emit machine-readable JSON (no tables)."
        ),
    ):
        """Show TAPDB configuration and status."""
        import json
        import shutil
        from datetime import datetime
        from urllib.parse import urlsplit, urlunsplit

        from daylily_tapdb import __version__
        from daylily_tapdb.cli.db_config import (
            get_config_path,
            get_config_paths,
            get_db_config_for_env,
        )

        def _sanitize_url(raw: str) -> str:
            if not raw:
                return ""
            try:
                parts = urlsplit(raw)
                if parts.username and parts.password:
                    host = parts.hostname or ""
                    netloc = f"{parts.username}@{host}"
                    if parts.port:
                        netloc = f"{netloc}:{parts.port}"
                    return urlunsplit(
                        (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
                    )
            except Exception:
                return raw
            return raw

        def _psql_query(cfg: dict[str, str], sql: str) -> tuple[bool, str]:
            psql = shutil.which("psql")
            if not psql:
                return False, "psql not found"

            env_vars = os.environ.copy()
            env_vars["PGCONNECT_TIMEOUT"] = "3"
            if cfg.get("password"):
                env_vars["PGPASSWORD"] = cfg["password"]

            cmd = [
                psql,
                "-X",
                "-q",
                "-t",
                "-A",
                "-w",  # never prompt for password
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                cfg["host"],
                "-p",
                cfg["port"],
                "-U",
                cfg["user"],
                "-d",
                cfg["database"],
                "-c",
                sql,
            ]
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    env=env_vars,
                    timeout=5,
                )
            except Exception as e:
                return False, str(e)

            if result.returncode != 0:
                return (
                    False,
                    (result.stderr or "").strip() or f"psql exit={result.returncode}",
                )
            return True, (result.stdout or "").strip()

        def _human_duration(seconds: int | None) -> str:
            if seconds is None:
                return "-"
            if seconds < 0:
                return "0s"
            days, rem = divmod(seconds, 86400)
            hours, rem = divmod(rem, 3600)
            minutes, secs = divmod(rem, 60)
            parts: list[str] = []
            if days:
                parts.append(f"{days}d")
            if hours:
                parts.append(f"{hours}h")
            if minutes:
                parts.append(f"{minutes}m")
            parts.append(f"{secs}s")
            return " ".join(parts)

        def _ui_process_times(pid: int) -> dict[str, object]:
            """Return UI process start time + uptime, best-effort.

            Uses `ps` (per requirement) for process start time.
            """
            result: dict[str, object] = {
                "pid": pid,
                "running": True,
                "start_time": None,
                "uptime_seconds": None,
                "uptime_human": None,
                "error": None,
            }
            try:
                ps = shutil.which("ps") or "ps"
                r = subprocess.run(
                    [ps, "-p", str(pid), "-o", "lstart="],
                    capture_output=True,
                    text=True,
                    timeout=2,
                )
                if r.returncode != 0:
                    result["error"] = (
                        r.stderr or ""
                    ).strip() or f"ps exit={r.returncode}"
                    return result
                raw = (r.stdout or "").strip()
                if not raw:
                    result["error"] = "ps returned empty start time"
                    return result

                # macOS/BSD ps lstart format: "Mon Jan  2 15:04:05 2006"
                start_dt = datetime.strptime(raw, "%a %b %d %H:%M:%S %Y")
                result["start_time"] = start_dt.isoformat(sep=" ")
                up_s = int((datetime.now() - start_dt).total_seconds())
                result["uptime_seconds"] = up_s
                result["uptime_human"] = _human_duration(up_s)
                return result
            except Exception as e:
                result["error"] = str(e)
                return result

        tapdb_env = os.environ.get("TAPDB_ENV", "dev").lower()
        test_dsn = os.environ.get("TAPDB_TEST_DSN", "")

        def _pg_probe(env_name: str, cfg: dict[str, str]) -> dict[str, object]:
            url = f"postgresql://{cfg['user']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
            should_check = check_all_envs or (env_name == tapdb_env)
            out: dict[str, object] = {
                "env": env_name,
                "url": url,
                "password_set": bool(cfg.get("password")),
                "checked": should_check,
                "status": None,
                "error": None,
                "uptime": None,
            }
            if not should_check:
                return out

            ok, msg = _psql_query(cfg, "select 1;")
            if not ok:
                out["status"] = "error"
                out["error"] = msg
                return out
            out["status"] = "ok"

            ok_u, msg_u = _psql_query(cfg, "select now() - pg_postmaster_start_time();")
            if ok_u:
                out["uptime"] = msg_u
            else:
                out["uptime"] = f"error: {msg_u}"
            return out

        # NOTE: This function is nested inside build_app();
        # keep indentation purely spaces to avoid TabError.
        ctx = _require_context(env_name=tapdb_env)
        config_paths = get_config_paths()
        effective_config_path = get_config_path()
        ui_pid_file, ui_log_file, _ = _ui_runtime_paths(tapdb_env)
        runtime_root = ctx.runtime_dir(tapdb_env)

        # Template JSON config dir (repo-local)
        template_config_dir: str | None = None
        template_config_error: str | None = None
        try:
            from daylily_tapdb.cli.db import _find_config_dir  # type: ignore

            template_config_dir = str(_find_config_dir())
        except Exception as e:
            template_config_error = str(e)

        ui_pid = _get_pid(ui_pid_file)
        ui_times: dict[str, object] | None = None
        if ui_pid:
            ui_times = _ui_process_times(ui_pid)

        pg_envs: dict[str, dict[str, object]] = {}
        for env_name in ["dev", "test", "prod"]:
            try:
                cfg = get_db_config_for_env(env_name)
                pg_envs[env_name] = _pg_probe(env_name, cfg)
            except Exception as e:
                checked = check_all_envs or (env_name == tapdb_env)
                pg_envs[env_name] = {
                    "env": env_name,
                    "url": "(unconfigured)",
                    "password_set": False,
                    "checked": checked,
                    "status": "error" if checked else None,
                    "error": str(e),
                    "uptime": None,
                }

        if as_json:
            payload: dict[str, object] = {
                "version": __version__,
                "python": sys.version.split()[0],
                "tapdb_env": tapdb_env,
                "client_id": ctx.client_id,
                "database_name": ctx.database_name,
                "check_all_envs": check_all_envs,
                "tapdb_test_dsn": _sanitize_url(test_dsn) if test_dsn else None,
                "paths": {
                    "ui_pid_file": str(ui_pid_file),
                    "ui_log_file": str(ui_log_file),
                    "config_search_order": [
                        {"path": str(p), "exists": p.exists()} for p in config_paths
                    ],
                    "effective_config": {
                        "path": str(effective_config_path),
                        "exists": effective_config_path.exists(),
                    },
                    "config_dir": str(effective_config_path.parent),
                    "runtime_root": str(runtime_root),
                    "db_log_dir": str(runtime_root / "logs"),
                    "template_config_dir": template_config_dir,
                    "template_config_error": template_config_error,
                },
                "ui": {
                    "running": bool(ui_pid),
                    "pid": ui_pid,
                    "process": ui_times,
                },
                "postgres": pg_envs,
            }
            sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
            return

        # --- General ---
        general = Table(title="TAPDB Info", show_header=True)
        general.add_column("Property", style="cyan")
        general.add_column("Value")
        general.add_row("Version", __version__)
        general.add_row("Python", sys.version.split()[0])
        general.add_row("TAPDB_ENV", tapdb_env)
        general.add_row("Client ID", ctx.client_id)
        general.add_row("Database Name", ctx.database_name)
        general.add_row("Namespace", ctx.namespace_slug())
        general.add_row("DB probes", "all envs" if check_all_envs else "TAPDB_ENV only")
        if test_dsn:
            general.add_row("TAPDB_TEST_DSN", f"[dim]{_sanitize_url(test_dsn)}[/dim]")

        general.add_row("UI Server", f"Running (PID {ui_pid})" if ui_pid else "Stopped")
        if ui_times and ui_times.get("start_time"):
            general.add_row("UI Start Time", str(ui_times.get("start_time")))
            general.add_row("UI Uptime", str(ui_times.get("uptime_human") or "-"))
        general.add_row("UI PID File", str(ui_pid_file))
        general.add_row("UI Log File", str(ui_log_file))
        console.print(general)

        # --- Config ---
        config_table = Table(title="Config", show_header=True)
        config_table.add_column("Property", style="cyan")
        config_table.add_column("Value")

        config_table.add_row(
            "Config search order",
            "\n".join(
                [f"{p} ({'exists' if p.exists() else 'missing'})" for p in config_paths]
            ),
        )

        exists_label = "exists" if effective_config_path.exists() else "missing"
        config_table.add_row(
            "Effective config",
            f"{effective_config_path} ({exists_label})",
        )
        config_table.add_row("Config dir", str(effective_config_path.parent))
        config_table.add_row("Runtime root", str(runtime_root))
        config_table.add_row("DB log dir", str(runtime_root / "logs"))

        if template_config_dir:
            config_table.add_row("Template config dir", template_config_dir)
        else:
            config_table.add_row(
                "Template config dir", f"(not found) {template_config_error}"
            )

        console.print(config_table)

        # --- Postgres ---
        pg_table = Table(title="PostgreSQL", show_header=True)
        pg_table.add_column("Env", style="cyan")
        pg_table.add_column("URL")
        pg_table.add_column("Password")
        pg_table.add_column("Status")
        pg_table.add_column("Uptime")

        for env_name in ["dev", "test", "prod"]:
            row = pg_envs[env_name]
            url = str(row.get("url") or "")
            pw = "set" if row.get("password_set") else "(not set)"
            checked = bool(row.get("checked"))
            if not checked:
                status = "-"
                uptime = "-"
            else:
                status = str(row.get("status") or "-")
                if status == "error" and row.get("error"):
                    status = f"error: {row.get('error')}"
                uptime = str(row.get("uptime") or "-")

            pg_table.add_row(env_name, f"[dim]{url}[/dim]", pw, status, uptime)

        console.print(pg_table)

    return app


# Expose a module-level Typer app for tests and embedding.
#
# Keep this guarded so imports don't explode in partially-provisioned
# environments (e.g., importing the package without console scripts).
try:
    if (
        importlib.util.find_spec("typer") is not None
        and importlib.util.find_spec("rich") is not None
    ):
        app = build_app()
    else:
        app = None
except Exception:
    app = None


def main():
    """Main CLI entry point."""
    cli_app = app or build_app()
    cli_app()


if __name__ == "__main__":
    raise SystemExit(main())
