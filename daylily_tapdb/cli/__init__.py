"""CLI entry point for daylily-tapdb."""

import importlib.util
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# PID file location
PID_FILE = Path.home() / ".tapdb" / "ui.pid"
LOG_FILE = Path.home() / ".tapdb" / "ui.log"
DEFAULT_UI_PORT = 8911
DEFAULT_UI_SCHEME = "https"
DEFAULT_UI_TLS_DIR = Path.home() / ".tapdb" / "certs"
DEFAULT_UI_TLS_CERT = DEFAULT_UI_TLS_DIR / "localhost.crt"
DEFAULT_UI_TLS_KEY = DEFAULT_UI_TLS_DIR / "localhost.key"


def _ensure_dir():
    """Ensure .tapdb directory exists."""
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)


def _resolve_tls_paths() -> tuple[Path, Path]:
    """Resolve TLS cert/key paths, allowing env overrides."""
    cert = Path(os.environ.get("TAPDB_UI_SSL_CERT", str(DEFAULT_UI_TLS_CERT)))
    key = Path(os.environ.get("TAPDB_UI_SSL_KEY", str(DEFAULT_UI_TLS_KEY)))
    return cert, key


def _ensure_tls_certificates(host: str) -> tuple[Path, Path]:
    """Ensure TLS cert/key exist for HTTPS UI startup."""
    cert_path, key_path = _resolve_tls_paths()
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

    san = f"DNS:localhost,IP:127.0.0.1,DNS:{host}"
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
            raise RuntimeError(f"Failed to generate TLS certificate with openssl: {msg}")

    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return cert_path, key_path


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
    from daylily_tapdb.cli.cognito import cognito_app
    from daylily_tapdb.cli.pg import pg_app, pg_init, pg_start_local
    from daylily_tapdb.cli.user import user_app

    app = typer.Typer(
        name="tapdb",
        help="TAPDB - Templated Abstract Polymorphic Database CLI",
        add_completion=True,
    )

    @app.callback()
    def _root_callback(
        database_name: Optional[str] = typer.Option(
            None,
            "--database-name",
            help=(
                "Use database-scoped config file names"
                " (e.g., ~/.config/tapdb/tapdb-config-<name>.yaml)."
            ),
        ),
    ):
        """Set global CLI context options."""
        if database_name:
            os.environ["TAPDB_DATABASE_NAME"] = database_name

    bootstrap_app = typer.Typer(help="One-command environment bootstrap")
    ui_app = typer.Typer(help="Admin UI management commands")
    app.add_typer(bootstrap_app, name="bootstrap")
    app.add_typer(ui_app, name="ui")
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
        port: int = typer.Option(
            DEFAULT_UI_PORT, "--port", "-p", help="Port to run the server on"
        ),
        host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host to bind to"),
        reload: bool = typer.Option(False, "--reload", "-r", help="Enable auto-reload"),
        background: bool = typer.Option(
            True, "--background/--foreground", "-b/-f", help="Run in background"
        ),
    ):
        """Start the TAPDB Admin UI server."""
        _ensure_dir()

        try:
            _require_admin_extras()
        except SystemExit:
            console.print("[red]✗[/red] Admin UI dependencies are not installed.")
            console.print(
                "  Install with: [cyan]pip install 'daylily-tapdb[admin]'[/cyan]"
            )
            raise typer.Exit(1)

        pid = _get_pid()
        if pid:
            console.print(f"[yellow]⚠[/yellow]  UI server already running (PID {pid})")
            console.print(f"   URL: [cyan]{DEFAULT_UI_SCHEME}://{host}:{port}[/cyan]")
            return

        try:
            admin_module = _find_admin_module()
        except ValueError as e:
            console.print(f"[red]✗[/red]  {e}")
            raise typer.Exit(1)

        try:
            cert_path, key_path = _ensure_tls_certificates(host)
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
            with open(LOG_FILE, "w") as log_f:
                proc = subprocess.Popen(
                    cmd,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )

            time.sleep(1)
            if proc.poll() is not None:
                console.print("[red]✗[/red]  Server failed to start. Check logs:")
                console.print(f"   [dim]{LOG_FILE}[/dim]")
                raise typer.Exit(1)

            PID_FILE.write_text(str(proc.pid))
            console.print(f"[green]✓[/green]  UI server started (PID {proc.pid})")
            console.print(f"   URL: [cyan]{DEFAULT_UI_SCHEME}://{host}:{port}[/cyan]")
            console.print(f"   Logs: [dim]{LOG_FILE}[/dim]")
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
        cert_file: Path = typer.Option(
            DEFAULT_UI_TLS_CERT,
            "--cert-file",
            help="Path to write mkcert-generated TLS certificate",
        ),
        key_file: Path = typer.Option(
            DEFAULT_UI_TLS_KEY,
            "--key-file",
            help="Path to write mkcert-generated TLS private key",
        ),
    ):
        """Install mkcert local CA and generate localhost TLS certs for the UI."""
        mkcert = shutil.which("mkcert")
        if not mkcert:
            console.print("[red]✗[/red] mkcert is required for trusted local HTTPS certs.")
            console.print("  Install mkcert first, then rerun [cyan]tapdb ui mkcert[/cyan].")
            raise typer.Exit(1)

        cert_path = cert_file.expanduser()
        key_path = key_file.expanduser()
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
            "127.0.0.1",
            "::1",
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
        if cert_path != DEFAULT_UI_TLS_CERT or key_path != DEFAULT_UI_TLS_KEY:
            console.print("   Set env overrides before start:")
            console.print(f"   [cyan]export TAPDB_UI_SSL_CERT={cert_path}[/cyan]")
            console.print(f"   [cyan]export TAPDB_UI_SSL_KEY={key_path}[/cyan]")
        console.print(
            f"   Restart UI: [cyan]tapdb ui restart --port {DEFAULT_UI_PORT}[/cyan]"
        )

    @ui_app.command("stop")
    def ui_stop():
        """Stop the TAPDB Admin UI server."""
        pid = _get_pid()
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
            console.print(
                f"[green]●[/green]  UI server is [green]running[/green] (PID {pid})"
            )
            console.print(f"   Logs: [dim]{LOG_FILE}[/dim]")
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
        if not LOG_FILE.exists():
            console.print(
                "[yellow]⚠[/yellow]  No log file found. Start the server first."
            )
            return

        if follow:
            console.print(f"[dim]Following {LOG_FILE} (Ctrl+C to stop)[/dim]\n")
            try:
                subprocess.run(["tail", "-f", "-n", str(lines), str(LOG_FILE)])
            except KeyboardInterrupt:
                console.print("\n[dim]Stopped.[/dim]")
        else:
            try:
                with open(LOG_FILE, "r") as f:
                    all_lines = f.readlines()
                    for line in all_lines[-lines:]:
                        console.print(line.rstrip())
            except Exception as e:
                console.print(f"[red]✗[/red]  Error reading logs: {e}")

    @ui_app.command("restart")
    def ui_restart(
        port: int = typer.Option(
            DEFAULT_UI_PORT, "--port", "-p", help="Port to run the server on"
        ),
        host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host to bind to"),
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
        if no_gui:
            console.print("  [dim]○[/dim] UI start skipped (--no-gui)")
            return
        try:
            ui_start(
                port=DEFAULT_UI_PORT,
                host="127.0.0.1",
                reload=False,
                background=True,
            )
        except Exception as e:
            console.print(f"[yellow]⚠[/yellow] DB is ready, but UI start failed: {e}")
            console.print(
                "  Recover with: "
                f"[cyan]tapdb ui start --background --port {DEFAULT_UI_PORT}[/cyan]"
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
        config_paths = get_config_paths()
        # get_config_path() returns None when no config file exists yet.
        # For diagnostics, we still want to print a deterministic "effective" path.
        effective_config = get_config_path()
        effective_config_path = effective_config or config_paths[0]

        # Template JSON config dir (repo-local)
        template_config_dir: str | None = None
        template_config_error: str | None = None
        try:
            from daylily_tapdb.cli.db import _find_config_dir  # type: ignore

            template_config_dir = str(_find_config_dir())
        except Exception as e:
            template_config_error = str(e)

        ui_pid = _get_pid()
        ui_times: dict[str, object] | None = None
        if ui_pid:
            ui_times = _ui_process_times(ui_pid)

        pg_envs: dict[str, dict[str, object]] = {}
        for env_name in ["dev", "test", "prod"]:
            cfg = get_db_config_for_env(env_name)
            pg_envs[env_name] = _pg_probe(env_name, cfg)

        if as_json:
            payload: dict[str, object] = {
                "version": __version__,
                "python": sys.version.split()[0],
                "tapdb_env": tapdb_env,
                "check_all_envs": check_all_envs,
                "tapdb_test_dsn": _sanitize_url(test_dsn) if test_dsn else None,
                "paths": {
                    "ui_pid_file": str(PID_FILE),
                    "ui_log_file": str(LOG_FILE),
                    "config_search_order": [
                        {"path": str(p), "exists": p.exists()} for p in config_paths
                    ],
                    "effective_config": {
                        "path": str(effective_config_path),
                        "exists": effective_config_path.exists(),
                    },
                    "config_dir": str(effective_config_path.parent),
                    "db_log_dir": str(effective_config_path.parent / "logs"),
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
        general.add_row("DB probes", "all envs" if check_all_envs else "TAPDB_ENV only")
        if test_dsn:
            general.add_row("TAPDB_TEST_DSN", f"[dim]{_sanitize_url(test_dsn)}[/dim]")

        general.add_row("UI Server", f"Running (PID {ui_pid})" if ui_pid else "Stopped")
        if ui_times and ui_times.get("start_time"):
            general.add_row("UI Start Time", str(ui_times.get("start_time")))
            general.add_row("UI Uptime", str(ui_times.get("uptime_human") or "-"))
        general.add_row("UI PID File", str(PID_FILE))
        general.add_row("UI Log File", str(LOG_FILE))
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
        config_table.add_row("DB log dir", str(effective_config_path.parent / "logs"))

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
