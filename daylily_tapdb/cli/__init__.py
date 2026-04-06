"""CLI entry point for daylily-tapdb."""

import importlib.util
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from cli_core_yo import ccyo_out

from daylily_tapdb.cli.context import (
    TapdbContext,
    active_context_overrides,
    active_env_name,
    clear_cli_context,
    resolve_context,
    set_cli_context,
)
from daylily_tapdb.cli.output import print_renderable
from daylily_tapdb.euid import (
    normalize_euid_client_code,
    resolve_client_scoped_core_prefix,
)

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
    "info",
}


def _active_env_name() -> str:
    return active_env_name("dev").strip().lower()


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


def _resolve_tls_paths(
    env_name: Optional[str] = None,
    *,
    cert_file: Optional[Path] = None,
    key_file: Optional[Path] = None,
) -> tuple[Path, Path]:
    """Resolve TLS cert/key paths from CLI overrides, config, or runtime defaults."""
    from daylily_tapdb.cli.db_config import get_admin_settings_for_env

    pid_file, _, certs_dir = _ui_runtime_paths(env_name)
    _ = pid_file  # path access validates context + env
    default_cert = certs_dir / "localhost.crt"
    default_key = certs_dir / "localhost.key"
    resolved_env = env_name or _active_env_name()
    admin_settings = get_admin_settings_for_env(resolved_env)
    if cert_file is not None:
        cert = cert_file.expanduser()
    elif admin_settings.get("tls_cert_path"):
        cert = Path(str(admin_settings["tls_cert_path"])).expanduser()
    else:
        cert = default_cert

    if key_file is not None:
        key = key_file.expanduser()
    elif admin_settings.get("tls_key_path"):
        key = Path(str(admin_settings["tls_key_path"])).expanduser()
    else:
        key = default_key
    return cert, key


def _ensure_tls_certificates(
    host: str,
    *,
    env_name: Optional[str] = None,
    cert_file: Optional[Path] = None,
    key_file: Optional[Path] = None,
) -> tuple[Path, Path]:
    """Ensure TLS cert/key exist for HTTPS UI startup."""
    cert_path, key_path = _resolve_tls_paths(
        env_name,
        cert_file=cert_file,
        key_file=key_file,
    )
    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    openssl = shutil.which("openssl")
    if not openssl:
        raise RuntimeError(
            "openssl is required to start the UI over HTTPS. "
            "Install openssl or set admin.ui.tls.cert_path/admin.ui.tls.key_path."
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
    from rich.table import Table

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
            help="Namespace metadata key for config init/migration flows.",
        ),
        database_name: Optional[str] = typer.Option(
            None,
            "--database-name",
            help="Namespace metadata key for config init/migration flows.",
        ),
        config_path: Optional[Path] = typer.Option(
            None,
            "--config",
            exists=False,
            file_okay=True,
            dir_okay=False,
            readable=False,
            help="Explicit TapDB config file path for this invocation.",
        ),
        env_name: Optional[str] = typer.Option(
            None,
            "--env",
            help="Explicit TapDB environment name for this invocation.",
        ),
    ):
        """Set global CLI context options."""
        if ctx.resilient_parsing:
            return
        prior_context = active_context_overrides()
        requested_help = any(
            arg in ("--help", "-h")
            for arg in [
                *list(getattr(ctx, "args", []) or []),
                *sys.argv[1:],
            ]
        )
        set_cli_context(
            client_id=(
                client_id if client_id is not None else prior_context.get("client_id")
            ),
            database_name=(
                database_name
                if database_name is not None
                else prior_context.get("database_name")
            ),
            env_name=env_name
            if env_name is not None
            else prior_context.get("env_name"),
            config_path=(
                config_path
                if config_path is not None
                else prior_context.get("config_path")
            ),
        )

        if requested_help:
            return

        invoked = (ctx.invoked_subcommand or "").strip().lower()
        strict = invoked in NAMESPACE_REQUIRED_TOPLEVEL
        if not strict:
            return

        current = active_context_overrides()
        if not current["config_path"] or not current["env_name"]:
            ccyo_out.error("Runtime TapDB commands require both --config and --env.")
            ccyo_out.print_text(
                "  Example: [cyan]tapdb --config "
                "~/.config/tapdb/atlas/app/tapdb-config.yaml --env dev info[/cyan]"
            )
            raise typer.Exit(1)

        try:
            _require_context()
        except RuntimeError as exc:
            ccyo_out.error(f"{exc}")
            ccyo_out.print_text(
                "  Example: [cyan]tapdb --config "
                "~/.config/tapdb/atlas/app/tapdb-config.yaml --env dev info[/cyan]"
            )
            raise typer.Exit(1)

    bootstrap_app = typer.Typer(help="One-command environment bootstrap")
    ui_app = typer.Typer(help="Admin UI server management commands")
    config_root_app = typer.Typer(help="TAPDB config namespace commands")
    app.add_typer(bootstrap_app, name="bootstrap")
    app.add_typer(ui_app, name="ui")
    app.add_typer(config_root_app, name="config")
    app.add_typer(db_app, name="db")
    app.add_typer(pg_app, name="pg")
    app.add_typer(user_app, name="users")
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
            ccyo_out.error("boto3 is required for Aurora commands.")
            ccyo_out.print_text(
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
        ssl_certfile: Optional[Path] = typer.Option(
            None,
            "--ssl-certfile",
            help="Explicit TLS certificate path for this invocation",
        ),
        ssl_keyfile: Optional[Path] = typer.Option(
            None,
            "--ssl-keyfile",
            help="Explicit TLS private key path for this invocation",
        ),
    ):
        """Start the TAPDB Admin UI server."""
        from daylily_tapdb.cli.db_config import get_config_path, get_db_config_for_env

        env_name = _active_env_name()
        pid_file, log_file, _ = _ui_runtime_paths(env_name)
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        cfg = get_db_config_for_env(env_name)
        configured_port = int(str(cfg.get("ui_port") or DEFAULT_UI_PORT))
        if port is None:
            port = configured_port
        elif port != configured_port:
            ccyo_out.error("UI port override is not allowed in strict mode.")
            ccyo_out.print_text(
                f"  Configured ui_port for env {env_name}: "
                f"[cyan]{configured_port}[/cyan]"
            )
            raise typer.Exit(1)

        try:
            _require_admin_extras()
        except SystemExit:
            ccyo_out.error("Admin UI dependencies are not installed.")
            ccyo_out.print_text(
                "  Install with: [cyan]pip install 'daylily-tapdb[admin]'[/cyan]"
            )
            raise typer.Exit(1)

        pid = _get_pid(pid_file)
        if pid:
            ccyo_out.warning(f"UI server already running (PID {pid})")
            ccyo_out.print_text(
                f"   URL: [cyan]{DEFAULT_UI_SCHEME}://{host}:{port}[/cyan]"
            )
            ccyo_out.print_text(f"   PID file: [dim]{pid_file}[/dim]")
            return

        if not _port_is_available(host, port):
            ccyo_out.error(f"{_port_conflict_details(port)}")
            ns = _require_context(env_name=env_name).namespace_slug()
            ccyo_out.print_text(f"  Namespace: [dim]{ns}[/dim]")
            ccyo_out.print_text(
                "  Update environments."
                f"{env_name}.ui_port in the namespaced config to a free port."
            )
            raise typer.Exit(1)

        try:
            cert_path, key_path = _ensure_tls_certificates(
                host,
                env_name=env_name,
                cert_file=ssl_certfile,
                key_file=ssl_keyfile,
            )
        except RuntimeError as e:
            ccyo_out.error(f"{e}")
            raise typer.Exit(1)

        effective_config_path = get_config_path()
        cmd = [
            sys.executable,
            "-m",
            "daylily_tapdb.cli.admin_server",
            "--config",
            str(effective_config_path),
            "--env",
            env_name,
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
                ccyo_out.error("Server failed to start. Check logs:")
                ccyo_out.print_text(f"   [dim]{log_file}[/dim]")
                raise typer.Exit(1)

            pid_file.write_text(str(proc.pid))
            ccyo_out.success(f"UI server started (PID {proc.pid})")
            ccyo_out.print_text(
                f"   URL: [cyan]{DEFAULT_UI_SCHEME}://{host}:{port}[/cyan]"
            )
            ccyo_out.print_text(f"   Logs: [dim]{log_file}[/dim]")
            ccyo_out.print_text(f"   PID:  [dim]{pid_file}[/dim]")
        else:
            ccyo_out.success(
                f"Starting UI server on {DEFAULT_UI_SCHEME}://{host}:{port}"
            )
            ccyo_out.print_text("   Press Ctrl+C to stop\n")
            try:
                subprocess.run(cmd)
            except KeyboardInterrupt:
                ccyo_out.warning("\nServer stopped")

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
            ccyo_out.error("mkcert is required for trusted local HTTPS certs.")
            ccyo_out.print_text(
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
            ccyo_out.error("Failed to install mkcert local CA.")
            if msg:
                ccyo_out.print_text(f"  [dim]{msg}[/dim]")
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
            ccyo_out.error("Failed to generate mkcert TLS files.")
            if msg:
                ccyo_out.print_text(f"  [dim]{msg}[/dim]")
            raise typer.Exit(1)

        try:
            os.chmod(key_path, 0o600)
        except OSError:
            pass

        ccyo_out.success("mkcert certificate ready for TAPDB UI HTTPS")
        ccyo_out.print_text(f"   Cert: [dim]{cert_path}[/dim]")
        ccyo_out.print_text(f"   Key:  [dim]{key_path}[/dim]")
        ccyo_out.print_text(
            "   Restart UI: [cyan]tapdb --config <path> --env <name> ui restart[/cyan]"
        )

    @ui_app.command("stop")
    def ui_stop():
        """Stop the TAPDB Admin UI server."""
        env_name = _active_env_name()
        pid_file, _, _ = _ui_runtime_paths(env_name)
        pid = _get_pid(pid_file)
        if not pid:
            ccyo_out.warning("No UI server running")
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
            ccyo_out.success(f"UI server stopped (was PID {pid})")
        except ProcessLookupError:
            pid_file.unlink(missing_ok=True)
            ccyo_out.warning("Server was not running")
        except PermissionError:
            ccyo_out.error(f"Permission denied stopping PID {pid}")
            raise typer.Exit(1)

    @ui_app.command("status")
    def ui_status():
        """Check the status of the TAPDB Admin UI server."""
        env_name = _active_env_name()
        pid_file, log_file, _ = _ui_runtime_paths(env_name)
        pid = _get_pid(pid_file)
        if pid:
            ccyo_out.success(f"UI server is running (PID {pid})")
            ccyo_out.print_text(f"   Logs: [dim]{log_file}[/dim]")
            ccyo_out.print_text(f"   PID:  [dim]{pid_file}[/dim]")
        else:
            ccyo_out.print_text("UI server is [dim]not running[/dim]")

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
            ccyo_out.warning("No log file found. Start the server first.")
            return

        if follow:
            ccyo_out.print_text(f"[dim]Following {log_file} (Ctrl+C to stop)[/dim]\n")
            try:
                subprocess.run(["tail", "-f", "-n", str(lines), str(log_file)])
            except KeyboardInterrupt:
                ccyo_out.print_text("\n[dim]Stopped.[/dim]")
        else:
            try:
                with open(log_file, "r") as f:
                    all_lines = f.readlines()
                    for line in all_lines[-lines:]:
                        ccyo_out.print_text(line.rstrip())
            except Exception as e:
                ccyo_out.error(f"Error reading logs: {e}")

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
        raw = str(active_context_overrides().get("env_name") or "").strip().lower()
        if not raw:
            ccyo_out.error("TapDB bootstrap requires an explicit --env value.")
            ccyo_out.print_text(
                "  Example: [cyan]tapdb --config <path> --env dev bootstrap local[/cyan]"
            )
            raise typer.Exit(1)
        try:
            return DbEnvironment(raw)
        except ValueError:
            ccyo_out.error(f"Unsupported TapDB env '{raw}'")
            ccyo_out.print_text("  Supported values: dev, test, prod")
            raise typer.Exit(1)

    def _maybe_start_ui_after_bootstrap(no_gui: bool) -> None:
        from daylily_tapdb.cli.db_config import get_db_config_for_env

        if no_gui:
            ccyo_out.print_text("  UI start skipped (--no-gui)")
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
            ccyo_out.error(f"DB is ready, but UI start failed: {e}")
            ccyo_out.print_text(
                "  Recover with: "
                f"[cyan]tapdb --config <path> --env {env_name} "
                f"ui start --background --port {ui_port}[/cyan]"
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
            help="Include optional non-core templates if present in config",
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
            ccyo_out.error("Active target is Aurora; use bootstrap aurora")
            raise typer.Exit(1)

        ccyo_out.print_text(
            f"\n[bold cyan]━━━ Bootstrap Local ({env.value}) ━━━[/bold cyan]"
        )

        if env in (DbEnvironment.dev, DbEnvironment.test):
            ccyo_out.print_text(
                "\n[bold]Step 1/6: Ensure local PostgreSQL runtime[/bold]"
            )
            pg_init(env=env, force=False)
            pg_start_local(env=env, port=None)
        else:
            ccyo_out.print_text("\n[bold]Step 1/6: Local runtime management[/bold]")
            ccyo_out.print_text("  Skipping local runtime management for prod target")

        ccyo_out.print_text("\n[bold]Step 2/6: Ensure database exists[/bold]")
        create_database(env=env, owner=None)

        ccyo_out.print_text("\n[bold]Step 3/6: Apply schema[/bold]")
        apply_schema(env=env, reinitialize=False)

        ccyo_out.print_text("\n[bold]Step 4/6: Run migrations[/bold]")
        run_migrations(env=env, dry_run=False)

        ccyo_out.print_text("\n[bold]Step 5/6: Seed templates[/bold]")
        seed_templates(
            env=env,
            config_path=None,
            include_workflow=include_workflow,
            skip_existing=True,
            dry_run=False,
        )

        ccyo_out.print_text("\n[bold]Step 6/6: Ensure admin user[/bold]")
        _create_default_admin(env=env, insecure_dev_defaults=insecure_dev_defaults)

        ccyo_out.print_text("\n[bold]UI startup[/bold]")
        _maybe_start_ui_after_bootstrap(no_gui=no_gui)

        ccyo_out.success("\n✓ Local bootstrap complete")

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
            help="Include optional non-core templates if present in config",
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

        ccyo_out.print_text(
            f"\n[bold cyan]━━━ Bootstrap Aurora ({env.value} -> {cluster})"
            " ━━━[/bold cyan]"
        )

        ccyo_out.print_text("\n[bold]Step 1/7: Ensure Aurora cluster[/bold]")
        mgr = AuroraStackManager(region=region)
        try:
            info = mgr.get_stack_status(stack_name)
            ccyo_out.success(f"  Reusing existing stack {stack_name}")
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
            ccyo_out.success(f"  Created Aurora stack {stack_name}")

        outputs = info.get("outputs", {})
        endpoint = outputs.get("ClusterEndpoint", "")
        port = str(outputs.get("ClusterPort", "5432"))
        if not endpoint:
            info = mgr.get_stack_status(stack_name)
            outputs = info.get("outputs", {})
            endpoint = outputs.get("ClusterEndpoint", "")
            port = str(outputs.get("ClusterPort", "5432"))
        if not endpoint:
            ccyo_out.error(f"Aurora endpoint not available for {stack_name}")
            raise typer.Exit(1)

        ccyo_out.print_text("\n[bold]Step 2/7: Update TAPDB target config[/bold]")
        _update_config_file(
            env.value,
            endpoint,
            port,
            region,
            cluster_identifier=cluster,
        )

        ccyo_out.print_text("\n[bold]Step 3/7: Ensure database exists[/bold]")
        create_database(env=env, owner=None)

        ccyo_out.print_text("\n[bold]Step 4/7: Apply schema[/bold]")
        apply_schema(env=env, reinitialize=False)

        ccyo_out.print_text("\n[bold]Step 5/7: Run migrations[/bold]")
        run_migrations(env=env, dry_run=False)

        ccyo_out.print_text("\n[bold]Step 6/7: Seed templates[/bold]")
        seed_templates(
            env=env,
            config_path=None,
            include_workflow=include_workflow,
            skip_existing=True,
            dry_run=False,
        )

        ccyo_out.print_text("\n[bold]Step 7/7: Ensure admin user[/bold]")
        _create_default_admin(env=env, insecure_dev_defaults=insecure_dev_defaults)

        ccyo_out.print_text("\n[bold]UI startup[/bold]")
        _maybe_start_ui_after_bootstrap(no_gui=no_gui)

        ccyo_out.success("\n✓ Aurora bootstrap complete")

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
                ccyo_out.error(f"Invalid {field} for {env_name}")

        raise RuntimeError(
            f"Missing required {field} for env {env_name}. "
            f"Set via --db-port {env_name}=<port> or --ui-port {env_name}=<port>."
        )

    def _require_explicit_config_flag() -> Path:
        config_path = active_context_overrides().get("config_path")
        if not config_path:
            raise RuntimeError(
                "TapDB config commands require --config. "
                "Example: tapdb --config ~/.config/tapdb/atlas/app/tapdb-config.yaml "
                "config init --client-id atlas --database-name app --euid-client-code A"
            )
        return Path(config_path)

    def _default_admin_config() -> dict:
        return {
            "footer": {
                "repo_url": "https://github.com/Daylily-Informatics/daylily-tapdb",
            },
            "session": {
                "secret": secrets.token_hex(32),
            },
            "auth": {
                "mode": "tapdb",
                "disabled_user": {
                    "email": "tapdb-admin@localhost",
                    "role": "admin",
                },
                "shared_host": {
                    "session_secret": "",
                    "session_cookie": "session",
                    "session_max_age_seconds": 1209600,
                },
            },
            "cors": {
                "allowed_origins": [],
            },
            "ui": {
                "tls": {
                    "cert_path": "",
                    "key_path": "",
                },
            },
            "metrics": {
                "enabled": True,
                "queue_max": 20000,
                "flush_seconds": 1.0,
            },
            "db_pool_size": 5,
            "db_max_overflow": 10,
            "db_pool_timeout": 30,
            "db_pool_recycle": 1800,
        }

    @config_root_app.command("init")
    def config_init(
        client_id: str = typer.Option(..., "--client-id", help="Client namespace key"),
        database_name: str = typer.Option(
            ..., "--database-name", help="Database namespace key"
        ),
        euid_client_code: str = typer.Option(
            ...,
            "--euid-client-code",
            help="Single-letter client code used to derive the namespace TapDB core prefix",
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
        """Initialize a namespaced TAPDB v3 config."""
        current = active_context_overrides()
        set_cli_context(
            client_id=client_id,
            database_name=database_name,
            config_path=current["config_path"],
        )
        config_path = _require_explicit_config_flag()

        env_names = sorted({e.strip().lower() for e in env if str(e).strip()})
        if not env_names:
            raise RuntimeError("At least one --env must be provided")
        normalized_client_code = normalize_euid_client_code(euid_client_code)
        core_euid_prefix = resolve_client_scoped_core_prefix(normalized_client_code)

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
            "config_version": 3,
            "client_id": client_id,
            "database_name": database_name,
            "euid_client_code": normalized_client_code,
        }
        admin_root = root.get("admin")
        if not isinstance(admin_root, dict) or force:
            root["admin"] = _default_admin_config()
        else:
            merged_admin = _default_admin_config()
            for key, value in admin_root.items():
                if key in {
                    "footer",
                    "session",
                    "auth",
                    "cors",
                    "ui",
                    "metrics",
                } and isinstance(value, dict):
                    merged_admin[key].update(value)
                else:
                    merged_admin[key] = value
            root["admin"] = merged_admin

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
                "audit_log_euid_prefix": core_euid_prefix,
                "support_email": str(prior.get("support_email") or ""),
            }

        _write_yaml_or_json_file(config_path, root)
        ccyo_out.success("TAPDB namespaced config initialized")
        ccyo_out.print_text(f"  Namespace: [bold]{client_id}/{database_name}[/bold]")
        ccyo_out.print_text(f"  Path:      [dim]{config_path}[/dim]")
        for env_name in env_names:
            env_cfg = root["environments"][env_name]
            ccyo_out.print_text(
                f"  {env_name}: db_port={env_cfg['port']} ui_port={env_cfg['ui_port']}"
            )

    @config_root_app.command("update")
    def config_update(
        env: str = typer.Option(..., "--env", help="Environment to update"),
        engine_type: Optional[str] = typer.Option(
            None, "--engine-type", help="Database engine type for this environment"
        ),
        host: Optional[str] = typer.Option(None, "--host", help="Database host"),
        port: Optional[int] = typer.Option(None, "--port", help="Database port"),
        ui_port: Optional[int] = typer.Option(None, "--ui-port", help="TapDB UI port"),
        user: Optional[str] = typer.Option(None, "--user", help="Database user"),
        password: Optional[str] = typer.Option(
            None, "--password", help="Database password"
        ),
        database: Optional[str] = typer.Option(
            None, "--database", help="Database name"
        ),
        cognito_user_pool_id: Optional[str] = typer.Option(
            None, "--cognito-user-pool-id", help="Bound Cognito user pool ID"
        ),
        cognito_app_client_id: Optional[str] = typer.Option(
            None, "--cognito-app-client-id", help="Bound Cognito app client ID"
        ),
        cognito_app_client_secret: Optional[str] = typer.Option(
            None, "--cognito-app-client-secret", help="Bound Cognito app client secret"
        ),
        cognito_client_name: Optional[str] = typer.Option(
            None, "--cognito-client-name", help="Bound Cognito app client name"
        ),
        cognito_region: Optional[str] = typer.Option(
            None, "--cognito-region", help="Bound Cognito region"
        ),
        cognito_domain: Optional[str] = typer.Option(
            None, "--cognito-domain", help="Bound Cognito hosted UI domain"
        ),
        cognito_callback_url: Optional[str] = typer.Option(
            None, "--cognito-callback-url", help="Bound Cognito callback URL"
        ),
        cognito_logout_url: Optional[str] = typer.Option(
            None, "--cognito-logout-url", help="Bound Cognito logout URL"
        ),
        audit_log_euid_prefix: Optional[str] = typer.Option(
            None, "--audit-log-euid-prefix", help="Audit-log EUID prefix"
        ),
        support_email: Optional[str] = typer.Option(
            None, "--support-email", help="Support email address"
        ),
        admin_repo_url: Optional[str] = typer.Option(
            None, "--admin-repo-url", help="Admin footer repository URL"
        ),
        admin_session_secret: Optional[str] = typer.Option(
            None, "--admin-session-secret", help="Admin session signing secret"
        ),
        admin_auth_mode: Optional[str] = typer.Option(
            None,
            "--admin-auth-mode",
            help="Admin auth mode: tapdb, shared_host, or disabled",
        ),
        admin_disabled_user_email: Optional[str] = typer.Option(
            None,
            "--admin-disabled-user-email",
            help="Synthetic disabled-auth admin email",
        ),
        admin_disabled_user_role: Optional[str] = typer.Option(
            None,
            "--admin-disabled-user-role",
            help="Synthetic disabled-auth admin role",
        ),
        admin_shared_host_session_secret: Optional[str] = typer.Option(
            None,
            "--admin-shared-host-session-secret",
            help="Shared-host session signing secret",
        ),
        admin_shared_host_session_cookie: Optional[str] = typer.Option(
            None,
            "--admin-shared-host-session-cookie",
            help="Shared-host session cookie name",
        ),
        admin_shared_host_session_max_age_seconds: Optional[int] = typer.Option(
            None,
            "--admin-shared-host-session-max-age-seconds",
            help="Shared-host session max age in seconds",
        ),
        admin_allowed_origin: list[str] = typer.Option(
            [],
            "--admin-allowed-origin",
            help="Allowed admin CORS origin (repeatable)",
        ),
        admin_tls_cert_path: Optional[str] = typer.Option(
            None, "--admin-tls-cert-path", help="Configured admin TLS certificate path"
        ),
        admin_tls_key_path: Optional[str] = typer.Option(
            None, "--admin-tls-key-path", help="Configured admin TLS private key path"
        ),
        admin_metrics_enabled: Optional[bool] = typer.Option(
            None,
            "--admin-metrics-enabled/--no-admin-metrics-enabled",
            help="Enable admin DB metrics",
        ),
        admin_metrics_queue_max: Optional[int] = typer.Option(
            None, "--admin-metrics-queue-max", help="Admin DB metrics queue size"
        ),
        admin_metrics_flush_seconds: Optional[float] = typer.Option(
            None,
            "--admin-metrics-flush-seconds",
            help="Admin DB metrics flush interval",
        ),
        clear: list[str] = typer.Option(
            [],
            "--clear",
            help="Environment field to clear (repeatable)",
        ),
    ) -> None:
        """Update fields inside a namespaced TAPDB v3 config."""
        from daylily_tapdb.cli.db_config import get_config_path

        ctx = _require_context()
        env_name = str(env or "").strip().lower()
        if not env_name:
            raise RuntimeError("--env is required")

        allowed_fields = {
            "engine_type",
            "host",
            "port",
            "ui_port",
            "user",
            "password",
            "database",
            "cognito_user_pool_id",
            "cognito_app_client_id",
            "cognito_app_client_secret",
            "cognito_client_name",
            "cognito_region",
            "cognito_domain",
            "cognito_callback_url",
            "cognito_logout_url",
            "audit_log_euid_prefix",
            "support_email",
        }
        clear_fields = {str(item).strip() for item in clear if str(item).strip()}
        invalid_fields = sorted(clear_fields - allowed_fields)
        if invalid_fields:
            raise RuntimeError(
                "Unknown field(s) for --clear: " + ", ".join(invalid_fields)
            )

        config_path = get_config_path()
        root = _read_yaml_or_json_file(config_path)
        if not root:
            raise RuntimeError(
                f"No TAPDB config found at {config_path}. "
                "Run: tapdb config init --client-id <id> --database-name <name>"
            )

        meta = root.get("meta") if isinstance(root, dict) else None
        if not isinstance(meta, dict):
            raise RuntimeError(
                "Config metadata is required. "
                f"Run: tapdb config init --client-id {ctx.client_id} --database-name {ctx.database_name}"
            )

        envs = root.setdefault("environments", {})
        if not isinstance(envs, dict):
            envs = {}
            root["environments"] = envs
        env_cfg = envs.setdefault(env_name, {})
        if not isinstance(env_cfg, dict):
            env_cfg = {}
            envs[env_name] = env_cfg

        admin_root = root.setdefault("admin", _default_admin_config())
        if not isinstance(admin_root, dict):
            raise RuntimeError("Config admin section must be a mapping.")
        footer = admin_root.setdefault("footer", {})
        session = admin_root.setdefault("session", {})
        auth = admin_root.setdefault("auth", {})
        disabled_user = auth.setdefault("disabled_user", {})
        shared_host = auth.setdefault("shared_host", {})
        cors = admin_root.setdefault("cors", {})
        ui_root = admin_root.setdefault("ui", {})
        tls = ui_root.setdefault("tls", {})
        metrics = admin_root.setdefault("metrics", {})

        updates: dict[str, str] = {}
        if engine_type is not None:
            updates["engine_type"] = str(engine_type).strip().lower()
        if host is not None:
            updates["host"] = str(host).strip()
        if port is not None:
            updates["port"] = str(port)
        if ui_port is not None:
            updates["ui_port"] = str(ui_port)
        if user is not None:
            updates["user"] = str(user).strip()
        if password is not None:
            updates["password"] = str(password)
        if database is not None:
            updates["database"] = str(database).strip()
        if cognito_user_pool_id is not None:
            updates["cognito_user_pool_id"] = str(cognito_user_pool_id).strip()
        if cognito_app_client_id is not None:
            updates["cognito_app_client_id"] = str(cognito_app_client_id).strip()
        if cognito_app_client_secret is not None:
            updates["cognito_app_client_secret"] = str(cognito_app_client_secret)
        if cognito_client_name is not None:
            updates["cognito_client_name"] = str(cognito_client_name).strip()
        if cognito_region is not None:
            updates["cognito_region"] = str(cognito_region).strip()
        if cognito_domain is not None:
            updates["cognito_domain"] = str(cognito_domain).strip()
        if cognito_callback_url is not None:
            updates["cognito_callback_url"] = str(cognito_callback_url).strip()
        if cognito_logout_url is not None:
            updates["cognito_logout_url"] = str(cognito_logout_url).strip()
        if audit_log_euid_prefix is not None:
            updates["audit_log_euid_prefix"] = str(audit_log_euid_prefix).strip()
        if support_email is not None:
            updates["support_email"] = str(support_email).strip()

        admin_changed = False
        if admin_repo_url is not None:
            footer["repo_url"] = str(admin_repo_url).strip()
            admin_changed = True
        if admin_session_secret is not None:
            session["secret"] = str(admin_session_secret)
            admin_changed = True
        if admin_auth_mode is not None:
            auth["mode"] = str(admin_auth_mode).strip().lower()
            admin_changed = True
        if admin_disabled_user_email is not None:
            disabled_user["email"] = str(admin_disabled_user_email).strip().lower()
            admin_changed = True
        if admin_disabled_user_role is not None:
            disabled_user["role"] = str(admin_disabled_user_role).strip().lower()
            admin_changed = True
        if admin_shared_host_session_secret is not None:
            shared_host["session_secret"] = str(admin_shared_host_session_secret)
            admin_changed = True
        if admin_shared_host_session_cookie is not None:
            shared_host["session_cookie"] = str(
                admin_shared_host_session_cookie
            ).strip()
            admin_changed = True
        if admin_shared_host_session_max_age_seconds is not None:
            shared_host["session_max_age_seconds"] = int(
                admin_shared_host_session_max_age_seconds
            )
            admin_changed = True
        if admin_allowed_origin:
            cors["allowed_origins"] = [
                str(item).strip() for item in admin_allowed_origin if str(item).strip()
            ]
            admin_changed = True
        if admin_tls_cert_path is not None:
            tls["cert_path"] = str(admin_tls_cert_path).strip()
            admin_changed = True
        if admin_tls_key_path is not None:
            tls["key_path"] = str(admin_tls_key_path).strip()
            admin_changed = True
        if admin_metrics_enabled is not None:
            metrics["enabled"] = bool(admin_metrics_enabled)
            admin_changed = True
        if admin_metrics_queue_max is not None:
            metrics["queue_max"] = int(admin_metrics_queue_max)
            admin_changed = True
        if admin_metrics_flush_seconds is not None:
            metrics["flush_seconds"] = float(admin_metrics_flush_seconds)
            admin_changed = True

        if not updates and not clear_fields and not admin_changed:
            raise RuntimeError("No config changes requested.")

        for field_name in clear_fields:
            env_cfg[field_name] = ""
        env_cfg.update(updates)

        _write_yaml_or_json_file(config_path, root)
        ccyo_out.success("TAPDB namespaced config updated")
        ccyo_out.print_text(f"  Namespace: [bold]{ctx.namespace_slug()}[/bold]")
        ccyo_out.print_text(f"  Path:      [dim]{config_path}[/dim]")
        ccyo_out.print_text(f"  Env:       [bold]{env_name}[/bold]")
        for field_name in sorted(clear_fields):
            ccyo_out.print_text(f"  cleared:   {field_name}")
        for field_name in sorted(updates.keys()):
            ccyo_out.print_text(f"  set:       {field_name}={env_cfg[field_name]}")

    @app.command("version")
    def version():
        """Show TAPDB version."""
        from daylily_tapdb import __version__

        ccyo_out.print_text(f"daylily-tapdb [cyan]{__version__}[/cyan]")

    @app.command("info")
    def info(
        check_all_envs: bool = typer.Option(
            False,
            "--check-all-envs",
            help=(
                "Probe PostgreSQL status for dev/test/prod (may contact remote hosts). "
                "Default probes only the active TapDB env."
            ),
        ),
        as_json: bool = typer.Option(
            False, "--json", help="Emit machine-readable JSON (no tables)."
        ),
    ):
        """Show TAPDB configuration and status."""
        import json
        import shutil
        from datetime import UTC, datetime
        from urllib.parse import urlsplit, urlunsplit

        from daylily_tapdb import __version__
        from daylily_tapdb.cli.db_config import (
            get_config_path,
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
                start_dt = datetime.strptime(raw, "%a %b %d %H:%M:%S %Y").replace(
                    tzinfo=UTC
                )
                result["start_time"] = start_dt.isoformat(sep=" ")
                up_s = int((datetime.now(UTC) - start_dt).total_seconds())
                result["uptime_seconds"] = up_s
                result["uptime_human"] = _human_duration(up_s)
                return result
            except Exception as e:
                result["error"] = str(e)
                return result

        tapdb_env = _active_env_name()

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
                "paths": {
                    "ui_pid_file": str(ui_pid_file),
                    "ui_log_file": str(ui_log_file),
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
        general.add_row("TapDB Env", tapdb_env)
        general.add_row("Client ID", ctx.client_id)
        general.add_row("Database Name", ctx.database_name)
        general.add_row("Namespace", ctx.namespace_slug())
        general.add_row(
            "DB probes", "all envs" if check_all_envs else "active env only"
        )

        general.add_row("UI Server", f"Running (PID {ui_pid})" if ui_pid else "Stopped")
        if ui_times and ui_times.get("start_time"):
            general.add_row("UI Start Time", str(ui_times.get("start_time")))
            general.add_row("UI Uptime", str(ui_times.get("uptime_human") or "-"))
        general.add_row("UI PID File", str(ui_pid_file))
        general.add_row("UI Log File", str(ui_log_file))
        print_renderable(general)

        # --- Config ---
        config_table = Table(title="Config", show_header=True)
        config_table.add_column("Property", style="cyan")
        config_table.add_column("Value")

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

        print_renderable(config_table)

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

        print_renderable(pg_table)

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


def register(registry, spec) -> None:
    cli_app = app or build_app()
    for cmd in getattr(cli_app, "registered_commands", []):
        cmd_name = cmd.name or cmd.callback.__name__.replace("_", "-")
        if cmd_name in ("version", "info", "config"):
            continue
        registry.add_command(
            group_path=None,
            name=cmd_name,
            callback=cmd.callback,
            help_text=cmd.help or "",
        )
    for group in getattr(cli_app, "registered_groups", []):
        group_name = group.name or group.typer_instance.info.name
        if group_name == "config":
            group_name = "db-config"
        registry.add_typer_app(
            group_path=None,
            typer_app=group.typer_instance,
            name=group_name,
            help_text=group.help or group.typer_instance.info.help or "",
        )


def main():
    """Main CLI entry point."""
    import sys

    from cli_core_yo.app import run

    from daylily_tapdb.cli.spec import spec

    argv = sys.argv[1:]
    clear_cli_context()
    argv = _prime_cli_context_from_argv(argv)
    sys.exit(run(spec, argv))


def _consume_root_option(
    argv: list[str],
    index: int,
    option: str,
) -> tuple[bool, Optional[str], int]:
    arg = argv[index]
    if arg == option:
        if index + 1 >= len(argv):
            return True, None, 1
        return True, argv[index + 1], 2
    prefix = f"{option}="
    if arg.startswith(prefix):
        return True, arg[len(prefix) :], 1
    return False, None, 0


def _prime_cli_context_from_argv(argv: list[str]) -> list[str]:
    """Capture TAPDB root context flags before cli-core-yo parses argv."""

    client_id: Optional[str] = None
    database_name: Optional[str] = None
    env_name: Optional[str] = None
    config_path: Optional[str] = None
    cleaned: list[str] = []
    saw_command = False
    index = 0

    while index < len(argv):
        arg = argv[index]
        if saw_command:
            cleaned.append(arg)
            index += 1
            continue

        if arg == "--":
            saw_command = True
            cleaned.append(arg)
            index += 1
            continue

        if not arg.startswith("-"):
            saw_command = True
            cleaned.append(arg)
            index += 1
            continue

        matched, value, consumed = _consume_root_option(argv, index, "--config")
        if matched:
            config_path = value
            cleaned.extend(argv[index : index + consumed])
            index += consumed
            continue

        matched, value, consumed = _consume_root_option(argv, index, "--env")
        if matched:
            env_name = value
            index += consumed
            continue

        matched, value, consumed = _consume_root_option(argv, index, "--client-id")
        if matched:
            client_id = value
            index += consumed
            continue

        matched, value, consumed = _consume_root_option(argv, index, "--database-name")
        if matched:
            database_name = value
            index += consumed
            continue

        cleaned.append(arg)
        index += 1

    set_cli_context(
        client_id=client_id,
        database_name=database_name,
        env_name=env_name,
        config_path=config_path,
    )
    return cleaned


if __name__ == "__main__":
    raise SystemExit(main())
