"""Database management commands for TAPDB CLI."""

import importlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import typer
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from daylily_tapdb import TAPDBConnection
from daylily_tapdb.cli.db_config import get_config_path, get_db_config_for_env
from daylily_tapdb.validation.instantiation_layouts import (
    format_validation_error,
    validate_instantiation_layouts,
)

console = Console()

_MERIDIAN_PREFIX_RE = re.compile(r"^[A-HJ-KMNP-TV-Z]{2,3}$")
_RESERVED_PREFIXES = {"GT", "GX", "GN", "WX", "WSX", "XX", "AY"}


def _normalize_instance_prefix(prefix: str) -> str:
    """Normalize/validate an instance_prefix.

    Phase 1 rule: prefixes drive per-prefix sequences;
    missing/invalid prefixes should fail early.
    """
    if prefix is None:
        raise ValueError("instance_prefix cannot be None")
    normalized = str(prefix).strip().upper()
    if not normalized:
        raise ValueError("instance_prefix cannot be empty")
    if not normalized.isalpha():
        raise ValueError(f"instance_prefix must be letters only (A-Z), got: {prefix!r}")
    return normalized


def _normalize_meridian_prefix(prefix: str, field_name: str) -> str:
    """Normalize/validate Meridian-safe EUID prefixes."""
    if prefix is None:
        raise ValueError(f"{field_name} cannot be None")
    normalized = str(prefix).strip().upper()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty")
    if not _MERIDIAN_PREFIX_RE.match(normalized):
        raise ValueError(
            f"{field_name} must match ^[A-HJ-KMNP-TV-Z]{{2,3}}$, got: {prefix!r}"
        )
    if normalized in _RESERVED_PREFIXES:
        raise ValueError(
            f"{field_name} cannot reuse reserved TAPDB prefix {normalized!r}"
        )
    return normalized


def _required_identity_prefixes(env: "Environment") -> str:
    """Return validated audit_log_prefix for env config."""
    cfg = _get_db_config(env)
    audit_prefix = _normalize_meridian_prefix(
        cfg.get("audit_log_euid_prefix", ""),
        "audit_log_euid_prefix",
    )
    return audit_prefix


def _sync_identity_prefix_config(env: "Environment") -> None:
    """Persist required identity prefix config and ensure backing sequences."""
    audit_prefix = _required_identity_prefixes(env)
    audit_seq = f"{audit_prefix.lower()}_audit_seq"
    sql = f"""
    INSERT INTO tapdb_identity_prefix_config(entity, prefix)
    VALUES ('audit_log', '{audit_prefix}')
    ON CONFLICT (entity) DO UPDATE
      SET prefix = EXCLUDED.prefix, updated_dt = NOW();

    CREATE SEQUENCE IF NOT EXISTS "{audit_seq}";
    """
    success, output = _run_psql(env, sql=sql)
    if not success:
        raise RuntimeError(
            "Failed to sync identity prefix config for audit_log: " f"{output[:200]}"
        )


def _ensure_instance_prefix_sequence(env: "Environment", prefix: str) -> None:
    """Create + initialize the per-prefix instance sequence.

    Sequence init algorithm (REFACTOR_TAPDB.md Phase 1):
    next nextval() should yield max(existing numeric suffix) + 1.
    """
    prefix = _normalize_instance_prefix(prefix)

    # Defense-in-depth: reject non-alpha prefixes before SQL interpolation
    if not prefix or not prefix.isalpha():
        raise ValueError(f"Instance prefix must be alphabetic, got: {prefix!r}")

    seq_name = f"{prefix.lower()}_instance_seq"

    sql = f"""
    CREATE SEQUENCE IF NOT EXISTS "{seq_name}";

    -- Initialize sequence so next nextval() yields max(existing numeric suffix) + 1.
    -- Also: never move the sequence backwards (avoid reusing previously-issued EUIs).
    WITH
      desired AS (
        SELECT
          COALESCE(
            (
              SELECT max(euid_seq)
              FROM generic_instance
              WHERE euid_prefix = '{prefix}'
            ),
            0
          ) + 1 AS next_val
      ),
      seq_state AS (
        SELECT last_value, is_called FROM "{seq_name}"
      ),
      seq_next AS (
        SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END AS next_val
        FROM seq_state
      ),
      final_next AS (
        SELECT GREATEST(
          (SELECT next_val FROM desired),
          (SELECT next_val FROM seq_next)
        ) AS next_val
      )
    SELECT setval(
      '"{seq_name}"',
      (SELECT next_val FROM final_next),
      false
    );
    """

    success, output = _run_psql(env, sql=sql)
    if not success:
        raise RuntimeError(
            f"Failed to ensure sequence for prefix {prefix}: {output[:200]}"
        )


def _write_migration_baseline(env: "Environment") -> None:
    """Write a migration baseline so fresh installs never apply legacy migrations."""
    migrations_dir = Path(__file__).parent.parent.parent / "schema" / "migrations"
    if not migrations_dir.exists():
        return

    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        return

    # Ensure tracking table exists (also created by base schema on fresh installs)
    ok, out = _run_psql(
        env,
        sql="""
        CREATE TABLE IF NOT EXISTS _tapdb_migrations (
            filename TEXT PRIMARY KEY,
            applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )
    if not ok:
        raise RuntimeError(out)

    for mf in migration_files:
        filename = mf.name.replace("'", "''")
        ok, out = _run_psql(
            env,
            sql=(
                "INSERT INTO _tapdb_migrations (filename) "
                f"VALUES ('{filename}') ON CONFLICT (filename) DO NOTHING"
            ),
        )
        if not ok:
            raise RuntimeError(out)


def _get_project_root() -> Path:
    """Get the project root directory."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path.cwd()


def _find_config_dir() -> Path:
    """Find the TAPDB config directory with template JSON files."""
    # Check relative to this file
    pkg_config = Path(__file__).parent.parent.parent / "config"
    if pkg_config.exists():
        return pkg_config

    # Check current directory
    cwd_config = Path.cwd() / "config"
    if cwd_config.exists():
        return cwd_config

    # Check project root
    root_config = _get_project_root() / "config"
    if root_config.exists():
        return root_config

    raise FileNotFoundError(
        "Cannot find config/ directory with template JSON files. "
        "Run from the daylily-tapdb repo root or ensure config is installed."
    )


def _find_tapdb_core_config_dir() -> Path:
    """Find TAPDB's built-in core template config directory."""
    candidates: list[Path] = []

    try:
        tapdb_pkg = importlib.import_module("daylily_tapdb")
        pkg_file = Path(tapdb_pkg.__file__).resolve()
        candidates.extend(
            [
                pkg_file.parent / "core_config",
                pkg_file.parents[1] / "config",
                pkg_file.parents[2] / "config",
            ]
        )
    except Exception:
        pass

    current = Path(__file__).resolve()
    candidates.extend(
        [
            current.parents[1] / "core_config",
            current.parents[2] / "config",
            current.parents[3] / "config",
        ]
    )

    for candidate in candidates:
        if not candidate.exists() or not candidate.is_dir():
            continue
        if (candidate / "actor" / "actor.json").exists() and (
            candidate / "generic" / "generic.json"
        ).exists():
            return candidate

    raise FileNotFoundError(
        "Cannot find TAPDB core config directory with actor/generic templates."
    )


def _resolve_seed_config_dirs(config_path: Optional[Path]) -> list[Path]:
    """Resolve ordered template config directories for seeding.

    Always includes TAPDB core config first, then caller-provided/auto-discovered
    client config when different.
    """
    core_dir = _find_tapdb_core_config_dir().resolve()
    dirs: list[Path] = [core_dir]

    client_dir: Path | None = config_path.resolve() if config_path is not None else None

    if client_dir is not None and client_dir != core_dir:
        dirs.append(client_dir)

    return dirs


def _normalize_config_dirs(config_dirs: Path | list[Path]) -> list[Path]:
    """Normalize config directory input into a de-duplicated ordered list."""
    dirs = [config_dirs] if isinstance(config_dirs, Path) else list(config_dirs)
    seen_dirs: set[Path] = set()
    unique_dirs: list[Path] = []
    for directory in dirs:
        resolved = directory.resolve()
        if resolved in seen_dirs:
            continue
        seen_dirs.add(resolved)
        unique_dirs.append(resolved)
    return unique_dirs


# Environment enum
class Environment(str, Enum):
    dev = "dev"
    test = "test"
    prod = "prod"


# Legacy compatibility constants; runtime code resolves active namespace paths lazily.
# Keep these import-safe and context-free to avoid stale/incorrect values.
CONFIG_DIR = Path.home() / ".config" / "tapdb"
LOG_DIR = CONFIG_DIR / "logs"


def _ensure_dirs():
    """Ensure config directories exist."""
    config_dir = get_config_path().parent
    log_dir = config_dir / "logs"
    config_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)


def _log_operation(env: str, operation: str, details: str = ""):
    """Log database operations for audit trail."""
    _ensure_dirs()
    log_file = (get_config_path().parent / "logs") / "db_operations.log"
    timestamp = datetime.now().isoformat()
    user = os.environ.get("USER", "unknown")
    with open(log_file, "a") as f:
        f.write(f"{timestamp} | {user} | {env} | {operation} | {details}\n")


def _get_db_config(env: Environment) -> dict:
    """Get database configuration for environment."""
    return get_db_config_for_env(env.value)


def _get_connection_string(env: Environment, database: Optional[str] = None) -> str:
    """Build PostgreSQL connection string for display.

    Intentionally omits any password. Commands use PGPASSWORD/.pgpass for auth.
    For aurora environments, appends ``?sslmode=verify-full``.
    """
    cfg = _get_db_config(env)
    db = database or cfg["database"]
    base = f"postgresql://{cfg['user']}@{cfg['host']}:{cfg['port']}/{db}"
    if cfg.get("engine_type") == "aurora":
        return f"{base}?sslmode=verify-full"
    return base


def _find_schema_file() -> Path:
    """Find the TAPDB schema SQL file."""
    # Check relative to this file
    pkg_schema = Path(__file__).parent.parent.parent / "schema" / "tapdb_schema.sql"
    if pkg_schema.exists():
        return pkg_schema

    # Check current directory
    cwd_schema = Path.cwd() / "schema" / "tapdb_schema.sql"
    if cwd_schema.exists():
        return cwd_schema

    raise FileNotFoundError(
        "Cannot find schema/tapdb_schema.sql. "
        "Run from the daylily-tapdb repo root or ensure schema is installed."
    )


def _run_psql(
    env: Environment, sql: str = None, file: Path = None, database: str = None
) -> tuple[bool, str]:
    """Run psql command and return (success, output).

    For aurora engine_type environments, delegates to
    ``AuroraSchemaDeployer.run_psql`` which enforces SSL
    (``sslmode=verify-full``) and uses IAM auth or Secrets Manager.
    """
    cfg = _get_db_config(env)
    db = database or cfg["database"]

    if cfg.get("engine_type") == "aurora":
        from daylily_tapdb.aurora.schema_deployer import AuroraSchemaDeployer

        iam_auth = cfg.get("iam_auth", "true").lower() in ("true", "1", "yes")
        return AuroraSchemaDeployer.run_psql(
            host=cfg["host"],
            port=int(cfg["port"]),
            user=cfg["user"],
            database=db,
            region=cfg.get("region", "us-west-2"),
            iam_auth=iam_auth,
            password=cfg.get("password") or None,
            sql=sql,
            file=file,
        )

    cmd = [
        "psql",
        "-X",  # do not read ~/.psqlrc
        "-q",  # quiet
        "-t",  # tuples only
        "-A",  # unaligned
        "-h",
        cfg["host"],
        "-p",
        cfg["port"],
        "-U",
        cfg["user"],
        "-d",
        db,
        "-v",
        "ON_ERROR_STOP=1",
    ]

    if file:
        cmd.extend(["-f", str(file)])
    elif sql:
        cmd.extend(["-c", sql])

    env_vars = os.environ.copy()
    if cfg["password"]:
        env_vars["PGPASSWORD"] = cfg["password"]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env_vars,
        )
        if result.returncode == 0:
            return True, (result.stdout or "").strip()
        return False, (result.stdout + result.stderr).strip()
    except FileNotFoundError:
        return False, "psql not found. Please install PostgreSQL client."
    except Exception as e:
        return False, str(e)


def _check_db_exists(env: Environment, database: str) -> bool:
    """Check if database exists."""
    _get_db_config(env)
    success, output = _run_psql(
        env,
        sql=f"SELECT 1 FROM pg_database WHERE datname = '{database}'",
        database="postgres",
    )
    return success and output.strip() == "1"


def _parse_single_int(output: str) -> int:
    """Parse a single integer value from machine-formatted psql output."""
    for ln in (output or "").splitlines():
        s = ln.strip()
        if not s:
            continue
        try:
            return int(s)
        except ValueError:
            continue
    raise ValueError(f"Could not parse int from output: {output!r}")


def _get_table_counts(env: Environment) -> dict:
    """Get row counts for TAPDB tables."""
    tables = [
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
        "tapdb_identity_prefix_config",
    ]
    counts = {}
    for table in tables:
        success, output = _run_psql(env, sql=f"SELECT COUNT(*) FROM {table}")
        if success:
            try:
                counts[table] = _parse_single_int(output)
            except ValueError:
                counts[table] = "?"
        else:
            counts[table] = None
    return counts


def _schema_exists(env: Environment) -> bool:
    """Check if TAPDB schema exists in database."""
    success, output = _run_psql(
        env,
        sql=(
            "SELECT COUNT(*) FROM information_schema.tables"
            " WHERE table_name = 'generic_template'"
        ),
    )
    if not success:
        return False
    try:
        return _parse_single_int(output) > 0
    except ValueError:
        return False


# ============================================================================
# CLI Commands
# ============================================================================

db_app = typer.Typer(help="Database lifecycle commands")
schema_app = typer.Typer(help="Schema lifecycle commands")
data_app = typer.Typer(help="Data operations")
config_app = typer.Typer(help="Configuration validation commands")

db_app.add_typer(schema_app, name="schema")
db_app.add_typer(data_app, name="data")
db_app.add_typer(config_app, name="config")


@db_app.command("create")
def db_create(
    env: Environment = typer.Argument(..., help="Target environment"),
    owner: Optional[str] = typer.Option(
        None, "--owner", "-o", help="Database owner (default: connection user)"
    ),
):
    """Create the TAPDB database for the specified environment."""
    cfg = _get_db_config(env)
    db_name = cfg["database"]
    db_owner = owner or cfg["user"]

    console.print(
        f"\n[bold cyan]━━━ Create TAPDB Database ({env.value}) ━━━[/bold cyan]"
    )
    console.print(f"  Host:     {cfg['host']}:{cfg['port']}")
    console.print(f"  Database: {db_name}")
    console.print(f"  Owner:    {db_owner}")

    ok, out = _run_psql(env, sql="SELECT 1", database="postgres")
    if not ok:
        console.print("[red]✗[/red] Cannot connect to PostgreSQL for this environment")
        console.print(f"  {out}")
        raise typer.Exit(1)

    if _check_db_exists(env, db_name):
        console.print(f"[yellow]⚠[/yellow] Database '{db_name}' already exists")
        return

    console.print(f"[yellow]►[/yellow] Creating database '{db_name}'...")
    sql = f'CREATE DATABASE "{db_name}" OWNER "{db_owner}"'
    success, output = _run_psql(env, sql=sql, database="postgres")
    if not success:
        console.print("[red]✗[/red] Failed to create database")
        console.print(f"  {output}")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Database '{db_name}' created")
    console.print(f"  Next: [cyan]tapdb db schema apply {env.value}[/cyan]")


@db_app.command("delete")
def db_delete(
    env: Environment = typer.Argument(..., help="Target environment"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Delete the TAPDB database for the specified environment."""
    cfg = _get_db_config(env)
    db_name = cfg["database"]

    ok, out = _run_psql(env, sql="SELECT 1", database="postgres")
    if not ok:
        console.print("[red]✗[/red] Cannot connect to PostgreSQL for this environment")
        console.print(f"  {out}")
        raise typer.Exit(1)

    if not _check_db_exists(env, db_name):
        console.print(f"[yellow]⚠[/yellow] Database '{db_name}' does not exist")
        return

    if not force:
        console.print("\n[bold red]⚠️  WARNING: DESTRUCTIVE OPERATION[/bold red]")
        console.print(f"This will permanently delete database: [bold]{db_name}[/bold]")
        console.print("All data will be lost!\n")

        if env == Environment.prod:
            console.print("[bold red]🚨 THIS IS A PRODUCTION DATABASE! 🚨[/bold red]\n")

        if not Confirm.ask(f"Delete database '{db_name}'?", default=False):
            console.print("[dim]Aborted.[/dim]")
            raise typer.Exit(0)

        if env == Environment.prod:
            typed = typer.prompt("Type the database name to confirm")
            if typed != db_name:
                console.print("[red]✗[/red] Name mismatch. Aborted.")
                raise typer.Exit(1)

    console.print(f"[yellow]►[/yellow] Deleting database '{db_name}'...")
    term_sql = f"""
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '{db_name}' AND pid <> pg_backend_pid()
    """
    _run_psql(env, sql=term_sql, database="postgres")

    success, output = _run_psql(
        env, sql=f'DROP DATABASE "{db_name}"', database="postgres"
    )
    if not success:
        console.print("[red]✗[/red] Failed to delete database")
        console.print(f"  {output}")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Database '{db_name}' deleted")


@schema_app.command("apply")
def db_schema_apply(
    env: Environment = typer.Argument(..., help="Target environment"),
    reinitialize: bool = typer.Option(
        False, "--reinitialize", "-r", help="Reapply schema even if it already exists"
    ),
):
    """Apply TAPDB schema to an existing database."""
    _ensure_dirs()
    cfg = _get_db_config(env)

    console.print(f"\n[bold cyan]━━━ Apply TAPDB Schema ({env.value}) ━━━[/bold cyan]")
    console.print(f"  Host:     {cfg['host']}:{cfg['port']}")
    console.print(f"  Database: {cfg['database']}")
    console.print(f"  User:     {cfg['user']}")
    console.print()

    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        console.print(f"  Create with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    try:
        schema_file = _find_schema_file()
        console.print(f"[green]✓[/green] Schema file: {schema_file}")
    except FileNotFoundError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    if _schema_exists(env) and not reinitialize:
        console.print(
            f"[dim]○[/dim] Schema already exists in {cfg['database']} (skipping apply)"
        )
        console.print(
            "[yellow]►[/yellow] Syncing required identity prefixes "
            "(audit_log)..."
        )
        try:
            _sync_identity_prefix_config(env)
            console.print("[green]✓[/green] Identity prefixes synced")
        except (ValueError, RuntimeError) as e:
            console.print(f"[red]✗[/red] {e}")
            raise typer.Exit(1)
        return

    console.print("[yellow]►[/yellow] Applying schema...")
    success, output = _run_psql(env, file=schema_file)
    if not success:
        console.print(f"[red]✗[/red] Schema apply failed:\n{output}")
        _log_operation(env.value, "SCHEMA_APPLY_FAILED", output[:200])
        raise typer.Exit(1)

    _log_operation(env.value, "SCHEMA_APPLY", f"Schema applied from {schema_file}")
    console.print("[green]✓[/green] Schema applied successfully")
    console.print(
        "[yellow]►[/yellow] Syncing required identity prefixes "
        "(audit_log)..."
    )
    try:
        _sync_identity_prefix_config(env)
        console.print("[green]✓[/green] Identity prefixes synced")
    except (ValueError, RuntimeError) as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    try:
        _write_migration_baseline(env)
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to write migration baseline: {e}")
        raise typer.Exit(1)

    console.print("\n[bold]Tables available:[/bold]")
    for table in [
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
        "tapdb_identity_prefix_config",
    ]:
        console.print(f"  [green]✓[/green] {table}")


@schema_app.command("status")
def db_status(
    env: Environment = typer.Argument(..., help="Target environment"),
):
    """Check TAPDB schema status in the specified environment."""
    cfg = _get_db_config(env)

    console.print(f"\n[bold cyan]━━━ TAPDB Status ({env.value}) ━━━[/bold cyan]")

    # Check database exists
    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        console.print(f"\n  Create with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Database: {cfg['database']}")

    # Check schema
    if not _schema_exists(env):
        console.print("[red]✗[/red] TAPDB schema not found")
        console.print(
            f"\n  Initialize with: [cyan]tapdb db schema apply {env.value}[/cyan]"
        )
        raise typer.Exit(1)

    console.print("[green]✓[/green] Schema: installed")

    # Get table counts
    counts = _get_table_counts(env)

    table = Table(title="Table Statistics")
    table.add_column("Table", style="cyan")
    table.add_column("Rows", justify="right")

    for tbl, count in counts.items():
        if count is None:
            table.add_row(tbl, "[red]error[/red]")
        else:
            table.add_row(tbl, str(count))

    console.print()
    console.print(table)

    # Connection info
    console.print("\n[bold]Connection:[/bold]")
    console.print(f"  Host: {cfg['host']}:{cfg['port']}")
    console.print(f"  User: {cfg['user']}")
    if cfg.get("engine_type") == "aurora":
        console.print("  Engine: [bold yellow]Aurora PostgreSQL[/bold yellow]")
        console.print(f"  Region: {cfg.get('region', 'us-west-2')}")
        console.print("  SSL:    verify-full (enforced)")
        iam = cfg.get("iam_auth", "true").lower() in ("true", "1", "yes")
        console.print(f"  Auth:   {'IAM' if iam else 'password'}")
    console.print(f"  URL:  [dim]{_get_connection_string(env)}[/dim]")


@schema_app.command("reset")
def db_nuke(
    env: Environment = typer.Argument(..., help="Target environment"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Skip confirmations (for CI/automation)"
    ),
):
    """
    Completely drop all TAPDB tables and data.

    ⚠️  DESTRUCTIVE OPERATION - This cannot be undone!
    """
    cfg = _get_db_config(env)
    is_aurora = cfg.get("engine_type") == "aurora"

    # Get what will be deleted
    if not _check_db_exists(env, cfg["database"]):
        db_name = cfg["database"]
        console.print(
            f"[yellow]⚠[/yellow] Database '{db_name}' does not exist. Nothing to nuke."
        )
        return

    counts = _get_table_counts(env)
    total_rows = sum(c for c in counts.values() if isinstance(c, int))

    aurora_warning = ""
    if is_aurora:
        aurora_warning = (
            "\n[bold yellow]⚠ AURORA CLUSTER:[/bold yellow] This drops schema "
            "objects only.\n  To delete the Aurora cluster itself, use: "
            "[cyan]tapdb aurora delete[/cyan]\n"
        )

    # Show what will be deleted
    console.print(
        Panel(
            f"[bold red]⚠️  DESTRUCTIVE OPERATION[/bold red]\n\n"
            f"Environment: [bold]{env.value.upper()}[/bold]\n"
            f"Database:    [bold]{cfg['database']}[/bold]\n"
            f"Host:        {cfg['host']}:{cfg['port']}\n"
            f"{aurora_warning}\n"
            f"[yellow]Data to be deleted:[/yellow]\n"
            f"  • generic_template:         "
            f"{counts.get('generic_template', '?')} rows\n"
            f"  • generic_instance:         "
            f"{counts.get('generic_instance', '?')} rows\n"
            f"  • generic_instance_lineage: "
            f"{counts.get('generic_instance_lineage', '?')}"
            f" rows\n"
            f"  • audit_log:                {counts.get('audit_log', '?')} rows\n"
            f"  • tapdb_identity_prefix_config: "
            f"{counts.get('tapdb_identity_prefix_config', '?')} rows\n"
            f"  • All sequences, triggers, and functions\n\n"
            f"[bold]Total: {total_rows} rows[/bold]",
            title="[red]DATABASE NUKE[/red]",
            border_style="red",
        )
    )

    if not force:
        # Confirmation 1: Environment
        env_upper = env.value.upper()
        console.print(
            f"\n[bold]Confirmation 1/3:[/bold] You are about"
            f" to nuke the [bold red]{env_upper}[/bold red]"
            " database."
        )
        if not Confirm.ask("  Proceed?", default=False):
            console.print("[dim]Aborted.[/dim]")
            return

        # Confirmation 2: Type environment name
        console.print(
            "\n[bold]Confirmation 2/3:[/bold] Type the environment name to confirm:"
        )
        typed_env = Prompt.ask("  Environment name")
        if typed_env.lower() != env.value.lower():
            console.print(
                f"[red]✗[/red] Input '{typed_env}' does not"
                f" match '{env.value}'. Aborted."
            )
            return

        # Confirmation 3: Type DELETE EVERYTHING
        console.print(
            "\n[bold]Confirmation 3/3:[/bold] Type"
            " [bold red]DELETE EVERYTHING[/bold red]"
            " to proceed:"
        )
        typed_confirm = Prompt.ask("  Confirm")
        if typed_confirm != "DELETE EVERYTHING":
            console.print(
                "[red]✗[/red] Input does not match 'DELETE EVERYTHING'. Aborted."
            )
            return

    console.print("\n[yellow]►[/yellow] Nuking TAPDB schema...")

    # Drop order matters for foreign keys
    drop_sql = """  -- noqa: E501
    -- Drop triggers first
    DROP TRIGGER IF EXISTS trigger_set_generic_instance_euid
      ON generic_instance;
    DROP TRIGGER IF EXISTS soft_delete_generic_template
      ON generic_template;
    DROP TRIGGER IF EXISTS soft_delete_generic_instance
      ON generic_instance;
    DROP TRIGGER IF EXISTS soft_delete_generic_instance_lineage
      ON generic_instance_lineage;
    DROP TRIGGER IF EXISTS audit_insert_generic_template
      ON generic_template;
    DROP TRIGGER IF EXISTS audit_insert_generic_instance
      ON generic_instance;
    DROP TRIGGER IF EXISTS audit_insert_generic_instance_lineage
      ON generic_instance_lineage;
    DROP TRIGGER IF EXISTS audit_update_generic_template
      ON generic_template;
    DROP TRIGGER IF EXISTS audit_update_generic_instance
      ON generic_instance;
    DROP TRIGGER IF EXISTS audit_update_generic_instance_lineage
      ON generic_instance_lineage;
    DROP TRIGGER IF EXISTS update_modified_dt_generic_template
      ON generic_template;
    DROP TRIGGER IF EXISTS update_modified_dt_generic_instance
      ON generic_instance;
    DROP TRIGGER IF EXISTS update_modified_dt_generic_instance_lineage
      ON generic_instance_lineage;

    -- Drop tables (order matters for FK constraints)
    DROP TABLE IF EXISTS audit_log CASCADE;
    DROP TABLE IF EXISTS generic_instance_lineage CASCADE;
    DROP TABLE IF EXISTS generic_instance CASCADE;
    DROP TABLE IF EXISTS generic_template CASCADE;
    DROP TABLE IF EXISTS tapdb_identity_prefix_config CASCADE;

    -- Drop sequences
    DROP SEQUENCE IF EXISTS generic_template_seq;
    DROP SEQUENCE IF EXISTS gx_instance_seq;
    DROP SEQUENCE IF EXISTS generic_instance_lineage_seq;
    DROP SEQUENCE IF EXISTS wx_instance_seq;
    DROP SEQUENCE IF EXISTS wsx_instance_seq;
    DROP SEQUENCE IF EXISTS xx_instance_seq;
    DROP SEQUENCE IF EXISTS ay_instance_seq;

    -- Drop functions
    DROP FUNCTION IF EXISTS set_generic_template_euid();
    DROP FUNCTION IF EXISTS set_generic_instance_euid();
    DROP FUNCTION IF EXISTS set_generic_instance_lineage_euid();
    DROP FUNCTION IF EXISTS set_audit_log_euid();
    DROP FUNCTION IF EXISTS tapdb_get_identity_prefix(TEXT);
    DROP FUNCTION IF EXISTS tapdb_validate_meridian_prefix(TEXT);
    DROP FUNCTION IF EXISTS meridian_euid_prefix(TEXT);
    DROP FUNCTION IF EXISTS meridian_euid_seq_from_euid(TEXT);
    DROP FUNCTION IF EXISTS crockford_base32_decode(TEXT);
    DROP FUNCTION IF EXISTS soft_delete_row();
    DROP FUNCTION IF EXISTS record_update();
    DROP FUNCTION IF EXISTS record_insert();
    DROP FUNCTION IF EXISTS update_modified_dt();
    """

    success, output = _run_psql(env, sql=drop_sql)

    if success:
        _log_operation(env.value, "NUKE", f"Deleted {total_rows} rows from all tables")
        console.print("[green]✓[/green] TAPDB schema nuked successfully")
        console.print(
            f"\n  Recreate with: [cyan]tapdb db schema apply {env.value}[/cyan]"
        )
    else:
        console.print(f"[red]✗[/red] Nuke failed:\n{output}")
        _log_operation(env.value, "NUKE_FAILED", output[:200])
        raise typer.Exit(1)


@schema_app.command("migrate")
def db_migrate(
    env: Environment = typer.Argument(..., help="Target environment"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be done without making changes"
    ),
):
    """Apply schema migrations/updates to the specified environment."""
    cfg = _get_db_config(env)

    console.print(
        f"\n[bold cyan]━━━ Migrate TAPDB Schema ({env.value}) ━━━[/bold cyan]"
    )

    # Check database and schema exist
    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        raise typer.Exit(1)

    if not _schema_exists(env):
        console.print(
            "[red]✗[/red] TAPDB schema not found. "
            "Use 'tapdb db schema apply' first."
        )
        raise typer.Exit(1)

    # Find migration files
    migrations_dir = Path(__file__).parent.parent.parent / "schema" / "migrations"
    if not migrations_dir.exists():
        console.print(
            f"[yellow]⚠[/yellow] No migrations directory found at {migrations_dir}"
        )
        console.print("[dim]Schema is up to date (no migrations to apply).[/dim]")
        return

    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        console.print("[dim]No migration files found. Schema is up to date.[/dim]")
        return

    # Track applied migrations
    ok, out = _run_psql(
        env,
        sql="""
        CREATE TABLE IF NOT EXISTS _tapdb_migrations (
            filename TEXT PRIMARY KEY,
            applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )
    if not ok:
        console.print(f"[red]✗[/red] Failed to ensure migrations table:\n{out}")
        raise typer.Exit(1)

    # Get already applied migrations (parse conservatively from default psql output)
    success, output = _run_psql(env, sql="SELECT filename FROM _tapdb_migrations")
    applied = (
        {ln.strip() for ln in output.splitlines() if ln.strip().endswith(".sql")}
        if success
        else set()
    )

    pending = [f for f in migration_files if f.name not in applied]

    if not pending:
        console.print("[green]✓[/green] All migrations already applied")
        return

    console.print(f"[yellow]►[/yellow] {len(pending)} migration(s) pending:")
    for mf in pending:
        console.print(f"  • {mf.name}")

    if dry_run:
        console.print("\n[dim]Dry run - no changes made.[/dim]")
        return

    for mf in pending:
        console.print(f"\n[yellow]►[/yellow] Applying {mf.name}...")
        success, output = _run_psql(env, file=mf)

        if success:
            # Record migration
            filename = mf.name.replace("'", "''")
            _run_psql(
                env,
                sql=(
                    "INSERT INTO _tapdb_migrations (filename) "
                    f"VALUES ('{filename}') ON CONFLICT (filename) DO NOTHING"
                ),
            )
            console.print(f"[green]✓[/green] {mf.name} applied")
            _log_operation(env.value, "MIGRATE", mf.name)
        else:
            console.print(f"[red]✗[/red] Migration failed:\n{output}")
            _log_operation(env.value, "MIGRATE_FAILED", f"{mf.name}: {output[:100]}")
            raise typer.Exit(1)

    console.print("\n[green]✓[/green] All migrations applied successfully")


@data_app.command("backup")
def db_backup(
    env: Environment = typer.Argument(..., help="Target environment"),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output file path"
    ),
    data_only: bool = typer.Option(
        False, "--data-only", help="Backup data only (no schema)"
    ),
):
    """Backup TAPDB data from the specified environment."""
    cfg = _get_db_config(env)

    console.print(f"\n[bold cyan]━━━ Backup TAPDB ({env.value}) ━━━[/bold cyan]")

    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        raise typer.Exit(1)

    # Generate output filename
    if output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output = Path(f"tapdb_{env.value}_{timestamp}.sql")

    # Build pg_dump command
    cmd = [
        "pg_dump",
        "-h",
        cfg["host"],
        "-p",
        cfg["port"],
        "-U",
        cfg["user"],
        "-d",
        cfg["database"],
        "-f",
        str(output),
        "--no-owner",
        "--no-privileges",
    ]

    # Only backup TAPDB tables
    tables = [
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
        "tapdb_identity_prefix_config",
    ]
    for table in tables:
        cmd.extend(["-t", table])

    if data_only:
        cmd.append("--data-only")

    env_vars = os.environ.copy()
    if cfg["password"]:
        env_vars["PGPASSWORD"] = cfg["password"]

    console.print("[yellow]►[/yellow] Creating backup...")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, env=env_vars)

        if result.returncode == 0:
            file_size = output.stat().st_size
            size_str = (
                f"{file_size / 1024:.1f} KB"
                if file_size > 1024
                else f"{file_size} bytes"
            )

            _log_operation(env.value, "BACKUP", str(output))
            console.print(f"[green]✓[/green] Backup created: {output} ({size_str})")
        else:
            console.print(f"[red]✗[/red] Backup failed:\n{result.stderr}")
            raise typer.Exit(1)
    except FileNotFoundError:
        console.print(
            "[red]✗[/red] pg_dump not found. Please install PostgreSQL client."
        )
        raise typer.Exit(1)


@data_app.command("restore")
def db_restore(
    env: Environment = typer.Argument(..., help="Target environment"),
    input_file: Path = typer.Option(..., "--input", "-i", help="Input backup file"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Restore TAPDB data from a backup file."""
    cfg = _get_db_config(env)

    console.print(f"\n[bold cyan]━━━ Restore TAPDB ({env.value}) ━━━[/bold cyan]")

    if not input_file.exists():
        console.print(f"[red]✗[/red] Backup file not found: {input_file}")
        raise typer.Exit(1)

    file_size = input_file.stat().st_size
    size_str = (
        f"{file_size / 1024:.1f} KB" if file_size > 1024 else f"{file_size} bytes"
    )

    console.print(f"  File:     {input_file} ({size_str})")
    console.print(f"  Target:   {cfg['database']} ({env.value})")

    if not force:
        console.print(
            "\n[yellow]⚠[/yellow] This will overwrite"
            f" existing data in {cfg['database']}"
        )
        if not Confirm.ask("  Proceed?", default=False):
            console.print("[dim]Aborted.[/dim]")
            return

    # Ensure database exists
    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        console.print(f"  Create with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    console.print("[yellow]►[/yellow] Restoring from backup...")

    success, output = _run_psql(env, file=input_file)

    if success:
        _log_operation(env.value, "RESTORE", str(input_file))
        console.print("[green]✓[/green] Restore completed")

        # Show counts
        counts = _get_table_counts(env)
        console.print("\n[bold]Restored data:[/bold]")
        for table, count in counts.items():
            console.print(f"  {table}: {count} rows")
    else:
        console.print(f"[red]✗[/red] Restore failed:\n{output}")
        _log_operation(env.value, "RESTORE_FAILED", output[:200])
        raise typer.Exit(1)


# Core template categories (always seeded)
CORE_CATEGORIES = {"generic", "actor"}

# Optional template categories (only when provided via external config packs)
# TAPDB no longer bundles non-core template packs in this repository.
OPTIONAL_CATEGORIES: set[str] = set()


def _load_template_configs(
    config_dirs: Path | list[Path], include_optional: bool = False
) -> list[dict]:
    """Load template configurations from one or more config directories.

    Args:
        config_dirs: Path or list of paths to config directories
        include_optional: If True, include optional non-core template packs

    Returns list of template dicts ready for database insertion.
    """
    templates = []
    allowed_categories = CORE_CATEGORIES.copy()
    if include_optional:
        allowed_categories.update(OPTIONAL_CATEGORIES)

    unique_dirs = _normalize_config_dirs(config_dirs)

    for config_dir in unique_dirs:
        if not config_dir.exists() or not config_dir.is_dir():
            console.print(
                f"[yellow]⚠[/yellow] Config directory not found or not a directory: {config_dir}"
            )
            continue

        for category_dir in sorted(config_dir.iterdir()):
            if not category_dir.is_dir() or category_dir.name.startswith("_"):
                continue

            # Filter by allowed categories
            if category_dir.name not in allowed_categories:
                continue

            for json_file in sorted(category_dir.glob("*.json")):
                try:
                    with open(json_file, "r") as f:
                        data = json.load(f)

                    # Extract templates from the file
                    if "templates" in data:
                        for tmpl in data["templates"]:
                            tmpl = dict(tmpl)
                            tmpl["_source_file"] = str(json_file)
                            templates.append(tmpl)

                except json.JSONDecodeError as e:
                    console.print(f"[yellow]⚠[/yellow] Invalid JSON in {json_file}: {e}")
                except Exception as e:
                    console.print(f"[yellow]⚠[/yellow] Error reading {json_file}: {e}")

    return templates


def _find_duplicate_template_keys(
    templates: list[dict],
) -> dict[tuple[str, str, str, str], list[str]]:
    """Return duplicate template keys with source files for hard-fail checks."""
    key_sources: dict[tuple[str, str, str, str], list[str]] = {}
    for tmpl in templates:
        key = _template_key(tmpl)
        source = str(tmpl.get("_source_file") or "(unknown)")
        key_sources.setdefault(key, []).append(source)
    return {key: sources for key, sources in key_sources.items() if len(sources) > 1}


@dataclass(frozen=True)
class _ConfigIssue:
    level: str  # "error" | "warning"
    message: str
    source_file: str | None = None
    template_code: str | None = None


def _normalize_template_code_str(code: Any) -> str:
    s = str(code).strip()
    if s.endswith("/"):
        s = s[:-1]
    return s


def _is_template_code_str(code: Any) -> bool:
    s = _normalize_template_code_str(code)
    parts = [p for p in s.split("/") if p]
    return len(parts) == 4


def _extract_template_refs(obj: Any) -> list[str]:
    """Return any template-code-like strings embedded in known config fields.

    This is intentionally conservative: we only look at the known fields used
    by current configs (action_imports / expected_inputs / expected_outputs /
    instantiation_layouts.child_templates).
    """

    refs: list[str] = []

    def _maybe_add(val: Any):
        if isinstance(val, str):
            refs.append(val)

    def _walk(container: Any):
        if not isinstance(container, dict):
            return
        ai = container.get("action_imports")
        if isinstance(ai, dict):
            for v in ai.values():
                _maybe_add(v)

        for k in ["expected_inputs", "expected_outputs"]:
            vals = container.get(k)
            if isinstance(vals, list):
                for v in vals:
                    _maybe_add(v)

        layouts = container.get("instantiation_layouts")
        if isinstance(layouts, list):
            for layout in layouts:
                if not isinstance(layout, dict):
                    continue
                children = layout.get("child_templates")
                if isinstance(children, list):
                    for c in children:
                        if isinstance(c, str):
                            _maybe_add(c)
                        elif isinstance(c, dict):
                            _maybe_add(c.get("template_code"))

    if isinstance(obj, dict):
        _walk(obj)
        ja = obj.get("json_addl")
        if isinstance(ja, dict):
            _walk(ja)

    return refs


def _validate_template_configs(
    config_dirs: Path | list[Path], *, strict: bool
) -> tuple[list[dict], list[_ConfigIssue]]:
    """Load and validate template config JSON files.

    This is a lightweight, dependency-free validator intended for operator
    safety (Phase 3). It validates:
    - JSON parses
    - file shape ({"templates": [...]})
    - basic required keys + types per template
    - duplicate (category, type, subtype, version) keys
    - template-code string formatting in reference fields

    If strict=True, missing referenced templates become errors.
    """

    issues: list[_ConfigIssue] = []
    templates: list[dict] = []

    unique_dirs = _normalize_config_dirs(config_dirs)
    if not unique_dirs:
        return [], [_ConfigIssue(level="error", message="No config directories provided")]

    # Load
    for config_dir in unique_dirs:
        if not config_dir.exists() or not config_dir.is_dir():
            issues.append(
                _ConfigIssue(
                    level="error",
                    message=f"Config directory not found: {config_dir}",
                )
            )
            continue

        for category_dir in sorted(config_dir.iterdir()):
            if not category_dir.is_dir() or category_dir.name.startswith("_"):
                continue

            for json_file in sorted(category_dir.glob("*.json")):
                source_file = str(json_file)
                try:
                    data = json.loads(json_file.read_text(encoding="utf-8"))
                except json.JSONDecodeError as e:
                    issues.append(
                        _ConfigIssue(
                            level="error",
                            source_file=source_file,
                            message=f"Invalid JSON: {e}",
                        )
                    )
                    continue
                except Exception as e:
                    issues.append(
                        _ConfigIssue(
                            level="error",
                            source_file=source_file,
                            message=f"Error reading file: {e}",
                        )
                    )
                    continue

                if not isinstance(data, dict):
                    issues.append(
                        _ConfigIssue(
                            level="error",
                            source_file=source_file,
                            message="Config root must be an object/dict",
                        )
                    )
                    continue
                tmpl_list = data.get("templates")
                if not isinstance(tmpl_list, list):
                    issues.append(
                        _ConfigIssue(
                            level="error",
                            source_file=source_file,
                            message="Missing or invalid 'templates' list",
                        )
                    )
                    continue

                for i, tmpl in enumerate(tmpl_list):
                    if not isinstance(tmpl, dict):
                        issues.append(
                            _ConfigIssue(
                                level="error",
                                source_file=source_file,
                                message=(
                                    f"Template[{i}] must be an"
                                    f" object/dict, got"
                                    f" {type(tmpl).__name__}"
                                ),
                            )
                        )
                        continue
                    tmpl = dict(tmpl)
                    tmpl["_source_file"] = source_file
                    templates.append(tmpl)

    if not templates:
        issues.append(
            _ConfigIssue(
                level="error",
                message="No templates found under configured directories",
            )
        )

    # Validate templates
    required_str = [
        "polymorphic_discriminator",
        "category",
        "type",
        "subtype",
        "version",
        "instance_prefix",
    ]
    keys_seen: dict[tuple[str, str, str, str], str] = {}
    codes: set[str] = set()
    refs: list[tuple[str, str, str]] = []  # (source_file, template_code, ref)

    def _validate_ref_container(
        container: Any, *, source_file: str | None, template_code: str
    ) -> None:
        if not isinstance(container, dict):
            return

        if (
            "action_imports" in container
            and container.get("action_imports") is not None
            and not isinstance(container.get("action_imports"), dict)
        ):
            issues.append(
                _ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=template_code,
                    message=(
                        "Field 'action_imports' must be an object/dict "
                        f"(got {type(container.get('action_imports')).__name__})"
                    ),
                )
            )

        for k in ["expected_inputs", "expected_outputs"]:
            if (
                k in container
                and container.get(k) is not None
                and not isinstance(container.get(k), list)
            ):
                issues.append(
                    _ConfigIssue(
                        level="error",
                        source_file=source_file,
                        template_code=template_code,
                        message=(
                            f"Field '{k}' must be an"
                            " array/list (got"
                            f" {type(container.get(k)).__name__})"
                        ),
                    )
                )

        if (
            "instantiation_layouts" in container
            and container.get("instantiation_layouts") is not None
        ):
            try:
                validate_instantiation_layouts(container.get("instantiation_layouts"))
            except ValidationError as e:
                issues.append(
                    _ConfigIssue(
                        level="error",
                        source_file=source_file,
                        template_code=template_code,
                        message=(
                            "Invalid instantiation_layouts:"
                            f" {format_validation_error(e)}"
                        ),
                    )
                )

    for tmpl in templates:
        source_file = str(tmpl.get("_source_file") or "") or None

        # required keys
        for k in required_str:
            v = tmpl.get(k)
            if not isinstance(v, str) or not v.strip():
                issues.append(
                    _ConfigIssue(
                        level="error",
                        source_file=source_file,
                        template_code=None,
                        message=(
                            f"Missing/invalid required field"
                            f" '{k}' (must be non-empty string)"
                        ),
                    )
                )

        code = _normalize_template_code_str(_template_code(tmpl))
        codes.add(code)

        # Validate instance_prefix formatting early (operator safety)
        try:
            _normalize_instance_prefix(str(tmpl.get("instance_prefix")))
        except Exception as e:
            issues.append(
                _ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=f"Invalid instance_prefix: {e}",
                )
            )

        # duplicate key
        key = _template_key(tmpl)
        if key in keys_seen:
            issues.append(
                _ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=(
                        f"Duplicate template key {key} also defined in {keys_seen[key]}"
                    ),
                )
            )
        else:
            keys_seen[key] = source_file or "(unknown)"

        # basic types for commonly-used fields
        if (
            "json_addl" in tmpl
            and tmpl.get("json_addl") is not None
            and not isinstance(tmpl.get("json_addl"), dict)
        ):
            issues.append(
                _ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=(
                        "Field 'json_addl' must be an"
                        " object/dict (got"
                        f" {type(tmpl.get('json_addl')).__name__})"
                    ),
                )
            )

        # Validate reference container fields at both top-level and under json_addl
        _validate_ref_container(tmpl, source_file=source_file, template_code=code)
        if isinstance(tmpl.get("json_addl"), dict):
            _validate_ref_container(
                tmpl.get("json_addl"), source_file=source_file, template_code=code
            )
        if "is_singleton" in tmpl and not isinstance(tmpl.get("is_singleton"), bool):
            issues.append(
                _ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=(
                        "Field 'is_singleton' must be"
                        " boolean (got"
                        f" {type(tmpl.get('is_singleton')).__name__})"
                    ),
                )
            )
        if (
            "instance_prefix" in tmpl
            and tmpl.get("instance_prefix") is not None
            and not isinstance(tmpl.get("instance_prefix"), str)
        ):
            issues.append(
                _ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=(
                        "Field 'instance_prefix' must be"
                        " string (got"
                        f" {type(tmpl.get('instance_prefix')).__name__})"
                    ),
                )
            )

        for ref in _extract_template_refs(tmpl):
            refs.append((source_file or "(unknown)", code, ref))
            if not _is_template_code_str(ref):
                issues.append(
                    _ConfigIssue(
                        level="error",
                        source_file=source_file,
                        template_code=code,
                        message=(
                            "Invalid template reference"
                            " (expected 'category/type/"
                            f"subtype/version'): {ref!r}"
                        ),
                    )
                )

    # Reference existence (optional)
    if refs:
        for source_file, owner_code, ref in refs:
            if not _is_template_code_str(ref):
                continue
            norm_ref = _normalize_template_code_str(ref)
            if norm_ref not in codes:
                lvl = "error" if strict else "warning"
                issues.append(
                    _ConfigIssue(
                        level=lvl,
                        source_file=source_file,
                        template_code=owner_code,
                        message=(
                            f"Referenced template not found in config set: {norm_ref}"
                        ),
                    )
                )

    return templates, issues


@config_app.command("validate")
def db_validate_config(
    config_path: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to template config directory"
    ),
    strict: bool = typer.Option(
        True,
        "--strict/--no-strict",
        help=(
            "If strict, missing referenced templates"
            " are treated as errors (non-zero exit)."
        ),
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Emit machine-readable JSON report"
    ),
):
    """Validate template JSON config files (no database required)."""

    try:
        config_dirs = _resolve_seed_config_dirs(config_path)
    except FileNotFoundError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    templates, issues = _validate_template_configs(config_dirs, strict=strict)
    errors = [i for i in issues if i.level == "error"]
    warnings = [i for i in issues if i.level == "warning"]

    if json_output:
        payload = {
            "config_dir": str(config_dirs[0]),
            "config_dirs": [str(d) for d in config_dirs],
            "strict": strict,
            "templates": len(templates),
            "errors": len(errors),
            "warnings": len(warnings),
            "issues": [
                {
                    "level": i.level,
                    "message": i.message,
                    "source_file": i.source_file,
                    "template_code": i.template_code,
                }
                for i in issues
            ],
        }
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        raise typer.Exit(1 if errors else 0)

    mode = "strict" if strict else "non-strict"
    console.print(f"\n[bold cyan]━━━ Validate Template Config ({mode}) ━━━[/bold cyan]")
    console.print("  Config directories:")
    for directory in config_dirs:
        console.print(f"    - [dim]{directory}[/dim]")
    console.print(f"  Templates loaded: {len(templates)}")

    if issues:
        # Use folding for long messages so validation details are not truncated
        # (important for test output capture and operator clarity).
        table = Table(title="Config validation issues", show_lines=False, expand=True)
        table.add_column("Level", style="bold")
        table.add_column("File")
        table.add_column("Template")
        table.add_column("Message", overflow="fold")
        for i in issues:
            lvl_style = "red" if i.level == "error" else "yellow"
            table.add_row(
                f"[{lvl_style}]{i.level}[/{lvl_style}]",
                i.source_file or "",
                i.template_code or "",
                i.message,
            )
        console.print(table)

    if errors:
        console.print(
            f"\n[red]✗[/red] Validation failed:"
            f" {len(errors)} error(s),"
            f" {len(warnings)} warning(s)"
        )
        raise typer.Exit(1)
    console.print(f"\n[green]✓[/green] Validation OK: {len(warnings)} warning(s)")


def _sql_escape_literal(val: str) -> str:
    return str(val).replace("'", "''")


def _template_code(template: dict) -> str:
    cat = template.get("category")
    typ = template.get("type")
    sub = template.get("subtype")
    ver = template.get("version")
    return f"{cat}/{typ}/{sub}/{ver}/"


def _template_key(template: dict) -> tuple[str, str, str, str]:
    return (
        str(template.get("category", "")),
        str(template.get("type", "")),
        str(template.get("subtype", "")),
        str(template.get("version", "")),
    )


def _template_exists(
    env: Environment, category: str, type_: str, subtype: str, version: str
) -> bool:
    """Check if a template exists by canonical uniqueness key."""
    sql = (
        "SELECT 1 FROM generic_template "
        f"WHERE category = '{_sql_escape_literal(category)}' "
        f"AND type = '{_sql_escape_literal(type_)}' "
        f"AND subtype = '{_sql_escape_literal(subtype)}' "
        f"AND version = '{_sql_escape_literal(version)}'"
    )
    success, output = _run_psql(env, sql=sql)
    return success and output.strip() == "1"


def _upsert_template(
    env: Environment, template: dict, overwrite: bool
) -> tuple[bool, str]:
    """Upsert a template. If overwrite=False, existing rows are left untouched."""

    name = _sql_escape_literal(template.get("name", ""))
    pd = _sql_escape_literal(template.get("polymorphic_discriminator", ""))
    category = _sql_escape_literal(template.get("category", ""))
    type_ = _sql_escape_literal(template.get("type", ""))
    subtype = _sql_escape_literal(template.get("subtype", ""))
    version = _sql_escape_literal(template.get("version", ""))
    instance_prefix = _sql_escape_literal(template.get("instance_prefix", "GX"))
    bstatus = _sql_escape_literal(template.get("bstatus", "active"))

    instance_pi = template.get("instance_polymorphic_identity")
    instance_pi_sql = f"'{_sql_escape_literal(instance_pi)}'" if instance_pi else "NULL"

    json_addl = _sql_escape_literal(json.dumps(template.get("json_addl", {})))
    if template.get("json_addl_schema") is None:
        json_addl_schema_sql = "NULL"
    else:
        schema_json = json.dumps(template.get("json_addl_schema"))
        escaped = _sql_escape_literal(schema_json)
        json_addl_schema_sql = f"'{escaped}'::jsonb"

    is_singleton = str(bool(template.get("is_singleton", False))).upper()

    if overwrite:
        # Report whether we inserted (t) or updated (f)
        sql = f"""
        INSERT INTO generic_template (
            name, polymorphic_discriminator, category, type, subtype, version,
            instance_prefix, instance_polymorphic_identity, json_addl, json_addl_schema,
            bstatus, is_singleton, is_deleted
        ) VALUES (
            '{name}',
            '{pd}',
            '{category}',
            '{type_}',
            '{subtype}',
            '{version}',
            '{instance_prefix}',
            {instance_pi_sql},
            '{json_addl}'::jsonb,
            {json_addl_schema_sql},
            '{bstatus}',
            {is_singleton},
            FALSE
        )
        ON CONFLICT (category, type, subtype, version)
        DO UPDATE SET
            name = EXCLUDED.name,
            polymorphic_discriminator = EXCLUDED.polymorphic_discriminator,
            instance_prefix = EXCLUDED.instance_prefix,
            instance_polymorphic_identity = EXCLUDED.instance_polymorphic_identity,
            json_addl = EXCLUDED.json_addl,
            json_addl_schema = EXCLUDED.json_addl_schema,
            bstatus = EXCLUDED.bstatus,
            is_singleton = EXCLUDED.is_singleton,
            is_deleted = FALSE
        RETURNING (xmax = 0) AS inserted;
        """
        return _run_psql(env, sql=sql)

    # overwrite=False: do not touch existing templates; return 1 iff inserted
    sql = f"""
    INSERT INTO generic_template (
        name, polymorphic_discriminator, category, type, subtype, version,
        instance_prefix, instance_polymorphic_identity, json_addl, json_addl_schema,
        bstatus, is_singleton, is_deleted
    ) VALUES (
        '{name}',
        '{pd}',
        '{category}',
        '{type_}',
        '{subtype}',
        '{version}',
        '{instance_prefix}',
        {instance_pi_sql},
        '{json_addl}'::jsonb,
        {json_addl_schema_sql},
        '{bstatus}',
        {is_singleton},
        FALSE
    )
    ON CONFLICT (category, type, subtype, version)
    DO NOTHING
    RETURNING 1;
    """
    return _run_psql(env, sql=sql)


def _create_default_admin(env: Environment, insecure_dev_defaults: bool) -> bool:
    """Create default actor-backed tapdb_admin user for development flows."""
    if not insecure_dev_defaults:
        console.print(
            "  [dim]○[/dim] Skipping default admin"
            " creation (use --insecure-dev-defaults)"
        )
        return False
    if env == Environment.prod:
        console.print("  [red]✗[/red] Refusing to create default admin in prod")
        return False

    from daylily_tapdb.cli.user import _hash_password
    from daylily_tapdb.user_store import create_or_get

    cfg = _get_db_config(env)
    engine_type = (cfg.get("engine_type") or "local").strip().lower()
    iam_auth = (cfg.get("iam_auth") or "true").strip().lower() in (
        "true",
        "1",
        "yes",
        "on",
    )
    region = (cfg.get("region") or "us-west-2").strip()
    db_pass = cfg.get("password") or None

    try:
        with TAPDBConnection(
            db_hostname=f"{cfg['host']}:{cfg['port']}",
            db_user=cfg["user"],
            db_pass=db_pass,
            db_name=cfg["database"],
            engine_type=engine_type,
            region=region,
            iam_auth=iam_auth,
            app_username="tapdb_admin",
        ) as conn:
            with conn.session_scope(commit=True) as session:
                user, created = create_or_get(
                    session,
                    login_identifier="tapdb_admin",
                    email="tapdb_admin",
                    display_name="TAPDB Administrator",
                    role="admin",
                    is_active=True,
                    require_password_change=True,
                    password_hash=_hash_password("passw0rd"),
                    cognito_username="tapdb_admin",
                )
        if created:
            console.print("  [green]✓[/green] Created admin user: tapdb_admin")
            return True
        console.print(
            f"  [green]✓[/green] Admin user already exists ({user.username})"
        )
        return False
    except Exception as e:
        console.print(f"  [red]✗[/red] Failed to create admin user: {e}")
        return False


@data_app.command("seed")
def db_seed(
    env: Environment = typer.Argument(..., help="Target environment"),
    config_path: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to config directory"
    ),
    include_workflow: bool = typer.Option(
        False,
        "--include-workflow",
        "-w",
        help="Include optional non-core templates if present in config",
    ),
    skip_existing: bool = typer.Option(
        True,
        "--skip-existing/--overwrite",
        help="Skip existing templates (overwrite uses upsert)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be seeded without making changes"
    ),
):
    """Seed TAPDB with template definitions from config files.

    By default, seeds only CORE templates (generic + actor templates, including
    generic/external_object_link).

    --include-workflow is retained for compatibility and includes optional
    non-core template packs when present in the provided config directory.
    """
    cfg = _get_db_config(env)

    mode = "core + optional packs" if include_workflow else "core only"
    console.print(
        f"\n[bold cyan]━━━ Seed TAPDB Templates ({env.value}) ━━━[/bold cyan]"
    )
    console.print(f"  Mode: {mode}")
    console.print(f"  Core categories: {', '.join(sorted(CORE_CATEGORIES))}")
    if include_workflow and OPTIONAL_CATEGORIES:
        console.print(
            f"  Optional categories: {', '.join(sorted(OPTIONAL_CATEGORIES))}"
        )

    # Resolve config directories (always include TAPDB core config first)
    try:
        seed_config_dirs = _resolve_seed_config_dirs(config_path)
    except FileNotFoundError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    console.print("[green]✓[/green] Seed config directories:")
    for directory in seed_config_dirs:
        console.print(f"  - {directory}")

    # Check database and schema exist
    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        console.print(f"  Create with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    if not _schema_exists(env):
        console.print("[red]✗[/red] TAPDB schema not found")
        console.print(
            f"  Initialize with: [cyan]tapdb db schema apply {env.value}[/cyan]"
        )
        raise typer.Exit(1)

    # Load templates
    console.print("[yellow]►[/yellow] Loading template configurations...")
    templates = _load_template_configs(
        seed_config_dirs, include_optional=include_workflow
    )

    if not templates:
        console.print("[yellow]⚠[/yellow] No templates found in configured seed directories")
        return

    duplicates = _find_duplicate_template_keys(templates)
    if duplicates:
        console.print(
            "[red]✗[/red] Duplicate template keys detected across seed configs. "
            "Aborting to prevent clashing templates:"
        )
        for key, sources in sorted(duplicates.items()):
            console.print(f"  • {key}")
            for source in sources:
                console.print(f"      - {source}")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Found {len(templates)} template(s)")

    # Ensure per-prefix sequences exist + are initialized safely (Phase 1)
    prefixes = sorted(
        {_normalize_instance_prefix(t.get("instance_prefix", "GX")) for t in templates}
    )
    console.print(
        f"[yellow]►[/yellow] Ensuring {len(prefixes)} instance-prefix sequence(s)..."
    )
    for p in prefixes:
        if dry_run:
            console.print(f"  [dim]○[/dim] would ensure {p.lower()}_instance_seq")
        else:
            _ensure_instance_prefix_sequence(env, p)

    # Group by category for display
    by_type = {}
    for t in templates:
        st = t.get("category", "unknown")
        by_type.setdefault(st, []).append(t)

    console.print("\n[bold]Templates by category:[/bold]")
    for st, tlist in sorted(by_type.items()):
        console.print(f"  {st}: {len(tlist)}")

    if dry_run:
        console.print("\n[bold]Templates to seed:[/bold]")
        for t in templates:
            console.print(f"  • {_template_code(t)} ({t.get('name', '')})")
        console.print("\n[dim]Dry run - no changes made.[/dim]")
        return

    # Seed templates
    console.print("\n[yellow]►[/yellow] Seeding templates...")

    inserted = 0
    updated = 0
    skipped = 0
    failed = 0

    for template in templates:
        code = _template_code(template)
        overwrite = not skip_existing
        success, output = _upsert_template(env, template, overwrite=overwrite)

        if success:
            out = (output or "").strip().lower()
            if overwrite:
                # returns 't' if inserted, 'f' if updated
                if out == "t":
                    console.print(f"  [green]✓[/green] {code} [dim](inserted)[/dim]")
                    inserted += 1
                else:
                    console.print(f"  [green]✓[/green] {code} [dim](updated)[/dim]")
                    updated += 1
            else:
                if out == "1":
                    console.print(f"  [green]✓[/green] {code} [dim](inserted)[/dim]")
                    inserted += 1
                else:
                    console.print(f"  [dim]○[/dim] {code} [dim](exists, skipped)[/dim]")
                    skipped += 1
        else:
            console.print(f"  [red]✗[/red] {code}")
            console.print(f"      Error: {output[:100]}")
            failed += 1

    # Summary
    console.print("\n[bold]Seed Summary:[/bold]")
    console.print(f"  [green]Inserted:[/green] {inserted}")
    if updated:
        console.print(f"  [yellow]Updated:[/yellow]  {updated}")
    console.print(f"  [dim]Skipped:[/dim]  {skipped}")
    if failed > 0:
        console.print(f"  [red]Failed:[/red]   {failed}")

    _log_operation(
        env.value,
        "SEED",
        f"Inserted {inserted}, updated {updated}, skipped {skipped}, failed {failed}",
    )

    if failed > 0:
        raise typer.Exit(1)


@db_app.command("setup")
def db_setup(
    env: Environment = typer.Argument(..., help="Target environment"),
    force: bool = typer.Option(False, "--force", "-f", help="Reinitialize if exists"),
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
    """Full database setup: create database, apply schema, seed templates.

    By default, seeds only CORE templates (generic + actor templates, including
    generic/external_object_link).

    --include-workflow is retained for compatibility and includes optional
    non-core template packs when present in the provided config directory.

    Combines: tapdb db create + tapdb db schema apply + tapdb db data seed

    For aurora environments, the database is already created by CloudFormation,
    so the "create database" step is skipped.
    """
    cfg = _get_db_config(env)
    is_aurora = cfg.get("engine_type") == "aurora"

    mode = "core + optional packs" if include_workflow else "core only"
    console.print(f"\n[bold cyan]━━━ TAPDB Full Setup ({env.value}) ━━━[/bold cyan]")
    console.print(f"  Database: {cfg['database']}")
    console.print(f"  Host:     {cfg['host']}:{cfg['port']}")
    if is_aurora:
        console.print("  Engine:   [bold yellow]Aurora PostgreSQL[/bold yellow]")
        console.print(f"  Region:   {cfg.get('region', 'us-west-2')}")
        console.print("  SSL:      verify-full (enforced)")
    console.print(f"  Seed mode: {mode}")

    # Step 1: Ensure database exists
    console.print("\n[bold]Step 1/5: Ensure Database[/bold]")
    if force and not is_aurora and _check_db_exists(env, cfg["database"]):
        console.print("  [yellow]►[/yellow] --force requested; recreating database")
        db_delete(env, force=True)
    db_create(env, owner=None)

    # Step 2: Apply schema
    console.print("\n[bold]Step 2/5: Apply Schema[/bold]")
    db_schema_apply(env, reinitialize=force)

    # Step 3: Apply migrations
    console.print("\n[bold]Step 3/5: Apply Migrations[/bold]")
    db_migrate(env, dry_run=False)

    # Step 4: Seed templates
    console.print("\n[bold]Step 4/5: Seed Templates[/bold]")
    db_seed(
        env,
        config_path=None,
        include_workflow=include_workflow,
        skip_existing=not force,
        dry_run=False,
    )

    # Step 5: Create default admin user
    console.print("\n[bold]Step 5/5: Create Admin User[/bold]")
    created_admin = _create_default_admin(
        env, insecure_dev_defaults=insecure_dev_defaults
    )

    # Summary
    console.print("\n[bold green]✓ TAPDB setup complete![/bold green]")
    console.print("\n[bold]Connection string:[/bold]")
    console.print(f"  {_get_connection_string(env)}")
    if created_admin:
        console.print("\n[bold yellow]⚠ Default admin credentials:[/bold yellow]")
        console.print("  Username: [cyan]tapdb_admin[/cyan]")
        console.print("  Password: [cyan]passw0rd[/cyan]")
        console.print("  [dim](Password change required on first login)[/dim]")

    _log_operation(env.value, "SETUP", "Full setup completed")


# Shared operation entry points used by orchestrators (e.g. bootstrap).
def create_database(env: Environment, owner: Optional[str] = None) -> None:
    db_create(env=env, owner=owner)


def delete_database(env: Environment, force: bool = False) -> None:
    db_delete(env=env, force=force)


def apply_schema(env: Environment, reinitialize: bool = False) -> None:
    db_schema_apply(env=env, reinitialize=reinitialize)


def schema_status(env: Environment) -> None:
    db_status(env=env)


def reset_schema(env: Environment, force: bool = False) -> None:
    db_nuke(env=env, force=force)


def run_migrations(env: Environment, dry_run: bool = False) -> None:
    db_migrate(env=env, dry_run=dry_run)


def seed_templates(
    env: Environment,
    config_path: Optional[Path] = None,
    include_workflow: bool = False,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> None:
    db_seed(
        env=env,
        config_path=config_path,
        include_workflow=include_workflow,
        skip_existing=skip_existing,
        dry_run=dry_run,
    )
