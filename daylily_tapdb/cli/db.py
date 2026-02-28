"""Database management commands for TAPDB CLI."""

import json
import os
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

from daylily_tapdb.cli.db_config import get_config_path, get_db_config_for_env
from daylily_tapdb.validation.instantiation_layouts import (
    format_validation_error,
    validate_instantiation_layouts,
)

console = Console()


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


def _ensure_instance_prefix_sequence(env: "Environment", prefix: str) -> None:
    """Create + initialize the per-prefix instance sequence.

    Sequence init algorithm (REFACTOR_TAPDB.md Phase 1):
    next nextval() should yield max(existing numeric suffix) + 1.
    """
    prefix = _normalize_instance_prefix(prefix)
    seq_name = f"{prefix.lower()}_instance_seq"

    sql = f"""
    CREATE SEQUENCE IF NOT EXISTS {seq_name};

    -- Initialize sequence so next nextval() yields max(existing numeric suffix) + 1.
    -- Also: never move the sequence backwards (avoid reusing previously-issued EUIs).
    WITH
      desired AS (
        SELECT
          COALESCE(
            (
              SELECT max(NULLIF(regexp_replace(euid, '[^0-9]', '', 'g'), '')::bigint)
              FROM generic_instance
              WHERE euid LIKE '{prefix}%'
            ),
            0
          ) + 1 AS next_val
      ),
      seq_state AS (
        SELECT last_value, is_called FROM {seq_name}
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
      '{seq_name}',
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


# Environment enum
class Environment(str, Enum):
    dev = "dev"
    test = "test"
    prod = "prod"


# Config directory (Phase 3: ~/.config/tapdb/*)
CONFIG_DIR = get_config_path().parent
LOG_DIR = CONFIG_DIR / "logs"


def _ensure_dirs():
    """Ensure config directories exist."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _log_operation(env: str, operation: str, details: str = ""):
    """Log database operations for audit trail."""
    _ensure_dirs()
    log_file = LOG_DIR / "db_operations.log"
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

db_app = typer.Typer(help="Database management commands")


@db_app.command("create")
def db_create(
    env: Environment = typer.Argument(..., help="Target environment"),
    create_db: bool = typer.Option(
        True, "--create-db/--no-create-db", help="Create database if not exists"
    ),
):
    """Initialize TAPDB schema in the specified environment database."""
    _ensure_dirs()
    cfg = _get_db_config(env)

    console.print(f"\n[bold cyan]━━━ Create TAPDB Schema ({env.value}) ━━━[/bold cyan]")
    console.print(f"  Host:     {cfg['host']}:{cfg['port']}")
    console.print(f"  Database: {cfg['database']}")
    console.print(f"  User:     {cfg['user']}")
    console.print()

    # Find schema file
    try:
        schema_file = _find_schema_file()
        console.print(f"[green]✓[/green] Schema file: {schema_file}")
    except FileNotFoundError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    # Create database if needed
    if create_db:
        if _check_db_exists(env, cfg["database"]):
            console.print(f"[green]✓[/green] Database '{cfg['database']}' exists")
        else:
            console.print(
                f"[yellow]►[/yellow] Creating database '{cfg['database']}'..."
            )
            success, output = _run_psql(
                env, sql=f"CREATE DATABASE {cfg['database']}", database="postgres"
            )
            if not success:
                console.print(f"[red]✗[/red] Failed to create database:\n{output}")
                raise typer.Exit(1)
            console.print("[green]✓[/green] Database created")

    # Check if schema already exists
    if _schema_exists(env):
        console.print(f"[yellow]⚠[/yellow] Schema already exists in {cfg['database']}")
        if not Confirm.ask("  Reinitialize schema?", default=False):
            console.print("[dim]Aborted.[/dim]")
            return

    # Apply schema
    console.print("[yellow]►[/yellow] Applying schema...")
    success, output = _run_psql(env, file=schema_file)

    if success:
        _log_operation(env.value, "CREATE", f"Schema applied from {schema_file}")
        console.print("[green]✓[/green] Schema created successfully")

        # Write migration baseline so fresh installs don't try to re-apply migrations.
        # (Migrations are only for evolving existing databases.)
        try:
            _write_migration_baseline(env)
        except Exception as e:
            console.print(f"[red]✗[/red] Failed to write migration baseline: {e}")
            raise typer.Exit(1)

        # Show table status
        console.print("\n[bold]Tables created:[/bold]")
        for table in [
            "generic_template",
            "generic_instance",
            "generic_instance_lineage",
            "audit_log",
        ]:
            console.print(f"  [green]✓[/green] {table}")
    else:
        console.print(f"[red]✗[/red] Schema creation failed:\n{output}")
        _log_operation(env.value, "CREATE_FAILED", output[:200])
        raise typer.Exit(1)


@db_app.command("status")
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
        console.print(f"\n  Initialize with: [cyan]tapdb db create {env.value}[/cyan]")
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


@db_app.command("nuke")
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

    -- Drop sequences
    DROP SEQUENCE IF EXISTS generic_template_seq;
    DROP SEQUENCE IF EXISTS gx_instance_seq;
    DROP SEQUENCE IF EXISTS generic_instance_seq;
    DROP SEQUENCE IF EXISTS generic_instance_lineage_seq;
    DROP SEQUENCE IF EXISTS audit_log_seq;
    DROP SEQUENCE IF EXISTS wx_instance_seq;
    DROP SEQUENCE IF EXISTS wsx_instance_seq;
    DROP SEQUENCE IF EXISTS xx_instance_seq;

    -- Drop functions
    DROP FUNCTION IF EXISTS set_generic_instance_euid();
    DROP FUNCTION IF EXISTS soft_delete_row();
    DROP FUNCTION IF EXISTS record_update();
    DROP FUNCTION IF EXISTS record_insert();
    DROP FUNCTION IF EXISTS update_modified_dt();
    """

    success, output = _run_psql(env, sql=drop_sql)

    if success:
        _log_operation(env.value, "NUKE", f"Deleted {total_rows} rows from all tables")
        console.print("[green]✓[/green] TAPDB schema nuked successfully")
        console.print(f"\n  Recreate with: [cyan]tapdb db create {env.value}[/cyan]")
    else:
        console.print(f"[red]✗[/red] Nuke failed:\n{output}")
        _log_operation(env.value, "NUKE_FAILED", output[:200])
        raise typer.Exit(1)


@db_app.command("migrate")
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
            "[red]✗[/red] TAPDB schema not found. Use 'tapdb db create' first."
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


@db_app.command("backup")
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


@db_app.command("restore")
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

# Optional template categories (seeded with --include-workflow)
OPTIONAL_CATEGORIES = {"workflow", "workflow_step", "action"}


def _load_template_configs(
    config_dir: Path, include_optional: bool = False
) -> list[dict]:
    """Load template configurations from JSON files in config directory.

    Args:
        config_dir: Path to config directory
        include_optional: If True, include workflow/workflow_step/action templates

    Returns list of template dicts ready for database insertion.
    """
    templates = []
    allowed_categories = CORE_CATEGORIES.copy()
    if include_optional:
        allowed_categories.update(OPTIONAL_CATEGORIES)

    # Walk through config subdirectories
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
                        tmpl["_source_file"] = str(json_file.relative_to(config_dir))
                        templates.append(tmpl)

            except json.JSONDecodeError as e:
                console.print(f"[yellow]⚠[/yellow] Invalid JSON in {json_file}: {e}")
            except Exception as e:
                console.print(f"[yellow]⚠[/yellow] Error reading {json_file}: {e}")

    return templates


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
    config_dir: Path, *, strict: bool
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

    if not config_dir.exists():
        return [], [
            _ConfigIssue(
                level="error", message=f"Config directory not found: {config_dir}"
            )
        ]

    # Load
    for category_dir in sorted(config_dir.iterdir()):
        if not category_dir.is_dir() or category_dir.name.startswith("_"):
            continue

        for json_file in sorted(category_dir.glob("*.json")):
            rel = str(json_file.relative_to(config_dir))
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                issues.append(
                    _ConfigIssue(
                        level="error", source_file=rel, message=f"Invalid JSON: {e}"
                    )
                )
                continue
            except Exception as e:
                issues.append(
                    _ConfigIssue(
                        level="error",
                        source_file=rel,
                        message=f"Error reading file: {e}",
                    )
                )
                continue

            if not isinstance(data, dict):
                issues.append(
                    _ConfigIssue(
                        level="error",
                        source_file=rel,
                        message="Config root must be an object/dict",
                    )
                )
                continue
            tmpl_list = data.get("templates")
            if not isinstance(tmpl_list, list):
                issues.append(
                    _ConfigIssue(
                        level="error",
                        source_file=rel,
                        message="Missing or invalid 'templates' list",
                    )
                )
                continue

            for i, tmpl in enumerate(tmpl_list):
                if not isinstance(tmpl, dict):
                    issues.append(
                        _ConfigIssue(
                            level="error",
                            source_file=rel,
                            message=(
                                f"Template[{i}] must be an"
                                f" object/dict, got"
                                f" {type(tmpl).__name__}"
                            ),
                        )
                    )
                    continue
                tmpl = dict(tmpl)
                tmpl["_source_file"] = rel
                templates.append(tmpl)

    if not templates:
        issues.append(
            _ConfigIssue(
                level="error", message=f"No templates found under: {config_dir}"
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


@db_app.command("validate-config")
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
        config_dir = config_path if config_path else _find_config_dir()
    except FileNotFoundError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    templates, issues = _validate_template_configs(config_dir, strict=strict)
    errors = [i for i in issues if i.level == "error"]
    warnings = [i for i in issues if i.level == "warning"]

    if json_output:
        payload = {
            "config_dir": str(config_dir),
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
    console.print(f"  Config directory: [dim]{config_dir}[/dim]")
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
    """Create default tapdb_admin user with password requiring change on first login.

    Returns True if user was created, False if already exists.
    """
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

    # Check if user already exists
    check_sql = "SELECT 1 FROM tapdb_user WHERE username = 'tapdb_admin'"
    success, output = _run_psql(env, sql=check_sql)

    if success and output.strip() == "1":
        console.print("  [green]✓[/green] Admin user already exists")
        return False

    # Create admin user with default password
    pw_hash = _hash_password("passw0rd")
    sql = f"""
        INSERT INTO tapdb_user (
            username, display_name, role,
            password_hash, require_password_change
        )
        VALUES (
            'tapdb_admin', 'TAPDB Administrator',
            'admin', '{pw_hash}', TRUE
        )
    """

    success, output = _run_psql(env, sql=sql)

    if success:
        console.print("  [green]✓[/green] Created admin user: tapdb_admin")
        return True
    else:
        console.print(f"  [red]✗[/red] Failed to create admin user: {output}")
        return False


@db_app.command("seed")
def db_seed(
    env: Environment = typer.Argument(..., help="Target environment"),
    config_path: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to config directory"
    ),
    include_workflow: bool = typer.Option(
        False,
        "--include-workflow",
        "-w",
        help="Include workflow/action templates (optional)",
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

    By default, seeds only CORE templates (generic, actor).
    Use --include-workflow to also seed workflow, workflow_step, and action templates.
    """
    cfg = _get_db_config(env)

    mode = "core + workflow" if include_workflow else "core only"
    console.print(
        f"\n[bold cyan]━━━ Seed TAPDB Templates ({env.value}) ━━━[/bold cyan]"
    )
    console.print(f"  Mode: {mode}")
    console.print(f"  Core categories: {', '.join(sorted(CORE_CATEGORIES))}")
    if include_workflow:
        console.print(
            f"  Optional categories: {', '.join(sorted(OPTIONAL_CATEGORIES))}"
        )

    # Find config directory
    try:
        config_dir = config_path if config_path else _find_config_dir()
        console.print(f"[green]✓[/green] Config directory: {config_dir}")
    except FileNotFoundError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    # Check database and schema exist
    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        console.print(f"  Create with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    if not _schema_exists(env):
        console.print("[red]✗[/red] TAPDB schema not found")
        console.print(f"  Initialize with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    # Load templates
    console.print("[yellow]►[/yellow] Loading template configurations...")
    templates = _load_template_configs(config_dir, include_optional=include_workflow)

    if not templates:
        console.print(f"[yellow]⚠[/yellow] No templates found in {config_dir}")
        return

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
        False, "--include-workflow", "-w", help="Include workflow/action templates"
    ),
    insecure_dev_defaults: bool = typer.Option(
        False,
        "--insecure-dev-defaults",
        help="DEV ONLY: create default admin user (tapdb_admin/passw0rd)",
    ),
):
    """Full database setup: create database, apply schema, seed templates.

    By default, seeds only CORE templates (generic, actor).
    Use --include-workflow to also seed workflow, workflow_step, and action templates.

    Combines: tapdb pg create + tapdb db create + tapdb db seed

    For aurora environments, the database is already created by CloudFormation,
    so the "create database" step is skipped.
    """
    cfg = _get_db_config(env)
    is_aurora = cfg.get("engine_type") == "aurora"

    mode = "core + workflow" if include_workflow else "core only"
    console.print(f"\n[bold cyan]━━━ TAPDB Full Setup ({env.value}) ━━━[/bold cyan]")
    console.print(f"  Database: {cfg['database']}")
    console.print(f"  Host:     {cfg['host']}:{cfg['port']}")
    if is_aurora:
        console.print("  Engine:   [bold yellow]Aurora PostgreSQL[/bold yellow]")
        console.print(f"  Region:   {cfg.get('region', 'us-west-2')}")
        console.print("  SSL:      verify-full (enforced)")
    console.print(f"  Seed mode: {mode}")

    # Step 1: Create database (skipped for aurora — CFN creates it)
    console.print("\n[bold]Step 1/3: Create Database[/bold]")
    if is_aurora:
        console.print(
            "  [green]✓[/green] Database managed by CloudFormation (skipping create)"
        )
    elif _check_db_exists(env, cfg["database"]):
        if force:
            console.print("  [yellow]►[/yellow] Database exists, recreating...")
            # Use pg module functions
            from daylily_tapdb.cli.pg import (
                _get_db_config as pg_get_db_config,
            )
            from daylily_tapdb.cli.pg import (
                _run_psql as pg_run_psql,
            )

            config = pg_get_db_config(env)
            # Terminate connections
            term_sql = f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{cfg["database"]}' AND pid <> pg_backend_pid()
            """
            pg_run_psql(term_sql, "postgres", config)
            pg_run_psql(
                f'DROP DATABASE IF EXISTS "{cfg["database"]}"', "postgres", config
            )
            pg_run_psql(f'CREATE DATABASE "{cfg["database"]}"', "postgres", config)
            console.print("  [green]✓[/green] Database recreated")
        else:
            console.print("  [green]✓[/green] Database already exists")
    else:
        success, output = _run_psql(
            env, sql=f'CREATE DATABASE "{cfg["database"]}"', database="postgres"
        )
        if success:
            console.print("  [green]✓[/green] Database created")
        else:
            console.print(f"  [red]✗[/red] Failed: {output}")
            raise typer.Exit(1)

    # Step 2: Apply schema
    console.print("\n[bold]Step 2/3: Apply Schema[/bold]")
    try:
        schema_file = _find_schema_file()
        success, output = _run_psql(env, file=schema_file)
        if success:
            console.print("  [green]✓[/green] Schema applied")
            # Ensure fresh installs never attempt to apply legacy migrations.
            _write_migration_baseline(env)
        else:
            console.print(f"  [red]✗[/red] Failed: {output[:200]}")
            raise typer.Exit(1)
    except FileNotFoundError as e:
        console.print(f"  [red]✗[/red] {e}")
        raise typer.Exit(1)

    # Step 3: Seed templates
    console.print("\n[bold]Step 3/4: Seed Templates[/bold]")
    try:
        config_dir = _find_config_dir()
        templates = _load_template_configs(
            config_dir, include_optional=include_workflow
        )

        # Ensure per-prefix instance sequences exist before
        # anything starts inserting instances.
        prefixes = sorted(
            {
                _normalize_instance_prefix(t.get("instance_prefix", "GX"))
                for t in templates
            }
        )
        for p in prefixes:
            _ensure_instance_prefix_sequence(env, p)

        inserted = 0
        skipped = 0
        for template in templates:
            ok, out = _upsert_template(env, template, overwrite=False)
            if ok and (out or "").strip() == "1":
                inserted += 1
            elif ok:
                skipped += 1

        console.print(
            f"  [green]✓[/green] Seeded {inserted} templates (skipped {skipped})"
        )
    except FileNotFoundError:
        console.print("  [yellow]⚠[/yellow] No config directory found, skipping seed")

    # Step 4: Create default admin user
    console.print("\n[bold]Step 4/4: Create Admin User[/bold]")
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
