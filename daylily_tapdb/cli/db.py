"""Database management commands for TAPDB CLI."""

import json
import os
import subprocess
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm

console = Console()


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


# Config directory
CONFIG_DIR = Path.home() / ".tapdb"
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
    env_prefix = f"TAPDB_{env.value.upper()}_"
    
    # Check for environment-specific vars first, then fall back to defaults
    config = {
        "host": os.environ.get(f"{env_prefix}HOST", os.environ.get("PGHOST", "localhost")),
        "port": os.environ.get(f"{env_prefix}PORT", os.environ.get("PGPORT", "5432")),
        "user": os.environ.get(f"{env_prefix}USER", os.environ.get("PGUSER", os.environ.get("USER", "postgres"))),
        "password": os.environ.get(f"{env_prefix}PASSWORD", os.environ.get("PGPASSWORD", "")),
        "database": os.environ.get(f"{env_prefix}DATABASE", f"tapdb_{env.value}"),
    }
    return config


def _get_connection_string(env: Environment, database: Optional[str] = None) -> str:
    """Build PostgreSQL connection string."""
    cfg = _get_db_config(env)
    db = database or cfg["database"]
    password = f":{cfg['password']}" if cfg['password'] else ""
    return f"postgresql://{cfg['user']}{password}@{cfg['host']}:{cfg['port']}/{db}"


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


def _run_psql(env: Environment, sql: str = None, file: Path = None, database: str = None) -> tuple[bool, str]:
    """Run psql command and return (success, output)."""
    cfg = _get_db_config(env)
    db = database or cfg["database"]
    
    cmd = [
        "psql",
        "-h", cfg["host"],
        "-p", cfg["port"],
        "-U", cfg["user"],
        "-d", db,
        "-v", "ON_ERROR_STOP=1",
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
        output = result.stdout + result.stderr
        return result.returncode == 0, output
    except FileNotFoundError:
        return False, "psql not found. Please install PostgreSQL client."
    except Exception as e:
        return False, str(e)


def _check_db_exists(env: Environment, database: str) -> bool:
    """Check if database exists."""
    cfg = _get_db_config(env)
    success, output = _run_psql(
        env,
        sql=f"SELECT 1 FROM pg_database WHERE datname = '{database}'",
        database="postgres"
    )
    return success and "1" in output


def _get_table_counts(env: Environment) -> dict:
    """Get row counts for TAPDB tables."""
    tables = ["generic_template", "generic_instance", "generic_instance_lineage", "audit_log"]
    counts = {}
    for table in tables:
        success, output = _run_psql(env, sql=f"SELECT COUNT(*) FROM {table}")
        if success:
            try:
                # Parse count from psql output
                lines = [l.strip() for l in output.strip().split("\n") if l.strip() and not l.startswith("-")]
                counts[table] = int(lines[-1]) if lines else 0
            except (ValueError, IndexError):
                counts[table] = "?"
        else:
            counts[table] = None
    return counts


def _schema_exists(env: Environment) -> bool:
    """Check if TAPDB schema exists in database."""
    success, output = _run_psql(
        env,
        sql="SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'generic_template'"
    )
    return success and "1" in output


# ============================================================================
# CLI Commands
# ============================================================================

db_app = typer.Typer(help="Database management commands")


@db_app.command("create")
def db_create(
    env: Environment = typer.Argument(..., help="Target environment"),
    create_db: bool = typer.Option(True, "--create-db/--no-create-db", help="Create database if not exists"),
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
            console.print(f"[yellow]►[/yellow] Creating database '{cfg['database']}'...")
            success, output = _run_psql(
                env,
                sql=f"CREATE DATABASE {cfg['database']}",
                database="postgres"
            )
            if not success:
                console.print(f"[red]✗[/red] Failed to create database:\n{output}")
                raise typer.Exit(1)
            console.print(f"[green]✓[/green] Database created")

    # Check if schema already exists
    if _schema_exists(env):
        console.print(f"[yellow]⚠[/yellow] Schema already exists in {cfg['database']}")
        if not Confirm.ask("  Reinitialize schema?", default=False):
            console.print("[dim]Aborted.[/dim]")
            return

    # Apply schema
    console.print(f"[yellow]►[/yellow] Applying schema...")
    success, output = _run_psql(env, file=schema_file)

    if success:
        _log_operation(env.value, "CREATE", f"Schema applied from {schema_file}")
        console.print(f"[green]✓[/green] Schema created successfully")

        # Show table status
        console.print("\n[bold]Tables created:[/bold]")
        for table in ["generic_template", "generic_instance", "generic_instance_lineage", "audit_log"]:
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
        console.print(f"[red]✗[/red] TAPDB schema not found")
        console.print(f"\n  Initialize with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Schema: installed")

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
    console.print(f"\n[bold]Connection:[/bold]")
    console.print(f"  Host: {cfg['host']}:{cfg['port']}")
    console.print(f"  User: {cfg['user']}")
    console.print(f"  URL:  [dim]{_get_connection_string(env)}[/dim]")


@db_app.command("nuke")
def db_nuke(
    env: Environment = typer.Argument(..., help="Target environment"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmations (for CI/automation)"),
):
    """
    Completely drop all TAPDB tables and data.

    ⚠️  DESTRUCTIVE OPERATION - This cannot be undone!
    """
    cfg = _get_db_config(env)

    # Get what will be deleted
    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[yellow]⚠[/yellow] Database '{cfg['database']}' does not exist. Nothing to nuke.")
        return

    counts = _get_table_counts(env)
    total_rows = sum(c for c in counts.values() if isinstance(c, int))

    # Show what will be deleted
    console.print(Panel(
        f"[bold red]⚠️  DESTRUCTIVE OPERATION[/bold red]\n\n"
        f"Environment: [bold]{env.value.upper()}[/bold]\n"
        f"Database:    [bold]{cfg['database']}[/bold]\n"
        f"Host:        {cfg['host']}:{cfg['port']}\n\n"
        f"[yellow]Data to be deleted:[/yellow]\n"
        f"  • generic_template:         {counts.get('generic_template', '?')} rows\n"
        f"  • generic_instance:         {counts.get('generic_instance', '?')} rows\n"
        f"  • generic_instance_lineage: {counts.get('generic_instance_lineage', '?')} rows\n"
        f"  • audit_log:                {counts.get('audit_log', '?')} rows\n"
        f"  • All sequences, triggers, and functions\n\n"
        f"[bold]Total: {total_rows} rows[/bold]",
        title="[red]DATABASE NUKE[/red]",
        border_style="red",
    ))

    if not force:
        # Confirmation 1: Environment
        console.print(f"\n[bold]Confirmation 1/3:[/bold] You are about to nuke the [bold red]{env.value.upper()}[/bold red] database.")
        if not Confirm.ask("  Proceed?", default=False):
            console.print("[dim]Aborted.[/dim]")
            return

        # Confirmation 2: Type environment name
        console.print(f"\n[bold]Confirmation 2/3:[/bold] Type the environment name to confirm:")
        typed_env = Prompt.ask("  Environment name")
        if typed_env.lower() != env.value.lower():
            console.print(f"[red]✗[/red] Input '{typed_env}' does not match '{env.value}'. Aborted.")
            return

        # Confirmation 3: Type DELETE EVERYTHING
        console.print(f"\n[bold]Confirmation 3/3:[/bold] Type [bold red]DELETE EVERYTHING[/bold red] to proceed:")
        typed_confirm = Prompt.ask("  Confirm")
        if typed_confirm != "DELETE EVERYTHING":
            console.print(f"[red]✗[/red] Input does not match 'DELETE EVERYTHING'. Aborted.")
            return

    console.print(f"\n[yellow]►[/yellow] Nuking TAPDB schema...")

    # Drop order matters for foreign keys
    drop_sql = """
    -- Drop triggers first
    DROP TRIGGER IF EXISTS trigger_set_generic_instance_euid ON generic_instance;
    DROP TRIGGER IF EXISTS soft_delete_generic_template ON generic_template;
    DROP TRIGGER IF EXISTS soft_delete_generic_instance ON generic_instance;
    DROP TRIGGER IF EXISTS soft_delete_generic_instance_lineage ON generic_instance_lineage;
    DROP TRIGGER IF EXISTS audit_insert_generic_template ON generic_template;
    DROP TRIGGER IF EXISTS audit_insert_generic_instance ON generic_instance;
    DROP TRIGGER IF EXISTS audit_insert_generic_instance_lineage ON generic_instance_lineage;
    DROP TRIGGER IF EXISTS audit_update_generic_template ON generic_template;
    DROP TRIGGER IF EXISTS audit_update_generic_instance ON generic_instance;
    DROP TRIGGER IF EXISTS audit_update_generic_instance_lineage ON generic_instance_lineage;
    DROP TRIGGER IF EXISTS update_modified_dt_generic_template ON generic_template;
    DROP TRIGGER IF EXISTS update_modified_dt_generic_instance ON generic_instance;
    DROP TRIGGER IF EXISTS update_modified_dt_generic_instance_lineage ON generic_instance_lineage;

    -- Drop tables (order matters for FK constraints)
    DROP TABLE IF EXISTS audit_log CASCADE;
    DROP TABLE IF EXISTS generic_instance_lineage CASCADE;
    DROP TABLE IF EXISTS generic_instance CASCADE;
    DROP TABLE IF EXISTS generic_template CASCADE;

    -- Drop sequences
    DROP SEQUENCE IF EXISTS generic_template_seq;
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
        console.print(f"[green]✓[/green] TAPDB schema nuked successfully")
        console.print(f"\n  Recreate with: [cyan]tapdb db create {env.value}[/cyan]")
    else:
        console.print(f"[red]✗[/red] Nuke failed:\n{output}")
        _log_operation(env.value, "NUKE_FAILED", output[:200])
        raise typer.Exit(1)


@db_app.command("migrate")
def db_migrate(
    env: Environment = typer.Argument(..., help="Target environment"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
):
    """Apply schema migrations/updates to the specified environment."""
    cfg = _get_db_config(env)

    console.print(f"\n[bold cyan]━━━ Migrate TAPDB Schema ({env.value}) ━━━[/bold cyan]")

    # Check database and schema exist
    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        raise typer.Exit(1)

    if not _schema_exists(env):
        console.print(f"[red]✗[/red] TAPDB schema not found. Use 'tapdb db create' first.")
        raise typer.Exit(1)

    # Find migration files
    migrations_dir = Path(__file__).parent.parent.parent / "schema" / "migrations"
    if not migrations_dir.exists():
        console.print(f"[yellow]⚠[/yellow] No migrations directory found at {migrations_dir}")
        console.print("[dim]Schema is up to date (no migrations to apply).[/dim]")
        return

    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        console.print("[dim]No migration files found. Schema is up to date.[/dim]")
        return

    # Track applied migrations (store in a migrations table)
    _run_psql(env, sql="""
        CREATE TABLE IF NOT EXISTS _tapdb_migrations (
            id SERIAL PRIMARY KEY,
            filename TEXT UNIQUE NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Get already applied migrations
    success, output = _run_psql(env, sql="SELECT filename FROM _tapdb_migrations")
    applied = set(output.split()) if success else set()

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
            _run_psql(env, sql=f"INSERT INTO _tapdb_migrations (filename) VALUES ('{mf.name}')")
            console.print(f"[green]✓[/green] {mf.name} applied")
            _log_operation(env.value, "MIGRATE", mf.name)
        else:
            console.print(f"[red]✗[/red] Migration failed:\n{output}")
            _log_operation(env.value, "MIGRATE_FAILED", f"{mf.name}: {output[:100]}")
            raise typer.Exit(1)

    console.print(f"\n[green]✓[/green] All migrations applied successfully")


@db_app.command("backup")
def db_backup(
    env: Environment = typer.Argument(..., help="Target environment"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file path"),
    data_only: bool = typer.Option(False, "--data-only", help="Backup data only (no schema)"),
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
        "-h", cfg["host"],
        "-p", cfg["port"],
        "-U", cfg["user"],
        "-d", cfg["database"],
        "-f", str(output),
        "--no-owner",
        "--no-privileges",
    ]

    # Only backup TAPDB tables
    tables = ["generic_template", "generic_instance", "generic_instance_lineage", "audit_log"]
    for table in tables:
        cmd.extend(["-t", table])

    if data_only:
        cmd.append("--data-only")

    env_vars = os.environ.copy()
    if cfg["password"]:
        env_vars["PGPASSWORD"] = cfg["password"]

    console.print(f"[yellow]►[/yellow] Creating backup...")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, env=env_vars)

        if result.returncode == 0:
            file_size = output.stat().st_size
            size_str = f"{file_size / 1024:.1f} KB" if file_size > 1024 else f"{file_size} bytes"

            _log_operation(env.value, "BACKUP", str(output))
            console.print(f"[green]✓[/green] Backup created: {output} ({size_str})")
        else:
            console.print(f"[red]✗[/red] Backup failed:\n{result.stderr}")
            raise typer.Exit(1)
    except FileNotFoundError:
        console.print("[red]✗[/red] pg_dump not found. Please install PostgreSQL client.")
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
    size_str = f"{file_size / 1024:.1f} KB" if file_size > 1024 else f"{file_size} bytes"

    console.print(f"  File:     {input_file} ({size_str})")
    console.print(f"  Target:   {cfg['database']} ({env.value})")

    if not force:
        console.print(f"\n[yellow]⚠[/yellow] This will overwrite existing data in {cfg['database']}")
        if not Confirm.ask("  Proceed?", default=False):
            console.print("[dim]Aborted.[/dim]")
            return

    # Ensure database exists
    if not _check_db_exists(env, cfg["database"]):
        console.print(f"[red]✗[/red] Database '{cfg['database']}' does not exist")
        console.print(f"  Create with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    console.print(f"[yellow]►[/yellow] Restoring from backup...")

    success, output = _run_psql(env, file=input_file)

    if success:
        _log_operation(env.value, "RESTORE", str(input_file))
        console.print(f"[green]✓[/green] Restore completed")

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


def _load_template_configs(config_dir: Path, include_optional: bool = False) -> list[dict]:
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


def _template_exists(env: Environment, polymorphic_discriminator: str) -> bool:
    """Check if a template with the given discriminator already exists."""
    sql = f"SELECT 1 FROM generic_template WHERE polymorphic_discriminator = '{polymorphic_discriminator}'"
    success, output = _run_psql(env, sql=sql)
    return success and "1" in output


def _insert_template(env: Environment, template: dict) -> tuple[bool, str]:
    """Insert a template into the database. Returns (success, message)."""
    # Escape single quotes in JSON
    json_addl = json.dumps(template.get("json_addl", {})).replace("'", "''")
    json_addl_schema = json.dumps(template.get("json_addl_schema")).replace("'", "''") if template.get("json_addl_schema") else "NULL"

    # Build INSERT statement
    sql = f"""
    INSERT INTO generic_template (
        name, polymorphic_discriminator, category, type, subtype, version,
        instance_prefix, instance_polymorphic_identity, json_addl, json_addl_schema,
        bstatus, is_singleton, is_deleted
    ) VALUES (
        '{template["name"].replace("'", "''")}',
        '{template["polymorphic_discriminator"]}',
        '{template["category"]}',
        '{template["type"]}',
        '{template["subtype"]}',
        '{template["version"]}',
        '{template.get("instance_prefix", "GX")}',
        {f"'{template['instance_polymorphic_identity']}'" if template.get("instance_polymorphic_identity") else "NULL"},
        '{json_addl}'::jsonb,
        {f"'{json_addl_schema}'::jsonb" if json_addl_schema != "NULL" else "NULL"},
        '{template.get("bstatus", "active")}',
        {str(template.get("is_singleton", False)).upper()},
        FALSE
    )
    """

    return _run_psql(env, sql=sql)


def _create_default_admin(env: Environment) -> bool:
    """Create default tapdb_admin user with password requiring change on first login.

    Returns True if user was created, False if already exists.
    """
    from daylily_tapdb.cli.user import _hash_password

    # Check if user already exists
    check_sql = "SELECT 1 FROM tapdb_user WHERE username = 'tapdb_admin'"
    success, output = _run_psql(env, sql=check_sql)

    if success and "1" in output:
        console.print(f"  [green]✓[/green] Admin user already exists")
        return False

    # Create admin user with default password
    pw_hash = _hash_password("passw0rd")
    sql = f"""
        INSERT INTO tapdb_user (username, display_name, role, password_hash, require_password_change)
        VALUES ('tapdb_admin', 'TAPDB Administrator', 'admin', '{pw_hash}', TRUE)
    """

    success, output = _run_psql(env, sql=sql)

    if success:
        console.print(f"  [green]✓[/green] Created admin user: tapdb_admin")
        return True
    else:
        console.print(f"  [red]✗[/red] Failed to create admin user: {output}")
        return False


@db_app.command("seed")
def db_seed(
    env: Environment = typer.Argument(..., help="Target environment"),
    config_path: Optional[Path] = typer.Option(None, "--config", "-c", help="Path to config directory"),
    include_workflow: bool = typer.Option(False, "--include-workflow", "-w", help="Include workflow/action templates (optional)"),
    skip_existing: bool = typer.Option(True, "--skip-existing/--overwrite", help="Skip existing templates"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be seeded without making changes"),
):
    """Seed TAPDB with template definitions from config files.

    By default, seeds only CORE templates (generic, actor).
    Use --include-workflow to also seed workflow, workflow_step, and action templates.
    """
    cfg = _get_db_config(env)

    mode = "core + workflow" if include_workflow else "core only"
    console.print(f"\n[bold cyan]━━━ Seed TAPDB Templates ({env.value}) ━━━[/bold cyan]")
    console.print(f"  Mode: {mode}")
    console.print(f"  Core categories: {', '.join(sorted(CORE_CATEGORIES))}")
    if include_workflow:
        console.print(f"  Optional categories: {', '.join(sorted(OPTIONAL_CATEGORIES))}")

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
        console.print(f"[red]✗[/red] TAPDB schema not found")
        console.print(f"  Initialize with: [cyan]tapdb db create {env.value}[/cyan]")
        raise typer.Exit(1)

    # Load templates
    console.print(f"[yellow]►[/yellow] Loading template configurations...")
    templates = _load_template_configs(config_dir, include_optional=include_workflow)

    if not templates:
        console.print(f"[yellow]⚠[/yellow] No templates found in {config_dir}")
        return

    console.print(f"[green]✓[/green] Found {len(templates)} template(s)")

    # Group by category for display
    by_type = {}
    for t in templates:
        st = t.get("category", "unknown")
        by_type.setdefault(st, []).append(t)

    console.print(f"\n[bold]Templates by category:[/bold]")
    for st, tlist in sorted(by_type.items()):
        console.print(f"  {st}: {len(tlist)}")

    if dry_run:
        console.print(f"\n[bold]Templates to seed:[/bold]")
        for t in templates:
            console.print(f"  • {t['polymorphic_discriminator']} ({t['name']})")
        console.print(f"\n[dim]Dry run - no changes made.[/dim]")
        return

    # Seed templates
    console.print(f"\n[yellow]►[/yellow] Seeding templates...")

    inserted = 0
    skipped = 0
    failed = 0

    for template in templates:
        pd = template["polymorphic_discriminator"]

        # Check if exists
        if _template_exists(env, pd):
            if skip_existing:
                console.print(f"  [dim]○[/dim] {pd} [dim](exists, skipped)[/dim]")
                skipped += 1
                continue
            else:
                # Delete existing
                _run_psql(env, sql=f"DELETE FROM generic_template WHERE polymorphic_discriminator = '{pd}'")

        # Insert template
        success, output = _insert_template(env, template)

        if success:
            console.print(f"  [green]✓[/green] {pd}")
            inserted += 1
        else:
            console.print(f"  [red]✗[/red] {pd}")
            console.print(f"      Error: {output[:100]}")
            failed += 1

    # Summary
    console.print(f"\n[bold]Seed Summary:[/bold]")
    console.print(f"  [green]Inserted:[/green] {inserted}")
    console.print(f"  [dim]Skipped:[/dim]  {skipped}")
    if failed > 0:
        console.print(f"  [red]Failed:[/red]   {failed}")

    _log_operation(env.value, "SEED", f"Inserted {inserted}, skipped {skipped}, failed {failed}")

    if failed > 0:
        raise typer.Exit(1)


@db_app.command("setup")
def db_setup(
    env: Environment = typer.Argument(..., help="Target environment"),
    force: bool = typer.Option(False, "--force", "-f", help="Reinitialize if exists"),
    include_workflow: bool = typer.Option(False, "--include-workflow", "-w", help="Include workflow/action templates"),
):
    """Full database setup: create database, apply schema, seed templates.

    By default, seeds only CORE templates (generic, actor).
    Use --include-workflow to also seed workflow, workflow_step, and action templates.

    Combines: tapdb pg create + tapdb db create + tapdb db seed
    """
    cfg = _get_db_config(env)

    mode = "core + workflow" if include_workflow else "core only"
    console.print(f"\n[bold cyan]━━━ TAPDB Full Setup ({env.value}) ━━━[/bold cyan]")
    console.print(f"  Database: {cfg['database']}")
    console.print(f"  Host:     {cfg['host']}:{cfg['port']}")
    console.print(f"  Seed mode: {mode}")

    # Step 1: Create database
    console.print(f"\n[bold]Step 1/3: Create Database[/bold]")
    if _check_db_exists(env, cfg["database"]):
        if force:
            console.print(f"  [yellow]►[/yellow] Database exists, recreating...")
            # Use pg module functions
            from daylily_tapdb.cli.pg import _run_psql as pg_run_psql, _get_db_config as pg_get_db_config
            config = pg_get_db_config(env)
            # Terminate connections
            term_sql = f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{cfg["database"]}' AND pid <> pg_backend_pid()
            """
            pg_run_psql(term_sql, "postgres", config)
            pg_run_psql(f'DROP DATABASE IF EXISTS "{cfg["database"]}"', "postgres", config)
            pg_run_psql(f'CREATE DATABASE "{cfg["database"]}"', "postgres", config)
            console.print(f"  [green]✓[/green] Database recreated")
        else:
            console.print(f"  [green]✓[/green] Database already exists")
    else:
        success, output = _run_psql(env, sql=f'CREATE DATABASE "{cfg["database"]}"', database="postgres")
        if success:
            console.print(f"  [green]✓[/green] Database created")
        else:
            console.print(f"  [red]✗[/red] Failed: {output}")
            raise typer.Exit(1)

    # Step 2: Apply schema
    console.print(f"\n[bold]Step 2/3: Apply Schema[/bold]")
    try:
        schema_file = _find_schema_file()
        success, output = _run_psql(env, file=schema_file)
        if success:
            console.print(f"  [green]✓[/green] Schema applied")
        else:
            console.print(f"  [red]✗[/red] Failed: {output[:200]}")
            raise typer.Exit(1)
    except FileNotFoundError as e:
        console.print(f"  [red]✗[/red] {e}")
        raise typer.Exit(1)

    # Step 3: Seed templates
    console.print(f"\n[bold]Step 3/4: Seed Templates[/bold]")
    try:
        config_dir = _find_config_dir()
        templates = _load_template_configs(config_dir, include_optional=include_workflow)

        inserted = 0
        for template in templates:
            pd = template["polymorphic_discriminator"]
            if not _template_exists(env, pd):
                success, _ = _insert_template(env, template)
                if success:
                    inserted += 1

        console.print(f"  [green]✓[/green] Seeded {inserted} templates")
    except FileNotFoundError:
        console.print(f"  [yellow]⚠[/yellow] No config directory found, skipping seed")

    # Step 4: Create default admin user
    console.print(f"\n[bold]Step 4/4: Create Admin User[/bold]")
    _create_default_admin(env)

    # Summary
    console.print(f"\n[bold green]✓ TAPDB setup complete![/bold green]")
    console.print(f"\n[bold]Connection string:[/bold]")
    console.print(f"  {_get_connection_string(env)}")
    console.print(f"\n[bold yellow]⚠ Default admin credentials:[/bold yellow]")
    console.print(f"  Username: [cyan]tapdb_admin[/cyan]")
    console.print(f"  Password: [cyan]passw0rd[/cyan]")
    console.print(f"  [dim](Password change required on first login)[/dim]")

    _log_operation(env.value, "SETUP", "Full setup completed")
