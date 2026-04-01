"""Database management commands for TAPDB CLI."""

import json
import os
import re
import subprocess
import sysconfig
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from daylily_tapdb import TAPDBConnection
from daylily_tapdb.cli.db_config import get_config_path, get_db_config_for_env
from daylily_tapdb.schema_inventory import (
    build_expected_schema_inventory,
    diff_schema_inventory,
    drift_entry_counts,
    inventory_counts,
    load_live_schema_inventory,
    schema_asset_files,
)
from daylily_tapdb.templates import (
    ConfigIssue as _ConfigIssue,
)
from daylily_tapdb.templates import (
    find_config_dir as _loader_find_config_dir,
)
from daylily_tapdb.templates import (
    find_duplicate_template_keys as _loader_find_duplicate_template_keys,
)
from daylily_tapdb.templates import (
    find_tapdb_core_config_dir as _loader_find_tapdb_core_config_dir,
)
from daylily_tapdb.templates import (
    load_template_configs as _loader_load_template_configs,
)
from daylily_tapdb.templates import (
    resolve_seed_config_dirs as _loader_resolve_seed_config_dirs,
)
from daylily_tapdb.templates import (
    seed_templates as _loader_seed_templates,
)
from daylily_tapdb.templates import (
    validate_template_configs as _loader_validate_template_configs,
)
from daylily_tapdb.timezone_utils import utc_now

console = Console()

_MERIDIAN_PREFIX_RE = re.compile(r"^[A-HJ-KMNP-TV-Z]{2,3}$")
_RESERVED_PREFIXES = {"GX", "TGX", "WX", "WSX", "XX", "AY"}


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


def _shared_sequence_name(prefix: str) -> str:
    return f"{_normalize_instance_prefix(prefix).lower()}_instance_seq"


def _required_identity_prefixes(env: "Environment") -> dict[str, str]:
    """Return validated TapDB-managed identity prefixes for the namespace."""
    cfg = _get_db_config(env)
    core_prefix = _normalize_meridian_prefix(
        cfg.get("core_euid_prefix", ""),
        "core_euid_prefix",
    )
    audit_prefix = _normalize_meridian_prefix(
        cfg.get("audit_log_euid_prefix", ""),
        "audit_log_euid_prefix",
    )
    if audit_prefix != core_prefix:
        raise ValueError(
            "audit_log_euid_prefix must match the namespace-scoped TapDB core "
            f"prefix {core_prefix!r}"
        )
    return {
        "generic_template": core_prefix,
        "generic_instance": core_prefix,
        "generic_instance_lineage": core_prefix,
        "audit_log": audit_prefix,
    }


def _sync_identity_prefix_config(env: "Environment") -> None:
    """Persist required identity prefix config and ensure backing sequences."""
    prefixes = _required_identity_prefixes(env)
    values_sql = ",\n        ".join(
        f"('{entity}', '{prefix}')"
        for entity, prefix in prefixes.items()
    )
    sequences_sql = "\n    ".join(
        f'CREATE SEQUENCE IF NOT EXISTS "{_shared_sequence_name(prefix)}";'
        for prefix in sorted(set(prefixes.values()))
    )
    sql = f"""
    INSERT INTO tapdb_identity_prefix_config(entity, prefix)
    VALUES {values_sql}
    ON CONFLICT (entity) DO UPDATE
      SET prefix = EXCLUDED.prefix, updated_dt = NOW();

    {sequences_sql}
    """
    success, output = _run_psql(env, sql=sql)
    if not success:
        raise RuntimeError(f"Failed to sync identity prefix config: {output[:200]}")


def _ensure_instance_prefix_sequence(env: "Environment", prefix: str) -> None:
    """Create + initialize the per-prefix instance sequence.

    Sequence init algorithm (REFACTOR_TAPDB.md Phase 1):
    next nextval() should yield max(existing numeric suffix) + 1.
    """
    prefix = _normalize_instance_prefix(prefix)

    # Defense-in-depth: reject non-alpha prefixes before SQL interpolation
    if not prefix or not prefix.isalpha():
        raise ValueError(f"Instance prefix must be alphabetic, got: {prefix!r}")

    seq_name = _shared_sequence_name(prefix)

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
              FROM (
                SELECT euid_seq FROM generic_template WHERE euid_prefix = '{prefix}'
                UNION ALL
                SELECT euid_seq FROM generic_instance WHERE euid_prefix = '{prefix}'
                UNION ALL
                SELECT euid_seq FROM generic_instance_lineage WHERE euid_prefix = '{prefix}'
                UNION ALL
                SELECT euid_seq FROM audit_log WHERE euid_prefix = '{prefix}'
              ) all_euid_rows
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
    try:
        schema_root = _find_schema_root(required_subpath=Path("migrations"))
    except FileNotFoundError:
        return
    migrations_dir = schema_root / "migrations"

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


def _find_config_dir() -> Path:
    """Find the TAPDB config directory with template JSON files."""
    return _loader_find_config_dir()


def _find_tapdb_core_config_dir() -> Path:
    """Find TAPDB's built-in core template config directory."""
    return _loader_find_tapdb_core_config_dir()


def _resolve_seed_config_dirs(config_path: Optional[Path]) -> list[Path]:
    """Resolve ordered template config directories for seeding.

    Always includes TAPDB core config first, then caller-provided/auto-discovered
    client config when different.
    """
    return _loader_resolve_seed_config_dirs(config_path)


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
    timestamp = utc_now().isoformat()
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


def _schema_root_candidates() -> list[Path]:
    """Return ordered candidate roots for TAPDB schema assets."""
    current = Path(__file__).resolve()
    candidates: list[Path] = [current.parents[2] / "schema"]

    data_dir = sysconfig.get_paths().get("data")
    if data_dir:
        candidates.append(Path(data_dir) / "schema")

    candidates.append(Path.cwd() / "schema")

    seen: set[Path] = set()
    unique_candidates: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_candidates.append(candidate)

    return unique_candidates


def _find_schema_root(required_subpath: Optional[Path] = None) -> Path:
    """Resolve the schema root from known candidate locations."""
    for schema_root in _schema_root_candidates():
        if not schema_root.exists() or not schema_root.is_dir():
            continue
        if required_subpath is None or (schema_root / required_subpath).exists():
            return schema_root

    if required_subpath is None:
        raise FileNotFoundError("Cannot find TAPDB schema root.")
    raise FileNotFoundError(
        f"Cannot find TAPDB schema root containing {required_subpath.as_posix()}."
    )


def _find_schema_file() -> Path:
    """Find the TAPDB schema SQL file."""
    try:
        schema_root = _find_schema_root(required_subpath=Path("tapdb_schema.sql"))
        return schema_root / "tapdb_schema.sql"
    except FileNotFoundError:
        pass

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


def _run_schema_drift_check(
    env: Environment,
    *,
    strict: bool,
) -> tuple[dict[str, Any], bool]:
    """Build drift payload for CLI output."""
    cfg = _get_db_config(env)
    schema_root = _find_schema_root(required_subpath=Path("tapdb_schema.sql"))
    asset_paths = schema_asset_files(schema_root)
    identity_prefixes = _required_identity_prefixes(env)
    dynamic_sequence = _shared_sequence_name(identity_prefixes["generic_template"])

    expected = build_expected_schema_inventory(
        asset_paths,
        dynamic_sequence_name=dynamic_sequence,
    )
    with _tapdb_connection_for_env(
        env,
        app_username="tapdb_schema_drift_check",
    ) as conn:
        with conn.session_scope(commit=False) as session:
            live = load_live_schema_inventory(session)

    drift_result = diff_schema_inventory(
        expected,
        live,
        env=env.value,
        database=str(cfg["database"]),
        strict=strict,
        expected_asset_paths=[str(path.resolve()) for path in asset_paths],
    )
    has_drift = drift_result.has_drift
    payload = drift_result.to_payload()
    payload["counts"] = {
        "expected": inventory_counts(drift_result.expected),
        "live": inventory_counts(drift_result.live),
        "missing": drift_entry_counts(drift_result.missing),
        "unexpected": drift_entry_counts(drift_result.unexpected),
    }
    return payload, has_drift


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


@db_app.callback()
def _db_callback(ctx: typer.Context) -> None:
    """Require a TapDB namespace for DB commands except config validation."""
    if ctx.resilient_parsing:
        return

    invoked = (ctx.invoked_subcommand or "").strip().lower()
    if invoked == "config":
        return

    from daylily_tapdb.cli import _require_context

    try:
        _require_context()
    except RuntimeError as exc:
        console.print(f"[red]✗[/red] {exc}")
        console.print(
            "  Example: [cyan]tapdb --client-id atlas --database-name app "
            "db create dev[/cyan]"
        )
        raise typer.Exit(1) from exc


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
            "[yellow]►[/yellow] Syncing required identity prefixes..."
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
        "[yellow]►[/yellow] Syncing required identity prefixes..."
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


@schema_app.command("drift-check")
def db_schema_drift_check(
    env: Environment = typer.Argument(..., help="Target environment"),
    json_output: bool = typer.Option(
        False, "--json", help="Emit machine-readable JSON report"
    ),
    strict: bool = typer.Option(
        False,
        "--strict/--no-strict",
        help=(
            "If strict, fail on unexpected TapDB-owned objects in the TapDB "
            "schema in addition to missing expected objects."
        ),
    ),
):
    """Detect TAPDB schema drift against canonical TAPDB schema assets."""
    cfg = _get_db_config(env)
    if not _check_db_exists(env, cfg["database"]):
        message = f"Database '{cfg['database']}' does not exist"
        if json_output:
            typer.echo(
                json.dumps(
                    {
                        "status": "error",
                        "env": env.value,
                        "database": cfg["database"],
                        "schema_name": None,
                        "strict": strict,
                        "error": message,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            console.print(f"[red]✗[/red] {message}")
        raise typer.Exit(2)

    try:
        payload, has_drift = _run_schema_drift_check(env, strict=strict)
    except Exception as exc:
        message = str(exc)
        if json_output:
            typer.echo(
                json.dumps(
                    {
                        "status": "error",
                        "env": env.value,
                        "database": cfg["database"],
                        "schema_name": None,
                        "strict": strict,
                        "error": message,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            console.print(
                f"[red]✗[/red] Drift check failed for {env.value}: {message}"
            )
        raise typer.Exit(2) from exc

    if json_output:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        raise typer.Exit(1 if has_drift else 0)

    schema_name = payload.get("schema_name") or "(not found)"
    console.print(
        f"\n[bold cyan]━━━ TAPDB Schema Drift Check ({env.value}) ━━━[/bold cyan]"
    )
    console.print(f"  Database: {payload['database']}")
    console.print(f"  Schema:   {schema_name}")
    console.print(f"  Strict:   {'yes' if payload['strict'] else 'no'}")
    counts = payload["counts"]
    console.print(
        "  Counts:   "
        f"expected={counts['expected']} "
        f"live={counts['live']}"
    )

    if has_drift:
        console.print("\n[red]✗[/red] Drift detected")
        for section_name in ("missing", "unexpected"):
            entries = payload[section_name]
            if not any(entries.values()):
                continue
            console.print(f"\n[bold]{section_name.title()}[/bold]")
            for category, values in entries.items():
                if not values:
                    continue
                console.print(f"  {category} ({len(values)}):")
                for value in values:
                    console.print(f"    - {value}")
        raise typer.Exit(1)

    console.print("\n[green]✓[/green] No TAPDB schema drift detected")


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

    -- Drop dynamic/shared sequences
    DO $$
    DECLARE
        seq_record RECORD;
    BEGIN
        FOR seq_record IN
            SELECT sequencename
            FROM pg_sequences
            WHERE schemaname = current_schema()
              AND (
                  sequencename LIKE '%_instance_seq'
                  OR sequencename LIKE '%_audit_seq'
              )
        LOOP
            EXECUTE format('DROP SEQUENCE IF EXISTS %I CASCADE', seq_record.sequencename);
        END LOOP;
    END $$;

    -- Drop functions
    DROP FUNCTION IF EXISTS set_generic_template_euid();
    DROP FUNCTION IF EXISTS set_generic_instance_euid();
    DROP FUNCTION IF EXISTS set_generic_instance_lineage_euid();
    DROP FUNCTION IF EXISTS set_audit_log_euid();
    DROP FUNCTION IF EXISTS tapdb_get_identity_prefix(TEXT);
    DROP FUNCTION IF EXISTS tapdb_validate_meridian_prefix(TEXT);
    DROP FUNCTION IF EXISTS tapdb_validate_sandbox_prefix(TEXT);
    DROP FUNCTION IF EXISTS tapdb_current_sandbox_prefix();
    DROP FUNCTION IF EXISTS meridian_generate_euid(TEXT, BIGINT, TEXT);
    DROP FUNCTION IF EXISTS meridian_generate_euid(TEXT, BIGINT);
    DROP FUNCTION IF EXISTS meridian_euid_sandbox_prefix(TEXT);
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
            "[red]✗[/red] TAPDB schema not found. Use 'tapdb db schema apply' first."
        )
        raise typer.Exit(1)

    # Find migration files
    default_migrations_dir = _schema_root_candidates()[0] / "migrations"
    try:
        schema_root = _find_schema_root(required_subpath=Path("migrations"))
    except FileNotFoundError:
        console.print(
            f"[yellow]⚠[/yellow] No migrations directory found at {default_migrations_dir}"
        )
        console.print("[dim]Schema is up to date (no migrations to apply).[/dim]")
        return
    migrations_dir = schema_root / "migrations"

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
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
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
    """Load template configurations from one or more config directories."""
    del include_optional
    return _loader_load_template_configs(config_dirs)


def _find_duplicate_template_keys(
    templates: list[dict],
) -> dict[tuple[str, str, str, str], list[str]]:
    """Return duplicate template keys with source files for hard-fail checks."""
    return _loader_find_duplicate_template_keys(templates)


def _validate_template_configs(
    config_dirs: Path | list[Path], *, strict: bool
) -> tuple[list[dict], list[_ConfigIssue]]:
    """Load and validate template config JSON files."""
    return _loader_validate_template_configs(config_dirs, strict=strict)


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


def _tapdb_connection_for_env(
    env: Environment,
    *,
    app_username: str,
) -> TAPDBConnection:
    cfg = _get_db_config(env)
    engine_type = (cfg.get("engine_type") or "local").strip().lower()
    iam_auth = (cfg.get("iam_auth") or "true").strip().lower() in {
        "true",
        "1",
        "yes",
        "on",
    }
    region = (cfg.get("region") or "us-west-2").strip()
    db_pass = cfg.get("password") or None
    return TAPDBConnection(
        db_hostname=f"{cfg['host']}:{cfg['port']}",
        db_user=cfg["user"],
        db_pass=db_pass,
        db_name=cfg["database"],
        engine_type=engine_type,
        region=region,
        iam_auth=iam_auth,
        app_username=app_username,
    )


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
        console.print(f"  [green]✓[/green] Admin user already exists ({user.username})")
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

    console.print("[yellow]►[/yellow] Loading template configurations...")
    templates, issues = _validate_template_configs(seed_config_dirs, strict=True)
    errors = [issue for issue in issues if issue.level == "error"]
    warnings = [issue for issue in issues if issue.level == "warning"]
    if warnings:
        for issue in warnings:
            console.print(
                f"  [yellow]⚠[/yellow] {issue.message}"
                + (f" [dim]({issue.source_file})[/dim]" if issue.source_file else "")
            )
    if errors:
        console.print("[red]✗[/red] Template config validation failed:")
        for issue in errors:
            detail = issue.message
            if issue.source_file:
                detail += f" ({issue.source_file})"
            if issue.template_code:
                detail += f" [{issue.template_code}]"
            console.print(f"  • {detail}")
        raise typer.Exit(1)

    if not templates:
        console.print(
            "[yellow]⚠[/yellow] No templates found in configured seed directories"
        )
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

    console.print("\n[yellow]►[/yellow] Seeding templates...")
    overwrite = not skip_existing
    failed = 0
    try:
        with _tapdb_connection_for_env(
            env,
            app_username="tapdb_template_seed",
        ) as conn:
            with conn.session_scope(commit=True) as session:
                summary = _loader_seed_templates(
                    session,
                    templates,
                    overwrite=overwrite,
                    core_config_dir=_loader_find_tapdb_core_config_dir(),
                    core_instance_prefix=str(_get_db_config(env)["core_euid_prefix"]),
                )
    except Exception as exc:
        console.print(f"[red]✗[/red] Template seed failed: {exc}")
        raise typer.Exit(1) from exc

    # Summary
    console.print("\n[bold]Seed Summary:[/bold]")
    console.print(f"  [green]Inserted:[/green] {summary.inserted}")
    if summary.updated:
        console.print(f"  [yellow]Updated:[/yellow]  {summary.updated}")
    console.print(f"  [dim]Skipped:[/dim]  {summary.skipped}")
    console.print(f"  [dim]Prefixes ensured:[/dim] {summary.prefixes_ensured}")

    _log_operation(
        env.value,
        "SEED",
        "Inserted "
        f"{summary.inserted}, updated {summary.updated}, skipped {summary.skipped}, "
        f"failed {failed}",
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
