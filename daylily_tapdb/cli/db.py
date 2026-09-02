"""Database management commands for TAPDB CLI."""

import getpass
import json
import os
import re
import subprocess
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.parse import urlencode

import typer
from cli_core_yo import ccyo_out
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from daylily_tapdb import TAPDBConnection
from daylily_tapdb.backup.engine import sanitized_libpq_environment
from daylily_tapdb.cli.db_config import get_config_path, get_db_config
from daylily_tapdb.euid import (
    AUDIT_LOG_PREFIX,
    GENERIC_INSTANCE_LINEAGE_PREFIX,
    GENERIC_TEMPLATE_PREFIX,
)
from daylily_tapdb.governance import GovernanceContext
from daylily_tapdb.migration_identity import (
    MigrationPreflightError,
    apply_migration_preflight,
    build_migration_preflight,
    load_json_receipt,
    write_json_receipt,
)
from daylily_tapdb.schema_inventory import (
    diff_schema_inventory,
    drift_entry_counts,
    find_schema_root,
    load_expected_schema_inventory,
    load_live_schema_inventory,
    schema_asset_files,
    schema_root_candidates,
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

_MERIDIAN_PREFIX_RE = re.compile(r"^[0-9A-HJ-KMNP-TV-Z]{1,4}$")
_RESERVED_PREFIXES = {"GX", "TGX", "WX", "WSX", "XX", "AY"}
_TAPDB_CORE_OWNER = "daylily-tapdb"


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
    if not _MERIDIAN_PREFIX_RE.fullmatch(normalized):
        raise ValueError(
            f"instance_prefix must match ^[0-9A-HJ-KMNP-TV-Z]{{1,4}}$, got: {prefix!r}"
        )
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
            f"{field_name} must match ^[0-9A-HJ-KMNP-TV-Z]{{1,4}}$, got: {prefix!r}"
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
    return {
        "generic_template": GENERIC_TEMPLATE_PREFIX,
        "generic_instance_lineage": GENERIC_INSTANCE_LINEAGE_PREFIX,
        "audit_log": AUDIT_LOG_PREFIX,
    }


def _sync_identity_prefix_config(env: "Environment") -> None:
    """Persist required identity prefix config and ensure backing sequences."""
    cfg = _get_db_config(env)
    prefixes = _required_identity_prefixes(env)
    core_governance = GovernanceContext.load(
        domain_code=str(cfg["domain_code"]),
        owner_repo_name=_TAPDB_CORE_OWNER,
        domain_registry_path=str(cfg["domain_registry_path"]),
        prefix_ownership_registry_path=str(cfg["prefix_ownership_registry_path"]),
    )
    for prefix in prefixes.values():
        core_governance.require_prefix(prefix)
    domain_code = core_governance.domain_code
    owner_repo_name = str(cfg["owner_repo_name"]).strip()
    if not owner_repo_name:
        raise ValueError("owner_repo_name is required for identity prefix sync")
    values_sql = ",\n        ".join(
        "("
        f"{_quoted_sql_literal(entity)}, "
        f"{_quoted_sql_literal(domain_code)}, "
        f"{_quoted_sql_literal(owner_repo_name)}, "
        f"{_quoted_sql_literal(prefix)}"
        ")"
        for entity, prefix in prefixes.items()
    )
    sequences_sql = "\n    ".join(
        f'CREATE SEQUENCE IF NOT EXISTS "{_shared_sequence_name(prefix)}";'
        for prefix in sorted(set(prefixes.values()))
    )
    sql = f"""
    BEGIN;

    DO $tapdb$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM tapdb_identity_prefix_config AS existing
        JOIN (VALUES {values_sql})
          AS required(entity, domain_code, issuer_app_code, prefix)
          USING (entity, domain_code, issuer_app_code)
        WHERE existing.prefix IS DISTINCT FROM required.prefix
      ) THEN
        RAISE EXCEPTION
          'Existing TapDB identity prefix configuration conflicts with the required registry';
      END IF;
    END
    $tapdb$;

    INSERT INTO tapdb_identity_prefix_config(entity, domain_code, issuer_app_code, prefix)
    VALUES {values_sql}
    ON CONFLICT (entity, domain_code, issuer_app_code) DO NOTHING;

    {sequences_sql}

    COMMIT;
    """
    success, psql_out = _run_psql(env, sql=sql, connection_role="operator")
    if not success:
        raise RuntimeError(f"Failed to sync identity prefix config: {psql_out[:200]}")


def _ensure_instance_prefix_sequence(env: "Environment", prefix: str) -> None:
    """Create + initialize the per-prefix instance sequence.

    Sequence init algorithm (REFACTOR_TAPDB.md Phase 1):
    next nextval() should yield max(existing numeric suffix) + 1.
    """
    prefix = _normalize_instance_prefix(prefix)

    # Defense-in-depth: reject anything that is not a validated Meridian prefix.
    if not _MERIDIAN_PREFIX_RE.fullmatch(prefix):
        raise ValueError(f"Instance prefix must be Meridian-safe, got: {prefix!r}")

    seq_name = _shared_sequence_name(prefix)

    sql = f"""
    BEGIN;
    CREATE SEQUENCE IF NOT EXISTS "{seq_name}" CACHE 1 NO CYCLE;
    LOCK TABLE generic_template, generic_instance,
      generic_instance_lineage, audit_log IN ACCESS EXCLUSIVE MODE;

    DO $tapdb$
    DECLARE
      desired_next BIGINT;
      current_next BIGINT;
      sequence_increment BIGINT;
      sequence_maximum BIGINT;
      sequence_cycles BOOLEAN;
      sequence_cache BIGINT;
    BEGIN
      SELECT COALESCE(max(euid_seq), 0) + 1
      INTO desired_next
      FROM (
        SELECT euid_seq FROM generic_template WHERE euid_prefix = '{prefix}'
        UNION ALL
        SELECT euid_seq FROM generic_instance WHERE euid_prefix = '{prefix}'
        UNION ALL
        SELECT euid_seq FROM generic_instance_lineage WHERE euid_prefix = '{prefix}'
        UNION ALL
        SELECT euid_seq FROM audit_log WHERE euid_prefix = '{prefix}'
      ) all_euid_rows;

      SELECT
        CASE WHEN sequence_state.is_called
          THEN sequence_state.last_value + sequence_catalog.seqincrement
          ELSE sequence_state.last_value
        END,
        sequence_catalog.seqincrement,
        sequence_catalog.seqmax,
        sequence_catalog.seqcycle,
        sequence_catalog.seqcache
      INTO current_next, sequence_increment, sequence_maximum,
        sequence_cycles, sequence_cache
      FROM "{seq_name}" AS sequence_state
      CROSS JOIN pg_sequence AS sequence_catalog
      WHERE sequence_catalog.seqrelid = '"{seq_name}"'::regclass;

      IF sequence_increment <> 1 OR sequence_cycles OR sequence_cache <> 1 THEN
        RAISE EXCEPTION
          'Sequence {seq_name} has ambiguous issuance settings; expected INCREMENT 1, NO CYCLE, CACHE 1';
      END IF;
      IF current_next < desired_next THEN
        IF desired_next > sequence_maximum THEN
          RAISE EXCEPTION 'Sequence {seq_name} cannot advance without wrapping';
        END IF;
        EXECUTE 'ALTER SEQUENCE "{seq_name}" RESTART WITH ' || desired_next;
      END IF;
    END
    $tapdb$;
    COMMIT;
    """

    success, psql_out = _run_psql(env, sql=sql, connection_role="operator")
    if not success:
        raise RuntimeError(
            f"Failed to ensure sequence for prefix {prefix}: {psql_out[:200]}"
        )


def _write_migration_baseline(env: "Environment") -> None:
    """Write a migration baseline so fresh installs never re-apply prior migrations."""
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
        connection_role="operator",
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
            connection_role="operator",
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


class Environment(str, Enum):
    target = "target"


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
    """Get database configuration for the explicit target."""
    _ = env
    return get_db_config()


def _auth_for_connection_role(
    cfg: Mapping[str, Any], connection_role: str
) -> dict[str, Any]:
    """Select explicit runtime or separately authenticated operator credentials."""

    if connection_role == "runtime":
        return {
            "user": cfg["user"],
            "password": cfg.get("password") if "password" in cfg else None,
            "secret_arn": cfg.get("secret_arn") or None,
            "iam_auth": str(cfg.get("iam_auth") or "").strip().lower()
            in {"true", "1", "yes", "on"},
        }
    if connection_role != "operator":
        raise ValueError("connection_role must be 'runtime' or 'operator'")
    if cfg.get("operator_configured") is not True:
        raise RuntimeError(
            "This operation requires explicit target.operator credentials"
        )
    operator_user = str(cfg.get("operator_user") or "").strip()
    runtime_user = str(cfg.get("user") or "").strip()
    if not operator_user or operator_user == runtime_user:
        raise RuntimeError(
            "target.operator.user must be non-empty and distinct from target.user"
        )
    return {
        "user": operator_user,
        "password": cfg.get("operator_password")
        if "operator_password" in cfg
        else None,
        "secret_arn": cfg.get("operator_secret_arn") or None,
        "iam_auth": cfg.get("operator_iam_auth") is True,
    }


def _configured_schema_name(env: Environment) -> Optional[str]:
    cfg = _get_db_config(env)
    raw_schema_name = cfg.get("schema_name")
    if raw_schema_name is None:
        return None
    schema_name = str(raw_schema_name).strip()
    return schema_name or None


def _get_schema_name(env: Environment) -> str:
    """Return the configured PostgreSQL schema name for TAPDB objects."""
    schema_name = _configured_schema_name(env)
    if schema_name is None:
        raise ValueError("schema_name must be configured for TAPDB DB commands")
    return schema_name


def _get_connection_string(env: Environment, database: Optional[str] = None) -> str:
    """Build PostgreSQL connection string for display.

    Intentionally omits any password. Commands use PGPASSWORD/.pgpass for auth.
    For aurora environments, appends ``?sslmode=verify-full``.
    """
    cfg = _get_db_config(env)
    db = database or cfg["database"]
    base = f"postgresql://{cfg['user']}@{cfg['host']}:{cfg['port']}/{db}"
    if cfg.get("engine_type") == "aurora":
        query = {"sslmode": "verify-full"}
        hostaddr = str(cfg.get("hostaddr") or "").strip()
        if hostaddr:
            query["hostaddr"] = hostaddr
        return f"{base}?{urlencode(query)}"
    return base


def _resolved_target_label(cfg: dict[str, str]) -> str:
    return (
        f"{cfg.get('client_id')}/{cfg.get('database_name')}/"
        f"{cfg.get('schema_name')}@{cfg.get('database')}"
    )


def _require_destructive_confirmation(
    cfg: dict[str, str], *, operation: str, confirm_target: Optional[str]
) -> None:
    policy = str(cfg.get("destructive_operations") or "confirm_required").strip()
    target_label = _resolved_target_label(cfg)
    if policy == "blocked":
        raise RuntimeError(
            f"Destructive operation '{operation}' is blocked for target {target_label}."
        )
    if policy == "allowed":
        return
    if confirm_target != target_label:
        ccyo_out.error("\nWARNING: DESTRUCTIVE OPERATION")
        ccyo_out.print_text(f"Operation: [bold]{operation}[/bold]")
        ccyo_out.print_text(f"Target:    [bold]{target_label}[/bold]")
        ccyo_out.print_text(
            f"Rerun with [cyan]--confirm-target {target_label}[/cyan] to proceed."
        )
        raise typer.Exit(1)


def _schema_root_candidates() -> list[Path]:
    """Return ordered candidate roots for TAPDB schema assets."""
    return schema_root_candidates()


def _find_schema_root(required_subpath: Optional[Path] = None) -> Path:
    """Resolve the schema root from known candidate locations."""
    return find_schema_root(required_subpath)


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
    env: Environment,
    sql: str = None,
    file: Path = None,
    database: str = None,
    user: Optional[str] = None,
    connection_role: str = "runtime",
) -> tuple[bool, str]:
    """Run psql command and return (success, output).

    For aurora engine_type environments, delegates to
    ``AuroraSchemaDeployer.run_psql`` which enforces SSL
    (``sslmode=verify-full``) and uses IAM auth or Secrets Manager.
    """
    cfg = _get_db_config(env)
    auth = _auth_for_connection_role(cfg, connection_role)
    selected_user = user or str(auth["user"])
    db = database or cfg["database"]
    schema_name = (
        _configured_schema_name(env) if _uses_configured_database(database) else None
    )
    apply_search_path = schema_name is not None

    if cfg.get("engine_type") == "aurora":
        from daylily_tapdb.aurora.schema_deployer import AuroraSchemaDeployer

        aurora_sql = sql
        aurora_file = file
        if apply_search_path:
            context_sql = (
                _set_operator_context_sql(schema_name, cfg)
                if connection_role == "operator"
                else _set_runtime_context_sql(schema_name, cfg)
            )
            if file:
                aurora_sql = f"{context_sql};\n{file.read_text(encoding='utf-8')}"
                aurora_file = None
            elif sql:
                aurora_sql = f"{context_sql};\n{sql}"
        elif connection_role == "operator" and sql:
            aurora_sql = f"{_operator_role_assertion_sql()};\n{sql}"
        return AuroraSchemaDeployer.run_psql(
            host=cfg["host"],
            port=int(cfg["port"]),
            user=selected_user,
            database=db,
            region=cfg.get("region", "us-west-2"),
            iam_auth=bool(auth["iam_auth"]),
            secret_arn=auth["secret_arn"],
            password=auth["password"],
            hostaddr=cfg.get("hostaddr") or None,
            sql=aurora_sql,
            file=aurora_file,
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
        selected_user,
        "-d",
        db,
        "-v",
        "ON_ERROR_STOP=1",
    ]

    if apply_search_path:
        context_sql = (
            _set_operator_context_sql(schema_name, cfg)
            if connection_role == "operator"
            else _set_runtime_context_sql(schema_name, cfg)
        )
        cmd.extend(["-c", context_sql])
    elif connection_role == "operator":
        cmd.extend(["-c", _operator_role_assertion_sql()])

    if file:
        cmd.extend(["-f", str(file)])
    elif sql:
        cmd.extend(["-c", sql])

    env_vars = sanitized_libpq_environment()
    if auth["password"]:
        env_vars["PGPASSWORD"] = str(auth["password"])

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


def _quoted_sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quoted_sql_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _set_search_path_sql(schema_name: str) -> str:
    return f"SET search_path TO {_quoted_sql_ident(schema_name)}"


def _set_runtime_context_sql(schema_name: str, cfg: Mapping[str, Any]) -> str:
    domain_code = str(cfg.get("domain_code") or "").strip()
    owner_repo_name = str(cfg.get("owner_repo_name") or "").strip()
    if not domain_code or not owner_repo_name:
        raise ValueError("domain_code and owner_repo_name are required for DB commands")
    actor = f"cli:{getpass.getuser()}"
    return "; ".join(
        [
            _set_search_path_sql(schema_name),
            "SET session.current_config_identity = "
            + _quoted_sql_literal(str(cfg["config_path"])),
            "SET session.current_schema_name = " + _quoted_sql_literal(schema_name),
            "SET session.current_domain_code = " + _quoted_sql_literal(domain_code),
            "SET session.current_owner_repo_name = "
            + _quoted_sql_literal(owner_repo_name),
            "SET session.current_tenant_id = "
            + _quoted_sql_literal(str(cfg.get("tenant_id") or "")),
            "SET session.current_username = " + _quoted_sql_literal(actor),
            "SET session.allow_global_rows = "
            + _quoted_sql_literal(
                "true" if bool(cfg.get("allow_global_claims")) else "false"
            ),
        ]
    )


def _set_operator_context_sql(schema_name: str, cfg: Mapping[str, Any]) -> str:
    """Install operator context and prove the authenticated physical DB role."""

    base = _set_runtime_context_sql(schema_name, cfg)
    base = base.replace(
        "SET session.current_tenant_id = "
        + _quoted_sql_literal(str(cfg.get("tenant_id") or "")),
        "SET session.current_tenant_id = ''",
    )
    allow_setting = "true" if bool(cfg.get("allow_global_claims")) else "false"
    base = base.replace(
        "SET session.allow_global_rows = " + _quoted_sql_literal(allow_setting),
        "SET session.allow_global_rows = 'true'",
    )
    return f"{base}; {_operator_role_assertion_sql()}"


def _operator_role_assertion_sql() -> str:
    return (
        "DO $tapdb_operator$ BEGIN "
        "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = current_user "
        "AND (rolsuper OR rolbypassrls)) THEN "
        "RAISE EXCEPTION 'TapDB operator connection must authenticate as a distinct "
        "SUPERUSER or BYPASSRLS role'; END IF; END $tapdb_operator$"
    )


def _runtime_scope_binding_sql(schema_name: str, cfg: Mapping[str, Any]) -> str:
    """Create one immutable authenticated-principal scope binding.

    Re-applying an identical binding is safe.  Changing any security-relevant
    field requires an explicit operator migration instead of silently moving a
    login between tenants or targets.
    """

    runtime_user = str(cfg.get("user") or "").strip()
    config_identity = str(cfg["config_path"]).strip()
    domain_code = str(cfg.get("domain_code") or "").strip().upper()
    owner_repo_name = str(cfg.get("owner_repo_name") or "").strip().lower()
    tenant_id = str(cfg.get("tenant_id") or "").strip()
    if (
        not runtime_user
        or not config_identity
        or not domain_code
        or not owner_repo_name
    ):
        raise ValueError("runtime scope binding requires complete target identity")
    tenant_sql = "NULL" if not tenant_id else f"{_quoted_sql_literal(tenant_id)}::uuid"
    values = ", ".join(
        [
            _quoted_sql_literal(runtime_user),
            _quoted_sql_literal(config_identity),
            _quoted_sql_literal(schema_name),
            _quoted_sql_literal(domain_code),
            _quoted_sql_literal(owner_repo_name),
            tenant_sql,
            "TRUE" if bool(cfg.get("allow_global_claims")) else "FALSE",
        ]
    )
    exact = " AND ".join(
        [
            f"config_identity = {_quoted_sql_literal(config_identity)}",
            f"schema_name = {_quoted_sql_literal(schema_name)}::name",
            f"domain_code = {_quoted_sql_literal(domain_code)}",
            f"issuer_app_code = {_quoted_sql_literal(owner_repo_name)}",
            f"tenant_id IS NOT DISTINCT FROM {tenant_sql}",
            "allow_global_rows IS "
            + ("TRUE" if bool(cfg.get("allow_global_claims")) else "FALSE"),
        ]
    )
    role_literal = _quoted_sql_literal(runtime_user)
    return (
        "INSERT INTO tapdb_runtime_principal_scope "
        "(role_name, config_identity, schema_name, domain_code, issuer_app_code, "
        f"tenant_id, allow_global_rows) VALUES ({values}) "
        "ON CONFLICT (role_name) DO NOTHING; "
        "DO $tapdb_scope_binding$ BEGIN "
        "IF NOT EXISTS (SELECT 1 FROM tapdb_runtime_principal_scope "
        f"WHERE role_name = {role_literal}::name AND {exact}) THEN "
        "RAISE EXCEPTION 'TapDB runtime principal scope binding conflicts with "
        "the configured target'; END IF; END $tapdb_scope_binding$"
    )


def _with_schema_search_path(
    schema_name: str, sql: str, *, cfg: Optional[Mapping[str, Any]] = None
) -> str:
    setup = (
        _set_runtime_context_sql(schema_name, cfg)
        if cfg is not None
        else _set_search_path_sql(schema_name)
    )
    return f"{setup};\n{sql}"


def _read_file_with_schema_search_path(
    schema_name: str,
    file: Path,
    *,
    cfg: Optional[Mapping[str, Any]] = None,
) -> str:
    return _with_schema_search_path(
        schema_name, file.read_text(encoding="utf-8"), cfg=cfg
    )


def _uses_configured_database(database: Optional[str]) -> bool:
    return database is None


def _ensure_local_role(env: Environment, role_name: str) -> None:
    cfg = _get_db_config(env)
    if cfg.get("engine_type") != "local":
        return

    requested_role = str(role_name or "").strip()
    if not requested_role:
        return

    password = str(cfg.get("password") or "")
    password_clause = " PASSWORD " + _quoted_sql_literal(password) if password else ""
    role_literal = _quoted_sql_literal(requested_role)
    create_sql = (
        "DO $tapdb_roles$ DECLARE target_super BOOLEAN; target_bypass BOOLEAN; "
        "target_createdb BOOLEAN; target_createrole BOOLEAN; "
        "target_replication BOOLEAN; BEGIN "
        "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = current_user "
        "AND (rolsuper OR rolbypassrls)) THEN "
        "RAISE EXCEPTION 'TapDB operator role must be SUPERUSER or BYPASSRLS'; "
        "END IF; "
        f"SELECT rolsuper, rolbypassrls, rolcreatedb, rolcreaterole, rolreplication "
        "INTO target_super, target_bypass, target_createdb, target_createrole, "
        "target_replication "
        f"FROM pg_roles WHERE rolname = {role_literal}; "
        "IF NOT FOUND THEN "
        f"CREATE ROLE {_quoted_sql_ident(requested_role)} LOGIN NOSUPERUSER "
        f"NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION{password_clause}; "
        "ELSIF target_super OR target_bypass OR target_createdb "
        "OR target_createrole OR target_replication THEN "
        "RAISE EXCEPTION 'TapDB runtime role has forbidden PostgreSQL privileges'; "
        "END IF; END $tapdb_roles$;"
    )
    create_ok, create_out = _run_psql(
        env,
        sql=create_sql,
        database="postgres",
        connection_role="operator",
    )
    if not create_ok:
        raise RuntimeError(
            f"Failed to establish safe local PostgreSQL runtime role "
            f"{requested_role!r}: {create_out}"
        )


def _check_db_exists(
    env: Environment, database: str, *, connection_role: str = "runtime"
) -> bool:
    """Check if database exists."""
    _get_db_config(env)
    success, psql_out = _run_psql(
        env,
        sql=f"SELECT 1 FROM pg_database WHERE datname = '{database}'",
        database="postgres",
        connection_role=connection_role,
    )
    return success and psql_out.strip() == "1"


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
    _get_schema_name(env)
    tables = [
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
        "tapdb_identity_prefix_config",
    ]
    counts = {}
    for table in tables:
        success, psql_out = _run_psql(env, sql=f"SELECT COUNT(*) FROM {table}")
        if success:
            try:
                counts[table] = _parse_single_int(psql_out)
            except ValueError:
                counts[table] = "?"
        else:
            counts[table] = None
    return counts


def _schema_exists(env: Environment) -> bool:
    """Check if TAPDB schema exists in database."""
    schema_name = _get_schema_name(env)
    success, psql_out = _run_psql(
        env,
        sql=(
            "SELECT COUNT(*) FROM information_schema.tables"
            f" WHERE table_schema = {_quoted_sql_literal(schema_name)}"
            " AND table_name = 'generic_template'"
        ),
    )
    if not success:
        return False
    try:
        return _parse_single_int(psql_out) > 0
    except ValueError:
        return False


def _ensure_schema_exists(env: Environment) -> None:
    """Create the configured PostgreSQL schema when it is absent."""
    schema_name = _get_schema_name(env)
    success, psql_out = _run_psql(
        env,
        sql=f"CREATE SCHEMA IF NOT EXISTS {_quoted_sql_ident(schema_name)}",
        connection_role="operator",
    )
    if not success:
        raise RuntimeError(f"Failed to create schema {schema_name!r}: {psql_out}")


def _run_schema_drift_check(
    env: Environment,
    *,
    strict: bool,
) -> tuple[dict[str, Any], bool]:
    """Build drift payload for CLI output."""
    cfg = _get_db_config(env)
    schema_name = _get_schema_name(env)
    schema_root = _find_schema_root(required_subpath=Path("tapdb_schema.sql"))
    asset_paths = schema_asset_files(schema_root)
    identity_prefixes = _required_identity_prefixes(env)
    dynamic_sequence = _shared_sequence_name(identity_prefixes["generic_template"])

    expected = load_expected_schema_inventory(
        asset_paths,
        dynamic_sequence_name=dynamic_sequence,
    )
    with _tapdb_connection_for_env(
        env,
        app_username="tapdb_schema_drift_check",
        connection_role="operator",
    ) as conn:
        with conn.session_scope(commit=False) as session:
            live = load_live_schema_inventory(session, schema_name=schema_name)

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
        "expected": drift_result.expected.counts(),
        "live": drift_result.live.counts(),
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
        ccyo_out.error(f"{exc}")
        ccyo_out.print_text(
            "  Example: [cyan]tapdb --client-id atlas --database-name app "
            "--config ~/.config/tapdb/atlas/app/tapdb-config.yaml db create[/cyan]"
        )
        raise typer.Exit(1) from exc


@db_app.command("create")
def db_create(
    owner: Optional[str] = typer.Option(
        None,
        "--owner",
        "-o",
        help="Database owner (must be the configured target.operator.user)",
    ),
):
    """Create the TAPDB database for the explicit target."""
    env = Environment.target
    cfg = _get_db_config(env)
    db_name = cfg["database"]
    operator_user = str(_auth_for_connection_role(cfg, "operator")["user"])
    db_owner = str(owner or operator_user).strip()
    if db_owner != operator_user:
        ccyo_out.error("Database owner must be the configured target.operator.user")
        raise typer.Exit(1)

    ccyo_out.print_text(
        "\n[bold cyan]━━━ Create TAPDB Database (explicit target) ━━━[/bold cyan]"
    )
    ccyo_out.print_text(f"  Host:     {cfg['host']}:{cfg['port']}")
    ccyo_out.print_text(f"  Database: {db_name}")
    ccyo_out.print_text(f"  Owner:    {db_owner}")

    try:
        _ensure_local_role(env, cfg["user"])
    except RuntimeError as exc:
        ccyo_out.error(f"{exc}")
        raise typer.Exit(1) from exc

    ok, out = _run_psql(
        env, sql="SELECT 1", database="postgres", connection_role="operator"
    )
    if not ok:
        ccyo_out.error("Cannot connect to PostgreSQL for this environment")
        ccyo_out.print_text(f"  {out}")
        raise typer.Exit(1)

    if _check_db_exists(env, db_name, connection_role="operator"):
        ccyo_out.warning(f"Database '{db_name}' already exists")
        return

    ccyo_out.warning(f"► Creating database '{db_name}'...")
    sql = f'CREATE DATABASE "{db_name}" OWNER "{db_owner}"'
    success, psql_out = _run_psql(
        env, sql=sql, database="postgres", connection_role="operator"
    )
    if not success:
        ccyo_out.error("Failed to create database")
        ccyo_out.print_text(f"  {psql_out}")
        raise typer.Exit(1)

    hardening_sql = (
        f"REVOKE CREATE ON DATABASE {_quoted_sql_ident(db_name)} FROM PUBLIC; "
        f"GRANT CONNECT ON DATABASE {_quoted_sql_ident(db_name)} "
        f"TO {_quoted_sql_ident(str(cfg['user']))}; "
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC"
    )
    hardened, hardening_out = _run_psql(
        env,
        sql=hardening_sql,
        connection_role="operator",
    )
    if not hardened:
        ccyo_out.error("Database created but privilege hardening failed")
        ccyo_out.print_text(f"  {hardening_out}")
        raise typer.Exit(1)

    ccyo_out.success(f"Database '{db_name}' created")
    ccyo_out.print_text("  Next: [cyan]tapdb db schema apply[/cyan]")


@db_app.command("delete")
def db_delete(
    confirm_target: Optional[str] = typer.Option(
        None,
        "--confirm-target",
        help="Required target label for destructive delete confirmation",
    ),
):
    """Delete the TAPDB database for the explicit target."""
    env = Environment.target
    cfg = _get_db_config(env)
    db_name = cfg["database"]

    ok, out = _run_psql(
        env, sql="SELECT 1", database="postgres", connection_role="operator"
    )
    if not ok:
        ccyo_out.error("Cannot connect to PostgreSQL for this environment")
        ccyo_out.print_text(f"  {out}")
        raise typer.Exit(1)

    if not _check_db_exists(env, db_name, connection_role="operator"):
        ccyo_out.warning(f"Database '{db_name}' does not exist")
        return

    _require_destructive_confirmation(
        cfg,
        operation="delete database",
        confirm_target=confirm_target,
    )

    ccyo_out.warning(f"► Deleting database '{db_name}'...")
    term_sql = f"""
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '{db_name}' AND pid <> pg_backend_pid()
    """
    _run_psql(env, sql=term_sql, database="postgres", connection_role="operator")

    success, psql_out = _run_psql(
        env,
        sql=f'DROP DATABASE "{db_name}"',
        database="postgres",
        connection_role="operator",
    )
    if not success:
        ccyo_out.error("Failed to delete database")
        ccyo_out.print_text(f"  {psql_out}")
        raise typer.Exit(1)

    ccyo_out.success(f"Database '{db_name}' deleted")


@schema_app.command("apply")
def db_schema_apply(
    reinitialize: bool = typer.Option(
        False,
        "--reinitialize",
        "-r",
        help="Re-apply idempotent schema operations and refresh existing objects.",
    ),
):
    """Apply TAPDB schema to an existing database."""
    env = Environment.target
    _ensure_dirs()
    cfg = _get_db_config(env)

    ccyo_out.print_text(
        "\n[bold cyan]━━━ Apply TAPDB Schema (explicit target) ━━━[/bold cyan]"
    )
    ccyo_out.print_text(f"  Host:     {cfg['host']}:{cfg['port']}")
    ccyo_out.print_text(f"  Database: {cfg['database']}")
    ccyo_out.print_text(f"  Schema:   {_get_schema_name(env)}")
    ccyo_out.print_text(f"  User:     {cfg['user']}")
    ccyo_out.print_text("")

    if not _check_db_exists(env, cfg["database"]):
        ccyo_out.error(f"Database '{cfg['database']}' does not exist")
        ccyo_out.print_text("  Create with: [cyan]tapdb db create[/cyan]")
        raise typer.Exit(1)

    try:
        schema_file = _find_schema_file()
        ccyo_out.success(f"Schema file: {schema_file}")
    except FileNotFoundError as e:
        ccyo_out.error(f"{e}")
        raise typer.Exit(1)

    try:
        _ensure_schema_exists(env)
    except RuntimeError as e:
        ccyo_out.error(f"{e}")
        raise typer.Exit(1)

    schema_preexisted = _schema_exists(env)
    if schema_preexisted:
        if reinitialize:
            ccyo_out.warning("► Reapplying schema to refresh existing TAPDB objects...")
        else:
            ccyo_out.warning(
                "► Schema already exists; reapplying idempotent schema to "
                "refresh functions, triggers, and tables..."
            )
    else:
        ccyo_out.warning("► Applying schema...")
    rls_file = schema_file.parent / "rls.sql"
    if not rls_file.is_file():
        ccyo_out.error(f"Required RLS schema asset not found: {rls_file}")
        raise typer.Exit(1)
    schema_name = _get_schema_name(env)
    runtime_user = str(cfg["user"])
    operator_user = str(_auth_for_connection_role(cfg, "operator")["user"])
    operator_context = "\n".join(
        [
            f"SET LOCAL search_path TO {_quoted_sql_ident(schema_name)};",
            "SELECT set_config('session.current_config_identity', "
            f"{_quoted_sql_literal(str(cfg['config_path']))}, true);",
            "SELECT set_config('session.current_schema_name', "
            f"{_quoted_sql_literal(schema_name)}, true);",
            "SELECT set_config('session.current_domain_code', "
            f"{_quoted_sql_literal(str(cfg['domain_code']))}, true);",
            "SELECT set_config('session.current_owner_repo_name', "
            f"{_quoted_sql_literal(str(cfg['owner_repo_name']))}, true);",
            "SELECT set_config('session.current_tenant_id', '', true);",
            "SELECT set_config('session.current_username', "
            "'migration:schema-apply', true);",
            "SELECT set_config('session.allow_global_rows', 'true', true);",
        ]
    )
    schema_bundle = (
        "BEGIN;\n"
        + operator_context
        + "\n"
        + schema_file.read_text(encoding="utf-8")
        + "\n"
        + rls_file.read_text(encoding="utf-8")
        + "\n"
        + _runtime_scope_binding_sql(schema_name, cfg)
        + ";\n"
        + f"GRANT CONNECT ON DATABASE {_quoted_sql_ident(str(cfg['database']))} "
        + f"TO {_quoted_sql_ident(runtime_user)};\n"
        + f"GRANT USAGE ON SCHEMA {_quoted_sql_ident(schema_name)} "
        + f"TO {_quoted_sql_ident(runtime_user)};\n"
        + "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
        + f"{_quoted_sql_ident(schema_name)} TO {_quoted_sql_ident(runtime_user)};\n"
        + "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA "
        + f"{_quoted_sql_ident(schema_name)} TO {_quoted_sql_ident(runtime_user)};\n"
        + f"ALTER DEFAULT PRIVILEGES FOR ROLE {_quoted_sql_ident(operator_user)} "
        + f"IN SCHEMA {_quoted_sql_ident(schema_name)} GRANT SELECT, INSERT, UPDATE, "
        + f"DELETE ON TABLES TO {_quoted_sql_ident(runtime_user)};\n"
        + f"ALTER DEFAULT PRIVILEGES FOR ROLE {_quoted_sql_ident(operator_user)} "
        + f"IN SCHEMA {_quoted_sql_ident(schema_name)} GRANT USAGE, SELECT, UPDATE "
        + f"ON SEQUENCES TO {_quoted_sql_ident(runtime_user)};\n"
        + "REVOKE ALL ON TABLE tapdb_runtime_principal_scope FROM "
        + f"{_quoted_sql_ident(runtime_user)};\n"
        + "\nCOMMIT;\n"
    )
    success, psql_out = _run_psql(env, sql=schema_bundle, connection_role="operator")
    if not success:
        ccyo_out.error(f"Schema apply failed:\n{psql_out}")
        _log_operation(env.value, "SCHEMA_APPLY_FAILED", psql_out[:200])
        raise typer.Exit(1)

    _log_operation(env.value, "SCHEMA_APPLY", f"Schema applied from {schema_file}")
    ccyo_out.success("Schema applied successfully")
    ccyo_out.warning("► Syncing required identity prefixes...")
    try:
        _sync_identity_prefix_config(env)
        ccyo_out.success("Identity prefixes synced")
    except (ValueError, RuntimeError) as e:
        ccyo_out.error(f"{e}")
        raise typer.Exit(1)

    if not schema_preexisted:
        try:
            _write_migration_baseline(env)
        except Exception as e:
            ccyo_out.error(f"Failed to write migration baseline: {e}")
            raise typer.Exit(1)

    ccyo_out.heading("Tables available:")
    for table in [
        "generic_template",
        "generic_instance",
        "generic_instance_lineage",
        "audit_log",
        "tapdb_identity_prefix_config",
    ]:
        ccyo_out.success(f"  {table}")


@schema_app.command("status")
def db_status():
    """Check TAPDB schema status for the explicit target."""
    env = Environment.target
    cfg = _get_db_config(env)

    ccyo_out.print_text(
        "\n[bold cyan]━━━ TAPDB Status (explicit target) ━━━[/bold cyan]"
    )

    # Check database exists
    if not _check_db_exists(env, cfg["database"]):
        ccyo_out.error(f"Database '{cfg['database']}' does not exist")
        ccyo_out.print_text("\n  Create with: [cyan]tapdb db create[/cyan]")
        raise typer.Exit(1)

    ccyo_out.success(f"Database: {cfg['database']}")
    ccyo_out.success(f"Schema: {_get_schema_name(env)}")

    # Check schema
    if not _schema_exists(env):
        ccyo_out.error("TAPDB schema not found")
        ccyo_out.print_text("\n  Initialize with: [cyan]tapdb db schema apply[/cyan]")
        raise typer.Exit(1)

    ccyo_out.success("Schema objects: installed")

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

    ccyo_out.print_text("")
    ccyo_out.print_text(table)

    # Connection info
    ccyo_out.print_text("\n[bold]Connection:[/bold]")
    ccyo_out.print_text(f"  Host: {cfg['host']}:{cfg['port']}")
    ccyo_out.print_text(f"  User: {cfg['user']}")
    if cfg.get("engine_type") == "aurora":
        ccyo_out.warning("  Engine: Aurora PostgreSQL")
        ccyo_out.print_text(f"  Region: {cfg.get('region', 'us-west-2')}")
        ccyo_out.print_text("  SSL:    verify-full (enforced)")
        iam = cfg.get("iam_auth", "true").lower() in ("true", "1", "yes")
        ccyo_out.print_text(f"  Auth:   {'IAM' if iam else 'password'}")
    ccyo_out.print_text(f"  URL:  [dim]{_get_connection_string(env)}[/dim]")


@schema_app.command("drift-check")
def db_schema_drift_check(
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
    env = Environment.target
    cfg = _get_db_config(env)
    if not _check_db_exists(env, cfg["database"]):
        message = f"Database '{cfg['database']}' does not exist"
        if json_output:
            ccyo_out.print_text(
                json.dumps(
                    {
                        "status": "error",
                        "target": "explicit",
                        "database": cfg["database"],
                        "schema_name": cfg["schema_name"],
                        "strict": strict,
                        "error": message,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            ccyo_out.error(f"{message}")
        raise typer.Exit(2)

    try:
        payload, has_drift = _run_schema_drift_check(env, strict=strict)
    except Exception as exc:
        message = str(exc)
        if json_output:
            ccyo_out.print_text(
                json.dumps(
                    {
                        "status": "error",
                        "target": "explicit",
                        "database": cfg["database"],
                        "schema_name": cfg["schema_name"],
                        "strict": strict,
                        "error": message,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            ccyo_out.error(f"Drift check failed: {message}")
        raise typer.Exit(2) from exc

    if json_output:
        ccyo_out.print_text(json.dumps(payload, indent=2, sort_keys=True))
        raise typer.Exit(1 if has_drift else 0)

    schema_name = payload.get("schema_name") or "(not found)"
    ccyo_out.print_text(
        "\n[bold cyan]━━━ TAPDB Schema Drift Check (explicit target) ━━━[/bold cyan]"
    )
    ccyo_out.print_text(f"  Database: {payload['database']}")
    ccyo_out.print_text(f"  Schema:   {schema_name}")
    ccyo_out.print_text(f"  Strict:   {'yes' if payload['strict'] else 'no'}")
    counts = payload["counts"]
    ccyo_out.print_text(
        f"  Counts:   expected={counts['expected']} live={counts['live']}"
    )

    if has_drift:
        ccyo_out.error("\nDrift detected")
        for section_name in ("missing", "unexpected"):
            entries = payload[section_name]
            if not any(entries.values()):
                continue
            ccyo_out.print_text(f"\n[bold]{section_name.title()}[/bold]")
            for category, values in entries.items():
                if not values:
                    continue
                ccyo_out.print_text(f"  {category} ({len(values)}):")
                for value in values:
                    ccyo_out.print_text(f"    - {value}")
        raise typer.Exit(1)

    ccyo_out.success("\nNo TAPDB schema drift detected")


@schema_app.command("reset")
def db_nuke(
    confirm_target: Optional[str] = typer.Option(
        None,
        "--confirm-target",
        help="Required target label for destructive reset confirmation",
    ),
):
    """
    Completely drop all TAPDB tables and data.

    ⚠️  DESTRUCTIVE OPERATION - This cannot be undone!
    """
    env = Environment.target
    cfg = _get_db_config(env)
    is_aurora = cfg.get("engine_type") == "aurora"

    # Get what will be deleted
    if not _check_db_exists(env, cfg["database"]):
        db_name = cfg["database"]
        ccyo_out.warning(f"Database '{db_name}' does not exist. Nothing to nuke.")
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
    ccyo_out.print_text(
        Panel(
            f"[bold red]⚠️  DESTRUCTIVE OPERATION[/bold red]\n\n"
            f"Target:      [bold]{_resolved_target_label(cfg)}[/bold]\n"
            f"Database:    [bold]{cfg['database']}[/bold]\n"
            f"Schema:      [bold]{_get_schema_name(env)}[/bold]\n"
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

    _require_destructive_confirmation(
        cfg,
        operation="reset schema",
        confirm_target=confirm_target,
    )

    ccyo_out.warning("\n► Nuking TAPDB schema...")

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
    DROP TABLE IF EXISTS outbox_event_attempt CASCADE;
    DROP TABLE IF EXISTS outbox_event CASCADE;
    DROP TABLE IF EXISTS inbox_message CASCADE;
    DROP TABLE IF EXISTS audit_log CASCADE;
    DROP TABLE IF EXISTS generic_instance_lineage CASCADE;
    DROP TABLE IF EXISTS generic_instance CASCADE;
    DROP TABLE IF EXISTS generic_template CASCADE;
    DROP TABLE IF EXISTS tapdb_identity_prefix_config CASCADE;
    DROP TABLE IF EXISTS _tapdb_migrations CASCADE;

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
    DROP FUNCTION IF EXISTS tapdb_validate_domain_code(TEXT);
    DROP FUNCTION IF EXISTS tapdb_validate_owner_repo_name(TEXT);
    DROP FUNCTION IF EXISTS tapdb_current_domain_code();
    DROP FUNCTION IF EXISTS tapdb_current_owner_repo_name();
    DROP FUNCTION IF EXISTS tapdb_validate_sandbox_prefix(TEXT);
    DROP FUNCTION IF EXISTS tapdb_current_sandbox_prefix();
    DROP FUNCTION IF EXISTS meridian_generate_euid(TEXT, BIGINT, TEXT);
    DROP FUNCTION IF EXISTS meridian_generate_euid(TEXT, BIGINT);
    DROP FUNCTION IF EXISTS meridian_euid_domain_code(TEXT);
    DROP FUNCTION IF EXISTS meridian_euid_sandbox_prefix(TEXT);
    DROP FUNCTION IF EXISTS meridian_euid_prefix(TEXT);
    DROP FUNCTION IF EXISTS meridian_euid_seq_from_euid(TEXT);
    DROP FUNCTION IF EXISTS crockford_base32_decode(TEXT);
    DROP FUNCTION IF EXISTS soft_delete_row();
    DROP FUNCTION IF EXISTS record_update();
    DROP FUNCTION IF EXISTS record_insert();
    DROP FUNCTION IF EXISTS update_modified_dt();
    """

    success, psql_out = _run_psql(env, sql=drop_sql)

    if success:
        _log_operation(env.value, "NUKE", f"Deleted {total_rows} rows from all tables")
        ccyo_out.success("TAPDB schema nuked successfully")
        ccyo_out.print_text("\n  Recreate with: [cyan]tapdb db schema apply[/cyan]")
    else:
        ccyo_out.error(f"Nuke failed:\n{psql_out}")
        _log_operation(env.value, "NUKE_FAILED", psql_out[:200])
        raise typer.Exit(1)


@schema_app.command("migrate")
def db_migrate(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Capture preflight evidence without changes"
    ),
    apply: bool = typer.Option(
        False, "--apply", help="Apply exactly a previously captured preflight"
    ),
    receipt: Optional[Path] = typer.Option(
        None, "--receipt", help="Absolute, new JSON receipt output path"
    ),
    preflight_receipt: Optional[Path] = typer.Option(
        None,
        "--preflight-receipt",
        help="Absolute dry-run receipt required by --apply",
    ),
):
    """Preflight or transactionally apply identity-preserving migrations."""
    env = Environment.target
    cfg = _get_db_config(env)

    ccyo_out.print_text(
        "\n[bold cyan]━━━ Migrate TAPDB Schema (explicit target) ━━━[/bold cyan]"
    )

    # Check database and schema exist
    if not _check_db_exists(env, cfg["database"]):
        ccyo_out.error(f"Database '{cfg['database']}' does not exist")
        raise SystemExit(1)

    if not _schema_exists(env):
        ccyo_out.error("TAPDB schema not found. Use 'tapdb db schema apply' first.")
        raise SystemExit(1)

    if dry_run and apply:
        ccyo_out.error("Choose exactly one of --dry-run or --apply")
        raise SystemExit(2)
    effective_apply = bool(apply)
    if receipt is None or not receipt.is_absolute():
        ccyo_out.error("--receipt must be an absolute, new JSON file path")
        raise SystemExit(2)
    if receipt.exists():
        ccyo_out.error(f"Receipt path already exists: {receipt}")
        raise SystemExit(2)
    if effective_apply and (
        preflight_receipt is None or not preflight_receipt.is_absolute()
    ):
        ccyo_out.error("--apply requires an absolute --preflight-receipt path")
        raise SystemExit(2)
    if not effective_apply and preflight_receipt is not None:
        ccyo_out.error("--preflight-receipt is valid only with --apply")
        raise SystemExit(2)

    # Find migration files
    default_migrations_dir = _schema_root_candidates()[0] / "migrations"
    try:
        schema_root = _find_schema_root(required_subpath=Path("migrations"))
    except FileNotFoundError:
        ccyo_out.error(f"No migrations directory found at {default_migrations_dir}")
        raise SystemExit(1)
    migrations_dir = schema_root / "migrations"

    target = {
        "engine_type": cfg["engine_type"],
        "host": cfg["host"],
        "port": cfg["port"],
        "database": cfg["database"],
        "schema_name": cfg["schema_name"],
        "config_identity": cfg["config_path"],
        "domain_code": cfg["domain_code"],
        "owner_repo_name": cfg["owner_repo_name"],
    }
    try:
        with _tapdb_connection_for_env(
            env,
            app_username="tapdb_schema_migrate",
            connection_role="operator",
        ) as tapdb_connection:
            if not effective_apply:
                with tapdb_connection.engine.connect() as connection:
                    transaction = connection.begin()
                    try:
                        payload = build_migration_preflight(
                            connection,
                            migrations_dir=migrations_dir,
                            target=target,
                        )
                    finally:
                        transaction.rollback()
                write_json_receipt(receipt, payload)
                ccyo_out.success(
                    f"Preflight captured: {len(payload['pending_migrations'])} pending"
                )
                ccyo_out.print_text(f"  Receipt: {receipt}")
                return

            approved = load_json_receipt(preflight_receipt)
            with tapdb_connection.engine.connect() as connection:
                transaction = connection.begin()
                try:
                    result = apply_migration_preflight(
                        connection,
                        migrations_dir=migrations_dir,
                        preflight=approved,
                        target=target,
                    )
                    connection.exec_driver_sql(
                        _runtime_scope_binding_sql(_get_schema_name(env), cfg)
                    )
                    transaction.commit()
                except Exception:
                    transaction.rollback()
                    raise
            write_json_receipt(receipt, result.receipt)
    except (MigrationPreflightError, OSError, ValueError) as exc:
        _log_operation(env.value, "MIGRATE_FAILED", str(exc)[:200])
        ccyo_out.error(f"Migration refused: {exc}")
        raise SystemExit(1) from exc

    applied_names = result.receipt["applied_migrations"]
    _log_operation(env.value, "MIGRATE", ",".join(applied_names) or "no-op")
    ccyo_out.success(
        "Migration applied" if applied_names else "Migration verified as a true no-op"
    )
    ccyo_out.print_text(f"  Receipt: {receipt}")


def _warn_legacy_backup_command(legacy: str, replacement: str, *, reason: str) -> None:
    """Emit a deprecation notice for a superseded backup command.

    Behaviour of the legacy command is deliberately unchanged -- scripts
    pinned to it keep working across the upgrade, and removal is a later
    major. Only the notice is new, and it names the specific shortcoming so
    the warning is actionable rather than nagging.
    """
    ccyo_out.warning(
        f"DEPRECATED: `{legacy}` is superseded by `{replacement}`, because "
        f"{reason}. The legacy behaviour is unchanged for now and will be "
        "removed in a future major release."
    )


@data_app.command("backup")
def db_backup(
    backup_path: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output file path"
    ),
    data_only: bool = typer.Option(
        False, "--data-only", help="Backup data only (no schema)"
    ),
):
    """Backup TAPDB data from the explicit target.

    DEPRECATED: superseded by ``tapdb backup create``.
    """
    env = Environment.target
    cfg = _get_db_config(env)

    _warn_legacy_backup_command(
        "tapdb db data backup",
        "tapdb backup create",
        reason=(
            "it captures only 5 of the 9 tables, no sequences, no functions "
            "or triggers, and cannot reach an aurora target"
        ),
    )

    ccyo_out.print_text(
        "\n[bold cyan]━━━ Backup TAPDB (explicit target) ━━━[/bold cyan]"
    )

    if not _check_db_exists(env, cfg["database"]):
        ccyo_out.error(f"Database '{cfg['database']}' does not exist")
        raise typer.Exit(1)

    # Generate output filename
    if backup_path is None:
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        backup_path = Path(f"tapdb_target_{timestamp}.sql")

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
        str(backup_path),
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

    env_vars = sanitized_libpq_environment()
    if cfg["password"]:
        env_vars["PGPASSWORD"] = cfg["password"]
    if cfg.get("hostaddr"):
        env_vars["PGHOSTADDR"] = str(cfg["hostaddr"])

    ccyo_out.warning("► Creating backup...")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, env=env_vars)

        if result.returncode == 0:
            file_size = backup_path.stat().st_size
            size_str = (
                f"{file_size / 1024:.1f} KB"
                if file_size > 1024
                else f"{file_size} bytes"
            )

            _log_operation(env.value, "BACKUP", str(backup_path))
            ccyo_out.success(f"Backup created: {backup_path} ({size_str})")
        else:
            ccyo_out.error(f"Backup failed:\n{result.stderr}")
            raise typer.Exit(1)
    except FileNotFoundError:
        ccyo_out.error("pg_dump not found. Please install PostgreSQL client.")
        raise typer.Exit(1)


@data_app.command("restore")
def db_restore(
    input_file: Path = typer.Option(..., "--input", "-i", help="Input backup file"),
    confirm_target: Optional[str] = typer.Option(
        None,
        "--confirm-target",
        help="Required target label for destructive restore confirmation",
    ),
):
    """Restore TAPDB data into the explicit target from a backup file.

    DEPRECATED: superseded by ``tapdb backup restore``.
    """
    env = Environment.target
    cfg = _get_db_config(env)

    _warn_legacy_backup_command(
        "tapdb db data restore",
        "tapdb backup restore",
        reason=(
            "it performs no integrity verification, no compatibility check, "
            "and no sequence reconciliation before mutating the target"
        ),
    )

    ccyo_out.print_text(
        "\n[bold cyan]━━━ Restore TAPDB (explicit target) ━━━[/bold cyan]"
    )

    if not input_file.exists():
        ccyo_out.error(f"Backup file not found: {input_file}")
        raise typer.Exit(1)

    file_size = input_file.stat().st_size
    size_str = (
        f"{file_size / 1024:.1f} KB" if file_size > 1024 else f"{file_size} bytes"
    )

    ccyo_out.print_text(f"  File:     {input_file} ({size_str})")
    ccyo_out.print_text(f"  Target:   {_resolved_target_label(cfg)}")

    _require_destructive_confirmation(
        cfg,
        operation="restore data",
        confirm_target=confirm_target,
    )

    # Ensure database exists
    if not _check_db_exists(env, cfg["database"]):
        ccyo_out.error(f"Database '{cfg['database']}' does not exist")
        ccyo_out.print_text("  Create with: [cyan]tapdb db create[/cyan]")
        raise typer.Exit(1)

    ccyo_out.warning("► Restoring from backup...")

    success, psql_out = _run_psql(env, file=input_file)

    if success:
        _log_operation(env.value, "RESTORE", str(input_file))
        ccyo_out.success("Restore completed")

        # Show counts
        counts = _get_table_counts(env)
        ccyo_out.print_text("\n[bold]Restored data:[/bold]")
        for table, count in counts.items():
            ccyo_out.print_text(f"  {table}: {count} rows")
    else:
        ccyo_out.error(f"Restore failed:\n{psql_out}")
        _log_operation(env.value, "RESTORE_FAILED", psql_out[:200])
        raise typer.Exit(1)


# Core template categories (always seeded)
CORE_CATEGORIES = {"generic", "actor", "system"}

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
        ccyo_out.error(f"{e}")
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
        ccyo_out.print_text(json.dumps(payload, indent=2, sort_keys=True))
        raise typer.Exit(1 if errors else 0)

    mode = "strict" if strict else "non-strict"
    ccyo_out.print_text(
        f"\n[bold cyan]━━━ Validate Template Config ({mode}) ━━━[/bold cyan]"
    )
    ccyo_out.print_text("  Config directories:")
    for directory in config_dirs:
        ccyo_out.print_text(f"    - [dim]{directory}[/dim]")
    ccyo_out.print_text(f"  Templates loaded: {len(templates)}")

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
        ccyo_out.print_text(table)

    if errors:
        ccyo_out.error(
            f"\nValidation failed: {len(errors)} error(s), {len(warnings)} warning(s)"
        )
        raise typer.Exit(1)
    ccyo_out.success(f"\nValidation OK: {len(warnings)} warning(s)")


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
    connection_role: str = "runtime",
) -> TAPDBConnection:
    cfg = _get_db_config(env)
    engine_type = str(cfg["engine_type"]).strip().lower()
    auth = _auth_for_connection_role(cfg, connection_role)
    region = str(cfg["region"]).strip()
    return TAPDBConnection(
        db_hostname=f"{cfg['host']}:{cfg['port']}",
        db_hostaddr=cfg.get("hostaddr") or None,
        db_user=auth["user"],
        db_pass=auth["password"],
        secret_arn=auth["secret_arn"],
        db_name=cfg["database"],
        engine_type=engine_type,
        region=region,
        iam_auth=auth["iam_auth"],
        app_username=app_username,
        domain_code=str(cfg["domain_code"]),
        owner_repo_name=str(cfg["owner_repo_name"]),
        schema_name=str(cfg["schema_name"]),
        tenant_id=str(cfg.get("tenant_id") or "") or None,
        allow_global_rows=bool(cfg.get("allow_global_claims")),
        config_identity=str(cfg["config_path"]),
        echo_sql=False,
        connection_role=connection_role,
    )


def _create_default_admin(env: Environment, insecure_dev_defaults: bool) -> bool:
    """Create default actor-backed tapdb_admin user for development flows."""
    if not insecure_dev_defaults:
        ccyo_out.print_text(
            "  Skipping default admin creation (use --insecure-dev-defaults)"
        )
        return False

    cfg = _get_db_config(env)
    if str(cfg.get("safety_tier") or "").strip().lower() == "production":
        ccyo_out.error("  Refusing to create default admin for production safety tier")
        return False

    from daylily_tapdb.cli.user import _hash_password
    from daylily_tapdb.user_store import create_or_get

    engine_type = str(cfg["engine_type"]).strip().lower()
    auth = _auth_for_connection_role(cfg, "operator")
    region = str(cfg["region"]).strip()

    try:
        with TAPDBConnection(
            db_hostname=f"{cfg['host']}:{cfg['port']}",
            db_hostaddr=cfg.get("hostaddr") or None,
            db_user=auth["user"],
            db_pass=auth["password"],
            secret_arn=auth["secret_arn"],
            db_name=cfg["database"],
            engine_type=engine_type,
            region=region,
            iam_auth=auth["iam_auth"],
            app_username="tapdb_admin",
            domain_code=str(cfg["domain_code"]),
            owner_repo_name=str(cfg["owner_repo_name"]),
            schema_name=str(cfg["schema_name"]),
            config_identity=str(cfg["config_path"]),
            echo_sql=False,
            connection_role="operator",
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
            ccyo_out.success("  Created admin user: tapdb_admin")
            return True
        ccyo_out.success(f"  Admin user already exists ({user.username})")
        return False
    except Exception as e:
        ccyo_out.error(f"  Failed to create admin user: {e}")
        return False


@data_app.command("seed")
def db_seed(
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

    --include-workflow includes optional non-core template packs when present
    in the provided config directory.
    """
    env = Environment.target
    cfg = _get_db_config(env)

    mode = "core + optional packs" if include_workflow else "core only"
    ccyo_out.print_text(
        "\n[bold cyan]━━━ Seed TAPDB Templates (explicit target) ━━━[/bold cyan]"
    )
    ccyo_out.print_text(f"  Mode: {mode}")
    ccyo_out.print_text(f"  Core categories: {', '.join(sorted(CORE_CATEGORIES))}")
    if include_workflow and OPTIONAL_CATEGORIES:
        ccyo_out.print_text(
            f"  Optional categories: {', '.join(sorted(OPTIONAL_CATEGORIES))}"
        )

    # Resolve config directories (always include TAPDB core config first)
    try:
        seed_config_dirs = _resolve_seed_config_dirs(config_path)
    except FileNotFoundError as e:
        ccyo_out.error(f"{e}")
        raise SystemExit(1)

    ccyo_out.success("Seed config directories:")
    for directory in seed_config_dirs:
        ccyo_out.print_text(f"  - {directory}")

    # Check database and schema exist
    if not _check_db_exists(env, cfg["database"]):
        ccyo_out.error(f"Database '{cfg['database']}' does not exist")
        ccyo_out.print_text("  Create with: [cyan]tapdb db create[/cyan]")
        raise SystemExit(1)

    if not _schema_exists(env):
        ccyo_out.error("TAPDB schema not found")
        ccyo_out.print_text("  Initialize with: [cyan]tapdb db schema apply[/cyan]")
        raise SystemExit(1)

    ccyo_out.warning("► Loading template configurations...")
    templates, issues = _validate_template_configs(seed_config_dirs, strict=True)
    errors = [issue for issue in issues if issue.level == "error"]
    warnings = [issue for issue in issues if issue.level == "warning"]
    if warnings:
        for issue in warnings:
            ccyo_out.warning(
                f"  {issue.message}"
                + (f" ({issue.source_file})" if issue.source_file else "")
            )
    if errors:
        ccyo_out.error("Template config validation failed:")
        for issue in errors:
            detail = issue.message
            if issue.source_file:
                detail += f" ({issue.source_file})"
            if issue.template_code:
                detail += f" [{issue.template_code}]"
            ccyo_out.print_text(f"  • {detail}")
        raise SystemExit(1)

    if not templates:
        ccyo_out.warning("No templates found in configured seed directories")
        return

    duplicates = _find_duplicate_template_keys(templates)
    if duplicates:
        ccyo_out.error(
            "Duplicate template keys detected across seed configs. "
            "Aborting to prevent clashing templates:"
        )
        for key, sources in sorted(duplicates.items()):
            ccyo_out.print_text(f"  • {key}")
            for source in sources:
                ccyo_out.print_text(f"      - {source}")
        raise SystemExit(1)

    ccyo_out.success(f"Found {len(templates)} template(s)")

    # Group by category for display
    by_type = {}
    for t in templates:
        st = t.get("category", "unknown")
        category_entries = by_type.get(st)
        if category_entries is None:
            category_entries = []
            by_type[st] = category_entries
        category_entries.append(t)

    ccyo_out.print_text("\n[bold]Templates by category:[/bold]")
    for st, tlist in sorted(by_type.items()):
        ccyo_out.print_text(f"  {st}: {len(tlist)}")

    if dry_run:
        ccyo_out.print_text("\n[bold]Templates to seed:[/bold]")
        for t in templates:
            ccyo_out.print_text(f"  • {_template_code(t)} ({t.get('name', '')})")
        ccyo_out.print_text("\n[dim]Dry run - no changes made.[/dim]")
        return

    ccyo_out.warning("\n► Seeding templates...")
    overwrite = not skip_existing
    failed = 0
    try:
        with _tapdb_connection_for_env(
            env,
            app_username="tapdb_template_seed",
            connection_role="operator",
        ) as conn:
            with conn.session_scope(commit=True) as session:
                summary = _loader_seed_templates(
                    session,
                    templates,
                    overwrite=overwrite,
                    core_config_dir=_loader_find_tapdb_core_config_dir(),
                    domain_code=str(_get_db_config(env)["domain_code"]),
                    owner_repo_name=str(_get_db_config(env)["owner_repo_name"]),
                    domain_registry_path=str(
                        _get_db_config(env)["domain_registry_path"]
                    ),
                    prefix_registry_path=str(
                        _get_db_config(env)["prefix_ownership_registry_path"]
                    ),
                )
    except Exception as exc:
        ccyo_out.error(f"Template seed failed: {exc}")
        raise SystemExit(1) from exc

    # Summary
    ccyo_out.print_text("\n[bold]Seed Summary:[/bold]")
    ccyo_out.print_text(f"  [green]Inserted:[/green] {summary.inserted}")
    if summary.updated:
        ccyo_out.warning(f"  Updated:  {summary.updated}")
    ccyo_out.print_text(f"  [dim]Skipped:[/dim]  {summary.skipped}")
    ccyo_out.print_text(f"  [dim]Prefixes ensured:[/dim] {summary.prefixes_ensured}")

    _log_operation(
        env.value,
        "SEED",
        "Inserted "
        f"{summary.inserted}, updated {summary.updated}, skipped {summary.skipped}, "
        f"failed {failed}",
    )

    if failed > 0:
        raise SystemExit(1)


@db_app.command("setup")
def db_setup(
    recreate: bool = typer.Option(
        False,
        "--recreate",
        help="Delete and recreate the target database before setup",
    ),
    confirm_target: Optional[str] = typer.Option(
        None,
        "--confirm-target",
        help="Required target label when --recreate is used",
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
        help="LOCAL/SHARED ONLY: create default admin user (tapdb_admin/passw0rd)",
    ),
):
    """Full database setup: create database, apply schema, seed templates.

    By default, seeds only CORE templates (generic + actor templates, including
    generic/external_object_link).

    --include-workflow includes optional non-core template packs when present
    in the provided config directory.

    Combines: tapdb db create + tapdb db schema apply + tapdb db data seed

    For aurora environments, the database is already created by CloudFormation,
    so the "create database" step is skipped.
    """
    env = Environment.target
    cfg = _get_db_config(env)
    is_aurora = cfg.get("engine_type") == "aurora"

    mode = "core + optional packs" if include_workflow else "core only"
    ccyo_out.print_text(
        "\n[bold cyan]━━━ TAPDB Full Setup (explicit target) ━━━[/bold cyan]"
    )
    ccyo_out.print_text(f"  Database: {cfg['database']}")
    ccyo_out.print_text(f"  Host:     {cfg['host']}:{cfg['port']}")
    if is_aurora:
        ccyo_out.warning("  Engine:   Aurora PostgreSQL")
        ccyo_out.print_text(f"  Region:   {cfg.get('region', 'us-west-2')}")
        ccyo_out.print_text("  SSL:      verify-full (enforced)")
    ccyo_out.print_text(f"  Seed mode: {mode}")

    # Step 1: Ensure database exists
    ccyo_out.print_text("\n[bold]Step 1/5: Ensure Database[/bold]")
    if recreate and is_aurora:
        ccyo_out.error("--recreate is not supported for Aurora targets.")
        raise typer.Exit(1)
    if recreate and _check_db_exists(env, cfg["database"]):
        ccyo_out.warning("  ► --recreate requested; recreating database")
        db_delete(confirm_target=confirm_target)
    db_create(owner=None)

    # Step 2: Apply schema
    ccyo_out.print_text("\n[bold]Step 2/5: Apply Schema[/bold]")
    db_schema_apply(reinitialize=recreate)

    # Step 3: Fresh schema apply records the exact packaged migration baseline.
    ccyo_out.print_text("\n[bold]Step 3/5: Record Migration Baseline[/bold]")
    ccyo_out.success("  Packaged migrations baselined by schema apply")

    # Step 4: Seed templates
    ccyo_out.print_text("\n[bold]Step 4/5: Seed Templates[/bold]")
    db_seed(
        config_path=None,
        include_workflow=include_workflow,
        skip_existing=not recreate,
        dry_run=False,
    )

    # Step 5: Create default admin user
    ccyo_out.print_text("\n[bold]Step 5/5: Create Admin User[/bold]")
    created_admin = _create_default_admin(
        env, insecure_dev_defaults=insecure_dev_defaults
    )

    # Summary
    ccyo_out.success("\n✓ TAPDB setup complete!")
    ccyo_out.print_text("\n[bold]Connection string:[/bold]")
    ccyo_out.print_text(f"  {_get_connection_string(env)}")
    if created_admin:
        ccyo_out.warning("\n⚠ Default admin credentials:")
        ccyo_out.print_text("  Username: [cyan]tapdb_admin[/cyan]")
        ccyo_out.print_text("  Password: [cyan]passw0rd[/cyan]")
        ccyo_out.print_text("  [dim](Password change required on first login)[/dim]")

    _log_operation(env.value, "SETUP", "Full setup completed")


# Shared operation entry points used by orchestrators (e.g. bootstrap).
def create_database(
    env: Environment = Environment.target,
    owner: Optional[str] = None,
) -> None:
    _ = env
    db_create(owner=owner)


def delete_database(
    env: Environment = Environment.target,
    *,
    confirm_target: Optional[str] = None,
) -> None:
    _ = env
    db_delete(confirm_target=confirm_target)


def apply_schema(
    env: Environment = Environment.target,
    reinitialize: bool = False,
) -> None:
    _ = env
    db_schema_apply(reinitialize=reinitialize)


def schema_status(env: Environment = Environment.target) -> None:
    _ = env
    db_status()


def reset_schema(
    env: Environment = Environment.target,
    *,
    confirm_target: Optional[str] = None,
) -> None:
    _ = env
    db_nuke(confirm_target=confirm_target)


def run_migrations(
    env: Environment = Environment.target,
    *,
    dry_run: bool = True,
    apply: bool = False,
    receipt: Optional[Path] = None,
    preflight_receipt: Optional[Path] = None,
) -> None:
    _ = env
    if not dry_run and not apply and receipt is None and preflight_receipt is None:
        preflight_path, result_path = _next_bootstrap_migration_receipt_paths()
        db_migrate(
            dry_run=True,
            apply=False,
            receipt=preflight_path,
            preflight_receipt=None,
        )
        db_migrate(
            dry_run=False,
            apply=True,
            receipt=result_path,
            preflight_receipt=preflight_path,
        )
        return
    db_migrate(
        dry_run=dry_run,
        apply=apply,
        receipt=receipt,
        preflight_receipt=preflight_receipt,
    )


def _next_bootstrap_migration_receipt_paths() -> tuple[Path, Path]:
    """Resolve the next immutable receipt pair under this target's runtime.

    The smallest unused ordinal makes path selection deterministic for the
    current explicit target while preserving evidence from earlier bootstrap
    attempts, including a preflight whose apply failed.
    """
    runtime_dir = (
        get_config_path().resolve().parent / "runtime" / "migrations" / "receipts"
    )
    for ordinal in range(1, 1_000_000):
        stem = f"bootstrap-migrate-{ordinal:06d}"
        preflight_path = runtime_dir / f"{stem}-preflight.json"
        result_path = runtime_dir / f"{stem}-result.json"
        if not preflight_path.exists() and not result_path.exists():
            return preflight_path, result_path
    raise RuntimeError(
        f"No unused bootstrap migration receipt pair remains under {runtime_dir}"
    )


def seed_templates(
    env: Environment = Environment.target,
    config_path: Optional[Path] = None,
    include_workflow: bool = False,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> None:
    _ = env
    db_seed(
        config_path=config_path,
        include_workflow=include_workflow,
        skip_existing=skip_existing,
        dry_run=dry_run,
    )
