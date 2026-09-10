"""Fail-closed PostgreSQL transaction context for scope, audit, and RLS."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text


def _exact(value: Any, field: str, *, allow_empty: bool = False) -> str:
    normalized = str(value or "")
    if normalized != normalized.strip() or (not normalized and not allow_empty):
        qualifier = "exact" if allow_empty else "exact and non-empty"
        raise ValueError(f"{field} must be {qualifier}")
    if any(ord(char) < 32 or ord(char) == 127 for char in normalized):
        raise ValueError(f"{field} must not contain control characters")
    return normalized


@dataclass(frozen=True)
class TapdbTransactionContext:
    """All security-relevant state installed together inside one transaction."""

    config_identity: str
    schema_name: str
    domain_code: str
    owner_repo_name: str
    tenant_id: str | uuid.UUID | None
    actor: str
    allow_global_rows: bool = False

    def __post_init__(self) -> None:
        _exact(self.config_identity, "config_identity")
        _exact(self.schema_name, "schema_name")
        _exact(self.domain_code, "domain_code")
        _exact(self.owner_repo_name, "owner_repo_name")
        _exact(self.actor, "actor")
        if self.tenant_id is not None:
            uuid.UUID(str(self.tenant_id))
        if not isinstance(self.allow_global_rows, bool):
            raise ValueError("allow_global_rows must be boolean")

    @property
    def tenant_setting(self) -> str:
        return "" if self.tenant_id is None else str(self.tenant_id)


def is_postgresql_session(session: Any) -> bool:
    bind = getattr(session, "bind", None)
    dialect = getattr(bind, "dialect", None) or getattr(session, "dialect", None)
    return str(getattr(dialect, "name", "") or "").strip().lower() == "postgresql"


def transaction_context_pgoptions(context: TapdbTransactionContext) -> str:
    """Render complete per-process libpq context for a database client.

    ``pg_dump`` opens its own database transaction, so SQLAlchemy's ``SET
    LOCAL`` state cannot flow into it. ``PGOPTIONS`` is the libpq-supported
    way to install the same explicit context at connection startup. Values
    are escaped for libpq's whitespace-separated option parser; ambient
    ``PGOPTIONS`` is deliberately replaced by the caller rather than merged.
    """

    def escaped(value: str) -> str:
        return value.replace("\\", "\\\\").replace(" ", "\\ ")

    settings = (
        ("search_path", context.schema_name),
        ("session.current_config_identity", context.config_identity),
        ("session.current_schema_name", context.schema_name),
        ("session.current_domain_code", context.domain_code),
        ("session.current_owner_repo_name", context.owner_repo_name),
        ("session.current_tenant_id", context.tenant_setting),
        ("session.current_username", context.actor),
        (
            "session.allow_global_rows",
            "true" if context.allow_global_rows else "false",
        ),
    )
    return " ".join(f"-c{name}={escaped(value)}" for name, value in settings)


def apply_transaction_context(
    session: Any,
    context: TapdbTransactionContext,
    *,
    assert_runtime_role: bool = True,
) -> None:
    """Install one complete transaction context or raise without fallback."""

    if not is_postgresql_session(session):
        return
    settings = (
        ("search_path", context.schema_name),
        ("session.current_config_identity", context.config_identity),
        ("session.current_schema_name", context.schema_name),
        ("session.current_domain_code", context.domain_code),
        ("session.current_owner_repo_name", context.owner_repo_name),
        ("session.current_tenant_id", context.tenant_setting),
        ("session.current_username", context.actor),
        (
            "session.allow_global_rows",
            "true" if context.allow_global_rows else "false",
        ),
    )
    for name, value in settings:
        session.execute(
            text("SELECT set_config(:name, :value, true)"),
            {"name": name, "value": value},
        )
    if assert_runtime_role:
        session.execute(text("SELECT tapdb_assert_runtime_role()"))


def operator_role_assertion_sql(
    *,
    schema_name: str | None = None,
    operator_user: str | None = None,
    allow_database_owner: bool = False,
    allow_create_database: bool = False,
) -> str:
    """Assert authenticated operator authority, including constrained Aurora owners.

    Database ownership is sufficient only for a lifecycle operation that has
    explicitly selected ``allow_database_owner``. It does not establish complete
    visibility through existing FORCE-RLS tables. Data operations require the
    exact schema owner and the canonical unrestricted owner policy on every RLS
    table. Membership in ``rds_superuser`` is deliberately not special-cased.
    """

    def literal(value: str) -> str:
        return "'" + _exact(value, "operator target").replace("'", "''") + "'"

    schema = literal(schema_name) if schema_name is not None else "current_schema()"
    login = (
        f"session_user = {literal(operator_user)} AND "
        if operator_user is not None
        else ""
    )
    database_owner = (
        "OR EXISTS (SELECT 1 FROM pg_catalog.pg_database d "
        "WHERE d.datname = current_database() AND d.datdba = r.oid)"
        if allow_database_owner
        else ""
    )
    create_database = "OR r.rolcreatedb" if allow_create_database else ""
    return f"""DO $tapdb_operator$ BEGIN
      IF NOT ({login}session_user = current_user AND EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname = session_user
        AND (r.rolsuper OR r.rolbypassrls {database_owner} {create_database} OR EXISTS (
          SELECT 1 FROM pg_catalog.pg_namespace n
          WHERE n.nspname = {schema} AND n.nspowner = r.oid
          AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class unreadable
            WHERE unreadable.relnamespace = n.oid
              AND unreadable.relkind IN ('r', 'p', 'v', 'm', 'f')
              AND NOT pg_catalog.has_table_privilege(r.oid, unreadable.oid, 'SELECT')
          )
          AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
            WHERE c.relnamespace = n.oid AND c.relrowsecurity
            AND (c.relforcerowsecurity OR c.relowner <> r.oid)
            AND (NOT EXISTS (
              SELECT 1 FROM pg_catalog.pg_policy p
              WHERE p.polrelid = c.oid AND p.polname = 'tapdb_operator_access'
              AND p.polcmd = '*' AND p.polpermissive AND p.polroles = ARRAY[r.oid]
              AND pg_catalog.pg_get_expr(p.polqual, p.polrelid) = 'true'
              AND pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid) = 'true'
            ) OR EXISTS (
              SELECT 1 FROM pg_catalog.pg_policy restriction
              CROSS JOIN LATERAL unnest(restriction.polroles) policy_role(oid)
              WHERE restriction.polrelid = c.oid AND NOT restriction.polpermissive
              AND CASE WHEN policy_role.oid = 0 THEN true ELSE
                pg_catalog.pg_has_role(r.oid, policy_role.oid, 'USAGE') END
            ))
          )
        ))
      )) THEN
        RAISE EXCEPTION 'TapDB operator requires an exact authenticated owner with complete FORCE-RLS policies, or SUPERUSER or BYPASSRLS';
      END IF;
    END $tapdb_operator$"""


def assert_operator_role(
    connection: Any, *, schema_name: str | None = None, operator_user: str | None = None
) -> None:
    """Require a separately authenticated role with complete FORCE-RLS access."""

    row = connection.execute(
        text(
            "SELECT rolname, rolsuper, rolbypassrls "
            "FROM pg_catalog.pg_roles WHERE rolname = current_user"
        )
    ).one_or_none()
    if row is None:
        raise RuntimeError(
            "TapDB operator connection has no authenticated PostgreSQL role"
        )
    connection.execute(
        text(
            operator_role_assertion_sql(
                schema_name=schema_name, operator_user=operator_user
            )
        )
    )


__all__ = [
    "TapdbTransactionContext",
    "apply_transaction_context",
    "assert_operator_role",
    "is_postgresql_session",
    "operator_role_assertion_sql",
    "transaction_context_pgoptions",
]
