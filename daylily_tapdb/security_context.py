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


def assert_operator_role(connection: Any) -> None:
    """Require a separately authenticated role that can see FORCE-RLS rows."""

    row = connection.execute(
        text(
            "SELECT rolname, rolsuper, rolbypassrls "
            "FROM pg_roles WHERE rolname = current_user"
        )
    ).one_or_none()
    if row is None or not (bool(row[1]) or bool(row[2])):
        raise RuntimeError(
            "TapDB operator connection must authenticate as a distinct "
            "SUPERUSER or BYPASSRLS role"
        )


__all__ = [
    "TapdbTransactionContext",
    "apply_transaction_context",
    "assert_operator_role",
    "is_postgresql_session",
    "transaction_context_pgoptions",
]
