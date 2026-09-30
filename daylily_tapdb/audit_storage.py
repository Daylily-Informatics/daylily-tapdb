"""Native audit-writer authority. No application role owns audit routines."""

from __future__ import annotations

import hashlib
from typing import Any
from sqlalchemy import text


def audit_writer_role(database: str, schema: str) -> str:
    return (
        "tapdb_audit_"
        + hashlib.sha256((database + "\0" + schema).encode()).hexdigest()[:24]
    )


def _ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def audit_writer_install_sql(database: str, schema: str, operator: str) -> str:
    """Provision one constrained, nonlogin writer for the selected database/schema.

    Requires native operator CREATEROLE authority. It never grants membership to
    the runtime login. The operator already owns the storage and remains outside
    the constrained-runtime guarantee.
    """
    name = audit_writer_role(database, schema)
    role, ns, admin = _ident(name), _ident(schema), _ident(operator)
    return f"""
DO $audit_writer$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = {_literal(name)}) THEN
        CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = {_literal(name)}
       AND (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit OR rolreplication OR rolbypassrls)) THEN
        RAISE EXCEPTION 'Unsafe TapDB audit writer role';
    END IF;
END;
$audit_writer$;
GRANT {role} TO {admin};
GRANT USAGE, CREATE ON SCHEMA {ns} TO {role};
REVOKE ALL ON TABLE {ns}.audit_log FROM PUBLIC;
GRANT INSERT ON TABLE {ns}.audit_log TO {role};
GRANT SELECT ON TABLE {ns}.generic_template, {ns}.tapdb_history_epoch, {ns}.tapdb_identity_prefix_config TO {role};
ALTER FUNCTION {ns}.record_insert() OWNER TO {role};
ALTER FUNCTION {ns}.record_update() OWNER TO {role};
REVOKE ALL ON FUNCTION {ns}.record_insert(), {ns}.record_update() FROM PUBLIC;
REVOKE CREATE ON SCHEMA {ns} FROM {role};
GRANT USAGE ON SEQUENCE {ns}.audit_log_uid_seq TO {role};
"""


def grant_audit_sequences(connection: Any, *, database: str, schema: str) -> None:
    """Only existing configured audit sequences; no allocator creation/change."""
    role = _ident(audit_writer_role(database, schema))
    ns = _ident(schema)
    prefixes = connection.execute(
        text(
            f"SELECT DISTINCT lower(prefix) || '_instance_seq' FROM {ns}.tapdb_identity_prefix_config WHERE entity='audit_log'"
        )
    ).scalars()
    for sequence in prefixes:
        connection.execute(
            text(f"GRANT USAGE, SELECT ON SEQUENCE {ns}.{_ident(sequence)} TO {role}")
        )


def enforce_audit_read_grants(connection: Any, *, database: str, schema: str) -> None:
    """Remove old direct/column grants for every bound runtime, then fail closed
    on inherited authority. Never rewrite unrelated role memberships implicitly.
    The caller keeps adoption/restore fenced if effective privileges remain.
    """
    ns = _ident(schema)
    writer = audit_writer_role(database, schema)
    roles = (
        connection.execute(
            text(f"SELECT role_name::text FROM {ns}.tapdb_runtime_principal_scope")
        )
        .scalars()
        .all()
    )
    columns = (
        connection.execute(
            text("""SELECT attname FROM pg_catalog.pg_attribute
        WHERE attrelid=CAST(:table AS regclass) AND attnum>0 AND NOT attisdropped"""),
            {"table": f"{ns}.audit_log"},
        )
        .scalars()
        .all()
    )
    for name in roles:
        role = _ident(name)
        if connection.execute(
            text("SELECT pg_catalog.pg_has_role(:role,:writer,'MEMBER')"),
            {"role": name, "writer": writer},
        ).scalar_one():
            raise ValueError("runtime must not be a member of the audit-writer role")
        connection.execute(text(f"REVOKE ALL ON TABLE {ns}.audit_log FROM {role}"))
        for column in columns:
            connection.execute(
                text(
                    f"REVOKE INSERT ({_ident(column)}), UPDATE ({_ident(column)}), REFERENCES ({_ident(column)}) ON {ns}.audit_log FROM {role}"
                )
            )
        connection.execute(text(f"GRANT SELECT ON TABLE {ns}.audit_log TO {role}"))
        forbidden = [
            f"pg_catalog.has_table_privilege(:role,:table,'{p}')"
            for p in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")
        ]
        forbidden += [
            f"pg_catalog.has_any_column_privilege(:role,:table,'{p}')"
            for p in ("INSERT", "UPDATE", "REFERENCES")
        ]
        if connection.execute(
            text("SELECT " + " OR ".join(forbidden)),
            {"role": name, "table": f"{ns}.audit_log"},
        ).scalar_one():
            raise ValueError(
                "runtime retains inherited or PUBLIC audit mutation privileges: " + name
            )
