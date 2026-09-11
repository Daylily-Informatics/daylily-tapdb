"""Explicit principal lifecycle, separate from schema and application lifecycle.

Bootstrap plans are entirely offline. Bootstrap apply has exactly two effects:
create the configured constrained login if absent, and grant CONNECT to the
configured existing database (plus explicitly requested non-admin rds_iam
membership). Schema binding is a separate, reviewed catalog-bound operation.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from daylily_tapdb.runtime_catalog_contract import (
    RuntimeCatalogContractError,
    canonical_security_contract,
    validate_managed_security,
)
from daylily_tapdb.security_context import operator_role_assertion_sql

_FORMAT = "tapdb-runtime-principal/v1"
_WRITABLE = {
    "generic_template",
    "generic_instance",
    "generic_instance_lineage",
    "audit_log",
    "outbox_event",
    "outbox_event_attempt",
    "inbox_message",
}
_READABLE = {
    "tapdb_identity_prefix_config",
    "tapdb_legacy_outbox_mapping",
    "_tapdb_migrations",
}
_SCOPE = "tapdb_runtime_principal_scope"


class RuntimePrincipalError(RuntimeError):
    """The explicit target, principal, or reviewed receipt is unsafe."""


def _exact(value: Any, field: str, *, empty: bool = False) -> str:
    if value is None:
        raise RuntimePrincipalError(f"{field} is required")
    result = str(value)
    if (not result and not empty) or result != result.strip():
        raise RuntimePrincipalError(f"{field} must be exact and non-empty")
    if any(ord(c) < 32 or ord(c) == 127 for c in result):
        raise RuntimePrincipalError(f"{field} contains a control character")
    return result


def _ident(value: str) -> str:
    value = _exact(value, "PostgreSQL identifier")
    if len(value.encode("utf-8")) > 63:
        raise RuntimePrincipalError("PostgreSQL identifiers must fit in 63 bytes")
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _boolean(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if value in ("true", "false"):
        return bool(value == "true")
    raise RuntimePrincipalError(f"{field} must be explicitly true or false")


def _iam_mode(cfg: Mapping[str, Any], *, operator: bool) -> bool:
    key = "operator_iam_auth" if operator else "iam_auth"
    value = cfg.get(key)
    if cfg["engine_type"] in {"local", "compose"}:
        # IAM is inapplicable to the explicitly selected non-Aurora engine.
        # Existing local configs therefore need no Aurora-only field added.
        if value is None or value == "":
            return False
        if _boolean(value, key):
            raise RuntimePrincipalError("IAM authentication requires an Aurora target")
        return False
    return _boolean(value, key)


def _target(cfg: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "config_path",
        "engine_type",
        "host",
        "port",
        "database",
        "schema_name",
        "user",
        "operator_user",
        "client_id",
        "database_name",
        "domain_code",
        "owner_repo_name",
    )
    target: dict[str, Any] = {k: _exact(cfg.get(k), k) for k in fields}
    if not Path(target["config_path"]).is_absolute():
        raise RuntimePrincipalError("config_path must be an explicit absolute path")
    if target["engine_type"] not in {"local", "compose", "aurora"}:
        raise RuntimePrincipalError("Unsupported explicit target engine_type")
    try:
        target["port"] = int(target["port"])
    except ValueError as exc:
        raise RuntimePrincipalError("port must be an integer") from exc
    if not 1 <= target["port"] <= 65535:
        raise RuntimePrincipalError("port is outside 1..65535")
    if "server_port" in cfg:
        server_port = _exact(cfg["server_port"], "server_port")
        if not server_port.isdigit() or not 1 <= int(server_port) <= 65535:
            raise RuntimePrincipalError("server_port is outside 1..65535")
        target["server_port"] = int(server_port)
    for key in ("database", "schema_name", "user", "operator_user"):
        _ident(target[key])
    if cfg.get("operator_configured") is not True:
        raise RuntimePrincipalError(
            "Explicit target.operator configuration is required"
        )
    if target["operator_user"] == target["user"]:
        raise RuntimePrincipalError("Operator/runtime role collision")
    if target["user"].startswith(("pg_", "rds_")):
        raise RuntimePrincipalError("Runtime principal cannot use a reserved role name")
    target["iam_auth"] = _iam_mode(cfg, operator=False)
    target["operator_iam_auth"] = _iam_mode(cfg, operator=True)
    target["tenant_id"] = _exact(cfg.get("tenant_id"), "target.tenant_id", empty=True)
    if target["tenant_id"]:
        try:
            if str(uuid.UUID(target["tenant_id"])) != target["tenant_id"]:
                raise ValueError("noncanonical UUID")
        except ValueError as exc:
            raise RuntimePrincipalError(
                "tenant_id must be an exact canonical UUID"
            ) from exc
    target["allow_global_claims"] = _boolean(
        cfg.get("allow_global_claims"), "target.allow_global_claims"
    )
    if not re.fullmatch(r"[0-9A-HJ-KMNP-TV-Z]{1,4}", target["domain_code"]):
        raise RuntimePrincipalError("domain_code must be an exact Meridian domain code")
    if not re.fullmatch(r"[a-z0-9]+([._-][a-z0-9]+)*", target["owner_repo_name"]):
        raise RuntimePrincipalError("owner_repo_name must be an exact issuer name")
    if target["engine_type"] == "aurora":
        for key in ("region", "cluster_identifier", "sslrootcert"):
            target[key] = _exact(cfg.get(key), key)
        if not Path(target["sslrootcert"]).is_absolute():
            raise RuntimePrincipalError("sslrootcert must be an explicit absolute path")
        target["hostaddr"] = _exact(cfg.get("hostaddr", ""), "hostaddr", empty=True)
    return target


def _seal(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("sha256", None)
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    result["sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
    return result


def build_runtime_principal_bootstrap_plan(cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Return a configuration-only plan; never resolve credentials or connect."""
    target = _target(cfg)
    return _seal(
        {
            "format": _FORMAT,
            "operation": "bootstrap",
            "status": "planned",
            "target": target,
            "attributes": {
                "login": True,
                "superuser": False,
                "bypassrls": False,
                "createdb": False,
                "createrole": False,
                "replication": False,
                "inherit": False,
            },
            "membership": ["rds_iam"] if target["iam_auth"] else [],
            "grants": [{"database": target["database"], "privileges": ["CONNECT"]}],
            "schema_changes": [],
        }
    )


def _credential(cfg: Mapping[str, Any], *, operator: bool) -> str:
    prefix = "operator_" if operator else ""
    iam = _iam_mode(cfg, operator=operator)
    user = str(cfg[prefix + "user"])
    if iam:
        from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

        return str(
            AuroraConnectionBuilder.get_iam_auth_token(
                str(cfg["region"]),
                str(cfg["host"]),
                int(cfg["server_port"] if "server_port" in cfg else cfg["port"]),
                user,
                profile=str(cfg.get("aws_profile") or "") or None,
            )
        )
    secret = str(cfg.get(prefix + "secret_arn") or "")
    if secret:
        from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

        return str(
            AuroraConnectionBuilder.get_secret_password(
                secret,
                str(cfg["region"]),
                profile=str(cfg.get("aws_profile") or "") or None,
            )
        )
    if prefix + "password" not in cfg:
        raise RuntimePrincipalError(
            f"Explicit {prefix}password, secret_arn, or IAM mode is required"
        )
    password = str(cfg[prefix + "password"])
    if cfg["engine_type"] == "aurora" and not password:
        raise RuntimePrincipalError(
            "Aurora password authentication requires a credential"
        )
    return password


def _operator_engine(
    cfg: Mapping[str, Any], target: Mapping[str, Any], isolation_level: str | None
) -> Engine:
    """One credential and engine builder for transactional and retained sessions."""
    query = {
        "options": "-csearch_path=pg_catalog",
        "application_name": "tapdb-runtime-principal",
    }
    if target["engine_type"] == "aurora":
        if not Path(target["sslrootcert"]).is_file():
            raise RuntimePrincipalError(
                "The explicitly configured sslrootcert does not exist"
            )
        query.update(sslmode="verify-full", sslrootcert=target["sslrootcert"])
        if target["hostaddr"]:
            query["hostaddr"] = target["hostaddr"]
    else:
        query["sslmode"] = "disable"
    url = URL.create(
        "postgresql+psycopg2",
        username=target["operator_user"],
        password=_credential(cfg, operator=True),
        host=target["host"],
        port=target["port"],
        database=target["database"],
        query=query,
    )
    options: dict[str, Any] = {"poolclass": NullPool, "hide_parameters": True}
    if isolation_level is not None:
        options["isolation_level"] = isolation_level
    return create_engine(url, **options)


@contextmanager
def operator_connection(
    cfg: Mapping[str, Any],
    *,
    isolation_level: str | None = None,
    read_only: bool = False,
) -> Iterator[Connection]:
    """Run one transaction on the exact existing database as its operator.

    No runtime ORM initialization, reflection, or schema availability is needed.
    Ambient libpq options are replaced and credentials are never serialized.
    """
    target = _target(cfg)
    engine = _operator_engine(cfg, target, isolation_level)
    try:
        with engine.begin() as connection:
            if read_only:
                connection.execute(text("SET TRANSACTION READ ONLY"))
            _check_connection(connection, target)
            yield connection
    finally:
        engine.dispose()


@contextmanager
def operator_session(
    cfg: Mapping[str, Any], *, isolation_level: str | None = None
) -> Iterator[Connection]:
    """Retain one verified physical operator session across caller transactions.

    The verification transaction is closed before yielding. Callers explicitly
    own subsequent begin/commit/rollback boundaries and may hold session locks
    across them. Context exit rolls back unfinished work and closes the session.
    """
    target = _target(cfg)
    engine = _operator_engine(cfg, target, isolation_level)
    try:
        with engine.connect() as connection:
            _check_connection(connection, target)
            connection.rollback()
            yield connection
    finally:
        engine.dispose()


def _check_connection(connection: Any, target: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(
        connection.execute(
            text("""/* tapdb_principal:identity */
        SELECT current_database() AS database, session_user::text AS session_user,
               current_user::text AS current_user, d.oid::bigint AS database_oid
          FROM pg_catalog.pg_database d WHERE d.datname = current_database()
    """)
        )
        .mappings()
        .one()
    )
    if (
        row["database"] != target["database"]
        or row["session_user"] != target["operator_user"]
        or row["current_user"] != target["operator_user"]
    ):
        raise RuntimePrincipalError(
            "Authenticated connection does not match the exact configured operator/database"
        )
    return row


def _database_access(connection: Any, target: Mapping[str, Any]) -> dict[str, Any]:
    return dict(
        connection.execute(
            text("""/* tapdb_principal:database_access */
        SELECT d.datacl::text AS acl,
          pg_catalog.has_database_privilege(runtime.oid, d.oid, 'CONNECT') AS runtime_connect,
          pg_catalog.has_database_privilege(runtime.oid, d.oid, 'TEMPORARY') AS runtime_temp,
          pg_catalog.has_database_privilege(operator.oid, d.oid, 'TEMPORARY') AS operator_temp,
          (operator.rolsuper OR EXISTS (
            SELECT 1 FROM pg_catalog.aclexplode(
              COALESCE(d.datacl, pg_catalog.acldefault('d', d.datdba))
            ) permission
            WHERE permission.privilege_type = 'TEMPORARY'
              AND CASE WHEN permission.grantee NOT IN (
                0, runtime.oid
              ) THEN pg_catalog.pg_has_role(operator.oid, permission.grantee, 'USAGE')
              ELSE false END
          )) AS operator_temp_after_revokes
        FROM pg_catalog.pg_database d
          JOIN pg_catalog.pg_roles operator ON operator.rolname = :operator
          JOIN pg_catalog.pg_roles runtime ON runtime.rolname = :role
        WHERE d.datname = current_database()
    """),
            {"role": target["user"], "operator": target["operator_user"]},
        )
        .mappings()
        .one()
    )


def _role_state(
    connection: Any, target: Mapping[str, Any], *, missing_ok: bool = False
) -> dict[str, Any] | None:
    role = (
        connection.execute(
            text("""/* tapdb_principal:role */
        SELECT oid::bigint AS oid, rolname::text AS name, rolcanlogin AS login,
          rolsuper AS superuser, rolbypassrls AS bypassrls, rolcreatedb AS createdb,
          rolcreaterole AS createrole, rolreplication AS replication,
          rolinherit AS inherit FROM pg_catalog.pg_roles WHERE rolname = :role
    """),
            {"role": target["user"]},
        )
        .mappings()
        .one_or_none()
    )
    if role is None:
        if missing_ok:
            return None
        raise RuntimePrincipalError(
            "Configured runtime principal does not exist; bootstrap it first"
        )
    state = dict(role)
    if not state["login"] or any(
        state[k]
        for k in ("superuser", "bypassrls", "createdb", "createrole", "replication")
    ):
        raise RuntimePrincipalError(
            "Runtime role has elevated attributes or cannot LOGIN"
        )
    memberships = [
        dict(row)
        for row in connection.execute(
            text("""/* tapdb_principal:memberships */
        SELECT parent.rolname::text AS name, m.admin_option AS admin,
               m.inherit_option AS inherit, m.set_option AS set
          FROM pg_catalog.pg_auth_members m
          JOIN pg_catalog.pg_roles parent ON parent.oid = m.roleid
         WHERE m.member = :oid ORDER BY parent.rolname
    """),
            {"oid": state["oid"]},
        ).mappings()
    ]
    permitted = ["rds_iam"] if target["iam_auth"] else []
    if any(row["name"] not in permitted or row["admin"] for row in memberships):
        raise RuntimePrincipalError(
            "Runtime role has unexpected or admin role membership"
        )
    unsafe = (
        connection.execute(
            text("""/* tapdb_principal:unsafe */
        SELECT
          EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend WHERE refclassid = 'pg_catalog.pg_authid'::regclass
            AND refobjid = :oid AND deptype = 'o') AS ownership,
          EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole = :oid
            AND cardinality(setconfig) > 0) AS settings,
          pg_catalog.has_database_privilege(:role, current_database(), 'CREATE') AS database_create,
          EXISTS (SELECT 1 FROM pg_catalog.pg_roles candidate
            WHERE candidate.oid <> :oid AND candidate.rolname <> 'rds_iam'
              AND pg_catalog.pg_has_role(:oid, candidate.oid, 'MEMBER')) AS indirect_membership,
          EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m
            JOIN pg_catalog.pg_roles member ON member.oid = m.member
            WHERE m.roleid = :oid AND NOT (member.rolname = :operator
              AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option)) AS shared_role
    """),
            {
                "oid": state["oid"],
                "role": target["user"],
                "operator": target["operator_user"],
            },
        )
        .mappings()
        .one()
    )
    violations = [key for key, value in unsafe.items() if value]
    if violations:
        raise RuntimePrincipalError(
            "Unsafe runtime principal: " + ", ".join(violations)
        )
    state["memberships"] = memberships
    return state


def bootstrap_runtime_principal(
    cfg: Mapping[str, Any], *, apply: bool = False
) -> dict[str, Any]:
    """Create/validate only target.user and grant only target-database CONNECT."""
    plan = build_runtime_principal_bootstrap_plan(cfg)
    if not apply:
        return plan
    target = plan["target"]
    with operator_connection(cfg) as connection:
        _check_connection(connection, target)
        state = _role_state(connection, target, missing_ok=True)
        created = state is None
        if created:
            # Utility parameters are escaped by the driver; hide_parameters also
            # prevents SQLAlchemy exception text from serializing credentials.
            statement = f"CREATE ROLE {_ident(target['user'])} LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT"
            if target["iam_auth"]:
                connection.execute(text(statement))
            else:
                connection.execute(
                    text(statement + " PASSWORD :password"),
                    {"password": _credential(cfg, operator=False)},
                )
        if target["iam_auth"]:
            connection.execute(
                text(
                    f"GRANT rds_iam TO {_ident(target['user'])} WITH ADMIN FALSE, INHERIT FALSE, SET FALSE"
                )
            )
        # Validate all constraints before CONNECT. Failure rolls back role creation.
        state = _role_state(connection, target)
        if target["iam_auth"] and (
            state is None
            or {row["name"] for row in state["memberships"]} != {"rds_iam"}
        ):
            raise RuntimePrincipalError(
                "Explicit non-admin rds_iam membership was not established"
            )
        connection.execute(
            text(
                f"GRANT CONNECT ON DATABASE {_ident(target['database'])} TO {_ident(target['user'])}"
            )
        )
    return _seal(
        {
            "format": _FORMAT,
            "operation": "bootstrap",
            "status": "applied",
            "plan_sha256": plan["sha256"],
            "target": target,
            "created": created,
            "principal": state,
            "grants": plan["grants"],
        }
    )


def runtime_scope_binding_sql(schema_name: str, cfg: Mapping[str, Any]) -> str:
    """Insert an immutable scope or validate an identical existing binding."""
    target = _target(cfg)
    if schema_name != target["schema_name"]:
        raise RuntimePrincipalError("Scope schema differs from configured target")
    values = {
        "config_identity": target["config_path"],
        "schema_name": schema_name,
        "domain_code": target["domain_code"],
        "issuer_app_code": target["owner_repo_name"],
        "tenant_id": target["tenant_id"] or None,
        "allow_global_rows": target["allow_global_claims"],
    }
    rendered = {
        key: (
            "NULL"
            if value is None
            else "TRUE"
            if value is True
            else "FALSE"
            if value is False
            else _literal(str(value))
        )
        for key, value in values.items()
    }
    relation = f"{_ident(schema_name)}.{_SCOPE}"
    role = _literal(target["user"])
    matches = " AND ".join(
        f"{key} IS NOT DISTINCT FROM {value}" for key, value in rendered.items()
    )
    return f"""INSERT INTO {relation} (role_name, {", ".join(rendered)})
      VALUES ({role}, {", ".join(rendered.values())}) ON CONFLICT (role_name) DO NOTHING;
      DO $tapdb_binding$ BEGIN IF NOT EXISTS (
        SELECT 1 FROM {relation} WHERE role_name = {role} AND {matches}
      ) THEN RAISE EXCEPTION 'TapDB runtime principal scope binding conflicts with the configured target';
      END IF; END $tapdb_binding$"""


def _default_acl_sql(schema_name: str, runtime_user: str, operator_user: str) -> str:
    schema, runtime, operator = map(_ident, (schema_name, runtime_user, operator_user))
    return f"""ALTER DEFAULT PRIVILEGES FOR ROLE {operator} IN SCHEMA {schema}
        REVOKE ALL ON TABLES FROM {runtime};
        ALTER DEFAULT PRIVILEGES FOR ROLE {operator} IN SCHEMA {schema}
        REVOKE ALL ON SEQUENCES FROM {runtime}"""


def _default_acl_guard_sql(
    schema_name: str, runtime_user: str, operator_user: str
) -> str:
    """Reject global or PUBLIC defaults that scoped runtime regrants cannot fix."""
    schema, runtime, operator = map(
        _literal, (schema_name, runtime_user, operator_user)
    )
    return f"""DO $tapdb_default_acl$ BEGIN IF EXISTS (
      SELECT 1 FROM pg_catalog.pg_default_acl d
      JOIN pg_catalog.pg_roles operator ON operator.rolname = {operator}
      JOIN pg_catalog.pg_roles runtime ON runtime.rolname = {runtime}
      CROSS JOIN LATERAL pg_catalog.aclexplode(d.defaclacl) permission
      WHERE d.defaclrole = operator.oid
        AND d.defaclnamespace IN (0, (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = {schema}))
        AND d.defaclobjtype IN ('r', 'S')
        AND (permission.grantee = 0 OR CASE WHEN permission.grantee <> 0 THEN
          pg_catalog.pg_has_role(runtime.oid, permission.grantee, 'USAGE') ELSE false END)
        AND (permission.grantee = 0 OR d.defaclnamespace = 0)
    ) THEN RAISE EXCEPTION 'Global or PUBLIC default privileges conflict with constrained runtime binding';
    END IF; END $tapdb_default_acl$"""


def runtime_schema_grants_sql(
    schema_name: str, runtime_user: str, *, operator_user: str
) -> str:
    """Canonical narrow grants for the explicit fresh-schema lifecycle.

    This helper does not create objects or bind a scope. Restores and migrations
    use the separately reviewed ``bind`` lifecycle. New tables receive no
    implicit runtime authority. Sequence privileges require the shared catalog
    classifier in ``grant_proven_runtime_sequences`` after explicit provisioning.
    """
    schema, runtime = _ident(schema_name), _ident(runtime_user)
    _ident(operator_user)
    if runtime_user == operator_user:
        raise RuntimePrincipalError("Operator/runtime role collision")
    statements = [
        _default_acl_guard_sql(schema_name, runtime_user, operator_user),
        f"GRANT USAGE ON SCHEMA {schema} TO {runtime}",
    ]
    statements.extend(
        f"REVOKE ALL ON TABLE {schema}.{_ident(name)} FROM {runtime}"
        for name in sorted(_WRITABLE | _READABLE | {_SCOPE})
    )
    statements.extend(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {schema}.{_ident(name)} TO {runtime}"
        for name in sorted(_WRITABLE)
    )
    statements.extend(
        f"GRANT SELECT ON TABLE {schema}.{_ident(name)} TO {runtime}"
        for name in sorted(_READABLE)
    )
    statements.append(_default_acl_sql(schema_name, runtime_user, operator_user))
    return ";\n".join(statements)


def _managed_sequence_bindings(
    connection: Any, target: Mapping[str, Any], objects: list[dict[str, Any]]
) -> dict[str, Any]:
    from daylily_tapdb.identity_inventory import IdentityInventoryError
    from daylily_tapdb.sequences import capture_runtime_sequence_bindings

    try:
        bindings = capture_runtime_sequence_bindings(
            connection,
            schema_name=target["schema_name"],
            target={**target, "config_identity": target["config_path"]},
            managed_tables=sorted(_WRITABLE | _READABLE | {_SCOPE}),
        )
    except IdentityInventoryError as exc:
        raise RuntimePrincipalError(
            "Runtime allocator catalog cannot be proven: " + str(exc)
        ) from exc
    sequences = {row["name"]: row for row in objects if row["kind"] == "S"}
    authorized = {}
    for name, state in bindings.items():
        mapping = state["mapping"]
        if mapping["kind"] == "owned_column":
            allowed = bool(mapping["columns"]) and all(
                column["schema_name"] == target["schema_name"]
                and column["table_name"] in _WRITABLE
                for column in mapping["columns"]
            )
        elif mapping["kind"] == "prefix":
            allowed = any(
                evidence["source"]
                in {"stored_rows", "template_binding", "identity_binding"}
                and evidence.get("table") in _WRITABLE | _READABLE
                and evidence.get("scope")
                == {
                    "domain_code": target["domain_code"],
                    "owner_repo_name": target["owner_repo_name"],
                }
                for evidence in mapping["evidence"]
            ) or (
                mapping["prefix"] in {"WX", "WSX", "XX", "AY", "MSG"}
                and any(
                    evidence["source"] == "catalog_annotation"
                    for evidence in mapping["evidence"]
                )
            )
        else:
            allowed = False
        if not allowed:
            continue
        if (
            name not in sequences
            or state["owner"] != target["operator_user"]
            or sequences[name]["owner"] != state["owner"]
        ):
            raise RuntimePrincipalError(
                "Managed allocator must have the exact configured operator owner"
            )
        authorized[name] = state
    return authorized


@contextmanager
def _catalog_search_path(connection: Any) -> Iterator[None]:
    previous = connection.execute(
        text("/* tapdb_principal:search_path */ SELECT current_setting('search_path')")
    ).scalar_one()
    connection.execute(
        text("SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)")
    )
    failed_transaction = False
    try:
        yield
    except DBAPIError:
        # PostgreSQL restores SET LOCAL at the caller's mandatory rollback.
        # A query here would mask the original error with InFailedSqlTransaction.
        failed_transaction = True
        raise
    finally:
        if not failed_transaction:
            connection.execute(
                text("SELECT pg_catalog.set_config('search_path', :path, true)"),
                {"path": previous},
            )


def build_runtime_principal_binding_plan(
    connection: Any, cfg: Mapping[str, Any]
) -> dict[str, Any]:
    """Read a canonical catalog without changing the caller's search_path."""
    _target(cfg)
    with _catalog_search_path(connection):
        return _build_runtime_principal_binding_plan(connection, cfg)


def _build_runtime_principal_binding_plan(
    connection: Any, cfg: Mapping[str, Any]
) -> dict[str, Any]:
    """Read exact schema catalogs and scope; grant nothing and perform no DDL."""
    target = _target(cfg)
    identity = _check_connection(connection, target)
    connection.execute(
        text(
            operator_role_assertion_sql(
                schema_name=target["schema_name"], operator_user=target["operator_user"]
            )
        )
    )
    principal = _role_state(connection, target)
    database_access = _database_access(connection, target)
    connection.execute(
        text(
            _default_acl_guard_sql(
                target["schema_name"], target["user"], target["operator_user"]
            )
        )
    )
    schema = (
        connection.execute(
            text("""/* tapdb_principal:schema */
        SELECT n.oid::bigint AS oid, n.nspowner::bigint AS owner_oid,
          pg_catalog.pg_get_userbyid(n.nspowner)::text AS owner,
          pg_catalog.quote_ident(n.nspname) AS sql_name,
          n.nspacl::text AS acl, pg_catalog.has_schema_privilege(:role, n.oid, 'CREATE') AS runtime_create
        FROM pg_catalog.pg_namespace n WHERE n.nspname = :schema
    """),
            {"role": target["user"], "schema": target["schema_name"]},
        )
        .mappings()
        .one_or_none()
    )
    if schema is None or schema["owner"] != target["operator_user"]:
        raise RuntimePrincipalError(
            "Existing managed schema must be owned by the exact configured operator"
        )
    if schema["runtime_create"]:
        raise RuntimePrincipalError(
            "Runtime principal has effective CREATE on managed schema"
        )
    objects = [
        dict(row)
        for row in connection.execute(
            text("""/* tapdb_principal:objects */
        SELECT c.oid::bigint AS oid, c.relname::text AS name, c.relkind::text AS kind,
          pg_catalog.pg_get_userbyid(c.relowner)::text AS owner,
          c.relrowsecurity AS rls, c.relforcerowsecurity AS force_rls, c.relacl::text AS acl,
          COALESCE((SELECT jsonb_agg(jsonb_build_object('column', a.attname, 'acl', a.attacl::text)
              ORDER BY a.attnum) FROM pg_catalog.pg_attribute a
              WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
              AND a.attacl IS NOT NULL), '[]'::jsonb) AS column_acls
        FROM pg_catalog.pg_class c WHERE c.relnamespace = :schema_oid
          AND c.relkind IN ('r', 'p', 'S', 'v', 'm', 'f') ORDER BY c.relname
    """),
            {"schema_oid": schema["oid"]},
        ).mappings()
    ]
    policies = [
        dict(row)
        for row in connection.execute(
            text("""/* tapdb_principal:policies */
        SELECT c.relname::text AS relation, p.polname::text AS name, p.polcmd::text AS command,
          p.polpermissive AS permissive, p.polroles::text AS roles,
          pg_catalog.pg_get_expr(p.polqual, p.polrelid) AS using,
          pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid) AS with_check
        FROM pg_catalog.pg_policy p JOIN pg_catalog.pg_class c ON c.oid = p.polrelid
        WHERE c.relnamespace = :schema_oid ORDER BY c.relname, p.polname
    """),
            {"schema_oid": schema["oid"]},
        ).mappings()
    ]
    functions = [
        dict(row)
        for row in connection.execute(
            text("""/* tapdb_principal:functions */
        SELECT p.oid::bigint AS oid, p.proname::text AS name,
          pg_catalog.pg_get_function_identity_arguments(p.oid) AS arguments,
          pg_catalog.pg_get_userbyid(p.proowner)::text AS owner,
          p.prosecdef AS security_definer, p.proconfig AS settings,
          p.proargtypes::text AS argument_types, p.proargnames AS argument_names,
          p.proargmodes AS argument_modes, p.proallargtypes::text AS all_argument_types,
          p.pronargdefaults::int AS default_count, p.proargdefaults::text AS default_expression,
          p.provariadic::bigint AS variadic_type,
          p.prorettype::bigint AS return_type, l.lanname AS language, p.prokind::text AS kind,
          p.prosrc AS source, p.probin AS binary, p.prosqlbody::text AS sql_body,
          p.protrftypes::text AS transform_types,
          p.proisstrict AS strict, p.provolatile::text AS volatility,
          p.proparallel::text AS parallel, p.proleakproof AS leakproof, p.proretset AS returns_set,
          p.prosupport::oid::bigint AS support, p.procost::float AS cost, p.prorows::float AS rows,
          p.proacl::text AS acl,
          CASE WHEN p.prokind = 'a' THEN NULL
            ELSE md5(pg_catalog.pg_get_functiondef(p.oid)) END AS definition_hash
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid = p.prolang
        WHERE p.pronamespace = :schema_oid
        ORDER BY p.proname, p.oid
    """),
            {"schema_oid": schema["oid"]},
        ).mappings()
    ]
    default_acls = [
        dict(row)
        for row in connection.execute(
            text("""/* tapdb_principal:default_acls */
        SELECT d.defaclnamespace::bigint AS schema_oid, d.defaclobjtype::text AS object_type,
          d.defaclacl::text AS acl
        FROM pg_catalog.pg_default_acl d WHERE d.defaclrole = :owner_oid
          AND d.defaclnamespace IN (0, :schema_oid)
        ORDER BY d.defaclnamespace, d.defaclobjtype
    """),
            {"owner_oid": schema["owner_oid"], "schema_oid": schema["oid"]},
        ).mappings()
    ]
    tables = {row["name"]: row for row in objects if row["kind"] in ("r", "p")}
    expected = _WRITABLE | _READABLE | {_SCOPE}
    if not expected.issubset(tables):
        raise RuntimePrincipalError(
            "Managed schema table inventory differs from the explicit TapDB binding contract"
        )
    if any(tables[name]["owner"] != target["operator_user"] for name in expected):
        raise RuntimePrincipalError(
            "Schema objects must have the exact configured operator owner"
        )
    if any(
        not tables[name]["rls"] or not tables[name]["force_rls"]
        for name in expected - {"_tapdb_migrations"}
    ):
        raise RuntimePrincipalError(
            "Every protected table must retain enabled and forced RLS"
        )
    triggers = [
        dict(row)
        for row in connection.execute(
            text("""/* tapdb_principal:triggers */
        SELECT c.relname::text AS relation, t.tgname::text AS name,
          t.tgenabled::text AS enabled, p.proname::text AS function,
          n.nspname::text AS function_schema, pg_catalog.pg_get_triggerdef(t.oid) AS definition,
          pg_catalog.pg_get_function_identity_arguments(p.oid) AS function_arguments,
          t.tgtype::int AS type, pg_catalog.pg_get_expr(t.tgqual, t.tgrelid) AS when,
          t.tgattr::text AS columns, encode(t.tgargs, 'hex') AS arguments_hex,
          t.tgconstraint::bigint AS constraint_oid, t.tgdeferrable AS deferrable,
          t.tginitdeferred AS initially_deferred, t.tgoldtable AS old_table, t.tgnewtable AS new_table
        FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
        JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE c.relnamespace = :schema_oid AND NOT t.tgisinternal
        ORDER BY c.relname, t.tgname
    """),
            {"schema_oid": schema["oid"]},
        ).mappings()
    ]
    try:
        contract = canonical_security_contract(
            target["schema_name"], schema["sql_name"]
        )
        routine_grants = validate_managed_security(
            contract,
            owner=target["operator_user"],
            owner_oid=schema["owner_oid"],
            policies=policies,
            functions=functions,
            triggers=triggers,
            managed_tables=expected,
        )
    except RuntimeCatalogContractError as exc:
        raise RuntimePrincipalError(str(exc)) from exc
    existing = (
        connection.execute(
            text(
                f"SELECT config_identity, schema_name::text, domain_code, issuer_app_code, tenant_id::text, allow_global_rows FROM {_ident(target['schema_name'])}.{_SCOPE} WHERE role_name = :role"
            ),
            {"role": target["user"]},
        )
        .mappings()
        .one_or_none()
    )
    scope = {
        "config_identity": target["config_path"],
        "schema_name": target["schema_name"],
        "domain_code": target["domain_code"],
        "issuer_app_code": target["owner_repo_name"],
        "tenant_id": target["tenant_id"] or None,
        "allow_global_rows": target["allow_global_claims"],
    }
    if existing is not None and dict(existing) != scope:
        raise RuntimePrincipalError(
            "Runtime principal already has a conflicting immutable scope binding"
        )
    grants = []
    sequence_bindings = _managed_sequence_bindings(connection, target, objects)
    for row in objects:
        name = row["name"]
        if row["kind"] == "S" and name in sequence_bindings:
            privileges = ["USAGE", "SELECT"]
        elif name in _WRITABLE:
            privileges = ["SELECT", "INSERT", "UPDATE", "DELETE"]
        elif name in _READABLE:
            privileges = ["SELECT"]
        else:
            continue
        grants.append(
            {
                "kind": "SEQUENCE" if row["kind"] == "S" else "TABLE",
                "name": name,
                "privileges": privileges,
            }
        )
    plan = {
        "format": _FORMAT,
        "operation": "bind",
        "status": "planned",
        "target": target,
        "identity": identity,
        "database_access": database_access,
        "database_grants": [
            {"database": target["database"], "privileges": ["CONNECT"]}
        ],
        "database_revokes": [
            {
                "database": target["database"],
                "grantee": "PUBLIC",
                "grantee_kind": "public",
                "privileges": ["TEMPORARY"],
            },
            {
                "database": target["database"],
                "grantee": target["user"],
                "grantee_kind": "role",
                "privileges": ["TEMPORARY"],
            },
        ],
        "operator_database_grants": [
            {
                "database": target["database"],
                "grantee": target["operator_user"],
                "privileges": ["TEMPORARY"],
            }
        ]
        if database_access["operator_temp"]
        and not database_access["operator_temp_after_revokes"]
        else [],
        "runtime_session_requirement": {
            "action": "close_and_recreate_pre_binding_runtime_sessions",
            "verified": False,
            "performed_by_bind": False,
            "reason": (
                "Database TEMP revocation does not remove existing temporary objects "
                "or prove existing sessions cannot create more."
            ),
        },
        "schema": dict(schema),
        "principal": principal,
        "objects": objects,
        "policies": policies,
        "functions": functions,
        "routine_grants": routine_grants,
        "security_asset_sha256": contract["asset_sha256"],
        "sequence_bindings": sequence_bindings,
        "triggers": triggers,
        "default_acls": default_acls,
        "planned_default_privileges": {
            "tables": [],
            "sequences": [],
        },
        "existing_scope": dict(existing) if existing else None,
        "scope": scope,
        "grants": grants,
    }
    _verify_bound_permissions(connection, target, plan, unmanaged_only=True)
    return _seal(plan)


def _write_receipt(path: Path, payload: dict[str, Any]) -> None:
    if not path.is_absolute():
        raise RuntimePrincipalError("receipt_path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)


def _verify_bound_permissions(
    connection: Any,
    target: Mapping[str, Any],
    plan: Mapping[str, Any],
    *,
    unmanaged_only: bool = False,
) -> None:
    """Reject effective relation, column, routine and grant-option leaks.

    Preserved objects are never repaired by a core bind. Their effective
    runtime DENY must already hold, even before schema USAGE is granted.
    PostgreSQL's privilege predicates include PUBLIC and inherited authority.
    """
    grants = {grant["name"]: grant for grant in plan["grants"]}
    for obj in plan["objects"]:
        managed = obj["name"] in grants or obj["name"] == _SCOPE
        if unmanaged_only and managed:
            continue
        qualified = f"{_ident(target['schema_name'])}.{_ident(obj['name'])}"
        allowed = set(grants.get(obj["name"], {}).get("privileges", []))
        if obj["kind"] == "S":
            privileges = {"USAGE", "SELECT", "UPDATE"}
            checks = [
                f"pg_catalog.has_sequence_privilege(:role, :object, '{privilege}')"
                for privilege in sorted(privileges - allowed)
            ] + [
                f"pg_catalog.has_sequence_privilege(:role, :object, '{privilege} WITH GRANT OPTION')"
                for privilege in sorted(privileges)
            ]
        else:
            privileges = {
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
            }
            columns = {"SELECT", "INSERT", "UPDATE", "REFERENCES"}
            checks = (
                [
                    f"pg_catalog.has_table_privilege(:role, :object, '{privilege}')"
                    for privilege in sorted(privileges - allowed)
                ]
                + [
                    f"pg_catalog.has_table_privilege(:role, :object, '{privilege} WITH GRANT OPTION')"
                    for privilege in sorted(privileges)
                ]
                + [
                    f"pg_catalog.has_any_column_privilege(:role, :object, '{privilege}')"
                    for privilege in sorted(columns - allowed)
                ]
                + [
                    f"pg_catalog.has_any_column_privilege(:role, :object, '{privilege} WITH GRANT OPTION')"
                    for privilege in sorted(columns)
                ]
            )
            checks.append(
                "CASE WHEN current_setting('server_version_num')::int >= 170000 THEN "
                "pg_catalog.has_table_privilege(:role, :object, 'MAINTAIN') ELSE false END"
            )
        if connection.execute(
            text(
                "/* tapdb_principal:effective_permissions */ SELECT "
                + " OR ".join(checks)
            ),
            {"role": target["user"], "object": qualified},
        ).scalar_one():
            raise RuntimePrincipalError(
                f"Runtime principal retains forbidden effective object privileges: {obj['name']}"
            )
    allowed_routines = {
        (routine["name"], routine["arguments"]) for routine in plan["routine_grants"]
    }
    for routine in plan["functions"]:
        managed = (routine["name"], routine["arguments"]) in allowed_routines
        if unmanaged_only and managed:
            continue
        permission = "EXECUTE WITH GRANT OPTION" if managed else "EXECUTE"
        if connection.execute(
            text(
                "/* tapdb_principal:effective_permissions */ SELECT "
                f"pg_catalog.has_function_privilege(:role, :object, '{permission}')"
            ),
            {"role": target["user"], "object": routine["oid"]},
        ).scalar_one():
            raise RuntimePrincipalError(
                f"Runtime principal retains forbidden effective routine privileges: {routine['name']}"
            )


def bind_runtime_principal(
    cfg: Mapping[str, Any], *, apply: bool = False, receipt_path: Path
) -> dict[str, Any]:
    """Receipt-bind CONNECT/schema grants, TEMP denial and immutable scope.

    CONNECT is explicit here because a restored target can be created after
    role bootstrap. Existing operator TEMP is preserved, not expanded. Revoking
    TEMP does not close old runtime sessions or remove their temporary objects;
    service adoption must close and recreate those sessions separately.
    """
    target = _target(cfg)
    receipt_path = Path(receipt_path)
    if not receipt_path.is_absolute():
        raise RuntimePrincipalError("receipt_path must be absolute")
    reviewed = None
    if apply:
        reviewed = json.loads(receipt_path.read_text(encoding="utf-8"))
        if not isinstance(reviewed, dict) or reviewed != _seal(reviewed):
            raise RuntimePrincipalError("Runtime binding receipt has an invalid digest")
        if (
            reviewed.get("target") != target
            or reviewed.get("operation") != "bind"
            or reviewed.get("status") != "planned"
        ):
            raise RuntimePrincipalError(
                "Runtime binding receipt does not match configured target/operation"
            )
        result_path = receipt_path.with_name(receipt_path.stem + ".result.json")
        if result_path.exists():
            raise RuntimePrincipalError(
                "Binding result already exists; inspect it before any new operation"
            )
    elif receipt_path.exists():
        raise RuntimePrincipalError(
            "Binding receipt already exists; select an unused explicit path"
        )
    with operator_connection(
        cfg, isolation_level="SERIALIZABLE", read_only=not apply
    ) as connection:
        plan = build_runtime_principal_binding_plan(connection, cfg)
        if not apply:
            _write_receipt(receipt_path, plan)
            return plan
        if reviewed != plan:
            raise RuntimePrincipalError(
                "Runtime binding receipt is stale: role, scope, ownership, privileges, or schema changed"
            )
        schema = _ident(target["schema_name"])
        role = _ident(target["user"])
        connection.execute(
            text(f"GRANT CONNECT ON DATABASE {_ident(target['database'])} TO {role}")
        )
        for grant in plan["operator_database_grants"]:
            connection.execute(
                text(
                    f"GRANT TEMPORARY ON DATABASE {_ident(grant['database'])} "
                    f"TO {_ident(grant['grantee'])}"
                )
            )
        for revoke in plan["database_revokes"]:
            grantee = (
                "PUBLIC"
                if revoke["grantee_kind"] == "public"
                else _ident(revoke["grantee"])
            )
            # Default RESTRICT must reject dependent grants, never cascade
            # privilege changes into roles outside the reviewed target.
            connection.execute(
                text(
                    f"REVOKE TEMPORARY ON DATABASE {_ident(revoke['database'])} "
                    f"FROM {grantee} RESTRICT"
                )
            )
        connection.execute(text(runtime_scope_binding_sql(target["schema_name"], cfg)))
        connection.execute(text(f"REVOKE ALL ON SCHEMA {schema} FROM {role}"))
        connection.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {role}"))
        for grant in plan["grants"]:
            connection.execute(
                text(
                    f"REVOKE ALL ON {grant['kind']} {schema}.{_ident(grant['name'])} FROM {role}"
                )
            )
            connection.execute(
                text(
                    f"GRANT {', '.join(grant['privileges'])} ON {grant['kind']} {schema}.{_ident(grant['name'])} TO {role}"
                )
            )
        for function in plan["routine_grants"]:
            connection.execute(
                text(
                    f"REVOKE ALL ON ROUTINE {schema}.{_ident(function['name'])}({function['arguments']}) FROM {role}"
                )
            )
            connection.execute(
                text(
                    f"GRANT EXECUTE ON ROUTINE {schema}.{_ident(function['name'])}({function['arguments']}) TO {role}"
                )
            )
        connection.execute(text(f"REVOKE ALL ON TABLE {schema}.{_SCOPE} FROM {role}"))
        connection.execute(
            text(
                _default_acl_sql(
                    target["schema_name"], target["user"], target["operator_user"]
                )
            )
        )
        _verify_bound_permissions(connection, target, plan)
        database_access = _database_access(connection, target)
        if not database_access["runtime_connect"]:
            raise RuntimePrincipalError(
                "Runtime principal CONNECT to the exact target was not established"
            )
        if database_access["runtime_temp"]:
            raise RuntimePrincipalError(
                "Runtime principal retains effective TEMP on the exact target database"
            )
        if database_access["operator_temp"] != plan["database_access"]["operator_temp"]:
            raise RuntimePrincipalError(
                "Configured operator TEMP privilege was not preserved"
            )
    result = _seal(
        {
            "format": _FORMAT,
            "operation": "bind",
            "status": "applied",
            "plan_sha256": plan["sha256"],
            "target": target,
            "applied_at": datetime.now(UTC).isoformat(),
            "scope": plan["scope"],
            "grants": plan["grants"],
            "database_access": database_access,
            "database_grants": plan["database_grants"],
            "database_revokes": plan["database_revokes"],
            "operator_database_grants": plan["operator_database_grants"],
            "runtime_temp_denied": True,
            "runtime_session_requirement": plan["runtime_session_requirement"],
            "privileges_verified": True,
            "default_privileges": plan["planned_default_privileges"],
        }
    )
    _write_receipt(result_path, result)
    return result


def grant_proven_runtime_sequences(
    connection_or_session: Connection | Session, cfg: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Grant classified allocators after explicit operator provisioning.

    The caller owns one active REPEATABLE READ/SERIALIZABLE transaction and
    its commit. This is the fresh/provisioning API, not a restore or migration
    shortcut around receipt-bound runtime binding. Direct library loaders
    must supply cfg here explicitly or use the separate bind lifecycle.
    """
    connection = (
        connection_or_session.connection()
        if isinstance(connection_or_session, Session)
        else connection_or_session
    )
    plan = build_runtime_principal_binding_plan(connection, cfg)
    target = plan["target"]
    schema, runtime = _ident(target["schema_name"]), _ident(target["user"])
    grants = [grant for grant in plan["grants"] if grant["kind"] == "SEQUENCE"]
    for grant in grants:
        sequence = f"{schema}.{_ident(grant['name'])}"
        connection.execute(text(f"REVOKE ALL ON SEQUENCE {sequence} FROM {runtime}"))
        connection.execute(
            text(f"GRANT USAGE, SELECT ON SEQUENCE {sequence} TO {runtime}")
        )
    _verify_bound_permissions(connection, target, plan)
    return grants


__all__ = [
    "RuntimePrincipalError",
    "bind_runtime_principal",
    "bootstrap_runtime_principal",
    "build_runtime_principal_binding_plan",
    "build_runtime_principal_bootstrap_plan",
    "grant_proven_runtime_sequences",
    "operator_connection",
    "operator_session",
    "runtime_schema_grants_sql",
    "runtime_scope_binding_sql",
]
