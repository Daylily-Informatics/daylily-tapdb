"""Read-only historical principal, ACL, database and connection evidence.

No schema bootstrap, role changes, binding, allocator or writer-fence actions.
Only allowlisted catalog fields are selected; never query text or passwords.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import text

from daylily_tapdb.identity_inventory import (
    physical_target,
    quote_identifier,
    seal_receipt,
    validate_target,
)
from daylily_tapdb.runtime_principal import operator_connection


class PrincipalCensusError(ValueError):
    """Incomplete census or invalid explicit census inputs."""


def _rows(
    connection: Any, section: str, sql: str, parameters: dict[str, Any], limit: int
) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in connection.execute(
            text(f"/* tapdb_census:{section} */ {sql} LIMIT :census_limit"),
            dict(parameters, census_limit=limit + 1),
        ).mappings()
    ]
    if len(rows) > limit:
        raise PrincipalCensusError(
            f"Census section {section} exceeds max_catalog_rows={limit}; no complete receipt"
        )
    return rows


def capture_principal_census(
    cfg: Mapping[str, Any],
    *,
    database_names: Sequence[str] = (),
    statement_timeout_ms: int = 10000,
    max_catalog_rows: int = 100000,
) -> dict[str, Any]:
    """Inspect the configured source and explicitly named databases on its server.

    Named database existence is authoritative pg_database evidence, not a
    connection attempt. It does not certify successful login to that database.
    An operator connection/authorization error propagates; it is never absence.
    """
    for name, value in (
        ("statement_timeout_ms", statement_timeout_ms),
        ("max_catalog_rows", max_catalog_rows),
    ):
        if type(value) is not int or not 1 <= value <= 2147483646:
            raise PrincipalCensusError(f"{name} must be a positive bounded integer")
    if isinstance(database_names, (str, bytes)):
        raise PrincipalCensusError(
            "database_names must be an explicit sequence of database names"
        )
    names = list(dict.fromkeys([cfg["database"], *database_names]))
    if len(names) > max_catalog_rows:
        raise PrincipalCensusError("Named database count exceeds max_catalog_rows")
    for name in names:
        if (
            not isinstance(name, str)
            or name != name.strip()
            or any(ord(c) < 32 for c in name)
            or len(name.encode("utf-8")) > 63
        ):
            raise PrincipalCensusError(
                "Database names must be exact nonempty PostgreSQL identifiers"
            )
        quote_identifier(name)
    target = {
        key: cfg[key]
        for key in (
            "engine_type",
            "host",
            "port",
            "database",
            "schema_name",
            "domain_code",
            "owner_repo_name",
        )
    }
    target["config_identity"] = str(cfg["config_path"])
    if "server_port" in cfg:
        target["server_port"] = cfg["server_port"]
    target = validate_target(target, str(cfg["schema_name"]))
    params = {"schema": cfg["schema_name"], "databases": names}
    with operator_connection(
        cfg, isolation_level="REPEATABLE READ", read_only=True
    ) as connection:
        connection.execute(
            text(
                "SELECT pg_catalog.set_config('statement_timeout', :timeout, true), pg_catalog.set_config('lock_timeout', :timeout, true)"
            ),
            {"timeout": str(statement_timeout_ms)},
        )
        connection.execute(text("SET LOCAL TimeZone = 'UTC'"))
        physical = physical_target(connection, target)
        identity = _rows(
            connection,
            "identity",
            """
            SELECT session_user::text AS operator, current_user::text AS current_role,
                   current_setting('server_version') AS server_version,
                   current_setting('transaction_read_only') AS read_only,
                   pg_catalog.statement_timestamp()::text AS observed_at,
                   r.rolsuper OR pg_catalog.pg_has_role(r.oid, 'pg_read_all_stats', 'USAGE') AS activity_complete
              FROM pg_catalog.pg_roles r WHERE r.rolname = session_user
        """,
            params,
            max_catalog_rows,
        )[0]
        if (
            identity["operator"] != cfg["operator_user"]
            or identity["current_role"] != cfg["operator_user"]
            or identity["read_only"] != "on"
        ):
            raise PrincipalCensusError(
                "Census requires the exact authenticated operator and a read-only transaction"
            )
        databases = _rows(
            connection,
            "databases",
            """
            SELECT d.oid::bigint AS oid, d.datname::text AS name, d.datdba::bigint AS owner_oid,
                   pg_catalog.pg_get_userbyid(d.datdba) AS owner, d.datacl::text AS acl,
                   d.datallowconn AS connections_allowed, d.datconnlimit AS connection_limit
              FROM pg_catalog.pg_database d WHERE d.datname = ANY(:databases) ORDER BY d.datname
        """,
            params,
            max_catalog_rows,
        )
        schemas = _rows(
            connection,
            "schemas",
            """
            SELECT n.oid::bigint AS oid, n.nspname::text AS name, n.nspowner::bigint AS owner_oid,
                   pg_catalog.pg_get_userbyid(n.nspowner) AS owner, n.nspacl::text AS acl
              FROM pg_catalog.pg_namespace n WHERE n.nspname=:schema
        """,
            params,
            max_catalog_rows,
        )
        objects = _rows(
            connection,
            "objects",
            """
            SELECT 'relation' AS object_type, c.oid::bigint AS oid, c.relname::text AS name,
                   c.relkind::text AS kind, c.relowner::bigint AS owner_oid,
                   pg_catalog.pg_get_userbyid(c.relowner) AS owner, c.relacl::text AS acl,
                   c.relrowsecurity AS rls_enabled, c.relforcerowsecurity AS rls_forced
              FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname=:schema
            UNION ALL
            SELECT 'routine', p.oid::bigint,
                   p.proname || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')',
                   p.prokind::text, p.proowner::bigint, pg_catalog.pg_get_userbyid(p.proowner),
                   p.proacl::text, false, false
              FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
             WHERE n.nspname=:schema
            UNION ALL
            SELECT 'type', t.oid::bigint, t.typname::text, t.typtype::text,
                   t.typowner::bigint, pg_catalog.pg_get_userbyid(t.typowner), t.typacl::text, false, false
              FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
             WHERE n.nspname=:schema ORDER BY object_type, name
        """,
            params,
            max_catalog_rows,
        )
        column_acls = _rows(
            connection,
            "column_acls",
            """
            SELECT c.relname::text AS relation, a.attname::text AS column, a.attacl::text AS acl
              FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
              JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname=:schema AND a.attnum>0 AND NOT a.attisdropped AND a.attacl IS NOT NULL
             ORDER BY c.relname,a.attnum
        """,
            params,
            max_catalog_rows,
        )
        default_acls = _rows(
            connection,
            "default_acls",
            """
            SELECT d.oid::bigint AS oid, d.defaclrole::bigint AS owner_oid,
                   pg_catalog.pg_get_userbyid(d.defaclrole) AS owner,
                   d.defaclnamespace::bigint AS namespace_oid, d.defaclobjtype::text AS object_type,
                   d.defaclacl::text AS acl
              FROM pg_catalog.pg_default_acl d LEFT JOIN pg_catalog.pg_namespace n ON n.oid=d.defaclnamespace
             WHERE d.defaclnamespace=0 OR n.nspname=:schema ORDER BY d.oid
        """,
            params,
            max_catalog_rows,
        )
        # Role definitions contain no secrets; all catalog-visible roles retain
        # names referenced by ACL grantors/grantees and transitive memberships.
        roles = _rows(
            connection,
            "roles",
            """
            SELECT oid::bigint AS oid, rolname::text AS name, rolsuper AS superuser,
                   rolinherit AS inherit, rolcreaterole AS create_role, rolcreatedb AS create_database,
                   rolcanlogin AS login, rolreplication AS replication, rolbypassrls AS bypass_rls,
                   rolconnlimit AS connection_limit, rolvaliduntil::text AS valid_until
              FROM pg_catalog.pg_roles ORDER BY rolname
        """,
            params,
            max_catalog_rows,
        )
        memberships = _rows(
            connection,
            "memberships",
            """
            SELECT pg_catalog.pg_get_userbyid(m.roleid) AS role,
                   pg_catalog.pg_get_userbyid(m.member) AS member,
                   pg_catalog.pg_get_userbyid(m.grantor) AS grantor, m.admin_option,
                   m.inherit_option, m.set_option
              FROM pg_catalog.pg_auth_members m ORDER BY m.roleid,m.member,m.grantor
        """,
            params,
            max_catalog_rows,
        )
        privileges = _rows(
            connection,
            "effective_privileges",
            """
            SELECT r.rolname::text AS role, d.datname::text AS database,
                   pg_catalog.has_database_privilege(r.oid,d.oid,'CONNECT') AS connect,
                   pg_catalog.has_database_privilege(r.oid,d.oid,'CREATE') AS create,
                   pg_catalog.has_database_privilege(r.oid,d.oid,'TEMP') AS temp,
                   CASE WHEN d.datname=current_database() AND n.oid IS NOT NULL THEN
                     pg_catalog.has_schema_privilege(r.oid,n.oid,'USAGE') END AS schema_usage,
                   CASE WHEN d.datname=current_database() AND n.oid IS NOT NULL THEN
                     pg_catalog.has_schema_privilege(r.oid,n.oid,'CREATE') END AS schema_create
              FROM pg_catalog.pg_roles r CROSS JOIN pg_catalog.pg_database d
              LEFT JOIN pg_catalog.pg_namespace n ON n.nspname=:schema
             WHERE d.datname=ANY(:databases) ORDER BY d.datname,r.rolname
        """,
            params,
            max_catalog_rows,
        )
        activity = _rows(
            connection,
            "activity",
            """
            SELECT datid::bigint AS database_oid, datname::text AS database, pid,
                   usesysid::bigint AS role_oid, usename::text AS role, application_name,
                   client_addr::text AS client_address, backend_start::text AS backend_start,
                   xact_start::text AS transaction_start, state, wait_event_type, wait_event, backend_type
              FROM pg_catalog.pg_stat_activity WHERE datname=ANY(:databases) ORDER BY datname,pid
        """,
            params,
            max_catalog_rows,
        )
        binding_catalog = _rows(
            connection,
            "binding_catalog",
            """
            SELECT c.oid::bigint AS oid, pg_catalog.row_security_active(c.oid) AS row_security_active,
                   pg_catalog.has_table_privilege(c.oid,'SELECT') AS readable,
                   ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a
                         WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns
              FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname=:schema AND c.relname='tapdb_runtime_principal_scope' AND c.relkind IN ('r','p')
        """,
            params,
            max_catalog_rows,
        )
        scope_columns = (
            "role_name",
            "config_identity",
            "schema_name",
            "domain_code",
            "issuer_app_code",
            "tenant_id",
            "allow_global_rows",
        )
        bindings: dict[str, Any] = {
            "status": "unavailable_historical_structure",
            "rows": [],
        }
        if binding_catalog:
            catalog = binding_catalog[0]
            if not set(scope_columns) <= set(catalog["columns"]):
                bindings["status"] = "unavailable_historical_columns"
            elif not catalog["readable"] or catalog["row_security_active"]:
                bindings["status"] = "unavailable_unfiltered_access"
            else:
                columns = ",".join(
                    quote_identifier(c) + "::text AS " + quote_identifier(c)
                    for c in scope_columns
                )
                bindings = {
                    "status": "available",
                    "rows": _rows(
                        connection,
                        "bindings",
                        f"SELECT {columns} FROM {quote_identifier(cfg['schema_name'])}.tapdb_runtime_principal_scope ORDER BY role_name",
                        params,
                        max_catalog_rows,
                    ),
                }
        known = {d["name"]: d for d in databases}
        return seal_receipt(
            {
                "schema_version": "tapdb-principal-census/v1",
                "target": target,
                "physical_target": physical,
                "authenticated_operator": identity,
                "database_checks": [
                    {
                        "name": name,
                        "exists": name in known,
                        **(
                            {
                                "database": known[name],
                                "physical_target": dict(
                                    physical,
                                    database=name,
                                    database_oid=known[name]["oid"],
                                ),
                            }
                            if name in known
                            else {}
                        ),
                    }
                    for name in names
                ],
                "schemas": schemas,
                "objects": objects,
                "column_acls": column_acls,
                "default_acls": default_acls,
                "roles": roles,
                "memberships": memberships,
                "iam_memberships": [m for m in memberships if m["role"] == "rds_iam"],
                "effective_privileges": privileges,
                "scope_bindings": bindings,
                "activity": {
                    "complete_visibility": identity["activity_complete"],
                    "connections": activity,
                    "boundary": "Database activity is not a complete HTTP/worker/scheduler/client inventory",
                },
                "limits": {
                    "statement_timeout_ms": statement_timeout_ms,
                    "max_catalog_rows": max_catalog_rows,
                },
            }
        )
