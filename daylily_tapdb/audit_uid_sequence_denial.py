"""Receipt-bound removal of direct runtime access to the owned audit UID sequence.

This operator lifecycle operation does not repair inherited/PUBLIC authority,
create allocators, change sequence state, or grant any privilege.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from sqlalchemy import text

from daylily_tapdb.identity_inventory import quote_identifier, seal_receipt, validate_receipt

VERSION = "tapdb-audit-uid-sequence-denial/v1"


class AuditUIDSequenceDenialError(ValueError):
    """The exact native audit allocator cannot be safely denied to the runtimes."""


def plan_audit_uid_sequence_denial(
    connection: Any, cfg: Mapping[str, Any], *, roles: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Read only: bind identity ownership, complete ACL and direct revocations.

    With no explicit role list, adoption covers every existing scope binding and
    its selected principal. Binding supplies only its exact runtime principal.
    Missing/ambiguous or externally owned allocators are unsupported.
    """
    schema, operator = cfg["schema_name"], cfg["operator_user"]
    ns = quote_identifier(schema)
    identity = dict(connection.execute(text("""/* tapdb_audit_uid:identity */
        SELECT current_database() AS database, session_user::text AS session_user,
               current_user::text AS current_user,
               (SELECT oid::bigint FROM pg_catalog.pg_database
                WHERE datname=current_database()) AS database_oid
    """)).mappings().one())
    if (identity["database"] != cfg["database"]
            or identity["session_user"] != operator or identity["current_user"] != operator):
        raise AuditUIDSequenceDenialError("Exact native operator/database required")
    if roles is None:
        roles = list(connection.execute(text(
            f"SELECT role_name::text FROM {ns}.tapdb_runtime_principal_scope ORDER BY role_name"
        )).scalars()) + [cfg["user"]]
    if isinstance(roles, (str, bytes)) or not roles:
        raise AuditUIDSequenceDenialError("Explicit runtime role set required")
    names = sorted(set(roles))
    for name in names:
        quote_identifier(name)
        if name == operator:
            raise AuditUIDSequenceDenialError("Operator cannot be a denied runtime")
    rows = [dict(row) for row in connection.execute(text("""/* tapdb_audit_uid:ownership */
        SELECT s.oid::bigint AS oid, s.relname::text AS name,
               sn.nspname::text AS schema, s.relowner::bigint AS owner_oid,
               pg_catalog.pg_get_userbyid(s.relowner)::text AS owner,
               t.oid::bigint AS table_oid, t.relname::text AS table_name,
               pg_catalog.pg_get_userbyid(t.relowner)::text AS table_owner,
               a.attnum::int AS column_number, a.attname::text AS column_name,
               a.attidentity::text AS identity_kind, a.atttypid::bigint AS type_oid,
               d.deptype::text AS dependency_type,
               (SELECT count(*) FROM pg_catalog.pg_depend x
                WHERE x.classid='pg_catalog.pg_class'::regclass AND x.objid=s.oid
                  AND x.refclassid='pg_catalog.pg_class'::regclass
                  AND x.deptype IN ('a','i')) AS ownership_dependencies
          FROM pg_catalog.pg_class t
          JOIN pg_catalog.pg_namespace tn ON tn.oid=t.relnamespace
          JOIN pg_catalog.pg_attribute a ON a.attrelid=t.oid
          JOIN pg_catalog.pg_depend d ON d.refclassid='pg_catalog.pg_class'::regclass
               AND d.refobjid=t.oid AND d.refobjsubid=a.attnum
               AND d.classid='pg_catalog.pg_class'::regclass AND d.deptype IN ('a','i')
          JOIN pg_catalog.pg_class s ON s.oid=d.objid AND s.relkind='S'
          JOIN pg_catalog.pg_namespace sn ON sn.oid=s.relnamespace
         WHERE tn.nspname=:schema AND t.relname='audit_log' AND t.relkind='r'
           AND a.attname='uid' AND a.attnum>0 AND NOT a.attisdropped
         ORDER BY s.oid
    """), {"schema": schema}).mappings()]
    if len(rows) != 1:
        raise AuditUIDSequenceDenialError("One owned native audit UID allocator required")
    sequence = rows[0]
    if (sequence["schema"] != schema or sequence["name"] != "audit_log_uid_seq"
            or sequence["owner"] != operator or sequence["table_owner"] != operator
            or sequence["identity_kind"] not in {"a", "d"}
            or sequence["type_oid"] != 20 or sequence["dependency_type"] != "i"
            or sequence["ownership_dependencies"] != 1):
        raise AuditUIDSequenceDenialError("Native audit UID identity/ownership contract differs")
    acl = [dict(row) for row in connection.execute(text("""/* tapdb_audit_uid:acl */
        SELECT x.grantor::bigint AS grantor, x.grantee::bigint AS grantee,
               x.privilege_type AS privilege, x.is_grantable AS grantable
          FROM pg_catalog.pg_class s,
               LATERAL pg_catalog.aclexplode(COALESCE(s.relacl,
                   pg_catalog.acldefault('S',s.relowner))) x
         WHERE s.oid=:oid ORDER BY grantor,grantee,privilege,grantable
    """), {"oid": sequence["oid"]}).mappings()]
    principals, revokes = [], []
    for name in names:
        state = dict(connection.execute(text("""/* tapdb_audit_uid:principal */
            SELECT r.oid::bigint AS oid, r.rolname::text AS name,
                   r.rolsuper AS superuser,
                   pg_catalog.pg_has_role(r.oid,:owner,'MEMBER') AS owner_member,
                   EXISTS (SELECT 1 FROM pg_catalog.pg_roles p
                           WHERE p.rolname IN ('pg_read_all_data','pg_write_all_data')
                             AND pg_catalog.pg_has_role(r.oid,p.oid,'USAGE')) AS predefined_authority,
                   EXISTS (SELECT 1 FROM pg_catalog.pg_class s,
                           LATERAL pg_catalog.aclexplode(COALESCE(s.relacl,
                               pg_catalog.acldefault('S',s.relowner))) x
                           WHERE s.oid=:sequence AND x.grantee<>r.oid
                             AND CASE WHEN x.grantee=0 THEN true
                                 ELSE pg_catalog.pg_has_role(r.oid,x.grantee,'USAGE') END
                          ) AS external_authority,
                   pg_catalog.has_sequence_privilege(r.oid,:sequence,'USAGE') AS usage,
                   pg_catalog.has_sequence_privilege(r.oid,:sequence,'SELECT') AS select,
                   pg_catalog.has_sequence_privilege(r.oid,:sequence,'UPDATE') AS update
              FROM pg_catalog.pg_roles r WHERE r.rolname=:role
        """), {"owner": sequence["owner_oid"], "sequence": sequence["oid"],
                 "role": name}).mappings().one())
        if any(state[key] for key in ("superuser", "owner_member", "predefined_authority", "external_authority")):
            raise AuditUIDSequenceDenialError("Runtime audit UID authority is not exclusively direct: " + name)
        direct = [entry for entry in acl if entry["grantee"] == state["oid"]]
        if any(entry["grantor"] == state["oid"] for entry in acl):
            raise AuditUIDSequenceDenialError("Runtime audit UID downstream grants require separate review: " + name)
        if any(entry["grantor"] != sequence["owner_oid"]
               or entry["privilege"] not in {"USAGE", "SELECT", "UPDATE"} for entry in direct):
            raise AuditUIDSequenceDenialError("Runtime audit UID grant provenance is unsupported: " + name)
        privileges = sorted({entry["privilege"] for entry in direct})
        if {p for p in ("USAGE", "SELECT", "UPDATE") if state[p.lower()]} != set(privileges):
            raise AuditUIDSequenceDenialError("Unexplained effective audit UID authority: " + name)
        principals.append(state)
        if privileges:
            revokes.append({"role": name, "role_oid": state["oid"],
                            "privileges": privileges, "behavior": "RESTRICT"})
    return seal_receipt({"schema_version": VERSION, "identity": identity,
                         "schema": schema, "operator": operator, "sequence": sequence,
                         "acl": acl, "principals": principals, "revokes": revokes})


def apply_audit_uid_sequence_denial(connection: Any, cfg: Mapping[str, Any], *, plan: Mapping[str, Any]) -> dict[str, Any]:
    """Apply only reviewed direct revocations in the caller's transaction."""
    validate_receipt(plan, VERSION)
    names = [role["name"] for role in plan["principals"]]
    if plan_audit_uid_sequence_denial(connection, cfg, roles=names) != plan:
        raise AuditUIDSequenceDenialError("Reviewed audit UID denial plan changed")
    qualified = quote_identifier(plan["schema"]) + "." + quote_identifier(plan["sequence"]["name"])
    for revoke in plan["revokes"]:
        connection.execute(text(f"REVOKE {', '.join(revoke['privileges'])} ON SEQUENCE {qualified} "
                                f"FROM {quote_identifier(revoke['role'])} RESTRICT"))
    after = plan_audit_uid_sequence_denial(connection, cfg, roles=names)
    denied_oids = {entry["role_oid"] for entry in plan["revokes"]}
    expected_acl = [entry for entry in plan["acl"] if entry["grantee"] not in denied_oids]
    if (after["sequence"] != plan["sequence"] or after["identity"] != plan["identity"]
            or after["acl"] != expected_acl or after["revokes"]
            or any(role[key] for role in after["principals"] for key in ("usage", "select", "update"))):
        raise AuditUIDSequenceDenialError("Audit UID denial or unrelated ACL preservation failed")
    return seal_receipt({"schema_version": VERSION, "status": "applied_in_transaction",
                         "plan_sha256": plan["sha256"], "after_sha256": after["sha256"],
                         "sequence": plan["sequence"], "revokes": plan["revokes"],
                         "effective_runtime_privileges_absent": True,
                         "unrelated_acl_entries_preserved": True})
