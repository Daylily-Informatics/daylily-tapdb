"""Explicit control-first allocator-fence recovery and ACL quarantine.

The public API is exported by :mod:`daylily_tapdb.sequences`. PostgreSQL,
explicitly verified Aurora provider internals, and authorized gate-controlling
administrators are the trusted computing base. Customer allocator sessions,
including competing maintenance sessions, must be excluded.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import asdict
from pathlib import Path
from typing import Any, NoReturn

from sqlalchemy import text

from daylily_tapdb.identity_inventory import (
    InventoryLimits,
    capture_identity_inventory,
    quote_identifier,
    seal_receipt,
    validate_receipt,
    validate_target,
)

TAKEOVER_VERSION = "tapdb-writer-fence-takeover/v1"
FENCE_VERSION = "tapdb-writer-fence/v1"
PROVIDER_VERSION = "tapdb-fence-provider/v1"


def _fail(message: str) -> NoReturn:
    from daylily_tapdb.sequences import SequenceProtectionError

    raise SequenceProtectionError(message)


def read_fence_history(directory: Path) -> tuple[list[Any], Any]:
    from daylily_tapdb.backup.receipts import (
        read_head,
        read_receipts,
        verify_receipt_chain,
    )

    if not directory.is_absolute():
        _fail("Fence reconciliation requires an absolute external receipt directory")
    receipts, head = read_receipts(directory), read_head(directory)
    if (
        not verify_receipt_chain(receipts, head=head).ok
        or (receipts and head is None)
        or len(list(directory.glob("*.json"))) != len(receipts)
    ):
        _fail("Fence reconciliation receipt chain is incomplete or corrupt")
    return receipts, head


def control_state(connection: Any, target: Mapping[str, Any]) -> dict[str, Any]:
    """Verify a different explicit control database and observe the target catalog."""
    exact = validate_target(target, str(target["schema_name"]))
    connection.execute(text("SET LOCAL TimeZone = 'UTC'"))
    url = connection.engine.url
    if (
        not url.database
        or url.database == exact["database"]
        or url.host != exact["host"]
        or url.port != exact["port"]
    ):
        _fail(
            "An explicit different control database on the exact target transport is required"
        )
    row = (
        connection.execute(
            text(
                "SELECT d.datname AS database,d.oid::bigint AS database_oid,d.datallowconn, "
                "d.datdba::bigint AS owner_oid,pg_get_userbyid(d.datdba) AS owner_role, "
                "session_user::text AS operator_role,current_user::text AS current_role, "
                "current_database() AS control_database,inet_server_addr()::text AS server_address, "
                "inet_server_port() AS server_port,current_setting('server_version_num')::int AS server_version_num "
                "FROM pg_database d WHERE d.datname=:database"
            ),
            {"database": exact["database"]},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        _fail("The explicitly identified fenced database does not exist")
    result = dict(row)
    if (
        result["control_database"] != url.database
        or result["operator_role"] != url.username
        or result["current_role"] != result["operator_role"]
        or result["server_port"] != exact.get("server_port", exact["port"])
        or result["server_version_num"] < 160000
    ):
        _fail("Control connection does not match the exact operator/server contract")
    return result


def backend_identity(connection: Any) -> dict[str, Any]:
    row = (
        connection.execute(
            text(
                "SELECT pid,backend_start AT TIME ZONE 'UTC' AS backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()"
            )
        )
        .mappings()
        .one()
    )
    return {"pid": int(row["pid"]), "backend_start": row["backend_start"].isoformat()}


def role_catalog(connection: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    roles = [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT oid::bigint AS oid,rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin, "
                "rolreplication,rolbypassrls,rolconnlimit,rolvaliduntil::text AS rolvaliduntil "
                "FROM pg_roles ORDER BY rolname"
            )
        ).mappings()
    ]
    memberships = [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT roleid::bigint AS roleid,member::bigint AS member,grantor::bigint AS grantor, "
                "admin_option,inherit_option,set_option FROM pg_auth_members ORDER BY roleid,member,grantor"
            )
        ).mappings()
    ]
    return roles, memberships


def provider_evidence(
    connection: Any,
    *,
    target: Mapping[str, Any],
    operator_role: str,
    provider_contract: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Authenticate the explicit Aurora resource, never a role-name heuristic."""
    roles, memberships = role_catalog(connection)
    operator = next((role for role in roles if role["rolname"] == operator_role), None)
    if operator is None or not operator["rolcanlogin"]:
        _fail("Fence operator is not an exact existing LOGIN principal")
    provider = None
    metadata = None
    aurora_version = None
    if target["engine_type"] == "aurora":
        fields = {
            "schema_version",
            "engine",
            "engine_version",
            "aws_profile",
            "region",
            "cluster_identifier",
            "cluster_arn",
            "cluster_resource_id",
            "writer_endpoint",
            "sslmode",
        }
        if (
            not isinstance(provider_contract, Mapping)
            or set(provider_contract) != fields
            or provider_contract["schema_version"] != PROVIDER_VERSION
            or provider_contract["engine"] != "aurora-postgresql"
            or provider_contract["engine_version"] != "16.13"
            or provider_contract["writer_endpoint"] != target["host"]
            or provider_contract["sslmode"] != "verify-full"
            or any(
                not isinstance(value, str) or not value.strip()
                for value in provider_contract.values()
            )
            or connection.engine.url.query.get("sslmode") != "verify-full"
            or not connection.engine.url.query.get("sslrootcert")
        ):
            _fail(
                "Aurora fencing requires its explicit pinned provider contract and verify-full TLS"
            )
        import boto3

        response = (
            boto3.Session(profile_name=provider_contract["aws_profile"])
            .client("rds", region_name=provider_contract["region"])
            .describe_db_clusters(
                DBClusterIdentifier=provider_contract["cluster_identifier"]
            )
        )
        clusters = response.get("DBClusters", [])
        if len(clusters) != 1:
            _fail(
                "Authenticated provider lookup did not identify exactly one Aurora cluster"
            )
        keys = {
            "Engine": "engine",
            "EngineVersion": "engine_version",
            "DBClusterIdentifier": "cluster_identifier",
            "DBClusterArn": "cluster_arn",
            "DbClusterResourceId": "cluster_resource_id",
            "Endpoint": "writer_endpoint",
        }
        if any(
            clusters[0].get(key) != provider_contract[field]
            for key, field in keys.items()
        ):
            _fail(
                "Authenticated Aurora metadata differs from the pinned provider contract"
            )
        metadata = {key: clusters[0][key] for key in keys}
        provider = next((role for role in roles if role["rolname"] == "rdsadmin"), None)
        required_true = (
            "rolsuper",
            "rolcanlogin",
            "rolcreatedb",
            "rolcreaterole",
            "rolreplication",
            "rolbypassrls",
        )
        if (
            provider is None
            or any(provider[field] is not True for field in required_true)
            or operator["rolsuper"] is not False
            or any(
                item["roleid"] == provider["oid"] or item["member"] == provider["oid"]
                for item in memberships
            )
        ):
            _fail(
                "Aurora provider/operator role attributes or memberships are unsupported"
            )
        version = connection.execute(
            text("SELECT current_setting('server_version_num')::int")
        ).scalar_one()
        if version != 160013:
            _fail(
                "The live server does not corroborate the pinned Aurora provider identity"
            )
        # Aurora exposes this provider builtin without a pg_proc row. Resolve
        # it directly in pg_catalog, never through an application search path.
        aurora_version = connection.execute(
            text("SELECT pg_catalog.aurora_version()")
        ).scalar_one()
        if not isinstance(aurora_version, str) or not aurora_version.strip():
            _fail("Aurora version identity is unavailable")
    elif provider_contract is not None:
        _fail("Community PostgreSQL must not receive an Aurora provider exception")
    permitted_superusers = {operator_role} | (
        {provider["rolname"]} if provider else set()
    )
    if any(
        role["rolsuper"] and role["rolname"] not in permitted_superusers
        for role in roles
    ):
        _fail("An unknown privileged role prevents allocator fencing")
    return {
        "provider_contract": dict(provider_contract)
        if provider_contract is not None
        else None,
        "provider_metadata": metadata,
        "provider_role": provider,
        "aurora_version": aurora_version,
        "operator_role": operator_role,
        "roles": roles,
        "memberships": memberships,
    }


def census(
    connection: Any,
    *,
    database: str,
    database_oid: int,
    retained_backend: Mapping[str, Any] | None = None,
) -> None:
    """Require complete fresh statistics and no other target backend of any type."""
    visible = connection.execute(
        text(
            "SELECT rolsuper OR pg_has_role(session_user,'pg_read_all_stats','USAGE') "
            "FROM pg_roles WHERE rolname=session_user"
        )
    ).scalar_one()
    if visible is not True:
        _fail("Complete pg_read_all_stats visibility is required for writer fencing")
    connection.execute(text("SELECT pg_stat_clear_snapshot()"))
    rows = (
        connection.execute(
            text(
                "SELECT pid,backend_start AT TIME ZONE 'UTC' AS backend_start,backend_type,usename "
                "FROM pg_stat_activity WHERE datid=:oid ORDER BY pid"
            ),
            {"oid": database_oid},
        )
        .mappings()
        .all()
    )
    for row in rows:
        identity = {
            "pid": row["pid"],
            "backend_start": row["backend_start"].isoformat()
            if row["backend_start"]
            else None,
        }
        if (
            identity != retained_backend
            or not row["backend_type"]
            or not row["usename"]
        ):
            _fail(
                "Allocator sessions or unsupported background workers remain: "
                f"pid={row['pid']}, backend_type={row['backend_type']!r}, "
                f"backend_start={identity['backend_start']!r}, "
                f"expected_retained_backend={retained_backend!r}"
            )
    prepared = connection.execute(
        text("SELECT count(*) FROM pg_prepared_xacts WHERE database=:database"),
        {"database": database},
    ).scalar_one()
    subscriptions = connection.execute(
        text("SELECT count(*) FROM pg_subscription WHERE subdbid=:oid AND subenabled"),
        {"oid": database_oid},
    ).scalar_one()
    if prepared or subscriptions:
        _fail(
            "Prepared transactions or enabled target subscriptions prevent allocator fencing"
        )


def worker_baseline(
    connection: Any, *, database_oid: int, operator_role: str, aurora: bool
) -> dict[str, Any]:
    """Reject startup-loaded schedulers/code outside the explicit supported TCB."""
    names = (
        "shared_preload_libraries",
        "session_preload_libraries",
        "local_preload_libraries",
    )
    settings = {
        name: connection.execute(
            text("SELECT current_setting(:name)"), {"name": name}
        ).scalar_one()
        for name in names
    }
    overrides = [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT s.setdatabase::bigint AS database_oid,s.setrole::bigint AS role_oid,item AS setting "
                "FROM pg_db_role_setting s CROSS JOIN LATERAL unnest(s.setconfig) AS item "
                "WHERE s.setdatabase IN (0,:oid) AND (s.setrole=0 OR s.setrole=(SELECT oid FROM pg_roles WHERE rolname=:role)) "
                "AND split_part(item,'=',1) IN ('shared_preload_libraries','session_preload_libraries','local_preload_libraries') "
                "ORDER BY s.setdatabase,s.setrole,item"
            ),
            {"oid": database_oid, "role": operator_role},
        ).mappings()
    ]
    # These are the provider-loaded baseline on the authenticated Aurora16.13
    # target, not customer schedulers or arbitrary configurable extensions.
    allowed = {"pg_stat_statements"} | (
        {"rdsutils", "rds_casts", "writeforward", "aws_s3_native", "rds_blue_green"}
        if aurora
        else set()
    )
    for value in [
        *settings.values(),
        *(item["setting"].split("=", 1)[1] for item in overrides),
    ]:
        libraries = {
            part.strip().strip('"') for part in value.split(",") if part.strip()
        }
        if libraries - allowed:
            _fail(
                "Unsupported startup-loaded code or scheduler prevents allocator fencing"
            )
    return {"settings": settings, "target_operator_overrides": overrides}


def target_extensions(connection: Any) -> list[dict[str, Any]]:
    extensions = [
        dict(row)
        for row in connection.execute(
            text("SELECT extname,extversion FROM pg_extension ORDER BY extname")
        ).mappings()
    ]
    if any(
        row["extname"] not in {"plpgsql", "pg_stat_statements", "pgcrypto", "uuid-ossp"}
        for row in extensions
    ):
        _fail("Unsupported target extension prevents allocator fencing")
    return extensions


def acl_snapshot(connection: Any, database_oid: int) -> dict[str, Any]:
    """Retain complete semantic ACLs without passwords or unrelated settings."""
    default = connection.execute(
        text("SELECT datacl IS NULL FROM pg_database WHERE oid=:oid"),
        {"oid": database_oid},
    ).scalar_one()
    entries = [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT a.grantor::bigint AS grantor,a.grantee::bigint AS grantee, "
                "pg_get_userbyid(a.grantor) AS grantor_name, "
                "CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee_name, "
                "a.privilege_type,a.is_grantable FROM pg_database d "
                "CROSS JOIN LATERAL aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a "
                "WHERE d.oid=:oid ORDER BY a.grantee,a.grantor,a.privilege_type,a.is_grantable"
            ),
            {"oid": database_oid},
        ).mappings()
    ]
    return {"was_default": default, "entries": entries}


def _restorable_acl(acl: Mapping[str, Any], state: Mapping[str, Any]) -> None:
    if state["owner_role"] != state["operator_role"]:
        _fail("ACL quarantine requires the authenticated database owner")
    if any(
        item["privilege_type"] == "CONNECT" and item["grantor"] != state["owner_oid"]
        for item in acl["entries"]
    ):
        _fail("CONNECT grantor chains are unsupported for exact ACL restoration")


def exclusive_acl(
    connection: Any, *, state: Mapping[str, Any], provider: Mapping[str, Any]
) -> None:
    """Check effective inherited CONNECT, never merely direct ACL entries."""
    provider_role = provider["provider_role"]
    permitted = {state["operator_role"]} | (
        {provider_role["rolname"]} if provider_role else set()
    )
    logins = set(
        connection.execute(
            text(
                "SELECT rolname FROM pg_roles WHERE rolcanlogin AND has_database_privilege(oid,:oid,'CONNECT')"
            ),
            {"oid": state["database_oid"]},
        ).scalars()
    )
    if state["operator_role"] not in logins or logins - permitted:
        _fail("Customer LOGIN CONNECT is not exclusively quarantined to the operator")
    # Membership in the operator, even without INHERIT, can permit SET ROLE or
    # future membership changes. A database-only quarantine does not repair it.
    operator_oid = next(
        role["oid"]
        for role in provider["roles"]
        if role["rolname"] == state["operator_role"]
    )
    parents = {operator_oid}
    while True:
        expanded = parents | {
            m["member"] for m in provider["memberships"] if m["roleid"] in parents
        }
        if expanded == parents:
            break
        parents = expanded
    if any(
        role["rolcanlogin"] and role["oid"] != operator_oid and role["oid"] in parents
        for role in provider["roles"]
    ):
        _fail("Customer membership into the operator prevents database-only quarantine")


def _set_connect_acl(
    connection: Any, state: Mapping[str, Any], entries: list[dict[str, Any]]
) -> None:
    database = quote_identifier(state["database"])
    current = acl_snapshot(connection, state["database_oid"])
    _restorable_acl(current, state)
    for name in sorted(
        {
            item["grantee_name"]
            for item in current["entries"]
            if item["privilege_type"] == "CONNECT"
        }
    ):
        grantee = "PUBLIC" if name == "PUBLIC" else quote_identifier(name)
        connection.execute(
            text(f"REVOKE CONNECT ON DATABASE {database} FROM {grantee}")
        )
    for entry in entries:
        if entry["privilege_type"] != "CONNECT":
            continue
        grantee = (
            "PUBLIC"
            if entry["grantee"] == 0
            else quote_identifier(entry["grantee_name"])
        )
        grant_option = " WITH GRANT OPTION" if entry["is_grantable"] else ""
        connection.execute(
            text(f"GRANT CONNECT ON DATABASE {database} TO {grantee}{grant_option}")
        )


def quarantine_acl(
    connection: Any, *, state: Mapping[str, Any], provider: Mapping[str, Any]
) -> dict[str, Any]:
    before = acl_snapshot(connection, state["database_oid"])
    _restorable_acl(before, state)
    _set_connect_acl(
        connection,
        state,
        [
            {
                "privilege_type": "CONNECT",
                "grantee": state["owner_oid"],
                "grantee_name": state["operator_role"],
                "is_grantable": False,
            }
        ],
    )
    exclusive_acl(connection, state=state, provider=provider)
    after = acl_snapshot(connection, state["database_oid"])
    if [item for item in before["entries"] if item["privilege_type"] != "CONNECT"] != [
        item for item in after["entries"] if item["privilege_type"] != "CONNECT"
    ]:
        _fail("Quarantine changed a non-CONNECT database privilege")
    return after


def restore_acl(
    connection: Any, *, state: Mapping[str, Any], original_acl: Mapping[str, Any]
) -> None:
    _restorable_acl(original_acl, state)
    _set_connect_acl(connection, state, original_acl["entries"])
    if (
        acl_snapshot(connection, state["database_oid"])["entries"]
        != original_acl["entries"]
    ):
        _fail("Original database ACL restoration did not verify")


def lock_session(connection: Any, database_oid: int) -> None:
    """Serialize a target epoch on its recorded control database, or target DB."""
    key = f"tapdb:allocator-fence:{database_oid}"
    if connection.info.get("tapdb_allocator_fence_lock") == key:
        return
    if connection.info.get("tapdb_allocator_fence_lock") is not None:
        _fail("Maintenance session already controls a different fence")
    if not connection.execute(
        text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": key}
    ).scalar_one():
        _fail("Another maintenance session holds the database fence epoch lock")
    connection.info["tapdb_allocator_fence_lock"] = key


def unlock_session(connection: Any) -> None:
    key = connection.info.pop("tapdb_allocator_fence_lock", None)
    if key is not None:
        connection.execute(
            text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"), {"key": key}
        )


def _detail_target(receipt: Any) -> Any:
    detail = receipt.detail
    return detail.get("target", detail.get("receipt", {}).get("target"))


def active_epoch(receipts: list[Any], target: Mapping[str, Any]) -> Any:
    relevant = [
        item
        for item in receipts
        if item.operation == "sequence_writer_fence" and _detail_target(item) == target
    ]
    intents = [
        item
        for item in relevant
        if item.detail.get("phase")
        in {"acquire_intent", "takeover_intent", "release_intent"}
    ]
    if not intents:
        return None
    latest = intents[-1]
    if latest.detail["phase"] == "release_intent" and any(
        item.detail.get("phase") in {"released", "release_reconciled"}
        and item.detail.get("receipt", {}).get("intent_receipt_id") == latest.receipt_id
        for item in relevant
    ):
        return None
    return latest


def journal(
    directory: Path, *, phase: str, actor: str, detail: Mapping[str, Any]
) -> Any:
    from daylily_tapdb.backup.receipts import Actor, write_receipt

    read_fence_history(directory)
    detail = dict(detail)
    if "recovery_family" in detail.get("receipt", {}):
        if (
            "recovery_family" in detail
            and detail["recovery_family"] != detail["receipt"]["recovery_family"]
        ):
            _fail(
                "Writer-fence envelope and receipt declare different recovery families"
            )
        detail["recovery_family"] = detail["receipt"]["recovery_family"]
    if "recovery_family" in detail:
        from daylily_tapdb.backup.recovery import (
            require_family_member,
            validate_recovery_family,
        )

        family = validate_recovery_family(
            detail["recovery_family"], required_directory=directory
        )
        member = detail.get("receipt", detail)
        if member.get("target") != detail.get("target"):
            _fail("Writer-fence envelope and receipt identify different targets")
        # All family-bearing phases prove a physical member before persistence.
        # A database name or an unvalidated predecessor identifier is not proof.
        require_family_member(family, member)
    return write_receipt(
        directory,
        operation="sequence_writer_fence",
        status="intent" if phase.endswith("_intent") else "succeeded",
        actor=Actor(surface="cli", username=actor),
        detail={"phase": phase, **detail},
    )


def retained_floors(
    receipts: list[Any], target: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Retain every target intent, including failed and ambiguous reservations."""
    result: dict[tuple[str, int, str], dict[str, Any]] = {}
    for item in receipts:
        detail = item.detail
        named = detail.get(
            "target",
            detail.get("plan", {}).get(
                "target",
                detail.get("result", {})
                .get("inventory", {})
                .get("target", detail.get("receipt", {}).get("target")),
            ),
        )
        if named != target:
            continue
        values = detail.get(
            "floors", detail.get("receipt", {}).get("retained_floors", [])
        )
        for floor in values:
            if (
                set(floor) != {"name", "value", "source"}
                or type(floor["value"]) is not int
                or floor["value"] < 0
            ):
                _fail("Malformed allocator floor in original recovery journal")
            result[(floor["name"], floor["value"], floor["source"])] = dict(floor)
    return [result[key] for key in sorted(result)]


def _verify_origin(state: Mapping[str, Any], origin: Mapping[str, Any]) -> None:
    physical = origin["physical_target"]
    if any(
        state[field] != physical[field]
        for field in ("database_oid", "server_address", "server_port")
    ):
        _fail("Fence reconciliation targets a different physical database/server")
    if (
        state["operator_role"] != origin["operator_role"]
        or state["control_database"] != origin["control_database"]
    ):
        _fail(
            "Fence reconciliation operator/control database differs from its original intent"
        )
    if not isinstance(origin.get("connection_backend"), Mapping) or set(
        origin["connection_backend"]
    ) != {"pid", "backend_start"}:
        _fail("Original fence intent lacks exact PID and backend_start evidence")


def _record_allocator_observations(
    directory: Path, quarantine: Mapping[str, Any]
) -> None:
    from daylily_tapdb.backup.receipts import Actor, write_receipt

    history, _ = read_fence_history(directory)
    resolved = {
        item.detail.get(
            "intent_receipt_id", item.detail.get("result", {}).get("intent_receipt_id")
        )
        for item in history
        if item.operation == "sequence_advance"
        and item.detail.get("phase")
        in {"committed", "rolled_back", "reconciled_observed"}
    }
    for item in history:
        if (
            item.operation != "sequence_advance"
            or item.detail.get("phase") != "intent"
            or item.detail["plan"]["target"] != quarantine["target"]
            or item.receipt_id in resolved
        ):
            continue
        family = item.detail.get("recovery_family")
        if family != quarantine.get("recovery_family"):
            _fail("Allocator observation cannot change its original recovery family")
        write_receipt(
            directory,
            operation="sequence_advance",
            status="observed",
            actor=Actor(surface="cli", username=quarantine["operator_role"]),
            detail={
                "phase": "reconciled_observed",
                "intent_receipt_id": item.receipt_id,
                "plan_sha256": item.detail["plan"]["sha256"],
                "target": quarantine["target"],
                "observed_inventory": quarantine["sequence_inventory"],
                "floors": item.detail["floors"],
                "prior_transaction_outcome": "unknown",
                "reconciliation_receipt_sha256": quarantine["sha256"],
                **({"recovery_family": family} if family is not None else {}),
            },
        )


def _record_recovery_observations(
    connection: Any, control: Any, directory: Path, quarantine: Mapping[str, Any]
) -> None:
    from daylily_tapdb.backup.recovery import retained_recovery_state

    pending = retained_recovery_state(
        directory, target=quarantine["target"], require_terminal=False
    )["pending"]
    if not pending:
        return
    from daylily_tapdb.backup.receipts import Actor
    from daylily_tapdb.backup.recovery import reconcile_recovery_observed

    for operation_id in pending:
        proof = validate_writer_quarantine(
            connection,
            control_connection=control,
            quarantine_receipt=quarantine,
            receipts_dir=directory,
            provider_contract=quarantine["provider_evidence"]["provider_contract"],
            recovery_family=quarantine.get("recovery_family"),
        )
        reconcile_recovery_observed(
            connection,
            directory=directory,
            operation_id=operation_id,
            quarantine_proof=proof,
            actor=Actor(surface="cli", username=quarantine["operator_role"]),
        )


def build_writer_fence_takeover_plan(
    control_connection: Any,
    *,
    target: Mapping[str, Any],
    receipts_dir: Path,
    fence_intent_receipt_id: str,
    provider_contract: Mapping[str, Any] | None = None,
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Read-only control-first review for a dead epoch; never connects to target."""
    if control_connection.in_transaction():
        _fail("Takeover planning requires a transaction-free control session")
    exact = validate_target(target, str(target["schema_name"]))
    if "inventory_limits" in target:
        target = dict(
            target,
            inventory_limits=asdict(InventoryLimits.parse(target["inventory_limits"])),
        )
    receipts, head = read_fence_history(receipts_dir)
    epoch = active_epoch(receipts, exact)
    if epoch is None or epoch.receipt_id != fence_intent_receipt_id:
        _fail("Explicit latest unresolved fence intent is required")
    origin = epoch.detail
    if origin.get("recovery_family") != recovery_family:
        _fail("Explicit recovery family must exactly match the original fence epoch")
    family_fields: dict[str, Any] = {}
    floors = retained_floors(receipts, exact)
    if recovery_family is not None:
        from daylily_tapdb.backup.recovery import (
            recovery_family_state,
            validate_recovery_family,
        )

        family = validate_recovery_family(
            recovery_family, required_directory=receipts_dir
        )
        family_state = recovery_family_state(family, require_terminal=False)
        family_fields = {
            "recovery_family": family,
            "family_journal_heads": family_state["journal_heads"],
        }
        floors.extend(family_state["floors"])
    with control_connection.begin():
        state = control_state(control_connection, exact)
        lock_session(control_connection, state["database_oid"])
        try:
            _verify_origin(state, origin)
            provider = provider_evidence(
                control_connection,
                target=exact,
                operator_role=state["operator_role"],
                provider_contract=provider_contract,
            )
            if (
                provider["provider_role"]
                != origin["provider_evidence"]["provider_role"]
            ):
                _fail("Provider role identity changed since the original fence epoch")
            census(
                control_connection,
                database=exact["database"],
                database_oid=state["database_oid"],
            )
            baseline = worker_baseline(
                control_connection,
                database_oid=state["database_oid"],
                operator_role=state["operator_role"],
                aurora=exact["engine_type"] == "aurora",
            )
            acl = acl_snapshot(control_connection, state["database_oid"])
            _restorable_acl(origin["original_acl"], state)
            if state["datallowconn"] is not False:
                if origin["phase"] != "takeover_intent":
                    _fail(
                        "Stalled acquire reconciliation requires the target connection gate to be closed"
                    )
                exclusive_acl(control_connection, state=state, provider=provider)
            return seal_receipt(
                {
                    "schema_version": TAKEOVER_VERSION,
                    "phase": "planned",
                    "target": exact,
                    **(
                        {"inventory_limits": target["inventory_limits"]}
                        if "inventory_limits" in target
                        else {}
                    ),
                    "sequence_mappings": dict(
                        target.get("sequence_mappings", origin["sequence_mappings"])
                    ),
                    "physical_target": origin["physical_target"],
                    "operator_role": state["operator_role"],
                    "control_database": state["control_database"],
                    "connection_backend": origin["connection_backend"],
                    "original_acl": origin["original_acl"],
                    "observed_acl": acl,
                    "observed_connections_allowed": state["datallowconn"],
                    "provider_evidence": provider,
                    "worker_baseline": baseline,
                    "predecessor_intent_receipt_id": epoch.receipt_id,
                    "origin_fence_intent_receipt_id": origin.get(
                        "origin_fence_intent_receipt_id"
                    )
                    or epoch.receipt_id,
                    "source_receipts_dir": str(receipts_dir),
                    "source_journal_head": head,
                    "retained_floors": floors,
                    **family_fields,
                }
            )
        finally:
            unlock_session(control_connection)


def apply_writer_fence_takeover(
    control_connection: Any,
    plan: Mapping[str, Any],
    *,
    target_connection_factory: Callable[[], AbstractContextManager[Any]],
    receipts_dir: Path,
) -> dict[str, Any]:
    """Leave explicit durable operator-only quarantine for a NEW human review.

    This does not infer whether an old migration committed. All original intent
    floors remain reserved; no allocator is advanced and no runtime ACL is
    restored. An interrupted phase is explicitly replanned from its new intent.
    """
    from daylily_tapdb.sequences import (
        build_sequence_advance_plan,
        capture_sequence_inventory,
    )

    validate_receipt(plan, TAKEOVER_VERSION)
    if plan["phase"] != "planned" or plan["source_receipts_dir"] != str(receipts_dir):
        _fail(
            "An unchanged reviewed takeover plan and its exact external journal are required"
        )
    target = dict(plan["target"], sequence_mappings=plan["sequence_mappings"])
    if "inventory_limits" in plan:
        target["inventory_limits"] = asdict(
            InventoryLimits.parse(plan["inventory_limits"])
        )
    expected = build_writer_fence_takeover_plan(
        control_connection,
        target=target,
        receipts_dir=receipts_dir,
        fence_intent_receipt_id=plan["predecessor_intent_receipt_id"],
        provider_contract=plan["provider_evidence"]["provider_contract"],
        recovery_family=plan.get("recovery_family"),
    )
    if expected != plan:
        _fail(
            "Stale takeover plan: control state, ACL, provider, or recovery journal changed"
        )
    with control_connection.begin():
        state = control_state(control_connection, target)
        lock_session(control_connection, state["database_oid"])
        if read_fence_history(receipts_dir)[1] != plan["source_journal_head"]:
            _fail("Takeover journal changed while acquiring its epoch lock")
        if (
            state["datallowconn"] != plan["observed_connections_allowed"]
            or acl_snapshot(control_connection, state["database_oid"])
            != plan["observed_acl"]
        ):
            _fail("Takeover ACL/gate changed while acquiring its epoch lock")
        census(
            control_connection,
            database=target["database"],
            database_oid=state["database_oid"],
        )
        intent = journal(
            receipts_dir,
            phase="takeover_intent",
            actor=state["operator_role"],
            detail={
                **{
                    key: value
                    for key, value in plan.items()
                    if key not in {"sha256", "schema_version", "phase"}
                },
                "reviewed_plan_sha256": plan["sha256"],
                "floors": plan["retained_floors"],
            },
        )
        # For an already-quarantined interrupted takeover, closing again is an
        # explicit reviewed transition, never an automatic normal-access repair.
        control_connection.execute(
            text(
                f"ALTER DATABASE {quote_identifier(target['database'])} ALLOW_CONNECTIONS false"
            )
        )
        quarantined_acl = quarantine_acl(
            control_connection, state=state, provider=plan["provider_evidence"]
        )
    journal(
        receipts_dir,
        phase="acl_quarantined",
        actor=state["operator_role"],
        detail={
            "target": plan["target"],
            "physical_target": plan["physical_target"],
            "intent_receipt_id": intent.receipt_id,
            "quarantine_acl": quarantined_acl,
            **(
                {"recovery_family": plan["recovery_family"]}
                if "recovery_family" in plan
                else {}
            ),
        },
    )
    with control_connection.begin():
        census(
            control_connection,
            database=target["database"],
            database_oid=state["database_oid"],
        )
        exclusive_acl(
            control_connection, state=state, provider=plan["provider_evidence"]
        )
        control_connection.execute(
            text(
                f"ALTER DATABASE {quote_identifier(target['database'])} ALLOW_CONNECTIONS true"
            )
        )
    # If opening or reconnecting is ambiguous, the durable ACL quarantine stays
    # in force. There is no exception-path reconnect/reopen/termination.
    with target_connection_factory() as connection:
        if connection.in_transaction():
            _fail(
                "Takeover target factory must yield a transaction-free operator session"
            )
        with connection.begin():
            backend = backend_identity(connection)
            lock_session(connection, state["database_oid"])
        with control_connection.begin():
            control_connection.execute(
                text(
                    f"ALTER DATABASE {quote_identifier(target['database'])} ALLOW_CONNECTIONS false"
                )
            )
        with connection.begin():
            from daylily_tapdb.identity_inventory import physical_target

            if physical_target(connection, plan["target"]) != plan["physical_target"]:
                _fail(
                    "Takeover target factory connected to a different physical database"
                )
            if (
                connection.execute(text("SELECT session_user")).scalar_one()
                != state["operator_role"]
            ):
                _fail("Takeover target factory authenticated a different operator")
            census(
                connection,
                database=target["database"],
                database_oid=state["database_oid"],
                retained_backend=backend,
            )
            extensions = target_extensions(connection)
            baseline = worker_baseline(
                connection,
                database_oid=state["database_oid"],
                operator_role=state["operator_role"],
                aurora=target["engine_type"] == "aurora",
            )
            identity = capture_identity_inventory(
                connection, schema_name=target["schema_name"], target=target
            )
            inventory = capture_sequence_inventory(
                connection, schema_name=target["schema_name"], target=target
            )
            # Also refuses missing generators/definitions and incompatible old floors.
            build_sequence_advance_plan(inventory, floors=plan["retained_floors"])
            census(
                connection,
                database=target["database"],
                database_oid=state["database_oid"],
                retained_backend=backend,
            )
        detail = {
            "schema_version": TAKEOVER_VERSION,
            "phase": "quarantined",
            "target": plan["target"],
            "physical_target": plan["physical_target"],
            "sequence_mappings": plan["sequence_mappings"],
            "operator_role": state["operator_role"],
            "control_database": state["control_database"],
            "connection_backend": backend,
            "previous_connection_backend": plan["connection_backend"],
            "intent_receipt_id": intent.receipt_id,
            "origin_fence_intent_receipt_id": plan["origin_fence_intent_receipt_id"],
            "predecessor_intent_receipt_id": plan["predecessor_intent_receipt_id"],
            "original_acl": plan["original_acl"],
            "quarantine_acl": quarantined_acl,
            "provider_evidence": plan["provider_evidence"],
            "worker_baseline": baseline,
            "target_extensions": extensions,
            "identity_inventory": identity,
            "sequence_inventory": inventory,
            "source_receipts_dir": str(receipts_dir),
            "source_journal_head": read_fence_history(receipts_dir)[1],
            "retained_floors": plan["retained_floors"],
            "old_operation_outcome": "unresolved_observed_only",
            "requires_new_review": True,
            **(
                {"recovery_family": plan["recovery_family"]}
                if "recovery_family" in plan
                else {}
            ),
        }
        journal(
            receipts_dir,
            phase="quarantine_open_intent",
            actor=state["operator_role"],
            detail={"target": plan["target"], "receipt": seal_receipt(detail)},
        )
        with control_connection.begin():
            census(
                control_connection,
                database=target["database"],
                database_oid=state["database_oid"],
                retained_backend=backend,
            )
            exclusive_acl(
                control_connection, state=state, provider=plan["provider_evidence"]
            )
            control_connection.execute(
                text(
                    f"ALTER DATABASE {quote_identifier(target['database'])} ALLOW_CONNECTIONS true"
                )
            )
        with control_connection.begin():
            final = control_state(control_connection, target)
            if (
                final["datallowconn"] is not True
                or acl_snapshot(control_connection, state["database_oid"])["entries"]
                != quarantined_acl["entries"]
            ):
                _fail("Stable operator quarantine could not be confirmed")
            exclusive_acl(
                control_connection, state=state, provider=plan["provider_evidence"]
            )
            census(
                control_connection,
                database=target["database"],
                database_oid=state["database_oid"],
                retained_backend=backend,
            )
            receipt = seal_receipt(detail)
            journal(
                receipts_dir,
                phase="quarantined",
                actor=state["operator_role"],
                detail={"target": plan["target"], "receipt": receipt},
            )
        _record_allocator_observations(receipts_dir, receipt)
        with connection.begin():
            _record_recovery_observations(
                connection, control_connection, receipts_dir, receipt
            )
            unlock_session(connection)
        with control_connection.begin():
            unlock_session(control_connection)
    return receipt


def acquire_database_writer_fence(
    connection: Any,
    *,
    control_connection: Any,
    inventory: Mapping[str, Any],
    receipts_dir: Path,
    quarantine_receipt: Mapping[str, Any] | None = None,
    provider_contract: Mapping[str, Any] | None = None,
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Explicitly close a gate; keep BOTH physical sessions through release."""
    from daylily_tapdb.sequences import (
        INVENTORY_VERSION,
        capture_sequence_inventory,
        validate_writer_fence,
    )

    if connection.in_transaction() or control_connection.in_transaction():
        _fail("Fence acquisition requires no active target/control transaction")
    validate_receipt(inventory, INVENTORY_VERSION)
    family_fields: dict[str, Any] = {}
    if recovery_family is not None:
        from daylily_tapdb.backup.recovery import (
            require_family_member,
            validate_recovery_family,
        )

        family = validate_recovery_family(
            recovery_family, required_directory=receipts_dir
        )
        require_family_member(family, inventory)
        family_fields = {"recovery_family": family}
    target = dict(inventory["target"], sequence_mappings=inventory["sequence_mappings"])
    with connection.begin():
        current = capture_sequence_inventory(
            connection, schema_name=inventory["schema_name"], target=target
        )
        if current != inventory:
            _fail("Writer fence inventory no longer matches the reviewed target")
        backend = backend_identity(connection)
        lock_session(connection, inventory["physical_target"]["database_oid"])
    with control_connection.begin():
        state = control_state(control_connection, target)
        physical = inventory["physical_target"]
        if any(
            state[field] != physical[field]
            for field in ("database_oid", "server_address", "server_port")
        ):
            _fail("Control connection does not match the exact target server")
        if state["datallowconn"] is not True:
            _fail(
                "Fence acquisition needs an open database owned by this authenticated operator"
            )
        lock_session(control_connection, state["database_oid"])
        provider = provider_evidence(
            control_connection,
            target=target,
            operator_role=state["operator_role"],
            provider_contract=provider_contract,
        )
        baseline = worker_baseline(
            control_connection,
            database_oid=state["database_oid"],
            operator_role=state["operator_role"],
            aurora=target["engine_type"] == "aurora",
        )
        original_acl = acl_snapshot(control_connection, state["database_oid"])
        _restorable_acl(original_acl, state)
        previous, _ = read_fence_history(receipts_dir)
        epoch = active_epoch(previous, inventory["target"])
        if quarantine_receipt is None:
            if epoch is not None:
                _fail(
                    "An unresolved writer fence requires explicit reconciliation before retry"
                )
        else:
            validate_receipt(quarantine_receipt, TAKEOVER_VERSION)
            if quarantine_receipt.get("recovery_family") != recovery_family:
                _fail(
                    "Quarantine promotion must preserve the exact explicit recovery family"
                )
            if (
                quarantine_receipt["phase"] != "quarantined"
                or epoch is None
                or epoch.receipt_id != quarantine_receipt["intent_receipt_id"]
                or quarantine_receipt["target"] != inventory["target"]
                or quarantine_receipt["physical_target"] != physical
                or quarantine_receipt["control_database"] != state["control_database"]
                or quarantine_receipt["source_receipts_dir"] != str(receipts_dir)
                or not any(
                    item.detail.get("receipt") == quarantine_receipt
                    for item in previous
                )
            ):
                _fail("The exact latest journal-bound quarantine receipt is required")
            if (
                original_acl["entries"]
                != quarantine_receipt["quarantine_acl"]["entries"]
            ):
                _fail("Quarantine ACL changed since the reviewed reconciliation")
            exclusive_acl(control_connection, state=state, provider=provider)
            original_acl = quarantine_receipt["original_acl"]
        detail = {
            "target": inventory["target"],
            "physical_target": physical,
            "sequence_mappings": inventory["sequence_mappings"],
            "operator_role": state["operator_role"],
            "connection_pid": backend["pid"],
            "connection_backend": backend,
            "inventory_sha256": inventory["sha256"],
            "control_database": state["control_database"],
            "original_acl": original_acl,
            "provider_evidence": provider,
            "worker_baseline": baseline,
            "quarantine_receipt": dict(quarantine_receipt)
            if quarantine_receipt
            else None,
            "origin_fence_intent_receipt_id": quarantine_receipt[
                "origin_fence_intent_receipt_id"
            ]
            if quarantine_receipt
            else None,
            **family_fields,
        }
        intent = journal(
            receipts_dir,
            phase="acquire_intent",
            actor=state["operator_role"],
            detail=detail,
        )
        control_connection.execute(
            text(
                f"ALTER DATABASE {quote_identifier(target['database'])} ALLOW_CONNECTIONS false"
            )
        )
    fence = {
        "mode": "database_connections_disabled",
        "database_oid": physical["database_oid"],
        "connection_backend": backend,
        "provider_contract": provider["provider_contract"],
    }
    with connection.begin():
        validate_writer_fence(connection, inventory, fence)
        final = capture_sequence_inventory(
            connection, schema_name=inventory["schema_name"], target=target
        )
        if final != inventory:
            _fail("Allocator changed while the database fence was being acquired")
    receipt = seal_receipt(
        {
            "schema_version": FENCE_VERSION,
            "phase": "acquired",
            "fence": fence,
            **detail,
            "intent_receipt_id": intent.receipt_id,
            "origin_fence_intent_receipt_id": detail["origin_fence_intent_receipt_id"]
            or intent.receipt_id,
        }
    )
    journal(
        receipts_dir,
        phase="acquired",
        actor=state["operator_role"],
        detail={"target": inventory["target"], "receipt": receipt},
    )
    return receipt


def validate_writer_quarantine(
    connection: Any,
    *,
    control_connection: Any,
    quarantine_receipt: Mapping[str, Any],
    receipts_dir: Path,
    provider_contract: Mapping[str, Any] | None = None,
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Read-only source fence proof; retain both sessions around source capture.

    The target is intentionally open only to the operator/provider TCB. This
    proof is for source observation, not a substitute for a closed apply gate.
    """
    from daylily_tapdb.identity_inventory import physical_target, require_snapshot

    require_snapshot(connection)
    if control_connection.in_transaction():
        _fail(
            "Quarantine verification requires a transaction-free retained control session"
        )
    validate_receipt(quarantine_receipt, TAKEOVER_VERSION)
    target = quarantine_receipt["target"]
    if quarantine_receipt["phase"] != "quarantined" or quarantine_receipt[
        "source_receipts_dir"
    ] != str(receipts_dir):
        _fail(
            "An exact successful quarantine receipt and its original journal are required"
        )
    if quarantine_receipt.get("recovery_family") != recovery_family:
        _fail("Quarantine verification requires the exact explicit recovery family")
    physical = physical_target(connection, target)
    if physical != quarantine_receipt["physical_target"]:
        _fail("Quarantine source identifies a different physical database")
    role = connection.execute(text("SELECT session_user")).scalar_one()
    if role != quarantine_receipt["operator_role"]:
        _fail("Quarantine source operator differs from the original receipt")
    backend = backend_identity(connection)
    lock_session(connection, physical["database_oid"])
    with control_connection.begin():
        state = control_state(control_connection, target)
        _verify_origin(state, quarantine_receipt)
        lock_session(control_connection, state["database_oid"])
        history, head = read_fence_history(receipts_dir)
        epoch = active_epoch(history, target)
        if (
            epoch is None
            or epoch.receipt_id != quarantine_receipt["intent_receipt_id"]
            or not any(
                item.detail.get("receipt") == quarantine_receipt for item in history
            )
        ):
            _fail("Quarantine receipt is not the exact latest journal-bound epoch")
        if (
            state["datallowconn"] is not True
            or acl_snapshot(control_connection, state["database_oid"])["entries"]
            != quarantine_receipt["quarantine_acl"]["entries"]
        ):
            _fail("The original operator-only quarantine ACL/gate has changed")
        provider = provider_evidence(
            control_connection,
            target=target,
            operator_role=role,
            provider_contract=provider_contract,
        )
        if (
            provider["provider_role"]
            != quarantine_receipt["provider_evidence"]["provider_role"]
        ):
            _fail("Provider identity changed since the quarantine receipt")
        exclusive_acl(control_connection, state=state, provider=provider)
        census(
            control_connection,
            database=target["database"],
            database_oid=state["database_oid"],
            retained_backend=backend,
        )
    worker_baseline(
        connection,
        database_oid=physical["database_oid"],
        operator_role=role,
        aurora=target["engine_type"] == "aurora",
    )
    target_extensions(connection)
    census(
        connection,
        database=target["database"],
        database_oid=physical["database_oid"],
        retained_backend=backend,
    )
    family_state = None
    if recovery_family is not None:
        from daylily_tapdb.backup.recovery import (
            recovery_family_state,
            validate_recovery_family,
        )

        family = validate_recovery_family(
            recovery_family, required_directory=receipts_dir
        )
        family_state = recovery_family_state(family, require_terminal=False)
    return seal_receipt(
        {
            "schema_version": "tapdb-writer-quarantine-verification/v1",
            "ok": True,
            "target": target,
            "physical_target": physical,
            "quarantine_sha256": quarantine_receipt["sha256"],
            "operator_role": role,
            "connection_backend": backend,
            "source_receipts_dir": str(receipts_dir),
            "source_journal_head": head,
            "recovery_family": recovery_family,
            "family_state": family_state,
        }
    )


def release_database_writer_fence(
    connection: Any,
    fence_receipt: Mapping[str, Any],
    *,
    result: Mapping[str, Any],
    receipts_dir: Path,
    control_connection: Any = None,
) -> dict[str, Any]:
    """Restore the exact original ACL and open atomically after verified commit."""
    from daylily_tapdb.sequences import (
        capture_sequence_inventory,
        validate_writer_fence,
        verify_sequence_floors,
    )

    if connection.in_transaction():
        _fail("Fence release requires the allocator transaction to have ended")
    validate_receipt(fence_receipt, FENCE_VERSION)
    validate_receipt(result, "tapdb-sequence-apply/v1")
    if fence_receipt["phase"] != "acquired" or result["phase"] != "committed":
        _fail(
            "Fence release requires an acquired fence and a committed allocator receipt"
        )
    final = result["inventory"]
    if (
        final["target"] != fence_receipt["target"]
        or final["physical_target"] != fence_receipt["physical_target"]
    ):
        _fail("Committed allocator evidence belongs to a different fenced database")
    if control_connection is None or control_connection.in_transaction():
        _fail("Fence release requires its transaction-free retained control session")
    target = dict(final["target"], sequence_mappings=final["sequence_mappings"])
    with connection.begin():
        backend = backend_identity(connection)
        if (
            backend != fence_receipt["connection_backend"]
            or backend["pid"] != fence_receipt["connection_pid"]
        ):
            _fail("Writer fence must be released by the retained maintenance session")
        validate_writer_fence(connection, final, fence_receipt["fence"])
        observed = capture_sequence_inventory(
            connection, schema_name=final["schema_name"], target=target
        )
        if (
            observed != final
            or not verify_sequence_floors(
                observed, floors=result["verification"]["floors"]
            )["ok"]
        ):
            _fail("Committed allocator state is not confirmed; database remains fenced")
    previous, _ = read_fence_history(receipts_dir)
    epoch = active_epoch(previous, final["target"])
    if epoch is None or epoch.receipt_id != fence_receipt["intent_receipt_id"]:
        _fail(
            "An unresolved or superseding fence transition requires explicit reconciliation"
        )
    if not any(
        item.detail.get("receipt") == fence_receipt for item in previous
    ) or not any(item.detail.get("result") == result for item in previous):
        _fail(
            "Acquired fence and committed allocator results must exist in the exact external journal"
        )
    with control_connection.begin():
        state = control_state(control_connection, target)
        _verify_origin(state, fence_receipt)
        if (
            control_connection.info.get("tapdb_allocator_fence_lock")
            != f"tapdb:allocator-fence:{state['database_oid']}"
        ):
            _fail(
                "Fence release requires the original retained control-session epoch lock"
            )
        census(
            control_connection,
            database=target["database"],
            database_oid=state["database_oid"],
            retained_backend=backend,
        )
        provider_evidence(
            control_connection,
            target=target,
            operator_role=state["operator_role"],
            provider_contract=fence_receipt["provider_evidence"]["provider_contract"],
        )
        detail = {
            **{
                key: fence_receipt[key]
                for key in (
                    "target",
                    "physical_target",
                    "sequence_mappings",
                    "operator_role",
                    "connection_backend",
                    "control_database",
                    "original_acl",
                    "provider_evidence",
                    "origin_fence_intent_receipt_id",
                )
            },
            "fence_sha256": fence_receipt["sha256"],
            "result_sha256": result["sha256"],
            "connection_pid": backend["pid"],
            "committed_result": dict(result),
            "floors": result["floors"],
            "fence_intent_receipt_id": fence_receipt["intent_receipt_id"],
            **(
                {"recovery_family": fence_receipt["recovery_family"]}
                if "recovery_family" in fence_receipt
                else {}
            ),
        }
        intent = journal(
            receipts_dir,
            phase="release_intent",
            actor=state["operator_role"],
            detail=detail,
        )
        restore_acl(
            control_connection, state=state, original_acl=fence_receipt["original_acl"]
        )
        control_connection.execute(
            text(
                f"ALTER DATABASE {quote_identifier(target['database'])} ALLOW_CONNECTIONS true"
            )
        )
    with control_connection.begin():
        state = control_state(control_connection, target)
        if (
            state["datallowconn"] is not True
            or acl_snapshot(control_connection, state["database_oid"])["entries"]
            != fence_receipt["original_acl"]["entries"]
        ):
            _fail("Database connection gate release could not be confirmed")
        receipt = seal_receipt(
            {
                "schema_version": FENCE_VERSION,
                "phase": "released",
                "target": fence_receipt["target"],
                "fence_sha256": fence_receipt["sha256"],
                "result_sha256": result["sha256"],
                "intent_receipt_id": intent.receipt_id,
                "restored_acl": fence_receipt["original_acl"],
                "physical_target": fence_receipt["physical_target"],
                **(
                    {"recovery_family": fence_receipt["recovery_family"]}
                    if "recovery_family" in fence_receipt
                    else {}
                ),
            }
        )
        journal(
            receipts_dir,
            phase="released",
            actor=state["operator_role"],
            detail={"target": final["target"], "receipt": receipt},
        )
        unlock_session(control_connection)
    with connection.begin():
        unlock_session(connection)
    return receipt


def reconcile_writer_fence_release(
    control_connection: Any,
    *,
    release_intent_receipt_id: str,
    receipts_dir: Path,
    provider_contract: Mapping[str, Any] | None = None,
    dry_run: bool = True,
    preflight_receipt: Mapping[str, Any] | None = None,
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Record an observed committed release after a lost acknowledgement, read-only."""
    if control_connection.in_transaction():
        _fail("Release reconciliation requires a transaction-free control session")
    receipts, _ = read_fence_history(receipts_dir)
    intent = next(
        (item for item in receipts if item.receipt_id == release_intent_receipt_id),
        None,
    )
    if (
        intent is None
        or intent.operation != "sequence_writer_fence"
        or intent.detail.get("phase") != "release_intent"
    ):
        _fail("An exact durable release intent is required")
    original = intent.detail
    if original.get("recovery_family") != recovery_family:
        _fail(
            "Release reconciliation requires the exact original explicit recovery family"
        )
    if active_epoch(receipts, original["target"]) is not intent:
        _fail("Release intent is resolved or superseded")
    validate_receipt(original["committed_result"], "tapdb-sequence-apply/v1")
    if original["committed_result"]["phase"] != "committed":
        _fail("Release intent lacks verified committed allocator evidence")
    with control_connection.begin():
        state = control_state(control_connection, original["target"])
        lock_session(control_connection, state["database_oid"])
        try:
            _verify_origin(state, original)
            provider = provider_evidence(
                control_connection,
                target=original["target"],
                operator_role=state["operator_role"],
                provider_contract=provider_contract,
            )
            if (
                provider["provider_role"]
                != original["provider_evidence"]["provider_role"]
            ):
                _fail("Provider identity changed since release intent")
            if (
                state["datallowconn"] is not True
                or acl_snapshot(control_connection, state["database_oid"])["entries"]
                != original["original_acl"]["entries"]
            ):
                _fail(
                    "Release was not observed; closed or quarantined state requires explicit takeover planning"
                )
            planned = seal_receipt(
                {
                    "schema_version": FENCE_VERSION,
                    "phase": "release_reconciliation_planned",
                    "target": original["target"],
                    "physical_target": original["physical_target"],
                    "intent_receipt_id": intent.receipt_id,
                    "fence_sha256": original["fence_sha256"],
                    "result_sha256": original["result_sha256"],
                    "restored_acl": original["original_acl"],
                    "observation": "original_acl_and_open_gate",
                    "source_receipts_dir": str(receipts_dir),
                    "source_journal_head": read_fence_history(receipts_dir)[1],
                    **(
                        {"recovery_family": original["recovery_family"]}
                        if "recovery_family" in original
                        else {}
                    ),
                }
            )
            if dry_run:
                return planned
            if preflight_receipt != planned:
                _fail(
                    "Release reconciliation requires its unchanged reviewed observation receipt"
                )
            receipt = seal_receipt({**planned, "phase": "release_reconciled"})
            journal(
                receipts_dir,
                phase="release_reconciled",
                actor=state["operator_role"],
                detail={"target": original["target"], "receipt": receipt},
            )
            return receipt
        finally:
            unlock_session(control_connection)
