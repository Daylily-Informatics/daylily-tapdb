"""Reviewed, fenced native adoption and restore-epoch lifecycle.

No operator SQL workaround: consumers call these operations through the CLI.
Baseline rows are observations, not new claims about who made historical edits.
They do not allocate business IDs or alter identity generator state.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4
from sqlalchemy import text
from daylily_tapdb.audit_storage import (
    audit_writer_install_sql,
    grant_audit_sequences,
    enforce_audit_read_grants,
)
from daylily_tapdb.audit_uid_sequence_denial import (
    apply_audit_uid_sequence_denial, plan_audit_uid_sequence_denial,
)
from daylily_tapdb.runtime_catalog_contract import security_assets
from daylily_tapdb.runtime_principal import (
    runtime_schema_grants_sql,
    runtime_scope_binding_sql,
)
from daylily_tapdb.security_context import (
    Attribution,
    TapdbTransactionContext,
    apply_transaction_context,
    assert_operator_role,
)
from daylily_tapdb.sequences import capture_sequence_inventory, validate_writer_fence
from daylily_tapdb.identity_inventory import seal_receipt, validate_receipt
from daylily_tapdb.adoption_allocators import (
    allocation_paths, build_adoption_allocator_inputs, capture_allocation_surface,
    require_adoption_history, require_bounded_allocation_surface,
)

_DOMAIN = ("generic_template", "generic_instance", "generic_instance_lineage")


def _q(value):
    return '"' + value.replace('"', '""') + '"'


def _target(cfg):
    from daylily_tapdb.backup.service import inventory_target

    return inventory_target(cfg)


def _context(connection, cfg, operation):
    connection.execute(text("SET LOCAL TimeZone = 'UTC'"))
    connection.execute(text("SET LOCAL DateStyle = 'ISO, YMD'"))
    apply_transaction_context(
        connection,
        TapdbTransactionContext(
            config_identity=str(cfg["config_path"]),
            schema_name=cfg["schema_name"],
            domain_code=cfg["domain_code"],
            owner_repo_name=cfg["owner_repo_name"],
            tenant_id=None,
            actor="tapdb-integrity-lifecycle",
            allow_global_rows=True,
            attribution=Attribution(
                "service",
                "daylily-tapdb",
                "integrity-lifecycle",
                "tapdb-cli",
                str(uuid4()),
                operation,
            ),
        ),
        assert_runtime_role=False,
    )
    assert_operator_role(
        connection, schema_name=cfg["schema_name"], operator_user=cfg["operator_user"]
    )


def _digest(connection, schema):
    result = {}
    for table in (*_DOMAIN, "audit_log"):
        digest = hashlib.sha256()
        count = 0
        for value in connection.execute(
            text(
                f"SELECT (to_jsonb(t) - 'record_revision')::text FROM {_q(schema)}.{table} t ORDER BY uid"
            )
        ).scalars():
            digest.update(value.encode() + b"\n")
            count += 1
        result[table] = {"count": count, "sha256": digest.hexdigest()}
    return result


def _catalog_evidence(connection, schema):
    """Sanitized pre-adoption fingerprints; no row contents or role passwords."""
    queries = {
        "relations": "SELECT c.relname,c.relkind,c.relowner,c.relacl::text,c.relrowsecurity,c.relforcerowsecurity FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema ORDER BY c.relname",
        "routines": "SELECT p.proname,pg_catalog.pg_get_function_identity_arguments(p.oid) AS arguments,p.proowner,p.proacl::text,p.prosecdef,p.proconfig,p.prosrc FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema ORDER BY p.proname,arguments",
        "policies": "SELECT c.relname,p.polname,pg_catalog.pg_get_expr(p.polqual,p.polrelid) AS using_expression,pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid) AS check_expression FROM pg_catalog.pg_policy p JOIN pg_catalog.pg_class c ON c.oid=p.polrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema ORDER BY c.relname,p.polname",
        "triggers": "SELECT c.relname,t.tgname,pg_catalog.pg_get_triggerdef(t.oid) AS definition,t.tgenabled FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND NOT t.tgisinternal ORDER BY c.relname,t.tgname",
    }
    evidence = {}
    for name, query in queries.items():
        rows = [
            dict(row)
            for row in connection.execute(text(query), {"schema": schema}).mappings()
        ]
        evidence[name] = {
            "count": len(rows),
            "sha256": hashlib.sha256(
                json.dumps(rows, sort_keys=True, default=str).encode()
            ).hexdigest(),
        }
    return evidence


def _integrity_templates():
    from daylily_tapdb.templates.loader import (
        find_tapdb_core_config_dir,
        load_template_configs,
    )

    root = find_tapdb_core_config_dir()
    keys = {
        ("reference", "annotation", "generic", "1.0"),
        ("governance", "correction_receipt", "generic", "1.0"),
    }
    rows = [
        item
        for item in load_template_configs(root)
        if tuple(item[k] for k in ("category", "type", "subtype", "version")) in keys
    ]
    if len(rows) != 2:
        raise ValueError("exact installed TapDB 11 template definitions are required")
    return rows


def _adoption_snapshot(connection: Any, cfg: Mapping[str, Any]) -> dict[str, Any]:
    _context(connection, cfg, "plan-adoption")
    schema = cfg["schema_name"]
    if (
        connection.execute(
            text("SELECT to_regclass(:table)"),
            {"table": _q(schema) + ".tapdb_history_epoch"},
        ).scalar_one()
        is not None
    ):
        raise ValueError(
            "TapDB 11 history already exists; repeated adoption is prohibited"
        )
    assets = security_assets()
    templates = _integrity_templates()
    plan = {
        "format": "tapdb.integrity-adoption/v2",
        "target": _target(cfg),
        "audit_uid_sequence_denial": plan_audit_uid_sequence_denial(connection, cfg),
        "tables": _digest(connection, schema),
        "catalog": _catalog_evidence(connection, schema),
        "sequences": capture_sequence_inventory(
            connection, schema_name=schema, target=_target(cfg)
        ),
        "assets": {
            k: hashlib.sha256(v.encode()).hexdigest() for k, v in assets.items()
        },
        "new_core_templates": [
            {k: v for k, v in item.items() if not k.startswith("_")}
            for item in templates
        ],
    }
    return plan


def plan_adoption(
    connection: Any, cfg: Mapping[str, Any], *, receipts_dir: Path,
    recovery_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Seal one complete adoption, prior-floor advance and allocation budget.

    The original canonical journal is explicit even for an empty first journal.
    Old snapshot-only plans are not executable through the combined lifecycle.
    """
    snapshot = _adoption_snapshot(connection, cfg)
    allocator = build_adoption_allocator_inputs(
        connection, cfg, snapshot["sequences"], snapshot["new_core_templates"],
        receipts_dir=receipts_dir, recovery_family=recovery_family,
    )
    return seal_receipt({"schema_version": "tapdb.integrity-adoption/v2",
                         **snapshot, **allocator})


def _establish_epoch(connection: Any, cfg: Mapping[str, Any], *, origin: str) -> str:
    """Caller holds verified physical writer fence, same transaction as baseline.

    This internal primitive never opens a connection or releases an operational
    fence. Native adoption/restore own admission and transaction boundaries.
    """
    if origin not in {"adoption", "restore"}:
        raise ValueError("explicit adoption or restore origin required")
    _context(connection, cfg, "history-" + origin)
    ns = _q(cfg["schema_name"])
    connection.execute(
        text(f"LOCK TABLE {ns}.tapdb_history_epoch IN ACCESS EXCLUSIVE MODE")
    )
    connection.execute(
        text(f"UPDATE {ns}.tapdb_history_epoch SET active=false WHERE active")
    )
    epoch = connection.execute(
        text(
            f"INSERT INTO {ns}.tapdb_history_epoch (origin,adoption_snapshot) VALUES (:origin,pg_current_snapshot()::text) RETURNING epoch::text"
        ),
        {"origin": origin},
    ).scalar_one()
    for table in _DOMAIN:
        if table == "generic_instance":
            template = f"(SELECT jsonb_build_object('uid',g.uid,'euid',g.euid,'domain_code',g.domain_code,'issuer_app_code',g.issuer_app_code,'revision',g.record_revision) FROM {ns}.generic_template g WHERE g.uid=t.template_uid)"
        elif table == "generic_template":
            template = "jsonb_build_object('uid',t.uid,'euid',t.euid,'domain_code',t.domain_code,'issuer_app_code',t.issuer_app_code,'revision',t.record_revision)"
        else:
            template = "NULL::jsonb"
        connection.execute(
            text(f"""INSERT INTO {ns}.tapdb_history_baseline
            (epoch,rel_table_name,rel_table_uid_fk,rel_table_euid_fk,tenant_id,domain_code,issuer_app_code,record_revision,attribution,governing_template,state)
            SELECT CAST(:epoch AS uuid),:table,uid,euid,tenant_id,domain_code,issuer_app_code,record_revision,{ns}.tapdb_current_attribution(),{template},to_jsonb(t)
            FROM {ns}.{table} t"""),
            {"epoch": epoch, "table": table},
        )
    return epoch


def apply_adoption(
    connection: Any,
    cfg: Mapping[str, Any],
    *,
    plan: dict[str, Any],
    fence_receipt: dict[str, Any],
    receipts_dir: Path,
) -> dict[str, Any]:
    """Apply the combined native plan in the caller's one transaction."""
    from daylily_tapdb.sequences import (
        apply_sequence_advance_plan, build_sequence_advance_plan,
        build_sequence_allocation_reservation, finalize_sequence_allocation,
    )

    validate_receipt(plan, "tapdb.integrity-adoption/v2")
    if plan["history"]["receipts_dir"] != str(receipts_dir):
        raise ValueError("adoption requires its exact reviewed original journal")
    _context(connection, cfg, "apply-adoption")
    ns = _q(cfg["schema_name"])
    validate_receipt(fence_receipt, "tapdb-writer-fence/v1")
    if (fence_receipt["phase"] != "acquired"
            or fence_receipt.get("recovery_family") != plan["history"]["recovery_family"]):
        raise ValueError("adoption requires its acquired original-family writer fence")
    writer_fence = fence_receipt["fence"]
    validate_writer_fence(connection, plan["sequences"], writer_fence)
    connection.execute(
        text(
            "LOCK TABLE "
            + ", ".join(ns + "." + t for t in (*_DOMAIN, "audit_log"))
            + " IN ACCESS EXCLUSIVE MODE"
        )
    )
    current = _adoption_snapshot(connection, cfg)
    if any(plan.get(key) != value for key, value in current.items()):
        raise ValueError("adoption plan changed; inspect and obtain a fresh plan")
    validate_receipt(plan["allocation_surface"], "tapdb-adoption-allocation-surface/v1")
    if capture_allocation_surface(connection, cfg) != plan["allocation_surface"]:
        raise ValueError("adoption allocation surface changed before advancement")
    require_adoption_history(plan["history"], current["sequences"], fence_receipt=fence_receipt)
    advance = build_sequence_advance_plan(
        current["sequences"], floors=plan["history"]["floors"],
        recovery_family=plan["history"]["recovery_family"],
    )
    paths = allocation_paths(connection, cfg, current["sequences"], current["new_core_templates"])
    reservation = build_sequence_allocation_reservation(
        advance, allocation_counts=paths["allocation_counts"])
    if (advance != plan["allocator_plan"] or paths != plan["allocation_paths"]
            or reservation != plan["allocation_reservation"]):
        raise ValueError("adoption allocator/history inputs changed after review")
    # Deny only the reviewed direct audit UID grants before allocator advancement
    # and before DDL legitimately grants the dedicated audit writer access.
    audit_uid_denial = apply_audit_uid_sequence_denial(
        connection, cfg, plan=plan["audit_uid_sequence_denial"])
    # No adoption schema/template/audit mutation occurs before this exact native
    # advance. Its intent reserves the permitted allocation window durably.
    allocated = apply_sequence_advance_plan(
        connection, advance, writer_fence=writer_fence, receipts_dir=receipts_dir,
        allocation_reservation=reservation, operation_sha256=plan["sha256"],
    )
    assets = security_assets()
    for name in ("tapdb_schema.sql", "rls.sql", "runtime_identity_authorization.sql"):
        connection.exec_driver_sql(assets[name].replace("%", "%%"))
    connection.exec_driver_sql(
        audit_writer_install_sql(
            cfg["database"], cfg["schema_name"], cfg["operator_user"]
        ).replace("%", "%%")
    )
    connection.exec_driver_sql(
        runtime_schema_grants_sql(
            cfg["schema_name"], cfg["user"], operator_user=cfg["operator_user"]
        ).replace("%", "%%")
    )
    connection.exec_driver_sql(
        runtime_scope_binding_sql(cfg["schema_name"], cfg).replace("%", "%%")
    )
    grant_audit_sequences(
        connection, database=cfg["database"], schema=cfg["schema_name"]
    )
    enforce_audit_read_grants(
        connection, database=cfg["database"], schema=cfg["schema_name"]
    )
    epoch = _establish_epoch(connection, cfg, origin="adoption")
    observed = _digest(connection, cfg["schema_name"])
    if observed != plan["tables"]:
        raise RuntimeError(
            "adoption altered preexisting record or audit evidence: "
            + json.dumps({"before": plan["tables"], "after": observed}, sort_keys=True)
        )
    after = capture_sequence_inventory(
        connection, schema_name=cfg["schema_name"], target=_target(cfg)
    )
    # Schema/baseline establishment must preserve the authorized advanced state.
    before_states = {
        s["name"]: (s["last_value"], s["is_called"])
        for s in allocated["inventory"]["sequences"]
    }
    after_states = {
        s["name"]: (s["last_value"], s["is_called"]) for s in after["sequences"]
    }
    if before_states != after_states:
        raise RuntimeError("adoption unexpectedly changed identity allocation state")
    catalog_proof = require_bounded_allocation_surface(connection, cfg)
    # Install only these two exact bundled definitions, using the native loader.
    # Their new identities are issued normally; no old identity or floor changes.
    from sqlalchemy.orm import Session
    from dataclasses import asdict
    from daylily_tapdb.templates.loader import (
        seed_templates,
        find_tapdb_core_config_dir,
    )

    with Session(bind=connection) as session:
        installed = seed_templates(
            session,
            _integrity_templates(),
            overwrite=False,
            core_config_dir=find_tapdb_core_config_dir(),
            domain_code=cfg["domain_code"],
            owner_repo_name=cfg["owner_repo_name"],
            domain_registry_path=Path(cfg["domain_registry_path"]),
            prefix_registry_path=Path(cfg["prefix_ownership_registry_path"]),
            governance_authorization=cfg.get("governance_authorization"),
            create_governance_objects=False,
        )
        session.flush()
    if (installed.inserted + installed.skipped != 2 or installed.updated != 0
            or installed.templates_loaded != 2):
        raise RuntimeError("native adoption exceeded its exact two-template operation")
    finalized = finalize_sequence_allocation(
        connection, allocated, writer_fence=writer_fence, receipts_dir=receipts_dir,
        operation_sha256=plan["sha256"], adoption_epoch=epoch,
        allocation_counts={name: count // 2 * installed.inserted
                           for name, count in paths["allocation_counts"].items()},
    )
    return {
        "templates": asdict(installed),
        "format": "tapdb.integrity-adoption-receipt/v2",
        "epoch": epoch,
        "plan_sha256": plan["sha256"],
        "history_completeness": "current state observed at adoption; earlier audit retained",
        "runtime_rebind_required": True,
        "consumer_write_context_required": "attribution/v1",
        "allocation_catalog_sha256": catalog_proof,
        "allocator_receipt": finalized,
        "audit_uid_sequence_denial": audit_uid_denial,
    }


def adopt_integrity(
    cfg: Mapping[str, Any],
    control_cfg: Mapping[str, Any],
    *,
    plan: dict[str, Any],
    receipts_dir,
    provider_contract=None,
) -> dict[str, Any]:
    """Own the existing native fence lifecycle on retained operator connections.

    A failure keeps the native fence closed for explicit recovery. There is no
    implicit cleanup/reopen path and no invented sequence floor.
    """
    from daylily_tapdb.runtime_principal import operator_session
    from daylily_tapdb.sequences import (
        acquire_database_writer_fence,
        release_database_writer_fence,
        record_sequence_advance_outcome,
    )

    if control_cfg["database"] == cfg["database"]:
        raise ValueError("explicit independent control database required")
    validate_receipt(plan, "tapdb.integrity-adoption/v2")
    if plan["history"]["receipts_dir"] != str(receipts_dir):
        raise ValueError("exact reviewed original receipts_dir required")
    require_adoption_history(plan["history"], plan["sequences"])
    with (
        operator_session(control_cfg, isolation_level="REPEATABLE READ") as control,
        operator_session(cfg, isolation_level="REPEATABLE READ") as conn,
    ):
        acquired = acquire_database_writer_fence(
            conn,
            control_connection=control,
            inventory=plan["sequences"],
            receipts_dir=receipts_dir,
            provider_contract=provider_contract,
            recovery_family=plan["history"]["recovery_family"],
        )
        with conn.begin():
            result = apply_adoption(
                conn, cfg, plan=plan, fence_receipt=acquired, receipts_dir=Path(receipts_dir)
            )
        committed = record_sequence_advance_outcome(
            result["allocator_receipt"],
            receipts_dir=receipts_dir,
            outcome="committed",
            actor=cfg["operator_user"],
        )
        release_database_writer_fence(
            conn,
            acquired,
            result=committed,
            receipts_dir=receipts_dir,
            control_connection=control,
        )
        return seal_receipt({
            **result, "schema_version": "tapdb.integrity-adoption-receipt/v2",
            "fence_released": True, "allocator_receipt": committed,
            "fence_sha256": acquired["sha256"],
        })
