"""Receipt-bound prospective template allocator transition for native operators.

Supports canonical template-prefix changes and single_prefix -> template_split.
The base instance policy, domain, owner, runtime scope, object EUIDs and history
are preserved. A later template_split profile may change only template_prefix.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from daylily_tapdb import __version__
from daylily_tapdb.governance import GovernanceContext
from daylily_tapdb.identity_inventory import physical_target
from daylily_tapdb.issuance import IssuanceProfile, profile_from_config
from daylily_tapdb.runtime_principal import (
    RuntimePrincipalError, _ident, _seal, _target, _write_receipt,
    grant_proven_runtime_sequences, operator_connection,
)
from daylily_tapdb.schema_inventory import find_schema_root
from daylily_tapdb.security_context import (
    Attribution, TapdbTransactionContext, apply_transaction_context, assert_operator_role,
)
from daylily_tapdb.sequences import ensure_instance_prefix_sequence

_FORMAT = "tapdb-issuance-transition/v1"
_CHECKS = {"tapdb_issuance_profile_mode_check", "tapdb_issuance_profile_check"}


class IssuanceTransitionError(RuntimePrincipalError):
    """The explicit prospective transition or its reviewed receipt is invalid."""


def _profiles(
    cfg: Mapping[str, Any], expected: Mapping[str, Any], requested: Mapping[str, Any],
) -> tuple[IssuanceProfile, IssuanceProfile]:
    before, after = IssuanceProfile.from_mapping(expected), IssuanceProfile.from_mapping(requested)
    canonical_change = before.mode == after.mode == "canonical"
    split_change = before.mode in {"single_prefix", "template_split"} and after.mode == "template_split"
    if (not (canonical_change or split_change) or before.prefix != after.prefix
            or after.template_prefix is None or before == after):
        raise IssuanceTransitionError(
            "A prospective template-prefix change must preserve the instance allocation policy"
        )
    if profile_from_config(cfg) != before:
        raise IssuanceTransitionError("The explicit config must retain the expected old profile until apply succeeds")
    return before, after


def _governance(cfg: Mapping[str, Any], after: IssuanceProfile) -> list[dict[str, str]]:
    claims = []
    for entity, prefix in sorted(after.identity_prefixes().items()):
        governance = GovernanceContext.load(
            domain_code=cfg["domain_code"],
            owner_repo_name=after.identity_prefix_owner(entity, cfg["owner_repo_name"]),
            domain_registry_path=cfg["domain_registry_path"],
            prefix_ownership_registry_path=cfg["prefix_ownership_registry_path"],
            governance_authorization=cfg.get("governance_authorization"),
        )
        governance.require_prefix(prefix)
        claims.append({"entity": entity, "domain_code": governance.domain_code, "prefix": prefix,
                       "owner_repo_name": governance.owner_repo_name})
    return claims


def _plan(
    connection: Any, cfg: Mapping[str, Any], before: IssuanceProfile,
    after: IssuanceProfile, attribution: Attribution,
) -> dict[str, Any]:
    target = _target(cfg)
    assert_operator_role(connection, schema_name=target["schema_name"], operator_user=target["operator_user"])
    apply_transaction_context(connection, TapdbTransactionContext(
        config_identity=target["config_path"], schema_name=target["schema_name"],
        domain_code=target["domain_code"], owner_repo_name=target["owner_repo_name"],
        tenant_id=target["tenant_id"] or None, actor=attribution.actor_subject,
        allow_global_rows=target["allow_global_claims"],
        additional_tenant_ids=tuple(target["additional_tenant_ids"]), attribution=attribution,
    ), assert_runtime_role=False)
    schema = _ident(target["schema_name"])
    controls = [dict(row) for row in connection.execute(text("""
        SELECT c.oid::bigint AS oid, c.relname AS name, c.relkind AS kind,
               pg_catalog.pg_get_userbyid(c.relowner) AS owner,
               pg_catalog.pg_get_userbyid(n.nspowner) AS schema_owner
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:schema
          AND c.relname IN ('tapdb_issuance_profile', 'tapdb_identity_prefix_config')
        ORDER BY c.relname
    """), {"schema": target["schema_name"]}).mappings()]
    if len(controls) != 2 or any(
        row["kind"] != "r" or row["owner"] != target["operator_user"]
        or row["schema_owner"] != target["operator_user"] for row in controls
    ):
        raise IssuanceTransitionError("Both native issuance control tables must have the exact configured operator owner")
    profile = connection.execute(text(f"""
        SELECT pg_catalog.to_jsonb(p) FROM {schema}.tapdb_issuance_profile p
        WHERE domain_code=:domain AND issuer_app_code=:owner
    """), {"domain": target["domain_code"], "owner": target["owner_repo_name"]}).scalar_one_or_none()
    expected_fields = {"version": before.version, "mode": before.mode,
                       "prefix": before.prefix, "template_prefix": before.template_prefix}
    if profile is None or any(profile.get(key) != value for key, value in expected_fields.items()):
        raise IssuanceTransitionError("Persisted issuance profile does not match the exact expected old profile")
    profile_oid = next(row["oid"] for row in controls if row["name"] == "tapdb_issuance_profile")
    has_template_prefix = connection.execute(text("""
        SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
          WHERE attrelid=:oid AND attname='template_prefix' AND attnum>0 AND NOT attisdropped)
    """), {"oid": profile_oid}).scalar_one()
    constraints = [dict(row) for row in connection.execute(text("""
        SELECT conname AS name, pg_catalog.pg_get_constraintdef(oid) AS definition
        FROM pg_catalog.pg_constraint WHERE conrelid=:oid AND contype='c' ORDER BY conname
    """), {"oid": profile_oid}).mappings()]
    if not _CHECKS <= {row["name"] for row in constraints}:
        raise IssuanceTransitionError("The profile table lacks the native profile constraints required for this upgrade")
    bindings = [dict(row) for row in connection.execute(text(f"""
        SELECT entity, prefix FROM {schema}.tapdb_identity_prefix_config
        WHERE domain_code=:domain AND issuer_app_code=:owner ORDER BY entity
    """), {"domain": target["domain_code"], "owner": target["owner_repo_name"]}).mappings()]
    if {row["entity"]: row["prefix"] for row in bindings} != before.identity_prefixes():
        raise IssuanceTransitionError("Persisted native identity bindings do not match the exact old profile")
    sequence = connection.execute(text("""
        SELECT c.oid::bigint AS oid, c.relname AS name, c.relkind AS kind,
               pg_catalog.pg_get_userbyid(c.relowner) AS owner,
               pg_catalog.obj_description(c.oid, 'pg_class') AS annotation
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:schema AND c.relname=:name
    """), {"schema": target["schema_name"], "name": after.template_prefix.lower() + "_instance_seq"}).mappings().one_or_none()
    return _seal({
        "format": _FORMAT, "operation": "template-prefix-transition", "status": "planned",
        "native_version": __version__,
        "target": target, "physical_target": physical_target(connection, target),
        "operator": target["operator_user"], "attribution": asdict(attribution),
        "expected_profile": before.to_dict(), "new_profile": after.to_dict(),
        "governed_claims": _governance(cfg, after), "controls": controls,
        "profile_row": profile, "identity_bindings": bindings,
        "schema_upgrade": {"add_template_prefix_column": not has_template_prefix,
                           "existing_constraints": constraints,
                           "install_allocator_asset": "allocator_functions.sql"},
        "template_allocator": {"prefix": after.template_prefix,
                               "previous_catalog_entry": dict(sequence) if sequence is not None else None},
        "changes": ["Install native unified issuance profile constraints and allocator routines",
                    "Provision the separate template allocator through the native sequence service",
                    "Replace only the generic_template identity binding and its explicit issuance profile",
                    "Grant proven native allocator usage to the existing constrained runtime"],
        "preserved": ["all persisted EUIDs", "all template definitions", "all object rows and history",
                      "base allocator and its sequence position", "domain and owner", "runtime scope and producer"],
    })


def _upgrade_profile_schema(connection: Any, schema_name: str, *, add_column: bool) -> None:
    table = f"{_ident(schema_name)}.tapdb_issuance_profile"
    if add_column:
        connection.execute(text(f"ALTER TABLE {table} ADD COLUMN template_prefix TEXT"))
    connection.execute(text(f"""
        ALTER TABLE {table}
          DROP CONSTRAINT tapdb_issuance_profile_mode_check,
          DROP CONSTRAINT tapdb_issuance_profile_check,
          DROP CONSTRAINT IF EXISTS tapdb_issuance_profile_template_prefix_check,
          ADD CONSTRAINT tapdb_issuance_profile_mode_check
            CHECK (mode IN ('canonical', 'single_prefix', 'template_split')),
          ADD CONSTRAINT tapdb_issuance_profile_check CHECK (
            (mode='canonical' AND prefix IS NULL
                AND (template_prefix IS NULL OR template_prefix ~ '^[0-9A-HJ-KMNP-TV-Z]{{1,4}}$'))
            OR (mode='single_prefix' AND prefix IS NOT NULL AND prefix ~ '^[0-9A-HJ-KMNP-TV-Z]{{1,4}}$'
                AND template_prefix IS NULL)
            OR (mode='template_split' AND prefix IS NOT NULL AND prefix ~ '^[0-9A-HJ-KMNP-TV-Z]{{1,4}}$'
                AND template_prefix IS NOT NULL AND template_prefix ~ '^[0-9A-HJ-KMNP-TV-Z]{{1,4}}$'
                AND template_prefix <> prefix))
    """))


def transition_issuance_profile(
    cfg: Mapping[str, Any], *, expected_profile: Mapping[str, Any],
    new_profile: Mapping[str, Any], attribution: Attribution,
    receipt_path: Path, apply: bool = False,
) -> dict[str, Any]:
    """Plan/apply a prospective split, retaining all previously issued objects.

    Apply consumes its unchanged plan receipt. The database changes and grants
    commit together; configuration is deliberately updated separately afterward.
    """
    if not isinstance(attribution, Attribution):
        raise IssuanceTransitionError("Explicit operator Attribution is required")
    before, after = _profiles(cfg, expected_profile, new_profile)
    target = _target(cfg)
    receipt_path = Path(receipt_path)
    if not receipt_path.is_absolute():
        raise IssuanceTransitionError("receipt_path must be absolute")
    result_path = receipt_path.with_name(receipt_path.stem + ".result.json")
    reviewed = None
    if apply:
        reviewed = json.loads(receipt_path.read_text(encoding="utf-8"))
        if not isinstance(reviewed, dict) or reviewed != _seal(reviewed):
            raise IssuanceTransitionError("Issuance transition receipt has an invalid native digest")
        if result_path.exists():
            raise IssuanceTransitionError("Issuance transition result already exists; do not replay")
    elif receipt_path.exists() or result_path.exists():
        raise IssuanceTransitionError("Issuance transition receipt path must be unused")
    with operator_connection(cfg, isolation_level="SERIALIZABLE", read_only=not apply) as connection:
        if apply:
            # Acquire native identity-table locks before the profile locks,
            # matching the INSERT -> allocator lookup order of runtime writes.
            schema = _ident(target["schema_name"])
            connection.execute(text("LOCK TABLE " + ", ".join(
                f"{schema}.{_ident(name)}" for name in (
                    "generic_template", "generic_instance", "generic_instance_lineage", "audit_log",
                    "tapdb_issuance_profile", "tapdb_identity_prefix_config",
                )
            ) + " IN ACCESS EXCLUSIVE MODE"))
        plan = _plan(connection, cfg, before, after, attribution)
        if not apply:
            _write_receipt(receipt_path, plan)
            return plan
        if reviewed != plan:
            raise IssuanceTransitionError("Issuance transition receipt is stale or does not match the exact request")
        _upgrade_profile_schema(connection, target["schema_name"],
                                add_column=plan["schema_upgrade"]["add_template_prefix_column"])
        asset = find_schema_root(Path("allocator_functions.sql")) / "allocator_functions.sql"
        connection.execute(text(asset.read_text(encoding="utf-8")))
        ensure_instance_prefix_sequence(connection, after.template_prefix)
        params = {"domain": target["domain_code"], "owner": target["owner_repo_name"],
                  "prefix": after.template_prefix}
        connection.execute(text(f"""
            UPDATE {schema}.tapdb_identity_prefix_config SET prefix=:prefix, updated_dt=CURRENT_TIMESTAMP
            WHERE entity='generic_template' AND domain_code=:domain AND issuer_app_code=:owner
        """), params)
        connection.execute(text(f"""
            UPDATE {schema}.tapdb_issuance_profile SET mode=:mode, template_prefix=:prefix
            WHERE domain_code=:domain AND issuer_app_code=:owner
        """), {**params, "mode": after.mode})
        grants = grant_proven_runtime_sequences(connection, {**cfg, "issuance_profile": after.to_dict()})
    result = _seal({
        "format": _FORMAT, "operation": "template-prefix-transition", "status": "applied",
        "native_version": __version__,
        "plan_sha256": plan["sha256"], "target": target, "physical_target": plan["physical_target"],
        "operator": target["operator_user"], "attribution": asdict(attribution),
        "previous_profile": before.to_dict(), "new_profile": after.to_dict(),
        "grants": grants, "preserved": plan["preserved"], "applied_at": datetime.now(UTC).isoformat(),
        "next_action": "Use db-config update --issuance-profile-json with new_profile, then restart consumers",
    })
    _write_receipt(result_path, result)
    return result
