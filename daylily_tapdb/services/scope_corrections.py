"""Reviewed operator correction of a bounded, unclaimed global source cohort.

This is separate from physical schema migration. The caller owns the transaction;
no commit, identity allocation repair, scope inference, or application fallback occurs.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator, Mapping
from uuid import UUID

from sqlalchemy import select, text

from daylily_tapdb.euid import validate_euid
from daylily_tapdb.external_references import ExternalReferenceService
from daylily_tapdb.factory.instance import IdentityClaimOutcome, IdentityScope, InstanceFactory
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.runtime_catalog_contract import canonical_security_contract
from daylily_tapdb.security_context import assert_operator_role
from daylily_tapdb.templates.manager import TemplateManager

CONTRACT = "tapdb.reviewed-scope-correction/v1"
TEMPLATE = "evidence/repair/scope_assignment/1.0/"
SETTING = "tapdb.reviewed_scope_correction"
MAX_SOURCES = 96
MAX_EDGES = 1024
_HASH = "encode(sha256(convert_to((to_jsonb(r) - ARRAY['tenant_id','modified_dt'])::text, 'UTF8')), 'hex')"
_COMMON = "r.uid, r.euid, r.tenant_id, r.domain_code, r.issuer_app_code, r.is_deleted, r.category, r.type, r.subtype, r.version, r.euid_prefix"


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def validate_request(request: Mapping[str, Any]) -> dict[str, Any]:
    """Validate explicit authority and bounds without reading a database."""
    fields = {"target", "operation_id", "actor", "reason", "approval_ref",
              "migration_receipt_sha256", "tenant_authority_sha256", "tenant_id",
              "source_euids", "allowed_templates", "retire_lineage_euids"}
    if set(request) != fields:
        raise ValueError("scope correction requires exactly the documented request fields")
    value = json.loads(_canonical(request))
    for key in ("operation_id", "actor", "reason", "approval_ref"):
        item = value[key]
        if not isinstance(item, str) or not item or item != item.strip():
            raise ValueError(f"explicit {key} is required")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._:-]{0,127}", value["operation_id"]):
        raise ValueError("operation_id must be an ordinary lowercase idempotency key")
    for key in ("migration_receipt_sha256", "tenant_authority_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", value[key]):
            raise ValueError(f"{key} must be an exact SHA256")
    if str(UUID(value["tenant_id"])) != value["tenant_id"]:
        raise ValueError("tenant_id must be a canonical explicit UUID")
    target = value["target"]
    if set(target) != {"database", "schema_name", "domain_code", "owner_repo_name", "operator_user", "config_path"}:
        raise ValueError("target requires exact database/schema/domain/owner/operator/config")
    if any(not isinstance(item, str) or not item or item != item.strip() for item in target.values()):
        raise ValueError("target values must be exact nonempty strings")
    if not Path(target["config_path"]).is_absolute():
        raise ValueError("config_path must be absolute")
    for key, maximum in (("source_euids", MAX_SOURCES), ("retire_lineage_euids", MAX_EDGES)):
        items = value[key]
        if not isinstance(items, list) or len(items) > maximum or len(set(items)) != len(items):
            raise ValueError(f"{key} must be a unique bounded list")
        if any(not isinstance(item, str) or not validate_euid(item) for item in items):
            raise ValueError(f"{key} must contain owner-issued canonical EUIDs")
        value[key] = sorted(items)
    if not value["source_euids"]:
        raise ValueError("source cohort must not be empty")
    templates = value["allowed_templates"]
    if not isinstance(templates, list) or not templates or len(templates) > MAX_SOURCES:
        raise ValueError("allowed_templates must be an explicit bounded list")
    if any(not isinstance(item, str) or len(item.strip("/").split("/")) != 4 for item in templates):
        raise ValueError("allowed_templates requires exact taxonomy/version codes")
    value["allowed_templates"] = sorted(set(templates))
    return value


def _authority(session: Any, request: dict[str, Any]) -> dict[str, Any]:
    if not session.in_transaction():
        raise RuntimeError("an active caller-owned operator transaction is required")
    target = request["target"]
    assert_operator_role(session, schema_name=target["schema_name"], operator_user=target["operator_user"])
    observed = dict(session.execute(text("""SELECT current_database() AS database,
        current_schema() AS schema_name, session_user AS operator_user,
        tapdb_current_domain_code() AS domain_code,
        tapdb_current_owner_repo_name() AS owner_repo_name,
        current_setting('session.current_config_identity') AS config_path,
        tapdb_current_actor() AS actor""")).mappings().one())
    if observed != {**target, "actor": request["actor"]}:
        raise PermissionError("scope correction target/actor differs from authenticated context")
    return observed


def _require_native_trigger(session: Any, request: dict[str, Any]) -> None:
    """Match installed native source and firing shape, not a marker or live hash."""
    target = request["target"]
    quoted = session.get_bind().dialect.identifier_preparer.quote(target["schema_name"])
    expected = canonical_security_contract(target["schema_name"], quoted)["functions"][("tapdb_validate_lineage_endpoint_scope", "")]
    actual = dict(session.execute(text("""SELECT p.prosrc AS source,
        p.prosecdef AS security_definer, p.provolatile::text AS volatility,
        p.proconfig AS settings, pg_get_userbyid(p.proowner) AS owner,
        t.tgenabled::text AS enabled, t.tgtype AS trigger_type,
        t.tgdeferrable AS deferrable, pg_get_expr(t.tgqual, t.tgrelid) AS qualifier
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_trigger t ON t.tgfoid=p.oid
        JOIN pg_class c ON c.oid=t.tgrelid AND c.relnamespace=n.oid
        WHERE n.nspname=:schema AND p.proname='tapdb_validate_lineage_endpoint_scope'
          AND p.pronargs=0 AND c.relname='generic_instance_lineage'
          AND t.tgname='zz_tapdb_validate_lineage_endpoint_scope'"""),
        {"schema": target["schema_name"]}).mappings().one())
    for key in ("source", "security_definer", "volatility", "settings"):
        if actual[key] != expected[key]:
            raise RuntimeError("installed scope trigger differs from the native package contract")
    if (actual["owner"] != target["operator_user"] or actual["enabled"] != "O"
        or actual["trigger_type"] != 23 or actual["deferrable"] or actual["qualifier"] is not None):
        raise RuntimeError("native scope trigger ownership/firing shape differs")


def _rows(session: Any, table: str, where: str, ids: list[Any], limit: int) -> list[dict[str, Any]]:
    # All SQL identifiers and predicates here are package constants, not request text.
    extra = {
        "generic_instance": "r.machine_uuid, r.template_uid, r.identity_key IS NOT NULL AS has_identity_key, r.json_addl ? 'identity_claim' AS has_identity_claim",
        "generic_instance_lineage": "r.parent_instance_uid, r.child_instance_uid, r.relationship_type",
        "generic_template": "r.instance_prefix",
    }[table]
    result = session.execute(text(
        f"SELECT {_COMMON}, {extra}, {_HASH} AS protected_sha256 FROM {table} r WHERE {where} ORDER BY r.uid LIMIT :bound"
    ), {"ids": ids, "bound": limit + 1}).mappings().all()
    if len(result) > limit:
        raise ValueError("scope correction inventory exceeds its bounded cohort")
    return json.loads(_canonical([dict(row) for row in result]))


def _inventory(session: Any, request: dict[str, Any]) -> dict[str, Any]:
    sources = _rows(session, "generic_instance", "r.euid = ANY(:ids)", request["source_euids"], MAX_SOURCES)
    if {row["euid"] for row in sources} != set(request["source_euids"]):
        raise ValueError("source cohort is absent or not completely visible")
    ids = [row["uid"] for row in sources]
    edges = _rows(session, "generic_instance_lineage", "(r.parent_instance_uid = ANY(:ids) OR r.child_instance_uid = ANY(:ids))", ids, MAX_EDGES)
    neighbors = sorted({row[field] for row in edges for field in ("parent_instance_uid", "child_instance_uid")} - set(ids))
    endpoints = _rows(session, "generic_instance", "r.uid = ANY(:ids)", neighbors, MAX_EDGES * 2)
    receipt_template = session.scalars(select(generic_template).where(
        generic_template.domain_code == request["target"]["domain_code"],
        generic_template.issuer_app_code == request["target"]["owner_repo_name"],
        generic_template.category == "evidence", generic_template.type == "repair",
        generic_template.subtype == "scope_assignment", generic_template.version == "1.0",
    )).one_or_none()
    if (receipt_template is None or receipt_template.is_deleted
        or receipt_template.tenant_id is not None or receipt_template.instance_prefix != "GVR"
        or receipt_template.instance_polymorphic_identity != "generic_instance"):
        raise RuntimeError("exact native scope-assignment evidence template must be provisioned first")
    template_ids = sorted({row["template_uid"] for row in sources} | {receipt_template.uid})
    templates = _rows(session, "generic_template", "r.uid = ANY(:ids)", template_ids, MAX_SOURCES + 1)
    return {"sources": sources, "lineages": edges, "neighbors": endpoints, "templates": templates}


def _validate_inventory(request: dict[str, Any], inventory: dict[str, Any]) -> None:
    target = request["target"]
    templates = {row["uid"]: row for row in inventory["templates"]}
    for source in inventory["sources"]:
        code = "/".join(source[key] for key in ("category", "type", "subtype", "version")) + "/"
        if source["is_deleted"] or source["tenant_id"] is not None:
            raise ValueError("only active native-global sources can be corrected")
        if source["has_identity_key"] or source["has_identity_claim"]:
            raise ValueError("claimed identities are outside this correction contract")
        if source["category"] in {"actor", "reference", "governance", "evidence"} or code not in request["allowed_templates"]:
            raise ValueError("source is outside the reviewed domain-template allowlist")
        template = templates.get(source["template_uid"])
        if template is None or template["is_deleted"] or template["tenant_id"] is not None:
            raise ValueError("source requires its exact active global native template")
        if template["instance_prefix"] != source["euid_prefix"]:
            raise ValueError("source/template prefix differs; no identity repair is permitted")
        if any(template[key] != source[key] for key in ("category", "type", "subtype", "version")):
            raise ValueError("source/template taxonomy differs")
    rows = inventory["sources"] + inventory["neighbors"] + inventory["templates"] + inventory["lineages"]
    for row in rows:
        if (row["domain_code"], row["issuer_app_code"]) != (target["domain_code"], target["owner_repo_name"]):
            raise ValueError("cohort crosses an owner/domain boundary")
    endpoint = {row["uid"]: row for row in inventory["sources"] + inventory["neighbors"]}
    active_edges = {row["euid"]: row for row in inventory["lineages"] if not row["is_deleted"]}
    for row in active_edges.values():
        if row["tenant_id"] is not None:
            raise ValueError("historical incident lineage must be native-global")
        for field in ("parent_instance_uid", "child_instance_uid"):
            item = endpoint.get(row[field])
            if item is None or item["is_deleted"] or item["tenant_id"] is not None:
                raise ValueError("incident active endpoints must be exact active global objects")
    if not set(request["retire_lineage_euids"]).issubset(active_edges):
        raise ValueError("every planned retirement must name an active incident lineage")
    if any(active_edges[euid]["relationship_type"] != "external_relation:identifies" for euid in request["retire_lineage_euids"]):
        raise ValueError("only reviewed legacy identifier assertions may be retired")


def preview_scope_correction(session: Any, request: Mapping[str, Any]) -> dict[str, Any]:
    """Read the exact bounded cohort; do not mutate, infer scope or allocate IDs."""
    request = validate_request(request)
    _authority(session, request)
    _require_native_trigger(session, request)
    inventory = _inventory(session, request)
    _validate_inventory(request, inventory)
    plan = {"contract": CONTRACT, "request": request, "before": inventory}
    return {**plan, "sha256": _sha(plan)}


def _validate_plan(plan: Mapping[str, Any], expected_sha256: str) -> dict[str, Any]:
    if set(plan) != {"contract", "request", "before", "sha256"} or plan["contract"] != CONTRACT:
        raise ValueError("unsupported scope correction plan")
    if plan["sha256"] != expected_sha256 or _sha({key: value for key, value in plan.items() if key != "sha256"}) != expected_sha256:
        raise ValueError("scope correction plan checksum differs from exact approval")
    validate_request(plan["request"])
    _validate_inventory(plan["request"], plan["before"])
    return dict(plan)


def _receipt_object(session: Any, request: dict[str, Any]) -> Any:
    key = "tapdb.scope-correction/v1:" + request["operation_id"]
    rows = session.scalars(select(generic_instance).where(
        generic_instance.identity_key == key,
        generic_instance.domain_code == request["target"]["domain_code"],
        generic_instance.issuer_app_code == request["target"]["owner_repo_name"],
        generic_instance.tenant_id == UUID(request["tenant_id"]),
        generic_instance.category == "evidence", generic_instance.type == "repair",
        generic_instance.subtype == "scope_assignment", generic_instance.version == "1.0",
    )).all()
    if len(rows) > 1 or (rows and rows[0].is_deleted):
        raise RuntimeError("scope correction evidence is ambiguous or inactive")
    return rows[0] if rows else None


def _verify_after(session: Any, plan: dict[str, Any], expected: dict[str, Any]) -> None:
    actual = _inventory(session, plan["request"])
    # Bind every old row, then admit only independently projected native XRFs
    # or this operation's exact receipt links. No arbitrary callback edges.
    for group in ("sources", "lineages", "neighbors", "templates"):
        indexed = {row["euid"]: row for row in actual[group]}
        for row in expected[group]:
            if indexed.get(row["euid"]) != row:
                raise RuntimeError(f"scope correction after-state changed: {group}/{row['euid']}")
    known = {row["euid"] for row in expected["lineages"]}
    added = [row for row in actual["lineages"] if row["euid"] not in known]
    source_ids = {row["uid"] for row in actual["sources"]}
    target = plan["request"]["target"]
    receipt = _receipt_object(session, plan["request"])
    canonical = set()
    service = ExternalReferenceService(session)
    for uid in {row["parent_instance_uid"] for row in added} & source_ids:
        source = session.get(generic_instance, uid)
        cursor = None
        count = 0
        while True:
            page = service.list_for_source(source, limit=500, cursor=cursor)
            count += len(page["items"])
            if count > MAX_EDGES:
                raise ValueError("canonical assertion projection exceeds correction bound")
            canonical.update(item["lineage_euid"] for item in page["items"])
            cursor = page["page"]["next_cursor"]
            if cursor is None:
                break
    allowed_neighbors = set()
    for row in added:
        native_scope = (not row["is_deleted"] and row["tenant_id"] == plan["request"]["tenant_id"]
            and row["domain_code"] == target["domain_code"]
            and row["issuer_app_code"] == target["owner_repo_name"])
        is_xrf = row["euid"] in canonical and row["parent_instance_uid"] in source_ids
        is_receipt = (receipt is not None and row["parent_instance_uid"] == receipt.uid
            and row["child_instance_uid"] in source_ids and row["relationship_type"] == "records_scope_correction")
        if not native_scope or not (is_xrf or is_receipt):
            raise RuntimeError("unexpected added lineage in reviewed correction")
        allowed_neighbors.add(row["child_instance_uid"] if is_xrf else row["parent_instance_uid"])
    previous_neighbors = {row["uid"] for row in expected["neighbors"]}
    if any(row["uid"] not in previous_neighbors | allowed_neighbors for row in actual["neighbors"]):
        raise RuntimeError("unexpected added neighbor in reviewed correction")


def get_scope_correction_status(session: Any, plan: Mapping[str, Any], expected_sha256: str) -> dict[str, Any]:
    """Resolve a lost commit result using durable evidence and exact readback."""
    plan = _validate_plan(plan, expected_sha256)
    _authority(session, plan["request"])
    obj = _receipt_object(session, plan["request"])
    if obj is not None:
        props = obj.json_addl["properties"]
        if props.get("plan_sha256") != expected_sha256:
            raise ValueError("operation ID is already bound to a different plan")
        _verify_after(session, plan, props["after"])
        return {"status": "committed", "receipt_euid": obj.euid, "plan_sha256": expected_sha256}
    if _inventory(session, plan["request"]) != plan["before"]:
        raise RuntimeError("no receipt and source state differs; outcome requires reconciliation")
    return {"status": "not_applied", "plan_sha256": expected_sha256}


@contextmanager
def scope_correction_transaction(session: Any, plan: Mapping[str, Any], expected_sha256: str) -> Iterator[dict[str, Any]]:
    """Correct scope, allow native XRF composition, then record exact evidence.

    Caller must propagate failures out of its outer session_scope. Planned legacy
    retirements must occur in the yielded body through soft_delete_object with
    an exact lineage ObjectSelector and dry_run=False, after native XRF attach.
    No runtime credentials or ad-hoc field mutation are authorized by this API.
    """
    plan = _validate_plan(plan, expected_sha256)
    request = plan["request"]
    _authority(session, request)
    session.execute(text("SET LOCAL lock_timeout = '10s'"))
    session.execute(text("LOCK TABLE generic_instance, generic_instance_lineage IN SHARE ROW EXCLUSIVE MODE"))
    status = get_scope_correction_status(session, plan, expected_sha256)
    if status["status"] == "committed":
        raise ValueError("correction already committed; use status, do not replay mutation body")
    if preview_scope_correction(session, request) != plan:
        raise ValueError("scope correction preview is stale")
    if session.execute(text("SELECT current_setting(:name, true)"), {"name": SETTING}).scalar():
        raise RuntimeError("nested reviewed scope correction is forbidden")
    _require_native_trigger(session, request)
    target = request["target"]
    tx = dict(session.execute(text("SELECT pg_backend_pid() AS backend_pid, pg_current_xact_id()::text AS transaction_id")).mappings().one())
    source_ids = {row["uid"] for row in plan["before"]["sources"]}
    endpoint = {row["uid"]: row for row in plan["before"]["sources"] + plan["before"]["neighbors"]}
    transitions = []
    for row in plan["before"]["lineages"]:
        if row["is_deleted"]:
            continue
        transitions.append({
            "uid": row["uid"], "euid": row["euid"], "protected_sha256": row["protected_sha256"],
            "parent_euid": endpoint[row["parent_instance_uid"]]["euid"],
            "child_euid": endpoint[row["child_instance_uid"]]["euid"],
            "parent_tenant": request["tenant_id"] if row["parent_instance_uid"] in source_ids else None,
            "child_tenant": request["tenant_id"] if row["child_instance_uid"] in source_ids else None,
            "old_tenant": None, "new_tenant": request["tenant_id"], "retire": False,
        })
    manifest = {"contract": CONTRACT, **target, **tx, "plan_sha256": expected_sha256,
                "operation_id": request["operation_id"], "actor": request["actor"], "transitions": transitions}
    savepoint = session.begin_nested()
    try:
        session.execute(text("SELECT set_config(:name, :value, true)"), {"name": SETTING, "value": _canonical(manifest)})
        for obj in session.scalars(select(generic_instance).where(generic_instance.uid.in_(source_ids))):
            obj.tenant_id = UUID(request["tenant_id"])
        session.flush()
        active_ids = [row["uid"] for row in plan["before"]["lineages"] if not row["is_deleted"]]
        for obj in session.scalars(select(generic_instance_lineage).where(generic_instance_lineage.uid.in_(active_ids))):
            obj.tenant_id = UUID(request["tenant_id"])
        session.flush()
        session.expire_all()
        after_scope = _inventory(session, request)
        for group in ("sources", "lineages", "neighbors", "templates"):
            for before, after in zip(plan["before"][group], after_scope[group], strict=True):
                expected = dict(before)
                if group == "sources" or (group == "lineages" and not before["is_deleted"]):
                    expected["tenant_id"] = request["tenant_id"]
                if expected != after:
                    raise RuntimeError("scope correction modified protected historical state")
        remaining = json.loads(session.execute(text("SELECT current_setting(:name)"), {"name": SETTING}).scalar_one())
        if remaining["transitions"]:
            raise RuntimeError("scope transition manifest was not completely consumed")
        # A second, exact transition permits only planned old-assertion retirement.
        retire = set(request["retire_lineage_euids"])
        remaining["transitions"] = [
            {**item, "old_tenant": request["tenant_id"], "retire": True}
            for item in transitions if item["euid"] in retire
        ]
        session.execute(text("SELECT set_config(:name, :value, true)"), {"name": SETTING, "value": _canonical(remaining)})
        outcome = {"status": "pending_commit", "plan_sha256": expected_sha256}
        yield outcome
        session.flush()
        session.expire_all()
        remaining = json.loads(session.execute(text("SELECT current_setting(:name)"), {"name": SETTING}).scalar_one())
        if remaining["transitions"]:
            raise RuntimeError("planned legacy retirement was not completed")
        after = _inventory(session, request)
        # Preserve the exact old cohort; new native XRFs are separately validated
        # by ExternalReferenceService, and represented by their native lineage.
        expected_after = json.loads(_canonical(after_scope))
        current_edges = {row["euid"]: row for row in after["lineages"]}
        for row in expected_after["lineages"]:
            if row["euid"] in retire:
                current = current_edges[row["euid"]]
                if not current["is_deleted"]:
                    raise RuntimeError("legacy assertion is still active")
                row.update(is_deleted=True, protected_sha256=current["protected_sha256"])
        _verify_after(session, plan, expected_after)
        session.execute(text("SELECT set_config(:name, '', true)"), {"name": SETTING})
        factory = InstanceFactory(TemplateManager(), domain_code=target["domain_code"])
        claim = factory.claim_instance_by_identity(
            session, template_code=TEMPLATE,
            identity_key="tapdb.scope-correction/v1:" + request["operation_id"],
            name="Reviewed native scope correction", scope=IdentityScope.TENANT,
            tenant_id=UUID(request["tenant_id"]), create_children=False,
            properties={"contract": CONTRACT, "scope_changed": True,
                "plan_sha256": expected_sha256, "operation_id": request["operation_id"],
                "actor": request["actor"], "reason": request["reason"],
                "approval_ref": request["approval_ref"], "target": target,
                "migration_receipt_sha256": request["migration_receipt_sha256"],
                "tenant_authority_sha256": request["tenant_authority_sha256"],
                "recorded_at": datetime.now(UTC).isoformat(),
                "before": plan["before"], "after": after},
            command_evidence={"plan_sha256": expected_sha256},
        )
        if claim.outcome != IdentityClaimOutcome.CREATED:
            raise RuntimeError("unexpected existing correction evidence; use status")
        for obj in session.scalars(select(generic_instance).where(generic_instance.uid.in_(source_ids))):
            factory.link_instances(session, claim.instance, obj, "records_scope_correction")
        session.flush()
        outcome["receipt_euid"] = claim.instance.euid
        savepoint.commit()
    except BaseException:
        # Keep the caller's outer transaction, but never leave a partial cohort.
        savepoint.rollback()
        raise


def apply_scope_correction(session: Any, plan: Mapping[str, Any], expected_sha256: str) -> dict[str, Any]:
    """Apply scope only; use transaction composition for paired XRF retirement."""
    plan = _validate_plan(plan, expected_sha256)
    if plan["request"]["retire_lineage_euids"]:
        raise ValueError("legacy retirement requires native XRF transaction composition")
    status = get_scope_correction_status(session, plan, expected_sha256)
    if status["status"] == "committed":
        return status
    with scope_correction_transaction(session, plan, expected_sha256) as outcome:
        pass
    return outcome


def load_scope_document(path: str) -> dict[str, Any]:
    source = Path(path)
    if not source.is_absolute() or not source.is_file():
        raise ValueError("scope request/plan must be an existing absolute file")
    if source.stat().st_size > 4_000_000:
        raise ValueError("scope request/plan exceeds the bounded document size")
    value = json.loads(source.read_text())
    if not isinstance(value, dict):
        raise ValueError("scope request/plan must be an object")
    return value


def write_scope_document(path: str, payload: Mapping[str, Any]) -> None:
    """Atomic evidence export, separate from the caller-owned DB transaction."""
    destination = Path(path)
    if not destination.is_absolute() or not destination.parent.is_dir():
        raise ValueError("receipt requires an absolute path in an existing directory")
    descriptor, temporary = tempfile.mkstemp(prefix=".scope-receipt-", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
