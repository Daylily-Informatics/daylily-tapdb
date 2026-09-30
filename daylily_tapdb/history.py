"""Local historical states and guarded forward correction, without messaging.

Boundary tokens identify MVCC visibility, not wall-clock commit ordering. Full
states are available only for prospective revision evidence or an explicit
adoption observation. Earlier audit formats remain separately inspectable.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from functools import cached_property
from typing import Any
from uuid import UUID

from sqlalchemy import text

from daylily_tapdb.advisory_locks import acquire_transaction_advisory_lock
from daylily_tapdb.external_references import _is_xrf_coordinates
from daylily_tapdb.factory import IdentityScope, InstanceFactory
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.revisions import lock_records, require_revision, apply_guarded_fields
from daylily_tapdb.services.object_operations import ObjectSelector, resolve_object
from daylily_tapdb.templates.manager import TemplateManager

_TABLES = {
    "instance": "generic_instance",
    "template": "generic_template",
    "lineage": "generic_instance_lineage",
}
_HISTORY_ROWS = """(
    SELECT uid,rel_table_name,rel_table_uid_fk,rel_table_euid_fk,domain_code,issuer_app_code,json_addl
    FROM audit_log
    UNION ALL
    SELECT 0,rel_table_name,rel_table_uid_fk,rel_table_euid_fk,domain_code,issuer_app_code,
        jsonb_build_object('format','tapdb.adoption/v1','after',state,'before',NULL,
            'revision',record_revision,'epoch',epoch,'transaction_id',transaction_id,
            'attribution',attribution,'governing_template',governing_template,'observed_at',observed_at)
    FROM tapdb_history_baseline
) history_rows"""
_MUTABLE = {"name", "bstatus", "json_addl", "is_deleted"}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class HistoryError(ValueError):
    pass


class HistoryService:
    def __init__(
        self, session: Any, *, owner_validators: dict[str, Callable] | None = None
    ):
        self.session = session
        self._explicit_owner_validators = owner_validators

    @cached_property
    def owner_validators(self):
        # Load deployment code only for correction planning/application.
        # Ordinary historical reads and references do not depend on it.
        from daylily_tapdb.owner_validation import load_owner_validators

        return load_owner_validators(self._explicit_owner_validators)

    def _scope(self) -> dict[str, Any]:
        row = (
            self.session.execute(text("""SELECT current_database() AS database,
            current_schema() AS schema, tapdb_current_domain_code() AS domain,
            tapdb_current_owner_repo_name() AS owner, tapdb_current_tenant_id()::text AS tenant,
            tapdb_allowed_tenant_ids()::text AS tenants,
            (SELECT epoch::text FROM tapdb_history_epoch WHERE active) AS epoch"""))
            .mappings()
            .one()
        )
        if not row["epoch"]:
            raise HistoryError("native history adoption required")
        return dict(row)

    def capture_boundary(self) -> str:
        if (
            self.session.execute(
                text("SELECT pg_current_xact_id_if_assigned()::text")
            ).scalar_one()
            is not None
        ):
            raise HistoryError(
                "capture_boundary requires a transaction with no preceding writes"
            )
        snapshot = self.session.execute(
            text("SELECT pg_current_snapshot()::text")
        ).scalar_one()
        payload = {
            "format": "tapdb.history-boundary/v1",
            **self._scope(),
            "snapshot": snapshot,
        }
        return base64.urlsafe_b64encode(_canonical(payload).encode()).decode()

    def _boundary(self, token: str) -> dict[str, Any]:
        if not isinstance(token, str) or len(token) > 1048576:
            raise HistoryError("invalid boundary token")
        try:
            value = json.loads(base64.b64decode(token, altchars=b"-_", validate=True))
        except (ValueError, UnicodeError) as exc:
            raise HistoryError("invalid boundary token") from exc
        scope = self._scope()
        if (
            not isinstance(value, dict)
            or value.get("format") != "tapdb.history-boundary/v1"
            or any(value.get(k) != v for k, v in scope.items())
        ):
            raise HistoryError(
                "boundary belongs to a different database, scope or history epoch"
            )
        self.session.execute(
            text("SELECT CAST(:snapshot AS pg_snapshot)"),
            {"snapshot": value["snapshot"]},
        )
        return value

    def _states(
        self,
        boundary: str,
        *,
        record_table: str,
        euids: list[str] | None = None,
        limit: int = 10001,
        uids: list[int] | None = None,
        endpoints: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        token = self._boundary(boundary)
        params = {
            "table": record_table,
            "epoch": token["epoch"],
            "snapshot": token["snapshot"],
            "limit": limit,
        }
        selection = ""
        if euids is not None:
            params["euids"] = euids
            selection = " AND rel_table_euid_fk = ANY(:euids)"
        outer = []
        if uids is not None:
            params["uids"] = uids
            outer.append(
                "(json_addl->'after'->>'uid')::bigint = ANY(CAST(:uids AS bigint[]))"
            )
        if endpoints is not None:
            params["endpoints"] = endpoints
            outer.append(
                "((json_addl->'after'->>'parent_instance_uid')::bigint = ANY(CAST(:endpoints AS bigint[])) OR (json_addl->'after'->>'child_instance_uid')::bigint = ANY(CAST(:endpoints AS bigint[])))"
            )
        rows = self.session.execute(
            text(
                """SELECT * FROM (SELECT DISTINCT ON (domain_code,issuer_app_code,rel_table_uid_fk,rel_table_euid_fk)
            rel_table_euid_fk AS euid, json_addl
            FROM """
                + _HISTORY_ROWS
                + """ WHERE rel_table_name=:table
            AND json_addl->>'format' IN ('tapdb.revision/v1','tapdb.adoption/v1')
            AND json_addl->>'epoch'=:epoch
            AND pg_visible_in_snapshot(CAST(CASE WHEN json_addl->>'format' IN ('tapdb.revision/v1','tapdb.adoption/v1') THEN json_addl->>'transaction_id' END AS xid8), CAST(:snapshot AS pg_snapshot))
            """
                + selection
                + """ ORDER BY domain_code,issuer_app_code,rel_table_uid_fk,rel_table_euid_fk,
            (CASE WHEN json_addl->>'format' IN ('tapdb.revision/v1','tapdb.adoption/v1') THEN json_addl->>'revision' END)::bigint DESC, uid DESC) latest
            """
                + (" WHERE " + " AND ".join(outer) if outer else "")
                + " ORDER BY euid LIMIT :limit"
            ),
            params,
        ).mappings()
        return [
            {
                "euid": r["euid"],
                "state": r["json_addl"]["after"],
                "completeness": (
                    "observed_at_adoption"
                    if r["json_addl"]["format"] == "tapdb.adoption/v1"
                    else "full_revision"
                ),
                "revision": r["json_addl"]["revision"],
                "epoch": r["json_addl"]["epoch"],
                "governing_template": r["json_addl"].get("governing_template"),
            }
            for r in rows
        ]

    def object_at(
        self,
        euid: str,
        *,
        record_type: str,
        boundary: str | None = None,
        revision: int | None = None,
        epoch: str | None = None,
    ) -> dict[str, Any]:
        if record_type not in _TABLES or (boundary is None) == (revision is None):
            raise HistoryError(
                "supply a record type and exactly one boundary or revision"
            )
        if boundary is not None:
            if epoch is not None:
                raise HistoryError("boundary already identifies its epoch")
            found = self._states(
                boundary, record_table=_TABLES[record_type], euids=[euid], limit=2
            )
            if len(found) > 1:
                raise HistoryError("ambiguous historical object identity")
            return (
                found[0]
                if found
                else {
                    "euid": euid,
                    "state": None,
                    "completeness": "not_established_at_boundary",
                }
            )
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            raise HistoryError("revision must be a nonnegative integer")
        scope = self._scope()
        selected_epoch = str(UUID(epoch)) if epoch is not None else scope["epoch"]
        rows = (
            self.session.execute(
                text("""SELECT json_addl FROM """ + _HISTORY_ROWS + """
            WHERE rel_table_name=:table AND rel_table_euid_fk=:euid
              AND json_addl->>'epoch'=:epoch AND (CASE WHEN json_addl->>'format' IN ('tapdb.revision/v1','tapdb.adoption/v1') THEN json_addl->>'revision' END)::bigint=:revision
              AND json_addl->>'format' IN ('tapdb.revision/v1','tapdb.adoption/v1')"""),
                {
                    "table": _TABLES[record_type],
                    "euid": euid,
                    "epoch": selected_epoch,
                    "revision": revision,
                },
            )
            .scalars()
            .all()
        )
        if len(rows) != 1:
            raise HistoryError(
                "exact revision is unavailable or ambiguous in this epoch"
            )
        row = rows[0]
        return {
            "euid": euid,
            "state": row["after"],
            "revision": revision,
            "epoch": selected_epoch,
            "governing_template": row.get("governing_template"),
            "completeness": (
                "observed_at_adoption"
                if row["format"] == "tapdb.adoption/v1"
                else "full_revision"
            ),
        }

    def graph_at(
        self,
        seeds: list[str],
        *,
        boundary: str,
        max_depth: int = 8,
        max_nodes: int = 1000,
    ) -> dict[str, Any]:
        if not seeds or not 0 <= max_depth <= 32 or not 1 <= max_nodes <= 10000:
            raise HistoryError("explicit seeds and bounded depth/node limits required")
        initial = self._states(
            boundary, record_table="generic_instance", euids=seeds, limit=max_nodes + 1
        )
        if len(initial) > max_nodes:
            raise HistoryError("seeds exceed node limit")
        nodes = {n["state"]["uid"]: n for n in initial}
        frontier = set(nodes)
        edges = {}
        missing_endpoints = set()
        complete = len(initial) == len(set(seeds))
        for depth in range(max_depth + 1):
            if not frontier:
                break
            adjacent = self._states(
                boundary,
                record_table="generic_instance_lineage",
                endpoints=sorted(frontier),
                limit=10001,
            )
            if len(adjacent) > 10000:
                raise HistoryError("bounded historical edge limit exceeded")
            active = [e for e in adjacent if not e["state"]["is_deleted"]]
            neighbors = {
                uid
                for edge in active
                for uid in (
                    edge["state"]["parent_instance_uid"],
                    edge["state"]["child_instance_uid"],
                )
            } - set(nodes)
            if depth == max_depth:
                complete = complete and not neighbors
                break
            candidates = (
                self._states(
                    boundary,
                    record_table="generic_instance",
                    uids=sorted(neighbors),
                    limit=max_nodes + 1,
                )
                if neighbors
                else []
            )
            if len(nodes) + len(candidates) > max_nodes:
                complete = False
                break
            missing_endpoints.update(
                neighbors - {n["state"]["uid"] for n in candidates}
            )
            if missing_endpoints:
                complete = False
            edges.update({e["euid"]: e for e in active})
            frontier = {n["state"]["uid"] for n in candidates}
            nodes.update({n["state"]["uid"]: n for n in candidates})
        # Include active edges whose endpoints are both included, including cycles.
        if nodes:
            adjacent = self._states(
                boundary,
                record_table="generic_instance_lineage",
                endpoints=sorted(nodes),
                limit=10001,
            )
            if len(adjacent) > 10000:
                raise HistoryError("bounded historical edge limit exceeded")
            edges = {
                e["euid"]: e
                for e in adjacent
                if not e["state"]["is_deleted"]
                and e["state"]["parent_instance_uid"] in nodes
                and e["state"]["child_instance_uid"] in nodes
            }
        return {
            "boundary": boundary,
            "nodes": sorted(nodes.values(), key=lambda n: n["euid"]),
            "edges": [edges[e] for e in sorted(edges)],
            "complete": complete,
            "missing_seeds": sorted(set(seeds) - {n["euid"] for n in nodes.values()}),
            "missing_endpoint_uids": sorted(missing_endpoints),
            "external_boundaries": [
                n["euid"]
                for n in nodes.values()
                if n["state"].get("category") == "reference"
                and n["state"].get("type") == "external_identifier"
            ],
            "external_expansion": False,
        }

    def describe(self) -> dict[str, Any]:
        """Read-only completeness diagnostics, never a backup/RPO guarantee."""
        scope = self._scope()
        epochs = [
            dict(row)
            for row in self.session.execute(
                text(
                    "SELECT epoch::text,created_at,active,origin FROM tapdb_history_epoch ORDER BY created_at,epoch"
                )
            ).mappings()
        ]
        evidence = [
            dict(row)
            for row in self.session.execute(
                text(
                    """SELECT
            COALESCE(json_addl->>'format','legacy_incomplete') AS format,
            json_addl->>'epoch' AS epoch,count(*) AS rows,min(changed_at) AS first_recorded,
            max(changed_at) AS last_recorded FROM audit_log GROUP BY 1,2 ORDER BY 1,2"""
                )
            ).mappings()
        ]
        return {
            "scope": scope,
            "epochs": epochs,
            "audit_evidence": evidence,
            "baseline_rows": self.session.execute(
                text("SELECT count(*) FROM tapdb_history_baseline")
            ).scalar_one(),
            "time_filters_are_commit_boundaries": False,
            "remote_reconstruction": False,
        }

    def plan_correction(
        self, changes: list[dict[str, Any]], *, operation_id: str, reason: str
    ) -> dict[str, Any]:
        if (
            not isinstance(operation_id, str)
            or not operation_id
            or operation_id != operation_id.strip()
            or not isinstance(reason, str)
            or not reason.strip()
            or not isinstance(changes, list)
            or not 1 <= len(changes) <= 500
        ):
            raise HistoryError(
                "explicit operation identity, reason, and changes are required"
            )
        planned = []
        dependencies = {}
        seen = set()
        for change in changes:
            obj, kind = resolve_object(
                self.session,
                ObjectSelector(euid=change["euid"], record_type=change["record_type"]),
            )
            if obj.euid in seen:
                raise HistoryError("duplicate correction subject")
            seen.add(obj.euid)
            if isinstance(obj, generic_instance_lineage):
                for uid in (obj.parent_instance_uid, obj.child_instance_uid):
                    endpoint = (
                        self.session.query(generic_instance).filter_by(uid=uid).one()
                    )
                    dependencies[endpoint.euid] = {
                        "euid": endpoint.euid,
                        "record_type": "instance",
                        "expected_revision": endpoint.record_revision,
                    }
            fields = change["fields"]
            if not isinstance(fields, dict) or not fields or set(fields) - _MUTABLE:
                raise HistoryError("correction may change only selected mutable fields")
            if _is_xrf_coordinates(obj):
                raise HistoryError(
                    "canonical references require native reference operations"
                )
            diff = {
                k: {"before": getattr(obj, k), "after": v}
                for k, v in fields.items()
                if getattr(obj, k) != v
            }
            planned.append(
                {
                    "euid": obj.euid,
                    "record_type": kind,
                    "expected_revision": obj.record_revision,
                    "owner": obj.issuer_app_code,
                    "fields": fields,
                    "diff": diff,
                    "owner_validator_available": obj.issuer_app_code
                    in self.owner_validators,
                }
            )
        plan = {
            "format": "tapdb.correction-plan/v1",
            "scope": self._scope(),
            "operation_id": operation_id,
            "reason": reason,
            "changes": planned,
            "dependencies": [dependencies[key] for key in sorted(dependencies)],
        }
        plan["sha256"] = hashlib.sha256(_canonical(plan).encode()).hexdigest()
        return plan

    def apply_correction(self, plan: dict[str, Any]) -> dict[str, Any]:
        supplied = dict(plan)
        digest = supplied.pop("sha256", None)
        if (
            digest != hashlib.sha256(_canonical(supplied).encode()).hexdigest()
            or supplied.get("format") != "tapdb.correction-plan/v1"
        ):
            raise HistoryError("invalid correction plan")
        if (
            not isinstance(supplied.get("changes"), list)
            or not 1 <= len(supplied["changes"]) <= 500
        ):
            raise HistoryError("correction requires 1..500 explicit changes")
        if supplied["scope"] != self._scope():
            raise HistoryError("correction scope or history epoch changed")
        ctx = self.session.execute(
            text("SELECT tapdb_current_attribution()")
        ).scalar_one()
        if (
            ctx["operation_id"] != plan["operation_id"]
            or ctx.get("correction_reason") != plan["reason"]
        ):
            raise HistoryError(
                "correction attribution must carry the planned operation and reason"
            )
        scope = supplied["scope"]
        acquire_transaction_advisory_lock(
            self.session,
            "tapdb.correction",
            scope["epoch"],
            scope["owner"],
            str(scope["tenant"]),
            plan["operation_id"],
        )
        key = (
            "tapdb.correction:"
            + hashlib.sha256(plan["operation_id"].encode()).hexdigest()
        )
        existing = (
            self.session.query(generic_instance)
            .filter_by(
                identity_key=key,
                category="governance",
                type="correction_receipt",
                subtype="generic",
                version="1.0",
                domain_code=scope["domain"],
                issuer_app_code=scope["owner"],
                tenant_id=UUID(scope["tenant"]) if scope["tenant"] else None,
            )
            .one_or_none()
        )
        if existing is not None:
            payload = existing.json_addl["properties"]
            if payload["plan_sha256"] != digest:
                raise HistoryError(
                    "operation identity already used for a different correction"
                )
            return {"euid": existing.euid, **payload, "replayed": True}
        objects = [
            resolve_object(
                self.session,
                ObjectSelector(euid=c["euid"], record_type=c["record_type"]),
            )[0]
            for c in plan["changes"]
        ]
        # Endpoint changes can invalidate a lineage correction even when the
        # lineage itself is unchanged. Preserve those preview preconditions too.
        dependencies = [
            resolve_object(
                self.session,
                ObjectSelector(euid=c["euid"], record_type=c["record_type"]),
            )[0]
            for c in plan["dependencies"]
        ]
        lock_records(self.session, objects + dependencies)
        for obj, dependency in zip(dependencies, plan["dependencies"], strict=True):
            require_revision(obj, dependency["expected_revision"])
        required_endpoints = {
            uid
            for obj in objects
            if isinstance(obj, generic_instance_lineage)
            for uid in (obj.parent_instance_uid, obj.child_instance_uid)
        }
        if required_endpoints != {obj.uid for obj in dependencies}:
            raise HistoryError("correction endpoint preconditions are incomplete")
        for obj, change in zip(objects, plan["changes"], strict=True):
            require_revision(obj, change["expected_revision"])
            validator = self.owner_validators.get(obj.issuer_app_code)
            if validator is None:
                raise HistoryError(
                    "owner validator is not registered: " + obj.issuer_app_code
                )
            if _is_xrf_coordinates(obj) or set(change["fields"]) - _MUTABLE:
                raise HistoryError("immutable correction target")
            for field, value in change["fields"].items():
                if field in {"name", "bstatus"} and (
                    not isinstance(value, str) or not value.strip()
                ):
                    raise HistoryError("name/status must be nonempty strings")
                if field == "is_deleted" and not isinstance(value, bool):
                    raise HistoryError("is_deleted must be boolean")
                if field == "json_addl" and not isinstance(value, dict):
                    raise HistoryError("json_addl must be an object")
            diff = {
                key: {"before": getattr(obj, key), "after": value}
                for key, value in change["fields"].items()
                if getattr(obj, key) != value
            }
            if diff != change["diff"] or obj.issuer_app_code != change["owner"]:
                raise HistoryError("correction preconditions changed")
            validator(self.session, obj, change["fields"])
        # Validate every subject before mutating any. Caller owns outer transaction.
        for obj, change in zip(objects, plan["changes"], strict=True):
            apply_guarded_fields(
                self.session,
                obj,
                change["fields"],
                expected_revision=change["expected_revision"],
            )
        self.session.flush()
        for obj in objects:
            self.session.refresh(obj)
        payload = {
            "format": "tapdb.correction-receipt/v1",
            "plan_sha256": digest,
            "operation_id": plan["operation_id"],
            "reason": plan["reason"],
            "revisions": {obj.euid: obj.record_revision for obj in objects},
        }
        receipt = (
            InstanceFactory(TemplateManager(), domain_code=scope["domain"])
            .claim_instance_by_identity(
                self.session,
                template_code="governance/correction_receipt/generic/1.0/",
                identity_key=key,
                name="Forward correction receipt",
                scope=IdentityScope.TENANT if scope["tenant"] else IdentityScope.GLOBAL,
                tenant_id=UUID(scope["tenant"]) if scope["tenant"] else None,
                properties=payload,
            )
            .instance
        )
        return {"euid": receipt.euid, **payload, "replayed": False}
