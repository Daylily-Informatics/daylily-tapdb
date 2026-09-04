"""Canonical TapDB external-reference identities, lifecycle, and projections.

Applications supply business meaning and remote validation. TapDB owns the
local XRF identity and the authoritative source-to-XRF lineage. No operation in
this module commits or rolls back the caller's transaction.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Literal, Sequence, TypeAlias, cast
from urllib.parse import urlsplit

from sqlalchemy.orm import aliased

from daylily_tapdb.euid import validate_euid
from daylily_tapdb.factory import IdentityClaimOutcome, IdentityScope, InstanceFactory
from daylily_tapdb.factory.instance import _external_reference_writer_token
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.templates.manager import TemplateManager

TAPDB_OBJECT_TEMPLATE_CODE = "reference/external_identifier/tapdb_object/1.0/"
OPAQUE_IDENTIFIER_TEMPLATE_CODE = "reference/external_identifier/opaque/1.0/"
TAPDB_OBJECT_IDENTITY_NAMESPACE = "tapdb.external-reference/v1"
OPAQUE_IDENTITY_NAMESPACE = "tapdb.external-identifier/v1"

_TAPDB_COORDS = ("reference", "external_identifier", "tapdb_object", "1.0")
_OPAQUE_COORDS = ("reference", "external_identifier", "opaque", "1.0")
_XRF_COORDS = {_TAPDB_COORDS, _OPAQUE_COORDS}
_SERVICE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_LOWER_TOKEN_RE = re.compile(r"^[a-z][a-z0-9._-]*$")
_URI_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*$")
_OUTCOME_STATUSES = {
    "created",
    "existing",
    "reactivated",
    "deactivated",
    "already_inactive",
}


class ExternalReferenceContractError(ValueError):
    """A persisted row violates the canonical external-reference contract."""


@dataclass(frozen=True)
class TapDBObjectTarget:
    """An exact graph-expandable object owned by another TapDB service."""

    target_service_id: str
    target_object_euid: str
    target_tenant_id: uuid.UUID | None = None
    target_object_kind: str | None = None

    def __post_init__(self) -> None:
        _require_service_id(self.target_service_id)
        _require_persisted_euid(self.target_object_euid, "target_object_euid")
        if self.target_tenant_id is not None and not isinstance(
            self.target_tenant_id, uuid.UUID
        ):
            raise ValueError("target_tenant_id must be a UUID")
        if self.target_object_kind is not None:
            _require_exact_text(
                self.target_object_kind, "target_object_kind", max_length=128
            )

    @property
    def identity_key(self) -> str:
        return (
            f"{TAPDB_OBJECT_IDENTITY_NAMESPACE}:"
            f"{self.target_service_id}:{self.target_object_euid}"
        )


@dataclass(frozen=True)
class ExternalIdentifierTarget:
    """An exact non-federated identifier with explicit persistence scope."""

    namespace: str
    kind: str
    value: str
    scope: Literal["tenant", "public_global"]
    tenant_id: uuid.UUID | None = None
    canonical_uri: str | None = None

    def __post_init__(self) -> None:
        _require_lower_token(self.namespace, "namespace")
        _require_lower_token(self.kind, "kind")
        _require_identifier_value(self.value)
        if self.scope == "tenant":
            if not isinstance(self.tenant_id, uuid.UUID):
                raise ValueError("tenant-scoped identifiers require a tenant UUID")
        elif self.scope == "public_global":
            if self.tenant_id is not None:
                raise ValueError("public_global identifiers forbid tenant_id")
        else:
            raise ValueError("scope must be 'tenant' or 'public_global'")
        if self.canonical_uri is not None:
            _require_canonical_uri(self.canonical_uri)

    @property
    def identity_key(self) -> str:
        canonical = json.dumps(
            [self.namespace, self.kind, self.value],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return f"{OPAQUE_IDENTITY_NAMESPACE}:sha256:{digest}"


ExternalTarget: TypeAlias = TapDBObjectTarget | ExternalIdentifierTarget


@dataclass(frozen=True)
class ExternalLinkSpec:
    """One exact source-to-XRF assertion owned by a stable authority."""

    target: ExternalTarget
    relationship_type: str
    assertion_authority: str
    asserted_at: datetime
    assertion_provenance: str

    def __post_init__(self) -> None:
        if not isinstance(self.target, (TapDBObjectTarget, ExternalIdentifierTarget)):
            raise ValueError("target must be a canonical external target")
        _require_exact_text(self.relationship_type, "relationship_type", max_length=128)
        _require_exact_text(
            self.assertion_authority, "assertion_authority", max_length=256
        )
        _require_timezone_aware(self.asserted_at, "asserted_at")
        _require_exact_text(
            self.assertion_provenance,
            "assertion_provenance",
            max_length=2048,
        )


@dataclass(frozen=True)
class ExternalLinkOutcome:
    """Lifecycle status and persisted rows for an external link operation."""

    status: Literal[
        "created", "existing", "reactivated", "deactivated", "already_inactive"
    ]
    reference: generic_instance | None
    lineage: generic_instance_lineage | None

    def __post_init__(self) -> None:
        if self.status not in _OUTCOME_STATUSES:
            raise ValueError(f"unsupported external-link outcome: {self.status}")


def _require_exact_text(value: Any, field: str, *, max_length: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    if not value or value != value.strip() or len(value) > max_length:
        raise ValueError(
            f"{field} must be non-empty, exact, and at most {max_length} characters"
        )
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ValueError(f"{field} must not contain control characters")
    return value


def _require_lower_token(value: Any, field: str) -> str:
    text = _require_exact_text(value, field, max_length=128)
    if _LOWER_TOKEN_RE.fullmatch(text) is None:
        raise ValueError(
            f"{field} must be a lowercase canonical token using a-z, 0-9, '.', '_' or '-'"
        )
    return text


def _require_service_id(value: Any) -> str:
    text = _require_exact_text(value, "target_service_id", max_length=128)
    if _SERVICE_ID_RE.fullmatch(text) is None:
        raise ValueError(
            "target_service_id must start with an alphanumeric character and "
            "contain only alphanumerics, '.', '_' or '-'"
        )
    return text


def _require_persisted_euid(value: Any, field: str) -> str:
    text = _require_exact_text(value, field, max_length=255)
    if any(character.isspace() for character in text) or not validate_euid(text):
        raise ValueError(f"{field} must be a canonical persisted Meridian EUID")
    return text


def _require_identifier_value(value: Any) -> str:
    return _require_exact_text(value, "value", max_length=2048)


def _require_canonical_uri(value: Any) -> str:
    text = _require_exact_text(value, "canonical_uri", max_length=2048)
    parsed = urlsplit(text)
    if not parsed.scheme or _URI_SCHEME_RE.fullmatch(parsed.scheme) is None:
        raise ValueError("canonical_uri must be absolute")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("canonical_uri must not contain credentials")
    if parsed.fragment:
        raise ValueError("canonical_uri must not contain a fragment")
    if parsed.scheme in {"http", "https"} and not parsed.hostname:
        raise ValueError("canonical_uri HTTP(S) values require a host")
    return text


def _require_timezone_aware(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _parse_timestamp(value: Any, field: str) -> str:
    if isinstance(value, datetime):
        return _require_timezone_aware(value, field).isoformat()
    if not isinstance(value, str):
        raise ExternalReferenceContractError(f"{field} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ExternalReferenceContractError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    return _require_timezone_aware(parsed, field).isoformat()


def _json_properties(obj: Any) -> dict[str, Any]:
    payload = getattr(obj, "json_addl", None)
    if not isinstance(payload, dict):
        raise ExternalReferenceContractError("json_addl must be an object")
    properties = payload.get("properties")
    if not isinstance(properties, dict):
        raise ExternalReferenceContractError("json_addl.properties must be an object")
    return dict(properties)


def _coordinates(obj: Any) -> tuple[Any, Any, Any, Any]:
    return (
        getattr(obj, "category", None),
        getattr(obj, "type", None),
        getattr(obj, "subtype", None),
        getattr(obj, "version", None),
    )


def _is_xrf_coordinates(obj: Any) -> bool:
    return _coordinates(obj) in _XRF_COORDS


@lru_cache(maxsize=2)
def _canonical_template_definition(subtype: str) -> dict[str, Any]:
    from daylily_tapdb.templates.loader import (
        find_tapdb_core_config_dir,
        load_template_configs,
    )

    for template in load_template_configs(find_tapdb_core_config_dir()):
        if (
            template.get("category"),
            template.get("type"),
            template.get("subtype"),
            template.get("version"),
            template.get("instance_prefix"),
        ) == ("reference", "external_identifier", subtype, "1.0", "XRF"):
            return dict(template)
    raise RuntimeError(f"installed TapDB core inventory has no exact {subtype} XRF")


def _is_exact_xrf_template(template: Any, subtype: str) -> bool:
    canonical = _canonical_template_definition(subtype)
    for field in (
        "name",
        "polymorphic_discriminator",
        "category",
        "type",
        "subtype",
        "version",
        "instance_prefix",
        "is_singleton",
        "bstatus",
        "json_addl",
        "json_addl_schema",
    ):
        if getattr(template, field, None) != canonical.get(field):
            return False
    return True


def _reference_kind(reference: Any) -> Literal["tapdb_object", "opaque"]:
    coords = _coordinates(reference)
    if coords not in _XRF_COORDS:
        raise ExternalReferenceContractError("lineage target is not a core XRF")
    subtype = str(coords[2])
    template = getattr(reference, "parent_template", None)
    if template is None or not _is_exact_xrf_template(template, subtype):
        raise ExternalReferenceContractError(
            f"{subtype} XRF is not backed by the exact seeded core template"
        )
    if getattr(reference, "domain_code", None) != getattr(
        template, "domain_code", None
    ) or getattr(reference, "issuer_app_code", None) != getattr(
        template, "issuer_app_code", None
    ):
        raise ExternalReferenceContractError("XRF and template ownership do not match")
    _require_persisted_euid(getattr(reference, "euid", None), "XRF euid")
    return subtype  # type: ignore[return-value]


def _target_properties(target: ExternalTarget) -> dict[str, Any]:
    if isinstance(target, TapDBObjectTarget):
        return {
            "target_service_id": target.target_service_id,
            "target_object_euid": target.target_object_euid,
            "target_tenant_id": (
                str(target.target_tenant_id)
                if target.target_tenant_id is not None
                else None
            ),
            "target_object_kind": target.target_object_kind,
        }
    return {
        "namespace": target.namespace,
        "kind": target.kind,
        "value": target.value,
        "scope": target.scope,
        "canonical_uri": target.canonical_uri,
    }


def _identity_scope(
    target: ExternalTarget,
) -> tuple[IdentityScope, uuid.UUID | None]:
    if isinstance(target, TapDBObjectTarget) or target.scope == "public_global":
        return IdentityScope.GLOBAL, None
    return IdentityScope.TENANT, target.tenant_id


def _target_template_code(target: ExternalTarget) -> str:
    return (
        TAPDB_OBJECT_TEMPLATE_CODE
        if isinstance(target, TapDBObjectTarget)
        else OPAQUE_IDENTIFIER_TEMPLATE_CODE
    )


def _target_name(target: ExternalTarget) -> str:
    if isinstance(target, TapDBObjectTarget):
        return f"External TapDB reference: {target.target_service_id}"
    return f"External identifier: {target.namespace}/{target.kind}"


def _target_from_reference(reference: generic_instance) -> ExternalTarget:
    kind = _reference_kind(reference)
    properties = _json_properties(reference)
    target: ExternalTarget
    if kind == "tapdb_object":
        if set(properties) != {
            "target_service_id",
            "target_object_euid",
            "target_tenant_id",
            "target_object_kind",
        }:
            raise ExternalReferenceContractError(
                "TapDB-object XRF properties must contain only canonical fields"
            )
        target_tenant = properties.get("target_tenant_id")
        try:
            target_tenant_id = uuid.UUID(target_tenant) if target_tenant else None
        except (TypeError, ValueError) as exc:
            raise ExternalReferenceContractError(
                "target_tenant_id must be a canonical UUID"
            ) from exc
        service_id = _require_service_id(properties.get("target_service_id"))
        object_euid = _require_persisted_euid(
            properties.get("target_object_euid"), "target_object_euid"
        )
        object_kind_value = properties.get("target_object_kind")
        object_kind = (
            None
            if object_kind_value is None
            else _require_exact_text(
                object_kind_value, "target_object_kind", max_length=128
            )
        )
        target = TapDBObjectTarget(
            target_service_id=service_id,
            target_object_euid=object_euid,
            target_tenant_id=target_tenant_id,
            target_object_kind=object_kind,
        )
    else:
        if set(properties) != {
            "namespace",
            "kind",
            "value",
            "scope",
            "canonical_uri",
        }:
            raise ExternalReferenceContractError(
                "opaque XRF properties must contain only canonical fields"
            )
        scope_value = properties.get("scope")
        if scope_value not in {"tenant", "public_global"}:
            raise ExternalReferenceContractError(
                "opaque XRF scope must be tenant or public_global"
            )
        scope: Literal["tenant", "public_global"] = scope_value
        canonical_uri_value = properties.get("canonical_uri")
        canonical_uri = (
            None
            if canonical_uri_value is None
            else _require_canonical_uri(canonical_uri_value)
        )
        row_tenant = getattr(reference, "tenant_id", None)
        target = ExternalIdentifierTarget(
            namespace=_require_lower_token(properties.get("namespace"), "namespace"),
            kind=_require_lower_token(properties.get("kind"), "kind"),
            value=_require_identifier_value(properties.get("value")),
            scope=scope,
            tenant_id=row_tenant,
            canonical_uri=canonical_uri,
        )
    if getattr(reference, "identity_key", None) != target.identity_key:
        raise ExternalReferenceContractError(
            "XRF identity_key does not match its fields"
        )
    if isinstance(target, TapDBObjectTarget) and getattr(reference, "tenant_id", None):
        raise ExternalReferenceContractError("TapDB-object XRF must be global")
    if (
        isinstance(target, ExternalIdentifierTarget)
        and target.scope == "public_global"
        and getattr(reference, "tenant_id", None) is not None
    ):
        raise ExternalReferenceContractError("public_global opaque XRF has a tenant")
    return target


def _lineage_assertion(lineage: generic_instance_lineage) -> dict[str, Any]:
    properties = _json_properties(lineage)
    required = {
        "assertion_authority",
        "asserted_at",
        "assertion_provenance",
        "approved_global_link",
        "deactivated_at",
        "deactivation_provenance",
    }
    if set(properties) != required:
        raise ExternalReferenceContractError(
            "external-reference lineage properties must contain only canonical fields"
        )
    return {
        **properties,
        "assertion_authority": _require_exact_text(
            properties.get("assertion_authority"),
            "assertion_authority",
            max_length=256,
        ),
        "asserted_at": _parse_timestamp(properties.get("asserted_at"), "asserted_at"),
        "assertion_provenance": _require_exact_text(
            properties.get("assertion_provenance"),
            "assertion_provenance",
            max_length=2048,
        ),
    }


def _reject_metadata_pseudo_edges(obj: Any) -> None:
    if _is_xrf_coordinates(obj):
        return
    properties = _json_properties(obj)
    external_payload = properties.get("external_payload")
    violations = {
        key
        for key in properties
        if key in {"object_euid", "target_object_euid"} or key.endswith("_object_euid")
    }
    if isinstance(external_payload, dict) and external_payload.get("tapdb_graph"):
        violations.add("external_payload.tapdb_graph")
    if violations:
        raise ExternalReferenceContractError(
            "DAG v2 accepts relationships only from canonical XRF lineage; "
            "non-authoritative field(s): " + ", ".join(sorted(violations))
        )


def _projection(
    lineage: generic_instance_lineage, reference: generic_instance
) -> tuple[Literal["tapdb_object", "opaque"], dict[str, Any]]:
    target = _target_from_reference(reference)
    assertion = _lineage_assertion(lineage)
    relationship = _require_exact_text(
        getattr(lineage, "relationship_type", None),
        "relationship_type",
        max_length=128,
    )
    common = {
        "relationship_type": relationship,
        "assertion_authority": assertion["assertion_authority"],
        "asserted_at": assertion["asserted_at"],
        "assertion_provenance": assertion["assertion_provenance"],
        "external_reference_euid": _require_persisted_euid(
            reference.euid, "external_reference_euid"
        ),
        "lineage_euid": _require_persisted_euid(lineage.euid, "lineage_euid"),
    }
    if isinstance(target, TapDBObjectTarget):
        item = {
            **common,
            "target_service_id": target.target_service_id,
            "target_object_euid": target.target_object_euid,
        }
        if target.target_tenant_id is not None:
            item["target_tenant_id"] = str(target.target_tenant_id)
        if target.target_object_kind is not None:
            item["target_object_kind"] = target.target_object_kind
        return "tapdb_object", item
    item = {
        **common,
        "namespace": target.namespace,
        "kind": target.kind,
        "value": target.value,
        "scope": target.scope,
    }
    if target.canonical_uri is not None:
        item["canonical_uri"] = target.canonical_uri
    return "opaque", item


def _project_outbound_external_references(obj: Any) -> dict[str, list[dict[str, Any]]]:
    """Internal DAG/UI projection from authoritative active lineage only."""

    _reject_metadata_pseudo_edges(obj)
    refs: list[dict[str, Any]] = []
    identifiers: list[dict[str, Any]] = []
    relation = getattr(obj, "parent_of_lineages", None)
    if relation is None:
        return {"external_refs": refs, "external_identifiers": identifiers}
    query = (
        relation.filter_by(is_deleted=False)
        if hasattr(relation, "filter_by")
        else relation
    )
    lineages = list(query.all() if hasattr(query, "all") else query)
    for lineage in lineages:
        reference = getattr(lineage, "child_instance", None)
        if reference is None or not _is_xrf_coordinates(reference):
            continue
        kind, item = _projection(lineage, reference)
        (refs if kind == "tapdb_object" else identifiers).append(item)

    def sort_key(item: dict[str, Any]) -> tuple[str, str, str, str, str]:
        return (
            str(item.get("target_service_id") or item.get("namespace") or ""),
            str(item.get("target_object_euid") or item.get("kind") or ""),
            str(item.get("value") or ""),
            str(item["relationship_type"]),
            str(item["lineage_euid"]),
        )

    refs.sort(key=sort_key)
    identifiers.sort(key=sort_key)
    return {"external_refs": refs, "external_identifiers": identifiers}


def _encode_cursor(uid: int) -> str:
    return base64.urlsafe_b64encode(str(uid).encode()).decode().rstrip("=")


def _decode_cursor(cursor: str | None) -> int:
    if cursor is None or cursor == "":
        return 0
    if not isinstance(cursor, str) or cursor != cursor.strip():
        raise ValueError("cursor is malformed")
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        uid = int(base64.urlsafe_b64decode(padded).decode())
    except Exception as exc:
        raise ValueError("cursor is malformed") from exc
    if uid < 1:
        raise ValueError("cursor is malformed")
    return uid


def _validate_page(limit: int, cursor: str | None) -> tuple[int, int]:
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
        raise ValueError("limit must be an integer from 1 through 500")
    return limit, _decode_cursor(cursor)


class ExternalReferenceService:
    """Sole supported writer and query API for core XRF objects and lineages."""

    def __init__(self, session: Any) -> None:
        self.session = session

    @staticmethod
    def _require_source(source: generic_instance) -> None:
        if not isinstance(source, generic_instance):
            raise ValueError("source must be a TapDB instance")
        if source.uid is None or not source.euid or source.is_deleted:
            raise ValueError("source must be a persisted active TapDB instance")
        _require_persisted_euid(source.euid, "source.euid")

    def _lock_source(self, source: generic_instance) -> generic_instance:
        self._require_source(source)
        locked = (
            self.session.query(generic_instance)
            .filter_by(uid=source.uid, is_deleted=False)
            .with_for_update()
            .one_or_none()
        )
        if locked is None or locked.euid != source.euid:
            raise LookupError("source is not visible in the caller's transaction")
        return locked

    def _claim_reference(
        self, source: generic_instance, target: ExternalTarget
    ) -> generic_instance:
        if isinstance(target, ExternalIdentifierTarget) and target.scope == "tenant":
            if source.tenant_id != target.tenant_id:
                raise ValueError(
                    "tenant-scoped identifier tenant_id must equal source.tenant_id"
                )
        scope, tenant_id = _identity_scope(target)
        factory = InstanceFactory(TemplateManager(), domain_code=source.domain_code)
        claim = factory.claim_instance_by_identity(
            self.session,
            template_code=_target_template_code(target),
            identity_key=target.identity_key,
            name=_target_name(target),
            scope=scope,
            tenant_id=tenant_id,
            properties=_target_properties(target),
            command_evidence={"contract": "tapdb.external-references/v2"},
            create_children=False,
            _external_reference_token=_external_reference_writer_token(),
        )
        reference = claim.instance
        if reference.is_deleted:
            raise ExternalReferenceContractError(
                "canonical XRF identity is soft-deleted and requires explicit repair"
            )
        expected = _target_properties(target)
        existing = _json_properties(reference)
        required = (
            ("target_service_id", "target_object_euid")
            if isinstance(target, TapDBObjectTarget)
            else ("namespace", "kind", "value", "scope")
        )
        optional = (
            ("target_tenant_id", "target_object_kind")
            if isinstance(target, TapDBObjectTarget)
            else ("canonical_uri",)
        )
        if set(existing) != set(expected):
            raise ExternalReferenceContractError(
                "existing XRF properties do not contain exactly the canonical fields"
            )
        if any(existing[key] != expected[key] for key in required):
            raise ExternalReferenceContractError(
                "existing XRF identity has divergent immutable target fields"
            )
        enriched = dict(existing)
        for key in optional:
            old = existing.get(key)
            new = expected.get(key)
            if old is not None and new is not None and old != new:
                raise ExternalReferenceContractError(
                    f"existing XRF has conflicting non-null {key}"
                )
            if old is None and new is not None:
                enriched[key] = new
        if enriched != existing:
            payload = dict(reference.json_addl)
            payload["properties"] = enriched
            reference.json_addl = payload
            self.session.flush()
        persisted = _target_from_reference(reference)
        if persisted.identity_key != target.identity_key:
            raise ExternalReferenceContractError(
                "natural-identity winner does not match the requested target"
            )
        if claim.outcome not in {
            IdentityClaimOutcome.CREATED,
            IdentityClaimOutcome.EXISTING,
        }:
            raise RuntimeError("unsupported natural-identity outcome")
        return reference

    def _lineage_candidates(
        self,
        source: generic_instance,
        reference: generic_instance,
        relationship_type: str,
    ) -> list[generic_instance_lineage]:
        return cast(
            list[generic_instance_lineage],
            self.session.query(generic_instance_lineage)
            .filter_by(
                parent_instance_uid=source.uid,
                child_instance_uid=reference.uid,
                relationship_type=relationship_type,
            )
            .order_by(generic_instance_lineage.uid.asc())
            .with_for_update()
            .all(),
        )

    def _attach_locked(
        self, source: generic_instance, spec: ExternalLinkSpec
    ) -> ExternalLinkOutcome:
        reference = self._claim_reference(source, spec.target)
        candidates = self._lineage_candidates(source, reference, spec.relationship_type)
        active = [item for item in candidates if not item.is_deleted]
        deleted = [item for item in candidates if item.is_deleted]
        if len(active) > 1:
            raise ExternalReferenceContractError(
                "multiple active external-reference lineages require explicit repair"
            )
        for item in candidates:
            authority = _lineage_assertion(item)["assertion_authority"]
            if authority != spec.assertion_authority:
                raise ExternalReferenceContractError(
                    "source/target/relationship is owned by a different assertion authority"
                )
        expected_assertion = {
            "assertion_authority": spec.assertion_authority,
            "asserted_at": spec.asserted_at.isoformat(),
            "assertion_provenance": spec.assertion_provenance,
        }
        if active:
            assertion = _lineage_assertion(active[0])
            if any(
                assertion[key] != value for key, value in expected_assertion.items()
            ):
                raise ExternalReferenceContractError(
                    "active replay has divergent immutable assertion data"
                )
            return ExternalLinkOutcome("existing", reference, active[0])
        assertion_properties = {
            **expected_assertion,
            "approved_global_link": reference.tenant_id is None,
            "deactivated_at": None,
            "deactivation_provenance": None,
        }
        if deleted:
            if len(deleted) != 1:
                raise ExternalReferenceContractError(
                    "multiple historical deleted links require explicit repair"
                )
            lineage = deleted[0]
            lineage.is_deleted = False
            lineage.bstatus = "active"
            lineage.json_addl = {"properties": assertion_properties}
            self.session.flush()
            return ExternalLinkOutcome("reactivated", reference, lineage)

        lineage = generic_instance_lineage(
            name=f"External reference asserted by {spec.assertion_authority}",
            tenant_id=source.tenant_id,
            polymorphic_discriminator="generic_instance_lineage",
            category="lineage",
            type="lineage",
            subtype="external_reference",
            version="1.0",
            bstatus="active",
            is_singleton=False,
            parent_instance_uid=source.uid,
            child_instance_uid=reference.uid,
            relationship_type=spec.relationship_type,
            parent_type=source.polymorphic_discriminator,
            child_type=reference.polymorphic_discriminator,
            json_addl={"properties": assertion_properties},
        )
        self.session.add(lineage)
        self.session.flush()
        _require_persisted_euid(lineage.euid, "lineage.euid")
        return ExternalLinkOutcome("created", reference, lineage)

    def attach(
        self, source: generic_instance, spec: ExternalLinkSpec
    ) -> ExternalLinkOutcome:
        """Create, replay, or reactivate exactly one canonical link."""

        if not isinstance(spec, ExternalLinkSpec):
            raise ValueError("spec must be an ExternalLinkSpec")
        locked = self._lock_source(source)
        return self._attach_locked(locked, spec)

    def _find_reference(self, target: ExternalTarget) -> generic_instance | None:
        scope, tenant_id = _identity_scope(target)
        query = self.session.query(generic_instance).filter_by(
            identity_key=target.identity_key,
            category="reference",
            type="external_identifier",
            subtype=(
                "tapdb_object" if isinstance(target, TapDBObjectTarget) else "opaque"
            ),
            version="1.0",
        )
        query = (
            query.filter(generic_instance.tenant_id.is_(None))
            if scope is IdentityScope.GLOBAL
            else query.filter(generic_instance.tenant_id == tenant_id)
        )
        reference = query.one_or_none()
        if reference is not None:
            persisted = _target_from_reference(reference)
            if persisted.identity_key != target.identity_key:
                raise ExternalReferenceContractError(
                    "stored XRF target does not match its natural identity"
                )
        return reference

    def detach(
        self,
        source: generic_instance,
        target: ExternalTarget,
        *,
        relationship_type: str,
        assertion_authority: str,
        deactivated_at: datetime,
        deactivation_provenance: str,
    ) -> ExternalLinkOutcome:
        """Soft-delete only the exact authority-owned lineage."""

        _require_exact_text(relationship_type, "relationship_type", max_length=128)
        _require_exact_text(assertion_authority, "assertion_authority", max_length=256)
        _require_timezone_aware(deactivated_at, "deactivated_at")
        _require_exact_text(
            deactivation_provenance,
            "deactivation_provenance",
            max_length=2048,
        )
        locked = self._lock_source(source)
        reference = self._find_reference(target)
        if reference is None:
            return ExternalLinkOutcome("already_inactive", None, None)
        candidates = self._lineage_candidates(locked, reference, relationship_type)
        if len([item for item in candidates if item.is_deleted]) > 1:
            raise ExternalReferenceContractError(
                "multiple historical deleted links require explicit repair"
            )
        for item in candidates:
            if _lineage_assertion(item)["assertion_authority"] != assertion_authority:
                raise ExternalReferenceContractError(
                    "source/target/relationship is owned by a different assertion authority"
                )
        active = [item for item in candidates if not item.is_deleted]
        if not active:
            historical = candidates[0] if candidates else None
            return ExternalLinkOutcome("already_inactive", reference, historical)
        if len(active) != 1:
            raise ExternalReferenceContractError(
                "multiple active external-reference lineages require explicit repair"
            )
        lineage = active[0]
        assertion = _lineage_assertion(lineage)
        assertion["deactivated_at"] = deactivated_at.isoformat()
        assertion["deactivation_provenance"] = deactivation_provenance
        lineage.json_addl = {"properties": assertion}
        lineage.bstatus = "inactive"
        lineage.is_deleted = True
        self.session.flush()
        return ExternalLinkOutcome("deactivated", reference, lineage)

    def reconcile(
        self,
        source: generic_instance,
        assertion_authority: str,
        desired: Sequence[ExternalLinkSpec],
        *,
        deactivated_at: datetime,
        deactivation_provenance: str,
    ) -> tuple[ExternalLinkOutcome, ...]:
        """Apply one authority's exact desired set, touching no other authority."""

        _require_exact_text(assertion_authority, "assertion_authority", max_length=256)
        if not isinstance(desired, Sequence) or isinstance(desired, (str, bytes)):
            raise ValueError("desired must be a sequence of ExternalLinkSpec values")
        if len(desired) > 500:
            raise ValueError("desired may contain at most 500 links")
        locked = self._lock_source(source)
        desired_keys: set[tuple[str, str]] = set()
        outcomes: list[ExternalLinkOutcome] = []
        for spec in desired:
            if not isinstance(spec, ExternalLinkSpec):
                raise ValueError("desired must contain only ExternalLinkSpec values")
            if spec.assertion_authority != assertion_authority:
                raise ValueError(
                    "every desired link must match reconcile assertion_authority"
                )
            key = (spec.target.identity_key, spec.relationship_type)
            if key in desired_keys:
                raise ValueError("desired contains a duplicate target/relationship")
            desired_keys.add(key)
            outcomes.append(self._attach_locked(locked, spec))

        active_lineages = (
            self.session.query(generic_instance_lineage)
            .filter_by(parent_instance_uid=locked.uid, is_deleted=False)
            .order_by(generic_instance_lineage.uid.asc())
            .with_for_update()
            .all()
        )
        for lineage in active_lineages:
            reference = getattr(lineage, "child_instance", None)
            if reference is None or not _is_xrf_coordinates(reference):
                continue
            assertion = _lineage_assertion(lineage)
            if assertion["assertion_authority"] != assertion_authority:
                continue
            target = _target_from_reference(reference)
            key = (target.identity_key, str(lineage.relationship_type))
            if key in desired_keys:
                continue
            outcomes.append(
                self.detach(
                    locked,
                    target,
                    relationship_type=str(lineage.relationship_type),
                    assertion_authority=assertion_authority,
                    deactivated_at=deactivated_at,
                    deactivation_provenance=deactivation_provenance,
                )
            )
        return tuple(outcomes)

    def list_for_source(
        self,
        source: generic_instance,
        *,
        relationship_type: str | None = None,
        assertion_authority: str | None = None,
        include_inactive: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Return bounded canonical links for a local source."""

        self._require_source(source)
        page_limit, after_uid = _validate_page(limit, cursor)
        authority = (
            _require_exact_text(
                assertion_authority, "assertion_authority", max_length=256
            )
            if assertion_authority is not None
            else None
        )
        reference_model = aliased(generic_instance)
        query = self.session.query(generic_instance_lineage).join(
            reference_model,
            generic_instance_lineage.child_instance_uid == reference_model.uid,
        )
        query = query.filter(
            generic_instance_lineage.parent_instance_uid == source.uid,
            generic_instance_lineage.uid > after_uid,
            reference_model.category == "reference",
            reference_model.type == "external_identifier",
            reference_model.subtype.in_(("tapdb_object", "opaque")),
            reference_model.version == "1.0",
            reference_model.is_deleted.is_(False),
        )
        if not include_inactive:
            query = query.filter(generic_instance_lineage.is_deleted.is_(False))
        if relationship_type is not None:
            query = query.filter(
                generic_instance_lineage.relationship_type
                == _require_exact_text(
                    relationship_type, "relationship_type", max_length=128
                )
            )
        if authority is not None:
            query = query.filter(
                generic_instance_lineage.json_addl["properties"][
                    "assertion_authority"
                ].as_string()
                == authority
            )
        rows = (
            query.order_by(generic_instance_lineage.uid.asc())
            .limit(page_limit + 1)
            .all()
        )
        items: list[dict[str, Any]] = []
        page_rows = rows[:page_limit]
        for lineage in page_rows:
            reference = getattr(lineage, "child_instance", None)
            if reference is None:
                raise ExternalReferenceContractError(
                    "visible external-reference lineage has no visible XRF"
                )
            assertion = _lineage_assertion(lineage)
            kind, item = _projection(lineage, reference)
            item["target_type"] = kind
            item["active"] = not lineage.is_deleted
            if lineage.is_deleted:
                item["deactivated_at"] = assertion["deactivated_at"]
                item["deactivation_provenance"] = assertion["deactivation_provenance"]
            items.append(item)
        has_more = len(rows) > page_limit
        last_uid = int(page_rows[-1].uid) if page_rows else after_uid
        return {
            "items": items,
            "page": {
                "limit": page_limit,
                "returned": len(items),
                "next_cursor": (
                    _encode_cursor(last_uid) if has_more and last_uid else None
                ),
            },
        }

    def find_sources(
        self,
        target: ExternalTarget,
        *,
        relationship_type: str | None = None,
        assertion_authority: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Return exact active reverse matches while preserving database RLS."""

        page_limit, after_uid = _validate_page(limit, cursor)
        authority = (
            _require_exact_text(
                assertion_authority, "assertion_authority", max_length=256
            )
            if assertion_authority is not None
            else None
        )
        reference = self._find_reference(target)
        if reference is None:
            return {
                "items": [],
                "page": {"limit": page_limit, "returned": 0, "next_cursor": None},
            }
        source_model = aliased(generic_instance)
        query = self.session.query(generic_instance_lineage).join(
            source_model,
            generic_instance_lineage.parent_instance_uid == source_model.uid,
        )
        query = query.filter(
            generic_instance_lineage.child_instance_uid == reference.uid,
            generic_instance_lineage.is_deleted.is_(False),
            generic_instance_lineage.uid > after_uid,
            source_model.is_deleted.is_(False),
        )
        if relationship_type is not None:
            query = query.filter(
                generic_instance_lineage.relationship_type
                == _require_exact_text(
                    relationship_type, "relationship_type", max_length=128
                )
            )
        if authority is not None:
            query = query.filter(
                generic_instance_lineage.json_addl["properties"][
                    "assertion_authority"
                ].as_string()
                == authority
            )
        rows = (
            query.order_by(generic_instance_lineage.uid.asc())
            .limit(page_limit + 1)
            .all()
        )
        items: list[dict[str, Any]] = []
        page_rows = rows[:page_limit]
        for lineage in page_rows:
            _lineage_assertion(lineage)
            source = getattr(lineage, "parent_instance", None)
            if source is None:
                raise ExternalReferenceContractError(
                    "visible external-reference lineage has no visible source"
                )
            _, matched = _projection(lineage, reference)
            items.append(
                {
                    "source": {
                        "euid": _require_persisted_euid(source.euid, "source.euid"),
                        "name": source.name,
                        "category": source.category,
                        "type": source.type,
                        "subtype": source.subtype,
                        "version": source.version,
                        "tenant_id": (
                            str(source.tenant_id)
                            if source.tenant_id is not None
                            else None
                        ),
                    },
                    "matched_external_reference": matched,
                }
            )
        has_more = len(rows) > page_limit
        last_uid = int(page_rows[-1].uid) if page_rows else after_uid
        return {
            "items": items,
            "page": {
                "limit": page_limit,
                "returned": len(items),
                "next_cursor": (
                    _encode_cursor(last_uid) if has_more and last_uid else None
                ),
            },
        }


__all__ = [
    "TapDBObjectTarget",
    "ExternalIdentifierTarget",
    "ExternalLinkSpec",
    "ExternalLinkOutcome",
    "ExternalReferenceService",
]
