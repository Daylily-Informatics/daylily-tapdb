"""Tenant-shared queries and independent DAG views on native TapDB objects.

This module owns definitions and access decisions, not a query language or a
second history store. Executors are explicitly registered Python services.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
import json
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator
from sqlalchemy import select

from daylily_tapdb.euid import validate_euid
from daylily_tapdb.factory import InstanceFactory
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.revisions import apply_guarded_fields, lock_records, require_revision
from daylily_tapdb.templates import TemplateManager

QUERY_TEMPLATE = "query/saved/generic/1.0"
VIEW_TEMPLATE = "view/dag/generic/1.0"
VIEW_DEFINITION_CONTRACT = "tapdb.dag-view-definition/v2"
TEMPLATES = {"query": QUERY_TEMPLATE, "view": VIEW_TEMPLATE}
QUERY_VARIANT = "saved_query_variant_of"
VIEW_VARIANT = "saved_view_variant_of"
VIEW_QUERY = "saved_view_of_query"
RELATIONSHIPS = frozenset({QUERY_VARIANT, VIEW_VARIANT, VIEW_QUERY})
_FORBIDDEN_FIELDS = frozenset({
    "sql", "statement", "query_text", "credentials", "credential", "password",
    "secret", "access_token", "refresh_token", "api_key", "authorization",
    "cookie", "headers", "connection_string", "dsn",
})


class DefinitionModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class QueryDefinition(DefinitionModel):
    contract: Literal["tapdb.saved-query-definition/v1"]
    executor: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9._/-]*$")
    specification: dict[str, JsonValue]


class ViewPoint(DefinitionModel):
    x: float = Field(allow_inf_nan=False)
    y: float = Field(allow_inf_nan=False)


class ViewElement(DefinitionModel):
    """A presentation selector copied from a native projection, not a relation."""

    service_id: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_.-]*$")
    euid: str = Field(min_length=1)

    @field_validator("euid")
    @classmethod
    def canonical_euid(cls, value: str) -> str:
        if not validate_euid(value):
            raise ValueError("presentation selectors require a canonical native EUID")
        return value


class ViewGroupOffset(DefinitionModel):
    """Model-coordinate delta for a group whose members come from the projection."""

    group: ViewElement
    delta: ViewPoint


class ViewNodeOffset(DefinitionModel):
    """Independent model-coordinate adjustment of one persisted projected node."""

    node: ViewElement
    delta: ViewPoint


class ViewLane(DefinitionModel):
    """Ordered display lane declared by the owning projection/layout."""

    lane_id: str = Field(min_length=1, pattern=r"^[A-Za-z][A-Za-z0-9_.:/-]*$")
    visible: bool
    delta: ViewPoint


class ViewVisibility(DefinitionModel):
    historical_placements: bool
    empty_positions: bool
    hidden_nodes: list[ViewElement]
    hidden_edges: list[ViewElement]
    hidden_relationship_types: list[str]

    @field_validator("hidden_relationship_types")
    @classmethod
    def exact_relationship_types(cls, values: list[str]) -> list[str]:
        for value in values:
            _exact(value, "relationship type")
        return values


class FitViewViewport(DefinitionModel):
    mode: Literal["fit"]


class FixedViewViewport(DefinitionModel):
    mode: Literal["fixed"]
    pan: ViewPoint
    zoom: float = Field(gt=0, allow_inf_nan=False)


class ViewReplay(DefinitionModel):
    rate: Literal[0.2, 0.5, 1.0, 2.0, 5.0]
    fit_on_step: bool


class ViewDefinition(DefinitionModel):
    """Complete v2 presentation; never supplies geometry or containment facts."""

    contract: Literal["tapdb.dag-view-definition/v2"]
    layout: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_.-]*$")
    arrow_direction: Literal["child_to_parent"]
    content_mode: Literal["embedded", "parallel"]
    time_order: Literal["older_left"]
    group_offsets: list[ViewGroupOffset]
    node_offsets: list[ViewNodeOffset]
    lanes: list[ViewLane]
    visibility: ViewVisibility
    viewport: Annotated[FitViewViewport | FixedViewViewport, Field(discriminator="mode")]
    replay: ViewReplay
    labels: list[Literal["euid", "name", "type", "position"]]

    @model_validator(mode="after")
    def unambiguous_presentation(self):
        groups = [(item.group.service_id, item.group.euid) for item in self.group_offsets]
        if len(groups) != len(set(groups)):
            raise ValueError("group_offsets must contain each owning-service/EUID pair once")
        nodes = [(item.node.service_id, item.node.euid) for item in self.node_offsets]
        if len(nodes) != len(set(nodes)):
            raise ValueError("node_offsets must contain each owning-service/EUID pair once")
        lane_ids = [lane.lane_id for lane in self.lanes]
        if len(lane_ids) != len(set(lane_ids)):
            raise ValueError("lanes must contain each declared lane_id once")
        return self


class ObjectSearchDefinition(DefinitionModel):
    """The native tenant-object search contract; there is no SQL input."""

    record_type: Literal["instance", "lineage"] = "instance"
    q: str = ""
    euid: str = ""
    name_like: str = ""
    euid_like: str = ""
    category: str = ""
    type_name: str = ""
    subtype: str = ""
    relationship_type: str = ""


class ObjectSearchParameters(DefinitionModel):
    limit: int = Field(default=25, ge=1, le=100)
    cursor: str = ""


@dataclass(frozen=True)
class SavedQueryPrincipal:
    """Trusted identity supplied by the host authentication boundary."""

    tenant_id: UUID
    actor_issuer: str
    actor_subject: str
    is_admin: bool = False

    def __post_init__(self):
        if not isinstance(self.tenant_id, UUID):
            raise ValueError("saved definitions require an explicit tenant UUID")
        for name in ("actor_issuer", "actor_subject"):
            _exact(getattr(self, name), name)
        if not isinstance(self.is_admin, bool):
            raise ValueError("is_admin must be an authenticated boolean")

    @property
    def creator(self) -> dict[str, str]:
        return {"issuer": self.actor_issuer, "subject": self.actor_subject}


@dataclass(frozen=True)
class QueryExecutor:
    """A host-owned, read-only implementation with explicit structured schemas.

    The callable receives (session, specification, parameters, principal,
    owner_repo_name). It must not commit, write, discover credentials, or fetch
    arbitrary destinations. The web surface supplies a native read-only
    transaction. Credentials belong to the host runtime, never the definition.
    """

    definition_model: type[BaseModel]
    parameters_model: type[BaseModel]
    execute: Callable[[Any, dict, dict, SavedQueryPrincipal, str], Any]


def _native_object_search(session, specification, parameters, principal, owner):
    from daylily_tapdb.services.object_search import search_objects

    return search_objects(
        session, service_name=owner, tenant_id=str(principal.tenant_id),
        **specification, **parameters,
    )


def query_executors(
    extensions: Mapping[str, QueryExecutor] | None = None,
) -> dict[str, QueryExecutor]:
    registered = {
        "tapdb.object-search/v1": QueryExecutor(
            ObjectSearchDefinition, ObjectSearchParameters, _native_object_search
        )
    }
    for key, executor in (extensions or {}).items():
        _exact(key, "executor identifier")
        if key in registered:
            raise ValueError(f"query executor is already registered: {key}")
        if not isinstance(executor, QueryExecutor):
            raise TypeError("executors must be QueryExecutor registrations")
        for model in (executor.definition_model, executor.parameters_model):
            if not isinstance(model, type) or not issubclass(model, BaseModel):
                raise TypeError("executor definitions and parameters need Pydantic models")
            if model.model_config.get("extra") != "forbid":
                raise ValueError("executor schemas must forbid unknown fields")
        if not callable(executor.execute):
            raise TypeError("query executor must be callable")
        registered[key] = executor
    return registered


def _exact(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field} must be an exact nonempty string")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{field} must not contain control characters")
    return value


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


def _definition_fields(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower().replace("-", "_") in _FORBIDDEN_FIELDS:
                raise ValueError(f"saved definitions cannot contain {key!r}")
            _definition_fields(child)
    elif isinstance(value, list):
        for child in value:
            _definition_fields(child)


def is_saved_definition(obj: Any) -> bool:
    """Identify protected objects/templates by taxonomy, never EUID prefix."""

    def value(name):
        return obj.get(name) if isinstance(obj, Mapping) else getattr(obj, name, None)

    return tuple(value(name) for name in ("category", "type", "subtype", "version")) in {
        tuple(QUERY_TEMPLATE.split("/")), tuple(VIEW_TEMPLATE.split("/"))
    }


class SavedQueryService:
    """Caller-owned transaction with native factory, lineage, CAS and history."""

    def __init__(
        self, session: Any, *, domain_code: str, owner_repo_name: str,
        principal: SavedQueryPrincipal,
        executors: Mapping[str, QueryExecutor] | None = None,
    ):
        self.session = session
        self.domain_code = _exact(domain_code, "domain_code")
        self.owner = _exact(owner_repo_name, "owner_repo_name")
        self.principal = principal
        if not isinstance(principal, SavedQueryPrincipal):
            raise TypeError("an authenticated SavedQueryPrincipal is required")
        self.executors = query_executors(executors)
        self.factory = InstanceFactory(TemplateManager(), domain_code=domain_code)

    def _selection(self, kind: str):
        if kind not in TEMPLATES:
            raise ValueError("saved definition kind must be query or view")
        category, type_name, subtype, version = TEMPLATES[kind].split("/")
        return select(generic_instance).where(
            generic_instance.domain_code == self.domain_code,
            generic_instance.issuer_app_code == self.owner,
            generic_instance.tenant_id == self.principal.tenant_id,
            generic_instance.category == category,
            generic_instance.type == type_name,
            generic_instance.subtype == subtype,
            generic_instance.version == version,
        )

    def _get(self, kind: str, euid: str, *, include_deleted: bool = False):
        stmt = self._selection(kind).where(
            generic_instance.euid == _exact(euid, "euid")
        )
        if not include_deleted:
            stmt = stmt.where(generic_instance.is_deleted.is_(False))
        obj = self.session.execute(stmt).scalar_one_or_none()
        if obj is None:
            raise LookupError(f"saved {kind} is not available in this tenant")
        return obj

    @staticmethod
    def _properties(obj) -> dict:
        props = (obj.json_addl or {}).get("properties")
        if not isinstance(props, dict):
            raise ValueError("saved definition properties are unavailable")
        return props

    def _can_edit(self, obj) -> bool:
        return self.principal.is_admin or (
            self._properties(obj).get("creator") == self.principal.creator
        )

    def _require_edit(self, obj) -> None:
        if not self._can_edit(obj):
            raise PermissionError("only the creator or a tenant administrator may edit")

    def _definition(self, kind: str, definition: dict) -> dict:
        _definition_fields(definition)
        if kind == "view":
            if not isinstance(definition, dict) or definition.get("contract") != VIEW_DEFINITION_CONTRACT:
                raise ValueError(
                    f"DAG view authoring and execution require {VIEW_DEFINITION_CONTRACT}; "
                    "supply a complete definition through a guarded update or variant"
                )
            return ViewDefinition.model_validate(definition).model_dump(mode="json")
        parsed = QueryDefinition.model_validate(definition)
        executor = self.executors.get(parsed.executor)
        if executor is None:
            raise ValueError(f"query executor is not registered: {parsed.executor}")
        specification = executor.definition_model.model_validate(
            parsed.specification
        ).model_dump(mode="json")
        _definition_fields(specification)
        return {
            "contract": parsed.contract, "executor": parsed.executor,
            "specification": _json(specification),
        }

    def _links(self, obj, relationship: str | None = None):
        stmt = select(generic_instance_lineage).where(
            generic_instance_lineage.child_instance_uid == obj.uid,
            generic_instance_lineage.domain_code == self.domain_code,
            generic_instance_lineage.issuer_app_code == self.owner,
            generic_instance_lineage.tenant_id == self.principal.tenant_id,
            generic_instance_lineage.is_deleted.is_(False),
            generic_instance_lineage.relationship_type.in_(RELATIONSHIPS),
        )
        if relationship is not None:
            stmt = stmt.where(generic_instance_lineage.relationship_type == relationship)
        return list(self.session.execute(stmt).scalars())

    def _payload(self, kind: str, obj) -> dict:
        return {
            "contract": "tapdb.saved-definition/v1", "kind": kind,
            "euid": obj.euid, "record_revision": obj.record_revision,
            "tenant_id": str(obj.tenant_id), "name": obj.name,
            "template_code": TEMPLATES[kind],
            "properties": deepcopy(self._properties(obj)),
            "created_at": obj.created_dt.isoformat(),
            "modified_at": obj.modified_dt.isoformat() if obj.modified_dt else None,
            "is_deleted": obj.is_deleted, "can_edit": self._can_edit(obj),
            "relationships": [
                {
                    "euid": edge.euid, "record_revision": edge.record_revision,
                    "relationship_type": edge.relationship_type,
                    "parent_euid": edge.parent_instance.euid,
                    "child_euid": edge.child_instance.euid,
                }
                for edge in self._links(obj)
            ],
        }

    def list(self, kind: str, *, limit: int = 25, after_uid: int = 0) -> dict:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer from 1 through 100")
        if type(after_uid) is not int or after_uid < 0:
            raise ValueError("after_uid must be a nonnegative integer")
        rows = list(self.session.execute(
            self._selection(kind).where(
                generic_instance.is_deleted.is_(False),
                generic_instance.uid > after_uid,
            ).order_by(generic_instance.uid).limit(limit + 1)
        ).scalars())
        return {
            "items": [self._payload(kind, row) for row in rows[:limit]],
            "page": {
                "limit": limit,
                "next_after_uid": rows[limit - 1].uid if len(rows) > limit else None,
            },
        }

    def get(self, kind: str, euid: str) -> dict:
        return self._payload(kind, self._get(kind, euid))

    def _create(self, kind, name, description, definition):
        if kind not in TEMPLATES:
            raise ValueError("saved definition kind must be query or view")
        if not isinstance(description, str):
            raise ValueError("description must be a string")
        obj = self.factory.create_instance(
            self.session, TEMPLATES[kind], _exact(name, "name"),
            properties={
                "contract": "tapdb.saved-definition/v1",
                "creator": self.principal.creator,
                "description": description,
                "definition": self._definition(kind, definition),
            },
            create_children=False, tenant_id=self.principal.tenant_id,
        )
        self.session.refresh(obj)
        return obj

    def create(self, kind, *, name, description, definition) -> dict:
        return self._payload(kind, self._create(kind, name, description, definition))

    def update(
        self, kind, euid, *, expected_revision, name, description, definition,
    ) -> dict:
        obj = self._get(kind, euid)
        lock_records(self.session, [obj])
        require_revision(obj, expected_revision)
        self._require_edit(obj)
        if obj.is_deleted:
            raise LookupError("saved definition was deleted")
        if not isinstance(description, str):
            raise ValueError("description must be a string")
        payload = deepcopy(obj.json_addl)
        payload["properties"].update(
            description=description, definition=self._definition(kind, definition)
        )
        apply_guarded_fields(
            self.session, obj, {"name": _exact(name, "name"), "json_addl": payload},
            expected_revision=expected_revision,
        )
        return self._payload(kind, obj)

    def delete(self, kind, euid, *, expected_revision) -> dict:
        obj = self._get(kind, euid)
        lock_records(self.session, [obj])
        self._require_edit(obj)
        apply_guarded_fields(
            self.session, obj, {"is_deleted": True},
            expected_revision=expected_revision,
        )
        return self._payload(kind, obj)

    def _edge(self, parent, child, relationship, *, source_revision=None):
        edge = generic_instance_lineage(
            name=f"{child.name}: {relationship}", tenant_id=self.principal.tenant_id,
            polymorphic_discriminator="generic_instance_lineage",
            category="generic", type="lineage", subtype="instance_lineage", version="1.0.0",
            bstatus="active", parent_instance_uid=parent.uid, child_instance_uid=child.uid,
            parent_type=parent.polymorphic_discriminator,
            child_type=child.polymorphic_discriminator, relationship_type=relationship,
            json_addl={"properties": {
                "contract": "tapdb.saved-definition-link/v1",
                **({"source_revision": source_revision} if source_revision is not None else {}),
            }},
        )
        self.session.add(edge)
        self.session.flush()
        return edge

    def variant(
        self, kind, euid, *, expected_revision, name, description, definition=None,
    ) -> dict:
        source = self._get(kind, euid)
        lock_records(self.session, [source])
        require_revision(source, expected_revision)
        if source.is_deleted:
            raise LookupError("variant source was deleted")
        copy_definition = (
            deepcopy(self._properties(source)["definition"])
            if definition is None else definition
        )
        variant = self._create(kind, name, description, copy_definition)
        self._edge(
            source, variant, QUERY_VARIANT if kind == "query" else VIEW_VARIANT,
            source_revision=expected_revision,
        )
        if kind == "view":
            binding = self._binding(source)
            if binding is not None:
                query = self._get("query", binding.parent_instance.euid)
                self._edge(query, variant, VIEW_QUERY)
        self.session.refresh(variant)
        return self._payload(kind, variant)

    def _binding(self, view):
        links = self._links(view, VIEW_QUERY)
        if len(links) > 1:
            raise ValueError("DAG view has ambiguous query relationships")
        return links[0] if links else None

    def bind_query(
        self, view_euid, *, expected_revision, query_euid=None,
        expected_query_revision=None,
    ) -> dict:
        view = self._get("view", view_euid)
        query = self._get("query", query_euid) if query_euid is not None else None
        if query is None and expected_query_revision is not None:
            raise ValueError("query revision requires a query EUID")
        # Serialize bindings through the view; new query edges are discovered
        # only after that lock, so concurrent rebinding cannot create two actives.
        lock_records(self.session, [view])
        require_revision(view, expected_revision)
        self._require_edit(view)
        if view.is_deleted:
            raise LookupError("DAG view was deleted")
        binding = self._binding(view)
        records = ([query] if query is not None else []) + ([binding] if binding else [])
        lock_records(self.session, records)
        if query is not None:
            require_revision(query, expected_query_revision)
            if query.is_deleted:
                raise LookupError("saved query was deleted")
        if (binding is None and query is None) or (
            binding is not None and query is not None
            and binding.parent_instance_uid == query.uid
        ):
            return self._payload("view", view)
        if binding is not None:
            apply_guarded_fields(
                self.session, binding, {"is_deleted": True},
                expected_revision=binding.record_revision,
            )
            recorded_at = binding.modified_dt
        if query is not None:
            new_binding = self._edge(query, view, VIEW_QUERY)
            recorded_at = new_binding.created_dt
        # A binding edit is an edit to this independently revisioned view.
        # The native revision trigger deliberately suppresses no-op updates.
        # Record the changed edge's native timestamp as display metadata; its
        # identity and relationship remain exclusively authoritative in lineage.
        payload = deepcopy(view.json_addl)
        payload["properties"]["query_binding_recorded_at"] = recorded_at.isoformat()
        apply_guarded_fields(
            self.session, view, {"json_addl": payload},
            expected_revision=expected_revision,
        )
        return self._payload("view", view)

    def run(self, euid, *, expected_revision, parameters: dict) -> dict:
        obj = self._get("query", euid)
        require_revision(obj, expected_revision)
        definition = self._definition("query", self._properties(obj)["definition"])
        executor = self.executors[definition["executor"]]
        parsed = executor.parameters_model.model_validate(parameters).model_dump(mode="json")
        _definition_fields(parsed)
        result = executor.execute(
            self.session, deepcopy(definition["specification"]), parsed,
            self.principal, self.owner,
        )
        return {
            "contract": "tapdb.saved-query-result/v1", "query_euid": obj.euid,
            "query_revision": obj.record_revision, "result": result,
        }

    def run_view(
        self, euid, *, expected_revision, expected_query_revision, parameters,
    ) -> dict:
        view = self._get("view", euid)
        require_revision(view, expected_revision)
        self._definition("view", self._properties(view)["definition"])
        binding = self._binding(view)
        if binding is None:
            raise ValueError("DAG view has no query relationship")
        return {
            "contract": "tapdb.dag-view-result/v1",
            "view": self._payload("view", view),
            "query": self.run(
                binding.parent_instance.euid, expected_revision=expected_query_revision,
                parameters=parameters,
            ),
        }

    def revision(self, kind, euid, *, revision, epoch=None) -> dict:
        from daylily_tapdb.history import HistoryService

        self._get(kind, euid, include_deleted=True)
        return HistoryService(self.session).object_at(
            euid, record_type="instance", revision=revision, epoch=epoch
        )
