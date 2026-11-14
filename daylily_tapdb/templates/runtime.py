"""Runtime template authoring under the caller's native scope and prefix rights.

This service inserts new definitions only. It never provisions allocators, changes
runtime identity, seeds governance objects, commits, or obtains operator access.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from daylily_tapdb.euid import EUIDConfig
from daylily_tapdb.governance import GovernanceContext
from daylily_tapdb.issuance import IssuanceProfile
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.sequences import (
    _normalize_instance_prefix, require_identity_prefix_sequence, require_instance_prefix_sequence,
)
from daylily_tapdb.services.object_operations import object_payload
from daylily_tapdb.templates.mutation import allow_template_mutations
from daylily_tapdb.templates.loader import find_tapdb_core_config_dir, load_template_configs
from daylily_tapdb.validation.governance import DEFAULT_VALIDATOR_REF, normalize_validator_ref
from daylily_tapdb.validation.instantiation_layouts import validate_instantiation_layouts


class TemplateExistsError(ValueError):
    """An immutable template coordinate is already occupied, including retirement."""


class RuntimeTemplateDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    name: str = Field(min_length=1)
    polymorphic_discriminator: str
    category: str
    type: str
    subtype: str
    version: str
    instance_prefix: str
    instance_polymorphic_identity: str
    json_addl: dict[str, Any]
    json_addl_schema: dict[str, Any] | None = None
    validator_ref: str = DEFAULT_VALIDATOR_REF
    bstatus: str = "active"
    is_singleton: bool = False

    @field_validator("name", "bstatus", "category", "type", "subtype", "version")
    @classmethod
    def exact_text(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("template fields must be nonblank exact text")
        return value

    @field_validator("category", "type", "subtype", "version")
    @classmethod
    def exact_component(cls, value: str) -> str:
        if "/" in value or "*" in value:
            raise ValueError("template coordinates require four exact components")
        return value

    @field_validator("instance_prefix")
    @classmethod
    def exact_prefix(cls, value: str) -> str:
        if _normalize_instance_prefix(value) != value:
            raise ValueError("instance_prefix must be the exact uppercase registered prefix")
        return value

    @field_validator("polymorphic_discriminator")
    @classmethod
    def template_discriminator(cls, value: str) -> str:
        if value not in generic_template.__mapper__.polymorphic_map:
            raise ValueError("unknown native template polymorphic discriminator")
        return value

    @field_validator("instance_polymorphic_identity")
    @classmethod
    def instance_discriminator(cls, value: str) -> str:
        if value not in generic_instance.__mapper__.polymorphic_map:
            raise ValueError("unknown native instance polymorphic identity")
        return value

    @field_validator("validator_ref")
    @classmethod
    def validator(cls, value: str) -> str:
        return normalize_validator_ref(value)

    @model_validator(mode="after")
    def layouts(self) -> RuntimeTemplateDefinition:
        if self.category.upper() == self.instance_prefix:
            raise ValueError("category must be semantic, not an allocation prefix")
        if self.instance_prefix in EUIDConfig().CORE_PREFIXES.values():
            raise ValueError("reserved TapDB operational prefixes require bundled core provisioning")
        if "instantiation_layouts" in self.json_addl:
            validate_instantiation_layouts(self.json_addl["instantiation_layouts"])
        if "properties" in self.json_addl and not isinstance(self.json_addl["properties"], dict):
            raise ValueError("json_addl.properties must be an object")
        return self

    @property
    def code(self) -> str:
        return "/".join((self.category, self.type, self.subtype, self.version))


class RuntimeTemplatePack(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    templates: list[RuntimeTemplateDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def distinct_coordinates(self) -> RuntimeTemplatePack:
        codes = [item.code for item in self.templates]
        if len(codes) != len(set(codes)):
            raise ValueError("template pack contains duplicate exact coordinates")
        return self


def author_templates(
    session: Session,
    payload: dict[str, Any],
    *,
    governance: GovernanceContext,
    issuance_profile: IssuanceProfile,
) -> dict[str, Any]:
    """Create immutable owner-scoped templates using preprovisioned allocators.

    Runtime RLS, native identity triggers and audit remain authoritative. The
    caller installs its existing authenticated transaction context and commits.
    Global template visibility requires the runtime's existing global-row grant.
    """
    pack = RuntimeTemplatePack.model_validate(payload)
    template_prefix = issuance_profile.identity_prefix("generic_template")
    if issuance_profile.mode != "canonical":
        governance.require_prefix(template_prefix)
    require_identity_prefix_sequence(session, "generic_template", template_prefix)
    core_coordinates = {
        "/".join(str(item[field]) for field in ("category", "type", "subtype", "version"))
        for item in load_template_configs(find_tapdb_core_config_dir())
    }
    provisioned_prefixes: set[str] = set()
    for definition in pack.templates:
        if definition.code in core_coordinates:
            raise PermissionError("native core template coordinates require bundled core provisioning")
        if not issuance_profile.accepts_template_prefix(definition.instance_prefix):
            raise ValueError("instance_prefix differs from the configured issuance profile")
        governance.require_prefix(definition.instance_prefix)
        if definition.instance_prefix not in provisioned_prefixes:
            require_instance_prefix_sequence(session, definition.instance_prefix)
            provisioned_prefixes.add(definition.instance_prefix)
        existing = session.execute(select(generic_template).where(
            generic_template.domain_code == governance.domain_code,
            generic_template.issuer_app_code == governance.owner_repo_name,
            generic_template.category == definition.category,
            generic_template.type == definition.type,
            generic_template.subtype == definition.subtype,
            generic_template.version == definition.version,
        )).scalar_one_or_none()
        if existing is not None:
            raise TemplateExistsError(
                f"Template {definition.code} already exists; publish a new version"
            )

    created = []
    with allow_template_mutations():
        for definition in pack.templates:
            model = generic_template.__mapper__.polymorphic_map[
                definition.polymorphic_discriminator
            ].class_
            row = model(
                **deepcopy(definition.model_dump()),
                domain_code=governance.domain_code,
                issuer_app_code=governance.owner_repo_name,
                tenant_id=None,
                is_deleted=False,
            )
            session.add(row)
            created.append(row)
        session.flush()

    return {
        "contract": "tapdb.runtime-template-authoring/v1",
        "inserted": len(created),
        "templates": [object_payload(row, "template") for row in created],
    }
