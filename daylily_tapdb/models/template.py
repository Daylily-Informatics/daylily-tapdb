"""TAPDB Template Model.

Phase 2 spec: ORM must match schema.
- json_addl_schema is JSONB
- instance_prefix is NOT NULL
"""

from __future__ import annotations

from sqlalchemy import Column, Text, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session, relationship

from daylily_tapdb.models.base import tapdb_core
from daylily_tapdb.templates.mutation import (
    TemplateMutationGuardError,
    template_mutation_error_message,
    template_mutations_allowed,
)


class generic_template(tapdb_core):
    """
    Template table - blueprints for creating instances.

    Templates define:
    - instance_prefix: EUID prefix for instances created from this template
    - json_addl_schema: Optional JSON Schema for validating instance json_addl
    - json_addl: Template-specific configuration (child_templates, action_imports, etc.)

    Polymorphic inheritance allows typed subclasses (workflow_template, etc.)
    """

    __tablename__ = "generic_template"
    __mapper_args__ = {
        "polymorphic_identity": "generic_template",
        "polymorphic_on": "polymorphic_discriminator",
        # Soft delete is implemented via BEFORE DELETE trigger that returns NULL,
        # so PostgreSQL reports 0 rows deleted. Tell SQLAlchemy not to expect
        # a matched rowcount.
        "confirm_deleted_rows": False,
    }

    instance_prefix = Column(Text, nullable=False)
    instance_polymorphic_identity = Column(Text, nullable=True)
    json_addl_schema = Column(JSONB, nullable=True)

    # Relationship to child instances
    child_instances = relationship(
        "generic_instance",
        primaryjoin=(
            "and_(generic_template.uid == foreign(generic_instance.template_uid))"
        ),
        backref="parent_template",
    )


# Typed template subclasses for polymorphic identity
class workflow_template(generic_template):
    """Template for workflow definitions."""

    __mapper_args__ = {
        "polymorphic_identity": "workflow_template",
        "confirm_deleted_rows": False,
    }


class workflow_step_template(generic_template):
    """Template for workflow step definitions."""

    __mapper_args__ = {
        "polymorphic_identity": "workflow_step_template",
        "confirm_deleted_rows": False,
    }


class container_template(generic_template):
    """Template for container definitions (plates, tubes, etc.)."""

    __mapper_args__ = {
        "polymorphic_identity": "container_template",
        "confirm_deleted_rows": False,
    }


class content_template(generic_template):
    """Template for content definitions (samples, reagents, etc.)."""

    __mapper_args__ = {
        "polymorphic_identity": "content_template",
        "confirm_deleted_rows": False,
    }


class equipment_template(generic_template):
    """Template for equipment definitions."""

    __mapper_args__ = {
        "polymorphic_identity": "equipment_template",
        "confirm_deleted_rows": False,
    }


class data_template(generic_template):
    """Template for data object definitions."""

    __mapper_args__ = {
        "polymorphic_identity": "data_template",
        "confirm_deleted_rows": False,
    }


class test_requisition_template(generic_template):
    """Template for test requisition definitions."""

    __mapper_args__ = {
        "polymorphic_identity": "test_requisition_template",
        "confirm_deleted_rows": False,
    }


class actor_template(generic_template):
    """Template for actor definitions (users, systems, etc.)."""

    __mapper_args__ = {
        "polymorphic_identity": "actor_template",
        "confirm_deleted_rows": False,
    }


class action_template(generic_template):
    """Template for action definitions."""

    __mapper_args__ = {
        "polymorphic_identity": "action_template",
        "confirm_deleted_rows": False,
    }


class health_event_template(generic_template):
    """Template for health event definitions."""

    __mapper_args__ = {
        "polymorphic_identity": "health_event_template",
        "confirm_deleted_rows": False,
    }


class file_template(generic_template):
    """Template for file definitions."""

    __mapper_args__ = {
        "polymorphic_identity": "file_template",
        "confirm_deleted_rows": False,
    }


class subject_template(generic_template):
    """Template for subject definitions (patients, participants, etc.)."""

    __mapper_args__ = {
        "polymorphic_identity": "subject_template",
        "confirm_deleted_rows": False,
    }


def _is_generic_template_row(obj) -> bool:
    try:
        state = sa_inspect(obj)
    except Exception:
        return False
    mapper = getattr(state, "mapper", None)
    local_table = getattr(mapper, "local_table", None)
    return str(getattr(local_table, "name", "") or "") == "generic_template"


def _template_code_for_error(obj) -> str | None:
    category = str(getattr(obj, "category", "") or "").strip()
    type_name = str(getattr(obj, "type", "") or "").strip()
    subtype = str(getattr(obj, "subtype", "") or "").strip()
    version = str(getattr(obj, "version", "") or "").strip()
    if all([category, type_name, subtype, version]):
        return f"{category}/{type_name}/{subtype}/{version}/"
    return None


@event.listens_for(Session, "before_flush")
def _block_direct_template_mutations(
    session: Session,
    flush_context,
    instances,
) -> None:  # pragma: no cover - exercised through client/session tests.
    del flush_context, instances
    if template_mutations_allowed():
        return

    for obj in session.new:
        if _is_generic_template_row(obj):
            raise TemplateMutationGuardError(
                template_mutation_error_message(_template_code_for_error(obj))
            )

    for obj in session.dirty:
        if _is_generic_template_row(obj) and session.is_modified(
            obj, include_collections=False
        ):
            raise TemplateMutationGuardError(
                template_mutation_error_message(_template_code_for_error(obj))
            )

    for obj in session.deleted:
        if _is_generic_template_row(obj):
            raise TemplateMutationGuardError(
                template_mutation_error_message(_template_code_for_error(obj))
            )
