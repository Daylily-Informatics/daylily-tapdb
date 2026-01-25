"""TAPDB Template Model.

Phase 2 spec: ORM must match schema.
- json_addl_schema is JSONB
- instance_prefix is NOT NULL
"""

from sqlalchemy import Column, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from daylily_tapdb.models.base import tapdb_core


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
        primaryjoin="and_(generic_template.uuid == foreign(generic_instance.template_uuid))",
        backref="parent_template",
    )


# Typed template subclasses for polymorphic identity
class workflow_template(generic_template):
    """Template for workflow definitions."""
    __mapper_args__ = {"polymorphic_identity": "workflow_template", "confirm_deleted_rows": False}


class workflow_step_template(generic_template):
    """Template for workflow step definitions."""
    __mapper_args__ = {"polymorphic_identity": "workflow_step_template", "confirm_deleted_rows": False}


class container_template(generic_template):
    """Template for container definitions (plates, tubes, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "container_template", "confirm_deleted_rows": False}


class content_template(generic_template):
    """Template for content definitions (samples, reagents, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "content_template", "confirm_deleted_rows": False}


class equipment_template(generic_template):
    """Template for equipment definitions."""
    __mapper_args__ = {"polymorphic_identity": "equipment_template", "confirm_deleted_rows": False}


class data_template(generic_template):
    """Template for data object definitions."""
    __mapper_args__ = {"polymorphic_identity": "data_template", "confirm_deleted_rows": False}


class test_requisition_template(generic_template):
    """Template for test requisition definitions."""
    __mapper_args__ = {"polymorphic_identity": "test_requisition_template", "confirm_deleted_rows": False}


class actor_template(generic_template):
    """Template for actor definitions (users, systems, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "actor_template", "confirm_deleted_rows": False}


class action_template(generic_template):
    """Template for action definitions."""
    __mapper_args__ = {"polymorphic_identity": "action_template", "confirm_deleted_rows": False}


class health_event_template(generic_template):
    """Template for health event definitions."""
    __mapper_args__ = {"polymorphic_identity": "health_event_template", "confirm_deleted_rows": False}


class file_template(generic_template):
    """Template for file definitions."""
    __mapper_args__ = {"polymorphic_identity": "file_template", "confirm_deleted_rows": False}


class subject_template(generic_template):
    """Template for subject definitions (patients, participants, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "subject_template", "confirm_deleted_rows": False}
