"""
TAPDB Template Model.

Defines the generic_template table and typed template subclasses.
Templates are blueprints that define how instances should be created.
"""
from sqlalchemy import Column, Text, JSON
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
    }

    instance_prefix = Column(Text, nullable=True)
    instance_polymorphic_identity = Column(Text, nullable=True)
    json_addl_schema = Column(JSON, nullable=True)

    # Relationship to child instances
    child_instances = relationship(
        "generic_instance",
        primaryjoin="and_(generic_template.uuid == foreign(generic_instance.template_uuid))",
        backref="parent_template",
    )


# Typed template subclasses for polymorphic identity
class workflow_template(generic_template):
    """Template for workflow definitions."""
    __mapper_args__ = {"polymorphic_identity": "workflow_template"}


class workflow_step_template(generic_template):
    """Template for workflow step definitions."""
    __mapper_args__ = {"polymorphic_identity": "workflow_step_template"}


class container_template(generic_template):
    """Template for container definitions (plates, tubes, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "container_template"}


class content_template(generic_template):
    """Template for content definitions (samples, reagents, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "content_template"}


class equipment_template(generic_template):
    """Template for equipment definitions."""
    __mapper_args__ = {"polymorphic_identity": "equipment_template"}


class data_template(generic_template):
    """Template for data object definitions."""
    __mapper_args__ = {"polymorphic_identity": "data_template"}


class test_requisition_template(generic_template):
    """Template for test requisition definitions."""
    __mapper_args__ = {"polymorphic_identity": "test_requisition_template"}


class actor_template(generic_template):
    """Template for actor definitions (users, systems, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "actor_template"}


class action_template(generic_template):
    """Template for action definitions."""
    __mapper_args__ = {"polymorphic_identity": "action_template"}


class health_event_template(generic_template):
    """Template for health event definitions."""
    __mapper_args__ = {"polymorphic_identity": "health_event_template"}


class file_template(generic_template):
    """Template for file definitions."""
    __mapper_args__ = {"polymorphic_identity": "file_template"}


class subject_template(generic_template):
    """Template for subject definitions (patients, participants, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "subject_template"}
