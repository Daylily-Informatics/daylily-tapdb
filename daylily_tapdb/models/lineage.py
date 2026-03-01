"""
TAPDB Lineage Model.

Defines the generic_instance_lineage table and typed lineage subclasses.
Lineages represent directed edges between instances (parent -> child).
"""

from sqlalchemy import BIGINT, Column, FetchedValue, ForeignKey, Text

from daylily_tapdb.models.base import tapdb_core


class generic_instance_lineage(tapdb_core):
    """
    Lineage table - directed edges between instances.

    Lineages connect instances in a DAG (Directed Acyclic Graph):
    - parent_instance_uuid: The parent instance ID
    - child_instance_uuid: The child instance ID
    - relationship_type: Type of relationship (e.g., "contains", "derived_from")
    - parent_type/child_type: Cached polymorphic types for query optimization

    Polymorphic inheritance allows typed subclasses (workflow_instance_lineage, etc.)
    """

    __tablename__ = "generic_instance_lineage"
    __mapper_args__ = {
        "polymorphic_identity": "generic_instance_lineage",
        "polymorphic_on": "polymorphic_discriminator",
        # Soft delete is implemented via BEFORE DELETE trigger that returns NULL,
        # so PostgreSQL reports 0 rows deleted. Tell SQLAlchemy not to expect
        # a matched rowcount.
        "confirm_deleted_rows": False,
    }

    parent_type = Column(Text, nullable=True)
    child_type = Column(Text, nullable=True)
    relationship_type = Column(Text, nullable=False, server_default=FetchedValue())

    parent_instance_uuid = Column(
        BIGINT, ForeignKey("generic_instance.uuid"), nullable=False
    )
    child_instance_uuid = Column(
        BIGINT, ForeignKey("generic_instance.uuid"), nullable=False
    )


# Typed lineage subclasses for polymorphic identity
class workflow_instance_lineage(generic_instance_lineage):
    """Lineage for workflow relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "workflow_instance_lineage",
        "confirm_deleted_rows": False,
    }


class workflow_step_instance_lineage(generic_instance_lineage):
    """Lineage for workflow step relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "workflow_step_instance_lineage",
        "confirm_deleted_rows": False,
    }


class container_instance_lineage(generic_instance_lineage):
    """Lineage for container relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "container_instance_lineage",
        "confirm_deleted_rows": False,
    }


class content_instance_lineage(generic_instance_lineage):
    """Lineage for content relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "content_instance_lineage",
        "confirm_deleted_rows": False,
    }


class equipment_instance_lineage(generic_instance_lineage):
    """Lineage for equipment relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "equipment_instance_lineage",
        "confirm_deleted_rows": False,
    }


class data_instance_lineage(generic_instance_lineage):
    """Lineage for data object relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "data_instance_lineage",
        "confirm_deleted_rows": False,
    }


class test_requisition_instance_lineage(generic_instance_lineage):
    """Lineage for test requisition relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "test_requisition_instance_lineage",
        "confirm_deleted_rows": False,
    }


class actor_instance_lineage(generic_instance_lineage):
    """Lineage for actor relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "actor_instance_lineage",
        "confirm_deleted_rows": False,
    }


class action_instance_lineage(generic_instance_lineage):
    """Lineage for action relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "action_instance_lineage",
        "confirm_deleted_rows": False,
    }


class health_event_instance_lineage(generic_instance_lineage):
    """Lineage for health event relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "health_event_instance_lineage",
        "confirm_deleted_rows": False,
    }


class file_instance_lineage(generic_instance_lineage):
    """Lineage for file relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "file_instance_lineage",
        "confirm_deleted_rows": False,
    }


class subject_instance_lineage(generic_instance_lineage):
    """Lineage for subject relationships."""

    __mapper_args__ = {
        "polymorphic_identity": "subject_instance_lineage",
        "confirm_deleted_rows": False,
    }
