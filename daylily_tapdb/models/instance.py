"""
TAPDB Instance Model.

Defines the generic_instance table and typed instance subclasses.
Instances are concrete objects created from templates.
"""
from typing import List, Optional, Dict, Any
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from daylily_tapdb.models.base import tapdb_core


class generic_instance(tapdb_core):
    """
    Instance table - concrete objects created from templates.

    Instances have:
    - template_uuid: Reference to the template this instance was created from
    - Lineage relationships: parent_of_lineages and child_of_lineages

    Polymorphic inheritance allows typed subclasses (workflow_instance, etc.)
    """
    __tablename__ = "generic_instance"
    __mapper_args__ = {
        "polymorphic_identity": "generic_instance",
        "polymorphic_on": "polymorphic_discriminator",
    }

    template_uuid = Column(UUID, ForeignKey("generic_template.uuid"), nullable=True)

    # Lineage relationships
    parent_of_lineages = relationship(
        "generic_instance_lineage",
        primaryjoin="and_(generic_instance.uuid == foreign(generic_instance_lineage.parent_instance_uuid))",
        backref="parent_instance",
        lazy="dynamic",
    )

    child_of_lineages = relationship(
        "generic_instance_lineage",
        primaryjoin="and_(generic_instance.uuid == foreign(generic_instance_lineage.child_instance_uuid))",
        backref="child_instance",
        lazy="dynamic",
    )

    def get_sorted_parent_of_lineages(
        self, priority_discriminators: Optional[List[str]] = None
    ) -> List:
        """
        Returns parent_of_lineages sorted by polymorphic_discriminator.
        Lineages with discriminator in priority_discriminators are put first.
        """
        if priority_discriminators is None:
            priority_discriminators = ["workflow_step_instance"]

        priority = [
            lin for lin in self.parent_of_lineages
            if lin.child_instance.polymorphic_discriminator in priority_discriminators
        ]
        other = [
            lin for lin in self.parent_of_lineages
            if lin.child_instance.polymorphic_discriminator not in priority_discriminators
        ]

        priority.sort(key=lambda x: x.child_instance.euid or "")
        other.sort(key=lambda x: x.child_instance.euid or "")

        return priority + other

    def get_sorted_child_of_lineages(
        self, priority_discriminators: Optional[List[str]] = None
    ) -> List:
        """
        Returns child_of_lineages sorted by polymorphic_discriminator.
        Lineages with discriminator in priority_discriminators are put first.
        """
        if priority_discriminators is None:
            priority_discriminators = ["workflow_step_instance"]

        priority = [
            lin for lin in self.child_of_lineages
            if lin.parent_instance.polymorphic_discriminator in priority_discriminators
        ]
        other = [
            lin for lin in self.child_of_lineages
            if lin.parent_instance.polymorphic_discriminator not in priority_discriminators
        ]

        priority.sort(key=lambda x: x.parent_instance.euid or "")
        other.sort(key=lambda x: x.parent_instance.euid or "")

        return priority + other

    def filter_lineage_members(
        self,
        of_lineage_type: str,
        lineage_member_type: str,
        filter_criteria: Dict[str, Any],
    ) -> List:
        """
        Filter lineage members based on criteria.

        Args:
            of_lineage_type: 'parent_of_lineages' or 'child_of_lineages'
            lineage_member_type: 'parent_instance' or 'child_instance'
            filter_criteria: Dict of attribute names to expected values
        """
        if of_lineage_type not in ["parent_of_lineages", "child_of_lineages"]:
            raise ValueError("of_lineage_type must be 'parent_of_lineages' or 'child_of_lineages'")
        if lineage_member_type not in ["parent_instance", "child_instance"]:
            raise ValueError("lineage_member_type must be 'parent_instance' or 'child_instance'")
        if not filter_criteria:
            raise ValueError("filter_criteria cannot be empty")

        lineage_members = getattr(self, of_lineage_type)
        filtered = []

        for member in lineage_members:
            instance = getattr(member, lineage_member_type)
            match = True
            for key, value in filter_criteria.items():
                attr_val = getattr(instance, key, None)
                json_val = (instance.json_addl or {}).get(key) if instance.json_addl else None
                if attr_val != value and json_val != value:
                    match = False
                    break
            if match:
                filtered.append(member)

        return filtered


# Typed instance subclasses for polymorphic identity
class workflow_instance(generic_instance):
    """Instance of a workflow."""
    __mapper_args__ = {"polymorphic_identity": "workflow_instance"}


class workflow_step_instance(generic_instance):
    """Instance of a workflow step."""
    __mapper_args__ = {"polymorphic_identity": "workflow_step_instance"}


class container_instance(generic_instance):
    """Instance of a container (plate, tube, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "container_instance"}


class content_instance(generic_instance):
    """Instance of content (sample, reagent, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "content_instance"}


class equipment_instance(generic_instance):
    """Instance of equipment."""
    __mapper_args__ = {"polymorphic_identity": "equipment_instance"}


class data_instance(generic_instance):
    """Instance of a data object."""
    __mapper_args__ = {"polymorphic_identity": "data_instance"}


class test_requisition_instance(generic_instance):
    """Instance of a test requisition."""
    __mapper_args__ = {"polymorphic_identity": "test_requisition_instance"}


class actor_instance(generic_instance):
    """Instance of an actor (user, system, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "actor_instance"}


class action_instance(generic_instance):
    """Instance of an action (audit record)."""
    __mapper_args__ = {"polymorphic_identity": "action_instance"}


class health_event_instance(generic_instance):
    """Instance of a health event."""
    __mapper_args__ = {"polymorphic_identity": "health_event_instance"}


class file_instance(generic_instance):
    """Instance of a file."""
    __mapper_args__ = {"polymorphic_identity": "file_instance"}


class subject_instance(generic_instance):
    """Instance of a subject (patient, participant, etc.)."""
    __mapper_args__ = {"polymorphic_identity": "subject_instance"}
