"""Instance factory for TAPDB."""

import copy
import logging
from typing import Any, Dict, Optional

from pydantic import ValidationError
from sqlalchemy.orm import Session

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.validation.instantiation_layouts import (
    format_validation_error,
    normalize_template_code_str,
    validate_instantiation_layouts,
)

logger = logging.getLogger(__name__)


def materialize_actions(
    session: Session, template: generic_template, template_manager
) -> Dict[str, Any]:
    """
    Materialize action_imports into action_groups for an instance.

    Reads action template definitions and expands them into the format
    expected by ActionDispatcher. Uses canonical group naming: {type}_actions.

    Args:
        template: The template to materialize actions from.
        template_manager: TemplateManager for resolving action templates.

    Returns:
        Dictionary of action groups.
    """
    action_groups: Dict[str, Any] = {}

    for action_key, template_code in template.json_addl.get(
        "action_imports", {}
    ).items():
        action_tmpl = template_manager.get_template(session, template_code)
        if action_tmpl is None:
            continue

        # Canonical group naming: {type}_actions
        group_name = f"{action_tmpl.type}_actions"
        if group_name not in action_groups:
            action_groups[group_name] = {}

        # Copy action definition with runtime tracking fields.
        # Phase 2 moonshot: carry action template identity through materialization
        # so ActionDispatcher can persist action_instance rows against the action
        # template (ensuring XX prefix), not against the target instance template.
        action_groups[group_name][action_key] = {
            "action_template_uuid": action_tmpl.uuid,
            "action_template_euid": action_tmpl.euid,
            "action_template_code": template_code,
            **action_tmpl.json_addl.get("action_definition", {}),
            "action_executed": "0",
            "executed_datetime": [],
            "action_enabled": "1",
        }

    return action_groups


class InstanceFactory:
    """
    Factory for creating instances from templates.

    Handles:
    - Instance creation from templates
    - Property merging (template defaults + custom properties)
    - Action materialization
    - Child object creation from instantiation_layouts
    - Cycle detection in recursive instantiation
    """

    # Maximum recursion depth for instantiation_layouts
    MAX_INSTANTIATION_DEPTH = 10

    def __init__(self, template_manager):
        """
        Initialize instance factory.

        Args:
            template_manager: TemplateManager for template resolution.
        """
        self.template_manager = template_manager

    def create_instance(
        self,
        session: Session,
        template_code: str,
        name: str,
        properties: Optional[Dict[str, Any]] = None,
        create_children: bool = True,
        _depth: int = 0,
        _visited: Optional[set] = None,
    ) -> generic_instance:
        """
        Create an instance from a template.

        Args:
            template_code: Template code string.
            name: Name for the new instance.
            properties: Custom properties to merge with template defaults.
            create_children: Whether to create child objects from instantiation_layouts.
            _depth: Internal recursion depth tracker.
            _visited: Internal visited set for cycle detection.

        Returns:
            The created instance.

        Raises:
            ValueError: If template not found, max depth exceeded, or cycle detected.
        """
        template_code = normalize_template_code_str(template_code)

        # Initialize visited set on first call
        if _visited is None:
            _visited = set()

        # Check recursion depth
        if _depth > self.MAX_INSTANTIATION_DEPTH:
            max_d = self.MAX_INSTANTIATION_DEPTH
            raise ValueError(
                f"Maximum instantiation depth ({max_d})"
                " exceeded. Check for cycles in"
                " instantiation_layouts."
            )

        # Check for cycles
        if template_code in _visited:
            raise ValueError(
                f"Cycle detected in instantiation_layouts: {template_code} "
                f"already visited in this instantiation chain."
            )
        _visited.add(template_code)

        # Get template
        template = self.template_manager.get_template(session, template_code)
        if not template:
            raise ValueError(f"Template not found: {template_code}")

        # Build json_addl
        json_addl = self._build_json_addl(session, template, properties)

        # Create instance
        instance = generic_instance(
            name=name,
            polymorphic_discriminator=template.instance_polymorphic_identity
            or template.polymorphic_discriminator.replace("_template", "_instance"),
            category=template.category,
            type=template.type,
            subtype=template.subtype,
            version=template.version,
            template_uuid=template.uuid,
            json_addl=json_addl,
            bstatus=template.json_addl.get("default_status", "created"),
            is_singleton=bool(template.is_singleton),
        )

        session.add(instance)
        session.flush()  # Get ID/EUID assigned

        # Create children if requested
        if create_children:
            self._create_children(session, instance, template, _depth, _visited.copy())

        return instance

    def _build_json_addl(
        self,
        session: Session,
        template: generic_template,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build json_addl for a new instance.

        Merges template defaults with custom properties and materializes actions.

        Args:
            template: Source template.
            properties: Custom properties to merge.

        Returns:
            Complete json_addl dictionary.
        """
        json_addl = {
            "properties": copy.deepcopy(template.json_addl.get("properties", {})),
            "action_groups": materialize_actions(
                session, template, self.template_manager
            ),
            "audit_log": [],
        }

        # Merge custom properties
        if properties:
            json_addl["properties"].update(properties)

        return json_addl

    def _create_children(
        self,
        session: Session,
        parent: generic_instance,
        template: generic_template,
        depth: int,
        visited: set,
    ):
        """
        Create child instances from instantiation_layouts.

        Args:
            parent: Parent instance.
            template: Parent's template.
            depth: Current recursion depth.
            visited: Set of visited template codes.
        """
        raw_layouts = template.json_addl.get("instantiation_layouts", [])
        try:
            layouts = validate_instantiation_layouts(raw_layouts)
        except ValidationError as e:
            raise ValueError(
                f"Invalid instantiation_layouts: {format_validation_error(e)}"
            ) from e

        if not layouts:
            return

        for layout_index, layout in enumerate(layouts):
            relationship_type = layout.relationship_type
            layout_name_pattern = layout.name_pattern

            for child_index, child in enumerate(layout.child_templates):
                root = child.root
                if isinstance(root, str):
                    child_template_code = root
                    count = 1
                    name_pattern = layout_name_pattern
                else:
                    child_template_code = root.template_code
                    count = root.count
                    name_pattern = root.name_pattern or layout_name_pattern

                # child_template_code is validated as category/type/subtype/version
                child_subtype = child_template_code.split("/")[2]
                name_pattern = name_pattern or "{parent_name}_{child_subtype}_{index}"

                for i in range(count):
                    child_name = name_pattern.format(
                        parent_name=parent.name,
                        parent_euid=parent.euid,
                        index=i + 1,
                        layout_index=layout_index,
                        child_index=child_index,
                        child_subtype=child_subtype,
                        child_template_code=child_template_code,
                    )

                    child_obj = self.create_instance(
                        session=session,
                        template_code=child_template_code,
                        name=child_name,
                        create_children=True,
                        _depth=depth + 1,
                        _visited=visited.copy(),
                    )

                    self._create_lineage(session, parent, child_obj, relationship_type)

    def _create_lineage(
        self,
        session: Session,
        parent: generic_instance,
        child: generic_instance,
        relationship_type: str = "contains",
    ) -> generic_instance_lineage:
        """
        Create a lineage relationship between two instances.

        Args:
            parent: Parent instance.
            child: Child instance.
            relationship_type: Type of relationship.

        Returns:
            The created lineage record.
        """
        lineage = generic_instance_lineage(
            name=f"{parent.euid}->{child.euid}",
            polymorphic_discriminator="generic_instance_lineage",
            category="generic",
            type="lineage",
            subtype="instance_lineage",
            version="1.0.0",
            bstatus="active",
            parent_instance_uuid=parent.uuid,
            child_instance_uuid=child.uuid,
            relationship_type=relationship_type,
            parent_type=parent.polymorphic_discriminator,
            child_type=child.polymorphic_discriminator,
        )

        session.add(lineage)
        session.flush()

        return lineage

    def link_instances(
        self,
        session: Session,
        parent: generic_instance,
        child: generic_instance,
        relationship_type: str = "generic",
    ) -> generic_instance_lineage:
        """
        Create a lineage link between two existing instances.

        Args:
            parent: Parent instance.
            child: Child instance.
            relationship_type: Type of relationship.

        Returns:
            The created lineage record.
        """
        return self._create_lineage(session, parent, child, relationship_type)

    def get_or_create_singleton_instance(
        self,
        session: Session,
        template_code: str,
        name: str,
        properties: Optional[Dict[str, Any]] = None,
        create_children: bool = True,
    ) -> generic_instance:
        """Get or create the singleton instance for a singleton template.

        Semantics (per Major): do not resurrect soft-deleted rows.
        """
        template = self.template_manager.get_template(session, template_code)
        if template is None:
            raise ValueError(f"Template not found: {template_code}")
        if not bool(template.is_singleton):
            raise ValueError(f"Template is not singleton: {template_code}")

        existing = (
            session.query(generic_instance)
            .filter(
                generic_instance.template_uuid == template.uuid,
                generic_instance.is_deleted.is_(False),
            )
            .order_by(generic_instance.created_dt.desc())
            .first()
        )
        if existing is not None:
            return existing

        return self.create_instance(
            session=session,
            template_code=template_code,
            name=name,
            properties=properties,
            create_children=create_children,
        )
