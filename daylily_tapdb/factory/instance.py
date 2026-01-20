"""Instance factory for TAPDB."""
import copy
import logging
from typing import Dict, List, Optional, Any

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.models.lineage import generic_instance_lineage

logger = logging.getLogger(__name__)


def materialize_actions(
    template: generic_template,
    template_manager
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
    action_groups = {}

    for action_key, template_code in template.json_addl.get("action_imports", {}).items():
        action_tmpl = template_manager.get_template(template_code)
        if action_tmpl:
            # Canonical group naming: {type}_actions
            group_name = f"{action_tmpl.type}_actions"
            if group_name not in action_groups:
                action_groups[group_name] = {}

            # Copy action definition with runtime tracking fields
            action_groups[group_name][action_key] = {
                **action_tmpl.json_addl.get("action_definition", {}),
                "action_executed": "0",
                "executed_datetime": [],
                "action_enabled": "1"
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

    def __init__(self, db, template_manager):
        """
        Initialize instance factory.

        Args:
            db: TAPDBConnection instance.
            template_manager: TemplateManager for template resolution.
        """
        self.db = db
        self.template_manager = template_manager

    def create_instance(
        self,
        template_code: str,
        name: str,
        properties: Optional[Dict[str, Any]] = None,
        create_children: bool = True,
        _depth: int = 0,
        _visited: Optional[set] = None
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
        # Initialize visited set on first call
        if _visited is None:
            _visited = set()

        # Check recursion depth
        if _depth > self.MAX_INSTANTIATION_DEPTH:
            raise ValueError(
                f"Maximum instantiation depth ({self.MAX_INSTANTIATION_DEPTH}) exceeded. "
                f"Check for cycles in instantiation_layouts."
            )

        # Check for cycles
        if template_code in _visited:
            raise ValueError(
                f"Cycle detected in instantiation_layouts: {template_code} "
                f"already visited in this instantiation chain."
            )
        _visited.add(template_code)

        # Get template
        template = self.template_manager.get_template(template_code)
        if not template:
            raise ValueError(f"Template not found: {template_code}")

        # Build json_addl
        json_addl = self._build_json_addl(template, properties)

        # Create instance
        instance = generic_instance(
            name=name,
            polymorphic_discriminator=template.instance_polymorphic_identity or template.polymorphic_discriminator.replace("_template", "_instance"),
            category=template.category,
            type=template.type,
            subtype=template.subtype,
            version=template.version,
            template_uuid=template.uuid,
            json_addl=json_addl,
            bstatus=template.json_addl.get("default_status", "created"),
            is_singleton=template.json_addl.get("singleton", False)
        )

        # Use the connection's session (not get_session() which creates a new one)
        self.db.session.add(instance)
        self.db.session.flush()  # Get UUID/EUID assigned

        # Create children if requested
        if create_children:
            self._create_children(instance, template, _depth, _visited.copy())

        return instance


    def _build_json_addl(
        self,
        template: generic_template,
        properties: Optional[Dict[str, Any]] = None
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
            "action_groups": materialize_actions(template, self.template_manager),
            "audit_log": []
        }

        # Merge custom properties
        if properties:
            json_addl["properties"].update(properties)

        return json_addl

    def _create_children(
        self,
        parent: generic_instance,
        template: generic_template,
        depth: int,
        visited: set
    ):
        """
        Create child instances from instantiation_layouts.

        Args:
            parent: Parent instance.
            template: Parent's template.
            depth: Current recursion depth.
            visited: Set of visited template codes.
        """
        layouts = template.json_addl.get("instantiation_layouts", {})

        # Handle both dict and list formats (empty list means no layouts)
        if isinstance(layouts, list):
            if not layouts:
                return  # Empty list, no children to create
            # Convert list to dict format: use index as key
            layouts = {str(i): item for i, item in enumerate(layouts)}

        if not isinstance(layouts, dict):
            return  # Invalid format, skip

        for layout_name, layout_def in layouts.items():
            child_template_code = layout_def.get("template_code")
            if not child_template_code:
                continue

            count = layout_def.get("count", 1)
            relationship_type = layout_def.get("relationship_type", "contains")
            name_pattern = layout_def.get("name_pattern", "{parent_name}_{index}")

            for i in range(count):
                child_name = name_pattern.format(
                    parent_name=parent.name,
                    parent_euid=parent.euid,
                    index=i + 1,
                    layout_name=layout_name
                )

                child = self.create_instance(
                    template_code=child_template_code,
                    name=child_name,
                    create_children=True,
                    _depth=depth + 1,
                    _visited=visited.copy()
                )

                # Create lineage relationship
                self._create_lineage(parent, child, relationship_type)

    def _create_lineage(
        self,
        parent: generic_instance,
        child: generic_instance,
        relationship_type: str = "contains"
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
            child_type=child.polymorphic_discriminator
        )

        # Use the connection's session (not get_session() which creates a new one)
        self.db.session.add(lineage)
        self.db.session.flush()

        return lineage

    def link_instances(
        self,
        parent: generic_instance,
        child: generic_instance,
        relationship_type: str = "generic"
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
        return self._create_lineage(parent, child, relationship_type)
