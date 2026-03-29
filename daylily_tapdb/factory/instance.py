"""Instance factory for TAPDB."""

import copy
import logging
import uuid
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

_SYSTEM_USER_COORDS = ("generic", "actor", "system_user")


def _norm_text(value: Any, *, lowercase: bool = False) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.lower() if lowercase else text


def _parse_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return default


def materialize_actions(
    session: Session, template: generic_template, template_manager
) -> Dict[str, Any]:
    """
    Materialize action_imports into action_groups for an instance.

    Reads action definitions from imported action templates and expands them
    into the format expected by ActionDispatcher. Uses canonical group naming:
    {type}_actions.

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

        definition = action_tmpl.json_addl.get("action_definition")
        if not isinstance(definition, dict) or not definition:
            logger.warning(
                "materialize_actions skipped %s: missing non-empty action_definition",
                template_code,
            )
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
            "action_template_uid": action_tmpl.uid,
            "action_template_euid": action_tmpl.euid,
            "action_template_code": template_code,
            **definition,
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
        tenant_id: Optional[uuid.UUID] = None,
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
            tenant_id: Optional tenant UUID; if provided, persists to the real column
                and also to json_addl["properties"]["tenant_id"] for transition
                compatibility.

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
        self._normalize_system_user_json_addl(template, json_addl)
        if tenant_id is not None:
            json_addl["properties"]["tenant_id"] = str(tenant_id)

        # Create instance
        instance = generic_instance(
            name=name,
            tenant_id=tenant_id,
            polymorphic_discriminator=template.instance_polymorphic_identity
            or template.polymorphic_discriminator.replace("_template", "_instance"),
            category=template.category,
            type=template.type,
            subtype=template.subtype,
            version=template.version,
            template_uid=template.uid,
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

    def _normalize_system_user_json_addl(
        self, template: generic_template, json_addl: Dict[str, Any]
    ) -> None:
        """Normalize actor/system_user payload shape for auth/user-store compatibility."""
        if (
            template.category,
            template.type,
            template.subtype,
        ) != _SYSTEM_USER_COORDS:
            return

        props = json_addl.get("properties")
        if not isinstance(props, dict):
            props = {}
            json_addl["properties"] = props

        def _pick(key: str) -> Any:
            top = json_addl.get(key)
            if top is not None and str(top).strip():
                return top
            return props.get(key)

        login_identifier = _norm_text(_pick("login_identifier"), lowercase=True)
        email = _norm_text(_pick("email"), lowercase=True)
        cognito_username = _norm_text(_pick("cognito_username"), lowercase=True)
        if not login_identifier:
            login_identifier = email or cognito_username
        if not login_identifier:
            raise ValueError(
                "system_user requires a non-empty login_identifier "
                "(or email/cognito_username)."
            )

        role = (_norm_text(_pick("role"), lowercase=True) or "user").lower()
        if role not in {"admin", "user"}:
            raise ValueError("system_user role must be 'admin' or 'user'.")

        normalized = {
            "login_identifier": login_identifier,
            "email": email or "",
            "display_name": _norm_text(_pick("display_name")) or "",
            "role": role,
            "is_active": _parse_bool(_pick("is_active"), default=True),
            "require_password_change": _parse_bool(
                _pick("require_password_change"), default=False
            ),
            "password_hash": _norm_text(_pick("password_hash")),
            "last_login_dt": _norm_text(_pick("last_login_dt")),
            "cognito_username": cognito_username or "",
        }

        # Keep both top-level keys (used by auth/user-store SQL and unique index)
        # and nested properties (used by generic template rendering/edit flows).
        for key, value in normalized.items():
            json_addl[key] = value
            props[key] = value

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
                    child_overrides: dict[str, Any] = {}
                else:
                    child_template_code = root.template_code
                    count = root.count
                    name_pattern = root.name_pattern or layout_name_pattern
                    child_overrides = dict(getattr(root, "model_extra", {}) or {})

                child_template_code = self._resolve_template_code_pattern(
                    session, child_template_code
                )

                if not name_pattern:
                    override_json = child_overrides.get("json_addl")
                    if isinstance(override_json, dict):
                        properties = override_json.get("properties")
                        if isinstance(properties, dict):
                            candidate_name = _norm_text(properties.get("name"))
                            if candidate_name:
                                name_pattern = candidate_name

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
                        tenant_id=getattr(parent, "tenant_id", None),
                    )

                    override_json = child_overrides.get("json_addl")
                    if isinstance(override_json, dict):
                        payload = dict(getattr(child_obj, "json_addl", {}) or {})
                        payload.update(copy.deepcopy(override_json))
                        child_obj.json_addl = payload
                        session.flush()

                    self._create_lineage(session, parent, child_obj, relationship_type)

    def _resolve_template_code_pattern(
        self, session: Session, template_code: str
    ) -> str:
        """Resolve wildcard template-code patterns against seeded templates."""

        normalized = normalize_template_code_str(str(template_code or ""))
        if "*" not in normalized:
            return f"{normalized}/"

        parts = [part for part in normalized.split("/") if part]
        if len(parts) != 4:
            raise ValueError(f"Invalid child template pattern: {template_code!r}")

        category, type_name, subtype, version = parts
        query = session.query(generic_template).filter(
            generic_template.is_deleted.is_(False)
        )
        if category != "*":
            query = query.filter(generic_template.category == category)
        if type_name != "*":
            query = query.filter(generic_template.type == type_name)
        if subtype != "*":
            query = query.filter(generic_template.subtype == subtype)
        if version != "*":
            query = query.filter(generic_template.version == version)

        matches = query.order_by(generic_template.version.desc()).all()
        if len(matches) != 1:
            raise ValueError(
                "Template pattern must resolve to exactly one seeded template: "
                f"{template_code!r} -> {len(matches)} matches"
            )

        match = matches[0]
        return f"{match.category}/{match.type}/{match.subtype}/{match.version}/"

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
            tenant_id=getattr(parent, "tenant_id", None),
            polymorphic_discriminator="generic_instance_lineage",
            category="generic",
            type="lineage",
            subtype="instance_lineage",
            version="1.0.0",
            bstatus="active",
            parent_instance_uid=parent.uid,
            child_instance_uid=child.uid,
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
                generic_instance.template_uid == template.uid,
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
