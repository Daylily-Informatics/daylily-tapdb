"""Template management for TAPDB.

Phase 2 policy:
- DB templates are the runtime source-of-truth
- Core library methods accept an explicit SQLAlchemy Session
- Do not cache ORM objects across sessions (cache IDs instead)
"""
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from daylily_tapdb.models.template import generic_template

logger = logging.getLogger(__name__)


class TemplateManager:
    """
    Manages template loading and caching.

    Provides methods to:
    - Load templates from JSON configuration files
    - Cache templates for efficient lookup
    - Resolve template codes to template objects
    """

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize template manager.

        Args:
            config_path: Optional path to template configuration directory
                (used by seeding tooling; DB remains the runtime source-of-truth).
        """
        self.config_path = config_path
        # Cache IDs only to avoid detached ORM objects.
        self._template_uuid_cache: Dict[str, Any] = {}
        self._template_euid_cache: Dict[str, Any] = {}

    def get_template(self, session: Session, template_code: str) -> Optional[generic_template]:
        """
        Get a template by its code string.

        Template code format: {category}/{type}/{subtype}/{version}/
        Example: container/plate/fixed-plate-96/1.0/

        Args:
            template_code: Template code string.

        Returns:
            The template object, or None if not found.
        """
        cached_uuid = self._template_uuid_cache.get(template_code)
        if cached_uuid is not None:
            tmpl = session.get(generic_template, cached_uuid)
            if tmpl is not None and tmpl.is_deleted is False:
                return tmpl

        # Parse template code
        parts = template_code.strip("/").split("/")
        if len(parts) != 4:
            logger.warning(f"Invalid template code format: {template_code}")
            return None

        category, type_, subtype, version = parts

        template = session.query(generic_template).filter(
            generic_template.category == category,
            generic_template.type == type_,
            generic_template.subtype == subtype,
            generic_template.version == version,
            generic_template.is_deleted == False
        ).first()

        if template:
            self._template_uuid_cache[template_code] = template.uuid
            self._template_euid_cache[template.euid] = template.uuid

        return template

    def get_template_by_euid(self, session: Session, euid: str) -> Optional[generic_template]:
        """
        Get a template by its EUID.

        Args:
            euid: Template EUID (e.g., GT123).

        Returns:
            The template object, or None if not found.
        """
        cached_uuid = self._template_euid_cache.get(euid)
        if cached_uuid is not None:
            tmpl = session.get(generic_template, cached_uuid)
            if tmpl is not None and tmpl.is_deleted is False:
                return tmpl

        tmpl = session.query(generic_template).filter(
            generic_template.euid == euid,
            generic_template.is_deleted == False,
        ).first()
        if tmpl is not None:
            self._template_euid_cache[euid] = tmpl.uuid
        return tmpl

    def clear_cache(self):
        """Clear the template cache."""
        self._template_uuid_cache.clear()
        self._template_euid_cache.clear()

    def list_templates(
        self,
        session: Session,
        category: Optional[str] = None,
        type_: Optional[str] = None,
        include_deleted: bool = False
    ) -> List[generic_template]:
        """
        List templates with optional filtering.

        Args:
            category: Filter by category.
            type_: Filter by type.
            include_deleted: Include soft-deleted templates.

        Returns:
            List of matching templates.
        """
        query = session.query(generic_template)

        if not include_deleted:
            query = query.filter(generic_template.is_deleted == False)
        if category:
            query = query.filter(generic_template.category == category)
        if type_:
            query = query.filter(generic_template.type == type_)

        return query.all()

    def template_code_from_template(self, template: generic_template) -> str:
        """
        Generate template code string from a template object.

        Args:
            template: Template object.

        Returns:
            Template code string.
        """
        return f"{template.category}/{template.type}/{template.subtype}/{template.version}/"
