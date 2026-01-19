"""Template management for TAPDB."""
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

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

    def __init__(self, db, config_path: Optional[Path] = None):
        """
        Initialize template manager.

        Args:
            db: TAPDBConnection instance.
            config_path: Path to template configuration directory.
        """
        self.db = db
        self.config_path = config_path
        self._template_cache: Dict[str, generic_template] = {}

    def get_template(self, template_code: str) -> Optional[generic_template]:
        """
        Get a template by its code string.

        Template code format: {super_type}/{btype}/{b_sub_type}/{version}/
        Example: container/plate/fixed-plate-96/1.0/

        Args:
            template_code: Template code string.

        Returns:
            The template object, or None if not found.
        """
        # Check cache first
        if template_code in self._template_cache:
            return self._template_cache[template_code]

        # Parse template code
        parts = template_code.strip("/").split("/")
        if len(parts) != 4:
            logger.warning(f"Invalid template code format: {template_code}")
            return None

        super_type, btype, b_sub_type, version = parts

        # Query database
        session = self.db.get_session()
        template = session.query(generic_template).filter(
            generic_template.super_type == super_type,
            generic_template.btype == btype,
            generic_template.b_sub_type == b_sub_type,
            generic_template.version == version,
            generic_template.is_deleted == False
        ).first()

        if template:
            self._template_cache[template_code] = template

        return template

    def get_template_by_euid(self, euid: str) -> Optional[generic_template]:
        """
        Get a template by its EUID.

        Args:
            euid: Template EUID (e.g., GT123).

        Returns:
            The template object, or None if not found.
        """
        session = self.db.get_session()
        return session.query(generic_template).filter(
            generic_template.euid == euid,
            generic_template.is_deleted == False
        ).first()

    def clear_cache(self):
        """Clear the template cache."""
        self._template_cache.clear()

    def list_templates(
        self,
        super_type: Optional[str] = None,
        btype: Optional[str] = None,
        include_deleted: bool = False
    ) -> List[generic_template]:
        """
        List templates with optional filtering.

        Args:
            super_type: Filter by super_type.
            btype: Filter by btype.
            include_deleted: Include soft-deleted templates.

        Returns:
            List of matching templates.
        """
        session = self.db.get_session()
        query = session.query(generic_template)

        if not include_deleted:
            query = query.filter(generic_template.is_deleted == False)
        if super_type:
            query = query.filter(generic_template.super_type == super_type)
        if btype:
            query = query.filter(generic_template.btype == btype)

        return query.all()

    def template_code_from_template(self, template: generic_template) -> str:
        """
        Generate template code string from a template object.

        Args:
            template: Template object.

        Returns:
            Template code string.
        """
        return f"{template.super_type}/{template.btype}/{template.b_sub_type}/{template.version}/"
