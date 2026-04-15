"""Template management for TAPDB.

Phase 2 policy:
- DB templates are the runtime source-of-truth
- Core library methods accept an explicit SQLAlchemy Session
- Do not cache ORM objects across sessions (cache IDs instead)
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from daylily_tapdb.models.template import generic_template

logger = logging.getLogger(__name__)


def _normalize_domain_code(domain_code: str | None) -> str:
    if domain_code is None:
        raise ValueError("domain_code is required for template lookups")
    normalized = str(domain_code).strip().upper()
    if not normalized:
        raise ValueError("domain_code is required for template lookups")
    return normalized


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
        self._template_uid_cache: Dict[str, Any] = {}
        self._template_euid_cache: Dict[str, Any] = {}

    def get_template(
        self,
        session: Session,
        template_code: str,
        *,
        domain_code: Optional[str] = None,
    ) -> Optional[generic_template]:
        """
        Get a template by its code string.

        Template code format: {category}/{type}/{subtype}/{version}/
        Example: container/plate/fixed-plate-96/1.0/

        Args:
            template_code: Template code string.
            domain_code: Required domain code for the effective identity.

        Returns:
            The template object, or None if not found.
        """
        normalized_domain = _normalize_domain_code(domain_code)
        cache_key = f"{normalized_domain}:{template_code}"
        cached_uid = self._template_uid_cache.get(cache_key)
        if cached_uid is not None:
            tmpl = session.get(generic_template, cached_uid)
            if tmpl is not None and tmpl.is_deleted is False:
                return tmpl

        # Parse template code
        parts = template_code.strip("/").split("/")
        if len(parts) != 4:
            logger.warning(f"Invalid template code format: {template_code}")
            return None

        category, type_, subtype, version = parts

        query = session.query(generic_template).filter(
            generic_template.domain_code == normalized_domain,
            generic_template.category == category,
            generic_template.type == type_,
            generic_template.subtype == subtype,
            generic_template.version == version,
            generic_template.is_deleted.is_(False),
        )

        template = query.first()

        if template:
            self._template_uid_cache[cache_key] = template.uid
            self._template_euid_cache[f"{normalized_domain}:{template.euid}"] = (
                template.uid
            )

        return template

    def get_template_by_euid(
        self,
        session: Session,
        euid: str,
        *,
        domain_code: Optional[str] = None,
    ) -> Optional[generic_template]:
        """
        Get a template by its EUID.

        Args:
            euid: Template EUID (e.g., GT123).

        Returns:
            The template object, or None if not found.
        """
        normalized_domain = _normalize_domain_code(domain_code)
        cache_key = f"{normalized_domain}:{euid}"
        cached_uuid = self._template_euid_cache.get(cache_key)
        if cached_uuid is not None:
            tmpl = session.get(generic_template, cached_uuid)
            if tmpl is not None and tmpl.is_deleted is False:
                return tmpl

        tmpl = (
            session.query(generic_template)
            .filter(
                generic_template.domain_code == normalized_domain,
                generic_template.euid == euid,
                generic_template.is_deleted.is_(False),
            )
            .first()
        )
        if tmpl is not None:
            self._template_euid_cache[cache_key] = tmpl.uid
            self._template_uid_cache[f"{normalized_domain}:{tmpl.category}/{tmpl.type}/{tmpl.subtype}/{tmpl.version}/"] = tmpl.uid
        return tmpl

    def clear_cache(self):
        """Clear the template cache."""
        self._template_uid_cache.clear()
        self._template_euid_cache.clear()

    def list_templates(
        self,
        session: Session,
        category: Optional[str] = None,
        type_: Optional[str] = None,
        include_deleted: bool = False,
        domain_code: Optional[str] = None,
    ) -> List[generic_template]:
        """
        List templates with optional filtering.

        Args:
            category: Filter by category.
            type_: Filter by type.
            include_deleted: Include soft-deleted templates.
            domain_code: Required domain code for the effective identity.

        Returns:
            List of matching templates.
        """
        normalized_domain = _normalize_domain_code(domain_code)
        query = session.query(generic_template)

        if not include_deleted:
            query = query.filter(generic_template.is_deleted.is_(False))
        query = query.filter(generic_template.domain_code == normalized_domain)
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
        return (
            f"{template.category}/{template.type}/"
            f"{template.subtype}/{template.version}/"
        )
