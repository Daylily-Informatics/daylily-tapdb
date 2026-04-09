"""Template management for TAPDB.

Phase 2 policy:
- DB templates are the runtime source-of-truth
- Core library methods accept an explicit SQLAlchemy Session
- Do not cache ORM objects across sessions (cache IDs instead)
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from daylily_tapdb.models.template import generic_template

logger = logging.getLogger(__name__)


def _resolve_template_scope(
    session: Session,
    *,
    domain_code: Optional[str] = None,
    issuer_app_code: Optional[str] = None,
) -> tuple[str, str]:
    """Resolve the effective template scope from the active TapDB session.

    Runtime template identity is session-scoped. Explicit scope kwargs are
    accepted only as invariant checks; they may not override the session scope.
    """
    resolved_domain_code, resolved_issuer_app_code = session.execute(
        text(
            "SELECT tapdb_current_domain_code() AS domain_code, "
            "tapdb_current_app_code() AS issuer_app_code"
        )
    ).one()

    session_domain_code = str(resolved_domain_code or "").strip()
    session_issuer_app_code = str(resolved_issuer_app_code or "").strip()

    if not session_domain_code or not session_issuer_app_code:
        raise ValueError(
            "TapDB session scope is not initialized; domain_code and "
            "issuer_app_code must be set on the session before template access."
        )

    if domain_code is not None and str(domain_code).strip() != session_domain_code:
        raise ValueError(
            "domain_code override does not match the active TapDB session scope"
        )
    if (
        issuer_app_code is not None
        and str(issuer_app_code).strip() != session_issuer_app_code
    ):
        raise ValueError(
            "issuer_app_code override does not match the active TapDB session scope"
        )

    return session_domain_code, session_issuer_app_code


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
        issuer_app_code: Optional[str] = None,
    ) -> Optional[generic_template]:
        """
        Get a template by its code string.

        Template code format: {category}/{type}/{subtype}/{version}/
        Example: container/plate/fixed-plate-96/1.0/

        Args:
            template_code: Template code string.
            domain_code: Optional invariant check against the active session scope.
            issuer_app_code: Optional invariant check against the active session
                scope.

        Returns:
            The template object, or None if not found.
        """
        resolved_domain_code, resolved_issuer_app_code = _resolve_template_scope(
            session,
            domain_code=domain_code,
            issuer_app_code=issuer_app_code,
        )
        cache_key = f"{resolved_domain_code}:{resolved_issuer_app_code}:{template_code}"
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
            generic_template.category == category,
            generic_template.type == type_,
            generic_template.subtype == subtype,
            generic_template.version == version,
            generic_template.domain_code == resolved_domain_code,
            generic_template.issuer_app_code == resolved_issuer_app_code,
            generic_template.is_deleted.is_(False),
        )

        template = query.first()

        if template:
            self._template_uid_cache[cache_key] = template.uid
            self._template_euid_cache[template.euid] = template.uid

        return template

    def get_template_by_euid(
        self, session: Session, euid: str
    ) -> Optional[generic_template]:
        """
        Get a template by its EUID.

        Args:
            euid: Template EUID (e.g., GT123).

        Returns:
            The template object, or None if not found.
        """
        resolved_domain_code, resolved_issuer_app_code = _resolve_template_scope(
            session
        )
        cached_uuid = self._template_euid_cache.get(euid)
        if cached_uuid is not None:
            tmpl = session.get(generic_template, cached_uuid)
            if (
                tmpl is not None
                and tmpl.is_deleted is False
                and str(tmpl.domain_code) == resolved_domain_code
                and str(tmpl.issuer_app_code) == resolved_issuer_app_code
            ):
                return tmpl

        tmpl = (
            session.query(generic_template)
            .filter(
                generic_template.euid == euid,
                generic_template.domain_code == resolved_domain_code,
                generic_template.issuer_app_code == resolved_issuer_app_code,
                generic_template.is_deleted.is_(False),
            )
            .first()
        )
        if tmpl is not None:
            self._template_euid_cache[euid] = tmpl.uid
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
        issuer_app_code: Optional[str] = None,
    ) -> List[generic_template]:
        """
        List templates with optional filtering.

        Args:
            category: Filter by category.
            type_: Filter by type.
            include_deleted: Include soft-deleted templates.
            domain_code: Optional invariant check against the active session scope.
            issuer_app_code: Optional invariant check against the active session
                scope.

        Returns:
            List of matching templates.
        """
        resolved_domain_code, resolved_issuer_app_code = _resolve_template_scope(
            session,
            domain_code=domain_code,
            issuer_app_code=issuer_app_code,
        )
        query = session.query(generic_template)

        if not include_deleted:
            query = query.filter(generic_template.is_deleted.is_(False))
        if category:
            query = query.filter(generic_template.category == category)
        if type_:
            query = query.filter(generic_template.type == type_)
        query = query.filter(generic_template.domain_code == resolved_domain_code)
        query = query.filter(
            generic_template.issuer_app_code == resolved_issuer_app_code
        )

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
