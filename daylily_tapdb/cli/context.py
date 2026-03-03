"""Shared TAPDB CLI namespace context helpers.

This module centralizes client/database namespace resolution so commands can
isolate config, runtime state, and local services per namespace.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

CONFIG_FILENAME = "tapdb-config.yaml"
_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _normalize_key(value: Optional[str], *, field_name: str) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if not _KEY_RE.match(normalized):
        raise RuntimeError(
            f"Invalid {field_name!r}: {normalized!r}. "
            "Use letters, numbers, '.', '_' or '-', and start with "
            "a letter or number."
        )
    return normalized


@dataclass(frozen=True)
class TapdbContext:
    """Resolved TAPDB namespace context."""

    client_id: str
    database_name: str
    env_name: Optional[str] = None

    def namespace_slug(self) -> str:
        return f"{self.client_id}/{self.database_name}"

    def config_dir(self) -> Path:
        return Path.home() / ".config" / "tapdb" / self.client_id / self.database_name

    def config_path(self) -> Path:
        return self.config_dir() / CONFIG_FILENAME

    def runtime_dir(self, env_name: Optional[str] = None) -> Path:
        resolved_env = (env_name or self.env_name or "").strip()
        if not resolved_env:
            raise RuntimeError("Environment name is required to resolve runtime_dir")
        return self.config_dir() / resolved_env

    def ui_dir(self, env_name: Optional[str] = None) -> Path:
        return self.runtime_dir(env_name) / "ui"

    def postgres_dir(self, env_name: Optional[str] = None) -> Path:
        return self.runtime_dir(env_name) / "postgres"

    def lock_dir(self, env_name: Optional[str] = None) -> Path:
        return self.runtime_dir(env_name) / "locks"


def resolve_context(
    *,
    require_keys: bool = True,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
    env_name: Optional[str] = None,
) -> Optional[TapdbContext]:
    """Resolve TAPDB namespace context from args/env.

    Precedence:
    1. Explicit function args
    2. Environment variables
    """

    resolved_client = _normalize_key(
        client_id if client_id is not None else os.environ.get("TAPDB_CLIENT_ID"),
        field_name="client-id",
    )
    resolved_db = _normalize_key(
        database_name
        if database_name is not None
        else os.environ.get("TAPDB_DATABASE_NAME"),
        field_name="database-name",
    )

    if not resolved_client or not resolved_db:
        if not require_keys:
            return None
        missing: list[str] = []
        if not resolved_client:
            missing.append("client-id")
        if not resolved_db:
            missing.append("database-name")
        missing_text = ", ".join(missing)
        raise RuntimeError(
            "Missing TAPDB namespace key(s): "
            f"{missing_text}. Set --client-id/--database-name or "
            "TAPDB_CLIENT_ID/TAPDB_DATABASE_NAME."
        )

    resolved_env = (env_name or os.environ.get("TAPDB_ENV") or "").strip() or None
    return TapdbContext(
        client_id=resolved_client,
        database_name=resolved_db,
        env_name=resolved_env,
    )

