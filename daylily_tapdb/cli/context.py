"""Shared TAPDB CLI namespace context helpers.

This module centralizes client/database namespace resolution so commands can
isolate config, runtime state, and local services per namespace.
"""

from __future__ import annotations

import hashlib
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

CONFIG_FILENAME = "tapdb-config.yaml"
RUNTIME_DIRNAME = "runtime"
_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_ACTIVE_CLIENT_ID: Optional[str] = None
_ACTIVE_DATABASE_NAME: Optional[str] = None
_ACTIVE_CONFIG_PATH: Optional[Path] = None


def _runtime_context():
    try:
        from cli_core_yo.runtime import get_context
    except Exception:
        return None

    try:
        return get_context()
    except Exception:
        return None


def _runtime_invocation_value(name: str) -> Optional[str]:
    runtime_context = _runtime_context()
    if runtime_context is None:
        return None
    invocation = getattr(runtime_context, "invocation", None)
    if not isinstance(invocation, dict):
        return None
    value = invocation.get(name)
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


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
    explicit_config_path: Optional[Path] = None

    def namespace_slug(self) -> str:
        return f"{self.client_id}/{self.database_name}"

    def config_dir(self) -> Path:
        if self.explicit_config_path is not None:
            return self.explicit_config_path.parent
        return Path.home() / ".config" / "tapdb" / self.client_id / self.database_name

    def config_path(self) -> Path:
        if self.explicit_config_path is not None:
            return self.explicit_config_path
        return self.config_dir() / CONFIG_FILENAME

    def runtime_dir(self) -> Path:
        return self.config_dir() / RUNTIME_DIRNAME

    def ui_dir(self) -> Path:
        return self.runtime_dir() / "ui"

    def postgres_dir(self) -> Path:
        return self.runtime_dir() / "postgres"

    def postgres_socket_dir(self) -> Path:
        candidate = self.postgres_dir() / "run"
        max_socket_path = 103
        sample_socket = candidate / ".s.PGSQL.65535"
        if len(str(sample_socket)) <= max_socket_path:
            return candidate

        digest = hashlib.sha256(
            f"{self.client_id}:{self.database_name}".encode("utf-8")
        ).hexdigest()[:12]
        return Path(tempfile.gettempdir()) / f"tapdb-pg-{digest}"

    def lock_dir(self) -> Path:
        return self.runtime_dir() / "locks"


def set_cli_context(
    *,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
    config_path: Optional[str | Path] = None,
) -> None:
    """Set process-local CLI context from explicit command-line inputs."""

    global _ACTIVE_CLIENT_ID, _ACTIVE_DATABASE_NAME, _ACTIVE_CONFIG_PATH
    _ACTIVE_CLIENT_ID = _normalize_key(client_id, field_name="client-id")
    _ACTIVE_DATABASE_NAME = _normalize_key(database_name, field_name="database-name")
    if config_path is None or str(config_path).strip() == "":
        _ACTIVE_CONFIG_PATH = None
    else:
        _ACTIVE_CONFIG_PATH = Path(config_path).expanduser().resolve()


def clear_cli_context() -> None:
    """Clear process-local CLI context."""

    set_cli_context()


def active_config_path() -> Optional[Path]:
    """Return the current explicit TapDB config path for this process, if any."""

    if _ACTIVE_CONFIG_PATH is not None:
        return _ACTIVE_CONFIG_PATH

    runtime_context = _runtime_context()
    if runtime_context is None:
        return None

    config_path = getattr(runtime_context, "config_path", None)
    if not config_path:
        return None
    return Path(config_path).expanduser().resolve()


def active_context_overrides() -> dict[str, Optional[str | Path]]:
    """Return the current process-local CLI overrides."""

    runtime_client = _runtime_invocation_value("client_id")
    runtime_database = _runtime_invocation_value("database_name")
    return {
        "client_id": _ACTIVE_CLIENT_ID
        or _normalize_key(runtime_client, field_name="client-id"),
        "database_name": _ACTIVE_DATABASE_NAME
        or _normalize_key(runtime_database, field_name="database-name"),
        "config_path": active_config_path(),
    }


def _load_meta_from_config_path(
    config_path: Path,
) -> tuple[Optional[str], Optional[str]]:
    if not config_path.exists():
        return None, None
    raw = config_path.read_text(encoding="utf-8")
    data: object
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(raw)
    except ModuleNotFoundError:
        import json

        data = json.loads(raw)
    if not isinstance(data, dict):
        return None, None
    meta = data.get("meta")
    if not isinstance(meta, dict):
        return None, None
    return (
        _normalize_key(meta.get("client_id"), field_name="client-id"),
        _normalize_key(meta.get("database_name"), field_name="database-name"),
    )


def resolve_context(
    *,
    require_keys: bool = True,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
    config_path: Optional[str | Path] = None,
) -> Optional[TapdbContext]:
    """Resolve TAPDB namespace context from config metadata.

    The config file's metadata section must contain ``client_id`` and
    ``database_name``.  If the metadata is missing or incomplete, a
    ``RuntimeError`` is raised (unless ``require_keys=False``).
    """

    resolved_config_path: Optional[Path] = None
    raw_config_path = config_path if config_path is not None else active_config_path()
    if raw_config_path is not None and str(raw_config_path).strip():
        resolved_config_path = Path(raw_config_path).expanduser().resolve()

    resolved_client: Optional[str] = None
    resolved_db: Optional[str] = None
    active_overrides = active_context_overrides()

    if resolved_config_path is not None:
        meta_client, meta_db = _load_meta_from_config_path(resolved_config_path)
        resolved_client = meta_client
        resolved_db = meta_db

    if resolved_client is None:
        resolved_client = _normalize_key(
            client_id or active_overrides["client_id"],
            field_name="client-id",
        )
    if resolved_db is None:
        resolved_db = _normalize_key(
            database_name or active_overrides["database_name"],
            field_name="database-name",
        )

    if (
        resolved_config_path is None
        and not require_keys
        and resolved_client
        and resolved_db
    ):
        return TapdbContext(
            client_id=resolved_client,
            database_name=resolved_db,
            explicit_config_path=None,
        )

    if not resolved_client or not resolved_db:
        if not require_keys:
            return None
        if resolved_config_path is not None:
            raise RuntimeError(
                "TapDB config metadata is required to resolve runtime context. "
                f"Config file {resolved_config_path} must contain "
                "'client_id' and 'database_name' in its metadata section."
            )
        raise RuntimeError("TapDB config path is required. Set --config.")

    return TapdbContext(
        client_id=resolved_client,
        database_name=resolved_db,
        explicit_config_path=resolved_config_path,
    )
