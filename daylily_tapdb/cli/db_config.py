"""DB connection config loader for CLI."""

from __future__ import annotations

import json
import os
import re
import stat
import warnings
from pathlib import Path
from typing import Any, Optional

from daylily_tapdb.cli.context import (
    CONFIG_FILENAME,
    TapdbContext,
    active_config_path,
    resolve_context,
)
from daylily_tapdb.euid import (
    AUDIT_LOG_PREFIX,
    GENERIC_INSTANCE_LINEAGE_PREFIX,
    GENERIC_TEMPLATE_PREFIX,
    SYSTEM_MESSAGE_PREFIX,
    SYSTEM_USER_PREFIX,
)
from daylily_tapdb.governance import (
    DEFAULT_DOMAIN_REGISTRY_PATH,
    DEFAULT_PREFIX_OWNERSHIP_REGISTRY_PATH,
    GovernanceContext,
    normalize_owner_repo_name,
)

DEFAULT_CONFIG_FILENAME = CONFIG_FILENAME
DEFAULT_TAPDB_POSTGRES_PORT = "5533"
DEFAULT_UI_PORT = "8911"
SUPPORTED_CONFIG_VERSION = "3"
DEFAULT_SUPPORT_EMAIL = "support@daylilyinformatics.com"
DEFAULT_GITHUB_REPO_URL = "https://github.com/Daylily-Informatics/daylily-tapdb"
DEFAULT_ADMIN_SHARED_SESSION_COOKIE = "session"
DEFAULT_ADMIN_SHARED_SESSION_MAX_AGE = 14 * 24 * 60 * 60
DEFAULT_ADMIN_METRICS_QUEUE_MAX = 20000
DEFAULT_ADMIN_METRICS_FLUSH_SECONDS = 1.0
DEFAULT_ADMIN_AUTH_MODE = "tapdb"
DEFAULT_DISABLED_ADMIN_EMAIL = "tapdb-admin@localhost"
DEFAULT_DISABLED_ADMIN_ROLE = "admin"
DEFAULT_DB_POOL_SIZE = 5
DEFAULT_DB_MAX_OVERFLOW = 10
DEFAULT_DB_POOL_TIMEOUT = 30
DEFAULT_DB_POOL_RECYCLE = 1800
_POSTGRES_IDENTIFIER_RE = re.compile(r"[^a-z0-9_]+")


def normalize_postgres_identifier_component(value: str) -> str:
    """Return a PostgreSQL-safe identifier component.

    The full TAPDB database identifier is prefixed with ``tapdb_`` elsewhere, so
    this helper only needs to normalize the namespace/environment fragments.
    """

    normalized = _POSTGRES_IDENTIFIER_RE.sub("_", str(value or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        raise RuntimeError("TapDB database namespace must yield a non-empty identifier")
    return normalized


def default_database_name_for_namespace(database_name: str, env_name: str) -> str:
    """Build the default logical database name for a namespace/env pair."""

    return (
        "tapdb_"
        f"{normalize_postgres_identifier_component(database_name)}_"
        f"{normalize_postgres_identifier_component(env_name)}"
    )


def get_config_paths(
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
) -> list[Path]:
    """Return ordered TAPDB config paths for the active namespace context."""
    override = config_path or active_config_path()
    if override:
        return [Path(override).expanduser().resolve()]

    ctx = resolve_context(
        require_keys=False,
        client_id=client_id,
        database_name=database_name,
    )
    if ctx:
        return [ctx.config_path()]

    raise RuntimeError("TapDB config path is required. Set --config.")


def get_config_path(
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
) -> Path:
    """Return effective config path (existing file preferred)."""
    paths = get_config_paths(
        config_path=config_path,
        client_id=client_id,
        database_name=database_name,
    )
    for p in paths:
        if p.exists():
            return p
    return paths[0]


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    try:
        file_stat = os.stat(path)
        if file_stat.st_mode & (stat.S_IRGRP | stat.S_IROTH):
            warnings.warn(
                f"Config file {path} is readable by other users. Run: chmod 600 {path}",
                stacklevel=2,
            )
    except OSError:
        pass

    raw = path.read_text(encoding="utf-8")

    try:
        import yaml  # type: ignore

        data = yaml.safe_load(raw)
    except ModuleNotFoundError:
        data = json.loads(raw)

    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(
            f"Config root must be a mapping/dict, got {type(data).__name__}"
        )
    return data


def _load_config_with_path(
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
) -> tuple[dict[str, Any], Path, bool]:
    paths = get_config_paths(
        config_path=config_path,
        client_id=client_id,
        database_name=database_name,
    )
    for path in paths:
        if path.exists():
            return _load_yaml_or_json(path), path, True
    return {}, paths[0], False


def load_config() -> dict[str, Any]:
    """Load the active config file if present; return {} if missing."""
    root, _, _ = _load_config_with_path()
    return root


def _validate_meta_for_context(root: dict[str, Any], ctx: TapdbContext) -> None:
    meta = root.get("meta")
    if not isinstance(meta, dict):
        raise RuntimeError(
            "Config metadata is required. Run: tapdb config init "
            f"--client-id {ctx.client_id} --database-name {ctx.database_name}"
        )

    cfg_version = meta.get("config_version")
    version_ok = str(cfg_version).strip() == SUPPORTED_CONFIG_VERSION
    if not version_ok:
        raise RuntimeError(
            f"Unsupported config_version {cfg_version!r}. Expected {SUPPORTED_CONFIG_VERSION}."
        )

    cfg_client = str(meta.get("client_id") or "").strip()
    cfg_db = str(meta.get("database_name") or "").strip()
    try:
        normalize_owner_repo_name(str(meta.get("owner_repo_name") or ""))
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    if cfg_client != ctx.client_id or cfg_db != ctx.database_name:
        raise RuntimeError(
            "Config metadata does not match active namespace. "
            f"Expected client_id={ctx.client_id!r}, "
            f"database_name={ctx.database_name!r}; "
            f"got client_id={cfg_client!r}, database_name={cfg_db!r}."
        )


def get_db_config_for_env(
    env_name: str,
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
) -> dict[str, str]:
    """Resolve DB config for an environment name (dev/test/prod/aurora_*)."""
    env_key = env_name.lower()
    ctx = resolve_context(
        require_keys=True,
        client_id=client_id,
        database_name=database_name,
        env_name=env_key,
        config_path=config_path,
    )

    root, resolved_config_path, config_exists = _load_config_with_path(
        config_path=config_path,
        client_id=ctx.client_id,
        database_name=ctx.database_name,
    )

    if not config_exists:
        raise RuntimeError(
            f"No TAPDB config found at {resolved_config_path}. "
            "Run: tapdb config init --client-id <id> --database-name <name>"
        )
    if ctx:
        _validate_meta_for_context(root, ctx)
    meta = root.get("meta") if isinstance(root, dict) else None
    if not isinstance(meta, dict):
        raise RuntimeError(f"Config metadata is required in {resolved_config_path}.")
    try:
        owner_repo_name = normalize_owner_repo_name(
            str(meta.get("owner_repo_name") or "")
        )
    except ValueError as exc:
        raise RuntimeError(
            f"Config {resolved_config_path} is missing valid meta.owner_repo_name: {exc}"
        ) from exc
    domain_registry_path = Path(
        str(meta.get("domain_registry_path") or DEFAULT_DOMAIN_REGISTRY_PATH)
    ).expanduser()
    prefix_ownership_registry_path = Path(
        str(
            meta.get("prefix_ownership_registry_path")
            or DEFAULT_PREFIX_OWNERSHIP_REGISTRY_PATH
        )
    ).expanduser()

    file_cfg: dict[str, Any] = {}
    if "environments" in root and isinstance(root.get("environments"), dict):
        file_cfg = root["environments"].get(env_key, {}) or {}
    else:
        file_cfg = root.get(env_key, {}) or {}

    if not file_cfg:
        raise RuntimeError(
            f"Environment {env_key!r} is not configured in {resolved_config_path}. "
            f"Run: tapdb config init --env {env_key}"
        )

    def _file_str(key: str) -> str | None:
        val = file_cfg.get(key)
        if val is None:
            return None
        return str(val)

    engine_type = (_file_str("engine_type") or "local").strip().lower()

    cfg: dict[str, str] = {
        "engine_type": engine_type,
        "host": _file_str("host") or "localhost",
        "port": _file_str("port") or DEFAULT_TAPDB_POSTGRES_PORT,
        "ui_port": _file_str("ui_port") or DEFAULT_UI_PORT,
        "user": _file_str("user") or "postgres",
        "password": _file_str("password") or "",
        "secret_arn": _file_str("secret_arn") or "",
        "database": _file_str("database") or f"tapdb_{env_key}",
        "cognito_user_pool_id": _file_str("cognito_user_pool_id") or "",
        "cognito_app_client_id": _file_str("cognito_app_client_id") or "",
        "cognito_app_client_secret": _file_str("cognito_app_client_secret") or "",
        "cognito_client_name": _file_str("cognito_client_name") or "",
        "cognito_region": _file_str("cognito_region") or "",
        "cognito_domain": _file_str("cognito_domain") or "",
        "cognito_callback_url": _file_str("cognito_callback_url") or "",
        "cognito_logout_url": _file_str("cognito_logout_url") or "",
        "support_email": _file_str("support_email") or "",
        "aws_profile": _file_str("aws_profile") or "",
        "domain_code": _file_str("domain_code") or "",
    }

    if ctx:
        cfg["client_id"] = ctx.client_id
        cfg["database_name"] = ctx.database_name
    cfg["owner_repo_name"] = owner_repo_name
    cfg["domain_registry_path"] = str(domain_registry_path)
    cfg["prefix_ownership_registry_path"] = str(prefix_ownership_registry_path)
    cfg["generic_template_euid_prefix"] = GENERIC_TEMPLATE_PREFIX
    cfg["generic_instance_lineage_euid_prefix"] = GENERIC_INSTANCE_LINEAGE_PREFIX
    cfg["audit_log_euid_prefix"] = AUDIT_LOG_PREFIX
    cfg["system_user_euid_prefix"] = SYSTEM_USER_PREFIX
    cfg["system_message_euid_prefix"] = SYSTEM_MESSAGE_PREFIX
    cfg["config_path"] = str(resolved_config_path)

    if engine_type == "aurora":
        cfg.setdefault("region", _file_str("region") or "us-west-2")
        cfg.setdefault("cluster_identifier", _file_str("cluster_identifier") or "")
        cfg.setdefault("iam_auth", _file_str("iam_auth") or "true")
        cfg.setdefault("secret_arn", _file_str("secret_arn") or "")
        cfg.setdefault("ssl", _file_str("ssl") or "true")
    else:
        cfg.setdefault(
            "unix_socket_dir",
            _file_str("unix_socket_dir") or str(ctx.postgres_socket_dir(env_key)),
        )

        # Local connections must always use localhost to avoid accidental cross-host
        # reuse.
        if str(cfg["host"]).strip().lower() != "localhost":
            raise RuntimeError(
                f"Invalid local host {cfg['host']!r} for env {env_key}. "
                "Local TAPDB must use host 'localhost'."
            )
        required_fields = ("port", "ui_port", "domain_code")
        missing_required = [
            field
            for field in required_fields
            if not str(file_cfg.get(field) or "").strip()
        ]
        if missing_required:
            raise RuntimeError(
                f"Config {resolved_config_path} is missing required field(s) for "
                f"env {env_key}: {', '.join(missing_required)}"
            )

    governance = GovernanceContext.load(
        domain_code=str(cfg["domain_code"]),
        owner_repo_name=owner_repo_name,
        domain_registry_path=domain_registry_path,
        prefix_ownership_registry_path=prefix_ownership_registry_path,
    )
    cfg["domain_code"] = governance.domain_code

    return cfg


def _as_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise RuntimeError(f"{field_name} must be a mapping in TapDB config.")
    return value


def _string(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _int(value: Any, *, default: int) -> int:
    raw = _string(value)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float(value: Any, *, default: float) -> float:
    raw = _string(value)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def get_admin_settings_for_env(
    env_name: str,
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
) -> dict[str, Any]:
    """Resolve normalized admin/UI settings from the active TapDB config."""
    env_key = str(env_name or "").strip().lower()
    if not env_key:
        raise RuntimeError("TapDB env name is required to load admin settings.")

    cfg = get_db_config_for_env(
        env_key,
        config_path=config_path,
        client_id=client_id,
        database_name=database_name,
    )
    root, resolved_config_path, config_exists = _load_config_with_path(
        config_path=config_path,
        client_id=cfg.get("client_id"),
        database_name=cfg.get("database_name"),
    )
    if not config_exists:
        raise RuntimeError(f"No TAPDB config found at {resolved_config_path}.")

    admin = _as_mapping(root.get("admin"), field_name="admin")
    footer = _as_mapping(admin.get("footer"), field_name="admin.footer")
    session = _as_mapping(admin.get("session"), field_name="admin.session")
    auth = _as_mapping(admin.get("auth"), field_name="admin.auth")
    disabled_user = _as_mapping(
        auth.get("disabled_user"),
        field_name="admin.auth.disabled_user",
    )
    shared_host = _as_mapping(
        auth.get("shared_host"),
        field_name="admin.auth.shared_host",
    )
    cors = _as_mapping(admin.get("cors"), field_name="admin.cors")
    ui = _as_mapping(admin.get("ui"), field_name="admin.ui")
    tls = _as_mapping(ui.get("tls"), field_name="admin.ui.tls")
    metrics = _as_mapping(admin.get("metrics"), field_name="admin.metrics")

    auth_mode = _string(auth.get("mode"), default=DEFAULT_ADMIN_AUTH_MODE).lower()
    if auth_mode not in {"tapdb", "shared_host", "disabled"}:
        raise RuntimeError(
            "TapDB config admin.auth.mode must be one of: tapdb, shared_host, disabled."
        )

    return {
        "config_path": str(resolved_config_path),
        "env_name": env_key,
        "support_email": _string(
            cfg.get("support_email"),
            default=DEFAULT_SUPPORT_EMAIL,
        ),
        "repo_url": _string(
            footer.get("repo_url"),
            default=DEFAULT_GITHUB_REPO_URL,
        ),
        "session_secret": _string(session.get("secret")),
        "auth_mode": auth_mode,
        "disabled_user_email": _string(
            disabled_user.get("email"),
            default=DEFAULT_DISABLED_ADMIN_EMAIL,
        ).lower(),
        "disabled_user_role": _string(
            disabled_user.get("role"),
            default=DEFAULT_DISABLED_ADMIN_ROLE,
        ).lower(),
        "shared_host_session_secret": _string(shared_host.get("session_secret")),
        "shared_host_session_cookie": _string(
            shared_host.get("session_cookie"),
            default=DEFAULT_ADMIN_SHARED_SESSION_COOKIE,
        ),
        "shared_host_session_max_age_seconds": _int(
            shared_host.get("session_max_age_seconds"),
            default=DEFAULT_ADMIN_SHARED_SESSION_MAX_AGE,
        ),
        "allowed_origins": _string_list(cors.get("allowed_origins")),
        "tls_cert_path": _string(tls.get("cert_path")),
        "tls_key_path": _string(tls.get("key_path")),
        "metrics_enabled": _bool(metrics.get("enabled"), default=True),
        "metrics_queue_max": _int(
            metrics.get("queue_max"),
            default=DEFAULT_ADMIN_METRICS_QUEUE_MAX,
        ),
        "metrics_flush_seconds": _float(
            metrics.get("flush_seconds"),
            default=DEFAULT_ADMIN_METRICS_FLUSH_SECONDS,
        ),
        "db_pool_size": _int(
            admin.get("db_pool_size"),
            default=DEFAULT_DB_POOL_SIZE,
        ),
        "db_max_overflow": _int(
            admin.get("db_max_overflow"),
            default=DEFAULT_DB_MAX_OVERFLOW,
        ),
        "db_pool_timeout": _int(
            admin.get("db_pool_timeout"),
            default=DEFAULT_DB_POOL_TIMEOUT,
        ),
        "db_pool_recycle": _int(
            admin.get("db_pool_recycle"),
            default=DEFAULT_DB_POOL_RECYCLE,
        ),
    }
