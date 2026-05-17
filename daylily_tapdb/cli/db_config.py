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
SUPPORTED_TARGET_CONFIG_VERSION = "4"
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
_SAFE_POSTGRES_IDENTIFIER_COMPONENT_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}")


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


def default_database_name_for_namespace(database_name: str, env_name: str = "") -> str:
    """Build the default physical database name for a namespace."""

    base = f"tapdb_{normalize_postgres_identifier_component(database_name)}"
    if str(env_name or "").strip():
        return f"{base}_{normalize_postgres_identifier_component(env_name)}"
    return base


def default_schema_name_for_database(database_name: str, env_name: str = "") -> str:
    """Build the default schema name for a database namespace."""

    return default_database_name_for_namespace(database_name, env_name)


def validate_postgres_identifier_component(value: str, *, field_name: str) -> str:
    """Validate an explicitly configured PostgreSQL identifier component."""

    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError(f"{field_name} is required")
    if not _SAFE_POSTGRES_IDENTIFIER_COMPONENT_RE.fullmatch(raw):
        raise RuntimeError(
            f"{field_name} must be a safe PostgreSQL identifier component "
            "([a-z_][a-z0-9_]{0,62})"
        )
    return raw


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
    version_ok = str(cfg_version).strip() in {
        SUPPORTED_CONFIG_VERSION,
        SUPPORTED_TARGET_CONFIG_VERSION,
    }
    if not version_ok:
        raise RuntimeError(
            f"Unsupported config_version {cfg_version!r}. "
            f"Expected {SUPPORTED_TARGET_CONFIG_VERSION}."
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


def get_db_config(
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
) -> dict[str, str]:
    """Resolve the single explicit TapDB target from the active config."""
    ctx = resolve_context(
        require_keys=True,
        client_id=client_id,
        database_name=database_name,
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
    _validate_target_meta_for_context(root, ctx, resolved_config_path)
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

    file_cfg = root.get("target") if isinstance(root, dict) else None
    if not isinstance(file_cfg, dict) or not file_cfg:
        raise RuntimeError(
            f"TapDB target is not configured in {resolved_config_path}. "
            "Run: tapdb config init --client-id <id> --database-name <name>."
        )

    def _file_str(key: str) -> str | None:
        val = file_cfg.get(key)
        if val is None:
            return None
        return str(val)

    engine_type = (_file_str("engine_type") or "local").strip().lower()
    try:
        schema_name = validate_postgres_identifier_component(
            _file_str("schema_name") or "",
            field_name="target.schema_name",
        )
    except RuntimeError as exc:
        raise RuntimeError(f"Config {resolved_config_path}: {exc}") from exc

    safety = root.get("safety") if isinstance(root.get("safety"), dict) else {}
    safety_tier = str(safety.get("safety_tier") or "local").strip().lower()
    destructive_operations = (
        str(safety.get("destructive_operations") or "confirm_required").strip().lower()
    )
    if safety_tier not in {"local", "shared", "production"}:
        raise RuntimeError(
            "TapDB config safety.safety_tier must be one of: local, shared, production."
        )
    if destructive_operations not in {"blocked", "confirm_required", "allowed"}:
        raise RuntimeError(
            "TapDB config safety.destructive_operations must be one of: "
            "blocked, confirm_required, allowed."
        )

    cfg: dict[str, str] = {
        "engine_type": engine_type,
        "host": _require_file_str(file_cfg, "host", resolved_config_path),
        "port": _require_file_str(file_cfg, "port", resolved_config_path),
        "ui_port": _require_file_str(file_cfg, "ui_port", resolved_config_path),
        "user": _require_file_str(file_cfg, "user", resolved_config_path),
        "password": _file_str("password") or "",
        "secret_arn": _file_str("secret_arn") or "",
        "database": _require_file_str(file_cfg, "database", resolved_config_path),
        "schema_name": schema_name,
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
        "region": _file_str("region") or "",
        "cluster_identifier": _file_str("cluster_identifier") or "",
        "iam_auth": _file_str("iam_auth") or "",
        "ssl": _file_str("ssl") or "",
        "domain_code": _require_file_str(file_cfg, "domain_code", resolved_config_path),
        "safety_tier": safety_tier,
        "destructive_operations": destructive_operations,
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
            _file_str("unix_socket_dir") or str(ctx.postgres_socket_dir()),
        )

        # Local connections must always use localhost to avoid accidental cross-host
        # reuse.
        if str(cfg["host"]).strip().lower() != "localhost":
            raise RuntimeError(
                f"Invalid local host {cfg['host']!r} for explicit target. "
                "Local TAPDB must use host 'localhost'."
            )

    governance = GovernanceContext.load(
        domain_code=str(cfg["domain_code"]),
        owner_repo_name=owner_repo_name,
        domain_registry_path=domain_registry_path,
        prefix_ownership_registry_path=prefix_ownership_registry_path,
    )
    cfg["domain_code"] = governance.domain_code

    return cfg


def _validate_target_meta_for_context(
    root: dict[str, Any], ctx: TapdbContext, resolved_config_path: Path
) -> None:
    meta = root.get("meta")
    if not isinstance(meta, dict):
        raise RuntimeError(f"Config metadata is required in {resolved_config_path}.")
    cfg_version = str(meta.get("config_version") or "").strip()
    if cfg_version != SUPPORTED_TARGET_CONFIG_VERSION:
        raise RuntimeError(
            f"Unsupported config_version {cfg_version!r}. "
            f"Expected {SUPPORTED_TARGET_CONFIG_VERSION} for explicit-target TapDB config."
        )
    _validate_meta_for_context(root, ctx)


def _require_file_str(
    file_cfg: dict[str, Any], key: str, resolved_config_path: Path
) -> str:
    val = file_cfg.get(key)
    if val is None or not str(val).strip():
        raise RuntimeError(f"Config {resolved_config_path} is missing target.{key}.")
    return str(val).strip()


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


def get_admin_settings(
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
) -> dict[str, Any]:
    """Resolve normalized admin/UI settings from the active TapDB config."""
    cfg = get_db_config(
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
        "target_name": "target",
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
