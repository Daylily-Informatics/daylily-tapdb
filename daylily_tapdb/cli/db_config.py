"""DB connection config loader for CLI.

Namespace-first search order:
1) TAPDB_CONFIG_PATH (explicit override)
2) ~/.config/tapdb/<client-id>/<database-name>/tapdb-config.yaml

Legacy fallback (only when strict namespace mode is disabled):
3) ~/.config/tapdb/tapdb-config-<database_name>.yaml (legacy)
4) ~/.config/tapdb/tapdb-config.yaml (legacy)
5) ./config/tapdb-config-<database_name>.yaml (legacy repo-local)
6) ./config/tapdb-config.yaml (legacy repo-local)
"""

from __future__ import annotations

import json
import os
import stat
import warnings
from pathlib import Path
from typing import Any, Optional

from daylily_tapdb.cli.context import CONFIG_FILENAME, TapdbContext, resolve_context

DEFAULT_CONFIG_FILENAME = CONFIG_FILENAME
DEFAULT_TAPDB_POSTGRES_PORT = "5533"
DEFAULT_UI_PORT = "8911"


def _strict_namespace_enabled() -> bool:
    raw = (os.environ.get("TAPDB_STRICT_NAMESPACE") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _legacy_default_config_path() -> Path:
    return Path.home() / ".config" / "tapdb" / DEFAULT_CONFIG_FILENAME


def _legacy_repo_config_path() -> Path:
    return Path.cwd() / "config" / DEFAULT_CONFIG_FILENAME


def _legacy_database_name_for_config() -> str | None:
    val = os.environ.get("TAPDB_DATABASE_NAME")
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s


def _legacy_scoped_config_paths(database_name: str) -> list[Path]:
    suffix = f"tapdb-config-{database_name}.yaml"
    return [
        Path.home() / ".config" / "tapdb" / suffix,
        Path.cwd() / "config" / suffix,
    ]


def get_legacy_config_paths(*, database_name: Optional[str] = None) -> list[Path]:
    """Return legacy TAPDB config search paths (for migration tooling)."""
    default_user = _legacy_default_config_path()
    default_repo = _legacy_repo_config_path()
    db_name = database_name or _legacy_database_name_for_config()
    if db_name:
        user_scoped, repo_scoped = _legacy_scoped_config_paths(db_name)
        return [user_scoped, default_user, repo_scoped, default_repo]
    return [default_user, default_repo]


def get_config_paths() -> list[Path]:
    """Return ordered TAPDB config paths for the active namespace context."""
    override = os.environ.get("TAPDB_CONFIG_PATH")
    if override:
        return [Path(override).expanduser()]

    ctx = resolve_context(require_keys=False)
    if ctx:
        return [ctx.config_path()]

    if _strict_namespace_enabled():
        raise RuntimeError(
            "TAPDB namespace is required in strict mode. "
            "Set --client-id/--database-name or "
            "TAPDB_CLIENT_ID/TAPDB_DATABASE_NAME."
        )

    return get_legacy_config_paths()


def get_config_path() -> Path:
    """Return effective config path (existing file preferred)."""
    paths = get_config_paths()
    for p in paths:
        if p.exists():
            return p
    return paths[0]


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    try:
        file_stat = os.stat(path)
        if file_stat.st_mode & (stat.S_IRGRP | stat.S_IROTH):
            warnings.warn(
                f"Config file {path} is readable by other users. "
                f"Run: chmod 600 {path}",
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


def _load_config_with_path() -> tuple[dict[str, Any], Path, bool]:
    paths = get_config_paths()
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
    version_ok = str(cfg_version).strip() == "2"
    if not version_ok:
        raise RuntimeError(
            f"Unsupported config_version {cfg_version!r}. Expected 2."
        )

    cfg_client = str(meta.get("client_id") or "").strip()
    cfg_db = str(meta.get("database_name") or "").strip()
    if cfg_client != ctx.client_id or cfg_db != ctx.database_name:
        raise RuntimeError(
            "Config metadata does not match active namespace. "
            f"Expected client_id={ctx.client_id!r}, "
            f"database_name={ctx.database_name!r}; "
            f"got client_id={cfg_client!r}, database_name={cfg_db!r}."
        )


def get_db_config_for_env(env_name: str) -> dict[str, str]:
    """Resolve DB config for an environment name (dev/test/prod/aurora_*)."""
    env_key = env_name.lower()
    env_prefix = f"TAPDB_{env_key.upper()}_"
    strict = _strict_namespace_enabled()
    ctx = resolve_context(require_keys=strict, env_name=env_key)

    root, config_path, config_exists = _load_config_with_path()

    if strict:
        if not config_exists:
            raise RuntimeError(
                f"No TAPDB config found at {config_path}. "
                "Run: tapdb config init --client-id <id> --database-name <name>"
            )
        if ctx:
            _validate_meta_for_context(root, ctx)

    file_cfg: dict[str, Any] = {}
    if "environments" in root and isinstance(root.get("environments"), dict):
        file_cfg = root["environments"].get(env_key, {}) or {}
    else:
        file_cfg = root.get(env_key, {}) or {}

    if strict and not file_cfg:
        raise RuntimeError(
            f"Environment {env_key!r} is not configured in {config_path}. "
            f"Run: tapdb config init --env {env_key}"
        )

    def _file_str(key: str) -> str | None:
        val = file_cfg.get(key)
        if val is None:
            return None
        return str(val)

    engine_type = os.environ.get(
        f"{env_prefix}ENGINE_TYPE", _file_str("engine_type") or "local"
    ).strip().lower()

    cfg: dict[str, str] = {
        "engine_type": engine_type,
        "host": os.environ.get(
            f"{env_prefix}HOST",
            os.environ.get("PGHOST", _file_str("host") or "localhost"),
        ),
        "port": os.environ.get(
            f"{env_prefix}PORT",
            os.environ.get("PGPORT", _file_str("port") or DEFAULT_TAPDB_POSTGRES_PORT),
        ),
        "ui_port": os.environ.get(
            f"{env_prefix}UI_PORT",
            _file_str("ui_port") or DEFAULT_UI_PORT,
        ),
        "user": os.environ.get(
            f"{env_prefix}USER",
            os.environ.get(
                "PGUSER", _file_str("user") or os.environ.get("USER", "postgres")
            ),
        ),
        "password": os.environ.get(
            f"{env_prefix}PASSWORD",
            os.environ.get("PGPASSWORD", _file_str("password") or ""),
        ),
        "database": os.environ.get(
            f"{env_prefix}DATABASE", _file_str("database") or f"tapdb_{env_key}"
        ),
        "cognito_user_pool_id": os.environ.get(
            f"{env_prefix}COGNITO_USER_POOL_ID",
            _file_str("cognito_user_pool_id") or "",
        ),
        "audit_log_euid_prefix": os.environ.get(
            f"{env_prefix}AUDIT_LOG_EUID_PREFIX",
            _file_str("audit_log_euid_prefix") or "",
        ),
        "support_email": os.environ.get(
            f"{env_prefix}SUPPORT_EMAIL",
            _file_str("support_email") or "",
        ),
        "aws_profile": os.environ.get(
            f"{env_prefix}AWS_PROFILE",
            os.environ.get("AWS_PROFILE", _file_str("aws_profile") or ""),
        ),
    }

    if ctx:
        cfg["client_id"] = ctx.client_id
        cfg["database_name"] = ctx.database_name
    cfg["config_path"] = str(config_path)

    if engine_type == "aurora":
        cfg["region"] = os.environ.get(
            f"{env_prefix}REGION", _file_str("region") or "us-west-2"
        )
        cfg["cluster_identifier"] = os.environ.get(
            f"{env_prefix}CLUSTER_IDENTIFIER",
            _file_str("cluster_identifier") or "",
        )
        cfg["iam_auth"] = os.environ.get(
            f"{env_prefix}IAM_AUTH", _file_str("iam_auth") or "true"
        )
        cfg["ssl"] = os.environ.get(f"{env_prefix}SSL", _file_str("ssl") or "true")
    else:
        # Local connections must always use localhost to avoid accidental cross-host
        # reuse.
        if str(cfg["host"]).strip().lower() != "localhost":
            raise RuntimeError(
                f"Invalid local host {cfg['host']!r} for env {env_key}. "
                "Local TAPDB must use host 'localhost'."
            )
        if strict:
            required_fields = ("port", "ui_port")
            missing_required = [
                field
                for field in required_fields
                if not str(file_cfg.get(field) or "").strip()
            ]
            if missing_required:
                raise RuntimeError(
                    f"Config {config_path} is missing required field(s) for "
                    f"env {env_key}: {', '.join(missing_required)}"
                )

    return cfg
