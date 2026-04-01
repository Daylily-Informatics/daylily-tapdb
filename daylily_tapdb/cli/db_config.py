"""DB connection config loader for CLI."""

from __future__ import annotations

import json
import os
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
    normalize_euid_client_code,
    resolve_client_scoped_core_prefix,
)

DEFAULT_CONFIG_FILENAME = CONFIG_FILENAME
DEFAULT_TAPDB_POSTGRES_PORT = "5533"
DEFAULT_UI_PORT = "8911"


def _legacy_default_config_path() -> Path:
    return Path.home() / ".config" / "tapdb" / DEFAULT_CONFIG_FILENAME


def _legacy_repo_config_path() -> Path:
    return Path.cwd() / "config" / DEFAULT_CONFIG_FILENAME


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
    db_name = database_name
    if db_name:
        user_scoped, repo_scoped = _legacy_scoped_config_paths(db_name)
        return [user_scoped, default_user, repo_scoped, default_repo]
    return [default_user, default_repo]


def get_config_paths(
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
    allow_namespace_fallback: bool = False,
) -> list[Path]:
    """Return ordered TAPDB config paths for the active namespace context."""
    override = config_path or active_config_path() or os.environ.get("TAPDB_CONFIG_PATH")
    if override:
        return [Path(override).expanduser().resolve()]

    ctx = resolve_context(
        require_keys=False,
        client_id=client_id,
        database_name=database_name,
        allow_namespace_fallback=True,
    )
    if ctx:
        return [ctx.config_path()]

    if not allow_namespace_fallback:
        raise RuntimeError("TapDB config path is required. Set --config.")

    raise RuntimeError("TapDB namespace is required. Set --client-id/--database-name.")


def get_config_path(
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
    allow_namespace_fallback: bool = False,
) -> Path:
    """Return effective config path (existing file preferred)."""
    paths = get_config_paths(
        config_path=config_path,
        client_id=client_id,
        database_name=database_name,
        allow_namespace_fallback=allow_namespace_fallback,
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


def _env_override(env_key: str, field_name: str) -> str | None:
    value = os.environ.get(f"TAPDB_{env_key.upper()}_{field_name.upper()}")
    if value is None:
        return None
    return str(value)


def _load_config_with_path(
    *,
    config_path: Optional[str | Path] = None,
    client_id: Optional[str] = None,
    database_name: Optional[str] = None,
    allow_namespace_fallback: bool = False,
) -> tuple[dict[str, Any], Path, bool]:
    paths = get_config_paths(
        config_path=config_path,
        client_id=client_id,
        database_name=database_name,
        allow_namespace_fallback=allow_namespace_fallback,
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
    version_ok = str(cfg_version).strip() == "2"
    if not version_ok:
        raise RuntimeError(f"Unsupported config_version {cfg_version!r}. Expected 2.")

    cfg_client = str(meta.get("client_id") or "").strip()
    cfg_db = str(meta.get("database_name") or "").strip()
    try:
        normalize_euid_client_code(meta.get("euid_client_code"))
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
    allow_namespace_fallback: bool = False,
) -> dict[str, str]:
    """Resolve DB config for an environment name (dev/test/prod/aurora_*)."""
    env_key = env_name.lower()
    use_namespace_fallback = True if config_path is None else allow_namespace_fallback
    ctx = resolve_context(
        require_keys=True,
        client_id=client_id,
        database_name=database_name,
        env_name=env_key,
        config_path=config_path,
        allow_namespace_fallback=use_namespace_fallback,
    )

    root, resolved_config_path, config_exists = _load_config_with_path(
        config_path=config_path,
        client_id=ctx.client_id,
        database_name=ctx.database_name,
        allow_namespace_fallback=use_namespace_fallback,
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
        raise RuntimeError(
            f"Config metadata is required in {resolved_config_path}."
        )
    try:
        euid_client_code = normalize_euid_client_code(meta.get("euid_client_code"))
    except ValueError as exc:
        raise RuntimeError(
            f"Config {resolved_config_path} is missing valid meta.euid_client_code: {exc}"
        ) from exc
    core_euid_prefix = resolve_client_scoped_core_prefix(euid_client_code)

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
        "database": _file_str("database") or f"tapdb_{env_key}",
        "cognito_user_pool_id": _file_str("cognito_user_pool_id") or "",
        "cognito_app_client_id": _file_str("cognito_app_client_id") or "",
        "cognito_app_client_secret": _file_str("cognito_app_client_secret") or "",
        "cognito_client_name": _file_str("cognito_client_name") or "",
        "cognito_region": _file_str("cognito_region") or "",
        "cognito_domain": _file_str("cognito_domain") or "",
        "cognito_callback_url": _file_str("cognito_callback_url") or "",
        "cognito_logout_url": _file_str("cognito_logout_url") or "",
        "audit_log_euid_prefix": _file_str("audit_log_euid_prefix") or "",
        "support_email": _file_str("support_email") or "",
        "aws_profile": _file_str("aws_profile") or "",
    }

    if ctx:
        cfg["client_id"] = ctx.client_id
        cfg["database_name"] = ctx.database_name
    cfg["euid_client_code"] = euid_client_code
    cfg["core_euid_prefix"] = core_euid_prefix
    cfg["config_path"] = str(resolved_config_path)

    explicit_config_mode = bool(config_path or active_config_path())
    if not explicit_config_mode:
        override_fields = [
            "host",
            "port",
            "ui_port",
            "user",
            "password",
            "database",
            "audit_log_euid_prefix",
            "region",
            "cluster_identifier",
            "iam_auth",
            "ssl",
            "unix_socket_dir",
        ]
        for field_name in override_fields:
            override = _env_override(env_key, field_name)
            if override is not None:
                cfg[field_name] = override

    if engine_type == "aurora":
        cfg.setdefault("region", _file_str("region") or "us-west-2")
        cfg.setdefault("cluster_identifier", _file_str("cluster_identifier") or "")
        cfg.setdefault("iam_auth", _file_str("iam_auth") or "true")
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
        required_fields = ("port", "ui_port")
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

    audit_prefix = str(cfg.get("audit_log_euid_prefix") or "").strip().upper()
    if audit_prefix != core_euid_prefix:
        raise RuntimeError(
            f"Config {resolved_config_path} env {env_key!r} must set "
            f"audit_log_euid_prefix={core_euid_prefix!r} to match "
            "the namespace-scoped TapDB core prefix."
        )

    return cfg
