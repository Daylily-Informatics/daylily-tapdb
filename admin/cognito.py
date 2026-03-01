"""Cognito runtime resolution helpers for TAPDB Admin.

TAPDB stores only ``cognito_user_pool_id`` in tapdb config. Runtime Cognito
details (app client ID, region, AWS profile) are resolved from daycog env
files in ``~/.config/daycog/*.env`` with preference for pool-scoped
``<pool>.<region>.env`` entries (daycog 0.1.22+).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional
import re

from daylily_tapdb.cli.db_config import get_db_config_for_env


@dataclass(frozen=True)
class DaycogPoolConfig:
    """Resolved Cognito config for a specific user pool."""

    pool_id: str
    app_client_id: str
    region: str
    aws_profile: Optional[str]
    source_file: Path


def _daycog_config_dir() -> Path:
    return Path.home() / ".config" / "daycog"


def _sanitize_filename_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "app"


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        # Allow values written as KEY="VALUE"
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _iter_daycog_env_files() -> list[Path]:
    cfg_dir = _daycog_config_dir()
    if not cfg_dir.exists():
        return []

    files = sorted(cfg_dir.glob("*.env"))
    non_default = [p for p in files if p.name != "default.env"]
    default = [p for p in files if p.name == "default.env"]
    # Prefer pool-named configs over default.env to avoid cross-app collisions.
    return non_default + default


def _score_daycog_env_match(path: Path, values: dict[str, str]) -> tuple[int, str]:
    score = 0
    name = path.name
    region = (values.get("COGNITO_REGION") or values.get("AWS_REGION") or "").strip()
    client_name = (values.get("COGNITO_CLIENT_NAME") or "").strip()

    if name != "default.env":
        score += 10

    if region and name.endswith(f".{region}.env"):
        # Pool-scoped file (<pool>.<region>.env) is selected app context.
        score += 70
    elif region and client_name:
        safe_client = _sanitize_filename_part(client_name)
        if name.endswith(f".{region}.{safe_client}.env"):
            # App-scoped file (<pool>.<region>.<app>.env) is secondary choice.
            score += 60

    return (score, name)


def _resolve_daycog_pool_config(pool_id: str) -> DaycogPoolConfig:
    pool_id = (pool_id or "").strip()
    if not pool_id:
        raise RuntimeError("Empty Cognito pool ID")

    matches: list[tuple[Path, dict[str, str]]] = []
    for env_file in _iter_daycog_env_files():
        values = _read_env_file(env_file)
        if values.get("COGNITO_USER_POOL_ID", "").strip() != pool_id:
            continue
        matches.append((env_file, values))

    if matches:
        matches.sort(
            key=lambda item: _score_daycog_env_match(item[0], item[1]),
            reverse=True,
        )
        env_file, values = matches[0]
        app_client_id = values.get("COGNITO_APP_CLIENT_ID", "").strip()
        region = (
            values.get("COGNITO_REGION", "").strip()
            or values.get("AWS_REGION", "").strip()
        )
        aws_profile = values.get("AWS_PROFILE", "").strip() or None

        if not app_client_id:
            raise RuntimeError(
                f"daycog config {env_file} is missing COGNITO_APP_CLIENT_ID "
                f"for pool {pool_id}"
            )
        if not region:
            raise RuntimeError(
                f"daycog config {env_file} is missing COGNITO_REGION/AWS_REGION "
                f"for pool {pool_id}"
            )

        return DaycogPoolConfig(
            pool_id=pool_id,
            app_client_id=app_client_id,
            region=region,
            aws_profile=aws_profile,
            source_file=env_file,
        )

    raise RuntimeError(
        "No daycog config found for Cognito pool "
        f"{pool_id}. Expected a matching file in "
        f"{_daycog_config_dir()} (run: daycog setup --name <pool-name> ...)."
    )


def resolve_tapdb_pool_config(env_name: Optional[str] = None) -> DaycogPoolConfig:
    """Resolve TAPDB Cognito runtime config for an environment."""
    env_key = (env_name or os.environ.get("TAPDB_ENV") or "dev").strip().lower()
    cfg = get_db_config_for_env(env_key)
    pool_id = (cfg.get("cognito_user_pool_id") or "").strip()
    if not pool_id:
        raise RuntimeError(
            f"TAPDB Cognito is not configured for env '{env_key}'. "
            f"Set environments.{env_key}.cognito_user_pool_id in tapdb config."
        )
    return _resolve_daycog_pool_config(pool_id)


@lru_cache(maxsize=8)
def get_cognito_auth(env_name: Optional[str] = None):
    """Build and cache a CognitoAuth instance for TAPDB Admin."""
    pool_cfg = resolve_tapdb_pool_config(env_name)
    from daylily_cognito import CognitoAuth

    return CognitoAuth(
        region=pool_cfg.region,
        user_pool_id=pool_cfg.pool_id,
        app_client_id=pool_cfg.app_client_id,
        profile=pool_cfg.aws_profile,
    )


def clear_cognito_auth_cache() -> None:
    """Clear cached CognitoAuth instances."""
    get_cognito_auth.cache_clear()
