"""Cognito runtime resolution helpers for TAPDB Admin."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

from daylily_tapdb.cli.cognito import REQUIRED_COGNITO_CLIENT_NAME
from daylily_tapdb.cli.db_config import get_db_config_for_env, get_config_path


@dataclass(frozen=True)
class TapdbPoolConfig:
    """Resolved Cognito config for a specific user pool."""

    pool_id: str
    app_client_id: str
    app_client_secret: str
    client_name: str
    region: str
    aws_profile: Optional[str]
    domain: str
    callback_url: str
    logout_url: str
    source_file: Path


def resolve_tapdb_pool_config(env_name: Optional[str] = None) -> TapdbPoolConfig:
    """Resolve TAPDB Cognito runtime config for an environment."""
    env_key = (env_name or os.environ.get("TAPDB_ENV") or "dev").strip().lower()
    cfg = get_db_config_for_env(env_key)
    pool_id = (cfg.get("cognito_user_pool_id") or "").strip()
    if not pool_id:
        raise RuntimeError(
            f"TAPDB Cognito is not configured for env '{env_key}'. "
            f"Set environments.{env_key}.cognito_user_pool_id in tapdb config."
        )

    app_client_id = (cfg.get("cognito_app_client_id") or "").strip()
    region = (cfg.get("cognito_region") or cfg.get("region") or "").strip()
    client_name = (cfg.get("cognito_client_name") or REQUIRED_COGNITO_CLIENT_NAME).strip()
    if client_name != REQUIRED_COGNITO_CLIENT_NAME:
        raise RuntimeError(
            f"TAPDB config must set cognito_client_name={REQUIRED_COGNITO_CLIENT_NAME!r} "
            f"(got {client_name!r})"
        )
    if not app_client_id:
        raise RuntimeError(
            f"TAPDB Cognito app client is not configured for env '{env_key}'. "
            f"Set environments.{env_key}.cognito_app_client_id in tapdb config."
        )
    if not region:
        raise RuntimeError(
            f"TAPDB Cognito region is not configured for env '{env_key}'. "
            f"Set environments.{env_key}.cognito_region in tapdb config."
        )

    return TapdbPoolConfig(
        pool_id=pool_id,
        app_client_id=app_client_id,
        app_client_secret=(cfg.get("cognito_app_client_secret") or "").strip(),
        client_name=client_name,
        region=region,
        aws_profile=(cfg.get("aws_profile") or "").strip() or None,
        domain=(cfg.get("cognito_domain") or "").strip(),
        callback_url=(cfg.get("cognito_callback_url") or "").strip(),
        logout_url=(cfg.get("cognito_logout_url") or "").strip(),
        source_file=get_config_path(),
    )


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
