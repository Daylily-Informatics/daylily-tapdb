"""Admin GUI database pooling helpers.

The admin GUI used to create a fresh SQLAlchemy Engine per request and dispose it
on exit. That defeats pooling (and is extremely slow for Aurora due to SSL/IAM
handshake costs).

This module provides:
- A single shared Engine per process (per env) with proper pooling.
- Per-request connection wrappers that provide `session_scope()` and audit
  attribution via `SET LOCAL session.current_username`.
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from collections.abc import Callable
from typing import Generator, Optional

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import Session, sessionmaker

from daylily_tapdb.aurora.connection import AuroraConnectionBuilder
from daylily_tapdb.cli.db_config import get_db_config_for_env

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using %s", name, raw, default)
        return default


def _parse_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _audit_username_for_session(value: Optional[str]) -> str:
    return (value or "").strip() or "unknown"


def _set_audit_username(session: Session, username: Optional[str]) -> None:
    """Set per-transaction audit username for TAPDB triggers (best-effort)."""
    try:
        session.execute(
            text("SET LOCAL session.current_username = :username"),
            {"username": _audit_username_for_session(username)},
        )
    except Exception as exc:
        # Don't break requests for audit attribution issues.
        logger.warning("Could not set session audit username: %s", exc)


@dataclass(frozen=True)
class EngineBundle:
    env_name: str
    engine: Engine
    SessionFactory: sessionmaker
    cfg: dict[str, str]


class AdminDBConnection:
    """Per-request DB wrapper backed by a shared Engine + SessionFactory."""

    def __init__(self, bundle: EngineBundle):
        self._bundle = bundle
        self.app_username: Optional[str] = None

    def __enter__(self) -> "AdminDBConnection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # Intentionally do not dispose the pooled engine here.
        return False

    @contextmanager
    def session_scope(self, commit: bool = False) -> Generator[Session, None, None]:
        from admin.db_metrics import db_username_var

        session = self._bundle.SessionFactory()
        trans = session.begin()
        token = db_username_var.set(_audit_username_for_session(self.app_username))
        try:
            _set_audit_username(session, self.app_username)
            yield session
            if commit:
                trans.commit()
            else:
                trans.rollback()
        except Exception:
            trans.rollback()
            raise
        finally:
            db_username_var.reset(token)
            session.close()

    def close(self) -> None:
        """No-op: pooled engine lifecycle is process-scoped."""
        return None


_engine_lock = threading.Lock()
_bundles_by_env: dict[str, EngineBundle] = {}


def _create_engine(url: URL, *, echo_sql: bool) -> Engine:
    pool_size = _env_int("TAPDB_DB_POOL_SIZE", 5)
    max_overflow = _env_int("TAPDB_DB_MAX_OVERFLOW", 10)
    pool_timeout = _env_int("TAPDB_DB_POOL_TIMEOUT", 30)
    pool_recycle = _env_int("TAPDB_DB_POOL_RECYCLE", 1800)

    return create_engine(
        url,
        echo=echo_sql,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        pool_pre_ping=True,
    )


def _attach_aurora_password_provider(
    engine: Engine,
    *,
    region: str,
    host: str,
    port: int,
    user: str,
    iam_auth: bool,
    password: str,
) -> Callable:
    """Ensure each new pool connection gets a fresh Aurora credential."""

    def _on_do_connect(dialect, conn_rec, cargs, cparams):
        _ = dialect, conn_rec, cargs
        if iam_auth:
            cparams["password"] = AuroraConnectionBuilder.get_iam_auth_token(
                region=region,
                host=host,
                port=port,
                user=user,
            )
        else:
            if not password:
                raise ValueError(
                    "Aurora connection requires a password when iam_auth is disabled"
                )
            cparams["password"] = password

    event.listen(engine, "do_connect", _on_do_connect)
    return _on_do_connect


def _build_engine_for_cfg(cfg: dict[str, str]) -> Engine:
    engine_type = (cfg.get("engine_type") or "local").strip().lower()
    echo_sql = _parse_bool(os.environ.get("ECHO_SQL"), default=False)

    host = str(cfg["host"]).strip()
    port = int(str(cfg["port"]).strip())
    database = str(cfg["database"]).strip()
    user = str(cfg["user"]).strip()
    password = str(cfg.get("password") or "")

    if engine_type == "aurora":
        region = str(cfg.get("region") or "us-west-2").strip()
        iam_auth = _parse_bool(cfg.get("iam_auth"), default=True)
        ca_path = AuroraConnectionBuilder.ensure_ca_bundle()
        url = URL.create(
            "postgresql+psycopg2",
            username=user,
            password=None,  # provided via do_connect for IAM or static password
            host=host,
            port=port,
            database=database,
            query={"sslmode": "verify-full", "sslrootcert": str(ca_path)},
        )
        engine = _create_engine(url, echo_sql=echo_sql)
        _attach_aurora_password_provider(
            engine,
            region=region,
            host=host,
            port=port,
            user=user,
            iam_auth=iam_auth,
            password=password,
        )
        return engine

    url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password or None,
        host=host,
        port=port,
        database=database,
    )
    return _create_engine(url, echo_sql=echo_sql)


def get_engine_bundle(env_name: str) -> EngineBundle:
    """Return (and cache) the shared engine bundle for an env."""
    env = (env_name or "dev").strip().lower()
    with _engine_lock:
        cached = _bundles_by_env.get(env)
        if cached is not None:
            return cached

        cfg = get_db_config_for_env(env)
        engine = _build_engine_for_cfg(cfg)
        from admin.db_metrics import maybe_install_engine_metrics

        maybe_install_engine_metrics(engine, env_name=env)
        SessionFactory = sessionmaker(bind=engine)
        bundle = EngineBundle(env_name=env, engine=engine, SessionFactory=SessionFactory, cfg=cfg)
        _bundles_by_env[env] = bundle
        return bundle


def get_db_connection(env_name: str) -> AdminDBConnection:
    """Create a per-request AdminDBConnection backed by the cached Engine."""
    return AdminDBConnection(get_engine_bundle(env_name))


def dispose_all_engines() -> None:
    """Dispose all cached engines (process shutdown)."""
    with _engine_lock:
        bundles = list(_bundles_by_env.values())
        _bundles_by_env.clear()

    for bundle in bundles:
        try:
            bundle.engine.dispose()
        except Exception as exc:
            logger.warning("Error disposing engine for env %s: %s", bundle.env_name, exc)


def _clear_engine_cache_for_tests() -> None:
    """Test helper: clear cached engines and dispose them."""
    dispose_all_engines()
