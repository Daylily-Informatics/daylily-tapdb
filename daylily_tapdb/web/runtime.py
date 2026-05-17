"""Runtime DB helpers for the reusable DAG router."""

from __future__ import annotations

import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Optional

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import Session, sessionmaker

from daylily_tapdb.aurora.connection import AuroraConnectionBuilder
from daylily_tapdb.cli.db_config import (
    get_admin_settings,
    get_db_config,
)

logger = logging.getLogger(__name__)


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
        logger.warning("Could not set session audit username: %s", exc)


def _require_schema_name(cfg: dict[str, str]) -> str:
    schema_name = str(cfg.get("schema_name") or "").strip()
    if not schema_name:
        raise RuntimeError("TapDB target config is missing required field: schema_name")
    return schema_name


def _set_search_path(session: Session, schema_name: str) -> None:
    """Set per-transaction PostgreSQL search_path for runtime queries."""
    bind = getattr(session, "bind", None)
    dialect = getattr(bind, "dialect", None)
    dialect_name = str(getattr(dialect, "name", "") or "").strip().lower()
    if dialect_name != "postgresql":
        return
    session.execute(
        text("SELECT set_config('search_path', :schema_name, true)"),
        {"schema_name": schema_name},
    )


@dataclass(frozen=True)
class RuntimeBundle:
    config_path: str
    target_name: str
    engine: Engine
    SessionFactory: sessionmaker
    cfg: dict[str, str]
    schema_name: str


class RuntimeDBConnection:
    """Per-request DB wrapper backed by a cached Engine + SessionFactory."""

    def __init__(self, bundle: RuntimeBundle):
        self._bundle = bundle
        self.app_username: Optional[str] = None

    def __enter__(self) -> "RuntimeDBConnection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    @contextmanager
    def session_scope(self, commit: bool = False) -> Generator[Session, None, None]:
        session = self._bundle.SessionFactory()
        trans = session.begin()
        try:
            _set_search_path(session, self._bundle.schema_name)
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
            session.close()


_bundle_lock = threading.Lock()
_bundles: dict[tuple[str, str], RuntimeBundle] = {}


def _create_engine(
    url: URL,
    *,
    config_path: str,
    echo_sql: bool,
) -> Engine:
    settings = get_admin_settings(config_path=config_path)
    pool_size = int(settings.get("db_pool_size") or 5)
    max_overflow = int(settings.get("db_max_overflow") or 10)
    pool_timeout = int(settings.get("db_pool_timeout") or 30)
    pool_recycle = int(settings.get("db_pool_recycle") or 1800)

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
    aws_profile: Optional[str],
    iam_auth: bool,
    secret_arn: Optional[str],
    password: str,
) -> None:
    """Ensure each new pooled Aurora connection gets a fresh credential."""

    def _on_do_connect(dialect, conn_rec, cargs, cparams):
        _ = dialect, conn_rec, cargs
        if iam_auth:
            cparams["password"] = AuroraConnectionBuilder.get_iam_auth_token(
                region=region,
                host=host,
                port=port,
                user=user,
                profile=aws_profile,
            )
            return
        if secret_arn:
            cparams["password"] = AuroraConnectionBuilder.get_secret_password(
                secret_arn=secret_arn,
                region=region,
            )
            return
        if not password:
            raise ValueError(
                "Aurora connection requires a password or secret_arn when iam_auth is disabled"
            )
        cparams["password"] = password

    event.listen(engine, "do_connect", _on_do_connect)


def _build_engine_for_cfg(
    cfg: dict[str, str],
    *,
    config_path: str,
) -> Engine:
    _require_schema_name(cfg)
    engine_type = (cfg.get("engine_type") or "local").strip().lower()
    echo_sql = _parse_bool(os.environ.get("ECHO_SQL"), default=False)

    host = str(cfg["host"]).strip()
    port = int(str(cfg["port"]).strip())
    database = str(cfg["database"]).strip()
    user = str(cfg["user"]).strip()
    password = str(cfg.get("password") or "")
    secret_arn = str(cfg.get("secret_arn") or "").strip() or None

    if engine_type == "aurora":
        region = str(cfg.get("region") or "us-west-2").strip()
        iam_auth = _parse_bool(cfg.get("iam_auth"), default=True)
        aws_profile = (
            str(cfg.get("aws_profile") or "").strip()
            or (os.environ.get("AWS_PROFILE") or "").strip()
            or None
        )
        ca_path = AuroraConnectionBuilder.ensure_ca_bundle()
        url = URL.create(
            "postgresql+psycopg2",
            username=user,
            password=None,
            host=host,
            port=port,
            database=database,
            query={"sslmode": "verify-full", "sslrootcert": str(ca_path)},
        )
        engine = _create_engine(
            url,
            config_path=config_path,
            echo_sql=echo_sql,
        )
        _attach_aurora_password_provider(
            engine,
            region=region,
            host=host,
            port=port,
            user=user,
            aws_profile=aws_profile,
            iam_auth=iam_auth,
            secret_arn=secret_arn,
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
    return _create_engine(
        url,
        config_path=config_path,
        echo_sql=echo_sql,
    )


def get_db(config_path: str) -> RuntimeDBConnection:
    """Return a pooled DB connection wrapper for the DAG router."""

    cfg = get_db_config(config_path=config_path)
    schema_name = _require_schema_name(cfg)
    resolved_config_path = str(cfg.get("config_path") or str(config_path)).strip()
    key = (resolved_config_path, schema_name)

    with _bundle_lock:
        bundle = _bundles.get(key)
        if bundle is None:
            engine = _build_engine_for_cfg(
                cfg,
                config_path=resolved_config_path,
            )
            bundle = RuntimeBundle(
                config_path=resolved_config_path,
                target_name="target",
                engine=engine,
                SessionFactory=sessionmaker(bind=engine),
                cfg=cfg,
                schema_name=schema_name,
            )
            _bundles[key] = bundle

    return RuntimeDBConnection(bundle)


def _clear_runtime_cache_for_tests() -> None:
    """Dispose cached engines and clear runtime state for tests."""

    with _bundle_lock:
        bundles = list(_bundles.values())
        _bundles.clear()

    for bundle in bundles:
        try:
            bundle.engine.dispose()
        except Exception as exc:
            logger.warning(
                "Error disposing DAG runtime engine for %s/%s: %s",
                bundle.config_path,
                bundle.target_name,
                exc,
            )
