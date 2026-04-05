"""TAPDB Database Connection Manager.

Moonshot Phase 2 policy:
- No surprise commits inside the library
- Callers control transaction boundaries
- Audit username is set per-transaction using `SET LOCAL session.current_username`
- Domain code is set per-session using `session.current_domain_code`
- Issuer app code is set per-session using `session.current_app_code`

Recommended usage:

    with TAPDBConnection() as conn:
        with conn.session_scope(commit=False) as session:
            rows = session.query(...).all()

For write operations:

    with TAPDBConnection() as conn:
        with conn.session_scope(commit=True) as session:
            session.add(obj)
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker

from daylily_tapdb.euid import resolve_runtime_domain_code

logger = logging.getLogger(__name__)
DEFAULT_TAPDB_POSTGRES_PORT = "5533"


class TAPDBConnection:
    """
    TAPDB Database Connection Manager.

    Usage (Phase 2 moonshot):
        # Read-only / query usage
        with TAPDBConnection() as conn:
            with conn.session_scope(commit=False) as session:
                rows = session.query(...).all()

        # Write usage (explicit opt-in commit)
        with TAPDBConnection() as conn:
            with conn.session_scope(commit=True) as session:
                session.add(obj)
                # commits on success, rolls back on exception
    """

    def __init__(
        self,
        db_url: Optional[str] = None,
        db_url_prefix: str = "postgresql://",
        db_hostname: Optional[str] = None,
        db_pass: Optional[str] = None,
        db_user: Optional[str] = None,
        db_name: str = "tapdb",
        app_username: Optional[str] = None,
        echo_sql: Optional[bool] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 1800,
        engine_type: Optional[str] = None,
        region: str = "us-west-2",
        iam_auth: bool = True,
        secret_arn: Optional[str] = None,
        domain_code: Optional[str] = None,
        issuer_app_code: Optional[str] = None,
    ):
        """
        Initialize database connection.

        Args:
            db_url: Full database URL (overrides other db_* params)
            db_url_prefix: Database URL prefix (default: postgresql://)
            db_hostname: Database host:port (default: localhost:5533)
            db_pass: Database password
            db_user: Database user (default: $USER)
            db_name: Database name (default: tapdb)
            app_username: Username for audit logging (default: $USER)
            echo_sql: Log SQL statements (default: $ECHO_SQL env var)
            pool_size: Connection pool size
            max_overflow: Max connections above pool_size
            pool_timeout: Seconds to wait for connection
            pool_recycle: Seconds before connection recycled
            engine_type: Connection type — None or "local" for local PG,
                "aurora" for Aurora PostgreSQL with SSL + IAM auth.
            region: AWS region (only used when engine_type="aurora").
            iam_auth: Use IAM database authentication (Aurora only).
            secret_arn: Secrets Manager ARN for password (Aurora fallback).
            domain_code: Domain code for session scoping (1-4 chars). Required.
            issuer_app_code: Issuer app code for session scoping (1-4 chars). Required.
        """
        self.logger = logging.getLogger(__name__ + ".TAPDBConnection")

        # Resolve defaults from environment
        db_user = db_user or os.environ.get("USER", "tapdb")
        self.app_username = app_username or os.environ.get("USER", "tapdb_orm")
        self.domain_code = domain_code or resolve_runtime_domain_code()
        self.issuer_app_code = issuer_app_code or os.environ.get("TAPDB_APP_CODE")
        if not self.domain_code:
            raise ValueError(
                "domain_code is required. Set MERIDIAN_DOMAIN_CODE env var or pass domain_code= param."
            )
        if not self.issuer_app_code:
            raise ValueError(
                "issuer_app_code is required. Set TAPDB_APP_CODE env var or pass issuer_app_code= param."
            )

        if echo_sql is None:
            echo_env = os.environ.get("ECHO_SQL", "").lower()
            echo_sql = echo_env in ("true", "1", "yes")

        # Build database URL
        if db_url:
            self._db_url = db_url
        elif engine_type == "aurora":
            from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

            # For Aurora, db_hostname must be the cluster endpoint (host:port
            # or just host).
            if not db_hostname:
                raise ValueError(
                    "db_hostname (Aurora cluster endpoint) is required "
                    "when engine_type='aurora'."
                )
            # Split host:port if provided together
            if ":" in db_hostname:
                host, port_str = db_hostname.rsplit(":", 1)
                port = int(port_str)
            else:
                host = db_hostname
                port = 5432

            self._db_url = AuroraConnectionBuilder.build_connection_url(
                host=host,
                port=port,
                database=db_name,
                user=db_user,
                region=region,
                iam_auth=iam_auth,
                secret_arn=secret_arn,
                password=db_pass,
            )
        else:
            # Local PostgreSQL (original behaviour)
            db_hostname = db_hostname or f"localhost:{DEFAULT_TAPDB_POSTGRES_PORT}"
            db_pass = db_pass or ""
            self._db_url = f"{db_url_prefix}{db_user}:{db_pass}@{db_hostname}/{db_name}"

        # Create engine with connection pooling
        self.engine = create_engine(
            self._db_url,
            echo=echo_sql,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,
        )

        # Create session factory
        self._Session = sessionmaker(bind=self.engine)

        # Create metadata and automap base for reflected tables
        metadata = MetaData()
        self.AutomapBase = automap_base(metadata=metadata)

    @staticmethod
    def _is_postgresql_session(session: Session) -> bool:
        bind = getattr(session, "bind", None)
        dialect = getattr(bind, "dialect", None)
        dialect_name = str(getattr(dialect, "name", "") or "").strip().lower()
        return dialect_name == "postgresql"

    def _set_session_timezone_utc(self, session: Session, *, local: bool) -> None:
        """Ensure the DB session timezone is pinned to UTC for PostgreSQL."""
        if not self._is_postgresql_session(session):
            return
        statement = "SET LOCAL TIME ZONE 'UTC'" if local else "SET TIME ZONE 'UTC'"
        try:
            session.execute(text(statement))
        except Exception as e:
            self.logger.warning(f"Could not set session timezone to UTC: {e}")

    def _set_session_username(self, session: Session) -> None:
        """Set the per-transaction username for audit logging (no commit)."""
        try:
            session.execute(
                text("SET LOCAL session.current_username = :username"),
                {"username": self.app_username},
            )
        except Exception as e:
            self.logger.warning(f"Could not set session username: {e}")

    def _set_session_domain_code(self, session: Session, *, local: bool) -> None:
        """Set the domain code and issuer app code seen by SQL triggers."""
        if not self._is_postgresql_session(session):
            return
        dc_stmt = (
            "SET LOCAL session.current_domain_code = :code"
            if local
            else "SET session.current_domain_code = :code"
        )
        app_stmt = (
            "SET LOCAL session.current_app_code = :code"
            if local
            else "SET session.current_app_code = :code"
        )
        try:
            session.execute(text(dc_stmt), {"code": self.domain_code or ""})
            session.execute(text(app_stmt), {"code": self.issuer_app_code or ""})
        except Exception as e:
            self.logger.warning(f"Could not set session domain/app code: {e}")

    def get_session(self) -> Session:
        """
        Get a new session.

        Note: this does NOT set the audit username because Phase 2 requires
        `SET LOCAL`, which is per-transaction. Prefer `session_scope()`.

        Returns:
            New SQLAlchemy Session (caller must close)
        """
        session = self._Session()
        self._set_session_timezone_utc(session, local=False)
        self._set_session_domain_code(session, local=False)
        return session

    @contextmanager
    def session_scope(self, commit: bool = False) -> Generator[Session, None, None]:
        """
        Context manager for scoped session operations.

        Args:
            commit: If True, commit on success. If False, caller manages transaction.

        Yields:
            SQLAlchemy Session

        Example:
            with conn.session_scope(commit=True) as session:
                session.add(obj)
                # Auto-commits on success, rolls back on exception
        """
        session = self._Session()
        trans = session.begin()
        try:
            # Must happen inside a transaction for SET LOCAL.
            self._set_session_timezone_utc(session, local=True)
            self._set_session_domain_code(session, local=True)
            self._set_session_username(session)
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

    def reflect_tables(self) -> None:
        """Reflect database tables into AutomapBase."""
        self.AutomapBase.prepare(autoload_with=self.engine)

    def __enter__(self) -> "TAPDBConnection":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit - cleanup resources."""
        if exc_type is not None:
            self.logger.warning(f"Exception in context: {exc_type.__name__}: {exc_val}")
        self.close()
        return False

    def close(self) -> None:
        """Dispose engine resources."""
        if self.engine:
            try:
                self.engine.dispose()
            except Exception as e:
                self.logger.warning(f"Error disposing engine: {e}")
