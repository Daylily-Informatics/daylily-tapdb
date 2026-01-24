"""TAPDB Database Connection Manager.

Moonshot Phase 2 policy:
- No surprise commits inside the library
- Callers control transaction boundaries
- Audit username is set per-transaction using `SET LOCAL session.current_username`

Recommended usage:

    with TAPDBConnection() as conn:
        with conn.session_scope(commit=False) as session:
            rows = session.query(...).all()

For write operations:

    with TAPDBConnection() as conn:
        with conn.session_scope(commit=True) as session:
            session.add(obj)
"""
import os
import logging
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, Session

from daylily_tapdb.models.base import Base

logger = logging.getLogger(__name__)


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
    ):
        """
        Initialize database connection.

        Args:
            db_url: Full database URL (overrides other db_* params)
            db_url_prefix: Database URL prefix (default: postgresql://)
            db_hostname: Database host:port (default: localhost:$PGPORT or 5432)
            db_pass: Database password (default: $PGPASSWORD)
            db_user: Database user (default: $USER)
            db_name: Database name (default: tapdb)
            app_username: Username for audit logging (default: $USER)
            echo_sql: Log SQL statements (default: $ECHO_SQL env var)
            pool_size: Connection pool size
            max_overflow: Max connections above pool_size
            pool_timeout: Seconds to wait for connection
            pool_recycle: Seconds before connection recycled
        """
        self.logger = logging.getLogger(__name__ + ".TAPDBConnection")

        # Resolve defaults from environment
        db_hostname = db_hostname or f"localhost:{os.environ.get('PGPORT', '5432')}"
        db_pass = db_pass or os.environ.get("PGPASSWORD", "")
        db_user = db_user or os.environ.get("USER", "tapdb")
        self.app_username = app_username or os.environ.get("USER", "tapdb_orm")

        if echo_sql is None:
            echo_env = os.environ.get("ECHO_SQL", "").lower()
            echo_sql = echo_env in ("true", "1", "yes")

        # Build database URL
        if db_url:
            self._db_url = db_url
        else:
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

    def _set_session_username(self, session: Session) -> None:
        """Set the per-transaction username for audit logging (no commit)."""
        try:
            session.execute(
                text("SET LOCAL session.current_username = :username"),
                {"username": self.app_username}
            )
        except Exception as e:
            self.logger.warning(f"Could not set session username: {e}")

    def get_session(self) -> Session:
        """
        Get a new session.

        Note: this does NOT set the audit username because Phase 2 requires
        `SET LOCAL`, which is per-transaction. Prefer `session_scope()`.

        Returns:
            New SQLAlchemy Session (caller must close)
        """
        return self._Session()

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
