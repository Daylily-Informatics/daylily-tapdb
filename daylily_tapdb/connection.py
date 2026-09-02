"""TAPDB Database Connection Manager.

Moonshot Phase 2 policy:
- No surprise commits inside the library
- Callers control transaction boundaries
- Audit username is set per-transaction using `SET LOCAL session.current_username`
- Domain code is set per-session using `session.current_domain_code`
- Repo ownership is set per-session using `session.current_owner_repo_name`

Recommended usage:

    with TAPDBConnection(
        db_url=postgresql_url,
        db_user=runtime_role,
        engine_type="local",
        app_username="catalog-api",
        domain_code="Z",
        owner_repo_name="catalog-service",
        schema_name="catalog",
        config_identity="/abs/path/to/tapdb-config.yaml",
    ) as conn:
        with conn.session_scope(commit=False) as session:
            rows = session.query(...).all()

For write operations:

    with TAPDBConnection(
        db_url=postgresql_url,
        db_user=runtime_role,
        engine_type="local",
        app_username="catalog-api",
        domain_code="Z",
        owner_repo_name="catalog-service",
        schema_name="catalog",
        config_identity="/abs/path/to/tapdb-config.yaml",
    ) as conn:
        with conn.session_scope(commit=True) as session:
            session.add(obj)
"""

import logging
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker

from daylily_tapdb.security_context import (
    TapdbTransactionContext,
    apply_transaction_context,
    assert_operator_role,
    is_postgresql_session,
)

logger = logging.getLogger(__name__)


class TAPDBConnection:
    """
    TAPDB Database Connection Manager.

    Construct connections from one explicit TapDB config. PostgreSQL sessions
    require the runtime role, audit actor, domain, owner, schema, and exact
    absolute config identity. Callers then choose a read-only or committing
    ``session_scope`` and retain control of the transaction boundary.
    """

    def __init__(
        self,
        db_url: Optional[str] = None,
        db_url_prefix: str = "postgresql://",
        db_hostname: Optional[str] = None,
        db_hostaddr: Optional[str] = None,
        db_pass: Optional[str] = None,
        db_user: Optional[str] = None,
        db_name: str = "tapdb",
        app_username: Optional[str] = None,
        echo_sql: bool = False,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 1800,
        engine_type: Optional[str] = None,
        region: str = "us-west-2",
        iam_auth: bool = False,
        secret_arn: Optional[str] = None,
        domain_code: Optional[str] = None,
        owner_repo_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        tenant_id: Optional[str] = None,
        allow_global_rows: bool = False,
        config_identity: Optional[str] = None,
        connection_role: str = "runtime",
    ):
        """
        Initialize database connection.

        Args:
            db_url: Full database URL (overrides other db_* params)
            db_url_prefix: Database URL prefix. Required when db_url is not supplied.
            db_hostname: Database host:port. Required when db_url is not supplied.
            db_hostaddr: Optional explicit network address for Aurora SSM tunnels.
            db_pass: Database password
            db_user: Database user. Required when db_url is not supplied.
            db_name: Database name. Required when db_url is not supplied.
            app_username: Username for audit logging. Required.
            echo_sql: Log every SQL statement the engine emits. Defaults to
                False; callers generally want this off outside debugging.
            pool_size: Connection pool size
            max_overflow: Max connections above pool_size
            pool_timeout: Seconds to wait for connection
            pool_recycle: Seconds before connection recycled
            engine_type: Connection type — "local" for local PG, "compose" for
                explicit Docker Compose PostgreSQL, or "aurora" for Aurora
                PostgreSQL with SSL + IAM auth.
            region: AWS region (only used when engine_type="aurora").
            iam_auth: Use IAM database authentication (Aurora only).
            secret_arn: Secrets Manager ARN for Aurora password retrieval.
            domain_code: Domain code for session scoping (1-4 chars). Required.
            owner_repo_name: Repo-name for session ownership scoping. Required.
            schema_name: PostgreSQL schema to use as this session's search_path.
            tenant_id: Fixed tenant UUID for this runtime principal, or ``None``
                for a deliberately global principal.
            allow_global_rows: Permit the fixed principal to access deliberate
                global rows in its own domain and owner scope.
            config_identity: Exact absolute config path bound to the database
                principal. Required for PostgreSQL sessions.
            connection_role: ``runtime`` for normal access or ``operator`` for
                the distinct migration/DDL role.
        """
        self.logger = logging.getLogger(__name__ + ".TAPDBConnection")

        if not db_user:
            raise ValueError("db_user is required")
        if not app_username:
            raise ValueError("app_username is required")
        self.app_username = app_username
        self.domain_code = domain_code
        self.owner_repo_name = owner_repo_name
        self.schema_name = (schema_name or "").strip() or None
        self.tenant_id = tenant_id
        self.allow_global_rows = allow_global_rows
        self.config_identity = str(config_identity or "").strip()
        postgres_target = (
            engine_type == "aurora"
            or not db_url
            or str(db_url).startswith(("postgresql://", "postgresql+"))
        )
        if postgres_target and not self.config_identity:
            raise ValueError(
                "config_identity is required for PostgreSQL TAPDB sessions"
            )
        if connection_role not in {"runtime", "operator"}:
            raise ValueError("connection_role must be 'runtime' or 'operator'")
        self.connection_role = connection_role
        if not self.domain_code:
            raise ValueError("domain_code is required")
        if not self.owner_repo_name:
            raise ValueError("owner_repo_name is required")

        if engine_type not in {"local", "compose", "aurora"}:
            raise ValueError("engine_type must be 'local', 'compose', or 'aurora'")

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
            if ":" not in db_hostname:
                raise ValueError("db_hostname must include an explicit port for Aurora")
            host, port_str = db_hostname.rsplit(":", 1)
            port = int(port_str)

            self._db_url = AuroraConnectionBuilder.build_connection_url(
                host=host,
                port=port,
                database=db_name,
                user=db_user,
                region=region,
                iam_auth=iam_auth,
                secret_arn=secret_arn,
                password=db_pass,
                hostaddr=db_hostaddr,
            )
        elif engine_type in {"local", "compose"}:
            if not db_hostname:
                raise ValueError(
                    f"db_hostname is required when engine_type={engine_type!r}"
                )
            if db_pass is None:
                raise ValueError(
                    f"db_pass is required when engine_type={engine_type!r}"
                )
            if not db_user:
                raise ValueError(
                    f"db_user is required when engine_type={engine_type!r}"
                )
            if not db_name:
                raise ValueError(
                    f"db_name is required when engine_type={engine_type!r}"
                )
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
        return is_postgresql_session(session)

    def _execute_session_setting(
        self,
        session: Session,
        statement: str,
        params: Optional[dict[str, object]] = None,
        *,
        use_savepoint: bool,
        warning: str,
    ) -> bool:
        """Execute required context SQL; any failure aborts the transaction."""
        del use_savepoint, warning
        session.execute(text(statement), params or {})
        return True

    def _set_session_timezone_utc(self, session: Session, *, local: bool) -> None:
        """Intentionally leave the session timezone unchanged.

        TAPDB timestamps are DB-managed `TIMESTAMP WITH TIME ZONE` values backed by
        `CURRENT_TIMESTAMP`/`NOW()` defaults and triggers, so mutating the session
        TimeZone is not required for storage correctness and breaks some
        PostgreSQL-compatible backends.
        """
        return

    def _set_session_username(self, session: Session) -> None:
        """Set the per-transaction username for audit logging (no commit)."""
        self._execute_session_setting(
            session,
            "SET LOCAL session.current_username = :username",
            {"username": self.app_username},
            use_savepoint=True,
            warning="Could not set session username",
        )

    def _set_session_domain_code(self, session: Session, *, local: bool) -> None:
        """Set the domain code and owner repo name seen by SQL triggers."""
        if not self._is_postgresql_session(session):
            return
        dc_stmt = (
            "SET LOCAL session.current_domain_code = :code"
            if local
            else "SET session.current_domain_code = :code"
        )
        owner_stmt = (
            "SET LOCAL session.current_owner_repo_name = :code"
            if local
            else "SET session.current_owner_repo_name = :code"
        )
        self._execute_session_setting(
            session,
            dc_stmt,
            {"code": self.domain_code or ""},
            use_savepoint=local,
            warning="Could not set session domain code",
        )
        self._execute_session_setting(
            session,
            owner_stmt,
            {"code": self.owner_repo_name or ""},
            use_savepoint=local,
            warning="Could not set session owner repo name",
        )

    def _set_session_search_path(self, session: Session, *, local: bool) -> None:
        """Set the PostgreSQL search_path for TAPDB runtime queries."""
        if not self._is_postgresql_session(session):
            return
        if not self.schema_name:
            raise ValueError("schema_name is required for PostgreSQL TAPDB sessions.")
        self._execute_session_setting(
            session,
            "SELECT set_config('search_path', :schema_name, :is_local)",
            {"schema_name": self.schema_name, "is_local": local},
            use_savepoint=local,
            warning="Could not set session search_path",
        )

    def get_session(self) -> Session:
        """
        Get a new session.

        Note: this does NOT set the audit username because Phase 2 requires
        `SET LOCAL`, which is per-transaction. Prefer `session_scope()`.

        Returns:
            New SQLAlchemy Session (caller must close)
        """
        raise RuntimeError(
            "get_session() cannot establish a fail-closed transaction context; "
            "use session_scope()"
        )

    def transaction_context(self) -> TapdbTransactionContext:
        """Return the complete immutable context for one database transaction."""
        return TapdbTransactionContext(
            config_identity=self.config_identity,
            schema_name=self.schema_name or "",
            domain_code=self.domain_code or "",
            owner_repo_name=self.owner_repo_name or "",
            tenant_id=self.tenant_id,
            actor=self.app_username,
            allow_global_rows=self.allow_global_rows,
        )

    def install_transaction_context(self, session: object) -> None:
        """Install this connection's context on a Session or Connection."""
        apply_transaction_context(
            session,
            self.transaction_context(),
            assert_runtime_role=self.connection_role == "runtime",
        )
        if self.connection_role == "operator" and is_postgresql_session(session):
            assert_operator_role(session)

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
            if self._is_postgresql_session(session) and not self.schema_name:
                raise ValueError(
                    "schema_name is required for PostgreSQL TAPDB sessions."
                )
            if self._is_postgresql_session(session):
                self.install_transaction_context(session)
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
