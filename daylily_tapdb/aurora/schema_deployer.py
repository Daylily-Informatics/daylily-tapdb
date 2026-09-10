"""Aurora PostgreSQL schema deployer.

Deploys the TAPDB schema to an Aurora PostgreSQL cluster using ``psql``
with IAM authentication or Secrets Manager password, enforcing
``sslmode=verify-full`` with the RDS CA bundle.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Optional

from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

logger = logging.getLogger(__name__)


class AuroraSchemaDeployer:
    """Deploy TAPDB schema to Aurora PostgreSQL via ``psql``.

    All connections use SSL (``sslmode=verify-full``) and authenticate
    via IAM token or Secrets Manager password.
    """

    @staticmethod
    def client_env(
        *,
        host: str,
        port: int,
        user: str,
        region: str,
        iam_auth: bool = True,
        secret_arn: Optional[str] = None,
        password: Optional[str] = None,
        hostaddr: Optional[str] = None,
        profile: Optional[str] = None,
        sslrootcert: Optional[str] = None,
        server_port: Optional[int] = None,
    ) -> dict[str, str]:
        """Build the environment any libpq client needs to reach Aurora.

        Returns credential and TLS material only -- ``PGPASSWORD``,
        ``PGSSLMODE``, ``PGSSLROOTCERT``, and optionally ``PGHOSTADDR``.

        It deliberately does **not** carry the connection target. Host, port,
        user, and database are passed as command-line flags, which ``psql``,
        ``pg_dump``, and ``pg_restore`` all accept identically -- so every
        client gets Aurora's IAM/Secrets-Manager auth and ``verify-full`` TLS
        from this one function while building its own argv.

        ``PGHOSTADDR`` pins the resolved address. That matters for
        snapshot-consistent dumps: ``pg_dump --snapshot`` is only valid if it
        reaches the same backend as the session that exported the snapshot, and
        an Aurora cluster's reader and writer endpoints are different hosts.
        ``server_port`` is an explicit remote port for IAM token signing only;
        the caller's ``port`` continues to select the transport endpoint.
        """
        if server_port is not None and (
            type(server_port) is not int or not 1 <= server_port <= 65535
        ):
            raise ValueError("server_port must be an integer in 1..65535")
        if iam_auth:
            credential = AuroraConnectionBuilder.get_iam_auth_token(
                region=region,
                host=host,
                port=port if server_port is None else server_port,
                user=user,
                profile=profile,
            )
        elif secret_arn:
            credential = AuroraConnectionBuilder.get_secret_password(
                secret_arn=secret_arn,
                region=region,
                profile=profile,
            )
        elif password:
            credential = password
        else:
            raise ValueError(
                "Aurora psql requires iam_auth=True, a secret_arn, "
                "or an explicit password."
            )

        if sslrootcert is not None:
            ca_path = Path(sslrootcert)
            if not ca_path.is_absolute() or not ca_path.is_file():
                raise ValueError(
                    "Explicit Aurora sslrootcert must be an existing absolute file"
                )
        else:
            ca_path = AuroraConnectionBuilder.ensure_ca_bundle()

        from daylily_tapdb.backup.engine import sanitized_libpq_environment

        env_vars = sanitized_libpq_environment()
        env_vars["PGPASSWORD"] = credential
        env_vars["PGSSLMODE"] = "verify-full"
        env_vars["PGSSLROOTCERT"] = str(ca_path)
        if hostaddr:
            env_vars["PGHOSTADDR"] = str(hostaddr).strip()

        return env_vars

    @classmethod
    def _build_psql_env(
        cls,
        *,
        host: str,
        port: int,
        user: str,
        database: str,
        region: str,
        iam_auth: bool = True,
        secret_arn: Optional[str] = None,
        password: Optional[str] = None,
        hostaddr: Optional[str] = None,
        profile: Optional[str] = None,
        sslrootcert: Optional[str] = None,
        server_port: Optional[int] = None,
    ) -> tuple[list[str], dict[str, str]]:
        """Build psql command and environment variables for Aurora.

        Thin wrapper over :meth:`client_env` that adds the psql argv, kept so
        ``run_psql`` behaviour is unchanged by the extraction.

        Returns:
            Tuple of (psql_cmd_args, env_vars).
        """
        env_vars = cls.client_env(
            host=host,
            port=port,
            user=user,
            region=region,
            iam_auth=iam_auth,
            secret_arn=secret_arn,
            password=password,
            hostaddr=hostaddr,
            profile=profile,
            sslrootcert=sslrootcert,
            server_port=server_port,
        )

        cmd = [
            "psql",
            "-X",
            "-q",
            "-t",
            "-A",
            "-h",
            host,
            "-p",
            str(port),
            "-U",
            user,
            "-d",
            database,
            "-v",
            "ON_ERROR_STOP=1",
        ]

        return cmd, env_vars

    @classmethod
    def run_psql(
        cls,
        *,
        host: str,
        port: int,
        user: str,
        database: str,
        region: str,
        iam_auth: bool = True,
        secret_arn: Optional[str] = None,
        password: Optional[str] = None,
        hostaddr: Optional[str] = None,
        profile: Optional[str] = None,
        sslrootcert: Optional[str] = None,
        sql: Optional[str] = None,
        file: Optional[Path] = None,
        setup_sql: Optional[str] = None,
        server_port: Optional[int] = None,
    ) -> tuple[bool, str]:
        """Run a psql command against Aurora with SSL + auth.

        Returns:
            Tuple of (success, output).
        """
        try:
            cmd, env_vars = cls._build_psql_env(
                host=host,
                port=port,
                user=user,
                database=database,
                region=region,
                iam_auth=iam_auth,
                secret_arn=secret_arn,
                password=password,
                hostaddr=hostaddr,
                profile=profile,
                sslrootcert=sslrootcert,
                server_port=server_port,
            )

            if setup_sql:
                cmd.extend(["-c", setup_sql])
            if file:
                cmd.extend(["-f", str(file)])
            elif sql:
                cmd.extend(["-c", sql])

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env_vars,
            )
            if result.returncode == 0:
                return True, (result.stdout or "").strip()
            return False, (result.stdout + result.stderr).strip()
        except FileNotFoundError:
            return False, "psql not found. Please install PostgreSQL client."
        except Exception as e:
            return False, str(e)

    @classmethod
    def deploy_schema(
        cls,
        *,
        host: str,
        port: int,
        user: str,
        database: str,
        region: str,
        schema_file: Path,
        iam_auth: bool = True,
        secret_arn: Optional[str] = None,
        password: Optional[str] = None,
        hostaddr: Optional[str] = None,
        profile: Optional[str] = None,
        sslrootcert: Optional[str] = None,
        server_port: Optional[int] = None,
    ) -> tuple[bool, str]:
        """Deploy the TAPDB schema to an Aurora PostgreSQL cluster.

        This applies the schema SQL file via ``psql`` with SSL enforced.
        The schema includes pgcrypto extension creation which Aurora
        PostgreSQL supports natively.

        Returns:
            Tuple of (success, output_message).
        """
        logger.info(
            "Deploying schema %s to %s:%s/%s",
            schema_file,
            host,
            port,
            database,
        )

        success, output = cls.run_psql(
            host=host,
            port=port,
            user=user,
            database=database,
            region=region,
            iam_auth=iam_auth,
            secret_arn=secret_arn,
            password=password,
            hostaddr=hostaddr,
            profile=profile,
            sslrootcert=sslrootcert,
            server_port=server_port,
            file=schema_file,
        )

        if success:
            logger.info("Schema deployed successfully to %s/%s", host, database)
        else:
            logger.error("Schema deployment failed: %s", output[:500])

        return success, output
