"""Aurora PostgreSQL schema deployer.

Deploys the TAPDB schema to an Aurora PostgreSQL cluster using ``psql``
with IAM authentication or Secrets Manager password, enforcing
``sslmode=verify-full`` with the RDS CA bundle.
"""

from __future__ import annotations

import logging
import os
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
    def _build_psql_env(
        *,
        host: str,
        port: int,
        user: str,
        database: str,
        region: str,
        iam_auth: bool = True,
        secret_arn: Optional[str] = None,
        password: Optional[str] = None,
    ) -> tuple[list[str], dict[str, str]]:
        """Build psql command and environment variables for Aurora.

        Returns:
            Tuple of (psql_cmd_args, env_vars).
        """
        # Resolve credential
        if iam_auth:
            credential = AuroraConnectionBuilder.get_iam_auth_token(
                region=region, host=host, port=port, user=user,
            )
        elif secret_arn:
            credential = AuroraConnectionBuilder.get_secret_password(
                secret_arn=secret_arn, region=region,
            )
        elif password:
            credential = password
        else:
            raise ValueError(
                "Aurora psql requires iam_auth=True, a secret_arn, "
                "or an explicit password."
            )

        # Ensure CA bundle
        ca_path = AuroraConnectionBuilder.ensure_ca_bundle()

        env_vars = os.environ.copy()
        env_vars["PGPASSWORD"] = credential
        env_vars["PGSSLMODE"] = "verify-full"
        env_vars["PGSSLROOTCERT"] = str(ca_path)

        cmd = [
            "psql",
            "-X",
            "-q",
            "-t",
            "-A",
            "-h", host,
            "-p", str(port),
            "-U", user,
            "-d", database,
            "-v", "ON_ERROR_STOP=1",
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
        sql: Optional[str] = None,
        file: Optional[Path] = None,
    ) -> tuple[bool, str]:
        """Run a psql command against Aurora with SSL + auth.

        Returns:
            Tuple of (success, output).
        """
        cmd, env_vars = cls._build_psql_env(
            host=host, port=port, user=user, database=database,
            region=region, iam_auth=iam_auth, secret_arn=secret_arn,
            password=password,
        )

        if file:
            cmd.extend(["-f", str(file)])
        elif sql:
            cmd.extend(["-c", sql])

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, env=env_vars,
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
    ) -> tuple[bool, str]:
        """Deploy the TAPDB schema to an Aurora PostgreSQL cluster.

        This applies the schema SQL file via ``psql`` with SSL enforced.
        The schema includes pgcrypto extension creation which Aurora
        PostgreSQL supports natively.

        Returns:
            Tuple of (success, output_message).
        """
        logger.info(
            "Deploying schema %s to %s:%s/%s", schema_file, host, port, database,
        )

        success, output = cls.run_psql(
            host=host, port=port, user=user, database=database,
            region=region, iam_auth=iam_auth, secret_arn=secret_arn,
            password=password, file=schema_file,
        )

        if success:
            logger.info("Schema deployed successfully to %s/%s", host, database)
        else:
            logger.error("Schema deployment failed: %s", output[:500])

        return success, output

