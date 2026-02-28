"""Aurora PostgreSQL connection builder.

Constructs SQLAlchemy connection URLs for Aurora PostgreSQL clusters with
IAM database authentication and mandatory SSL (``sslmode=verify-full``).

The RDS CA bundle is auto-downloaded and cached at
``~/.config/tapdb/rds-ca-bundle.pem`` on first use.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

# AWS publishes a combined CA bundle for all regions.
_RDS_CA_BUNDLE_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
_RDS_CA_BUNDLE_SHA256 = (
    "e5bb2084ccf45087bda1c9bffdea0eb15ee67f0b91646106e466714f9de3c7e3"
)
_CA_BUNDLE_DIR = Path.home() / ".config" / "tapdb"
_CA_BUNDLE_PATH = _CA_BUNDLE_DIR / "rds-ca-bundle.pem"

# IAM auth token cache: (region, host, port, user) -> (token, expires_at)
_iam_token_cache: dict[tuple, tuple[str, float]] = {}
_IAM_TOKEN_TTL = 14 * 60  # 14 minutes (tokens valid for 15)


def _ensure_boto3():
    """Import boto3, raising a clear error if missing."""
    try:
        import boto3

        return boto3
    except ImportError:
        raise ImportError(
            "boto3 is required for Aurora connections. "
            "Install it with: pip install daylily-tapdb[aurora]"
        ) from None


class AuroraConnectionBuilder:
    """Build SQLAlchemy connection URLs for Aurora PostgreSQL.

    Supports two authentication modes:

    1. **IAM auth** (default): generates a short-lived token via
       ``rds.generate_db_auth_token()``.
    2. **Secrets Manager**: retrieves the master password from a
       Secrets Manager secret ARN.

    SSL is always mandatory (``sslmode=verify-full``).
    """

    # ------------------------------------------------------------------
    # IAM auth token
    # ------------------------------------------------------------------

    @staticmethod
    def get_iam_auth_token(
        region: str,
        host: str,
        port: int,
        user: str,
    ) -> str:
        """Generate an RDS IAM authentication token.

        Uses a module-level cache with a 14-minute TTL (tokens are valid
        for 15 minutes) to avoid unnecessary API calls.

        Args:
            region: AWS region (e.g. ``us-west-2``).
            host: RDS cluster endpoint hostname.
            port: Database port (typically ``5432``).
            user: Database username.

        Returns:
            Short-lived IAM auth token string.
        """
        cache_key = (region, host, port, user)
        if cache_key in _iam_token_cache:
            token, expires_at = _iam_token_cache[cache_key]
            if time.monotonic() < expires_at:
                logger.debug("Using cached IAM auth token for %s@%s", user, host)
                return token

        boto3 = _ensure_boto3()
        client = boto3.client("rds", region_name=region)
        token = client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=user,
            Region=region,
        )
        _iam_token_cache[cache_key] = (token, time.monotonic() + _IAM_TOKEN_TTL)
        logger.debug("Generated new IAM auth token for %s@%s:%s", user, host, port)
        return token

    # ------------------------------------------------------------------
    # Secrets Manager password
    # ------------------------------------------------------------------

    @staticmethod
    def get_secret_password(secret_arn: str, region: Optional[str] = None) -> str:
        """Retrieve the master password from Secrets Manager.

        The secret value is expected to be a JSON object with a
        ``password`` key (the format used by RDS-managed secrets).

        Args:
            secret_arn: Full ARN of the Secrets Manager secret.
            region: AWS region.  Inferred from the ARN if omitted.

        Returns:
            The password string.
        """
        boto3 = _ensure_boto3()
        if region is None:
            # arn:aws:secretsmanager:<region>:<account>:secret:<name>
            parts = secret_arn.split(":")
            region = parts[3] if len(parts) > 3 else "us-west-2"
        client = boto3.client("secretsmanager", region_name=region)
        resp = client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(resp["SecretString"])
        return secret["password"]

    # ------------------------------------------------------------------
    # RDS CA bundle
    # ------------------------------------------------------------------

    @staticmethod
    def ensure_ca_bundle() -> Path:
        """Download the RDS CA bundle if not already cached.

        After download, the bundle's SHA-256 checksum is verified against
        a known-good value and file permissions are set to ``0644``.

        Returns:
            Path to the local CA bundle PEM file.

        Raises:
            RuntimeError: If the downloaded file fails checksum verification.
        """
        if _CA_BUNDLE_PATH.exists():
            logger.debug("RDS CA bundle already cached at %s", _CA_BUNDLE_PATH)
            return _CA_BUNDLE_PATH

        logger.info("Downloading RDS CA bundle to %s …", _CA_BUNDLE_PATH)
        import urllib.request

        _CA_BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(_RDS_CA_BUNDLE_URL, str(_CA_BUNDLE_PATH))

        # Verify checksum
        sha256 = hashlib.sha256(_CA_BUNDLE_PATH.read_bytes()).hexdigest()
        if sha256 != _RDS_CA_BUNDLE_SHA256:
            _CA_BUNDLE_PATH.unlink()
            raise RuntimeError(
                f"RDS CA bundle checksum mismatch: expected "
                f"{_RDS_CA_BUNDLE_SHA256}, got {sha256}. "
                "The file has been removed. Retry, or download manually "
                f"from {_RDS_CA_BUNDLE_URL}"
            )

        # Set permissions to 0644
        os.chmod(
            _CA_BUNDLE_PATH,
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH,
        )
        logger.info("RDS CA bundle verified (SHA-256: %s)", sha256[:12] + "...")
        return _CA_BUNDLE_PATH

    # ------------------------------------------------------------------
    # Connection URL builder
    # ------------------------------------------------------------------

    @classmethod
    def build_connection_url(
        cls,
        *,
        host: str,
        port: int = 5432,
        database: str,
        user: str,
        region: str = "us-west-2",
        iam_auth: bool = True,
        secret_arn: Optional[str] = None,
        password: Optional[str] = None,
    ) -> str:
        """Build a SQLAlchemy PostgreSQL URL with SSL for Aurora.

        Authentication priority:
        1. ``iam_auth=True`` → generate IAM token (default).
        2. ``secret_arn`` provided → fetch from Secrets Manager.
        3. ``password`` provided → use directly.

        Args:
            host: Aurora cluster endpoint.
            port: Database port.
            database: Database name.
            user: Database username.
            region: AWS region.
            iam_auth: Use IAM database authentication.
            secret_arn: Secrets Manager ARN for password fallback.
            password: Explicit password (lowest priority).

        Returns:
            SQLAlchemy connection URL string.
        """
        # Resolve password / token
        if iam_auth:
            credential = cls.get_iam_auth_token(region, host, port, user)
        elif secret_arn:
            credential = cls.get_secret_password(secret_arn, region)
        elif password:
            credential = password
        else:
            raise ValueError(
                "Aurora connection requires iam_auth=True, a secret_arn, "
                "or an explicit password."
            )

        # Ensure CA bundle is available
        ca_path = cls.ensure_ca_bundle()

        # URL-encode the credential (IAM tokens contain special chars)
        encoded_cred = quote_plus(credential)

        # Build URL with SSL query params
        url = (
            f"postgresql+psycopg2://{quote_plus(user)}:{encoded_cred}"
            f"@{host}:{port}/{database}"
            f"?sslmode=verify-full&sslrootcert={quote_plus(str(ca_path))}"
        )
        logger.debug(
            "Built Aurora connection URL for %s@%s:%s/%s (iam=%s)",
            user,
            host,
            port,
            database,
            iam_auth,
        )
        return url
