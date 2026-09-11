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
from urllib.parse import quote_plus, urlencode, urlsplit

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


def _require_https_url(url: str, *, label: str) -> str:
    """Reject non-HTTPS URLs before downloading remote assets."""
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError(f"{label} must be an https URL")
    return url


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
        profile: Optional[str] = None,
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
        cache_key = (region, host, port, user, profile or "")
        if cache_key in _iam_token_cache:
            token, expires_at = _iam_token_cache[cache_key]
            if time.monotonic() < expires_at:
                logger.debug("Using cached IAM auth token for %s@%s", user, host)
                return token

        boto3 = _ensure_boto3()
        if profile:
            session = boto3.session.Session(profile_name=profile)
            client = session.client("rds", region_name=region)
        else:
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
    def get_secret_password(
        secret_arn: str,
        region: Optional[str] = None,
        *,
        profile: Optional[str] = None,
    ) -> str:
        """Retrieve the master password from Secrets Manager.

        The secret value is expected to be a JSON object with a
        ``password`` key (the format used by RDS-managed secrets).

        Args:
            secret_arn: Full ARN of the Secrets Manager secret.
            region: AWS region.  Inferred from the ARN if omitted.
            profile: Explicit AWS profile. Existing callers may use the SDK's
                configured credential chain by omitting this keyword.

        Returns:
            The password string.
        """
        boto3 = _ensure_boto3()
        if region is None:
            # arn:aws:secretsmanager:<region>:<account>:secret:<name>
            parts = secret_arn.split(":")
            region = parts[3] if len(parts) > 3 else "us-west-2"
        if profile:
            session = boto3.session.Session(profile_name=profile)
            client = session.client("secretsmanager", region_name=region)
        else:
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
        download_url = _require_https_url(
            _RDS_CA_BUNDLE_URL,
            label="RDS CA bundle URL",
        )
        urllib.request.urlretrieve(download_url, str(_CA_BUNDLE_PATH))  # nosec B310

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
        region: str,
        iam_auth: bool,
        secret_arn: Optional[str] = None,
        password: Optional[str] = None,
        hostaddr: Optional[str] = None,
        profile: Optional[str] = None,
        sslrootcert: Optional[str] = None,
        server_port: Optional[int] = None,
    ) -> str:
        """Build a SQLAlchemy PostgreSQL URL with SSL for Aurora.

        Authentication priority:
        1. ``iam_auth=True`` → generate IAM token.
        2. ``secret_arn`` provided → fetch from Secrets Manager.
        3. ``password`` provided → use directly.

        Args:
            host: Aurora cluster endpoint.
            port: Database port.
            database: Database name.
            user: Database username.
            region: AWS region.
            iam_auth: Use IAM database authentication.
            secret_arn: Secrets Manager ARN for password authentication.
            password: Explicit password.
            hostaddr: Optional explicit network address for libpq. Use this for
                local SSM tunnels while keeping ``host`` as the RDS hostname for
                ``sslmode=verify-full``.
            profile: Explicit AWS profile for IAM or Secrets Manager. Omission
                retains the SDK credential chain for existing callers.
            sslrootcert: Explicit absolute path to an existing CA bundle. Only
                omission uses the cached or downloaded RDS CA bundle.
            server_port: Optional explicit remote port for IAM token signing.
                The socket still uses ``port``. Omission signs the supplied
                connection port; no different remote port is inferred.

        Returns:
            SQLAlchemy connection URL string.
        """
        if profile is not None and (not profile or profile != profile.strip()):
            raise ValueError("profile must be a nonempty exact AWS profile name")
        if server_port is not None and (
            type(server_port) is not int or not 1 <= server_port <= 65535
        ):
            raise ValueError("server_port must be an integer in 1..65535")

        # Reject an invalid explicit CA before fetching credentials or making
        # any network call. Never replace a supplied path with a default.
        ca_path = None
        if sslrootcert is not None:
            if not sslrootcert or sslrootcert != sslrootcert.strip():
                raise ValueError("sslrootcert must be an existing absolute file")
            ca_path = Path(sslrootcert)
            if not ca_path.is_absolute() or not ca_path.is_file():
                raise ValueError("sslrootcert must be an existing absolute file")

        # Resolve password / token
        if iam_auth:
            credential = cls.get_iam_auth_token(
                region,
                host,
                port if server_port is None else server_port,
                user,
                profile=profile,
            )
        elif secret_arn:
            credential = cls.get_secret_password(secret_arn, region, profile=profile)
        elif password:
            credential = password
        else:
            raise ValueError(
                "Aurora connection requires iam_auth=True, a secret_arn, "
                "or an explicit password."
            )

        # Preserve the established CA behavior only when no path was supplied.
        if ca_path is None:
            ca_path = cls.ensure_ca_bundle()

        # URL-encode the credential (IAM tokens contain special chars)
        encoded_cred = quote_plus(credential)

        # Build URL with SSL query params. When hostaddr is set, libpq connects
        # to that address but still uses host for certificate verification.
        query = {
            "sslmode": "verify-full",
            "sslrootcert": str(ca_path),
        }
        if hostaddr:
            query["hostaddr"] = str(hostaddr).strip()
        url = (
            f"postgresql+psycopg2://{quote_plus(user)}:{encoded_cred}"
            f"@{host}:{port}/{database}"
            f"?{urlencode(query)}"
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
