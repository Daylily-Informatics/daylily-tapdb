"""Password hashing helpers.

Phase 4 requirement: move off salted SHA-256 to a modern password hash.

Implementation:
- Preferred: bcrypt via passlib (install with passlib[bcrypt])
- Backward compatible verify for legacy stored hashes in "salt:sha256" format.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any


_PWD_CONTEXT_ERROR: Exception | None = None


def _get_pwd_context() -> Any:
    global _PWD_CONTEXT_ERROR
    try:
        from passlib.context import CryptContext  # type: ignore
    except ModuleNotFoundError:
        return None
    except Exception as e:  # pragma: no cover
        # e.g. passlib import can fail if an incompatible bcrypt backend is installed
        _PWD_CONTEXT_ERROR = e
        return None

    # passlib can raise at construction time if an incompatible bcrypt backend
    # is installed (e.g. bcrypt>=4). Treat that as "passlib unavailable" and
    # surface a clearer error at call sites.
    try:
        # bcrypt defaults are sane; keep one scheme to avoid ambiguity.
        return CryptContext(schemes=["bcrypt"], deprecated="auto")
    except Exception as e:  # pragma: no cover
        _PWD_CONTEXT_ERROR = e
        return None


_PWD_CONTEXT = _get_pwd_context()


def _is_legacy_sha256_hash(stored_hash: str) -> bool:
    # Historical format used by TAPDB: "salt:hexsha256(salt+password)"
    return ":" in (stored_hash or "") and not stored_hash.lstrip().startswith("$")


def hash_password(password: str) -> str:
    """Hash a password for storage (bcrypt via passlib).

    Raises RuntimeError if passlib is not installed.
    """
    if password is None or password == "":
        raise ValueError("password cannot be empty")
    if _PWD_CONTEXT is None:
        detail = ""
        if _PWD_CONTEXT_ERROR is not None:
            detail = (
                f" (passlib/bcrypt init failed: {type(_PWD_CONTEXT_ERROR).__name__}: {_PWD_CONTEXT_ERROR}; "
                "if you see a 72-byte limit error, pin bcrypt<4)"
            )
        raise RuntimeError(
            "passlib is required for password hashing (install passlib[bcrypt] / daylily-tapdb[admin])"
            + detail
        )
    return _PWD_CONTEXT.hash(password)


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored hash.

    Supports legacy salted SHA-256 hashes for backward compatibility.
    """
    if not stored_hash:
        return False

    if _is_legacy_sha256_hash(stored_hash):
        try:
            salt, hash_val = stored_hash.split(":", 1)
        except ValueError:
            return False
        digest = hashlib.sha256((salt + password).encode()).hexdigest()
        return hmac.compare_digest(digest, hash_val)

    if _PWD_CONTEXT is None:
        detail = ""
        if _PWD_CONTEXT_ERROR is not None:
            detail = (
                f" (passlib/bcrypt init failed: {type(_PWD_CONTEXT_ERROR).__name__}: {_PWD_CONTEXT_ERROR}; "
                "if you see a 72-byte limit error, pin bcrypt<4)"
            )
        raise RuntimeError(
            "passlib is required to verify bcrypt hashes (install passlib[bcrypt] / daylily-tapdb[admin])"
            + detail
        )
    return bool(_PWD_CONTEXT.verify(password, stored_hash))
