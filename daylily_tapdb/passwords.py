"""Password hashing helpers.

Implementation: bcrypt via passlib (install with passlib[bcrypt]).
"""

from __future__ import annotations

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


def hash_password(password: str) -> str:
    """Hash a password for storage (bcrypt via passlib).

    Raises RuntimeError if passlib is not installed.
    """
    if password is None or password == "":
        raise ValueError("password cannot be empty")
    if _PWD_CONTEXT is None:
        detail = ""
        if _PWD_CONTEXT_ERROR is not None:
            err_type = type(_PWD_CONTEXT_ERROR).__name__
            detail = (
                " (passlib/bcrypt init failed:"
                f" {err_type}: {_PWD_CONTEXT_ERROR};"
                " if you see a 72-byte limit error,"
                " pin bcrypt<4)"
            )
        raise RuntimeError(
            "passlib is required for password hashing"
            " (install passlib[bcrypt] /"
            " daylily-tapdb[admin])" + detail
        )
    return _PWD_CONTEXT.hash(password)


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored hash."""
    if not stored_hash:
        return False

    if _PWD_CONTEXT is None:
        detail = ""
        if _PWD_CONTEXT_ERROR is not None:
            err_type = type(_PWD_CONTEXT_ERROR).__name__
            detail = (
                " (passlib/bcrypt init failed:"
                f" {err_type}: {_PWD_CONTEXT_ERROR};"
                " if you see a 72-byte limit error,"
                " pin bcrypt<4)"
            )
        raise RuntimeError(
            "passlib is required to verify bcrypt"
            " hashes (install passlib[bcrypt] /"
            " daylily-tapdb[admin])" + detail
        )
    return bool(_PWD_CONTEXT.verify(password, stored_hash))
