"""Tests for password hashing helpers.

These tests are written to pass whether or not optional `passlib` is installed
by monkeypatching the module-level `_PWD_CONTEXT`.
"""

import hashlib

import pytest


def test_hash_password_empty_raises():
    from daylily_tapdb import passwords

    with pytest.raises(ValueError):
        passwords.hash_password("")


def test_hash_password_requires_passlib(monkeypatch):
    from daylily_tapdb import passwords

    monkeypatch.setattr(passwords, "_PWD_CONTEXT", None)
    with pytest.raises(RuntimeError):
        passwords.hash_password("pw")


def test_hash_password_surfaces_backend_init_error(monkeypatch):
    from daylily_tapdb import passwords

    monkeypatch.setattr(passwords, "_PWD_CONTEXT", None)
    monkeypatch.setattr(
        passwords,
        "_PWD_CONTEXT_ERROR",
        ValueError("password cannot be longer than 72 bytes"),
    )

    with pytest.raises(RuntimeError) as e:
        passwords.hash_password("pw")

    assert "bcrypt<4" in str(e.value)


def test_hash_password_delegates_to_context(monkeypatch):
    from daylily_tapdb import passwords

    class _Ctx:
        def hash(self, password: str) -> str:  # pragma: no cover
            return f"hashed:{password}"

    monkeypatch.setattr(passwords, "_PWD_CONTEXT", _Ctx())
    assert passwords.hash_password("pw") == "hashed:pw"


def test_verify_password_empty_hash_is_false():
    from daylily_tapdb import passwords

    assert passwords.verify_password("pw", "") is False


def test_verify_password_legacy_sha256_ok_and_bad():
    from daylily_tapdb import passwords

    salt = "abc"
    password = "pw"
    digest = hashlib.sha256((salt + password).encode()).hexdigest()
    stored = f"{salt}:{digest}"

    assert passwords.verify_password(password, stored) is True
    assert passwords.verify_password("wrong", stored) is False


def test_verify_password_bcrypt_requires_passlib(monkeypatch):
    from daylily_tapdb import passwords

    monkeypatch.setattr(passwords, "_PWD_CONTEXT", None)
    with pytest.raises(RuntimeError):
        passwords.verify_password("pw", "$2b$not-a-real-hash")


def test_verify_password_bcrypt_delegates_to_context(monkeypatch):
    from daylily_tapdb import passwords

    class _Ctx:
        def verify(self, password: str, stored_hash: str) -> bool:  # pragma: no cover
            return password == "pw" and stored_hash == "hash"

    monkeypatch.setattr(passwords, "_PWD_CONTEXT", _Ctx())
    assert passwords.verify_password("pw", "hash") is True
    assert passwords.verify_password("wrong", "hash") is False
