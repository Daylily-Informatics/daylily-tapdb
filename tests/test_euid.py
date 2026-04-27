"""Tests for the Meridian v0.4.x TapDB EUID facade."""

from __future__ import annotations

import pytest

from daylily_tapdb.euid import (
    AUDIT_LOG_PREFIX,
    GENERIC_INSTANCE_LINEAGE_PREFIX,
    GENERIC_TEMPLATE_PREFIX,
    SYSTEM_MESSAGE_PREFIX,
    SYSTEM_USER_PREFIX,
    EUIDConfig,
    format_euid,
    normalize_domain_code,
    normalize_prefix,
    resolve_runtime_domain_code,
    resolve_runtime_owner_repo_name,
    resolve_runtime_validation_context,
    validate_euid,
)


def test_core_prefix_catalog_matches_reserved_tapdb_prefixes():
    config = EUIDConfig()

    assert config.CORE_PREFIXES["generic_template"] == GENERIC_TEMPLATE_PREFIX
    assert config.CORE_PREFIXES["generic_instance_lineage"] == (
        GENERIC_INSTANCE_LINEAGE_PREFIX
    )
    assert config.CORE_PREFIXES["audit_log"] == AUDIT_LOG_PREFIX
    assert config.CORE_PREFIXES["system_user_instance"] == SYSTEM_USER_PREFIX
    assert config.CORE_PREFIXES["system_message_instance"] == SYSTEM_MESSAGE_PREFIX


def test_prefix_catalog_is_read_only():
    config = EUIDConfig()

    with pytest.raises(TypeError):
        config.CORE_PREFIXES["foo"] = "BAR"  # type: ignore[index]


def test_discriminator_lookup_uses_reserved_prefixes():
    config = EUIDConfig()

    assert config.get_discriminator_for_prefix("TPX") == "generic_template"
    assert config.get_discriminator_for_prefix("SYS") == "system_user_instance"
    assert config.get_discriminator_for_prefix("AGX") is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("z", "Z"),
        ("tapd", "TAPD"),
        ("a1", "A1"),
        ("agx", "AGX"),
    ],
)
def test_normalizers_accept_crockford_tokens(raw: str, expected: str):
    assert normalize_domain_code(raw) == expected
    assert normalize_prefix(raw) == expected


@pytest.mark.parametrize("raw", ["", "IL", "A-1", "ABCDE", "AIO"])
def test_prefix_normalizer_rejects_invalid_values(raw: str):
    if raw == "":
        assert normalize_prefix(raw) is None
        return
    with pytest.raises(ValueError):
        normalize_prefix(raw)


def test_format_euid_emits_canonical_domain_prefix_shape():
    euid = format_euid("AGX", 42, domain_code="Z")
    assert euid == "Z-AGX-1AD"
    assert validate_euid(euid) is True


def test_format_euid_requires_domain_code():
    with pytest.raises(ValueError, match="domain_code is required"):
        format_euid("AGX", 1)


@pytest.mark.parametrize(
    ("euid", "allowed", "expected"),
    [
        ("Z-AGX-1Q", None, True),
        ("Z-AGX-1Q", ["Z"], True),
        ("Z-AGX-1Q", ["T"], False),
        ("z-agx-1q", None, False),
        ("AGX-1Q", None, False),
        ("Z:AGX-1Q", None, False),
        ("Z-AGX-1R", None, False),
    ],
)
def test_validate_euid_enforces_canonical_meridian_shape(
    euid: str, allowed: list[str] | None, expected: bool
):
    assert validate_euid(euid, allowed_domain_codes=allowed) is expected


def test_resolve_runtime_domain_code_requires_env():
    with pytest.raises(ValueError, match="MERIDIAN_DOMAIN_CODE is required"):
        resolve_runtime_domain_code({})


def test_resolve_runtime_domain_code_normalizes_env():
    assert resolve_runtime_domain_code({"MERIDIAN_DOMAIN_CODE": "z"}) == "Z"


def test_resolve_runtime_owner_repo_name_requires_env():
    with pytest.raises(ValueError, match="TAPDB_OWNER_REPO is required"):
        resolve_runtime_owner_repo_name({})


def test_resolve_runtime_owner_repo_name_normalizes_env():
    assert (
        resolve_runtime_owner_repo_name({"TAPDB_OWNER_REPO": " lsmc-atlas "})
        == "lsmc-atlas"
    )


def test_resolve_runtime_validation_context_is_domain_scoped():
    assert resolve_runtime_validation_context({"MERIDIAN_DOMAIN_CODE": "z"}) == {
        "environment": "canonical",
        "allowed_domain_codes": ["Z"],
    }
