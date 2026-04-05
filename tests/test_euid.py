"""Tests for EUID configuration and Meridian conformance."""

import json
from pathlib import Path

import pytest

from daylily_tapdb.euid import (
    DEFAULT_SANDBOX_PREFIX,
    crockford_base32_encode,
    format_euid,
    meridian_checksum,
    resolve_runtime_sandbox_prefix,
    resolve_runtime_validation_context,
    validate_euid,
)

# Path to Meridian test vectors (read-only reference)
_TEST_VECTORS_PATH = Path(
    "/Users/jmajor/projects/lsmc/meridian-euid/test-vectors/v2.json"
)


class TestEUIDConfig:
    """Tests for EUIDConfig class."""

    def test_core_prefixes(self, euid_config):
        """Test that core prefixes are present."""
        assert euid_config.CORE_PREFIXES["generic_template"] == "GX"
        assert euid_config.CORE_PREFIXES["generic_instance"] == "GX"
        assert euid_config.CORE_PREFIXES["generic_instance_lineage"] == "GX"

    def test_optional_prefixes(self, euid_config):
        """Test that optional prefixes are present."""
        assert euid_config.OPTIONAL_PREFIXES["workflow_instance"] == "WX"
        assert euid_config.OPTIONAL_PREFIXES["workflow_step_instance"] == "WSX"
        assert euid_config.OPTIONAL_PREFIXES["action_instance"] == "XX"

    def test_get_discriminator_for_prefix(self, euid_config):
        """Test looking up canonical discriminator from a canonical prefix."""
        assert euid_config.get_discriminator_for_prefix("GX") is None
        assert euid_config.get_discriminator_for_prefix("WX") == "workflow_instance"
        assert euid_config.get_discriminator_for_prefix("ZZ") is None

    def test_get_all_prefixes(self, euid_config):
        """Test getting all canonical prefixes."""
        all_prefixes = euid_config.get_all_prefixes()
        assert all_prefixes["generic_template"] == "GX"
        assert all_prefixes["workflow_instance"] == "WX"
        assert all_prefixes["action_instance"] == "XX"

    def test_is_canonical_prefix(self, euid_config):
        """Only TapDB-managed prefixes are reported as canonical."""
        assert euid_config.is_canonical_prefix("GX") is True
        assert euid_config.is_canonical_prefix("XX") is True
        assert euid_config.is_canonical_prefix("CX") is False

    def test_prefix_maps_are_read_only(self, euid_config):
        """The exported prefix catalog must not be mutable."""
        with pytest.raises(TypeError):
            euid_config.CORE_PREFIXES["CX"] = "container_instance"  # type: ignore[index]


# ---------------------------------------------------------------------------
# Meridian Crockford Base32 encoding tests
# ---------------------------------------------------------------------------


class TestCrockfordBase32Encode:
    """Tests for crockford_base32_encode()."""

    def test_encode_1(self):
        assert crockford_base32_encode(1) == "1"

    def test_encode_10(self):
        assert crockford_base32_encode(10) == "A"

    def test_encode_31(self):
        assert crockford_base32_encode(31) == "Z"

    def test_encode_32(self):
        assert crockford_base32_encode(32) == "10"

    def test_encode_100(self):
        assert crockford_base32_encode(100) == "34"

    def test_encode_1000(self):
        assert crockford_base32_encode(1000) == "Z8"

    def test_encode_rejects_zero(self):
        with pytest.raises(ValueError, match="positive integer"):
            crockford_base32_encode(0)

    def test_encode_rejects_negative(self):
        with pytest.raises(ValueError, match="positive integer"):
            crockford_base32_encode(-1)

    def test_no_forbidden_chars_in_output(self):
        """Crockford Base32 never produces I, L, O, U."""
        forbidden = set("ILOU")
        for n in range(1, 10001):
            encoded = crockford_base32_encode(n)
            assert not forbidden.intersection(encoded), f"n={n} → {encoded}"

    def test_no_leading_zeros(self):
        """Body MUST NOT begin with 0 per SPEC §6.3."""
        for n in range(1, 10001):
            encoded = crockford_base32_encode(n)
            assert not encoded.startswith("0"), f"n={n} → {encoded}"


# ---------------------------------------------------------------------------
# Meridian checksum tests
# ---------------------------------------------------------------------------


class TestMeridianChecksum:
    """Tests for meridian_checksum() — Luhn MOD32."""

    def test_empty_payload_raises(self):
        with pytest.raises(ValueError):
            meridian_checksum("")

    def test_invalid_char_raises(self):
        with pytest.raises(ValueError, match="invalid character"):
            meridian_checksum("TXO1")  # O is forbidden

    def test_lowercase_raises(self):
        with pytest.raises(ValueError, match="invalid character"):
            meridian_checksum("tx1")


# ---------------------------------------------------------------------------
# format_euid tests
# ---------------------------------------------------------------------------


class TestFormatEuid:
    """Tests for format_euid()."""

    def test_basic_format(self):
        euid = format_euid("TX", 1)
        assert euid == "TX-1C"

    def test_format_has_delimiter(self):
        euid = format_euid("AGX", 42)
        assert "-" in euid

    def test_format_sandbox(self):
        euid = format_euid("TX", 1, sandbox="X")
        assert euid.startswith("X:TX-")


# ---------------------------------------------------------------------------
# validate_euid tests
# ---------------------------------------------------------------------------


class TestValidateEuid:
    """Tests for validate_euid()."""

    def test_valid_production(self):
        assert validate_euid("TX-1C") is True

    def test_reject_lowercase(self):
        assert validate_euid("tx-1c") is False

    def test_reject_whitespace(self):
        assert validate_euid(" TX-1C") is False

    def test_reject_sandbox_in_production(self):
        assert validate_euid("X:TX-1C") is False

    def test_reject_production_in_sandbox(self):
        assert (
            validate_euid(
                "TX-1C", environment="sandbox", allowed_sandbox_prefixes=["X"]
            )
            is False
        )

    def test_reject_wrong_checksum(self):
        assert validate_euid("TX-1D") is False

    def test_reject_body_leading_zero(self):
        assert validate_euid("TX-014") is False

    def test_reject_forbidden_char(self):
        assert validate_euid("TX-O14") is False

    def test_reject_missing_delimiter(self):
        assert validate_euid("TX14") is False


class TestRuntimeSandboxPrefix:
    def test_missing_prefix_defaults_to_t(self):
        assert resolve_runtime_sandbox_prefix({}) == DEFAULT_SANDBOX_PREFIX

    def test_explicit_empty_prefix_disables_prefixing(self):
        assert resolve_runtime_sandbox_prefix({"MERIDIAN_DOMAIN_CODE": ""}) is None

    def test_explicit_prefix_is_normalized(self):
        assert resolve_runtime_sandbox_prefix({"MERIDIAN_DOMAIN_CODE": "s"}) == "S"

    def test_runtime_validation_defaults_to_domain_t(self):
        assert resolve_runtime_validation_context({}) == {
            "environment": "domain",
            "allowed_domain_codes": ["T"],
        }

    def test_runtime_validation_respects_production_env(self):
        assert resolve_runtime_validation_context(
            {
                "MERIDIAN_ENVIRONMENT": "production",
                "MERIDIAN_DOMAIN_CODE": "",
            }
        ) == {"environment": "production"}


# ---------------------------------------------------------------------------
# Meridian v2 test vector conformance
# ---------------------------------------------------------------------------


class TestMeridianTestVectors:
    """Conformance tests against official Meridian test vectors (v2.json)."""

    @pytest.fixture(scope="class")
    def vectors(self):
        if not _TEST_VECTORS_PATH.exists():
            pytest.skip("Meridian test vectors not found")
        with open(_TEST_VECTORS_PATH) as f:
            return json.load(f)

    def test_valid_production_vectors(self, vectors):
        """Each valid_production vector must round-trip through format_euid."""
        for v in vectors["valid_production"]:
            euid_str = v["euid"]
            integer = v["integer"]
            # Extract category from the EUID (everything before the '-')
            category = euid_str.split("-")[0]
            generated = format_euid(category, integer)
            assert generated == euid_str, (
                f"format_euid({category!r}, {integer}) = {generated!r}, "
                f"expected {euid_str!r}"
            )
            assert validate_euid(euid_str) is True, (
                f"validate_euid({euid_str!r}) should be True"
            )

    def test_valid_domain_vectors(self, vectors):
        """Each valid_domain vector must round-trip through format_euid."""
        for v in vectors["valid_domain"]:
            euid_str = v["euid"]
            integer = v["integer"]
            allowed = v.get("allowed_domain_codes", v.get("allowed_sandbox_prefixes"))
            # Parse domain_code:category-bodycheck
            dc = euid_str.split(":")[0]
            category = euid_str.split(":")[1].split("-")[0]
            generated = format_euid(category, integer, domain_code=dc)
            assert generated == euid_str, (
                f"format_euid({category!r}, {integer}, domain_code={dc!r}) "
                f"= {generated!r}, expected {euid_str!r}"
            )
            assert (
                validate_euid(
                    euid_str,
                    environment="domain",
                    allowed_domain_codes=allowed,
                )
                is True
            )

    def test_invalid_vectors(self, vectors):
        """Each invalid vector must be rejected by validate_euid."""
        for v in vectors["invalid"]:
            euid_str = v["euid"]
            ctx = v.get("context", {})
            env = ctx.get("environment", "production")
            allowed = ctx.get("allowed_domain_codes", ctx.get("allowed_sandbox_prefixes"))
            result = validate_euid(
                euid_str,
                environment=env,
                allowed_domain_codes=allowed,
            )
            assert result is False, (
                f"validate_euid({euid_str!r}) should be False: {v['reason']}"
            )
