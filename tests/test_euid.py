"""Tests for EUID configuration and Meridian conformance."""

import json
from pathlib import Path

import pytest

from daylily_tapdb.euid import (
    crockford_base32_encode,
    format_euid,
    meridian_checksum,
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
        assert "GT" in euid_config.CORE_PREFIXES
        assert "GX" in euid_config.CORE_PREFIXES
        assert "GL" in euid_config.CORE_PREFIXES

    def test_optional_prefixes(self, euid_config):
        """Test that optional prefixes are present."""
        assert "WX" in euid_config.OPTIONAL_PREFIXES
        assert "WSX" in euid_config.OPTIONAL_PREFIXES
        assert "XX" in euid_config.OPTIONAL_PREFIXES

    def test_register_prefix(self, euid_config):
        """Test registering a new prefix."""
        euid_config.register_prefix("CX", "container_instance")
        assert "CX" in euid_config.application_prefixes
        assert euid_config.application_prefixes["CX"] == "container_instance"

    def test_register_duplicate_raises(self, euid_config):
        """Test that registering duplicate prefix raises error."""
        euid_config.register_prefix("CX", "container_instance")
        with pytest.raises(ValueError):
            euid_config.register_prefix("CX", "other_instance")

    def test_cannot_override_core(self, euid_config):
        """Test that core prefixes cannot be overridden."""
        with pytest.raises(ValueError):
            euid_config.register_prefix("GT", "something")

    def test_get_prefix_for_discriminator(self, euid_config):
        """Test getting prefix for discriminator."""
        assert euid_config.get_prefix_for_discriminator("generic_template") == "GT"
        assert euid_config.get_prefix_for_discriminator("generic_instance") == "GX"
        assert euid_config.get_prefix_for_discriminator("workflow_instance") == "WX"
        assert euid_config.get_prefix_for_discriminator("unknown") is None

    def test_get_all_prefixes(self, euid_config):
        """Test getting all prefixes."""
        euid_config.register_prefix("CX", "container_instance")
        all_prefixes = euid_config.get_all_prefixes()
        assert "GT" in all_prefixes
        assert "WX" in all_prefixes
        assert "CX" in all_prefixes


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
        euid = format_euid("GX", 42)
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

    def test_valid_sandbox_vectors(self, vectors):
        """Each valid_sandbox vector must round-trip through format_euid."""
        for v in vectors["valid_sandbox"]:
            euid_str = v["euid"]
            integer = v["integer"]
            allowed = v["allowed_sandbox_prefixes"]
            # Parse sandbox:category-bodycheck
            sandbox_prefix = euid_str[0]
            category = euid_str[2:].split("-")[0]
            generated = format_euid(category, integer, sandbox=sandbox_prefix)
            assert generated == euid_str, (
                f"format_euid({category!r}, {integer}, sandbox={sandbox_prefix!r}) "
                f"= {generated!r}, expected {euid_str!r}"
            )
            assert (
                validate_euid(
                    euid_str,
                    environment="sandbox",
                    allowed_sandbox_prefixes=allowed,
                )
                is True
            )

    def test_invalid_vectors(self, vectors):
        """Each invalid vector must be rejected by validate_euid."""
        for v in vectors["invalid"]:
            euid_str = v["euid"]
            ctx = v.get("context", {})
            env = ctx.get("environment", "production")
            allowed = ctx.get("allowed_sandbox_prefixes")
            result = validate_euid(
                euid_str,
                environment=env,
                allowed_sandbox_prefixes=allowed,
            )
            assert result is False, (
                f"validate_euid({euid_str!r}) should be False: {v['reason']}"
            )
