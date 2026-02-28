"""Tests for EUID configuration."""

import pytest


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
