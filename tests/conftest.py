"""Pytest configuration for daylily-tapdb tests."""

import pytest


@pytest.fixture
def euid_config():
    """Provide a fresh EUIDConfig for testing."""
    from daylily_tapdb.euid import EUIDConfig

    return EUIDConfig()
