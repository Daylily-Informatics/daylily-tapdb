"""Part 1 §8 — Domain scoping unit tests.

Tests:
- fail-fast on missing domain_code
- cross-domain isolation (no cache bleed, no query bleed)
- audit & stats scoping
- template manager cache isolation
"""

from __future__ import annotations

from unittest import mock

import pytest

from daylily_tapdb.euid import (
    normalize_domain_code,
    resolve_runtime_domain_code,
    validate_euid,
)

# ---------------------------------------------------------------------------
# §8.1 — fail-fast tests
# ---------------------------------------------------------------------------


class TestFailFastDomainCode:
    """domain_code must be provided; empty string raises."""

    def test_empty_env_var_raises(self):
        with pytest.raises(ValueError, match="empty string"):
            resolve_runtime_domain_code({"MERIDIAN_DOMAIN_CODE": ""})

    def test_missing_env_var_raises(self):
        with pytest.raises(ValueError, match="MERIDIAN_DOMAIN_CODE is required"):
            resolve_runtime_domain_code({})

    def test_explicit_valid_code(self):
        assert resolve_runtime_domain_code({"MERIDIAN_DOMAIN_CODE": "abcd"}) == "ABCD"

    def test_invalid_chars_raises(self):
        with pytest.raises(ValueError):
            normalize_domain_code("X@Z")

    def test_too_long_raises(self):
        with pytest.raises(ValueError):
            normalize_domain_code("ABCDE")  # 5 chars


class TestFailFastConnection:
    """TAPDBConnection must raise on missing domain_code / owner_repo_name."""

    def test_missing_owner_repo_raises(self, monkeypatch):
        from daylily_tapdb import connection as m

        monkeypatch.setenv("MERIDIAN_DOMAIN_CODE", "T")
        monkeypatch.delenv("TAPDB_OWNER_REPO", raising=False)

        # Patch engine creation to avoid real PG
        monkeypatch.setattr(m, "create_engine", lambda *a, **kw: mock.MagicMock())
        monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)

        with pytest.raises(ValueError, match="TAPDB_OWNER_REPO is required"):
            m.TAPDBConnection(db_url="postgresql://x", owner_repo_name=None)


# ---------------------------------------------------------------------------
# §8.2 — domain validation & normalization
# ---------------------------------------------------------------------------


class TestDomainCodeValidation:
    """Crockford Base32, 1-4 chars, normalized to uppercase."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("t", "T"),
            ("tapd", "TAPD"),
            ("AB", "AB"),
            ("X", "X"),
        ],
    )
    def test_valid_codes(self, raw, expected):
        assert normalize_domain_code(raw) == expected

    def test_empty_returns_none(self):
        assert normalize_domain_code("") is None

    @pytest.mark.parametrize("bad", ["X@Y", "12345", "hello"])
    def test_invalid_raises(self, bad):
        with pytest.raises(ValueError):
            normalize_domain_code(bad)


# ---------------------------------------------------------------------------
# §8.3 — EUID domain environment validation
# ---------------------------------------------------------------------------


class TestEuidDomainEnvironment:
    """validate_euid must reject cross-environment EUIDs."""

    def test_domain_mismatch_is_rejected(self):
        assert validate_euid("Z-AGX-1Q", allowed_domain_codes=["T"]) is False

    def test_domain_match_is_accepted(self):
        assert validate_euid("Z-AGX-1Q", allowed_domain_codes=["Z"]) is True


# ---------------------------------------------------------------------------
# §8.4 — TemplateManager cache isolation
# ---------------------------------------------------------------------------


class TestTemplateCacheIsolation:
    """Cache keys must include domain — no cross-domain bleed."""

    def test_cache_key_includes_domain(self):
        from daylily_tapdb.templates.manager import TemplateManager

        tm = TemplateManager()
        mock_session = mock.MagicMock()
        # Template code must be 4-part: category/type/subtype/version
        code = "SYS/actor/system_user/1.0"

        # session.query(...).filter(...).first() returns None (no hit)
        mock_session.query.return_value.filter.return_value.filter.return_value.first.return_value = None
        mock_session.query.return_value.filter.return_value.first.return_value = None

        result_a = tm.get_template(mock_session, code, domain_code="A")
        result_b = tm.get_template(mock_session, code, domain_code="B")

        # Both should have queried (different cache keys prevent bleed)
        assert mock_session.query.call_count >= 2
        assert result_a is None
        assert result_b is None


# ---------------------------------------------------------------------------
# §8.5 — Factory domain scoping
# ---------------------------------------------------------------------------


class TestFactoryDomainScoping:
    """InstanceFactory passes domain through to get_template."""

    def test_factory_passes_scope_kwargs(self):
        from daylily_tapdb.factory.instance import InstanceFactory

        mock_tm = mock.MagicMock()
        factory = InstanceFactory(mock_tm, domain_code="X")

        assert factory._scope_kwargs == {"domain_code": "X"}

    def test_factory_no_scope_when_none(self):
        from daylily_tapdb.factory.instance import InstanceFactory

        mock_tm = mock.MagicMock()
        factory = InstanceFactory(mock_tm)

        assert factory._scope_kwargs == {}
