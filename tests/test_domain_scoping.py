"""Part 1 §8 — Domain/app scoping unit tests.

Tests:
- fail-fast on missing domain_code / issuer_app_code
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
    """TAPDBConnection must raise on missing domain_code / issuer_app_code."""

    def test_missing_app_code_raises(self, monkeypatch):
        from daylily_tapdb import connection as m

        monkeypatch.setenv("MERIDIAN_DOMAIN_CODE", "T")
        monkeypatch.delenv("TAPDB_APP_CODE", raising=False)

        # Patch engine creation to avoid real PG
        monkeypatch.setattr(m, "create_engine", lambda *a, **kw: mock.MagicMock())
        monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)

        with pytest.raises(ValueError, match="issuer_app_code is required"):
            m.TAPDBConnection(db_url="postgresql://x", issuer_app_code=None)


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

    def test_domain_prefixed_rejected_in_production(self):
        assert validate_euid("T:TX-1C") is False

    def test_production_rejected_in_domain_mode(self):
        assert (
            validate_euid("TX-1C", environment="domain", allowed_domain_codes=["T"])
            is False
        )


# ---------------------------------------------------------------------------
# §8.4 — TemplateManager cache isolation
# ---------------------------------------------------------------------------


class TestTemplateCacheIsolation:
    """Cache keys must include domain:app — no cross-domain bleed."""

    def test_cache_key_includes_domain_and_app(self):
        from daylily_tapdb.templates.manager import TemplateManager

        tm = TemplateManager()
        session_a = mock.MagicMock()
        session_b = mock.MagicMock()
        code = "cat/typ/sub/1.0"

        scope_a = mock.MagicMock()
        scope_a.one.return_value = ("A", "APP")
        scope_b = mock.MagicMock()
        scope_b.one.return_value = ("B", "APP")

        session_a.execute.return_value = scope_a
        session_b.execute.return_value = scope_b
        session_a.query.return_value.filter.return_value.first.return_value = None
        session_b.query.return_value.filter.return_value.first.return_value = None

        result_a = tm.get_template(session_a, code)
        result_b = tm.get_template(session_b, code)

        assert session_a.query.call_count == 1
        assert session_b.query.call_count == 1
        assert result_a is None
        assert result_b is None

    def test_get_template_rejects_scope_override_that_differs_from_session(self):
        from daylily_tapdb.templates.manager import TemplateManager

        tm = TemplateManager()
        session = mock.MagicMock()
        scope = mock.MagicMock()
        scope.one.return_value = ("A", "APP")
        session.execute.return_value = scope

        with pytest.raises(ValueError, match="domain_code override"):
            tm.get_template(session, "cat/typ/sub/1.0", domain_code="B")


# ---------------------------------------------------------------------------
# §8.5 — Factory domain/app scoping
# ---------------------------------------------------------------------------


class TestFactoryDomainScoping:
    """InstanceFactory treats constructor scope as a session invariant."""

    def test_factory_passes_scope_kwargs(self):
        from daylily_tapdb.factory.instance import InstanceFactory

        mock_tm = mock.MagicMock()
        factory = InstanceFactory(mock_tm, domain_code="X", issuer_app_code="APP1")

        assert factory._scope_kwargs == {"domain_code": "X", "issuer_app_code": "APP1"}

    def test_factory_no_scope_when_none(self):
        from daylily_tapdb.factory.instance import InstanceFactory

        mock_tm = mock.MagicMock()
        factory = InstanceFactory(mock_tm)

        assert factory._scope_kwargs == {}

    def test_factory_rejects_session_scope_mismatch(self):
        from daylily_tapdb.factory.instance import InstanceFactory

        factory = InstanceFactory(mock.MagicMock(), domain_code="X", issuer_app_code="APP1")
        session = mock.MagicMock()
        scope = mock.MagicMock()
        scope.one.return_value = ("Y", "APP1")
        session.execute.return_value = scope

        with pytest.raises(ValueError, match="domain_code override"):
            factory.create_instance(session, "cat/typ/sub/1.0", "x")
