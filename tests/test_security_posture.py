"""Focused tests for security-sensitive configuration paths."""

from pathlib import Path

import pytest

from daylily_tapdb.aurora import connection as aurora_connection
from daylily_tapdb.gui import auth_routes
from daylily_tapdb.gui.router import _build_templates


def test_admin_templates_enable_html_autoescape():
    autoescape = _build_templates(None).autoescape
    assert callable(autoescape)
    assert autoescape("index.html") is True
    assert autoescape("index.txt") is False


@pytest.mark.parametrize(
    ("url", "label"),
    [
        ("http://example.com/oauth2/token", "Cognito token endpoint"),
        ("file:///tmp/local", "Cognito userInfo endpoint"),
    ],
)
def test_admin_https_helper_rejects_non_https_urls(url: str, label: str):
    with pytest.raises(RuntimeError, match="https URL"):
        auth_routes._require_https_url(url, label=label)


def test_aurora_https_helper_rejects_non_https_urls():
    with pytest.raises(RuntimeError, match="https URL"):
        aurora_connection._require_https_url(
            "http://example.com/bundle.pem",
            label="RDS CA bundle URL",
        )


def test_aurora_ca_bundle_rejects_non_https_download_url(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        aurora_connection,
        "_RDS_CA_BUNDLE_URL",
        "http://example.com/bundle.pem",
    )
    monkeypatch.setattr(
        aurora_connection,
        "_CA_BUNDLE_PATH",
        tmp_path / "rds-ca-bundle.pem",
    )
    monkeypatch.setattr(
        aurora_connection,
        "_CA_BUNDLE_DIR",
        tmp_path,
    )

    with pytest.raises(RuntimeError, match="https URL"):
        aurora_connection.AuroraConnectionBuilder.ensure_ca_bundle()


def test_env_selector_names_are_absent_from_security_surface():
    from daylily_tapdb.cli.db import Environment

    assert [item.value for item in Environment] == ["target"]
