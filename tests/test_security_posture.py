"""Focused tests for security-sensitive configuration paths."""

import tempfile
from pathlib import Path

import pytest

from admin import main as admin_main
from daylily_tapdb.aurora import connection as aurora_connection
from daylily_tapdb.cli.db import Environment
from daylily_tapdb.cli.pg import _get_instance_lock_file


def test_admin_templates_enable_html_autoescape():
    autoescape = admin_main.templates.autoescape
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
        admin_main._require_https_url(url, label=label)


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


def test_prod_instance_lock_uses_system_temp_dir():
    assert _get_instance_lock_file(Environment.prod) == (
        Path(tempfile.gettempdir()) / "tapdb-prod-instance.lock"
    )
