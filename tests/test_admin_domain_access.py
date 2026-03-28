from __future__ import annotations

from unittest.mock import Mock

from fastapi.testclient import TestClient

import admin.main as admin_main
from admin.main import app


def test_admin_allows_approved_origin_preflight() -> None:
    with TestClient(app) as client:
        response = client.options(
            "/",
            headers={
                "Origin": "https://portal.lsmc.bio",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://portal.lsmc.bio"


def test_admin_rejects_disallowed_origin() -> None:
    with TestClient(app) as client:
        response = client.options(
            "/",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )

    assert response.status_code == 403
    assert response.text == "Origin not allowed"


def test_admin_rejects_disallowed_host() -> None:
    with TestClient(app) as client:
        response = client.get(
            "/", headers={"host": "evil.example.com"}, follow_redirects=False
        )

    assert response.status_code == 400


def test_admin_shutdown_cleanup_runs(monkeypatch) -> None:
    stop_all = Mock()
    dispose_all = Mock()
    monkeypatch.setattr(admin_main, "stop_all_writers", stop_all)
    monkeypatch.setattr(admin_main, "dispose_all_engines", dispose_all)

    with TestClient(app):
        pass

    stop_all.assert_called_once_with()
    dispose_all.assert_called_once_with()
