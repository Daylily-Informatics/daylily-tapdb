from __future__ import annotations

from fastapi.testclient import TestClient

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
