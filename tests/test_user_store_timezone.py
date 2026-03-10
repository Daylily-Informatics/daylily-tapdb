from __future__ import annotations

from daylily_tapdb import user_store as m


def test_row_to_actor_user_defaults_display_timezone_to_utc():
    row = {
        "uid": 1,
        "euid": "AX-1",
        "created_dt": None,
        "modified_dt": None,
        "login_identifier": "john@example.com",
        "email": "john@example.com",
        "display_name": "John",
        "role": "admin",
        "is_active": True,
        "require_password_change": False,
        "password_hash": None,
        "last_login_dt": None,
        "cognito_username": None,
        "preferences": {},
    }
    user = m._row_to_actor_user(row)
    assert user.display_timezone == "UTC"
    assert user.preferences["display_timezone"] == "UTC"


def test_row_to_actor_user_normalizes_display_timezone_alias():
    row = {
        "uid": 1,
        "euid": "AX-1",
        "created_dt": None,
        "modified_dt": None,
        "login_identifier": "john@example.com",
        "email": "john@example.com",
        "display_name": "John",
        "role": "admin",
        "is_active": True,
        "require_password_change": False,
        "password_hash": None,
        "last_login_dt": None,
        "cognito_username": None,
        "preferences": {"display_timezone": "GMT"},
    }
    user = m._row_to_actor_user(row)
    assert user.display_timezone == "UTC"
    assert user.preferences["display_timezone"] == "UTC"


def test_set_display_timezone_by_login_or_email_normalizes_and_updates():
    executed: dict[str, object] = {}

    class FakeSession:
        def execute(self, stmt, params=None):
            executed["stmt"] = str(stmt)
            executed["params"] = dict(params or {})

            class _Row:
                def fetchone(self):
                    return (1,)

            return _Row()

    ok = m.set_display_timezone_by_login_or_email(
        FakeSession(),
        "John@Example.com",
        "GMT",
    )
    assert ok is True
    params = executed["params"]
    assert params["identifier"] == "john@example.com"
    assert params["display_timezone"] == "UTC"


def test_get_display_timezone_by_login_or_email_returns_default_when_missing(
    monkeypatch,
):
    monkeypatch.setattr(m, "get_by_login_or_email", lambda *_a, **_k: None)
    assert (
        m.get_display_timezone_by_login_or_email(object(), "missing@example.com")
        == "UTC"
    )
