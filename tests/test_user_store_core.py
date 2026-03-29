from __future__ import annotations

from types import SimpleNamespace

import pytest

from daylily_tapdb import user_store as m


class _RowResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row

    def mappings(self):
        return self

    def first(self):
        return self._row

    def all(self):
        if self._row is None:
            return []
        if isinstance(self._row, list):
            return list(self._row)
        return [self._row]


class _FakeSession:
    def __init__(self, row=(1,)):
        self.row = row
        self.calls: list[tuple[str, dict[str, object]]] = []

    def execute(self, stmt, params=None):
        captured_params = dict(params or {})
        self.calls.append((str(stmt), captured_params))
        return _RowResult(self.row)


def test_normalize_login_identifier_normalizes_case_and_whitespace():
    assert m.normalize_login_identifier("  John@Example.com  ") == "john@example.com"


def test_normalize_login_identifier_rejects_empty():
    with pytest.raises(ValueError, match="login_identifier is required"):
        m.normalize_login_identifier("   ")


def test_norm_optional_handles_none_blank_and_value():
    assert m._norm_optional(None) is None
    assert m._norm_optional("   ") is None
    assert m._norm_optional(" value ") == "value"


def test_select_user_columns_contains_expected_aliases_and_filters():
    sql = m._select_user_columns()
    assert "FROM generic_instance gi" in sql
    assert "AS login_identifier" in sql
    assert "AS email" in sql
    assert "AS role" in sql
    assert "AS is_active" in sql
    assert "AS require_password_change" in sql
    assert "polymorphic_discriminator = 'actor_instance'" in sql
    assert "gi.type = 'actor'" in sql
    assert "gi.subtype = 'system_user'" in sql


def test_get_system_user_template_uid_returns_uid_and_uses_expected_params():
    session = _FakeSession(row=(42,))

    uid = m._get_system_user_template_uid(session)

    assert uid == 42
    stmt, params = session.calls[0]
    assert "FROM generic_template" in stmt
    assert params == {
        "category": m.SYSTEM_USER_TEMPLATE_CATEGORY,
        "type": m.SYSTEM_USER_TEMPLATE_TYPE,
        "subtype": m.SYSTEM_USER_TEMPLATE_SUBTYPE,
        "version": m.SYSTEM_USER_TEMPLATE_VERSION,
    }


def test_get_system_user_template_uid_raises_when_template_missing():
    session = _FakeSession(row=None)
    with pytest.raises(RuntimeError, match="Run template seed first"):
        m._get_system_user_template_uid(session)


def test_set_last_login_writes_timestamp_and_uid(monkeypatch: pytest.MonkeyPatch):
    session = _FakeSession()
    monkeypatch.setattr(m, "utc_now_iso", lambda: "2026-03-29T12:30:00+00:00")

    m.set_last_login(session, "7")

    stmt, params = session.calls[0]
    assert "SET json_addl = jsonb_set" in stmt
    assert "modified_dt = NOW()" in stmt
    assert params["uid"] == 7
    assert params["last_login_dt"] == "2026-03-29T12:30:00+00:00"


def test_set_role_validates_role_and_normalizes_identifier():
    session = _FakeSession()

    ok = m.set_role(session, " Admin@Example.com ", "admin")

    assert ok is True
    stmt, params = session.calls[0]
    assert "RETURNING gi.uid" in stmt
    assert params["identifier"] == "admin@example.com"
    assert params["role"] == "admin"


def test_set_role_rejects_invalid_role():
    with pytest.raises(ValueError, match="invalid role"):
        m.set_role(_FakeSession(), "user@example.com", "owner")


def test_set_active_sets_boolean_flag():
    session = _FakeSession()

    ok = m.set_active(session, "User@Example.com", False)

    assert ok is True
    stmt, params = session.calls[0]
    assert "jsonb_set" in stmt
    assert "is_active" in stmt
    assert params["identifier"] == "user@example.com"
    assert params["is_active"] is False


def test_set_password_hash_without_require_password_change():
    session = _FakeSession()

    ok = m.set_password_hash(session, "USER@example.com", "  hash  ")

    assert ok is True
    stmt, params = session.calls[0]
    assert "password_hash" in stmt
    assert "require_password_change" not in stmt
    assert params["identifier"] == "user@example.com"
    assert params["password_hash"] == "hash"


def test_set_password_hash_with_require_password_change():
    session = _FakeSession()

    ok = m.set_password_hash(
        session,
        "USER@example.com",
        None,
        require_password_change=True,
    )

    assert ok is True
    stmt, params = session.calls[0]
    assert "require_password_change" in stmt
    assert params["identifier"] == "user@example.com"
    assert params["password_hash"] is None
    assert params["require_password_change"] is True


def test_soft_delete_marks_deleted():
    session = _FakeSession()

    ok = m.soft_delete(session, "DeleteMe@Example.com")

    assert ok is True
    stmt, params = session.calls[0]
    assert "SET is_deleted = TRUE" in stmt
    assert params["identifier"] == "deleteme@example.com"


def test_set_display_timezone_normalizes_timezone():
    session = _FakeSession()

    ok = m.set_display_timezone(session, "TZUser@Example.com", "GMT")

    assert ok is True
    stmt, params = session.calls[0]
    assert "{preferences,display_timezone}" in stmt
    assert params["identifier"] == "tzuser@example.com"
    assert params["display_timezone"] == "UTC"


def test_get_display_timezone_by_login_or_email_normalizes_returned_timezone(
    monkeypatch: pytest.MonkeyPatch,
):
    calls: list[tuple[str, bool]] = []

    def _fake_get_by_login_or_email(_session, identifier, *, include_inactive=False):
        calls.append((identifier, include_inactive))
        return SimpleNamespace(preferences={"display_timezone": "GMT"})

    monkeypatch.setattr(m, "get_by_login_or_email", _fake_get_by_login_or_email)

    tz = m.get_display_timezone_by_login_or_email(
        object(),
        "USER@example.com",
        include_inactive=True,
    )

    assert tz == "UTC"
    assert calls == [("USER@example.com", True)]


def test_get_by_login_identifier_returns_actor_user_and_applies_active_filter():
    row = {
        "uid": 9,
        "euid": "AX-9",
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
        "preferences": {"display_timezone": "UTC"},
    }
    session = _FakeSession(row=row)

    user = m.get_by_login_identifier(session, " John@Example.com ")

    assert user is not None
    assert user.username == "john@example.com"
    stmt, params = session.calls[0]
    assert "login_identifier" in stmt
    assert "AND COALESCE(NULLIF(gi.json_addl->>'is_active', '')::boolean, TRUE) = TRUE" in stmt
    assert params["identifier"] == "john@example.com"


def test_get_by_login_or_email_supports_inactive_lookup():
    row = {
        "uid": 10,
        "euid": "AX-10",
        "created_dt": None,
        "modified_dt": None,
        "login_identifier": "john@example.com",
        "email": "john@example.com",
        "display_name": "John",
        "role": "user",
        "is_active": False,
        "require_password_change": True,
        "password_hash": None,
        "last_login_dt": None,
        "cognito_username": None,
        "preferences": {},
    }
    session = _FakeSession(row=row)

    user = m.get_by_login_or_email(
        session,
        " John@Example.com ",
        include_inactive=True,
    )

    assert user is not None
    assert user.require_password_change is True
    stmt, params = session.calls[0]
    assert "OR lower(COALESCE(gi.json_addl->>'email', '')) = :identifier" in stmt
    assert "is_active" not in stmt or "= TRUE" not in stmt
    assert params["identifier"] == "john@example.com"


def test_get_by_uid_and_list_users_map_rows():
    row = {
        "uid": 11,
        "euid": "AX-11",
        "created_dt": None,
        "modified_dt": None,
        "login_identifier": "jane@example.com",
        "email": "jane@example.com",
        "display_name": "Jane",
        "role": "user",
        "is_active": True,
        "require_password_change": False,
        "password_hash": None,
        "last_login_dt": None,
        "cognito_username": None,
        "preferences": {},
    }

    one = _FakeSession(row=row)
    listed = _FakeSession(row=[row])

    user = m.get_by_uid(one, "11")
    users = m.list_users(listed)

    assert user is not None
    assert user.uid == 11
    assert users and users[0].username == "jane@example.com"
    assert "ORDER BY lower" in listed.calls[0][0]


def test_create_or_get_returns_existing_without_insert(monkeypatch: pytest.MonkeyPatch):
    existing = SimpleNamespace(uid=22, username="john@example.com")
    session = _FakeSession()
    monkeypatch.setattr(
        m,
        "get_by_login_identifier",
        lambda _session, _login, include_inactive=False: existing,
    )

    user, created = m.create_or_get(
        session,
        login_identifier="John@example.com",
    )

    assert user is existing
    assert created is False
    assert session.calls == []


def test_create_or_get_inserts_new_user(monkeypatch: pytest.MonkeyPatch):
    session = _FakeSession(row=(77,))
    created_user = SimpleNamespace(uid=77, username="new@example.com")
    lookup_calls: list[tuple[str, bool]] = []

    def _fake_get_by_login_identifier(_session, login_identifier, include_inactive=False):
        lookup_calls.append((login_identifier, include_inactive))
        return None

    monkeypatch.setattr(m, "get_by_login_identifier", _fake_get_by_login_identifier)
    monkeypatch.setattr(m, "_get_system_user_template_uid", lambda _session: 5)
    monkeypatch.setattr(m, "utc_now_iso", lambda: "2026-03-29T13:00:00+00:00")
    monkeypatch.setattr(
        m,
        "get_by_uid",
        lambda _session, uid, include_inactive=False: created_user,
    )

    user, created = m.create_or_get(
        session,
        login_identifier="New@example.com",
        email="New@example.com",
        display_name="New User",
        role="admin",
        password_hash="hash",
    )

    assert created is True
    assert user is created_user
    stmt, params = session.calls[0]
    assert "INSERT INTO generic_instance" in stmt
    assert params["template_uid"] == 5
    assert "new@example.com" in params["json_addl"]
    assert lookup_calls == [("new@example.com", True)]


def test_create_or_get_retries_lookup_after_integrity_error(monkeypatch: pytest.MonkeyPatch):
    existing = SimpleNamespace(uid=88, username="race@example.com")
    calls = {"count": 0}

    class _IntegritySession:
        def execute(self, stmt, params=None):
            calls["count"] += 1
            raise m.IntegrityError("insert failed", params, Exception("boom"))

    def _fake_get_by_login_identifier(_session, login_identifier, include_inactive=False):
        if calls["count"] == 0:
            return None
        return existing

    monkeypatch.setattr(m, "get_by_login_identifier", _fake_get_by_login_identifier)
    monkeypatch.setattr(m, "_get_system_user_template_uid", lambda _session: 5)
    monkeypatch.setattr(m, "utc_now_iso", lambda: "2026-03-29T13:00:00+00:00")

    user, created = m.create_or_get(
        _IntegritySession(),
        login_identifier="Race@example.com",
    )

    assert user is existing
    assert created is False


def test_create_or_get_rejects_invalid_role():
    with pytest.raises(ValueError, match="invalid role"):
        m.create_or_get(_FakeSession(), login_identifier="user@example.com", role="owner")
