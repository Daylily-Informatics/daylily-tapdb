from __future__ import annotations

import base64
import json
import sys
from contextlib import contextmanager
from types import SimpleNamespace

import itsdangerous
import pytest

import admin.auth as auth
import admin.cognito as cognito
import admin.domain_access as domain_access
import admin.main as admin_main


def _request(*, cookies=None, session=None, root_path="") -> SimpleNamespace:
    return SimpleNamespace(
        cookies=cookies or {},
        session=session or {},
        scope={"path": "/protected", "root_path": root_path},
        state=SimpleNamespace(),
    )


def _signed_cookie(secret: str, value: object) -> str:
    encoded = base64.b64encode(json.dumps(value).encode())
    return itsdangerous.TimestampSigner(secret).sign(encoded).decode()


def test_admin_auth_configuration_and_cookie_rejection_branches(monkeypatch):
    monkeypatch.setattr(auth, "get_admin_settings", lambda: {"auth_mode": "DISABLED"})
    assert auth._admin_settings() == {"auth_mode": "DISABLED"}
    assert auth._auth_disabled() is True
    assert auth._shared_auth_enabled() is False

    monkeypatch.setattr(
        auth,
        "_admin_settings",
        lambda: {
            "disabled_user_email": "",
            "disabled_user_role": "owner",
            "shared_host_session_secret": "cookie-secret",
            "shared_host_session_cookie": "",
            "shared_host_session_max_age_seconds": "not-a-number",
        },
    )
    disabled = auth._disabled_auth_user()
    assert disabled["email"] == "tapdb-admin@localhost"
    assert disabled["role"] == "admin"
    assert auth._bloom_session_secret() == "cookie-secret"
    assert auth._bloom_session_cookie_name() == "session"
    assert auth._bloom_session_max_age() == 14 * 24 * 60 * 60

    assert auth._extract_bloom_user(_request()) is None
    cookie = _signed_cookie("cookie-secret", {"not_user_data": True})
    assert auth._extract_bloom_user(_request(cookies={"session": cookie})) is None
    cookie = _signed_cookie("cookie-secret", {"user_data": {"email": ""}})
    assert auth._extract_bloom_user(_request(cookies={"session": cookie})) is None

    monkeypatch.setattr(
        auth.itsdangerous.TimestampSigner,
        "unsign",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("decoder broke")),
    )
    assert auth._extract_bloom_user(_request(cookies={"session": "signed"})) is None

    monkeypatch.setattr(
        auth,
        "_admin_settings",
        lambda: {
            "disabled_user_email": "USER@EXAMPLE.COM",
            "disabled_user_role": "user",
        },
    )
    assert auth._disabled_auth_user()["email"] == "user@example.com"
    assert auth._disabled_auth_user()["role"] == "user"


def test_admin_auth_shared_disabled_and_url_normalization(monkeypatch):
    monkeypatch.setattr(auth, "_shared_auth_enabled", lambda: False)
    assert auth._resolve_shared_auth_user(_request()) is None

    monkeypatch.setattr(auth, "_shared_auth_enabled", lambda: True)
    monkeypatch.setattr(auth, "_extract_bloom_user", lambda _request: None)
    assert auth._resolve_shared_auth_user(_request()) is None

    request = _request(root_path="/tapdb/")
    assert auth._tapdb_base_path(request) == "/tapdb"
    assert auth._tapdb_url(request, "login") == "/tapdb/login"
    assert auth._tapdb_url(request, "") == "/tapdb"
    request.scope["root_path"] = object()
    assert auth._tapdb_base_path(request) == ""


class _StoredUser:
    def __init__(self, *, active=True):
        self.is_active = active

    def to_session_user(self):
        return {"uid": 17, "email": "person@example.com", "role": "user"}


class _Connection:
    def __init__(self):
        self.app_username = None
        self.session = object()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self, **_kwargs):
        yield self.session


def test_admin_auth_user_store_decisions(monkeypatch):
    connection = _Connection()
    monkeypatch.setattr(auth, "get_db_connection", lambda: connection)
    assert auth.get_db() is connection

    monkeypatch.setattr(auth, "get_by_login_or_email", lambda *_args, **_kwargs: None)
    assert auth.get_user_by_username("missing@example.com") is None
    assert connection.app_username == "missing@example.com"

    monkeypatch.setattr(auth, "get_actor_user_by_uid", lambda *_args, **_kwargs: None)
    assert auth.get_user_by_uid(88) is None
    assert connection.app_username == "system"

    with pytest.raises(ValueError, match="email is required"):
        auth.get_or_create_user_from_email("  ")
    with pytest.raises(ValueError, match="invalid role"):
        auth.get_or_create_user_from_email("person@example.com", role="owner")

    monkeypatch.setattr(
        auth,
        "create_or_get",
        lambda *_args, **_kwargs: (_StoredUser(active=False), False),
    )
    with pytest.raises(RuntimeError, match="is inactive"):
        auth.get_or_create_user_from_email("person@example.com")

    calls = []
    monkeypatch.setattr(
        auth, "set_last_login", lambda session, uid: calls.append((session, uid))
    )
    auth.update_last_login(29)
    assert calls == [(connection.session, 29)]


class _UsernameExistsError(Exception):
    pass


class _InvalidPasswordError(Exception):
    pass


class _CognitoExceptions:
    UsernameExistsException = _UsernameExistsError
    InvalidPasswordException = _InvalidPasswordError


class _CognitoClient:
    exceptions = _CognitoExceptions

    def __init__(self):
        self.failure = None
        self.calls = []

    def admin_create_user(self, **kwargs):
        self.calls.append(("create", kwargs))
        if self.failure:
            raise self.failure

    def admin_set_user_password(self, **kwargs):
        self.calls.append(("password", kwargs))


def test_admin_auth_cognito_account_outcomes(monkeypatch):
    client = _CognitoClient()
    adapter = SimpleNamespace(cognito=client, user_pool_id="pool-1")
    monkeypatch.setattr(auth, "get_cognito_auth", lambda: adapter)

    with pytest.raises(ValueError, match="Email is required"):
        auth.create_cognito_user_account("", "secret")

    auth.create_cognito_user_account(
        " PERSON@EXAMPLE.COM ", "secret", display_name=" Person "
    )
    assert client.calls[0][1]["Username"] == "person@example.com"
    assert {"Name": "name", "Value": "Person"} in client.calls[0][1]["UserAttributes"]
    auth.create_cognito_user_account("person@example.com", "secret")

    client.failure = client.exceptions.UsernameExistsException()
    with pytest.raises(ValueError, match="already exists"):
        auth.create_cognito_user_account("person@example.com", "secret")
    client.failure = client.exceptions.InvalidPasswordException()
    with pytest.raises(ValueError, match="policy requirements"):
        auth.create_cognito_user_account("person@example.com", "secret")
    client.failure = OSError("offline")
    with pytest.raises(RuntimeError, match="offline"):
        auth.create_cognito_user_account("person@example.com", "secret")


@pytest.mark.anyio
async def test_admin_auth_current_user_and_admin_redirect_branches(monkeypatch):
    monkeypatch.setattr(auth, "normalize_host_user", lambda _value: None)
    monkeypatch.setattr(auth, "_auth_disabled", lambda: False)
    monkeypatch.setattr(auth, "_resolve_shared_auth_user", lambda _request: None)
    assert await auth.get_current_user(_request()) is None

    request = _request(session={"user_uid": 10})
    monkeypatch.setattr(auth, "get_user_by_uid", lambda _uid: None)
    assert await auth.get_current_user(request) is None

    async def _no_user(_request):
        return None

    monkeypatch.setattr(auth, "get_current_user", _no_user)

    @auth.require_admin
    async def handler(request):
        return request.state.user

    response = await handler(_request(root_path="/tapdb"))
    assert response.status_code == 302
    assert response.headers["location"] == "/tapdb/login"

    async def _challenge(_request):
        return {"role": "admin", "require_password_change": True}

    monkeypatch.setattr(auth, "get_current_user", _challenge)
    response = await handler(_request(root_path="/tapdb"))
    assert response.status_code == 302
    assert response.headers["location"] == "/tapdb/change-password"
    assert auth.get_user_permissions(None) == {}


def test_admin_cognito_resolution_constructor_and_cache(monkeypatch):
    monkeypatch.setattr(
        cognito,
        "get_db_config",
        lambda: {
            "cognito_user_pool_id": "pool-1",
            "cognito_app_client_id": "client-1",
            "cognito_region": "us-east-2",
            "cognito_client_name": cognito.REQUIRED_COGNITO_CLIENT_NAME,
            "aws_profile": "profile-a",
            "config_path": "/tmp/tapdb.toml",
        },
    )
    created = []

    class _Auth:
        def __init__(self, **kwargs):
            created.append(kwargs)

    monkeypatch.setitem(
        sys.modules, "daylily_cognito", SimpleNamespace(CognitoAuth=_Auth)
    )
    cognito.clear_cognito_auth_cache()
    first = cognito.get_cognito_auth("target-a")
    assert first is cognito.get_cognito_auth("target-a")
    assert created == [
        {
            "region": "us-east-2",
            "user_pool_id": "pool-1",
            "app_client_id": "client-1",
            "profile": "profile-a",
        }
    ]
    cognito.clear_cognito_auth_cache()
    assert cognito.get_cognito_auth("target-a") is not first


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({"config_path": "/tmp/cfg"}, "user_pool_id"),
        (
            {
                "cognito_user_pool_id": "pool",
                "cognito_app_client_id": "client",
                "cognito_region": "us-east-2",
                "cognito_client_name": "wrong-client",
                "config_path": "/tmp/cfg",
            },
            "cognito_client_name",
        ),
        (
            {
                "cognito_user_pool_id": "pool",
                "cognito_app_client_id": "",
                "cognito_region": "us-east-2",
                "config_path": "/tmp/cfg",
            },
            "cognito_app_client_id",
        ),
        (
            {
                "cognito_user_pool_id": "pool",
                "cognito_app_client_id": "client",
                "cognito_region": "",
                "config_path": "/tmp/cfg",
            },
            "cognito_region",
        ),
    ],
)
def test_admin_cognito_missing_configuration_errors(monkeypatch, config, message):
    monkeypatch.setattr(cognito, "get_db_config", lambda: config)
    with pytest.raises(RuntimeError, match=message):
        cognito.resolve_tapdb_pool_config()


def test_admin_domain_access_full_decision_surface():
    assert domain_access._normalize_host("https://[::1]:8443") == "::1"
    assert domain_access._normalize_host("[unterminated") == ""
    assert domain_access.is_approved_domain("") is False
    assert domain_access.is_approved_domain("api.dyly.bio") is True
    assert domain_access.is_allowed_host("", allow_local=True) is False
    assert domain_access.is_allowed_host("portal.lsmc.bio", allow_local=False) is True
    assert domain_access.is_allowed_host("localhost", allow_local=False) is False
    assert domain_access.is_allowed_origin("", allow_local=True) is False
    assert (
        domain_access.is_allowed_origin("mailto:user@example.com", allow_local=True)
        is False
    )
    assert domain_access.is_allowed_origin("https://[", allow_local=True) is False
    assert domain_access.is_allowed_origin("http://localhost", allow_local=True) is True
    with pytest.raises(ValueError, match="outside the approved allowlist"):
        domain_access.validate_allowed_origins(
            ["https://outside.example"], allow_local=False
        )
    assert domain_access.validate_allowed_origins(
        ["", "https://api.dyly.bio/"], allow_local=False
    ) == ["https://api.dyly.bio"]

    trusted = domain_access.build_trusted_hosts(allow_local=True)
    assert "testserver" in trusted
    assert "testserver" not in domain_access.build_trusted_hosts(allow_local=False)
    regex = domain_access.build_allowed_origin_regex(allow_local=True)
    assert domain_access.re.fullmatch(regex, "http://localhost:8123")
    assert not domain_access.re.fullmatch(regex, "https://example.net")
    public_regex = domain_access.build_allowed_origin_regex(allow_local=False)
    assert not domain_access.re.fullmatch(public_regex, "http://localhost")


def test_admin_domain_access_rejects_an_unusable_parsed_host(monkeypatch):
    monkeypatch.setattr(domain_access, "_normalize_host", lambda _value: "")
    assert (
        domain_access.is_allowed_origin("https://api.dyly.bio", allow_local=False)
        is False
    )


@pytest.mark.anyio
async def test_admin_lifespan_rejects_import_time_config_failure(monkeypatch):
    monkeypatch.setattr(
        admin_main,
        "_ADMIN_SETTINGS_LOAD_ERROR",
        RuntimeError("malformed explicit config"),
    )

    with pytest.raises(RuntimeError, match="valid explicit target config"):
        async with admin_main._lifespan(admin_main.app):
            pytest.fail("lifespan must not start with defaulted settings")
