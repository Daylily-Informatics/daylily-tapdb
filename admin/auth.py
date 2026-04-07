"""
TAPDB Admin Authentication.

Session-based authentication with role-based access control.
"""

import json
from base64 import b64decode
from functools import wraps
from typing import Any, Callable, Optional

import itsdangerous
from fastapi import HTTPException, Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature

from admin.cognito import get_cognito_auth
from admin.db_pool import get_db_connection
from daylily_tapdb.cli.context import active_env_name
from daylily_tapdb.cli.db_config import get_admin_settings_for_env
from daylily_tapdb.user_store import (
    create_or_get,
    get_by_login_or_email,
    set_last_login,
)
from daylily_tapdb.user_store import (
    get_by_uid as get_actor_user_by_uid,
)
from daylily_tapdb.web.bridge import normalize_host_user

# Session cookie settings
SESSION_COOKIE_NAME = "tapdb_session"
SESSION_MAX_AGE = 86400  # 24 hours


def _admin_settings() -> dict[str, Any]:
    env_name = active_env_name("dev").strip().lower()
    return get_admin_settings_for_env(env_name)


def _auth_disabled() -> bool:
    """Return True when TAPDB admin auth is explicitly disabled."""
    return str(_admin_settings().get("auth_mode") or "").strip().lower() == "disabled"


def _shared_auth_enabled() -> bool:
    """Return True when TAPDB admin trusts Bloom's authenticated session."""
    return (
        str(_admin_settings().get("auth_mode") or "").strip().lower() == "shared_host"
    )


def _disabled_auth_user() -> dict:
    """Synthetic user used when TAPDB admin auth is disabled."""
    settings = _admin_settings()
    email = str(settings.get("disabled_user_email") or "").strip().lower()
    if not email:
        email = "tapdb-admin@localhost"

    role = str(settings.get("disabled_user_role") or "admin").strip().lower()
    if role not in {"admin", "user"}:
        role = "admin"

    return {
        "uid": 0,
        "username": email,
        "email": email,
        "display_name": "TAPDB Admin (Auth Disabled)",
        "role": role,
        "is_active": True,
        "require_password_change": False,
    }


def _bloom_session_secret() -> str:
    """Resolve the signing key used for Bloom's SessionMiddleware cookie."""
    return str(_admin_settings().get("shared_host_session_secret") or "").strip()


def _bloom_session_cookie_name() -> str:
    return str(_admin_settings().get("shared_host_session_cookie") or "session").strip()


def _bloom_session_max_age() -> int:
    raw = str(
        _admin_settings().get("shared_host_session_max_age_seconds") or ""
    ).strip()
    if raw.isdigit():
        return int(raw)
    return 14 * 24 * 60 * 60


def _extract_bloom_user(request: Request) -> Optional[dict]:
    """Decode Bloom's signed session cookie and extract user identity."""
    cookie_name = _bloom_session_cookie_name()
    raw_cookie = request.cookies.get(cookie_name)
    if not raw_cookie:
        return None

    signer = itsdangerous.TimestampSigner(_bloom_session_secret())
    try:
        payload = signer.unsign(
            raw_cookie.encode("utf-8"), max_age=_bloom_session_max_age()
        )
        data = json.loads(b64decode(payload))
    except (BadSignature, ValueError, json.JSONDecodeError):
        return None
    except Exception:
        return None

    user_data = data.get("user_data")
    if not isinstance(user_data, dict):
        return None

    email = str(user_data.get("email") or "").strip().lower()
    if not email:
        return None

    role = str(user_data.get("role") or "user").strip().lower()
    if role not in {"admin", "user"}:
        role = "user"

    return {"email": email, "role": role}


def _resolve_shared_auth_user(request: Request) -> Optional[dict]:
    """Resolve TAPDB user from Bloom session when shared auth is enabled."""
    if not _shared_auth_enabled():
        return None

    bloom_user = _extract_bloom_user(request)
    if not bloom_user:
        return None

    email = bloom_user["email"]
    user = get_user_by_username(email)
    if not user:
        try:
            user = get_or_create_user_from_email(email, role=bloom_user["role"])
        except Exception:
            return None

    request.session["user_uid"] = user["uid"]
    request.session["cognito_username"] = email
    request.session.pop("cognito_challenge", None)
    request.session.pop("cognito_challenge_session", None)
    return user


def _tapdb_base_path(request: Request) -> str:
    raw = request.scope.get("root_path") or ""
    if not isinstance(raw, str):
        return ""
    return raw.rstrip("/")


def _tapdb_url(request: Request, path: str) -> str:
    base = _tapdb_base_path(request)
    if not path:
        return base or ""
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}"


def get_db():
    """Get a DB connection using the active explicit TapDB env."""
    env = active_env_name("dev").lower()
    return get_db_connection(env)


def get_user_by_username(username: str) -> Optional[dict]:
    """Fetch user from database by username or email."""
    with get_db() as conn:
        # Best-effort attribution for audit triggers.
        conn.app_username = username
        with conn.session_scope() as session:
            user = get_by_login_or_email(session, username, include_inactive=False)
        if user:
            return user.to_session_user()
    return None


def get_or_create_user_from_email(
    email: str,
    *,
    display_name: Optional[str] = None,
    role: str = "user",
) -> dict:
    """Ensure an actor-backed TAPDB user row exists for a Cognito identity email."""
    normalized = (email or "").strip().lower()
    if not normalized:
        raise ValueError("email is required")
    if role not in ("admin", "user"):
        raise ValueError(f"invalid role: {role}")

    with get_db() as conn:
        conn.app_username = normalized
        with conn.session_scope(commit=True) as session:
            user, _ = create_or_get(
                session,
                login_identifier=normalized,
                email=normalized,
                display_name=display_name,
                role=role,
                is_active=True,
                require_password_change=False,
                password_hash=None,
                cognito_username=normalized,
            )
        if not user.is_active:
            raise RuntimeError(f"TAPDB user {normalized} is inactive")
        return user.to_session_user()


def get_user_by_uid(user_uid: int | str) -> Optional[dict]:
    """Fetch user from database by integer primary key."""
    with get_db() as conn:
        conn.app_username = "system"
        with conn.session_scope() as session:
            user = get_actor_user_by_uid(session, user_uid, include_inactive=False)
        if user:
            return user.to_session_user()
    return None


def authenticate_with_cognito(username_or_email: str, password: str) -> dict:
    """Authenticate against Cognito using daylily-auth-cognito."""
    auth = get_cognito_auth()
    return auth.authenticate(email=username_or_email, password=password)


def create_cognito_user_account(
    email: str,
    password: str,
    *,
    display_name: Optional[str] = None,
) -> None:
    """Create a Cognito user with a permanent password."""
    normalized = (email or "").strip().lower()
    if not normalized:
        raise ValueError("Email is required")

    auth = get_cognito_auth()
    attrs = [
        {"Name": "email", "Value": normalized},
        {"Name": "email_verified", "Value": "true"},
    ]
    if display_name:
        attrs.append({"Name": "name", "Value": display_name.strip()})

    try:
        auth.cognito.admin_create_user(
            UserPoolId=auth.user_pool_id,
            Username=normalized,
            TemporaryPassword=password,
            UserAttributes=attrs,
            MessageAction="SUPPRESS",
        )
        auth.cognito.admin_set_user_password(
            UserPoolId=auth.user_pool_id,
            Username=normalized,
            Password=password,
            Permanent=True,
        )
    except auth.cognito.exceptions.UsernameExistsException:
        raise ValueError("Account already exists")
    except auth.cognito.exceptions.InvalidPasswordException:
        raise ValueError("Password does not meet Cognito policy requirements")
    except Exception as e:
        raise RuntimeError(f"Failed to create Cognito account: {e}") from e


def respond_to_new_password_challenge(
    username_or_email: str, new_password: str, challenge_session: str
) -> dict:
    """Complete Cognito NEW_PASSWORD_REQUIRED challenge."""
    auth = get_cognito_auth()
    return auth.respond_to_new_password_challenge(
        email=username_or_email,
        new_password=new_password,
        session=challenge_session,
    )


def change_cognito_password(
    access_token: str, current_password: str, new_password: str
) -> None:
    """Change password for an authenticated Cognito user."""
    auth = get_cognito_auth()
    auth.change_password(
        access_token=access_token,
        old_password=current_password,
        new_password=new_password,
    )


def update_last_login(user_uid: int | str) -> None:
    """Update user's last login timestamp."""
    with get_db() as conn:
        conn.app_username = "system"
        with conn.session_scope(commit=True) as session:
            set_last_login(session, user_uid)


async def get_current_user(request: Request) -> Optional[dict]:
    """Get current user from session."""
    host_user = normalize_host_user(request.scope.get("tapdb_host_user"))
    if host_user:
        return host_user

    if _auth_disabled():
        return _disabled_auth_user()

    shared_user = _resolve_shared_auth_user(request)
    if shared_user:
        return shared_user

    user_uid = request.session.get("user_uid")
    if not user_uid:
        return None
    user = get_user_by_uid(user_uid)
    if not user:
        return None

    # Challenge state is tracked in session for Cognito login flows.
    if request.session.get("cognito_challenge") == "NEW_PASSWORD_REQUIRED":
        user["require_password_change"] = True
    return user


def require_auth(func: Callable) -> Callable:
    """Decorator: require any authenticated user.

    Redirects to login if not authenticated.
    Redirects to password change if required.
    Injects user into request.state.user
    """

    @wraps(func)
    async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
        user = await get_current_user(request)
        if not user:
            return RedirectResponse(_tapdb_url(request, "/login"), status_code=302)

        # Check if password change required (except for change-password route itself)
        if (
            user.get("require_password_change")
            and request.scope.get("path") != "/change-password"
        ):
            return RedirectResponse(
                _tapdb_url(request, "/change-password"), status_code=302
            )

        request.state.user = user
        return await func(request, *args, **kwargs)

    return wrapper


def require_admin(func: Callable) -> Callable:
    """Decorator: require admin role.

    Returns 403 if not admin.
    """

    @wraps(func)
    async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
        user = await get_current_user(request)
        if not user:
            return RedirectResponse(_tapdb_url(request, "/login"), status_code=302)

        if user.get("require_password_change"):
            return RedirectResponse(
                _tapdb_url(request, "/change-password"), status_code=302
            )

        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        request.state.user = user
        return await func(request, *args, **kwargs)

    return wrapper


# Permission definitions for role-based access
PERMISSIONS = {
    "user": {
        "can_view_templates": True,
        "can_view_instances": True,
        "can_view_lineages": True,
        "can_view_graph": True,
        "can_create_instance": False,
        "can_create_lineage": False,
        "can_delete_object": False,
        "can_manage_users": False,
    },
    "admin": {
        "can_view_templates": True,
        "can_view_instances": True,
        "can_view_lineages": True,
        "can_view_graph": True,
        "can_create_instance": True,
        "can_create_lineage": True,
        "can_delete_object": True,
        "can_manage_users": True,
    },
}


def get_user_permissions(user: Optional[dict]) -> dict:
    """Get permissions for a user based on their role."""
    if not user:
        return {}
    role = user.get("role", "user")
    return PERMISSIONS.get(role, PERMISSIONS["user"])
