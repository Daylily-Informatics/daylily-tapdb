"""
TAPDB Admin Authentication.

Session-based authentication with role-based access control.
"""

import os
from functools import wraps
from typing import Any, Callable, Optional

from fastapi import Request, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy import text

from admin.cognito import get_cognito_auth
from daylily_tapdb import TAPDBConnection
from daylily_tapdb.cli.db_config import get_db_config_for_env


# Session cookie settings
SESSION_COOKIE_NAME = "tapdb_session"
SESSION_MAX_AGE = 86400  # 24 hours


def get_db() -> TAPDBConnection:
    """Get database connection using the canonical config loader.

    Config resolution order (highest precedence first):
    1. TAPDB_<ENV>_* environment variables
    2. PG* environment variables
    3. ~/.config/tapdb/tapdb-config.yaml or ./config/tapdb-config.yaml
    4. Hard-coded defaults
    """
    env = os.environ.get("TAPDB_ENV", "dev").lower()
    cfg = get_db_config_for_env(env)

    return TAPDBConnection(
        db_hostname=f"{cfg['host']}:{cfg['port']}",
        db_user=cfg["user"],
        db_pass=cfg["password"],
        db_name=cfg["database"],
    )

def get_user_by_username(username: str) -> Optional[dict]:
    """Fetch user from database by username or email."""
    with get_db() as conn:
        # Best-effort attribution for audit triggers.
        conn.app_username = username
        with conn.session_scope() as session:
            result = session.execute(
                text("""
                    SELECT uuid, username, email, display_name, role, is_active,
                           require_password_change
                    FROM tapdb_user
                    WHERE (username = :username OR email = :username)
                      AND is_active = TRUE
                """),
                {"username": username}
            ).fetchone()

        if result:
            return {
                "uuid": int(result[0]),
                "username": result[1],
                "email": result[2],
                "display_name": result[3],
                "role": result[4],
                "is_active": result[5],
                "require_password_change": result[6],
            }
    return None


def get_or_create_user_from_email(
    email: str,
    *,
    display_name: Optional[str] = None,
    role: str = "user",
) -> dict:
    """Ensure a TAPDB user row exists for a Cognito identity email."""
    normalized = (email or "").strip().lower()
    if not normalized:
        raise ValueError("email is required")
    if role not in ("admin", "user"):
        raise ValueError(f"invalid role: {role}")

    existing = get_user_by_username(normalized)
    if existing:
        return existing

    with get_db() as conn:
        conn.app_username = normalized
        with conn.session_scope(commit=True) as session:
            result = session.execute(
                text(
                    """
                    INSERT INTO tapdb_user (
                        username, email, display_name, role,
                        is_active, require_password_change, password_hash
                    )
                    VALUES (
                        :username, :email, :display_name, :role,
                        TRUE, FALSE, NULL
                    )
                    RETURNING uuid, username, email, display_name, role, is_active,
                              require_password_change
                    """
                ),
                {
                    "username": normalized,
                    "email": normalized,
                    "display_name": display_name,
                    "role": role,
                },
            ).fetchone()

    if not result:
        # Fallback: another concurrent insert may have won.
        existing = get_user_by_username(normalized)
        if existing:
            return existing
        raise RuntimeError(f"Failed to provision TAPDB user row for {normalized}")

    return {
        "uuid": int(result[0]),
        "username": result[1],
        "email": result[2],
        "display_name": result[3],
        "role": result[4],
        "is_active": result[5],
        "require_password_change": result[6],
    }


def get_user_by_uuid(user_uuid: int | str) -> Optional[dict]:
    """Fetch user from database by integer primary key."""
    with get_db() as conn:
        conn.app_username = "system"
        with conn.session_scope() as session:
            result = session.execute(
                text("""
                    SELECT uuid, username, email, display_name, role, is_active,
                           require_password_change
                    FROM tapdb_user
                    WHERE uuid = :uuid AND is_active = TRUE
                """),
                {"uuid": user_uuid}
            ).fetchone()

        if result:
            return {
                "uuid": int(result[0]),
                "username": result[1],
                "email": result[2],
                "display_name": result[3],
                "role": result[4],
                "is_active": result[5],
                "require_password_change": result[6],
            }
    return None


def authenticate_with_cognito(username_or_email: str, password: str) -> dict:
    """Authenticate against Cognito using daylily-cognito."""
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


def update_last_login(user_uuid: int | str) -> None:
    """Update user's last login timestamp."""
    with get_db() as conn:
        conn.app_username = "system"
        with conn.session_scope(commit=True) as session:
            session.execute(
                text("UPDATE tapdb_user SET last_login_dt = NOW() WHERE uuid = :uuid"),
                {"uuid": user_uuid}
            )


async def get_current_user(request: Request) -> Optional[dict]:
    """Get current user from session."""
    user_uuid = request.session.get("user_uuid")
    if not user_uuid:
        return None
    user = get_user_by_uuid(user_uuid)
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
            return RedirectResponse("/login", status_code=302)

        # Check if password change required (except for change-password route itself)
        if user.get("require_password_change") and request.url.path != "/change-password":
            return RedirectResponse("/change-password", status_code=302)

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
            return RedirectResponse("/login", status_code=302)

        if user.get("require_password_change"):
            return RedirectResponse("/change-password", status_code=302)

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
