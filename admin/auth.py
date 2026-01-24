"""
TAPDB Admin Authentication.

Session-based authentication with role-based access control.
"""
import os
from functools import wraps
from typing import Optional, Callable, Any

from fastapi import Request, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy import text

from daylily_tapdb import TAPDBConnection
from daylily_tapdb.passwords import hash_password, verify_password


# Session cookie settings
SESSION_COOKIE_NAME = "tapdb_session"
SESSION_MAX_AGE = 86400  # 24 hours


def get_db() -> TAPDBConnection:
    """Get database connection (mirrors main.py)."""
    env = os.environ.get("TAPDB_ENV", "dev").lower()
    env_upper = env.upper()

    db_host = os.environ.get(f"TAPDB_{env_upper}_HOST", "localhost")
    db_port = os.environ.get(f"TAPDB_{env_upper}_PORT", "5432")
    db_user = os.environ.get(f"TAPDB_{env_upper}_USER", os.environ.get("USER", "tapdb"))
    db_pass = os.environ.get(f"TAPDB_{env_upper}_PASSWORD", "")
    db_name = os.environ.get(f"TAPDB_{env_upper}_DATABASE", f"tapdb_{env}")

    return TAPDBConnection(
        db_hostname=f"{db_host}:{db_port}",
        db_user=db_user,
        db_pass=db_pass,
        db_name=db_name,
    )

def get_user_by_username(username: str) -> Optional[dict]:
    """Fetch user from database by username."""
    with get_db() as conn:
        # Best-effort attribution for audit triggers.
        conn.app_username = username
        with conn.session_scope() as session:
            result = session.execute(
                text("""
                    SELECT uuid, username, email, display_name, role, is_active,
                           require_password_change, password_hash
                    FROM tapdb_user
                    WHERE username = :username AND is_active = TRUE
                """),
                {"username": username}
            ).fetchone()
        
        if result:
            return {
                "uuid": str(result[0]),
                "username": result[1],
                "email": result[2],
                "display_name": result[3],
                "role": result[4],
                "is_active": result[5],
                "require_password_change": result[6],
                "password_hash": result[7],
            }
    return None


def get_user_by_uuid(user_uuid: str) -> Optional[dict]:
    """Fetch user from database by UUID."""
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
                "uuid": str(result[0]),
                "username": result[1],
                "email": result[2],
                "display_name": result[3],
                "role": result[4],
                "is_active": result[5],
                "require_password_change": result[6],
            }
    return None


def update_last_login(user_uuid: str) -> None:
    """Update user's last login timestamp."""
    with get_db() as conn:
        conn.app_username = "system"
        with conn.session_scope(commit=True) as session:
            session.execute(
                text("UPDATE tapdb_user SET last_login_dt = NOW() WHERE uuid = :uuid"),
                {"uuid": user_uuid}
            )


def update_password(user_uuid: str, new_password: str) -> None:
    """Update user's password and clear require_password_change flag."""
    pw_hash = hash_password(new_password)
    with get_db() as conn:
        conn.app_username = "system"
        with conn.session_scope(commit=True) as session:
            session.execute(
                text("""
                    UPDATE tapdb_user 
                    SET password_hash = :hash, require_password_change = FALSE, modified_dt = NOW()
                    WHERE uuid = :uuid
                """),
                {"hash": pw_hash, "uuid": user_uuid}
            )


async def get_current_user(request: Request) -> Optional[dict]:
    """Get current user from session."""
    user_uuid = request.session.get("user_uuid")
    if not user_uuid:
        return None
    return get_user_by_uuid(user_uuid)


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

