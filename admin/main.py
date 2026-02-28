"""
TAPDB Admin Application.

FastAPI-based admin interface for managing TAPDB objects with Cytoscape DAG visualization.

Usage:
    uvicorn admin.main:app --reload --port 8911
"""
import os
import json
import logging
import secrets
from pathlib import Path
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request, Query, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from jinja2 import Environment, FileSystemLoader

from daylily_tapdb import TAPDBConnection, TemplateManager, InstanceFactory
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.cli.db_config import get_db_config_for_env
from sqlalchemy.exc import IntegrityError

from admin.auth import (
    get_current_user, require_auth, require_admin,
    get_user_by_username,
    authenticate_with_cognito,
    create_cognito_user_account,
    get_or_create_user_from_email,
    respond_to_new_password_challenge,
    change_cognito_password,
    update_last_login,
    get_user_permissions,
    SESSION_COOKIE_NAME,
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Jinja2 environment
templates = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))

APP_ENV = os.environ.get("TAPDB_ENV", "dev").lower()
IS_PROD = APP_ENV == "prod"

# Session secret key
if IS_PROD and not os.environ.get("TAPDB_SESSION_SECRET"):
    raise RuntimeError("Refusing to start in prod without TAPDB_SESSION_SECRET")
SESSION_SECRET = os.environ.get("TAPDB_SESSION_SECRET", secrets.token_hex(32))

# FastAPI app
app = FastAPI(
    title="TAPDB Admin",
    description="Admin interface for TAPDB - Templated Abstract Polymorphic Database",
    version="0.1.0",
)

# Session middleware (must be added before CORS)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie=SESSION_COOKIE_NAME,
    max_age=86400,
    same_site="lax",
    https_only=IS_PROD,
)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

def _parse_allowed_origins(raw: str) -> List[str]:
    return [v.strip() for v in (raw or "").split(",") if v.strip()]


allowed_origins_raw = os.environ.get("TAPDB_ADMIN_ALLOWED_ORIGINS", "")
allowed_origins = _parse_allowed_origins(allowed_origins_raw)

if IS_PROD:
    # In prod we refuse to start unless CORS is explicitly configured.
    # (Also reject common foot-gun values like whitespace-only or '*'.)
    if not allowed_origins:
        raise RuntimeError("Refusing to start in prod without TAPDB_ADMIN_ALLOWED_ORIGINS")
    if any(o == "*" for o in allowed_origins):
        raise RuntimeError("Refusing to start in prod with wildcard CORS origin '*'")
else:
    if not allowed_origins:
        # Safe local dev defaults
        allowed_origins = ["http://localhost:8911", "http://127.0.0.1:8911"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


def get_db() -> TAPDBConnection:
    """Get database connection.

    Uses the canonical config loader from daylily_tapdb.cli.db_config.
    Respects TAPDB_ENV environment variable (dev/test/prod).
    Defaults to 'dev' environment.

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


def get_style() -> Dict[str, str]:
    """Get default style configuration."""
    return {"skin_css": "/static/css/style.css"}


def _resolve_lineage_targets_or_raise(
    session,
    *,
    parent_euids: List[str],
    child_euids: List[str],
) -> tuple[List[generic_instance], List[generic_instance]]:
    """Resolve requested lineage targets or raise a validation error.

    All requested parent/child EUIDs must exist. If any are missing, this
    raises ``ValueError`` so callers can abort the entire create flow.
    """
    resolved_parents: List[generic_instance] = []
    resolved_children: List[generic_instance] = []
    missing_parents: List[str] = []
    missing_children: List[str] = []

    seen_parent_euids: set[str] = set()
    for parent_euid in parent_euids:
        if parent_euid in seen_parent_euids:
            continue
        seen_parent_euids.add(parent_euid)
        parent_instance = session.query(generic_instance).filter_by(
            euid=parent_euid, is_deleted=False
        ).first()
        if not parent_instance:
            missing_parents.append(parent_euid)
            continue
        resolved_parents.append(parent_instance)

    seen_child_euids: set[str] = set()
    for child_euid in child_euids:
        if child_euid in seen_child_euids:
            continue
        seen_child_euids.add(child_euid)
        child_instance = session.query(generic_instance).filter_by(
            euid=child_euid, is_deleted=False
        ).first()
        if not child_instance:
            missing_children.append(child_euid)
            continue
        resolved_children.append(child_instance)

    if missing_parents or missing_children:
        parts: List[str] = []
        if missing_parents:
            parts.append(f"missing parent EUID(s): {', '.join(missing_parents)}")
        if missing_children:
            parts.append(f"missing child EUID(s): {', '.join(missing_children)}")
        raise ValueError("; ".join(parts))

    return resolved_parents, resolved_children


def _new_graph_lineage(
    *,
    parent: generic_instance,
    child: generic_instance,
    relationship_type: str,
) -> generic_instance_lineage:
    """Build a lineage row with all non-null base fields populated."""
    rel = (relationship_type or "").strip() or "generic"
    return generic_instance_lineage(
        name=f"{parent.euid}->{child.euid}:{rel}",
        polymorphic_discriminator="generic_instance_lineage",
        category="lineage",
        type="lineage",
        subtype="generic",
        version="1.0",
        bstatus="active",
        parent_instance_uuid=parent.uuid,
        child_instance_uuid=child.uuid,
        relationship_type=rel,
        parent_type=parent.polymorphic_discriminator,
        child_type=child.polymorphic_discriminator,
        json_addl={},
    )


# ============================================================================
# Authentication Routes
# ============================================================================

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    """Login page."""
    # If already logged in, redirect to home
    user = await get_current_user(request)
    if user:
        if user.get("require_password_change"):
            return RedirectResponse("/change-password", status_code=302)
        return RedirectResponse("/", status_code=302)

    content = templates.get_template("login.html").render(
        request=request,
        style=get_style(),
        error=error,
    )
    return HTMLResponse(content=content)


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    """Handle login form submission."""
    identity = (username or "").strip()
    user = get_user_by_username(identity)
    cognito_username = (user.get("email") if user else identity) or identity

    try:
        auth_result = authenticate_with_cognito(cognito_username, password)
    except ValueError:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(),
            error="Invalid username or password",
        )
        return HTMLResponse(content=content)
    except Exception as e:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(),
            error=f"Authentication error: {e}",
        )
        return HTMLResponse(content=content)

    # Provision DB user row on first successful Cognito authentication.
    if not user:
        try:
            user = get_or_create_user_from_email(cognito_username)
        except Exception as e:
            content = templates.get_template("login.html").render(
                request=request,
                style=get_style(),
                error=(
                    "Authenticated with Cognito, but failed to provision TAPDB user: "
                    f"{e}"
                ),
            )
            return HTMLResponse(content=content)

    # Set session (used for app auth/authorization)
    request.session["user_uuid"] = user["uuid"]
    request.session["cognito_username"] = cognito_username
    update_last_login(user["uuid"])

    if auth_result.get("challenge") == "NEW_PASSWORD_REQUIRED":
        request.session["cognito_challenge"] = "NEW_PASSWORD_REQUIRED"
        request.session["cognito_challenge_session"] = auth_result.get("session", "")
        logger.info(f"User requires new Cognito password: {cognito_username}")
        return RedirectResponse("/change-password", status_code=302)

    access_token = auth_result.get("access_token")
    if not access_token:
        request.session.clear()
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(),
            error="Authentication failed: no access token returned",
        )
        return HTMLResponse(content=content)

    request.session["cognito_access_token"] = access_token
    request.session.pop("cognito_challenge", None)
    request.session.pop("cognito_challenge_session", None)

    logger.info(f"User logged in: {username}")

    # Redirect to password change if required
    if user.get("require_password_change"):
        return RedirectResponse("/change-password", status_code=302)

    return RedirectResponse("/", status_code=302)


@app.get("/signup", response_class=HTMLResponse)
async def signup_page(
    request: Request,
    error: Optional[str] = None,
):
    """Account creation page."""
    user = await get_current_user(request)
    if user:
        return RedirectResponse("/", status_code=302)

    content = templates.get_template("signup.html").render(
        request=request,
        style=get_style(),
        error=error,
    )
    return HTMLResponse(content=content)


@app.post("/signup", response_class=HTMLResponse)
async def signup_submit(
    request: Request,
    email: str = Form(...),
    display_name: Optional[str] = Form(None),
    password: str = Form(...),
    confirm_password: str = Form(...),
):
    """Create Cognito account and provision TAPDB user row."""
    normalized_email = (email or "").strip().lower()
    if not normalized_email or "@" not in normalized_email:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(),
            error="Valid email is required",
        )
        return HTMLResponse(content=content)

    if len(password) < 8:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(),
            error="Password must be at least 8 characters",
        )
        return HTMLResponse(content=content)

    if password != confirm_password:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(),
            error="Passwords do not match",
        )
        return HTMLResponse(content=content)

    try:
        create_cognito_user_account(
            normalized_email,
            password,
            display_name=display_name,
        )
    except ValueError as e:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(),
            error=str(e),
        )
        return HTMLResponse(content=content)
    except Exception as e:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(),
            error=f"Account creation failed: {e}",
        )
        return HTMLResponse(content=content)

    try:
        user = get_or_create_user_from_email(
            normalized_email,
            display_name=display_name,
            role="user",
        )
    except Exception as e:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(),
            error=(
                "Cognito account created, but TAPDB user provisioning failed: "
                f"{e}"
            ),
        )
        return HTMLResponse(content=content)

    try:
        auth_result = authenticate_with_cognito(normalized_email, password)
    except Exception as e:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(),
            error=(
                "Account created but auto-login failed. Please sign in manually. "
                f"Details: {e}"
            ),
        )
        return HTMLResponse(content=content)

    request.session["user_uuid"] = user["uuid"]
    request.session["cognito_username"] = normalized_email
    update_last_login(user["uuid"])

    if auth_result.get("challenge") == "NEW_PASSWORD_REQUIRED":
        request.session["cognito_challenge"] = "NEW_PASSWORD_REQUIRED"
        request.session["cognito_challenge_session"] = auth_result.get("session", "")
        return RedirectResponse("/change-password", status_code=302)

    access_token = auth_result.get("access_token")
    if access_token:
        request.session["cognito_access_token"] = access_token
    request.session.pop("cognito_challenge", None)
    request.session.pop("cognito_challenge_session", None)
    return RedirectResponse("/", status_code=302)


@app.get("/logout")
async def logout(request: Request):
    """Logout and clear session."""
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/change-password", response_class=HTMLResponse)
async def change_password_page(request: Request, error: Optional[str] = None, success: Optional[str] = None):
    """Password change page."""
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    challenge_required = (
        request.session.get("cognito_challenge") == "NEW_PASSWORD_REQUIRED"
    )
    content = templates.get_template("change_password.html").render(
        request=request,
        style=get_style(),
        user=user,
        required=user.get("require_password_change", False),
        challenge_required=challenge_required,
        error=error,
        success=success,
    )
    return HTMLResponse(content=content)


@app.post("/change-password", response_class=HTMLResponse)
async def change_password_submit(
    request: Request,
    current_password: Optional[str] = Form(None),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    """Handle password change form."""
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    # Validate new password
    if len(new_password) < 8:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            challenge_required=(
                request.session.get("cognito_challenge") == "NEW_PASSWORD_REQUIRED"
            ),
            error="New password must be at least 8 characters",
        )
        return HTMLResponse(content=content)

    if new_password != confirm_password:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            challenge_required=(
                request.session.get("cognito_challenge") == "NEW_PASSWORD_REQUIRED"
            ),
            error="New passwords do not match",
        )
        return HTMLResponse(content=content)

    challenge_required = (
        request.session.get("cognito_challenge") == "NEW_PASSWORD_REQUIRED"
    )
    if challenge_required:
        challenge_session = request.session.get("cognito_challenge_session", "")
        if not challenge_session:
            content = templates.get_template("change_password.html").render(
                request=request,
                style=get_style(),
                user=user,
                required=True,
                challenge_required=True,
                error="Missing Cognito challenge session. Please sign in again.",
            )
            return HTMLResponse(content=content)

        cognito_username = (
            request.session.get("cognito_username")
            or user.get("email")
            or user.get("username")
        )

        try:
            auth_result = respond_to_new_password_challenge(
                cognito_username,
                new_password,
                challenge_session,
            )
            access_token = auth_result.get("access_token")
            if access_token:
                request.session["cognito_access_token"] = access_token
            request.session.pop("cognito_challenge", None)
            request.session.pop("cognito_challenge_session", None)
            logger.info(f"Cognito NEW_PASSWORD_REQUIRED completed: {cognito_username}")
            return RedirectResponse("/", status_code=302)
        except ValueError as e:
            content = templates.get_template("change_password.html").render(
                request=request,
                style=get_style(),
                user=user,
                required=True,
                challenge_required=True,
                error=str(e),
            )
            return HTMLResponse(content=content)
        except Exception as e:
            content = templates.get_template("change_password.html").render(
                request=request,
                style=get_style(),
                user=user,
                required=True,
                challenge_required=True,
                error=f"Password update failed: {e}",
            )
            return HTMLResponse(content=content)

    if not current_password:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            challenge_required=False,
            error="Current password is required",
        )
        return HTMLResponse(content=content)

    access_token = request.session.get("cognito_access_token")
    if not access_token:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            challenge_required=False,
            error="Session missing Cognito access token. Please sign in again.",
        )
        return HTMLResponse(content=content)

    try:
        change_cognito_password(access_token, current_password, new_password)
        logger.info(f"Password changed for user: {user['username']}")
    except ValueError as e:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            challenge_required=False,
            error=str(e),
        )
        return HTMLResponse(content=content)
    except Exception as e:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            challenge_required=False,
            error=f"Password update failed: {e}",
        )
        return HTMLResponse(content=content)

    # If was required, redirect to home. Otherwise show success.
    if user.get("require_password_change"):
        return RedirectResponse("/", status_code=302)

    content = templates.get_template("change_password.html").render(
        request=request,
        style=get_style(),
        user=user,
        required=False,
        challenge_required=False,
        success="Password changed successfully",
    )
    return HTMLResponse(content=content)


# ============================================================================
# HTML Routes (Protected)
# ============================================================================

@app.get("/", response_class=HTMLResponse)
@require_auth
async def index(request: Request):
    """Home page with overview."""
    user = request.state.user
    permissions = get_user_permissions(user)

    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            template_count = session.query(generic_template).filter_by(is_deleted=False).count()
            instance_count = session.query(generic_instance).filter_by(is_deleted=False).count()
            lineage_count = session.query(generic_instance_lineage).filter_by(is_deleted=False).count()

    content = templates.get_template("index.html").render(
        request=request,
        style=get_style(),
        user=user,
        permissions=permissions,
        template_count=template_count,
        instance_count=instance_count,
        lineage_count=lineage_count,
    )
    return HTMLResponse(content=content)


@app.get("/templates", response_class=HTMLResponse)
@require_auth
async def list_templates(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    category: Optional[str] = None,
):
    """List all templates."""
    user = request.state.user
    permissions = get_user_permissions(user)


    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            query = session.query(generic_template).filter_by(is_deleted=False)
            if category:
                query = query.filter_by(category=category)
            query = query.order_by(generic_template.category, generic_template.type)

            total = query.count()
            items = query.offset((page - 1) * page_size).limit(page_size).all()

            # Get unique categories for filter
            categories = session.query(generic_template.category).distinct().all()
            categories = sorted([s[0] for s in categories if s[0]])

            content = templates.get_template("templates_list.html").render(
                request=request,
                style=get_style(),
                user=user,
                permissions=permissions,
                items=items,
                total=total,
                page=page,
                page_size=page_size,
                pages=(total + page_size - 1) // page_size,
                categories=categories,
                current_category=category,
            )
            return HTMLResponse(content=content)


@app.get("/instances", response_class=HTMLResponse)
@require_auth
async def list_instances(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    category: Optional[str] = None,
    type_: Optional[str] = None,
):
    """List all instances."""
    user = request.state.user
    permissions = get_user_permissions(user)


    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            query = session.query(generic_instance).filter_by(is_deleted=False)
            if category:
                query = query.filter_by(category=category)
            if type_:
                query = query.filter_by(type=type_)
            query = query.order_by(generic_instance.created_dt.desc())

            total = query.count()
            items = query.offset((page - 1) * page_size).limit(page_size).all()

            # Get unique categories for filter
            categories = session.query(generic_instance.category).distinct().all()
            categories = sorted([s[0] for s in categories if s[0]])

            content = templates.get_template("instances_list.html").render(
                request=request,
                style=get_style(),
                user=user,
                permissions=permissions,
                items=items,
                total=total,
                page=page,
                page_size=page_size,
                pages=(total + page_size - 1) // page_size,
                categories=categories,
                current_category=category,
                current_type=type_,
            )
            return HTMLResponse(content=content)


@app.get("/lineages", response_class=HTMLResponse)
@require_auth
async def list_lineages(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """List all lineages."""
    user = request.state.user
    permissions = get_user_permissions(user)

    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            query = session.query(generic_instance_lineage).filter_by(is_deleted=False)
            query = query.order_by(generic_instance_lineage.created_dt.desc())

            total = query.count()
            lineages = query.offset((page - 1) * page_size).limit(page_size).all()

            # Pre-extract data while session is open to avoid DetachedInstanceError
            items = []
            for lin in lineages:
                items.append(
                    {
                        "euid": lin.euid,
                        "parent_euid": lin.parent_instance.euid if lin.parent_instance else None,
                        "parent_name": lin.parent_instance.name if lin.parent_instance else None,
                        "child_euid": lin.child_instance.euid if lin.child_instance else None,
                        "child_name": lin.child_instance.name if lin.child_instance else None,
                        "relationship_type": lin.relationship_type,
                        "created_dt": lin.created_dt,
                    }
                )

            content = templates.get_template("lineages_list.html").render(
                request=request,
                style=get_style(),
                user=user,
                permissions=permissions,
                items=items,
                total=total,
                page=page,
                page_size=page_size,
                pages=(total + page_size - 1) // page_size,
            )
            return HTMLResponse(content=content)


@app.get("/object/{euid}", response_class=HTMLResponse)
@require_auth
async def object_detail(request: Request, euid: str):
    """View object details by EUID."""
    user = request.state.user
    permissions = get_user_permissions(user)

    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            # Try to find in templates first
            obj = session.query(generic_template).filter_by(euid=euid, is_deleted=False).first()
            obj_type = "template"

            if not obj:
                obj = session.query(generic_instance).filter_by(euid=euid, is_deleted=False).first()
                obj_type = "instance"

            if not obj:
                obj = session.query(generic_instance_lineage).filter_by(euid=euid, is_deleted=False).first()
                obj_type = "lineage"

            if not obj:
                raise HTTPException(status_code=404, detail=f"Object not found: {euid}")

            # Get relationships for instances - extract data while session is open
            parent_lineages = []
            child_lineages = []
            if obj_type == "instance":
                # Fetch lineages and eagerly load related instances
                for lin in obj.parent_of_lineages.filter_by(is_deleted=False).all():
                    parent_lineages.append({
                        "euid": lin.euid,
                        "child_euid": lin.child_instance.euid if lin.child_instance else None,
                        "child_name": lin.child_instance.name if lin.child_instance else None,
                        "relationship_type": lin.relationship_type,
                    })
                for lin in obj.child_of_lineages.filter_by(is_deleted=False).all():
                    child_lineages.append({
                        "euid": lin.euid,
                        "parent_euid": lin.parent_instance.euid if lin.parent_instance else None,
                        "parent_name": lin.parent_instance.name if lin.parent_instance else None,
                        "relationship_type": lin.relationship_type,
                    })

            # Render template inside session context to avoid detached instance errors
            content = templates.get_template("object_detail.html").render(
                request=request,
                style=get_style(),
                user=user,
                permissions=permissions,
                obj=obj,
                obj_type=obj_type,
                parent_lineages=parent_lineages,
                child_lineages=child_lineages,
            )
            return HTMLResponse(content=content)


@app.get("/graph", response_class=HTMLResponse)
@require_auth
async def graph_view(
    request: Request,
    start_euid: Optional[str] = None,
    depth: int = Query(4, ge=1, le=10),
):
    """DAG graph visualization."""
    user = request.state.user
    permissions = get_user_permissions(user)

    content = templates.get_template("graph.html").render(
        request=request,
        style=get_style(),
        user=user,
        permissions=permissions,
        start_euid=start_euid or "",
        depth=depth,
    )
    return HTMLResponse(content=content)


@app.get("/create-instance/{template_euid}", response_class=HTMLResponse)
@require_auth
async def create_instance_form(request: Request, template_euid: str):
    """Display form to create an instance from a template."""
    user = request.state.user
    permissions = get_user_permissions(user)

    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            template = session.query(generic_template).filter_by(
                euid=template_euid, is_deleted=False
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"Template not found: {template_euid}")

            # Get default properties from json_addl
            default_properties = template.json_addl or {}
            has_instantiation_layouts = bool(default_properties.get("instantiation_layouts"))

            content = templates.get_template("create_instance.html").render(
                request=request,
                style=get_style(),
                user=user,
                permissions=permissions,
                template=template,
                default_properties=default_properties,
                has_instantiation_layouts=has_instantiation_layouts,
                form_data=None,
                error=None,
                success=None,
                created_instance=None,
            )
            return HTMLResponse(content=content)


@app.post("/create-instance/{template_euid}", response_class=HTMLResponse)
@require_auth
async def create_instance_submit(request: Request, template_euid: str):
    """Handle instance creation form submission."""
    user = request.state.user
    permissions = get_user_permissions(user)

    # Parse form data
    form = await request.form()
    instance_name = form.get("instance_name", "").strip()
    create_children = form.get("create_children") == "true"

    # Parse parent/child EUIDs
    parent_euids_raw = form.get("parent_euids", "").strip()
    child_euids_raw = form.get("child_euids", "").strip()
    relationship_type = form.get("relationship_type", "contains").strip()

    # Parse comma-separated EUIDs into lists
    parent_euids = [e.strip() for e in parent_euids_raw.split(",") if e.strip()] if parent_euids_raw else []
    child_euids = [e.strip() for e in child_euids_raw.split(",") if e.strip()] if child_euids_raw else []

    # Collect custom properties from form
    custom_properties = {}
    for key, value in form.items():
        if key.startswith("prop_"):
            prop_name = key[5:]  # Remove "prop_" prefix
            # Try to parse JSON for complex types
            try:
                custom_properties[prop_name] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                # Handle boolean strings
                if value == "true":
                    custom_properties[prop_name] = True
                elif value == "false":
                    custom_properties[prop_name] = False
                else:
                    custom_properties[prop_name] = value

    form_data = {
        "instance_name": instance_name,
        "create_children": create_children,
        "parent_euids": parent_euids_raw,
        "child_euids": child_euids_raw,
        "relationship_type": relationship_type,
    }

    with get_db() as conn:
        conn.app_username = user.get("username")

        with conn.session_scope() as session:
            template = session.query(generic_template).filter_by(
                euid=template_euid, is_deleted=False
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"Template not found: {template_euid}")

            default_properties = template.json_addl or {}
            has_instantiation_layouts = bool(default_properties.get("instantiation_layouts"))
            template_code = f"{template.category}/{template.type}/{template.subtype}/{template.version}/"

            # Validate required fields
            if not instance_name:
                content = templates.get_template("create_instance.html").render(
                    request=request,
                    style=get_style(),
                    user=user,
                    permissions=permissions,
                    template=template,
                    default_properties=default_properties,
                    has_instantiation_layouts=has_instantiation_layouts,
                    form_data=form_data,
                    error="Instance name is required.",
                    success=None,
                    created_instance=None,
                )
                return HTMLResponse(content=content)

        # Create instance using InstanceFactory
        try:
            with conn.session_scope(commit=True) as session:
                template_manager = TemplateManager()
                factory = InstanceFactory(template_manager)

                parent_instances, child_instances = _resolve_lineage_targets_or_raise(
                    session,
                    parent_euids=parent_euids,
                    child_euids=child_euids,
                )

                instance = factory.create_instance(
                    session=session,
                    template_code=template_code,
                    name=instance_name,
                    properties=custom_properties if custom_properties else None,
                    create_children=create_children,
                )

                # Create lineage relationships for parent instances
                linked_parents = []
                for parent_instance in parent_instances:
                    factory.link_instances(
                        session=session,
                        parent=parent_instance,
                        child=instance,
                        relationship_type=relationship_type,
                    )
                    linked_parents.append(parent_instance.euid)

                # Create lineage relationships for child instances
                linked_children = []
                for child_instance in child_instances:
                    factory.link_instances(
                        session=session,
                        parent=instance,
                        child=child_instance,
                        relationship_type=relationship_type,
                    )
                    linked_children.append(child_instance.euid)

                # Capture EUID before exiting context (session may close)
                instance_euid = instance.euid

            # Log creation with relationship info
            log_msg = f"Created instance {instance_euid} from template {template_euid} by user {user['username']}"
            if linked_parents:
                log_msg += f" with parents: {linked_parents}"
            if linked_children:
                log_msg += f" with children: {linked_children}"
            logger.info(log_msg)

            # Redirect to the new instance
            return RedirectResponse(f"/object/{instance_euid}", status_code=302)

        except ValueError as e:
            with conn.session_scope() as session:
                template = session.query(generic_template).filter_by(
                    euid=template_euid, is_deleted=False
                ).first()
                default_properties = template.json_addl or {} if template else {}
                has_instantiation_layouts = bool(default_properties.get("instantiation_layouts"))

                content = templates.get_template("create_instance.html").render(
                    request=request,
                    style=get_style(),
                    user=user,
                    permissions=permissions,
                    template=template,
                    default_properties=default_properties,
                    has_instantiation_layouts=has_instantiation_layouts,
                    form_data=form_data,
                    error=f"Validation error: {str(e)}",
                    success=None,
                    created_instance=None,
                )
                return HTMLResponse(content=content)

        except Exception as e:
            logger.exception(f"Error creating instance from template {template_euid}")
            with conn.session_scope() as session:
                template = session.query(generic_template).filter_by(
                    euid=template_euid, is_deleted=False
                ).first()
                default_properties = template.json_addl or {} if template else {}
                has_instantiation_layouts = bool(default_properties.get("instantiation_layouts"))

                content = templates.get_template("create_instance.html").render(
                    request=request,
                    style=get_style(),
                    user=user,
                    permissions=permissions,
                    template=template,
                    default_properties=default_properties,
                    has_instantiation_layouts=has_instantiation_layouts,
                    form_data=form_data,
                    error=f"Error creating instance: {str(e)}",
                    success=None,
                    created_instance=None,
                )
                return HTMLResponse(content=content)


# ============================================================================
# API Routes
# ============================================================================

@app.get("/api/graph/data")
async def get_graph_data(
    start_euid: Optional[str] = None,
    depth: int = Query(4, ge=1, le=10),
):
    """Get graph data for Cytoscape visualization."""
    nodes = []
    edges = []
    visited_nodes = set()
    visited_edges = set()

    colors = {
        "workflow": "#00FF7F",
        "workflow_step": "#ADFF2F",
        "container": "#8B00FF",
        "content": "#00BFFF",
        "equipment": "#FF4500",
        "data": "#FFD700",
        "actor": "#FF69B4",
        "action": "#FF8C00",
        "test_requisition": "#FFA500",
        "health_event": "#DC143C",
        "file": "#00FF00",
        "subject": "#9370DB",
    }

    with get_db() as conn:
        with conn.session_scope() as session:
            if start_euid:
                # Start from specific node and traverse
                start_obj = session.query(generic_instance).filter_by(
                    euid=start_euid, is_deleted=False
                ).first()
                if not start_obj:
                    return {"elements": {"nodes": [], "edges": []}}

                def traverse(instance, current_depth):
                    if current_depth > depth or instance.euid in visited_nodes:
                        return
                    visited_nodes.add(instance.euid)

                    nodes.append({
                        "data": {
                            "id": instance.euid,
                            "name": instance.name or instance.euid,
                            "type": instance.type,
                            "category": instance.category,
                            "subtype": instance.subtype,
                            "color": colors.get(instance.category, "#888888"),
                        }
                    })

                    # Traverse children
                    for lin in instance.parent_of_lineages.filter_by(is_deleted=False):
                        if lin.euid not in visited_edges:
                            visited_edges.add(lin.euid)
                            edges.append({
                                "data": {
                                    "id": lin.euid,
									# Major wants directionality: child -> parent
									"source": lin.child_instance.euid,
									"target": instance.euid,
                                    "relationship_type": lin.relationship_type or "related",
                                }
                            })
                        traverse(lin.child_instance, current_depth + 1)

                    # Traverse parents
                    for lin in instance.child_of_lineages.filter_by(is_deleted=False):
                        if lin.euid not in visited_edges:
                            visited_edges.add(lin.euid)
                            edges.append({
                                "data": {
                                    "id": lin.euid,
									# Major wants directionality: child -> parent
									"source": instance.euid,
									"target": lin.parent_instance.euid,
                                    "relationship_type": lin.relationship_type or "related",
                                }
                            })
                        traverse(lin.parent_instance, current_depth + 1)

                traverse(start_obj, 0)
            else:
                # Get all instances (limited)
                instances = session.query(generic_instance).filter_by(
                    is_deleted=False
                ).limit(200).all()

                for inst in instances:
                    nodes.append({
                        "data": {
                            "id": inst.euid,
                            "name": inst.name or inst.euid,
                            "type": inst.type,
                            "category": inst.category,
                            "subtype": inst.subtype,
                            "color": colors.get(inst.category, "#888888"),
                        }
                    })
                    visited_nodes.add(inst.euid)

                # Get lineages for these instances
                lineages = session.query(generic_instance_lineage).filter_by(
                    is_deleted=False
                ).limit(500).all()

                for lin in lineages:
                    p_euid = lin.parent_instance.euid if lin.parent_instance else None
                    c_euid = lin.child_instance.euid if lin.child_instance else None
                    if p_euid in visited_nodes and c_euid in visited_nodes:
                        edges.append({
                            "data": {
                                "id": lin.euid,
								# Major wants directionality: child -> parent
								"source": c_euid,
								"target": p_euid,
                                "relationship_type": lin.relationship_type or "related",
                            }
                        })

    return {"elements": {"nodes": nodes, "edges": edges}}


@app.get("/api/templates")
async def api_list_templates(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    category: Optional[str] = None,
):
    """API: List templates."""
    with get_db() as conn:
        with conn.session_scope() as session:
            query = session.query(generic_template).filter_by(is_deleted=False)
            if category:
                query = query.filter_by(category=category)

            total = query.count()
            items = query.offset((page - 1) * page_size).limit(page_size).all()

            return {
                "items": [
                    {
                        "uuid": str(t.uuid),
                        "euid": t.euid,
                        "name": t.name,
                        "category": t.category,
                        "type": t.type,
                        "subtype": t.subtype,
                        "version": t.version,
                    }
                    for t in items
                ],
                "total": total,
                "page": page,
                "page_size": page_size,
            }


@app.get("/api/instances")
async def api_list_instances(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    category: Optional[str] = None,
):
    """API: List instances."""
    with get_db() as conn:
        with conn.session_scope() as session:
            query = session.query(generic_instance).filter_by(is_deleted=False)
            if category:
                query = query.filter_by(category=category)

            total = query.count()
            items = query.offset((page - 1) * page_size).limit(page_size).all()

            return {
                "items": [
                    {
                        "uuid": str(i.uuid),
                        "euid": i.euid,
                        "name": i.name,
                        "category": i.category,
                        "type": i.type,
                        "subtype": i.subtype,
                        "bstatus": i.bstatus,
                    }
                    for i in items
                ],
                "total": total,
                "page": page,
                "page_size": page_size,
            }


@app.get("/api/object/{euid}")
async def api_get_object(euid: str):
    """API: Get object by EUID."""
    with get_db() as conn:
        with conn.session_scope() as session:
            obj = session.query(generic_template).filter_by(euid=euid, is_deleted=False).first()
            obj_type = "template"

            if not obj:
                obj = session.query(generic_instance).filter_by(euid=euid, is_deleted=False).first()
                obj_type = "instance"

            if not obj:
                obj = session.query(generic_instance_lineage).filter_by(euid=euid, is_deleted=False).first()
                obj_type = "lineage"

            if not obj:
                raise HTTPException(status_code=404, detail=f"Object not found: {euid}")

            return {
                "uuid": str(obj.uuid),
                "euid": obj.euid,
                "name": obj.name,
                "type": obj_type,
                "category": obj.category,
                "obj_type": obj.type,
                "subtype": obj.subtype,
                "version": obj.version,
                "bstatus": obj.bstatus,
                "json_addl": obj.json_addl,
                "created_dt": obj.created_dt.isoformat() if obj.created_dt else None,
            }


@app.post("/api/lineage")
@require_admin
async def api_create_lineage(request: Request):
    """API: Create a new lineage between two instances. (Admin only)"""
    data = await request.json()
    parent_euid = data.get("parent_euid")
    child_euid = data.get("child_euid")
    relationship_type = (data.get("relationship_type") or "").strip() or "generic"

    if not parent_euid or not child_euid:
        raise HTTPException(status_code=400, detail="parent_euid and child_euid required")

    user = getattr(request.state, "user", None)
    with get_db() as conn:
        conn.app_username = (user or {}).get("username")
        with conn.session_scope(commit=True) as session:
            parent = session.query(generic_instance).filter_by(euid=parent_euid, is_deleted=False).first()
            child = session.query(generic_instance).filter_by(euid=child_euid, is_deleted=False).first()

            if not parent:
                raise HTTPException(status_code=404, detail=f"Parent not found: {parent_euid}")
            if not child:
                raise HTTPException(status_code=404, detail=f"Child not found: {child_euid}")

            existing = session.query(generic_instance_lineage).filter_by(
                parent_instance_uuid=parent.uuid,
                child_instance_uuid=child.uuid,
                relationship_type=relationship_type,
                is_deleted=False,
            ).first()
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Lineage already exists for "
                        f"{child_euid} -> {parent_euid} ({relationship_type})"
                    ),
                )

            lineage = _new_graph_lineage(
                parent=parent,
                child=child,
                relationship_type=relationship_type,
            )
            session.add(lineage)
            try:
                session.flush()
            except IntegrityError as exc:
                # Keep duplicate edge inserts as a clear client error.
                if "idx_lineage_unique_edge" in str(exc) or "duplicate key" in str(exc).lower():
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "Lineage already exists for "
                            f"{child_euid} -> {parent_euid} ({relationship_type})"
                        ),
                    ) from exc
                raise

            return {"success": True, "euid": lineage.euid, "uuid": str(lineage.uuid)}


@app.delete("/api/object/{euid}")
@require_admin
async def api_delete_object(request: Request, euid: str, hard_delete: bool = False):
    """API: Soft-delete an object by EUID. (Admin only)"""
    user = getattr(request.state, "user", None)
    with get_db() as conn:
        conn.app_username = (user or {}).get("username")
        with conn.session_scope(commit=True) as session:
            obj = session.query(generic_template).filter_by(euid=euid, is_deleted=False).first()
            if not obj:
                obj = session.query(generic_instance).filter_by(euid=euid, is_deleted=False).first()
            if not obj:
                obj = session.query(generic_instance_lineage).filter_by(euid=euid, is_deleted=False).first()

            if not obj:
                raise HTTPException(status_code=404, detail=f"Object not found: {euid}")

            obj.is_deleted = True
            session.flush()

            return {"success": True, "message": f"Object {euid} soft-deleted"}
