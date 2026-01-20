"""
TAPDB Admin Application.

FastAPI-based admin interface for managing TAPDB objects with Cytoscape DAG visualization.

Usage:
    uvicorn admin.main:app --reload --port 8000
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

from admin.auth import (
    get_current_user, require_auth, require_admin,
    get_user_by_username, verify_password, update_last_login,
    update_password, get_user_permissions,
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

# Session secret key (use env var in production)
SESSION_SECRET = os.environ.get("TAPDB_SESSION_SECRET", secrets.token_hex(32))

# FastAPI app
app = FastAPI(
    title="TAPDB Admin",
    description="Admin interface for TAPDB - Templated Abstract Polymorphic Database",
    version="0.1.0",
)

# Session middleware (must be added before CORS)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, max_age=86400)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db() -> TAPDBConnection:
    """Get database connection.

    Respects TAPDB_ENV environment variable (dev/test/prod).
    Defaults to 'dev' environment.
    """
    env = os.environ.get("TAPDB_ENV", "dev").lower()
    env_upper = env.upper()

    # Check for environment-specific configuration
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


def get_style() -> Dict[str, str]:
    """Get default style configuration."""
    return {"skin_css": "/static/css/style.css"}


# ============================================================================
# Authentication Routes
# ============================================================================

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    """Login page."""
    # If already logged in, redirect to home
    user = await get_current_user(request)
    if user and not user.get("require_password_change"):
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
    user = get_user_by_username(username)

    if not user or not verify_password(password, user.get("password_hash", "")):
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(),
            error="Invalid username or password",
        )
        return HTMLResponse(content=content)

    # Set session
    request.session["user_uuid"] = user["uuid"]
    update_last_login(user["uuid"])

    logger.info(f"User logged in: {username}")

    # Redirect to password change if required
    if user.get("require_password_change"):
        return RedirectResponse("/change-password", status_code=302)

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

    content = templates.get_template("change_password.html").render(
        request=request,
        style=get_style(),
        user=user,
        required=user.get("require_password_change", False),
        error=error,
        success=success,
    )
    return HTMLResponse(content=content)


@app.post("/change-password", response_class=HTMLResponse)
async def change_password_submit(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    """Handle password change form."""
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    # Get full user with password hash
    full_user = get_user_by_username(user["username"])

    # Validate current password
    if not verify_password(current_password, full_user.get("password_hash", "")):
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            error="Current password is incorrect",
        )
        return HTMLResponse(content=content)

    # Validate new password
    if len(new_password) < 8:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            error="New password must be at least 8 characters",
        )
        return HTMLResponse(content=content)

    if new_password != confirm_password:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(),
            user=user,
            required=user.get("require_password_change", False),
            error="New passwords do not match",
        )
        return HTMLResponse(content=content)

    # Update password
    update_password(user["uuid"], new_password)
    logger.info(f"Password changed for user: {user['username']}")

    # If was required, redirect to home. Otherwise show success.
    if user.get("require_password_change"):
        return RedirectResponse("/", status_code=302)

    content = templates.get_template("change_password.html").render(
        request=request,
        style=get_style(),
        user=user,
        required=False,
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
        template_count = conn.session.query(generic_template).filter_by(is_deleted=False).count()
        instance_count = conn.session.query(generic_instance).filter_by(is_deleted=False).count()
        lineage_count = conn.session.query(generic_instance_lineage).filter_by(is_deleted=False).count()

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
        query = conn.session.query(generic_template).filter_by(is_deleted=False)
        if category:
            query = query.filter_by(category=category)
        query = query.order_by(generic_template.category, generic_template.type)

        total = query.count()
        items = query.offset((page - 1) * page_size).limit(page_size).all()

        # Get unique categories for filter
        categories = conn.session.query(generic_template.category).distinct().all()
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
        query = conn.session.query(generic_instance).filter_by(is_deleted=False)
        if category:
            query = query.filter_by(category=category)
        if type_:
            query = query.filter_by(type=type_)
        query = query.order_by(generic_instance.created_dt.desc())

        total = query.count()
        items = query.offset((page - 1) * page_size).limit(page_size).all()

        # Get unique categories for filter
        categories = conn.session.query(generic_instance.category).distinct().all()
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
        query = conn.session.query(generic_instance_lineage).filter_by(is_deleted=False)
        query = query.order_by(generic_instance_lineage.created_dt.desc())

        total = query.count()
        lineages = query.offset((page - 1) * page_size).limit(page_size).all()

        # Pre-extract data while session is open to avoid DetachedInstanceError
        items = []
        for lin in lineages:
            items.append({
                "euid": lin.euid,
                "parent_euid": lin.parent_instance.euid if lin.parent_instance else None,
                "parent_name": lin.parent_instance.name if lin.parent_instance else None,
                "child_euid": lin.child_instance.euid if lin.child_instance else None,
                "child_name": lin.child_instance.name if lin.child_instance else None,
                "relationship_type": lin.relationship_type,
                "created_dt": lin.created_dt,
            })

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
        # Try to find in templates first
        obj = conn.session.query(generic_template).filter_by(euid=euid, is_deleted=False).first()
        obj_type = "template"

        if not obj:
            obj = conn.session.query(generic_instance).filter_by(euid=euid, is_deleted=False).first()
            obj_type = "instance"

        if not obj:
            obj = conn.session.query(generic_instance_lineage).filter_by(euid=euid, is_deleted=False).first()
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
        template = conn.session.query(generic_template).filter_by(
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
        template = conn.session.query(generic_template).filter_by(
            euid=template_euid, is_deleted=False
        ).first()

        if not template:
            raise HTTPException(status_code=404, detail=f"Template not found: {template_euid}")

        default_properties = template.json_addl or {}
        has_instantiation_layouts = bool(default_properties.get("instantiation_layouts"))

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
            template_manager = TemplateManager(conn)
            factory = InstanceFactory(conn, template_manager)

            # Build template code from template
            template_code = f"{template.category}/{template.type}/{template.subtype}/{template.version}/"

            instance = factory.create_instance(
                template_code=template_code,
                name=instance_name,
                properties=custom_properties if custom_properties else None,
                create_children=create_children,
            )

            # Create lineage relationships for parent instances
            linked_parents = []
            for parent_euid in parent_euids:
                parent_instance = conn.session.query(generic_instance).filter_by(
                    euid=parent_euid, is_deleted=False
                ).first()
                if parent_instance:
                    factory.link_instances(parent_instance, instance, relationship_type)
                    linked_parents.append(parent_euid)
                else:
                    logger.warning(f"Parent instance not found: {parent_euid}")

            # Create lineage relationships for child instances
            linked_children = []
            for child_euid in child_euids:
                child_instance = conn.session.query(generic_instance).filter_by(
                    euid=child_euid, is_deleted=False
                ).first()
                if child_instance:
                    factory.link_instances(instance, child_instance, relationship_type)
                    linked_children.append(child_euid)
                else:
                    logger.warning(f"Child instance not found: {child_euid}")

            conn.session.commit()

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
            conn.session.rollback()
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
            conn.session.rollback()
            logger.exception(f"Error creating instance from template {template_euid}")
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
        if start_euid:
            # Start from specific node and traverse
            start_obj = conn.session.query(generic_instance).filter_by(
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
                                "source": instance.euid,
                                "target": lin.child_instance.euid,
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
                                "source": lin.parent_instance.euid,
                                "target": instance.euid,
                                "relationship_type": lin.relationship_type or "related",
                            }
                        })
                    traverse(lin.parent_instance, current_depth + 1)

            traverse(start_obj, 0)
        else:
            # Get all instances (limited)
            instances = conn.session.query(generic_instance).filter_by(
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
            lineages = conn.session.query(generic_instance_lineage).filter_by(
                is_deleted=False
            ).limit(500).all()

            for lin in lineages:
                p_euid = lin.parent_instance.euid if lin.parent_instance else None
                c_euid = lin.child_instance.euid if lin.child_instance else None
                if p_euid in visited_nodes and c_euid in visited_nodes:
                    edges.append({
                        "data": {
                            "id": lin.euid,
                            "source": p_euid,
                            "target": c_euid,
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
        query = conn.session.query(generic_template).filter_by(is_deleted=False)
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
        query = conn.session.query(generic_instance).filter_by(is_deleted=False)
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
        obj = conn.session.query(generic_template).filter_by(euid=euid, is_deleted=False).first()
        obj_type = "template"

        if not obj:
            obj = conn.session.query(generic_instance).filter_by(euid=euid, is_deleted=False).first()
            obj_type = "instance"

        if not obj:
            obj = conn.session.query(generic_instance_lineage).filter_by(euid=euid, is_deleted=False).first()
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
    relationship_type = data.get("relationship_type", "related")

    if not parent_euid or not child_euid:
        raise HTTPException(status_code=400, detail="parent_euid and child_euid required")

    with get_db() as conn:
        parent = conn.session.query(generic_instance).filter_by(euid=parent_euid, is_deleted=False).first()
        child = conn.session.query(generic_instance).filter_by(euid=child_euid, is_deleted=False).first()

        if not parent:
            raise HTTPException(status_code=404, detail=f"Parent not found: {parent_euid}")
        if not child:
            raise HTTPException(status_code=404, detail=f"Child not found: {child_euid}")

        lineage = generic_instance_lineage(
            parent_instance_uuid=parent.uuid,
            child_instance_uuid=child.uuid,
            relationship_type=relationship_type,
            parent_type=parent.polymorphic_discriminator,
            child_type=child.polymorphic_discriminator,
        )
        conn.session.add(lineage)
        conn.session.commit()

        return {"success": True, "euid": lineage.euid, "uuid": str(lineage.uuid)}


@app.delete("/api/object/{euid}")
@require_admin
async def api_delete_object(request: Request, euid: str, hard_delete: bool = False):
    """API: Soft-delete an object by EUID. (Admin only)"""
    with get_db() as conn:
        obj = conn.session.query(generic_template).filter_by(euid=euid, is_deleted=False).first()
        if not obj:
            obj = conn.session.query(generic_instance).filter_by(euid=euid, is_deleted=False).first()
        if not obj:
            obj = conn.session.query(generic_instance_lineage).filter_by(euid=euid, is_deleted=False).first()

        if not obj:
            raise HTTPException(status_code=404, detail=f"Object not found: {euid}")

        obj.is_deleted = True
        conn.session.commit()

        return {"success": True, "message": f"Object {euid} soft-deleted"}

