"""
TAPDB Admin Application.

FastAPI-based admin interface for managing TAPDB objects with Cytoscape DAG visualization.

Usage:
    uvicorn admin.main:app --reload --port 8911
"""

import base64
import json
import logging
import secrets
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from admin.auth import (
    SESSION_COOKIE_NAME,
    authenticate_with_cognito,
    change_cognito_password,
    create_cognito_user_account,
    get_current_user,
    get_or_create_user_from_email,
    get_user_by_username,
    get_user_permissions,
    require_admin,
    require_auth,
    respond_to_new_password_challenge,
    update_last_login,
)
from admin.cognito import get_cognito_auth, resolve_tapdb_pool_config
from admin.db_metrics import (
    build_metrics_page_context,
    request_method_var,
    request_path_var,
    stop_all_writers,
)
from admin.db_pool import dispose_all_engines, get_db_connection
from admin.domain_access import (
    build_allowed_origin_regex,
    build_trusted_hosts,
    is_allowed_origin,
    validate_allowed_origins,
)
from daylily_tapdb import InstanceFactory, TemplateManager, __version__
from daylily_tapdb.cli.context import active_env_name
from daylily_tapdb.cli.db_config import (
    get_admin_settings_for_env,
    get_config_path,
    get_db_config_for_env,
)
from daylily_tapdb.models.audit import audit_log
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.services.external_refs import (
    external_ref_payloads as _shared_external_ref_payloads,
)
from daylily_tapdb.services.external_refs import (
    fetch_remote_graph,
    fetch_remote_object_detail,
    get_external_ref_by_index,
    namespace_external_graph,
)
from daylily_tapdb.services.object_lookup import (
    find_object_by_euid as _shared_find_object_by_euid,
)
from daylily_tapdb.web.bridge import resolve_host_context, resolve_host_shell

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Jinja2 environment
templates = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "htm", "xml"]),
)


def _active_tapdb_env() -> str:
    return active_env_name("dev").lower()


APP_ENV = _active_tapdb_env()
IS_PROD = APP_ENV == "prod"
DEFAULT_SUPPORT_EMAIL = "support@daylilyinformatics.com"
DEFAULT_GITHUB_REPO_URL = "https://github.com/Daylily-Informatics/tapdb-core"
_RESERVED_TEMPLATE_COORDS = {("generic", "actor", "system_user")}


def _default_admin_settings() -> dict[str, Any]:
    return {
        "support_email": DEFAULT_SUPPORT_EMAIL,
        "repo_url": DEFAULT_GITHUB_REPO_URL,
        "session_secret": "",
        "auth_mode": "tapdb",
        "disabled_user_email": "tapdb-admin@localhost",
        "disabled_user_role": "admin",
        "shared_host_session_secret": "",
        "shared_host_session_cookie": "session",
        "shared_host_session_max_age_seconds": 1209600,
        "allowed_origins": [],
        "tls_cert_path": "",
        "tls_key_path": "",
        "metrics_enabled": True,
        "metrics_queue_max": 20000,
        "metrics_flush_seconds": 1.0,
        "db_pool_size": 5,
        "db_max_overflow": 10,
        "db_pool_timeout": 30,
        "db_pool_recycle": 1800,
    }


def _load_admin_settings() -> dict[str, Any]:
    try:
        return get_admin_settings_for_env(APP_ENV)
    except Exception as exc:
        logger.warning("Could not resolve TAPDB admin settings from config: %s", exc)
        return _default_admin_settings()


ADMIN_SETTINGS = _load_admin_settings()


def _require_https_url(url: str, *, label: str) -> str:
    """Reject non-HTTPS URLs before network access."""
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError(f"{label} must be an https URL")
    return url


def _git_output(*args: str) -> str:
    """Best-effort git command output for footer metadata."""
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(BASE_DIR.parent),
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return ""
    if proc.returncode != 0:
        return ""
    return (proc.stdout or "").strip()


def _resolve_support_email() -> str:
    """Resolve support email from TapDB config."""
    return str(ADMIN_SETTINGS.get("support_email") or DEFAULT_SUPPORT_EMAIL).strip()


def _build_footer_metadata() -> Dict[str, str]:
    """Build shared footer metadata visible on all admin pages."""
    git_hash = _git_output("rev-parse", "--short", "HEAD") or "n/a"
    git_branch = _git_output("rev-parse", "--abbrev-ref", "HEAD") or "n/a"
    git_tag = _git_output("describe", "--tags", "--exact-match") or _git_output(
        "describe", "--tags", "--abbrev=0"
    )
    if not git_tag:
        git_tag = "n/a"

    return {
        "version": __version__,
        "branch": git_branch,
        "tag": git_tag,
        "hash": git_hash,
        "support_email": _resolve_support_email(),
        "repo_url": str(ADMIN_SETTINGS.get("repo_url") or DEFAULT_GITHUB_REPO_URL),
    }


templates.globals["tapdb_footer"] = _build_footer_metadata()

# Session secret key
if IS_PROD and not str(ADMIN_SETTINGS.get("session_secret") or "").strip():
    raise RuntimeError("Refusing to start in prod without admin.session.secret")
SESSION_SECRET = str(
    ADMIN_SETTINGS.get("session_secret") or ""
).strip() or secrets.token_hex(32)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    try:
        yield
    finally:
        # Best-effort cleanup for pooled DB + metrics writer.
        stop_all_writers()
        dispose_all_engines()


# FastAPI app
app = FastAPI(
    title="TAPDB Admin",
    description="Admin interface for TAPDB - Templated Abstract Polymorphic Database",
    version="0.1.0",
    lifespan=_lifespan,
)


# Request context for DB metrics attribution (path/method).
@app.middleware("http")
async def _metrics_request_context(request: Request, call_next):
    token_path = request_path_var.set(request.url.path)
    token_method = request_method_var.set(request.method)
    try:
        return await call_next(request)
    finally:
        request_path_var.reset(token_path)
        request_method_var.reset(token_method)


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


allow_local_domain_access = not IS_PROD
allowed_origins = validate_allowed_origins(
    [str(item) for item in ADMIN_SETTINGS.get("allowed_origins") or []],
    allow_local=allow_local_domain_access,
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=build_trusted_hosts(allow_local=allow_local_domain_access),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_origin_regex=(
        None
        if allowed_origins
        else build_allowed_origin_regex(allow_local=allow_local_domain_access)
    ),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _enforce_origin_allowlist(request: Request, call_next):
    origin = request.headers.get("origin")
    if origin and not is_allowed_origin(origin, allow_local=allow_local_domain_access):
        return PlainTextResponse("Origin not allowed", status_code=403)
    return await call_next(request)


def tapdb_base_path(request: Request) -> str:
    """Base path for TAPDB UI when mounted as a sub-app (e.g. '/tapdb')."""
    raw = request.scope.get("root_path") or ""
    if not isinstance(raw, str):
        return ""
    return raw.rstrip("/")


def tapdb_url(request: Request, path: str) -> str:
    """Prefix an absolute path with the TAPDB mount root, if any."""
    base = tapdb_base_path(request)
    if not path:
        return base or ""
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}"


templates.globals["tapdb_base_path"] = tapdb_base_path
templates.globals["tapdb_url"] = tapdb_url


def tapdb_host_shell(request: Request) -> dict[str, Any]:
    """Resolve optional host-shell chrome for embedded TapDB mounts."""

    bridge = getattr(getattr(request.app, "state", None), "tapdb_host_bridge", None)
    return resolve_host_shell(bridge, request)


def tapdb_host_context(request: Request) -> dict[str, Any]:
    """Resolve optional host-provided template context."""

    bridge = getattr(getattr(request.app, "state", None), "tapdb_host_bridge", None)
    return resolve_host_context(bridge, request)


templates.globals["tapdb_host_shell"] = tapdb_host_shell
templates.globals["tapdb_host_context"] = tapdb_host_context


def _external_ref_payloads(obj: Any) -> list[dict[str, Any]]:
    return _shared_external_ref_payloads(obj)


def _find_object_by_euid(session: Any, euid: str) -> tuple[Any | None, str | None]:
    return _shared_find_object_by_euid(session, euid)


def get_db():
    """Get database connection.

    Uses the canonical config loader from daylily_tapdb.cli.db_config.
    The active env comes from explicit TapDB CLI/app context and defaults to 'dev'.
    """
    env = _active_tapdb_env()
    return get_db_connection(env)


def get_style(request: Optional[Request] = None) -> Dict[str, str]:
    """Get default style configuration."""
    base = tapdb_base_path(request) if request else ""
    return {"skin_css": f"{base}/static/css/style.css"}


def _is_reserved_template(template_obj: Any) -> bool:
    if template_obj is None:
        return False
    key = (
        str(getattr(template_obj, "category", "") or "").strip().lower(),
        str(getattr(template_obj, "type", "") or "").strip().lower(),
        str(getattr(template_obj, "subtype", "") or "").strip().lower(),
    )
    return key in _RESERVED_TEMPLATE_COORDS


def _ensure_template_manual_create_allowed(template_obj: Any) -> None:
    if _is_reserved_template(template_obj):
        raise HTTPException(
            status_code=403,
            detail=(
                "This template is reserved for TAPDB-managed provisioning and "
                "cannot be created manually."
            ),
        )


templates.globals["is_reserved_template"] = _is_reserved_template


def load_db_metrics_context(*, limit: int = 5000) -> dict:
    """Load DB metrics data for the admin metrics page (test-friendly wrapper)."""
    env = _active_tapdb_env()
    return build_metrics_page_context(env, limit=limit)


def _empty_db_inventory_context(*, error: Optional[str] = None) -> dict:
    """Default inventory context payload for /info."""
    return {
        "db_inventory_error": error,
        "db_inventory_db_name": None,
        "db_inventory_schema_names": [],
        "db_inventory_counts": {
            "schemas": 0,
            "tables": 0,
            "views": 0,
            "materialized_views": 0,
            "sequences": 0,
            "triggers": 0,
            "functions": 0,
            "indexes": 0,
        },
        "db_inventory_tables": [],
        "db_inventory_views": [],
        "db_inventory_materialized_views": [],
        "db_inventory_sequences": [],
        "db_inventory_triggers": [],
        "db_inventory_functions": [],
        "db_inventory_indexes": [],
    }


def load_db_inventory_context() -> dict:
    """Load DB object inventory for /info (test-friendly wrapper)."""
    ctx = _empty_db_inventory_context()

    try:
        with get_db() as conn:
            conn.app_username = "system"
            with conn.session_scope() as session:
                db_name = session.execute(text("SELECT current_database()")).scalar()
                ctx["db_inventory_db_name"] = str(db_name or "")

                schema_filter = """
                    nspname NOT IN ('pg_catalog', 'information_schema')
                    AND nspname NOT LIKE 'pg_toast%'
                    AND nspname NOT LIKE 'pg_temp_%'
                    AND nspname NOT LIKE 'pg_toast_temp_%'
                """
                schemaname_filter = """
                    schemaname NOT IN ('pg_catalog', 'information_schema')
                    AND schemaname NOT LIKE 'pg_toast%'
                    AND schemaname NOT LIKE 'pg_temp_%'
                    AND schemaname NOT LIKE 'pg_toast_temp_%'
                """

                schema_rows = session.execute(
                    text(
                        f"""
                        SELECT nspname AS schema_name
                        FROM pg_namespace
                        WHERE {schema_filter}
                        ORDER BY nspname
                        """
                    )
                ).mappings()
                schema_names = [str(row["schema_name"]) for row in schema_rows]
                ctx["db_inventory_schema_names"] = schema_names

                tables = [
                    dict(row)
                    for row in session.execute(
                        text(
                            f"""
                            SELECT schemaname AS schema_name, tablename AS table_name
                            FROM pg_tables
                            WHERE {schemaname_filter}
                            ORDER BY schemaname, tablename
                            """
                        )
                    ).mappings()
                ]
                views = [
                    dict(row)
                    for row in session.execute(
                        text(
                            f"""
                            SELECT schemaname AS schema_name, viewname AS view_name
                            FROM pg_views
                            WHERE {schemaname_filter}
                            ORDER BY schemaname, viewname
                            """
                        )
                    ).mappings()
                ]
                materialized_views = [
                    dict(row)
                    for row in session.execute(
                        text(
                            f"""
                            SELECT schemaname AS schema_name, matviewname AS materialized_view_name
                            FROM pg_matviews
                            WHERE {schemaname_filter}
                            ORDER BY schemaname, matviewname
                            """
                        )
                    ).mappings()
                ]
                sequences = [
                    dict(row)
                    for row in session.execute(
                        text(
                            f"""
                            SELECT schemaname AS schema_name, sequencename AS sequence_name
                            FROM pg_sequences
                            WHERE {schemaname_filter}
                            ORDER BY schemaname, sequencename
                            """
                        )
                    ).mappings()
                ]
                triggers = [
                    dict(row)
                    for row in session.execute(
                        text(
                            f"""
                            SELECT
                                ns.nspname AS schema_name,
                                cls.relname AS table_name,
                                tg.tgname AS trigger_name
                            FROM pg_trigger tg
                            JOIN pg_class cls ON cls.oid = tg.tgrelid
                            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                            WHERE NOT tg.tgisinternal
                              AND {schema_filter}
                            ORDER BY ns.nspname, cls.relname, tg.tgname
                            """
                        )
                    ).mappings()
                ]
                functions = [
                    dict(row)
                    for row in session.execute(
                        text(
                            f"""
                            SELECT
                                ns.nspname AS schema_name,
                                p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                                    AS function_signature
                            FROM pg_proc p
                            JOIN pg_namespace ns ON ns.oid = p.pronamespace
                            WHERE {schema_filter}
                            ORDER BY ns.nspname, function_signature
                            """
                        )
                    ).mappings()
                ]
                indexes = [
                    dict(row)
                    for row in session.execute(
                        text(
                            f"""
                            SELECT
                                schemaname AS schema_name,
                                tablename AS table_name,
                                indexname AS index_name
                            FROM pg_indexes
                            WHERE {schemaname_filter}
                            ORDER BY schemaname, tablename, indexname
                            """
                        )
                    ).mappings()
                ]

                ctx["db_inventory_tables"] = tables
                ctx["db_inventory_views"] = views
                ctx["db_inventory_materialized_views"] = materialized_views
                ctx["db_inventory_sequences"] = sequences
                ctx["db_inventory_triggers"] = triggers
                ctx["db_inventory_functions"] = functions
                ctx["db_inventory_indexes"] = indexes
                ctx["db_inventory_counts"] = {
                    "schemas": len(schema_names),
                    "tables": len(tables),
                    "views": len(views),
                    "materialized_views": len(materialized_views),
                    "sequences": len(sequences),
                    "triggers": len(triggers),
                    "functions": len(functions),
                    "indexes": len(indexes),
                }
    except Exception as exc:
        logger.warning("Could not load DB inventory: %s", exc)
        return _empty_db_inventory_context(error=str(exc))

    return ctx


def _mask_sensitive_value(key: str, value: str) -> str:
    """Redact sensitive values for safe UI display."""
    key_u = key.upper()
    if any(token in key_u for token in ("PASSWORD", "SECRET", "TOKEN")):
        return "(redacted)"
    return value or "(empty)"


def _normalize_cognito_domain(raw_domain: str) -> str:
    """Normalize configured Cognito domain to hostname-only form."""
    domain = (raw_domain or "").strip()
    if not domain:
        raise RuntimeError("COGNITO_DOMAIN is not configured")
    probe = domain if "://" in domain else f"https://{domain}"
    parts = urlsplit(probe)
    if not parts.netloc:
        raise RuntimeError(f"Invalid COGNITO_DOMAIN value: {raw_domain!r}")
    return parts.netloc


def _resolve_cognito_oauth_runtime(env_name: str) -> Dict[str, str]:
    """Resolve OAuth/Hosted-UI settings from TapDB config."""
    pool_cfg = resolve_tapdb_pool_config(env_name)
    domain = _normalize_cognito_domain(pool_cfg.domain)
    callback_url = (pool_cfg.callback_url or "").strip()
    if not callback_url:
        raise RuntimeError("cognito_callback_url is missing in tapdb config")
    client_id = (pool_cfg.app_client_id or "").strip()
    if not client_id:
        raise RuntimeError("cognito_app_client_id is missing in tapdb config")
    return {
        "domain": domain,
        "callback_url": callback_url,
        "client_id": client_id,
        "client_secret": pool_cfg.app_client_secret,
        "scope": "openid email profile",
    }


def _build_cognito_authorize_url(runtime: Dict[str, str], state: str) -> str:
    """Build Hosted UI authorize URL targeting Google IdP."""
    query = urlencode(
        {
            "response_type": "code",
            "client_id": runtime["client_id"],
            "redirect_uri": runtime["callback_url"],
            "scope": runtime["scope"],
            "state": state,
            "identity_provider": "Google",
        }
    )
    return f"https://{runtime['domain']}/oauth2/authorize?{query}"


def _exchange_oauth_authorization_code(
    runtime: Dict[str, str], code: str
) -> Dict[str, Any]:
    """Exchange Hosted UI auth code for Cognito tokens."""
    token_url = _require_https_url(
        f"https://{runtime['domain']}/oauth2/token",
        label="Cognito token endpoint",
    )
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": runtime["callback_url"],
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    client_secret = runtime.get("client_secret") or ""
    if client_secret:
        creds = f"{runtime['client_id']}:{client_secret}".encode("utf-8")
        headers["Authorization"] = f"Basic {base64.b64encode(creds).decode('ascii')}"
    else:
        payload["client_id"] = runtime["client_id"]

    req = UrlRequest(
        token_url,
        data=urlencode(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(req, timeout=15) as resp:  # nosec B310
            body = resp.read().decode("utf-8")
    except HTTPError as exc:
        details = ""
        try:
            details = exc.read().decode("utf-8").strip()
        except Exception:
            details = ""
        reason = details or str(exc)
        raise RuntimeError(f"Cognito token exchange failed: {reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"Cognito token endpoint is unreachable: {exc}") from exc

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Cognito token response was not valid JSON") from exc

    if "error" in data:
        msg = data.get("error_description") or data["error"]
        raise RuntimeError(f"Cognito token exchange failed: {msg}")
    return data


def _fetch_oauth_userinfo(runtime: Dict[str, str], access_token: str) -> Dict[str, Any]:
    """Fetch user claims from Cognito userInfo endpoint."""
    url = _require_https_url(
        f"https://{runtime['domain']}/oauth2/userInfo",
        label="Cognito userInfo endpoint",
    )
    req = UrlRequest(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        method="GET",
    )
    with urlopen(req, timeout=15) as resp:  # nosec B310
        body = resp.read().decode("utf-8")
    return json.loads(body)


def _resolve_oauth_user_profile(
    env_name: str,
    tokens: Dict[str, Any],
    runtime: Dict[str, str],
) -> Dict[str, str]:
    """Resolve normalized user profile (email/display_name) from OAuth tokens."""
    claims: Dict[str, Any] = {}
    access_token = str(tokens.get("access_token") or "")
    id_token = str(tokens.get("id_token") or "")

    if access_token:
        try:
            claims.update(_fetch_oauth_userinfo(runtime, access_token))
        except Exception as exc:
            logger.warning("Failed to fetch Cognito userInfo claims: %s", exc)

    if (not claims or not claims.get("email")) and id_token:
        try:
            claims.update(get_cognito_auth(env_name).verify_token(id_token))
        except Exception as exc:
            logger.warning("Failed to verify id_token claims: %s", exc)

    email = (
        str(
            claims.get("email")
            or claims.get("cognito:username")
            or claims.get("username")
            or ""
        )
        .strip()
        .lower()
    )
    if not email:
        raise RuntimeError(
            "OAuth login succeeded but no email/username claim was returned"
        )

    display_name = (
        str(
            claims.get("name")
            or claims.get("preferred_username")
            or claims.get("given_name")
            or ""
        ).strip()
        or None
    )
    return {"email": email, "display_name": display_name or ""}


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
        parent_instance = (
            session.query(generic_instance)
            .filter_by(euid=parent_euid, is_deleted=False)
            .first()
        )
        if not parent_instance:
            missing_parents.append(parent_euid)
            continue
        resolved_parents.append(parent_instance)

    seen_child_euids: set[str] = set()
    for child_euid in child_euids:
        if child_euid in seen_child_euids:
            continue
        seen_child_euids.add(child_euid)
        child_instance = (
            session.query(generic_instance)
            .filter_by(euid=child_euid, is_deleted=False)
            .first()
        )
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
        parent_instance_uid=parent.uid,
        child_instance_uid=child.uid,
        relationship_type=rel,
        parent_type=parent.polymorphic_discriminator,
        child_type=child.polymorphic_discriminator,
        json_addl={},
    )


# ============================================================================
# Authentication Routes
# ============================================================================


@app.get("/auth/login")
async def oauth_login(
    request: Request,
    next: str = Query("", description="Post-auth redirect path"),
):
    """Start Cognito Hosted UI login flow (Google IdP)."""
    user = await get_current_user(request)
    if user:
        return RedirectResponse(tapdb_url(request, "/"), status_code=302)

    env_name = _active_tapdb_env()
    try:
        runtime = _resolve_cognito_oauth_runtime(env_name)
    except Exception as exc:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
            error=f"OAuth login is not configured: {exc}",
        )
        return HTMLResponse(content=content)

    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state
    raw_next = (next or "").strip()
    if not raw_next.startswith("/"):
        raw_next = ""

    default_next = tapdb_url(request, "/")
    base = tapdb_base_path(request)
    if not raw_next or raw_next == "/":
        raw_next = default_next
    elif base and not (raw_next == base or raw_next.startswith(f"{base}/")):
        # Treat '/foo' as TAPDB-relative when mounted at a sub-path.
        raw_next = tapdb_url(request, raw_next)
    request.session["oauth_next"] = raw_next
    return RedirectResponse(
        _build_cognito_authorize_url(runtime, state), status_code=302
    )


@app.get("/auth/callback")
async def oauth_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
):
    """Handle Cognito Hosted UI OAuth callback."""
    env_name = _active_tapdb_env()
    if error:
        details = error_description or error
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
            error=f"OAuth login failed: {details}",
        )
        return HTMLResponse(content=content)

    expected_state = request.session.pop("oauth_state", None)
    if not expected_state or not state or state != expected_state:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
            error="OAuth login failed: invalid state",
        )
        return HTMLResponse(content=content)

    if not code:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
            error="OAuth login failed: missing authorization code",
        )
        return HTMLResponse(content=content)

    try:
        runtime = _resolve_cognito_oauth_runtime(env_name)
        tokens = _exchange_oauth_authorization_code(runtime, code)
        profile = _resolve_oauth_user_profile(env_name, tokens, runtime)
        user = get_or_create_user_from_email(
            profile["email"],
            display_name=profile.get("display_name") or None,
            role="user",
        )
    except Exception as exc:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
            error=f"OAuth login failed: {exc}",
        )
        return HTMLResponse(content=content)

    request.session["user_uid"] = user["uid"]
    request.session["cognito_username"] = profile["email"]
    if tokens.get("access_token"):
        request.session["cognito_access_token"] = tokens["access_token"]
    request.session.pop("cognito_challenge", None)
    request.session.pop("cognito_challenge_session", None)
    update_last_login(user["uid"])

    next_path = request.session.pop("oauth_next", "")
    next_path = (next_path or "").strip()
    if not next_path.startswith("/"):
        next_path = ""
    base = tapdb_base_path(request)
    if not next_path or next_path == "/":
        next_path = tapdb_url(request, "/")
    elif base and not (next_path == base or next_path.startswith(f"{base}/")):
        next_path = tapdb_url(request, next_path)
    return RedirectResponse(next_path, status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    """Login page."""
    # If already logged in, redirect to home
    user = await get_current_user(request)
    if user:
        if user.get("require_password_change"):
            return RedirectResponse(
                tapdb_url(request, "/change-password"), status_code=302
            )
        return RedirectResponse(tapdb_url(request, "/"), status_code=302)

    content = templates.get_template("login.html").render(
        request=request,
        style=get_style(request),
        error=error,
    )
    return HTMLResponse(content=content)


@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    """Handle login form submission."""
    identity = (username or "").strip()
    user = get_user_by_username(identity)
    cognito_username = (user.get("email") if user else identity) or identity

    try:
        auth_result = authenticate_with_cognito(cognito_username, password)
    except ValueError:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
            error="Invalid username or password",
        )
        return HTMLResponse(content=content)
    except Exception as e:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
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
                style=get_style(request),
                error=(
                    "Authenticated with Cognito, but failed to provision TAPDB user: "
                    f"{e}"
                ),
            )
            return HTMLResponse(content=content)

    # Set session (used for app auth/authorization)
    request.session["user_uid"] = user["uid"]
    request.session["cognito_username"] = cognito_username
    update_last_login(user["uid"])

    if auth_result.get("challenge") == "NEW_PASSWORD_REQUIRED":
        request.session["cognito_challenge"] = "NEW_PASSWORD_REQUIRED"
        request.session["cognito_challenge_session"] = auth_result.get("session", "")
        logger.info(f"User requires new Cognito password: {cognito_username}")
        return RedirectResponse(tapdb_url(request, "/change-password"), status_code=302)

    access_token = auth_result.get("access_token")
    if not access_token:
        request.session.clear()
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
            error="Authentication failed: no access token returned",
        )
        return HTMLResponse(content=content)

    request.session["cognito_access_token"] = access_token
    request.session.pop("cognito_challenge", None)
    request.session.pop("cognito_challenge_session", None)

    logger.info(f"User logged in: {username}")

    # Redirect to password change if required
    if user.get("require_password_change"):
        return RedirectResponse(tapdb_url(request, "/change-password"), status_code=302)

    return RedirectResponse(tapdb_url(request, "/"), status_code=302)


@app.get("/signup", response_class=HTMLResponse)
async def signup_page(
    request: Request,
    error: Optional[str] = None,
):
    """Account creation page."""
    user = await get_current_user(request)
    if user:
        return RedirectResponse(tapdb_url(request, "/"), status_code=302)

    content = templates.get_template("signup.html").render(
        request=request,
        style=get_style(request),
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
            style=get_style(request),
            error="Valid email is required",
        )
        return HTMLResponse(content=content)

    if len(password) < 8:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(request),
            error="Password must be at least 8 characters",
        )
        return HTMLResponse(content=content)

    if password != confirm_password:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(request),
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
            style=get_style(request),
            error=str(e),
        )
        return HTMLResponse(content=content)
    except Exception as e:
        content = templates.get_template("signup.html").render(
            request=request,
            style=get_style(request),
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
            style=get_style(request),
            error=(f"Cognito account created, but TAPDB user provisioning failed: {e}"),
        )
        return HTMLResponse(content=content)

    try:
        auth_result = authenticate_with_cognito(normalized_email, password)
    except Exception as e:
        content = templates.get_template("login.html").render(
            request=request,
            style=get_style(request),
            error=(
                "Account created but auto-login failed. Please sign in manually. "
                f"Details: {e}"
            ),
        )
        return HTMLResponse(content=content)

    request.session["user_uid"] = user["uid"]
    request.session["cognito_username"] = normalized_email
    update_last_login(user["uid"])

    if auth_result.get("challenge") == "NEW_PASSWORD_REQUIRED":
        request.session["cognito_challenge"] = "NEW_PASSWORD_REQUIRED"
        request.session["cognito_challenge_session"] = auth_result.get("session", "")
        return RedirectResponse(tapdb_url(request, "/change-password"), status_code=302)

    access_token = auth_result.get("access_token")
    if access_token:
        request.session["cognito_access_token"] = access_token
    request.session.pop("cognito_challenge", None)
    request.session.pop("cognito_challenge_session", None)
    return RedirectResponse(tapdb_url(request, "/"), status_code=302)


@app.get("/logout")
async def logout(request: Request):
    """Logout and clear session."""
    request.session.clear()
    return RedirectResponse(tapdb_url(request, "/login"), status_code=302)


@app.get("/change-password", response_class=HTMLResponse)
async def change_password_page(
    request: Request, error: Optional[str] = None, success: Optional[str] = None
):
    """Password change page."""
    user = await get_current_user(request)
    if not user:
        return RedirectResponse(tapdb_url(request, "/login"), status_code=302)

    challenge_required = (
        request.session.get("cognito_challenge") == "NEW_PASSWORD_REQUIRED"
    )
    content = templates.get_template("change_password.html").render(
        request=request,
        style=get_style(request),
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
        return RedirectResponse(tapdb_url(request, "/login"), status_code=302)

    # Validate new password
    if len(new_password) < 8:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(request),
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
            style=get_style(request),
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
                style=get_style(request),
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
            return RedirectResponse(tapdb_url(request, "/"), status_code=302)
        except ValueError as e:
            content = templates.get_template("change_password.html").render(
                request=request,
                style=get_style(request),
                user=user,
                required=True,
                challenge_required=True,
                error=str(e),
            )
            return HTMLResponse(content=content)
        except Exception as e:
            content = templates.get_template("change_password.html").render(
                request=request,
                style=get_style(request),
                user=user,
                required=True,
                challenge_required=True,
                error=f"Password update failed: {e}",
            )
            return HTMLResponse(content=content)

    if not current_password:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(request),
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
            style=get_style(request),
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
            style=get_style(request),
            user=user,
            required=user.get("require_password_change", False),
            challenge_required=False,
            error=str(e),
        )
        return HTMLResponse(content=content)
    except Exception as e:
        content = templates.get_template("change_password.html").render(
            request=request,
            style=get_style(request),
            user=user,
            required=user.get("require_password_change", False),
            challenge_required=False,
            error=f"Password update failed: {e}",
        )
        return HTMLResponse(content=content)

    # If was required, redirect to home. Otherwise show success.
    if user.get("require_password_change"):
        return RedirectResponse(tapdb_url(request, "/"), status_code=302)

    content = templates.get_template("change_password.html").render(
        request=request,
        style=get_style(request),
        user=user,
        required=False,
        challenge_required=False,
        success="Password changed successfully",
    )
    return HTMLResponse(content=content)


# ============================================================================
# Help Route
# ============================================================================


@app.get("/help", response_class=HTMLResponse)
async def help_page(request: Request):
    """GUI help and support page."""
    user = await get_current_user(request)
    permissions = get_user_permissions(user)
    content = templates.get_template("help.html").render(
        request=request,
        style=get_style(request),
        user=user,
        permissions=permissions,
    )
    return HTMLResponse(content=content)


@app.get("/info", response_class=HTMLResponse)
@require_auth
async def info_page(request: Request):
    """Runtime info page with DB and Cognito connection details."""
    user = request.state.user
    permissions = get_user_permissions(user)
    env = _active_tapdb_env()
    cfg = get_db_config_for_env(env)
    is_admin = str(user.get("role") or "").strip().lower() == "admin"

    inventory_ctx = _empty_db_inventory_context()
    if is_admin:
        inventory_ctx = load_db_inventory_context()
    inventory_ctx["db_inventory_visible"] = is_admin

    runtime_db_name = (
        str(inventory_ctx.get("db_inventory_db_name") or "").strip()
        or str(cfg.get("database") or "").strip()
    )

    db_rows: List[tuple[str, str]] = [
        ("environment", env),
        ("config_path", str(get_config_path())),
        ("runtime_database_name", runtime_db_name),
    ]
    for key in sorted(cfg.keys()):
        if key == "password":
            continue
        db_rows.append((key, _mask_sensitive_value(key, str(cfg.get(key, "")))))
    db_rows.append(
        ("password_configured", "yes" if bool(cfg.get("password")) else "no")
    )

    cognito_summary_rows: List[tuple[str, str]] = []
    cognito_env_rows: List[tuple[str, str]] = []
    cognito_error: Optional[str] = None
    try:
        pool_cfg = resolve_tapdb_pool_config(env)
        cognito_summary_rows = [
            ("pool_id", pool_cfg.pool_id),
            ("app_client_id", pool_cfg.app_client_id),
            ("region", pool_cfg.region),
            ("aws_profile", pool_cfg.aws_profile or "(not set)"),
            ("config_path", str(pool_cfg.source_file)),
            ("client_name", pool_cfg.client_name),
            ("domain", pool_cfg.domain or "(not set)"),
            ("callback_url", pool_cfg.callback_url or "(not set)"),
            ("logout_url", pool_cfg.logout_url or "(not set)"),
        ]
        cognito_env_rows = [
            ("COGNITO_USER_POOL_ID", pool_cfg.pool_id),
            ("COGNITO_APP_CLIENT_ID", pool_cfg.app_client_id),
            ("COGNITO_REGION", pool_cfg.region),
            ("COGNITO_DOMAIN", pool_cfg.domain or "(not set)"),
            ("COGNITO_CALLBACK_URL", pool_cfg.callback_url or "(not set)"),
            ("COGNITO_LOGOUT_URL", pool_cfg.logout_url or "(not set)"),
            ("AWS_PROFILE", pool_cfg.aws_profile or "(not set)"),
        ]
    except Exception as exc:
        cognito_error = str(exc)

    content = templates.get_template("info.html").render(
        request=request,
        style=get_style(request),
        user=user,
        permissions=permissions,
        db_rows=db_rows,
        cognito_summary_rows=cognito_summary_rows,
        cognito_env_rows=cognito_env_rows,
        cognito_error=cognito_error,
        **inventory_ctx,
    )
    return HTMLResponse(content=content)


@app.get("/admin/metrics", response_class=HTMLResponse)
@require_admin
async def admin_metrics_page(
    request: Request,
    limit: int = Query(5000, ge=1, le=20000),
):
    """Admin-only DB metrics dashboard."""
    user = request.state.user
    permissions = get_user_permissions(user)
    metrics_ctx = load_db_metrics_context(limit=limit)
    content = templates.get_template("admin_metrics.html").render(
        request=request,
        style=get_style(request),
        user=user,
        permissions=permissions,
        **metrics_ctx,
    )
    return HTMLResponse(content=content)


# ============================================================================
# HTML Routes (Protected)
# ============================================================================


_HOME_QUERY_SCOPES = {"all", "template", "instance", "lineage"}
_HOME_AUDIT_OPS = {"ALL", "INSERT", "UPDATE", "DELETE"}
_COMPLEX_QUERY_KINDS = {"all", "template", "instance", "lineage"}


def _normalize_home_limit(value: Any) -> int:
    """Normalize list/query limit for home query + audit panels."""
    try:
        parsed = int(str(value or "20").strip())
    except Exception:
        parsed = 20
    return max(1, min(100, parsed))


def _normalize_home_scope(value: Any) -> str:
    scope = str(value or "all").strip().lower()
    if scope not in _HOME_QUERY_SCOPES:
        return "all"
    return scope


def _normalize_home_op(value: Any) -> str:
    op = str(value or "ALL").strip().upper()
    if op not in _HOME_AUDIT_OPS:
        return "ALL"
    return op


def _match_object_query(obj: Any, query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    euid = str(getattr(obj, "euid", "") or "")
    name = str(getattr(obj, "name", "") or "")
    return q in f"{euid} {name}".lower()


def _to_object_result(kind: str, obj: Any) -> dict[str, Any]:
    return {
        "kind": kind,
        "euid": getattr(obj, "euid", None),
        "name": getattr(obj, "name", None),
        "category": getattr(obj, "category", None),
        "type": getattr(obj, "type", None),
        "subtype": getattr(obj, "subtype", None),
        "version": getattr(obj, "version", None),
        "created_dt": getattr(obj, "created_dt", None),
    }


def _timestamp_rank(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value.timestamp())
    except Exception:
        return 0.0


def _normalize_complex_kind(value: Any) -> str:
    kind = str(value or "all").strip().lower()
    if kind not in _COMPLEX_QUERY_KINDS:
        return "all"
    return kind


def _run_complex_query(
    session: Any,
    kind: str,
    category: str,
    type_name: str,
    subtype: str,
    name_like: str,
    euid_like: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Run advanced object search using multi-field filters."""
    scope_models = {
        "template": (generic_template, "template"),
        "instance": (generic_instance, "instance"),
        "lineage": (generic_instance_lineage, "lineage"),
    }
    selected_scopes = list(scope_models.keys()) if kind == "all" else [kind]
    category_q = (category or "").strip().lower()
    type_q = (type_name or "").strip().lower()
    subtype_q = (subtype or "").strip().lower()
    name_q = (name_like or "").strip().lower()
    euid_q = (euid_like or "").strip().lower()

    results: list[dict[str, Any]] = []
    for selected in selected_scopes:
        model, row_kind = scope_models[selected]
        rows = session.query(model).filter_by(is_deleted=False).all()
        for row in rows:
            row_category = str(getattr(row, "category", "") or "").lower()
            row_type = str(getattr(row, "type", "") or "").lower()
            row_subtype = str(getattr(row, "subtype", "") or "").lower()
            row_name = str(getattr(row, "name", "") or "").lower()
            row_euid = str(getattr(row, "euid", "") or "").lower()
            if category_q and category_q != row_category:
                continue
            if type_q and type_q != row_type:
                continue
            if subtype_q and subtype_q != row_subtype:
                continue
            if name_q and name_q not in row_name:
                continue
            if euid_q and euid_q not in row_euid:
                continue
            results.append(_to_object_result(row_kind, row))

    results.sort(
        key=lambda r: (
            -_timestamp_rank(r.get("created_dt")),
            str(r.get("kind") or ""),
            str(r.get("euid") or ""),
        )
    )
    return results[:limit]


def _run_simple_object_query(
    session: Any,
    q: str,
    scope: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Run simple text query across templates/instances/lineages."""
    normalized_q = (q or "").strip()
    if not normalized_q:
        return []

    results: list[dict[str, Any]] = []
    scope_models = {
        "template": (generic_template, "template"),
        "instance": (generic_instance, "instance"),
        "lineage": (generic_instance_lineage, "lineage"),
    }
    selected_scopes = list(scope_models.keys()) if scope == "all" else [scope]

    for selected in selected_scopes:
        model, kind = scope_models[selected]
        rows = session.query(model).filter_by(is_deleted=False).all()
        for row in rows:
            if _match_object_query(row, normalized_q):
                results.append(_to_object_result(kind, row))

    results.sort(
        key=lambda r: (
            -_timestamp_rank(r.get("created_dt")),
            str(r.get("kind") or ""),
            str(r.get("euid") or ""),
        )
    )
    return results[:limit]


def _load_object_audit(
    session: Any,
    object_euid: str,
    op: str,
    limit: int,
) -> list[SimpleNamespace]:
    """Load per-object audit trail rows."""
    target = (object_euid or "").strip().lower()
    if not target:
        return []
    rows = session.query(audit_log).filter_by(is_deleted=False).all()
    filtered: list[SimpleNamespace] = []
    for raw in rows:
        rel_euid = str(getattr(raw, "rel_table_euid_fk", "") or "").strip().lower()
        if rel_euid != target:
            continue
        if op != "ALL" and str(getattr(raw, "operation_type", "") or "").upper() != op:
            continue
        # Materialize rows before session closes to avoid detached-instance access in templates.
        filtered.append(
            SimpleNamespace(
                uid=getattr(raw, "uid", None),
                euid=getattr(raw, "euid", None),
                rel_table_name=getattr(raw, "rel_table_name", None),
                column_name=getattr(raw, "column_name", None),
                rel_table_uid_fk=getattr(raw, "rel_table_uid_fk", None),
                rel_table_euid_fk=getattr(raw, "rel_table_euid_fk", None),
                old_value=getattr(raw, "old_value", None),
                new_value=getattr(raw, "new_value", None),
                changed_by=getattr(raw, "changed_by", None),
                changed_at=getattr(raw, "changed_at", None),
                operation_type=getattr(raw, "operation_type", None),
            )
        )
    filtered.sort(key=lambda r: -_timestamp_rank(getattr(r, "changed_at", None)))
    return filtered[:limit]


def _load_user_audit(
    session: Any,
    requested_user: str,
    op: str,
    limit: int,
) -> list[SimpleNamespace]:
    """Load per-user audit trail rows."""
    target = (requested_user or "").strip().lower()
    if not target:
        return []
    rows = session.query(audit_log).filter_by(is_deleted=False).all()
    filtered: list[SimpleNamespace] = []
    for raw in rows:
        changed_by = str(getattr(raw, "changed_by", "") or "").strip().lower()
        if changed_by != target:
            continue
        if op != "ALL" and str(getattr(raw, "operation_type", "") or "").upper() != op:
            continue
        filtered.append(
            SimpleNamespace(
                uid=getattr(raw, "uid", None),
                euid=getattr(raw, "euid", None),
                rel_table_name=getattr(raw, "rel_table_name", None),
                column_name=getattr(raw, "column_name", None),
                rel_table_uid_fk=getattr(raw, "rel_table_uid_fk", None),
                rel_table_euid_fk=getattr(raw, "rel_table_euid_fk", None),
                old_value=getattr(raw, "old_value", None),
                new_value=getattr(raw, "new_value", None),
                changed_by=getattr(raw, "changed_by", None),
                changed_at=getattr(raw, "changed_at", None),
                operation_type=getattr(raw, "operation_type", None),
            )
        )
    filtered.sort(key=lambda r: -_timestamp_rank(getattr(r, "changed_at", None)))
    return filtered[:limit]


def _resolve_effective_audit_user(
    current_user: dict[str, Any],
    requested_user: str,
    permissions: dict[str, Any],
) -> tuple[str, Optional[str]]:
    """Resolve requested audit user with mixed-role access policy."""
    current_identifier = (
        str(current_user.get("username") or current_user.get("email") or "")
        .strip()
        .lower()
    )
    requested = (requested_user or "").strip().lower()
    effective = requested or current_identifier

    is_admin = str(current_user.get("role") or "").strip().lower() == "admin"
    can_manage_users = bool((permissions or {}).get("can_manage_users")) or is_admin
    if not can_manage_users and effective != current_identifier:
        return current_identifier, (
            "You can view only your own user audit trail. Showing your activity."
        )
    return effective, None


@app.get("/", response_class=HTMLResponse)
@require_auth
async def index(
    request: Request,
    q: str = Query("", description="Simple object query text"),
    scope: str = Query("all", description="Query scope: all|template|instance|lineage"),
    object_euid: str = Query("", description="EUID for per-object audit trail"),
    audit_user: str = Query("", description="User identifier for per-user audit trail"),
    op: str = Query(
        "ALL", description="Audit operation filter: INSERT|UPDATE|DELETE|ALL"
    ),
    limit: int = Query(20, description="Result row limit"),
):
    """Home page with overview, simple query, and audit trail panels."""
    user = request.state.user
    permissions = get_user_permissions(user)
    query_params = {
        "q": (q or "").strip(),
        "scope": _normalize_home_scope(scope),
        "object_euid": (object_euid or "").strip(),
        "audit_user": (audit_user or "").strip(),
        "op": _normalize_home_op(op),
        "limit": _normalize_home_limit(limit),
    }

    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            template_count = (
                session.query(generic_template).filter_by(is_deleted=False).count()
            )
            instance_count = (
                session.query(generic_instance).filter_by(is_deleted=False).count()
            )
            lineage_count = (
                session.query(generic_instance_lineage)
                .filter_by(is_deleted=False)
                .count()
            )
            object_results = _run_simple_object_query(
                session=session,
                q=query_params["q"],
                scope=query_params["scope"],
                limit=query_params["limit"],
            )
            object_audit_rows = _load_object_audit(
                session=session,
                object_euid=query_params["object_euid"],
                op=query_params["op"],
                limit=query_params["limit"],
            )
            audit_user_effective, audit_warning = _resolve_effective_audit_user(
                current_user=user,
                requested_user=query_params["audit_user"],
                permissions=permissions,
            )
            user_audit_rows = _load_user_audit(
                session=session,
                requested_user=audit_user_effective,
                op=query_params["op"],
                limit=query_params["limit"],
            )

    content = templates.get_template("index.html").render(
        request=request,
        style=get_style(request),
        user=user,
        permissions=permissions,
        template_count=template_count,
        instance_count=instance_count,
        lineage_count=lineage_count,
        query_params=query_params,
        object_results=object_results,
        object_audit_rows=object_audit_rows,
        user_audit_rows=user_audit_rows,
        audit_warning=audit_warning,
        audit_user_effective=audit_user_effective,
    )
    return HTMLResponse(content=content)


@app.get("/query", response_class=HTMLResponse)
@require_auth
async def complex_query_page(
    request: Request,
    kind: str = Query("all", description="Object kind: all|template|instance|lineage"),
    category: str = Query("", description="Exact category filter"),
    type_: str = Query("", alias="type", description="Exact type filter"),
    subtype: str = Query("", description="Exact subtype filter"),
    name_like: str = Query("", description="Case-insensitive name contains"),
    euid_like: str = Query("", description="Case-insensitive EUID contains"),
    limit: int = Query(50, description="Result row limit"),
):
    """Advanced multi-field object query page."""
    user = request.state.user
    permissions = get_user_permissions(user)

    query_params = {
        "kind": _normalize_complex_kind(kind),
        "category": (category or "").strip(),
        "type": (type_ or "").strip(),
        "subtype": (subtype or "").strip(),
        "name_like": (name_like or "").strip(),
        "euid_like": (euid_like or "").strip(),
        "limit": _normalize_home_limit(limit),
    }
    has_filters = any(
        [
            query_params["category"],
            query_params["type"],
            query_params["subtype"],
            query_params["name_like"],
            query_params["euid_like"],
        ]
    )
    should_run = has_filters or query_params["kind"] != "all"

    results: list[dict[str, Any]] = []
    if should_run:
        with get_db() as conn:
            conn.app_username = user.get("username")
            with conn.session_scope() as session:
                results = _run_complex_query(
                    session=session,
                    kind=query_params["kind"],
                    category=query_params["category"],
                    type_name=query_params["type"],
                    subtype=query_params["subtype"],
                    name_like=query_params["name_like"],
                    euid_like=query_params["euid_like"],
                    limit=query_params["limit"],
                )

    content = templates.get_template("complex_query.html").render(
        request=request,
        style=get_style(request),
        user=user,
        permissions=permissions,
        query_params=query_params,
        has_filters=has_filters,
        should_run=should_run,
        results=results,
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
                style=get_style(request),
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
                style=get_style(request),
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
                        "parent_euid": lin.parent_instance.euid
                        if lin.parent_instance
                        else None,
                        "parent_name": lin.parent_instance.name
                        if lin.parent_instance
                        else None,
                        "child_euid": lin.child_instance.euid
                        if lin.child_instance
                        else None,
                        "child_name": lin.child_instance.name
                        if lin.child_instance
                        else None,
                        "relationship_type": lin.relationship_type,
                        "created_dt": lin.created_dt,
                    }
                )

            content = templates.get_template("lineages_list.html").render(
                request=request,
                style=get_style(request),
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
            obj, obj_type = _find_object_by_euid(session, euid)

            if not obj:
                raise HTTPException(status_code=404, detail=f"Object not found: {euid}")

            # Get relationships for instances - extract data while session is open
            parent_lineages = []
            child_lineages = []
            if obj_type == "instance":
                # Fetch lineages and eagerly load related instances
                for lin in obj.parent_of_lineages.filter_by(is_deleted=False).all():
                    parent_lineages.append(
                        {
                            "euid": lin.euid,
                            "child_euid": lin.child_instance.euid
                            if lin.child_instance
                            else None,
                            "child_name": lin.child_instance.name
                            if lin.child_instance
                            else None,
                            "relationship_type": lin.relationship_type,
                        }
                    )
                for lin in obj.child_of_lineages.filter_by(is_deleted=False).all():
                    child_lineages.append(
                        {
                            "euid": lin.euid,
                            "parent_euid": lin.parent_instance.euid
                            if lin.parent_instance
                            else None,
                            "parent_name": lin.parent_instance.name
                            if lin.parent_instance
                            else None,
                            "relationship_type": lin.relationship_type,
                        }
                    )

            # Render template inside session context to avoid detached instance errors
            content = templates.get_template("object_detail.html").render(
                request=request,
                style=get_style(request),
                user=user,
                permissions=permissions,
                obj=obj,
                obj_type=obj_type,
                external_refs=_external_ref_payloads(obj),
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
    merge_ref: Optional[int] = Query(None, ge=0),
):
    """DAG graph visualization."""
    user = request.state.user
    permissions = get_user_permissions(user)

    content = templates.get_template("graph.html").render(
        request=request,
        style=get_style(request),
        user=user,
        permissions=permissions,
        start_euid=start_euid or "",
        depth=depth,
        merge_ref=merge_ref,
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
            template = (
                session.query(generic_template)
                .filter_by(euid=template_euid, is_deleted=False)
                .first()
            )

            if not template:
                raise HTTPException(
                    status_code=404, detail=f"Template not found: {template_euid}"
                )
            _ensure_template_manual_create_allowed(template)

            # Get default properties from json_addl
            default_properties = template.json_addl or {}
            has_instantiation_layouts = bool(
                default_properties.get("instantiation_layouts")
            )

            content = templates.get_template("create_instance.html").render(
                request=request,
                style=get_style(request),
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
    parent_euids = (
        [e.strip() for e in parent_euids_raw.split(",") if e.strip()]
        if parent_euids_raw
        else []
    )
    child_euids = (
        [e.strip() for e in child_euids_raw.split(",") if e.strip()]
        if child_euids_raw
        else []
    )

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
            template = (
                session.query(generic_template)
                .filter_by(euid=template_euid, is_deleted=False)
                .first()
            )

            if not template:
                raise HTTPException(
                    status_code=404, detail=f"Template not found: {template_euid}"
                )
            _ensure_template_manual_create_allowed(template)

            default_properties = template.json_addl or {}
            has_instantiation_layouts = bool(
                default_properties.get("instantiation_layouts")
            )
            template_code = f"{template.category}/{template.type}/{template.subtype}/{template.version}/"

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
            return RedirectResponse(
                tapdb_url(request, f"/object/{instance_euid}"), status_code=302
            )

        except ValueError as e:
            with conn.session_scope() as session:
                template = (
                    session.query(generic_template)
                    .filter_by(euid=template_euid, is_deleted=False)
                    .first()
                )
                default_properties = template.json_addl or {} if template else {}
                has_instantiation_layouts = bool(
                    default_properties.get("instantiation_layouts")
                )

                content = templates.get_template("create_instance.html").render(
                    request=request,
                    style=get_style(request),
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
                template = (
                    session.query(generic_template)
                    .filter_by(euid=template_euid, is_deleted=False)
                    .first()
                )
                default_properties = template.json_addl or {} if template else {}
                has_instantiation_layouts = bool(
                    default_properties.get("instantiation_layouts")
                )

                content = templates.get_template("create_instance.html").render(
                    request=request,
                    style=get_style(request),
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
                start_obj = (
                    session.query(generic_instance)
                    .filter_by(euid=start_euid, is_deleted=False)
                    .first()
                )
                if not start_obj:
                    return {"elements": {"nodes": [], "edges": []}}

                def traverse(instance, current_depth):
                    if current_depth > depth or instance.euid in visited_nodes:
                        return
                    visited_nodes.add(instance.euid)

                    nodes.append(
                        {
                            "data": {
                                "id": instance.euid,
                                "name": instance.name or instance.euid,
                                "type": instance.type,
                                "category": instance.category,
                                "subtype": instance.subtype,
                                "color": colors.get(instance.category, "#888888"),
                            }
                        }
                    )

                    # Traverse children
                    for lin in instance.parent_of_lineages.filter_by(is_deleted=False):
                        if lin.euid not in visited_edges:
                            visited_edges.add(lin.euid)
                            edges.append(
                                {
                                    "data": {
                                        "id": lin.euid,
                                        # Major wants directionality: child -> parent
                                        "source": lin.child_instance.euid,
                                        "target": instance.euid,
                                        "relationship_type": lin.relationship_type
                                        or "related",
                                    }
                                }
                            )
                        traverse(lin.child_instance, current_depth + 1)

                    # Traverse parents
                    for lin in instance.child_of_lineages.filter_by(is_deleted=False):
                        if lin.euid not in visited_edges:
                            visited_edges.add(lin.euid)
                            edges.append(
                                {
                                    "data": {
                                        "id": lin.euid,
                                        # Major wants directionality: child -> parent
                                        "source": instance.euid,
                                        "target": lin.parent_instance.euid,
                                        "relationship_type": lin.relationship_type
                                        or "related",
                                    }
                                }
                            )
                        traverse(lin.parent_instance, current_depth + 1)

                traverse(start_obj, 0)
            else:
                # Get all instances (limited)
                instances = (
                    session.query(generic_instance)
                    .filter_by(is_deleted=False)
                    .limit(200)
                    .all()
                )

                for inst in instances:
                    nodes.append(
                        {
                            "data": {
                                "id": inst.euid,
                                "name": inst.name or inst.euid,
                                "type": inst.type,
                                "category": inst.category,
                                "subtype": inst.subtype,
                                "color": colors.get(inst.category, "#888888"),
                            }
                        }
                    )
                    visited_nodes.add(inst.euid)

                # Get lineages for these instances
                lineages = (
                    session.query(generic_instance_lineage)
                    .filter_by(is_deleted=False)
                    .limit(500)
                    .all()
                )

                for lin in lineages:
                    p_euid = lin.parent_instance.euid if lin.parent_instance else None
                    c_euid = lin.child_instance.euid if lin.child_instance else None
                    if p_euid in visited_nodes and c_euid in visited_nodes:
                        edges.append(
                            {
                                "data": {
                                    "id": lin.euid,
                                    # Major wants directionality: child -> parent
                                    "source": c_euid,
                                    "target": p_euid,
                                    "relationship_type": lin.relationship_type
                                    or "related",
                                }
                            }
                        )

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
                        "uid": t.uid,
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
                        "uid": i.uid,
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
            obj, obj_type = _find_object_by_euid(session, euid)

            if not obj:
                raise HTTPException(status_code=404, detail=f"Object not found: {euid}")

            return {
                "uid": obj.uid,
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
                "external_refs": _external_ref_payloads(obj),
            }


@app.get("/api/graph/external")
@require_auth
async def api_get_external_graph(
    request: Request,
    source_euid: str,
    ref_index: int = Query(..., ge=0),
    depth: int = Query(4, ge=1, le=10),
):
    """Proxy a configured external graph and namespace it for merge-safe rendering."""
    user = request.state.user
    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            obj, _obj_type = _find_object_by_euid(session, source_euid)
            if obj is None:
                raise HTTPException(
                    status_code=404, detail=f"Object not found: {source_euid}"
                )
            try:
                ref = get_external_ref_by_index(obj, ref_index)
            except IndexError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            try:
                payload = fetch_remote_graph(request, ref, depth=depth)
                return namespace_external_graph(
                    payload,
                    ref=ref,
                    ref_index=ref_index,
                    source_euid=source_euid,
                )
            except Exception as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/graph/external/object")
@require_auth
async def api_get_external_graph_object(
    request: Request,
    source_euid: str,
    ref_index: int = Query(..., ge=0),
    euid: str = Query(...),
):
    """Proxy a configured external object detail payload."""
    user = request.state.user
    with get_db() as conn:
        conn.app_username = user.get("username")
        with conn.session_scope() as session:
            obj, _obj_type = _find_object_by_euid(session, source_euid)
            if obj is None:
                raise HTTPException(
                    status_code=404, detail=f"Object not found: {source_euid}"
                )
            try:
                ref = get_external_ref_by_index(obj, ref_index)
            except IndexError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            try:
                return fetch_remote_object_detail(request, ref, euid=euid)
            except Exception as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/lineage")
@require_admin
async def api_create_lineage(request: Request):
    """API: Create a new lineage between two instances. (Admin only)"""
    data = await request.json()
    parent_euid = data.get("parent_euid")
    child_euid = data.get("child_euid")
    relationship_type = (data.get("relationship_type") or "").strip() or "generic"

    if not parent_euid or not child_euid:
        raise HTTPException(
            status_code=400, detail="parent_euid and child_euid required"
        )

    user = getattr(request.state, "user", None)
    with get_db() as conn:
        conn.app_username = (user or {}).get("username")
        with conn.session_scope(commit=True) as session:
            parent = (
                session.query(generic_instance)
                .filter_by(euid=parent_euid, is_deleted=False)
                .first()
            )
            child = (
                session.query(generic_instance)
                .filter_by(euid=child_euid, is_deleted=False)
                .first()
            )

            if not parent:
                raise HTTPException(
                    status_code=404, detail=f"Parent not found: {parent_euid}"
                )
            if not child:
                raise HTTPException(
                    status_code=404, detail=f"Child not found: {child_euid}"
                )

            existing = (
                session.query(generic_instance_lineage)
                .filter_by(
                    parent_instance_uid=parent.uid,
                    child_instance_uid=child.uid,
                    relationship_type=relationship_type,
                    is_deleted=False,
                )
                .first()
            )
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
                if (
                    "idx_lineage_unique_edge" in str(exc)
                    or "duplicate key" in str(exc).lower()
                ):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "Lineage already exists for "
                            f"{child_euid} -> {parent_euid} ({relationship_type})"
                        ),
                    ) from exc
                raise

            return {"success": True, "euid": lineage.euid, "uid": lineage.uid}


@app.delete("/api/object/{euid}")
@require_admin
async def api_delete_object(request: Request, euid: str, hard_delete: bool = False):
    """API: Soft-delete an object by EUID. (Admin only)"""
    user = getattr(request.state, "user", None)
    with get_db() as conn:
        conn.app_username = (user or {}).get("username")
        with conn.session_scope(commit=True) as session:
            obj = (
                session.query(generic_template)
                .filter_by(euid=euid, is_deleted=False)
                .first()
            )
            if not obj:
                obj = (
                    session.query(generic_instance)
                    .filter_by(euid=euid, is_deleted=False)
                    .first()
                )
            if not obj:
                obj = (
                    session.query(generic_instance_lineage)
                    .filter_by(euid=euid, is_deleted=False)
                    .first()
                )

            if not obj:
                raise HTTPException(status_code=404, detail=f"Object not found: {euid}")

            obj.is_deleted = True
            session.flush()

            return {"success": True, "message": f"Object {euid} soft-deleted"}
