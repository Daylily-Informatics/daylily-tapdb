"""Standalone authentication routes for TapDB's canonical GUI."""

from __future__ import annotations

import base64
import json
import logging
import secrets
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment

from admin.auth import (
    authenticate_with_cognito,
    change_cognito_password,
    create_cognito_user_account,
    get_current_user,
    get_or_create_user_from_email,
    get_user_by_username,
    respond_to_new_password_challenge,
    update_last_login,
)
from admin.cognito import get_cognito_auth, resolve_tapdb_pool_config

LOGGER = logging.getLogger(__name__)


def _gui_base_path(request: Request) -> str:
    return str(request.scope.get("root_path") or "").rstrip("/")


def _gui_url(request: Request, path: str) -> str:
    suffix = "/" + str(path or "/").lstrip("/")
    return f"{_gui_base_path(request)}{suffix}"


def _render_auth(
    templates: Environment,
    request: Request,
    template_name: str,
    **context: Any,
) -> HTMLResponse:
    return HTMLResponse(
        templates.get_template(template_name).render(request=request, **context)
    )


def _require_https_url(url: str, *, label: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError(f"{label} must be an https URL")
    return url


def _normalize_cognito_domain(raw_domain: str) -> str:
    domain = str(raw_domain or "").strip()
    if not domain:
        raise RuntimeError("cognito_domain is not configured")
    parts = urlsplit(domain)
    if (
        parts.scheme
        or parts.netloc
        or "/" in domain
        or any(character.isspace() for character in domain)
    ):
        raise RuntimeError(f"Invalid cognito_domain value: {raw_domain!r}")
    return domain


def _resolve_cognito_oauth_runtime() -> dict[str, str]:
    pool = resolve_tapdb_pool_config()
    domain = _normalize_cognito_domain(pool.domain)
    callback_url = str(pool.callback_url or "").strip()
    if not callback_url:
        raise RuntimeError("cognito_callback_url is missing in TapDB config")
    client_id = str(pool.app_client_id or "").strip()
    if not client_id:
        raise RuntimeError("cognito_app_client_id is missing in TapDB config")
    return {
        "domain": domain,
        "callback_url": callback_url,
        "client_id": client_id,
        "client_secret": str(pool.app_client_secret or ""),
        "scope": "openid email profile",
    }


def _build_cognito_authorize_url(runtime: dict[str, str], state: str) -> str:
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
    runtime: dict[str, str], code: str
) -> dict[str, Any]:
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
    if runtime["client_secret"]:
        credentials = f"{runtime['client_id']}:{runtime['client_secret']}".encode(
            "utf-8"
        )
        headers["Authorization"] = (
            f"Basic {base64.b64encode(credentials).decode('ascii')}"
        )
    else:
        payload["client_id"] = runtime["client_id"]
    request = UrlRequest(
        token_url,
        data=urlencode(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:  # nosec B310
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        try:
            details = exc.read().decode("utf-8").strip()
        except Exception:
            details = ""
        raise RuntimeError(f"Cognito token exchange failed: {details or exc}") from exc
    except URLError as exc:
        raise RuntimeError(f"Cognito token endpoint is unreachable: {exc}") from exc
    try:
        result = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Cognito token response was not valid JSON") from exc
    if not isinstance(result, dict):
        raise RuntimeError("Cognito token response was not an object")
    if "error" in result:
        reason = result.get("error_description") or result["error"]
        raise RuntimeError(f"Cognito token exchange failed: {reason}")
    return result


def _fetch_oauth_userinfo(runtime: dict[str, str], access_token: str) -> dict[str, Any]:
    endpoint = _require_https_url(
        f"https://{runtime['domain']}/oauth2/userInfo",
        label="Cognito userInfo endpoint",
    )
    request = UrlRequest(
        endpoint,
        headers={"Authorization": f"Bearer {access_token}"},
        method="GET",
    )
    with urlopen(request, timeout=15) as response:  # nosec B310
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Cognito userInfo response was not an object")
    return payload


def _resolve_oauth_user_profile(
    tokens: dict[str, Any], runtime: dict[str, str]
) -> dict[str, str]:
    claims: dict[str, Any] = {}
    access_token = str(tokens.get("access_token") or "")
    id_token = str(tokens.get("id_token") or "")
    if access_token:
        try:
            claims.update(_fetch_oauth_userinfo(runtime, access_token))
        except Exception as exc:
            LOGGER.warning("Failed to fetch Cognito userInfo claims: %s", exc)
    if (not claims or not claims.get("email")) and id_token:
        try:
            claims.update(get_cognito_auth().verify_token(id_token))
        except Exception as exc:
            LOGGER.warning("Failed to verify Cognito id_token claims: %s", exc)
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
            "OAuth login succeeded but no email or username claim was returned"
        )
    display_name = str(
        claims.get("name")
        or claims.get("preferred_username")
        or claims.get("given_name")
        or ""
    ).strip()
    return {"email": email, "display_name": display_name}


def _safe_next_path(request: Request, value: str) -> str:
    raw = str(value or "").strip()
    base = _gui_base_path(request)
    if not raw or raw == "/":
        return _gui_url(request, "/")
    if not raw.startswith("/") or raw.startswith("//"):
        return _gui_url(request, "/")
    if base and not (raw == base or raw.startswith(f"{base}/")):
        return _gui_url(request, raw)
    return raw


async def _form(request: Request) -> dict[str, str]:
    form = await request.form()
    return {str(key): str(value) for key, value in form.items()}


def create_tapdb_gui_auth_router(*, templates: Environment) -> APIRouter:
    """Build the auth/account routes used by the standalone canonical GUI."""

    router = APIRouter()

    @router.get("/auth/login")
    async def oauth_login(request: Request, next: str = Query("")):
        if await get_current_user(request):
            return RedirectResponse(_gui_url(request, "/"), status_code=302)
        try:
            runtime = _resolve_cognito_oauth_runtime()
        except Exception as exc:
            return _render_auth(
                templates,
                request,
                "login.html",
                error=f"OAuth login is not configured: {exc}",
            )
        state = secrets.token_urlsafe(32)
        request.session["oauth_state"] = state
        request.session["oauth_next"] = _safe_next_path(request, next)
        return RedirectResponse(
            _build_cognito_authorize_url(runtime, state), status_code=302
        )

    @router.get("/auth/callback")
    async def oauth_callback(
        request: Request,
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
        error_description: str | None = None,
    ):
        if error:
            return _render_auth(
                templates,
                request,
                "login.html",
                error=f"OAuth login failed: {error_description or error}",
            )
        expected_state = request.session.pop("oauth_state", None)
        if (
            not expected_state
            or not state
            or not secrets.compare_digest(state, expected_state)
        ):
            return _render_auth(
                templates,
                request,
                "login.html",
                error="OAuth login failed: invalid state",
            )
        if not code:
            return _render_auth(
                templates,
                request,
                "login.html",
                error="OAuth login failed: missing authorization code",
            )
        try:
            runtime = _resolve_cognito_oauth_runtime()
            tokens = _exchange_oauth_authorization_code(runtime, code)
            profile = _resolve_oauth_user_profile(tokens, runtime)
            user = get_or_create_user_from_email(
                profile["email"],
                display_name=profile["display_name"] or None,
                role="user",
            )
        except Exception as exc:
            return _render_auth(
                templates,
                request,
                "login.html",
                error=f"OAuth login failed: {exc}",
            )
        request.session["user_uid"] = user["uid"]
        request.session["cognito_username"] = profile["email"]
        if tokens.get("access_token"):
            request.session["cognito_access_token"] = tokens["access_token"]
        request.session.pop("cognito_challenge", None)
        request.session.pop("cognito_challenge_session", None)
        update_last_login(user["uid"])
        return RedirectResponse(
            _safe_next_path(request, request.session.pop("oauth_next", "")),
            status_code=302,
        )

    @router.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request, error: str | None = None):
        user = await get_current_user(request)
        if user:
            destination = (
                "/change-password" if user.get("require_password_change") else "/"
            )
            return RedirectResponse(_gui_url(request, destination), status_code=302)
        return _render_auth(templates, request, "login.html", error=error)

    @router.post("/login", response_class=HTMLResponse)
    async def login_submit(request: Request):
        form = await _form(request)
        identity = form.get("username", "").strip()
        password = form.get("password", "")
        user = get_user_by_username(identity)
        cognito_username = str(user.get("email") if user else identity)
        try:
            auth_result = authenticate_with_cognito(cognito_username, password)
        except ValueError:
            return _render_auth(
                templates,
                request,
                "login.html",
                error="Invalid username or password",
            )
        except Exception as exc:
            return _render_auth(
                templates,
                request,
                "login.html",
                error=f"Authentication error: {exc}",
            )
        if not user:
            try:
                user = get_or_create_user_from_email(cognito_username)
            except Exception as exc:
                return _render_auth(
                    templates,
                    request,
                    "login.html",
                    error=(
                        "Authenticated with Cognito, but failed to provision the "
                        f"TapDB user: {exc}"
                    ),
                )
        request.session["user_uid"] = user["uid"]
        request.session["cognito_username"] = cognito_username
        update_last_login(user["uid"])
        if auth_result.get("challenge") == "NEW_PASSWORD_REQUIRED":
            request.session["cognito_challenge"] = "NEW_PASSWORD_REQUIRED"
            request.session["cognito_challenge_session"] = str(
                auth_result.get("session") or ""
            )
            return RedirectResponse(
                _gui_url(request, "/change-password"), status_code=302
            )
        access_token = str(auth_result.get("access_token") or "")
        if not access_token:
            request.session.clear()
            return _render_auth(
                templates,
                request,
                "login.html",
                error="Authentication failed: no access token returned",
            )
        request.session["cognito_access_token"] = access_token
        request.session.pop("cognito_challenge", None)
        request.session.pop("cognito_challenge_session", None)
        destination = "/change-password" if user.get("require_password_change") else "/"
        return RedirectResponse(_gui_url(request, destination), status_code=302)

    @router.get("/signup", response_class=HTMLResponse)
    async def signup_page(request: Request, error: str | None = None):
        if await get_current_user(request):
            return RedirectResponse(_gui_url(request, "/"), status_code=302)
        return _render_auth(templates, request, "signup.html", error=error)

    @router.post("/signup", response_class=HTMLResponse)
    async def signup_submit(request: Request):
        form = await _form(request)
        email = form.get("email", "").strip().lower()
        display_name = form.get("display_name", "").strip() or None
        password = form.get("password", "")
        confirm_password = form.get("confirm_password", "")
        if not email or "@" not in email:
            return _render_auth(
                templates, request, "signup.html", error="Valid email is required"
            )
        if len(password) < 8:
            return _render_auth(
                templates,
                request,
                "signup.html",
                error="Password must be at least 8 characters",
            )
        if password != confirm_password:
            return _render_auth(
                templates,
                request,
                "signup.html",
                error="Passwords do not match",
            )
        try:
            create_cognito_user_account(email, password, display_name=display_name)
            user = get_or_create_user_from_email(
                email, display_name=display_name, role="user"
            )
            auth_result = authenticate_with_cognito(email, password)
        except Exception as exc:
            return _render_auth(
                templates,
                request,
                "signup.html",
                error=f"Account creation failed: {exc}",
            )
        request.session["user_uid"] = user["uid"]
        request.session["cognito_username"] = email
        update_last_login(user["uid"])
        if auth_result.get("challenge") == "NEW_PASSWORD_REQUIRED":
            request.session["cognito_challenge"] = "NEW_PASSWORD_REQUIRED"
            request.session["cognito_challenge_session"] = str(
                auth_result.get("session") or ""
            )
            return RedirectResponse(
                _gui_url(request, "/change-password"), status_code=302
            )
        if auth_result.get("access_token"):
            request.session["cognito_access_token"] = auth_result["access_token"]
        request.session.pop("cognito_challenge", None)
        request.session.pop("cognito_challenge_session", None)
        return RedirectResponse(_gui_url(request, "/"), status_code=302)

    @router.get("/logout")
    async def logout(request: Request):
        request.session.clear()
        return RedirectResponse(_gui_url(request, "/login"), status_code=302)

    @router.get("/change-password", response_class=HTMLResponse)
    async def change_password_page(
        request: Request,
        error: str | None = None,
        success: str | None = None,
    ):
        user = await get_current_user(request)
        if not user:
            return RedirectResponse(_gui_url(request, "/login"), status_code=302)
        return _render_auth(
            templates,
            request,
            "change_password.html",
            user=user,
            required=bool(user.get("require_password_change")),
            challenge_required=(
                request.session.get("cognito_challenge") == "NEW_PASSWORD_REQUIRED"
            ),
            error=error,
            success=success,
        )

    @router.post("/change-password", response_class=HTMLResponse)
    async def change_password_submit(request: Request):
        user = await get_current_user(request)
        if not user:
            return RedirectResponse(_gui_url(request, "/login"), status_code=302)
        form = await _form(request)
        current_password = form.get("current_password", "")
        new_password = form.get("new_password", "")
        confirm_password = form.get("confirm_password", "")
        challenge_required = (
            request.session.get("cognito_challenge") == "NEW_PASSWORD_REQUIRED"
        )

        def render_error(message: str) -> HTMLResponse:
            return _render_auth(
                templates,
                request,
                "change_password.html",
                user=user,
                required=bool(user.get("require_password_change")),
                challenge_required=challenge_required,
                error=message,
                success=None,
            )

        if len(new_password) < 8:
            return render_error("New password must be at least 8 characters")
        if new_password != confirm_password:
            return render_error("New passwords do not match")
        if challenge_required:
            challenge_session = str(
                request.session.get("cognito_challenge_session") or ""
            )
            if not challenge_session:
                return render_error(
                    "Missing Cognito challenge session. Please sign in again."
                )
            username = str(
                request.session.get("cognito_username")
                or user.get("email")
                or user.get("username")
                or ""
            )
            try:
                result = respond_to_new_password_challenge(
                    username, new_password, challenge_session
                )
            except Exception as exc:
                return render_error(f"Password update failed: {exc}")
            if result.get("access_token"):
                request.session["cognito_access_token"] = result["access_token"]
            request.session.pop("cognito_challenge", None)
            request.session.pop("cognito_challenge_session", None)
            return RedirectResponse(_gui_url(request, "/"), status_code=302)
        if not current_password:
            return render_error("Current password is required")
        access_token = str(request.session.get("cognito_access_token") or "")
        if not access_token:
            return render_error(
                "Session missing Cognito access token. Please sign in again."
            )
        try:
            change_cognito_password(access_token, current_password, new_password)
        except Exception as exc:
            return render_error(f"Password update failed: {exc}")
        if user.get("require_password_change"):
            return RedirectResponse(_gui_url(request, "/"), status_code=302)
        return _render_auth(
            templates,
            request,
            "change_password.html",
            user=user,
            required=False,
            challenge_required=False,
            error=None,
            success="Password changed successfully",
        )

    return router


__all__ = ["create_tapdb_gui_auth_router"]
