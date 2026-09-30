"""Authenticated management API; native services own all integrity behavior."""

from __future__ import annotations
from typing import Any, Callable
from uuid import uuid4
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from daylily_tapdb.security_context import Attribution
from daylily_tapdb.services.integrity_operations import (
    READ_OPERATIONS,
    WRITE_OPERATIONS,
    execute_integrity_operation,
)
from daylily_tapdb.revisions import RevisionConflict
from daylily_tapdb.web.runtime import get_db


def authenticated_attribution(
    user: dict[str, Any],
    conn: Any,
    *,
    operation_id: str | None = None,
    reason: str | None = None,
) -> Attribution:
    """Use authenticated native actor identity or an explicit host identity.

    Username/email is display context, not an inferred stable external subject.
    Hosts must supply actor_issuer/actor_subject from their authentication layer.
    """
    cfg = conn._bundle.cfg
    if user.get("actor_issuer") and user.get("actor_subject"):
        issuer, subject = user["actor_issuer"], user["actor_subject"]
    elif user.get("authentication_source") == "host":
        raise ValueError(
            "host-authenticated writes require explicit actor_issuer and actor_subject"
        )
    elif user.get("euid"):
        issuer = f"tapdb:{cfg['domain_code']}:{cfg['owner_repo_name']}"
        subject = user["euid"]
    elif isinstance(user.get("uid"), int) and user["uid"] > 0:
        issuer = f"tapdb:{cfg['database']}:{cfg['schema_name']}:{cfg['domain_code']}:{cfg['owner_repo_name']}"
        subject = str(user["uid"])
    else:
        raise ValueError(
            "authenticated stable actor issuer/subject is required for TapDB 11 writes"
        )
    return Attribution(
        user.get("actor_kind", "human"),
        issuer,
        subject,
        str(cfg["owner_repo_name"]),
        str(uuid4()),
        operation_id or str(uuid4()),
        reason,
    )


def integrity_router(
    *,
    config_path: str,
    require_user: Callable,
    require_admin: Callable,
    owner_validators: dict | None = None,
) -> APIRouter:
    router = APIRouter()

    async def execute(operation, payload, user, writing):
        allowed = WRITE_OPERATIONS if writing else READ_OPERATIONS
        if operation not in allowed:
            raise HTTPException(404, "unknown operation for this read/write surface")
        try:
            with get_db(config_path) as conn:
                conn.app_username = user["username"]
                if writing:
                    conn.attribution = authenticated_attribution(
                        user,
                        conn,
                        operation_id=payload.get("operation_id"),
                        reason=payload.get("reason"),
                    )
                if operation == "audit" and user.get("role") != "admin":
                    actor = authenticated_attribution(user, conn)
                    payload = {
                        **payload,
                        "actor_issuer": actor.actor_issuer,
                        "actor_subject": actor.actor_subject,
                    }
                with conn.session_scope(commit=writing) as session:
                    result = execute_integrity_operation(
                        session, operation, payload, owner_validators=owner_validators
                    )
                return jsonable_encoder(result)
        except RevisionConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except PermissionError as exc:
            raise HTTPException(403, str(exc)) from exc
        except (ValueError, TypeError, KeyError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/api/integrity/read/{operation}")
    async def read(operation: str, payload: dict[str, Any], user=Depends(require_user)):
        # POST carries bounded query specifications; this endpoint never writes.
        return await execute(operation, payload, user, False)

    @router.post("/api/integrity/write/{operation}")
    async def write(
        operation: str, payload: dict[str, Any], user=Depends(require_admin)
    ):
        return await execute(operation, payload, user, True)

    return router
