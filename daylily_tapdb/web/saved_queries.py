"""Authenticated API for native saved queries and independent DAG views."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from pydantic import Field, JsonValue

from daylily_tapdb.gui.integrity import authenticated_attribution
from daylily_tapdb.revisions import RevisionConflict
from daylily_tapdb.security_context import canonical_additional_tenant_ids
from daylily_tapdb.services.saved_queries import (
    DefinitionModel,
    QueryDefinition,
    QueryExecutor,
    SavedQueryPrincipal,
    SavedQueryService,
    ViewDefinition,
    query_executors,
)
from daylily_tapdb.web.runtime import get_db


class CreateDefinition(DefinitionModel):
    name: str = Field(min_length=1)
    description: str = ""
    definition: dict[str, JsonValue]


class EditDefinition(CreateDefinition):
    expected_revision: int = Field(ge=0)


class RevisionRequest(DefinitionModel):
    expected_revision: int = Field(ge=0)


class VariantDefinition(RevisionRequest):
    name: str = Field(min_length=1)
    description: str = ""
    definition: dict[str, JsonValue] | None = None


class BindQuery(RevisionRequest):
    query_euid: str | None = None
    expected_query_revision: int | None = Field(default=None, ge=0)


class RunQuery(RevisionRequest):
    parameters: dict[str, JsonValue] = Field(default_factory=dict)


class RunView(RunQuery):
    expected_query_revision: int = Field(ge=0)


def _principal(user: Mapping[str, Any], conn: Any) -> SavedQueryPrincipal:
    cfg = conn._bundle.cfg
    primary = cfg.get("tenant_id") or None
    additional = canonical_additional_tenant_ids(cfg.get("additional_tenant_ids", ()))
    allowed = ({str(primary)} if primary is not None else set()) | set(additional)
    tenant = user.get("tenant_id")
    if tenant is None:
        if primary is None or allowed != {str(primary)}:
            raise PermissionError(
                "saved definitions require an authenticated tenant_id when the "
                "runtime does not select exactly one explicit primary tenant"
            )
        tenant = str(primary)
    if not isinstance(tenant, str) or str(UUID(tenant)) != tenant:
        raise PermissionError("authenticated tenant_id must be a canonical UUID")
    if tenant not in allowed:
        raise PermissionError("authenticated tenant is outside the configured runtime scope")
    attribution = authenticated_attribution(dict(user), conn)
    conn.attribution = attribution
    return SavedQueryPrincipal(
        tenant_id=UUID(tenant),
        actor_issuer=attribution.actor_issuer,
        actor_subject=attribution.actor_subject,
        is_admin=user.get("role") == "admin",
    )


def saved_queries_router(
    *,
    config_path: str,
    require_user: Callable,
    executors: Mapping[str, QueryExecutor] | None = None,
) -> APIRouter:
    """Mount under the host's authenticated GUI/API prefix.

    Executors are host-owned implementations registered at application startup.
    A request can select a registered executor, never install one or provide SQL.
    """
    router = APIRouter(tags=["saved queries and DAG views"])
    extensions = dict(executors or {})
    registered = query_executors(extensions)

    def execute(user, callback, *, writing=False):
        try:
            with get_db(config_path) as conn:
                conn.app_username = user["username"]
                principal = _principal(user, conn)
                with conn.session_scope(
                    commit=writing, read_only=not writing,
                    isolation_level="REPEATABLE READ",
                ) as session:
                    service = SavedQueryService(
                        session,
                        domain_code=conn._bundle.cfg["domain_code"],
                        owner_repo_name=conn._bundle.cfg["owner_repo_name"],
                        principal=principal, executors=extensions,
                    )
                    result = callback(service)
                    # Materialize a response before the request transaction closes.
                    payload = jsonable_encoder(result)
                return payload
        except RevisionConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except PermissionError as exc:
            raise HTTPException(403, str(exc)) from exc
        except (ValueError, TypeError, KeyError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.get("/api/saved-query-executors")
    def executor_schemas(user=Depends(require_user)):
        return {
            "contract": "tapdb.saved-query-executors/v1",
            "query_definition_schema": QueryDefinition.model_json_schema(),
            "view_definition_schema": ViewDefinition.model_json_schema(),
            "executors": [
                {
                    "executor": key,
                    "specification_schema": value.definition_model.model_json_schema(),
                    "parameters_schema": value.parameters_model.model_json_schema(),
                }
                for key, value in registered.items()
            ],
        }

    def mount(kind: str, path: str):
        @router.get(path, name=f"list_saved_{kind}")
        def list_definitions(
            limit: int = Query(default=25, ge=1, le=100),
            after_uid: int = Query(default=0, ge=0),
            user=Depends(require_user),
        ):
            return execute(user, lambda service: service.list(
                kind, limit=limit, after_uid=after_uid
            ))

        @router.post(path, status_code=201, name=f"create_saved_{kind}")
        def create_definition(payload: CreateDefinition, user=Depends(require_user)):
            return execute(user, lambda service: service.create(
                kind, **payload.model_dump(mode="json")
            ), writing=True)

        @router.get(path + "/{euid}", name=f"get_saved_{kind}")
        def get_definition(euid: str, user=Depends(require_user)):
            return execute(user, lambda service: service.get(kind, euid))

        @router.put(path + "/{euid}", name=f"edit_saved_{kind}")
        def edit_definition(
            euid: str, payload: EditDefinition, user=Depends(require_user),
        ):
            return execute(user, lambda service: service.update(
                kind, euid, **payload.model_dump(mode="json")
            ), writing=True)

        @router.delete(path + "/{euid}", name=f"delete_saved_{kind}")
        def delete_definition(
            euid: str, payload: RevisionRequest, user=Depends(require_user),
        ):
            return execute(user, lambda service: service.delete(
                kind, euid, expected_revision=payload.expected_revision
            ), writing=True)

        @router.post(path + "/{euid}/variants", status_code=201, name=f"variant_saved_{kind}")
        def create_variant(
            euid: str, payload: VariantDefinition, user=Depends(require_user),
        ):
            return execute(user, lambda service: service.variant(
                kind, euid, **payload.model_dump(mode="json")
            ), writing=True)

        @router.get(path + "/{euid}/revisions/{revision}", name=f"revision_saved_{kind}")
        def get_revision(
            euid: str, revision: int, epoch: str | None = None,
            user=Depends(require_user),
        ):
            return execute(user, lambda service: service.revision(
                kind, euid, revision=revision, epoch=epoch
            ))

    mount("query", "/api/saved-queries")
    mount("view", "/api/dag-views")

    @router.put("/api/dag-views/{euid}/query")
    def bind_query(euid: str, payload: BindQuery, user=Depends(require_user)):
        return execute(user, lambda service: service.bind_query(
            euid, **payload.model_dump(mode="json")
        ), writing=True)

    @router.post("/api/saved-queries/{euid}/run")
    def run_query(euid: str, payload: RunQuery, user=Depends(require_user)):
        return execute(user, lambda service: service.run(
            euid, **payload.model_dump(mode="json")
        ))

    @router.post("/api/dag-views/{euid}/run")
    def run_view(euid: str, payload: RunView, user=Depends(require_user)):
        return execute(user, lambda service: service.run_view(
            euid, **payload.model_dump(mode="json")
        ))

    return router
