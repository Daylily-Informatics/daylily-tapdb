"""Minimal canonical-user authorization through an exact runtime identity grant."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class UserAuthorization:
    uid: int
    euid: str
    role: str | None
    is_active: bool


def resolve_user_authorization(session: Session, user_uid: int) -> UserAuthorization | None:
    """Resolve one exact UID; never enumerate users or return authentication secrets.

    The canonical database routine enforces the authenticated runtime binding
    and any required cross-issuer grant. Missing grants/identities return None.
    Missing schema capability is an error, never a less-restricted query.
    """
    if isinstance(user_uid, bool) or not isinstance(user_uid, int) or not 0 < user_uid < 2**63:
        raise ValueError("user_uid must be an exact positive BIGINT")
    result = session.execute(
        text("SELECT tapdb_resolve_user_authorization(:user_uid)"),
        {"user_uid": user_uid},
    ).scalar_one()
    if result is None:
        return None
    if (
        not isinstance(result, dict)
        or set(result) != {"uid", "euid", "role", "is_active"}
        or type(result["uid"]) is not int
        or result["uid"] != user_uid
        or not isinstance(result["euid"], str)
        or not result["euid"]
        or type(result["is_active"]) is not bool
        or (result["role"] is not None and not isinstance(result["role"], str))
    ):
        raise ValueError("Canonical user authorization response is invalid")
    return UserAuthorization(**result)
