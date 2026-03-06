"""Actor-backed TAPDB auth user storage helpers.

Auth users are stored as `generic_instance` rows using the dedicated
`generic/actor/system_user/1.0` template.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

SYSTEM_USER_TEMPLATE_CATEGORY = "generic"
SYSTEM_USER_TEMPLATE_TYPE = "actor"
SYSTEM_USER_TEMPLATE_SUBTYPE = "system_user"
SYSTEM_USER_TEMPLATE_VERSION = "1.0"
SYSTEM_USER_TEMPLATE_CODE = (
    f"{SYSTEM_USER_TEMPLATE_CATEGORY}/{SYSTEM_USER_TEMPLATE_TYPE}/"
    f"{SYSTEM_USER_TEMPLATE_SUBTYPE}/{SYSTEM_USER_TEMPLATE_VERSION}"
)

_SYSTEM_USER_WHERE = """
    gi.is_deleted = FALSE
    AND gi.polymorphic_discriminator = 'actor_instance'
    AND gi.category = 'generic'
    AND gi.type = 'actor'
    AND gi.subtype = 'system_user'
"""

_ACTIVE_EXPR = "COALESCE(NULLIF(gi.json_addl->>'is_active', '')::boolean, TRUE)"
_REQUIRE_PASSWORD_CHANGE_EXPR = (
    "COALESCE(NULLIF(gi.json_addl->>'require_password_change', '')::boolean, FALSE)"
)


@dataclass
class ActorUser:
    uid: int
    username: str
    email: Optional[str]
    display_name: Optional[str]
    role: str
    is_active: bool
    require_password_change: bool
    password_hash: Optional[str]
    last_login_dt: Optional[str]
    cognito_username: Optional[str]
    euid: str
    created_dt: Any
    modified_dt: Any

    def to_session_user(self) -> dict[str, Any]:
        return {
            "uid": self.uid,
            "username": self.username,
            "email": self.email,
            "display_name": self.display_name,
            "role": self.role,
            "is_active": self.is_active,
            "require_password_change": self.require_password_change,
        }


def normalize_login_identifier(value: str) -> str:
    normalized = (value or "").strip().lower()
    if not normalized:
        raise ValueError("login_identifier is required")
    return normalized


def _norm_optional(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _row_to_actor_user(row: Any) -> ActorUser:
    role = (row["role"] or "user").strip().lower()
    if role not in ("admin", "user"):
        role = "user"
    username = (row["login_identifier"] or "").strip().lower()
    email = _norm_optional(row["email"])
    if not username and email:
        username = email.lower()
    return ActorUser(
        uid=int(row["uid"]),
        username=username,
        email=email.lower() if email else None,
        display_name=_norm_optional(row["display_name"]),
        role=role,
        is_active=bool(row["is_active"]),
        require_password_change=bool(row["require_password_change"]),
        password_hash=_norm_optional(row["password_hash"]),
        last_login_dt=_norm_optional(row["last_login_dt"]),
        cognito_username=_norm_optional(row["cognito_username"]),
        euid=row["euid"],
        created_dt=row["created_dt"],
        modified_dt=row["modified_dt"],
    )


def _select_user_columns() -> str:
    return f"""
        SELECT
            gi.uid,
            gi.euid,
            gi.created_dt,
            gi.modified_dt,
            NULLIF(gi.json_addl->>'login_identifier', '') AS login_identifier,
            NULLIF(gi.json_addl->>'email', '') AS email,
            NULLIF(gi.json_addl->>'display_name', '') AS display_name,
            COALESCE(NULLIF(gi.json_addl->>'role', ''), 'user') AS role,
            {_ACTIVE_EXPR} AS is_active,
            {_REQUIRE_PASSWORD_CHANGE_EXPR} AS require_password_change,
            NULLIF(gi.json_addl->>'password_hash', '') AS password_hash,
            NULLIF(gi.json_addl->>'last_login_dt', '') AS last_login_dt,
            NULLIF(gi.json_addl->>'cognito_username', '') AS cognito_username
        FROM generic_instance gi
        WHERE {_SYSTEM_USER_WHERE}
    """


def _get_system_user_template_uid(session: Session) -> int:
    row = session.execute(
        text(
            """
            SELECT uid
            FROM generic_template
            WHERE is_deleted = FALSE
              AND category = :category
              AND type = :type
              AND subtype = :subtype
              AND version = :version
            LIMIT 1
            """
        ),
        {
            "category": SYSTEM_USER_TEMPLATE_CATEGORY,
            "type": SYSTEM_USER_TEMPLATE_TYPE,
            "subtype": SYSTEM_USER_TEMPLATE_SUBTYPE,
            "version": SYSTEM_USER_TEMPLATE_VERSION,
        },
    ).fetchone()
    if not row:
        raise RuntimeError(
            "Missing required actor template "
            f"{SYSTEM_USER_TEMPLATE_CODE}. Run template seed first."
        )
    return int(row[0])


def get_by_login_identifier(
    session: Session,
    login_identifier: str,
    *,
    include_inactive: bool = False,
) -> Optional[ActorUser]:
    normalized = normalize_login_identifier(login_identifier)
    sql = _select_user_columns()
    if not include_inactive:
        sql += f"\n  AND {_ACTIVE_EXPR} = TRUE"
    sql += (
        "\n  AND lower(COALESCE(gi.json_addl->>'login_identifier', '')) = :identifier\n"
        "LIMIT 1"
    )
    row = session.execute(text(sql), {"identifier": normalized}).mappings().first()
    if not row:
        return None
    return _row_to_actor_user(row)


def get_by_login_or_email(
    session: Session,
    identifier: str,
    *,
    include_inactive: bool = False,
) -> Optional[ActorUser]:
    normalized = normalize_login_identifier(identifier)
    sql = _select_user_columns()
    if not include_inactive:
        sql += f"\n  AND {_ACTIVE_EXPR} = TRUE"
    sql += """
      AND (
            lower(COALESCE(gi.json_addl->>'login_identifier', '')) = :identifier
         OR lower(COALESCE(gi.json_addl->>'email', '')) = :identifier
      )
      LIMIT 1
    """
    row = session.execute(text(sql), {"identifier": normalized}).mappings().first()
    if not row:
        return None
    return _row_to_actor_user(row)


def get_by_uid(
    session: Session,
    user_uid: int | str,
    *,
    include_inactive: bool = False,
) -> Optional[ActorUser]:
    sql = _select_user_columns()
    if not include_inactive:
        sql += f"\n  AND {_ACTIVE_EXPR} = TRUE"
    sql += "\n  AND gi.uid = :uid\nLIMIT 1"
    row = session.execute(text(sql), {"uid": int(user_uid)}).mappings().first()
    if not row:
        return None
    return _row_to_actor_user(row)


def list_users(session: Session, *, include_inactive: bool = False) -> list[ActorUser]:
    sql = _select_user_columns()
    if not include_inactive:
        sql += f"\n  AND {_ACTIVE_EXPR} = TRUE"
    sql += "\nORDER BY lower(COALESCE(gi.json_addl->>'login_identifier', ''))"
    rows = session.execute(text(sql)).mappings().all()
    return [_row_to_actor_user(r) for r in rows]


def create_or_get(
    session: Session,
    *,
    login_identifier: str,
    email: Optional[str] = None,
    display_name: Optional[str] = None,
    role: str = "user",
    is_active: bool = True,
    require_password_change: bool = False,
    password_hash: Optional[str] = None,
    cognito_username: Optional[str] = None,
) -> tuple[ActorUser, bool]:
    normalized_login = normalize_login_identifier(login_identifier)
    normalized_email = _norm_optional(email)
    normalized_display_name = _norm_optional(display_name)
    normalized_cognito_username = _norm_optional(cognito_username)
    selected_role = (role or "user").strip().lower()
    if selected_role not in ("admin", "user"):
        raise ValueError(f"invalid role: {role}")

    existing = get_by_login_identifier(session, normalized_login, include_inactive=True)
    if existing:
        return existing, False

    template_uid = _get_system_user_template_uid(session)
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "login_identifier": normalized_login,
        "email": (normalized_email or normalized_login).lower(),
        "display_name": normalized_display_name,
        "role": selected_role,
        "is_active": bool(is_active),
        "require_password_change": bool(require_password_change),
        "password_hash": _norm_optional(password_hash),
        "last_login_dt": None,
        "cognito_username": normalized_cognito_username,
        "provisioned_dt": now_iso,
    }
    if payload["display_name"] is None:
        payload["display_name"] = normalized_login
    if payload["cognito_username"] is None and normalized_email:
        payload["cognito_username"] = normalized_email.lower()

    insert_sql = text(
        """
        INSERT INTO generic_instance (
            name,
            polymorphic_discriminator,
            category,
            type,
            subtype,
            version,
            template_uid,
            json_addl,
            bstatus,
            is_singleton,
            is_deleted
        )
        VALUES (
            :name,
            'actor_instance',
            'generic',
            'actor',
            'system_user',
            '1.0',
            :template_uuid,
            CAST(:json_addl AS jsonb),
            'active',
            FALSE,
            FALSE
        )
        RETURNING uid
        """
    )
    try:
        inserted = session.execute(
            insert_sql,
            {
                "name": payload["display_name"] or normalized_login,
                "template_uid": template_uid,
                "json_addl": json.dumps(payload),
            },
        ).fetchone()
        if inserted:
            created = get_by_uid(session, int(inserted[0]), include_inactive=True)
            if created:
                return created, True
    except IntegrityError:
        # Another session may have created this login identifier concurrently.
        pass

    existing = get_by_login_identifier(session, normalized_login, include_inactive=True)
    if existing:
        return existing, False
    raise RuntimeError(f"Failed to create actor user {normalized_login}")


def set_last_login(session: Session, user_uid: int | str) -> None:
    last_login_dt = datetime.now(timezone.utc).isoformat()
    session.execute(
        text(
            """
            UPDATE generic_instance gi
            SET json_addl = jsonb_set(
                    COALESCE(gi.json_addl, '{}'::jsonb),
                    '{last_login_dt}',
                    to_jsonb(CAST(:last_login_dt AS text)),
                    TRUE
                ),
                modified_dt = NOW()
            WHERE gi.uid = :uid
              AND gi.is_deleted = FALSE
              AND gi.polymorphic_discriminator = 'actor_instance'
              AND gi.category = 'generic'
              AND gi.type = 'actor'
              AND gi.subtype = 'system_user'
            """
        ),
        {"uid": int(user_uid), "last_login_dt": last_login_dt},
    )


def set_role(session: Session, login_identifier: str, role: str) -> bool:
    selected_role = (role or "").strip().lower()
    if selected_role not in ("admin", "user"):
        raise ValueError(f"invalid role: {role}")
    normalized_login = normalize_login_identifier(login_identifier)
    row = session.execute(
        text(
            f"""
            UPDATE generic_instance gi
            SET json_addl = jsonb_set(
                    COALESCE(gi.json_addl, '{{}}'::jsonb),
                    '{{role}}',
                    to_jsonb(CAST(:role AS text)),
                    TRUE
                ),
                modified_dt = NOW()
            WHERE {_SYSTEM_USER_WHERE}
              AND lower(COALESCE(gi.json_addl->>'login_identifier', '')) = :identifier
            RETURNING gi.uid
            """
        ),
        {"identifier": normalized_login, "role": selected_role},
    ).fetchone()
    return row is not None


def set_active(session: Session, login_identifier: str, is_active: bool) -> bool:
    normalized_login = normalize_login_identifier(login_identifier)
    row = session.execute(
        text(
            f"""
            UPDATE generic_instance gi
            SET json_addl = jsonb_set(
                    COALESCE(gi.json_addl, '{{}}'::jsonb),
                    '{{is_active}}',
                    to_jsonb(CAST(:is_active AS boolean)),
                    TRUE
                ),
                modified_dt = NOW()
            WHERE {_SYSTEM_USER_WHERE}
              AND lower(COALESCE(gi.json_addl->>'login_identifier', '')) = :identifier
            RETURNING gi.uid
            """
        ),
        {"identifier": normalized_login, "is_active": bool(is_active)},
    ).fetchone()
    return row is not None


def set_password_hash(
    session: Session,
    login_identifier: str,
    password_hash: Optional[str],
    *,
    require_password_change: Optional[bool] = None,
) -> bool:
    normalized_login = normalize_login_identifier(login_identifier)
    updates = """
        jsonb_set(
            COALESCE(gi.json_addl, '{}'::jsonb),
            '{password_hash}',
            to_jsonb(CAST(:password_hash AS text)),
            TRUE
        )
    """
    params: dict[str, Any] = {
        "identifier": normalized_login,
        "password_hash": _norm_optional(password_hash),
    }
    if require_password_change is not None:
        updates = f"""
            jsonb_set(
                {updates},
                '{{require_password_change}}',
                to_jsonb(CAST(:require_password_change AS boolean)),
                TRUE
            )
        """
        params["require_password_change"] = bool(require_password_change)

    row = session.execute(
        text(
            f"""
            UPDATE generic_instance gi
            SET json_addl = {updates},
                modified_dt = NOW()
            WHERE {_SYSTEM_USER_WHERE}
              AND lower(COALESCE(gi.json_addl->>'login_identifier', '')) = :identifier
            RETURNING gi.uid
            """
        ),
        params,
    ).fetchone()
    return row is not None


def soft_delete(session: Session, login_identifier: str) -> bool:
    normalized_login = normalize_login_identifier(login_identifier)
    row = session.execute(
        text(
            f"""
            UPDATE generic_instance gi
            SET is_deleted = TRUE,
                modified_dt = NOW()
            WHERE {_SYSTEM_USER_WHERE}
              AND lower(COALESCE(gi.json_addl->>'login_identifier', '')) = :identifier
            RETURNING gi.uid
            """
        ),
        {"identifier": normalized_login},
    ).fetchone()
    return row is not None
