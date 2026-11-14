"""Explicit native EUID issuance profiles, independent of object semantics.

Profiles select allocation prefixes. They never change template coordinates,
mapped classes, producer service identities, or persisted EUIDs. The database
owns allocation through its existing shared per-prefix sequences.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal, cast

from sqlalchemy import event
from sqlalchemy.orm import Session

from daylily_tapdb.euid import EUIDConfig, normalize_prefix

_PROFILE_INFO = "tapdb.issuance_profile"
_OWNER_INFO = "tapdb.issuance_owner"


@dataclass(frozen=True)
class IssuanceProfile:
    version: int
    mode: Literal["canonical", "single_prefix", "template_split"]
    prefix: str | None = None
    template_prefix: str | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("issuance_profile.version must be 1")
        if not isinstance(self.mode, str) or self.mode not in {"canonical", "single_prefix", "template_split"}:
            raise ValueError("issuance_profile.mode must be canonical, single_prefix, or template_split")
        if self.mode == "canonical":
            if self.prefix is not None or self.template_prefix is not None:
                raise ValueError("canonical issuance_profile must not specify allocation prefixes")
        elif not isinstance(self.prefix, str) or normalize_prefix(self.prefix) != self.prefix:
            raise ValueError("Noncanonical issuance_profile requires an exact Meridian prefix")
        if self.mode == "template_split":
            if (
                not isinstance(self.template_prefix, str)
                or normalize_prefix(self.template_prefix) != self.template_prefix
                or self.template_prefix == self.prefix
            ):
                raise ValueError("template_split requires a distinct exact template_prefix")
        elif self.template_prefix is not None:
            raise ValueError("Only template_split may specify template_prefix")

    @classmethod
    def from_mapping(cls, value: Any) -> IssuanceProfile:
        if not isinstance(value, Mapping):
            raise ValueError("Explicit meta.issuance_profile mapping is required")
        required = {"version", "mode"}
        if value.get("mode") in {"single_prefix", "template_split"}:
            required.add("prefix")
        if value.get("mode") == "template_split":
            required.add("template_prefix")
        if set(value) != required:
            raise ValueError(f"issuance_profile must contain exactly {sorted(required)}")
        return cls(**dict(value))

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"version": self.version, "mode": self.mode}
        if self.mode != "canonical":
            result["prefix"] = self.prefix
        if self.mode == "template_split":
            result["template_prefix"] = self.template_prefix
        return result

    def instance_prefix(self, declared_prefix: str) -> str:
        """Project an authenticated canonical definition's allocation prefix."""
        normalized = normalize_prefix(declared_prefix)
        if normalized is None:
            raise ValueError("A declared instance prefix is required")
        return normalized if self.mode == "canonical" else cast(str, self.prefix)

    def accepts_template_prefix(self, prefix: str) -> bool:
        """Whether a new application definition explicitly uses this profile."""
        try:
            normalized = normalize_prefix(prefix)
        except (TypeError, ValueError):
            return False
        return normalized is not None and normalized == prefix and (
            self.mode == "canonical" or normalized == self.prefix
        )

    def identity_prefix(self, entity: str) -> str:
        try:
            canonical = EUIDConfig().CORE_PREFIXES[entity]
        except KeyError as exc:
            raise ValueError(f"Unknown native identity entity: {entity!r}") from exc
        if entity == "generic_template" and self.mode == "template_split":
            return cast(str, self.template_prefix)
        return self.instance_prefix(canonical)

    def identity_prefixes(self) -> dict[str, str]:
        return {name: self.identity_prefix(name) for name in EUIDConfig().CORE_PREFIXES}

    def core_prefix_owner(self, owner_repo_name: str) -> str:
        return "daylily-tapdb" if self.mode == "canonical" else owner_repo_name

    def project_core_template(self, canonical: Mapping[str, Any]) -> dict[str, Any]:
        """Project an already authenticated bundled definition, preserving type."""
        result = deepcopy(dict(canonical))
        result["instance_prefix"] = self.instance_prefix(result["instance_prefix"])
        return result


def profile_from_config(cfg: Mapping[str, Any]) -> IssuanceProfile:
    """Read a normalized native DB config; omitted profiles are never inferred."""
    return IssuanceProfile.from_mapping(cfg.get("issuance_profile"))


def issuance_profile_binding_sql(cfg: Mapping[str, Any]) -> str:
    """Native operator bootstrap/adoption binding; never rewrite a prior profile.

    This control table has no EUID allocation. The caller owns the authenticated
    operator transaction and schema lifecycle.
    """
    from daylily_tapdb.identity_inventory import quote_identifier

    profile = profile_from_config(cfg)
    table = f"{quote_identifier(cfg['schema_name'])}.tapdb_issuance_profile"

    def literal(value: str | None) -> str:
        return "NULL" if value is None else "'" + value.replace("'", "''") + "'"

    domain, owner = literal(cfg["domain_code"]), literal(cfg["owner_repo_name"])
    mode, prefix = literal(profile.mode), literal(profile.prefix)
    template_prefix = literal(profile.template_prefix)
    return f"""
    DO $tapdb_issuance$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM {table}
        WHERE domain_code = {domain} AND issuer_app_code = {owner}
          AND (version <> {profile.version} OR mode <> {mode}
            OR prefix IS DISTINCT FROM {prefix}
            OR template_prefix IS DISTINCT FROM {template_prefix})
      ) THEN
        RAISE EXCEPTION 'Existing native issuance profile conflicts with explicit configuration';
      END IF;
    END;
    $tapdb_issuance$;
    INSERT INTO {table}(domain_code, issuer_app_code, version, mode, prefix, template_prefix)
    VALUES ({domain}, {owner}, {profile.version}, {mode}, {prefix}, {template_prefix})
    ON CONFLICT (domain_code, issuer_app_code) DO NOTHING;
    """


def install_engine_profile(engine: Any, profile: IssuanceProfile, *, owner_repo_name: str) -> None:
    """Attach the constructor's explicit context to every SQLAlchemy connection.

    Sessions created by a consuming application's own sessionmaker retain this
    context because they share the explicitly configured native engine.
    """
    if not isinstance(profile, IssuanceProfile) or not owner_repo_name:
        raise ValueError("An explicit issuance profile and owner are required")

    @event.listens_for(engine, "engine_connect")
    def _install(connection: Any) -> None:
        connection.info[_PROFILE_INFO] = profile
        connection.info[_OWNER_INFO] = owner_repo_name


def profile_for_session(session: Any) -> IssuanceProfile:
    if session is None:
        raise ValueError("An attached native session with an explicit issuance profile is required")
    connection = session.connection() if isinstance(session, Session) else session
    profile = connection.info.get(_PROFILE_INFO)
    if not isinstance(profile, IssuanceProfile):
        raise ValueError("Native connection has no explicit issuance profile")
    return profile


def owner_for_session(session: Any) -> str:
    connection = session.connection() if isinstance(session, Session) else session
    owner = connection.info.get(_OWNER_INFO)
    if not isinstance(owner, str) or not owner:
        raise ValueError("Native connection has no explicit issuance owner")
    return owner
