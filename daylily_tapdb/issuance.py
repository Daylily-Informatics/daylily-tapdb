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
    mode: Literal["canonical", "single_prefix"]
    prefix: str | None = None
    template_prefix: str | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("issuance_profile.version must be 1")
        if not isinstance(self.mode, str) or self.mode not in {"canonical", "single_prefix"}:
            raise ValueError("issuance_profile.mode must be canonical or single_prefix")
        if self.mode == "canonical":
            if self.prefix is not None:
                raise ValueError("canonical issuance_profile must not specify prefix")
            if self.template_prefix is not None and normalize_prefix(self.template_prefix) != self.template_prefix:
                raise ValueError("template_prefix requires an exact Meridian prefix")
        elif not isinstance(self.prefix, str) or normalize_prefix(self.prefix) != self.prefix:
            raise ValueError("single_prefix issuance_profile requires an exact Meridian prefix")
        if self.mode == "single_prefix" and self.template_prefix is not None:
            raise ValueError("template_prefix is available only with canonical issuance")

    @classmethod
    def from_mapping(cls, value: Any) -> IssuanceProfile:
        if not isinstance(value, Mapping):
            raise ValueError("Explicit meta.issuance_profile mapping is required")
        required = {"version", "mode"}
        if value.get("mode") == "single_prefix":
            required.add("prefix")
        if "template_prefix" in value:
            required.add("template_prefix")
        if set(value) != required:
            raise ValueError(f"issuance_profile must contain exactly {sorted(required)}")
        return cls(**dict(value))

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"version": self.version, "mode": self.mode}
        if self.mode == "single_prefix":
            result["prefix"] = self.prefix
        if self.template_prefix is not None:
            result["template_prefix"] = self.template_prefix
        return result

    def instance_prefix(self, declared_prefix: str) -> str:
        """Project an authenticated canonical definition's allocation prefix."""
        normalized = normalize_prefix(declared_prefix)
        if normalized is None:
            raise ValueError("A declared instance prefix is required")
        return cast(str, self.prefix) if self.mode == "single_prefix" else normalized

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
        if entity == "generic_template" and self.template_prefix is not None:
            return self.template_prefix
        try:
            canonical = EUIDConfig().CORE_PREFIXES[entity]
        except KeyError as exc:
            raise ValueError(f"Unknown native identity entity: {entity!r}") from exc
        return self.instance_prefix(canonical)

    def identity_prefixes(self) -> dict[str, str]:
        return {name: self.identity_prefix(name) for name in EUIDConfig().CORE_PREFIXES}

    def core_prefix_owner(self, owner_repo_name: str) -> str:
        return owner_repo_name if self.mode == "single_prefix" else "daylily-tapdb"

    def identity_prefix_owner(self, entity: str, owner_repo_name: str) -> str:
        if entity == "generic_template" and self.template_prefix is not None:
            return owner_repo_name
        return self.core_prefix_owner(owner_repo_name)

    def project_core_template(self, canonical: Mapping[str, Any]) -> dict[str, Any]:
        """Project an already authenticated bundled definition, preserving type."""
        result = deepcopy(dict(canonical))
        result["instance_prefix"] = self.instance_prefix(result["instance_prefix"])
        return result


def profile_from_config(cfg: Mapping[str, Any]) -> IssuanceProfile:
    """Read a normalized native DB config; omitted profiles are never inferred."""
    return IssuanceProfile.from_mapping(cfg.get("issuance_profile"))


def issuance_profile_binding_sql(cfg: Mapping[str, Any], *, adopt_template_prefix: bool = False) -> str:
    """Native binding, with explicit future-template adoption from canonical issuance.

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
    adoption = ""
    if adopt_template_prefix:
        if profile.mode != "canonical" or profile.template_prefix is None:
            raise ValueError("Template-prefix adoption requires an explicit canonical template_prefix")
        adoption = f"""
        UPDATE {table} SET template_prefix = {template_prefix}
        WHERE domain_code = {domain} AND issuer_app_code = {owner}
          AND version = 1 AND mode = 'canonical' AND prefix IS NULL
          AND template_prefix IS NULL;
        """
    return f"""
    {adoption}
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


def claim_configured_template_prefix(cfg: Mapping[str, Any]) -> None:
    """Register the explicit template prefix without changing any other claim."""
    import fcntl
    import json
    import os
    from pathlib import Path
    from tempfile import NamedTemporaryFile

    from daylily_tapdb.governance import (
        assert_registered_domain, load_prefix_ownership_registry, normalize_owner_repo_name,
    )

    profile = profile_from_config(cfg)
    if profile.mode != "canonical" or not profile.template_prefix:
        raise ValueError("An explicit canonical template_prefix is required for registration")
    domain = assert_registered_domain(str(cfg["domain_code"]),
        path=str(cfg["domain_registry_path"]), governance_authorization=cfg.get("governance_authorization"))
    owner = normalize_owner_repo_name(str(cfg["owner_repo_name"]))
    path = Path(cfg["prefix_ownership_registry_path"])
    if not path.is_absolute():
        raise ValueError("An absolute prefix ownership registry path is required")
    with Path(str(path)+".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        registered = load_prefix_ownership_registry(path).get((domain, profile.template_prefix))
        if registered is not None:
            if registered != owner:
                raise ValueError("Configured template prefix is already owned by another repository")
            return
        value = json.loads(path.read_text())
        claims = value["ownership"][domain]
        if not isinstance(claims, dict):
            raise ValueError("The selected domain requires an explicit ownership mapping")
        claims[profile.template_prefix] = {"issuer_app_code": owner}
        attributes = path.stat()
        with NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as output:
            temporary = Path(output.name)
            json.dump(value, output, indent=2)
            output.write("\n")
            os.chmod(temporary, attributes.st_mode & 0o777)
            os.chown(temporary, attributes.st_uid, attributes.st_gid)
        temporary.replace(path)


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
