"""TapDB governance helpers backed by Meridian EUID registries."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

try:
    from meridian_euid import (
        assert_registered_domain as meridian_assert_registered_domain,
    )
    from meridian_euid import load_domain_registry as meridian_load_domain_registry
    from meridian_euid import (
        load_prefix_ownership_registry as meridian_load_prefix_ownership_registry,
    )
    from meridian_euid import (
        validate_issuer_app_code as meridian_validate_issuer_app_code,
    )
    from meridian_euid import (
        validate_registries_consistent as meridian_validate_registries_consistent,
    )
except ImportError:  # pragma: no cover - compatibility with newer meridian_euid builds
    meridian_assert_registered_domain = None
    meridian_load_domain_registry = None
    meridian_load_prefix_ownership_registry = None
    meridian_validate_issuer_app_code = None
    meridian_validate_registries_consistent = None

DEFAULT_TAPDB_CONFIG_DIR = Path.home() / ".config" / "tapdb"
DEFAULT_DOMAIN_REGISTRY_PATH = DEFAULT_TAPDB_CONFIG_DIR / "domain_code_registry.json"
DEFAULT_PREFIX_OWNERSHIP_REGISTRY_PATH = (
    DEFAULT_TAPDB_CONFIG_DIR / "prefix_ownership_registry.json"
)


def _resolved_path(path: str | Path | None) -> Path:
    if path is None:
        raise ValueError("An explicit registry path is required")
    return Path(path).expanduser().resolve()


def _load_json_object(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Registry file must contain a JSON object: {path}")
    return payload


def _validate_owner_repo_name(owner_repo_name: str) -> str:
    normalized = str(owner_repo_name or "").strip()
    if not normalized:
        raise ValueError("owner_repo_name is required")
    return normalized


def _validate_domain_code(domain_code: str) -> str:
    normalized = str(domain_code or "").strip().upper()
    if not normalized or len(normalized) != 1 or not normalized.isalnum():
        raise ValueError(f"Invalid Meridian domain code: {domain_code!r}")
    return normalized


def _load_domain_registry_local(path: Path) -> frozenset[str]:
    payload = _load_json_object(path)
    domains = payload.get("domains")
    if not isinstance(domains, dict):
        raise ValueError(f"Domain registry must define an object 'domains': {path}")
    return frozenset(_validate_domain_code(str(code)) for code in domains)


def _load_prefix_ownership_registry_local(path: Path) -> dict[tuple[str, str], str]:
    payload = _load_json_object(path)
    ownership = payload.get("ownership")
    if not isinstance(ownership, dict):
        raise ValueError(f"Prefix registry must define an object 'ownership': {path}")
    registry: dict[tuple[str, str], str] = {}
    for domain_code, claims in ownership.items():
        normalized_domain_code = _validate_domain_code(str(domain_code))
        if not isinstance(claims, dict):
            raise ValueError(
                f"Prefix registry claims for domain {normalized_domain_code!r} must be an object: "
                f"{path}"
            )
        for prefix, claim in claims.items():
            normalized_prefix = str(prefix or "").strip().upper()
            if not normalized_prefix:
                raise ValueError(
                    f"Prefix registry contains an empty prefix for domain {normalized_domain_code!r}: "
                    f"{path}"
                )
            if not isinstance(claim, dict):
                raise ValueError(
                    f"Prefix {normalized_prefix!r} claim for domain {normalized_domain_code!r} "
                    f"must be an object: {path}"
                )
            owner = str(
                claim.get("issuer_app_code")
                or claim.get("owner_repo_name")
                or claim.get("repo_name")
                or ""
            ).strip()
            if not owner:
                raise ValueError(
                    f"Prefix {normalized_prefix!r} for domain {normalized_domain_code!r} "
                    f"is missing an owner claim: {path}"
                )
            registry[(normalized_domain_code, normalized_prefix)] = _validate_owner_repo_name(owner)
    return registry


def _validate_registries_consistent_local(
    *,
    domain_registry_path: Path,
    prefix_ownership_registry_path: Path,
) -> None:
    registered_domains = _load_domain_registry_local(domain_registry_path)
    prefix_ownership = _load_prefix_ownership_registry_local(prefix_ownership_registry_path)
    for domain_code, prefix in prefix_ownership:
        if domain_code not in registered_domains:
            raise ValueError(
                f"Prefix registry references unregistered domain {domain_code!r}: "
                f"{prefix_ownership_registry_path}"
            )


def normalize_owner_repo_name(owner_repo_name: str) -> str:
    """Validate the runtime repo-name token used for prefix ownership."""
    if meridian_validate_issuer_app_code is not None:
        return meridian_validate_issuer_app_code(owner_repo_name)
    return _validate_owner_repo_name(owner_repo_name)


def load_domain_registry(path: str | Path) -> frozenset[str]:
    resolved = _resolved_path(path)
    if meridian_load_domain_registry is not None:
        return meridian_load_domain_registry(resolved)
    return _load_domain_registry_local(resolved)


def load_prefix_ownership_registry(
    path: str | Path,
) -> dict[tuple[str, str], str]:
    resolved = _resolved_path(path)
    if meridian_load_prefix_ownership_registry is not None:
        return meridian_load_prefix_ownership_registry(resolved)
    return _load_prefix_ownership_registry_local(resolved)


def validate_registries_consistent(
    *,
    domain_registry_path: str | Path,
    prefix_ownership_registry_path: str | Path,
) -> None:
    resolved_domain_registry_path = _resolved_path(domain_registry_path)
    resolved_prefix_ownership_registry_path = _resolved_path(prefix_ownership_registry_path)
    if meridian_validate_registries_consistent is not None:
        meridian_validate_registries_consistent(
            domain_registry_path=resolved_domain_registry_path,
            prefix_ownership_registry_path=resolved_prefix_ownership_registry_path,
        )
        return
    _validate_registries_consistent_local(
        domain_registry_path=resolved_domain_registry_path,
        prefix_ownership_registry_path=resolved_prefix_ownership_registry_path,
    )


def assert_registered_domain(
    domain_code: str,
    *,
    registry: frozenset[str] | None = None,
    path: str | Path | None = None,
) -> str:
    normalized_domain_code = _validate_domain_code(domain_code)
    if registry is None:
        if meridian_assert_registered_domain is not None:
            return meridian_assert_registered_domain(
                normalized_domain_code,
                path=_resolved_path(path),
            )
        loaded_registry = load_domain_registry(path)
        if normalized_domain_code not in loaded_registry:
            raise ValueError(f"Domain {normalized_domain_code!r} is not registered")
        return normalized_domain_code
    if meridian_assert_registered_domain is not None:
        return meridian_assert_registered_domain(normalized_domain_code, registry=registry)
    if normalized_domain_code not in registry:
        raise ValueError(f"Domain {normalized_domain_code!r} is not registered")
    return normalized_domain_code


def resolve_prefix_owner_repo_name(
    domain_code: str,
    prefix: str,
    *,
    registry: Mapping[tuple[str, str], str] | None = None,
    path: str | Path | None = None,
) -> str:
    normalized_domain_code = _validate_domain_code(domain_code)
    normalized_prefix = str(prefix or "").strip().upper()
    ownership = registry if registry is not None else load_prefix_ownership_registry(path)
    try:
        return ownership[(normalized_domain_code, normalized_prefix)]
    except KeyError as exc:
        raise ValueError(
            f"prefix {normalized_prefix!r} is not registered in domain {normalized_domain_code!r}"
        ) from exc


def assert_prefix_owner_repo_name(
    domain_code: str,
    prefix: str,
    owner_repo_name: str,
    *,
    registry: Mapping[tuple[str, str], str] | None = None,
    path: str | Path | None = None,
) -> str:
    normalized_owner_repo_name = normalize_owner_repo_name(owner_repo_name)
    actual = resolve_prefix_owner_repo_name(
        domain_code,
        prefix,
        registry=registry,
        path=path,
    )
    if actual != normalized_owner_repo_name:
        raise ValueError(
            f"prefix {prefix!r} in domain {domain_code!r} is owned by "
            f"{actual!r}, not {normalized_owner_repo_name!r}"
        )
    return actual


@dataclass(frozen=True)
class GovernanceContext:
    """Loaded registry state for a single TapDB runtime context."""

    domain_code: str
    owner_repo_name: str
    domain_registry_path: Path
    prefix_ownership_registry_path: Path
    registered_domains: frozenset[str]
    prefix_ownership: Mapping[tuple[str, str], str]

    @classmethod
    def load(
        cls,
        *,
        domain_code: str,
        owner_repo_name: str,
        domain_registry_path: str | Path,
        prefix_ownership_registry_path: str | Path,
    ) -> "GovernanceContext":
        resolved_domain_registry_path = _resolved_path(domain_registry_path)
        resolved_prefix_ownership_registry_path = _resolved_path(
            prefix_ownership_registry_path
        )
        validate_registries_consistent(
            domain_registry_path=resolved_domain_registry_path,
            prefix_ownership_registry_path=resolved_prefix_ownership_registry_path,
        )
        registered_domains = load_domain_registry(resolved_domain_registry_path)
        prefix_ownership = load_prefix_ownership_registry(
            resolved_prefix_ownership_registry_path
        )
        normalized_domain_code = assert_registered_domain(
            domain_code,
            registry=registered_domains,
            path=resolved_domain_registry_path,
        )
        normalized_owner_repo_name = normalize_owner_repo_name(owner_repo_name)
        return cls(
            domain_code=normalized_domain_code,
            owner_repo_name=normalized_owner_repo_name,
            domain_registry_path=resolved_domain_registry_path,
            prefix_ownership_registry_path=resolved_prefix_ownership_registry_path,
            registered_domains=registered_domains,
            prefix_ownership=prefix_ownership,
        )

    def require_prefix(self, prefix: str) -> str:
        return assert_prefix_owner_repo_name(
            self.domain_code,
            prefix,
            self.owner_repo_name,
            registry=self.prefix_ownership,
            path=self.prefix_ownership_registry_path,
        )
