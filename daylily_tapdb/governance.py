"""TapDB governance helpers backed by Meridian EUID registries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from meridian_euid import assert_registered_domain as meridian_assert_registered_domain
from meridian_euid import load_domain_registry as meridian_load_domain_registry
from meridian_euid import (
    load_prefix_ownership_registry as meridian_load_prefix_ownership_registry,
)
from meridian_euid import validate_issuer_app_code as meridian_validate_issuer_app_code
from meridian_euid import (
    validate_registries_consistent as meridian_validate_registries_consistent,
)


def _resolved_path(path: str | Path | None) -> Path:
    if path is None:
        raise ValueError("An explicit registry path is required")
    return Path(path).expanduser().resolve()


def _validate_domain_code(domain_code: str) -> str:
    normalized = str(domain_code or "").strip().upper()
    if not normalized or len(normalized) != 1 or not normalized.isalnum():
        raise ValueError(f"Invalid Meridian domain code: {domain_code!r}")
    return normalized


def normalize_owner_repo_name(owner_repo_name: str) -> str:
    """Validate the runtime repo-name token used for prefix ownership."""
    return meridian_validate_issuer_app_code(owner_repo_name)


def load_domain_registry(path: str | Path) -> frozenset[str]:
    resolved = _resolved_path(path)
    return meridian_load_domain_registry(resolved)


def load_prefix_ownership_registry(
    path: str | Path,
) -> dict[tuple[str, str], str]:
    resolved = _resolved_path(path)
    return meridian_load_prefix_ownership_registry(resolved)


def validate_registries_consistent(
    *,
    domain_registry_path: str | Path,
    prefix_ownership_registry_path: str | Path,
) -> None:
    resolved_domain_registry_path = _resolved_path(domain_registry_path)
    resolved_prefix_ownership_registry_path = _resolved_path(
        prefix_ownership_registry_path
    )
    meridian_validate_registries_consistent(
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
        return meridian_assert_registered_domain(
            normalized_domain_code,
            path=_resolved_path(path),
        )
    return meridian_assert_registered_domain(normalized_domain_code, registry=registry)


def resolve_prefix_owner_repo_name(
    domain_code: str,
    prefix: str,
    *,
    registry: Mapping[tuple[str, str], str] | None = None,
    path: str | Path | None = None,
) -> str:
    normalized_domain_code = _validate_domain_code(domain_code)
    normalized_prefix = str(prefix or "").strip().upper()
    ownership = (
        registry if registry is not None else load_prefix_ownership_registry(path)
    )
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
