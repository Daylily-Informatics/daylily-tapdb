"""TapDB governance helpers backed by Meridian EUID registries."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from meridian_euid import (
    MERIDIAN_REGISTRY_INDEX_URL,
    MERIDIAN_REGISTRY_REPOSITORY,
    MERIDIAN_REGISTRY_VERSION,
)
from meridian_euid import assert_registered_domain as meridian_assert_registered_domain
from meridian_euid import load_domain_registry as meridian_load_domain_registry
from meridian_euid import (
    load_domain_registry_metadata as meridian_load_domain_registry_metadata,
)
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


@dataclass(frozen=True)
class GovernanceAuthorization:
    """Explicit caller authorization; Meridian remains the policy authority.

    These values must come from the caller's canonical configuration, never
    from the registry owner, runtime identity, or TapDB safety tier.
    """

    reserved_for: str | None = None
    deployment_environment: str | None = None
    approval_tokens: tuple[str, ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        for name in ("reserved_for", "deployment_environment"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(
                    f"governance_authorization.{name} must be a nonempty string"
                )
        if not isinstance(self.approval_tokens, (list, tuple)) or any(
            not isinstance(token, str) or not token.strip()
            for token in self.approval_tokens
        ):
            raise ValueError(
                "governance_authorization.approval_tokens must be an array of nonempty strings"
            )
        object.__setattr__(self, "approval_tokens", tuple(self.approval_tokens))

    @classmethod
    def from_value(
        cls, value: GovernanceAuthorization | Mapping[str, object] | None
    ) -> GovernanceAuthorization | None:
        if value is None or isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("governance_authorization must be an object")
        if set(value) - {"reserved_for", "deployment_environment", "approval_tokens"}:
            raise ValueError("governance_authorization contains unsupported fields")
        return cls(**value)

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {"approval_tokens": list(self.approval_tokens)}
        if self.reserved_for is not None:
            result["reserved_for"] = self.reserved_for
        if self.deployment_environment is not None:
            result["deployment_environment"] = self.deployment_environment
        return result


def load_domain_registry(path: str | Path) -> frozenset[str]:
    resolved = _resolved_path(path)
    return meridian_load_domain_registry(resolved)


def load_domain_registry_metadata(path: str | Path) -> dict[str, dict[str, object]]:
    resolved = _resolved_path(path)
    return meridian_load_domain_registry_metadata(resolved)


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
    registry_metadata: Mapping[str, Mapping[str, object]] | None = None,
    path: str | Path | None = None,
    governance_authorization: GovernanceAuthorization | Mapping[str, object] | None = None,
) -> str:
    authorization = GovernanceAuthorization.from_value(governance_authorization)
    authorization_kwargs = authorization.to_dict() if authorization is not None else {}
    normalized_domain_code = _validate_domain_code(domain_code)
    if registry is None and registry_metadata is None:
        resolved_path = _resolved_path(path)
    else:
        resolved_path = _resolved_path(path) if path is not None else None
    if registry is None:
        return meridian_assert_registered_domain(
            normalized_domain_code,
            registry_metadata=registry_metadata,
            path=resolved_path,
            **authorization_kwargs,
        )
    return meridian_assert_registered_domain(
        normalized_domain_code,
        registry=registry,
        registry_metadata=registry_metadata,
        path=resolved_path,
        **authorization_kwargs,
    )


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
    domain_registry_metadata: Mapping[str, Mapping[str, object]]
    prefix_ownership: Mapping[tuple[str, str], str]
    governance_authorization: GovernanceAuthorization | None = None
    public_domain_registry_repository: str = MERIDIAN_REGISTRY_REPOSITORY
    public_domain_registry_version: str = MERIDIAN_REGISTRY_VERSION
    public_domain_registry_index_url: str = MERIDIAN_REGISTRY_INDEX_URL

    @classmethod
    def load(
        cls,
        *,
        domain_code: str,
        owner_repo_name: str,
        domain_registry_path: str | Path,
        prefix_ownership_registry_path: str | Path,
        governance_authorization: GovernanceAuthorization | Mapping[str, object] | None = None,
    ) -> "GovernanceContext":
        authorization = GovernanceAuthorization.from_value(governance_authorization)
        resolved_domain_registry_path = _resolved_path(domain_registry_path)
        resolved_prefix_ownership_registry_path = _resolved_path(
            prefix_ownership_registry_path
        )
        validate_registries_consistent(
            domain_registry_path=resolved_domain_registry_path,
            prefix_ownership_registry_path=resolved_prefix_ownership_registry_path,
        )
        domain_registry_metadata = load_domain_registry_metadata(
            resolved_domain_registry_path
        )
        registered_domains = frozenset(domain_registry_metadata)
        prefix_ownership = load_prefix_ownership_registry(
            resolved_prefix_ownership_registry_path
        )
        normalized_domain_code = assert_registered_domain(
            domain_code,
            registry=registered_domains,
            registry_metadata=domain_registry_metadata,
            path=resolved_domain_registry_path,
            governance_authorization=authorization,
        )
        normalized_owner_repo_name = normalize_owner_repo_name(owner_repo_name)
        return cls(
            domain_code=normalized_domain_code,
            owner_repo_name=normalized_owner_repo_name,
            domain_registry_path=resolved_domain_registry_path,
            prefix_ownership_registry_path=resolved_prefix_ownership_registry_path,
            registered_domains=registered_domains,
            domain_registry_metadata=domain_registry_metadata,
            prefix_ownership=prefix_ownership,
            governance_authorization=authorization,
        )

    def require_prefix(self, prefix: str) -> str:
        return assert_prefix_owner_repo_name(
            self.domain_code,
            prefix,
            self.owner_repo_name,
            registry=self.prefix_ownership,
            path=self.prefix_ownership_registry_path,
        )
