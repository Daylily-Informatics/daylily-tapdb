"""TapDB EUID facade over the Meridian EUID reference implementation."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Dict, Mapping, Optional

from meridian_euid import EUIDError, encode, parse

from daylily_tapdb.governance import normalize_owner_repo_name

MERIDIAN_DOMAIN_CODE_ENV = "MERIDIAN_DOMAIN_CODE"
TAPDB_OWNER_REPO_ENV = "TAPDB_OWNER_REPO"

GENERIC_TEMPLATE_PREFIX = "TPX"
GENERIC_INSTANCE_LINEAGE_PREFIX = "EDG"
AUDIT_LOG_PREFIX = "ADT"
SYSTEM_USER_PREFIX = "SYS"
SYSTEM_MESSAGE_PREFIX = "MSG"
EXTERNAL_REFERENCE_PREFIX = "XRF"
GOVERNANCE_INSTANCE_PREFIX = "GVR"
GENERIC_SET_PREFIX = "GSE"

_TOKEN_RE = re.compile(r"^[0-9A-HJ-KMNP-TV-Z]{1,4}$")


def _normalize_token(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    if not normalized:
        return None
    if not _TOKEN_RE.fullmatch(normalized):
        raise ValueError(f"Invalid Meridian {field_name}: {value!r}")
    return normalized


def normalize_domain_code(code: str | None) -> str | None:
    """Normalize and validate a Meridian domain code."""
    return _normalize_token(code, field_name="domain code")


def normalize_prefix(prefix: str | None) -> str | None:
    """Normalize and validate a Meridian prefix token."""
    return _normalize_token(prefix, field_name="prefix")


def resolve_runtime_domain_code(
    environ: Mapping[str, str] | None = None,
) -> str:
    """Resolve the runtime domain code from the environment."""
    env = os.environ if environ is None else environ
    raw_code = env.get(MERIDIAN_DOMAIN_CODE_ENV)
    if raw_code is None:
        raise ValueError(
            "MERIDIAN_DOMAIN_CODE is required. Set it to a valid Meridian domain code."
        )
    normalized = normalize_domain_code(raw_code)
    if normalized is None:
        raise ValueError(
            "MERIDIAN_DOMAIN_CODE is set to empty string. A valid domain code is required."
        )
    return normalized


def resolve_runtime_owner_repo_name(
    environ: Mapping[str, str] | None = None,
) -> str:
    """Resolve the runtime repo ownership token from the environment."""
    env = os.environ if environ is None else environ
    raw_value = env.get(TAPDB_OWNER_REPO_ENV)
    if raw_value is None:
        raise ValueError(
            "TAPDB_OWNER_REPO is required. Set it to the repo-name that owns this TapDB runtime."
        )
    normalized = str(raw_value).strip()
    if not normalized:
        raise ValueError(
            "TAPDB_OWNER_REPO is set to empty string. A repo-name is required."
        )
    return normalize_owner_repo_name(normalized)


def resolve_runtime_validation_context(
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Return the canonical TapDB EUID validation context."""
    domain_code = resolve_runtime_domain_code(environ)
    return {
        "environment": "canonical",
        "allowed_domain_codes": [domain_code],
    }


def format_euid(
    prefix: str,
    seq_val: int,
    *,
    domain_code: str | None = None,
) -> str:
    """Format a native allocation as a three-hyphen Meridian v3 EUID."""
    normalized_domain_code = normalize_domain_code(domain_code)
    if normalized_domain_code is None:
        raise ValueError("domain_code is required for canonical Meridian EUIDs")
    normalized_prefix = normalize_prefix(prefix)
    if normalized_prefix is None:
        raise ValueError("prefix is required for canonical Meridian EUIDs")
    return encode(seq_val, normalized_prefix, domain_code=normalized_domain_code)


def validate_euid(
    euid: str,
    *,
    environment: str = "canonical",
    allowed_domain_codes: list[str] | None = None,
) -> bool:
    """Return True when *euid* is a valid canonical Meridian EUID."""
    if environment not in {"canonical", "production", "domain"}:
        raise ValueError(f"Unsupported EUID validation environment: {environment!r}")
    try:
        parsed = parse(euid)
    except EUIDError:
        return False
    domain_code = parsed["domain_code"]
    if allowed_domain_codes:
        allowed = {
            normalized
            for code in allowed_domain_codes
            if (normalized := normalize_domain_code(str(code).strip())) is not None
        }
        return domain_code in allowed
    return True


_CANONICAL_CORE_PREFIXES = MappingProxyType(
    {
        "generic_template": GENERIC_TEMPLATE_PREFIX,
        "generic_instance_lineage": GENERIC_INSTANCE_LINEAGE_PREFIX,
        "audit_log": AUDIT_LOG_PREFIX,
        "system_user_instance": SYSTEM_USER_PREFIX,
        "system_message_instance": SYSTEM_MESSAGE_PREFIX,
        "external_reference_instance": EXTERNAL_REFERENCE_PREFIX,
        "governance_instance": GOVERNANCE_INSTANCE_PREFIX,
        "generic_set_instance": GENERIC_SET_PREFIX,
    }
)


@dataclass(frozen=True)
class EUIDConfig:
    """Read-only canonical prefix catalog, not a runtime type discriminator.

    Explicit issuance profiles may project several canonical roles to one
    prefix. Resolve object types through persisted templates/mapped classes.
    """

    CORE_PREFIXES: Mapping[str, str] = field(
        default_factory=lambda: _CANONICAL_CORE_PREFIXES
    )

    def get_all_prefixes(self) -> Dict[str, str]:
        return dict(self.CORE_PREFIXES)

    def get_discriminator_for_prefix(self, prefix: str) -> Optional[str]:
        normalized_prefix = normalize_prefix(prefix)
        if normalized_prefix is None:
            return None
        matches = [
            discriminator
            for discriminator, owned_prefix in self.CORE_PREFIXES.items()
            if owned_prefix == normalized_prefix
        ]
        if len(matches) == 1:
            return matches[0]
        return None

    def is_canonical_prefix(self, prefix: str) -> bool:
        normalized_prefix = normalize_prefix(prefix)
        if normalized_prefix is None:
            return False
        return normalized_prefix in self.CORE_PREFIXES.values()
