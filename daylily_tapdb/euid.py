"""
TAPDB EUID helpers.

This module provides Meridian-format EUID validation/formatting utilities plus
the canonical TapDB-managed prefix catalog used by the shared substrate.

Application-specific issuing authority and business behavior do not live here.
Calling code must treat EUIDs as opaque identifiers.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Dict, Mapping, Optional

# ---------------------------------------------------------------------------
# Meridian Crockford Base32 utilities
# ---------------------------------------------------------------------------

CROCKFORD_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_CROCKFORD_VALUE: dict[str, int] = {ch: i for i, ch in enumerate(CROCKFORD_ALPHABET)}

# Canonical regex character classes from SPEC.md §6.4
_ALNUM32 = r"[0-9A-HJ-KMNP-TV-Z]"
_LETTERS32 = r"[A-HJ-KMNP-TV-Z]"

# Production: CATEGORY(2-3 letters) - BODY(no leading 0) CHECKSUM
_PRODUCTION_RE = re.compile(
    rf"^{_LETTERS32}{{2,3}}-([1-9A-HJ-KMNP-TV-Z]{_ALNUM32}*){_ALNUM32}$"
)
# Domain-scoped: DOMAIN_CODE(1-4 letters) : CATEGORY - BODY CHECKSUM
_DOMAIN_RE = re.compile(
    rf"^{_LETTERS32}{{1,4}}:{_LETTERS32}{{2,3}}-([1-9A-HJ-KMNP-TV-Z]{_ALNUM32}*){_ALNUM32}$"
)
# Backward compat alias
_SANDBOX_RE = _DOMAIN_RE

DEFAULT_DOMAIN_CODE = "T"
DEFAULT_SANDBOX_PREFIX = DEFAULT_DOMAIN_CODE  # backward compat
MERIDIAN_DOMAIN_CODE_ENV = "MERIDIAN_DOMAIN_CODE"
MERIDIAN_SANDBOX_PREFIX_ENV = MERIDIAN_DOMAIN_CODE_ENV  # backward compat
MERIDIAN_ENVIRONMENT_ENV = "MERIDIAN_ENVIRONMENT"
LSMC_ENV_ENV = "LSMC_ENV"
CORE_TEMPLATE_PLACEHOLDER_PREFIX = "GX"
_EUID_CLIENT_CODE_RE = re.compile(rf"^{_LETTERS32}{{1,4}}$")


def normalize_domain_code(code: str | None) -> str | None:
    """Normalize and validate a domain code (1-4 Crockford Base32 letters)."""
    if code is None:
        return None
    normalized = code.strip().upper()
    if not normalized:
        return None
    if not re.fullmatch(r"[A-HJ-KMNP-TV-Z]{1,4}", normalized):
        raise ValueError(
            f"Invalid domain code {code!r}; expected 1-4 Crockford Base32 letters"
        )
    return normalized


# Backward compat alias
normalize_sandbox_prefix = normalize_domain_code


def resolve_runtime_domain_code(
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Resolve the runtime domain code.

    Rules:
    - missing env var => default ``T``
    - explicit empty string => disable domain code prefixing
    - explicit non-empty value => validated 1-4 letter domain code
    """
    env = environ or os.environ
    raw_code = env.get(MERIDIAN_DOMAIN_CODE_ENV)
    if raw_code is None:
        return DEFAULT_DOMAIN_CODE
    return normalize_domain_code(raw_code)


# Backward compat alias
resolve_runtime_sandbox_prefix = resolve_runtime_domain_code


def resolve_runtime_validation_context(
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Resolve EUID validation mode from runtime env vars."""
    env = environ or os.environ
    raw_environment = (
        str(env.get(MERIDIAN_ENVIRONMENT_ENV) or env.get(LSMC_ENV_ENV) or "")
        .strip()
        .lower()
    )
    domain_code = resolve_runtime_domain_code(env)

    if raw_environment in ("sandbox", "domain"):
        return {
            "environment": "domain",
            "allowed_domain_codes": [domain_code] if domain_code else [],
        }
    if raw_environment:
        return {"environment": "production"}
    if domain_code:
        return {
            "environment": "domain",
            "allowed_domain_codes": [domain_code],
        }
    return {"environment": "production"}


def normalize_euid_client_code(code: str | None) -> str:
    """Normalize and validate a 1-4 letter client code for TapDB core prefixes."""
    normalized = str(code or "").strip().upper()
    if not normalized:
        raise ValueError("euid_client_code is required")
    if not _EUID_CLIENT_CODE_RE.fullmatch(normalized):
        raise ValueError(
            "euid_client_code must be 1-4 Meridian-safe letters (A-Z, excluding I/L/O/U)"
        )
    if normalized == DEFAULT_DOMAIN_CODE:
        raise ValueError(
            f"euid_client_code {normalized!r} is reserved for TapDB domain/runtime use"
        )
    return normalized


def resolve_client_scoped_core_prefix(code: str | None) -> str:
    """Return the namespace-scoped concrete TapDB core prefix for a client code."""
    return f"{normalize_euid_client_code(code)}{CORE_TEMPLATE_PLACEHOLDER_PREFIX}"


def crockford_base32_encode(n: int) -> str:
    """Encode a positive integer to Crockford Base32 (unpadded, no leading zeros).

    Raises ValueError if *n* < 1.
    """
    if n < 1:
        raise ValueError(f"EUID body must be a positive integer, got {n}")
    result: list[str] = []
    while n > 0:
        n, remainder = divmod(n, 32)
        result.append(CROCKFORD_ALPHABET[remainder])
    return "".join(reversed(result))


def meridian_checksum(payload: str) -> str:
    """Compute the Meridian Luhn-style MOD 32 check character.

    *payload* is CATEGORY + BODY (no delimiters). For sandbox EUIDs,
    include the sandbox prefix character as well.

    Implements SPEC.md §7.5 exactly.
    """
    if not payload:
        raise ValueError("payload must be non-empty")
    if not payload.isascii():
        raise ValueError("payload must be ASCII")

    s = 0
    factor = 2
    for ch in reversed(payload):
        v = _CROCKFORD_VALUE.get(ch)
        if v is None:
            raise ValueError(f"invalid character in payload: {ch!r}")
        p = v * factor
        s += (p // 32) + (p % 32)
        factor = 1 if factor == 2 else 2

    check_value = (32 - (s % 32)) % 32
    return CROCKFORD_ALPHABET[check_value]


def format_euid(
    prefix: str,
    seq_val: int,
    *,
    domain_code: str | None = None,
    sandbox: str | None = None,  # backward compat alias
) -> str:
    """Build a Meridian-conformant EUID string.

    Args:
        prefix: Category prefix (e.g. "TX", "AGX"). 2-3 uppercase Crockford letters.
        seq_val: Positive integer from the sequence.
        domain_code: Optional 1-4 letter domain code prefix.
        sandbox: Deprecated alias for domain_code.

    Returns:
        Formatted EUID, e.g. ``TX-1C`` or ``T:TX-1C``.
    """
    dc = domain_code or sandbox
    body = crockford_base32_encode(seq_val)
    if dc:
        payload = dc + prefix + body
    else:
        payload = prefix + body
    check = meridian_checksum(payload)
    if dc:
        return f"{dc}:{prefix}-{body}{check}"
    return f"{prefix}-{body}{check}"


def validate_euid(
    euid: str,
    *,
    environment: str = "production",
    allowed_domain_codes: list[str] | None = None,
    allowed_sandbox_prefixes: list[str] | None = None,  # backward compat alias
) -> bool:
    """Validate an EUID string against Meridian spec.

    Returns True if the EUID is syntactically valid and checksum-correct
    for the given environment.
    """
    allowed = allowed_domain_codes or allowed_sandbox_prefixes
    # Accept "sandbox" as alias for "domain"
    env = "domain" if environment == "sandbox" else environment

    # §8.1 — reject non-ASCII, whitespace, lowercase
    if not euid.isascii():
        return False
    if any(c.isspace() for c in euid):
        return False
    if any(c.islower() for c in euid):
        return False

    has_colon = ":" in euid
    if has_colon:
        # Domain-scoped EUID
        if env == "production":
            return False
        if not _DOMAIN_RE.match(euid):
            return False
        # Extract domain code (everything before ':')
        dc = euid.split(":")[0]
        if allowed is not None and dc not in allowed:
            return False
        # Checksum payload = domain_code + category + body (no delimiters)
        stripped = euid.replace(":", "").replace("-", "")
        payload = stripped[:-1]
        presented_check = stripped[-1]
    else:
        # Production EUID
        if env == "domain":
            return False
        if not _PRODUCTION_RE.match(euid):
            return False
        stripped = euid.replace("-", "")
        payload = stripped[:-1]
        presented_check = stripped[-1]

    return meridian_checksum(payload) == presented_check


_CANONICAL_CORE_PREFIXES = MappingProxyType(
    {
        "generic_template": CORE_TEMPLATE_PLACEHOLDER_PREFIX,
        "generic_instance": CORE_TEMPLATE_PLACEHOLDER_PREFIX,
        "generic_instance_lineage": CORE_TEMPLATE_PLACEHOLDER_PREFIX,
    }
)

_CANONICAL_OPTIONAL_PREFIXES = MappingProxyType(
    {
        "workflow_instance": "WX",
        "workflow_step_instance": "WSX",
        "action_instance": "XX",
    }
)


@dataclass(frozen=True)
class EUIDConfig:
    """
    Read-only catalog of TapDB-managed prefixes.

    This shared module intentionally exposes only the canonical substrate
    prefixes. Application-specific issuing authorities are governed outside of
    TapDB and must not be added here through mutable registration APIs.
    """

    CORE_PREFIXES: Mapping[str, str] = field(
        default_factory=lambda: _CANONICAL_CORE_PREFIXES
    )
    OPTIONAL_PREFIXES: Mapping[str, str] = field(
        default_factory=lambda: _CANONICAL_OPTIONAL_PREFIXES
    )

    def get_all_prefixes(self) -> Dict[str, str]:
        """Return a copy of canonical discriminator-to-prefix mappings."""
        result = dict(self.CORE_PREFIXES)
        result.update(self.OPTIONAL_PREFIXES)
        return result

    def get_discriminator_for_prefix(self, prefix: str) -> Optional[str]:
        """Return the canonical discriminator for a uniquely owned prefix."""
        matches = [
            discriminator
            for discriminator, owned_prefix in self.get_all_prefixes().items()
            if owned_prefix == prefix
        ]
        if len(matches) == 1:
            return matches[0]
        return None

    def is_canonical_prefix(self, prefix: str) -> bool:
        """Return True when *prefix* is part of the TapDB-managed catalog."""
        return (
            prefix in self.CORE_PREFIXES.values()
            or prefix in self.OPTIONAL_PREFIXES.values()
        )
