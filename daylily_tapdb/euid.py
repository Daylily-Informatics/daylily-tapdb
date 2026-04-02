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
_SANDBOX32 = r"[HJ-KMNP-TV-Z]"

# Production: CATEGORY(2-3 letters) - BODY(no leading 0) CHECKSUM
_PRODUCTION_RE = re.compile(
    rf"^{_LETTERS32}{{2,3}}-([1-9A-HJ-KMNP-TV-Z]{_ALNUM32}*){_ALNUM32}$"
)
# Sandbox: PREFIX : CATEGORY - BODY CHECKSUM
_SANDBOX_RE = re.compile(
    rf"^{_SANDBOX32}:{_LETTERS32}{{2,3}}-([1-9A-HJ-KMNP-TV-Z]{_ALNUM32}*){_ALNUM32}$"
)

DEFAULT_SANDBOX_PREFIX = "T"
MERIDIAN_SANDBOX_PREFIX_ENV = "MERIDIAN_SANDBOX_PREFIX"
MERIDIAN_ENVIRONMENT_ENV = "MERIDIAN_ENVIRONMENT"
LSMC_ENV_ENV = "LSMC_ENV"
CORE_TEMPLATE_PLACEHOLDER_PREFIX = "GX"
_EUID_CLIENT_CODE_RE = re.compile(rf"^{_LETTERS32}$")


def normalize_sandbox_prefix(prefix: str | None) -> str | None:
    """Normalize and validate a sandbox prefix."""
    if prefix is None:
        return None
    normalized = prefix.strip().upper()
    if not normalized:
        return None
    if not re.fullmatch(_SANDBOX32, normalized):
        raise ValueError(
            f"Invalid sandbox prefix {prefix!r}; expected a single Meridian sandbox letter"
        )
    return normalized


def resolve_runtime_sandbox_prefix(
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Resolve the runtime sandbox prefix.

    Rules:
    - missing env var => default ``T``
    - explicit empty string => disable sandbox prefixing
    - explicit non-empty value => validated single-letter prefix
    """
    env = environ or os.environ
    raw_prefix = env.get(MERIDIAN_SANDBOX_PREFIX_ENV)
    if raw_prefix is None:
        return DEFAULT_SANDBOX_PREFIX
    return normalize_sandbox_prefix(raw_prefix)


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
    sandbox_prefix = resolve_runtime_sandbox_prefix(env)

    if raw_environment == "sandbox":
        return {
            "environment": "sandbox",
            "allowed_sandbox_prefixes": [sandbox_prefix] if sandbox_prefix else [],
        }
    if raw_environment:
        return {"environment": "production"}
    if sandbox_prefix:
        return {
            "environment": "sandbox",
            "allowed_sandbox_prefixes": [sandbox_prefix],
        }
    return {"environment": "production"}


def normalize_euid_client_code(code: str | None) -> str:
    """Normalize and validate a one-letter client code for TapDB core prefixes."""
    normalized = str(code or "").strip().upper()
    if not normalized:
        raise ValueError("euid_client_code is required")
    if not _EUID_CLIENT_CODE_RE.fullmatch(normalized):
        raise ValueError(
            "euid_client_code must be a single Meridian-safe letter (A-Z, excluding I/L/O/U)"
        )
    if normalized == DEFAULT_SANDBOX_PREFIX:
        raise ValueError(
            f"euid_client_code {normalized!r} is reserved for TapDB sandbox/runtime use"
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


def format_euid(prefix: str, seq_val: int, *, sandbox: str | None = None) -> str:
    """Build a Meridian-conformant EUID string.

    Args:
        prefix: Category prefix (e.g. "TX", "AGX"). 2-3 uppercase Crockford letters.
        seq_val: Positive integer from the sequence.
        sandbox: Optional single-letter sandbox prefix.

    Returns:
        Formatted EUID, e.g. ``TX-1C`` or ``X:TX-1C``.
    """
    body = crockford_base32_encode(seq_val)
    if sandbox:
        payload = sandbox + prefix + body
    else:
        payload = prefix + body
    check = meridian_checksum(payload)
    if sandbox:
        return f"{sandbox}:{prefix}-{body}{check}"
    return f"{prefix}-{body}{check}"


def validate_euid(
    euid: str,
    *,
    environment: str = "production",
    allowed_sandbox_prefixes: list[str] | None = None,
) -> bool:
    """Validate an EUID string against Meridian spec.

    Returns True if the EUID is syntactically valid and checksum-correct
    for the given environment.
    """
    # §8.1 — reject non-ASCII, whitespace, lowercase
    if not euid.isascii():
        return False
    if any(c.isspace() for c in euid):
        return False
    if any(c.islower() for c in euid):
        return False

    has_colon = ":" in euid
    if has_colon:
        # Sandbox EUID
        if environment == "production":
            return False
        if not _SANDBOX_RE.match(euid):
            return False
        sandbox_prefix = euid[0]
        if (
            allowed_sandbox_prefixes is not None
            and sandbox_prefix not in allowed_sandbox_prefixes
        ):
            return False
        # Checksum payload = sandbox + category + body (no delimiters)
        stripped = euid.replace(":", "").replace("-", "")
        payload = stripped[:-1]
        presented_check = stripped[-1]
    else:
        # Production EUID
        if environment == "sandbox":
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
