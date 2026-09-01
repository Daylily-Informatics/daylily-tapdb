"""Backup manifest: canonical serialization, checksums, and signing stub.

The manifest is the description of what a backup captured. Two properties make
it load-bearing for verification:

1. **Canonical bytes.** ``canonical_json`` is the single serialization used
   everywhere, so the detached ``manifest.sha256`` always describes exactly the
   bytes written to ``manifest.json``. Any reader can re-derive the hash.
2. **No secrets, ever.** Manifests travel with the artifact and may be copied
   to shared storage. ``assert_no_secrets`` enforces the rule mechanically
   rather than by reviewer diligence -- notably excluding ``secret_arn``, which
   the target config does carry.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

MANIFEST_SCHEMA_VERSION = 1

BACKUP_CLASS_FULL = "full"
BACKUP_CLASS_TEMPLATE_PACK = "template-pack"
BACKUP_CLASS_PROVIDER_SNAPSHOT = "provider-snapshot"

BACKUP_CLASSES = (
    BACKUP_CLASS_FULL,
    BACKUP_CLASS_TEMPLATE_PACK,
    BACKUP_CLASS_PROVIDER_SNAPSHOT,
)

CONSISTENCY_SNAPSHOT = "snapshot"
CONSISTENCY_BEST_EFFORT = "best_effort"

#: ``provenance.created_by`` values -- why a backup exists.
#:
#: ``PROVENANCE_RESTORE`` marks a pre-restore safety backup, which is the last
#: copy of production if the restore it guards degrades. ``PROVENANCE_OPERATOR``
#: is a routine backup. Absence is a third state -- "written before this field
#: existed" -- and must never be read as ``PROVENANCE_OPERATOR``.
PROVENANCE_RESTORE = "restore"
PROVENANCE_OPERATOR = "operator"

#: Key fragments that must never appear anywhere in a manifest payload.
#: ``secret`` covers ``secret_arn``, which is deliberately excluded even though
#: it is only a reference -- it names a retrievable credential.
_FORBIDDEN_KEY_FRAGMENTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "pgpass",
)

#: Storage URIs must be credential-free. Matches ``scheme://user:pass@host``.
_URI_USERINFO = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://[^/@]*@")


def canonical_json(payload: Any) -> str:
    """Serialize a payload to the one canonical form used for hashing."""
    return json.dumps(payload, indent=2, sort_keys=True)


def canonical_bytes(payload: Any) -> bytes:
    """Return the exact bytes written to ``manifest.json``."""
    return canonical_json(payload).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    """Return the hex SHA-256 of ``data``."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Return the hex SHA-256 of a file, streamed so dumps need not fit in RAM."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def payload_checksum(payload: Any) -> str:
    """Return the SHA-256 of a payload's canonical bytes."""
    return sha256_hex(canonical_bytes(payload))


def signable_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """The manifest payload a signature covers: everything but the signature.

    A signature cannot cover itself. The producer hashed the payload while
    `signature` still held its default and then wrote the result *into* that
    field, so the stored manifest could never re-hash to the recorded value --
    `verify_manifest_signature` returned False for every manifest the system
    wrote, and nothing called it, so nothing noticed.
    """
    return {key: value for key, value in payload.items() if key != "signature"}


def find_secret_paths(payload: Any) -> list[str]:
    """Return dotted paths of any key that looks like it carries a secret.

    Values are not inspected -- a manifest legitimately contains hostnames and
    ARNs. Only key names are matched, which is what keeps this check free of
    false positives on data.
    """
    found: list[str] = []

    def _walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                key_text = str(key).lower()
                child = f"{path}.{key}" if path else str(key)
                if any(frag in key_text for frag in _FORBIDDEN_KEY_FRAGMENTS):
                    found.append(child)
                _walk(value, child)
        elif isinstance(node, (list, tuple)):
            for index, value in enumerate(node):
                _walk(value, f"{path}[{index}]")

    _walk(payload, "")
    return sorted(found)


def find_credential_uris(payload: Any) -> list[str]:
    """Return dotted paths of any *value* that is a credential-bearing URI.

    This is the one place values are inspected, and deliberately so: a URI with
    userinfo (``scheme://user:pass@host``) is unambiguously a secret, and the
    manifest records storage and mirror URIs as values under innocuous keys
    like ``uri`` that key-name matching would never catch.

    Settings load rejects such URIs too. This is the backstop, because the
    manifest is the thing that travels to shared storage.
    """
    found: list[str] = []

    def _walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                _walk(value, f"{path}.{key}" if path else str(key))
        elif isinstance(node, (list, tuple)):
            for index, value in enumerate(node):
                _walk(value, f"{path}[{index}]")
        elif isinstance(node, str) and _URI_USERINFO.match(node.strip()):
            found.append(path)

    _walk(payload, "")
    return sorted(found)


def assert_no_secrets(payload: Any) -> None:
    """Raise if a manifest payload carries anything secret-shaped."""
    offenders = find_secret_paths(payload)
    if offenders:
        raise ValueError(
            "Manifest payload contains forbidden secret-bearing keys: "
            + ", ".join(offenders)
        )
    credential_uris = find_credential_uris(payload)
    if credential_uris:
        raise ValueError(
            "Manifest payload contains credential-bearing URIs at: "
            + ", ".join(credential_uris)
        )


def assert_credential_free_uri(uri: str, *, field_name: str = "storage.uri") -> str:
    """Raise if a storage URI embeds credentials; return it otherwise."""
    text = (uri or "").strip()
    if text and _URI_USERINFO.match(text):
        raise ValueError(
            f"{field_name} must not embed credentials (found userinfo in URI)"
        )
    return text


@dataclass(frozen=True)
class AssetRef:
    """One file stored alongside the manifest."""

    name: str
    bytes: int
    sha256: str
    content_type: str = "application/octet-stream"

    def to_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "bytes": self.bytes,
            "sha256": self.sha256,
            "content_type": self.content_type,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "AssetRef":
        return cls(
            name=str(payload["name"]),
            bytes=int(payload["bytes"]),
            sha256=str(payload["sha256"]),
            content_type=str(payload.get("content_type", "application/octet-stream")),
        )

    @classmethod
    def from_file(
        cls,
        path: Path,
        *,
        name: Optional[str] = None,
        content_type: str = "application/octet-stream",
    ) -> "AssetRef":
        resolved = Path(path)
        return cls(
            name=name or resolved.name,
            bytes=resolved.stat().st_size,
            sha256=sha256_file(resolved),
            content_type=content_type,
        )


@dataclass(frozen=True)
class SequenceState:
    """A captured sequence high-water mark.

    Sequence values are non-transactional, so they are read after the dump
    completes. That makes the recorded value a lower bound on the live value at
    restore time -- verification asserts ``>=``, which is what guarantees no
    EUID is ever reissued.
    """

    name: str
    last_value: Optional[int]
    is_called: bool

    @property
    def next_value(self) -> Optional[int]:
        """The value the next ``nextval()`` will hand out.

        ``last_value`` alone does not determine this, and treating it as though
        it does is what allowed an in-place restore to reissue an EUID:

        =========================  ==========  =========  ==========
        state                      last_value  is_called  next value
        =========================  ==========  =========  ==========
        fresh                      1           False      1
        ``setval(s, 5, false)``    5           False      5
        ``setval(s, 5, true)``     5           True       6
        after ``nextval()``        1           True       2
        =========================  ==========  =========  ==========

        Two sequences with the same ``last_value`` hand out different next
        values, so comparisons for reuse safety must use this, not
        ``last_value``.

        ``None`` means the state was captured by a release that could not
        record it (see ``introspect.capture_sequences``); callers must treat
        that as "unknown" rather than as a low number, or they will report a
        regression on every old manifest.
        """
        if self.last_value is None:
            return None
        return self.last_value + (1 if self.is_called else 0)

    def to_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "last_value": self.last_value,
            "is_called": self.is_called,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "SequenceState":
        raw = payload.get("last_value")
        return cls(
            name=str(payload["name"]),
            last_value=None if raw is None else int(raw),
            is_called=bool(payload.get("is_called", False)),
        )


@dataclass
class BackupManifest:
    """Everything needed to verify and restore one backup artifact."""

    backup_id: str
    backup_class: str
    status: str = "complete"
    manifest_schema_version: int = MANIFEST_SCHEMA_VERSION

    tool: dict[str, Any] = field(default_factory=dict)
    target_identity: dict[str, Any] = field(default_factory=dict)
    postgres: dict[str, Any] = field(default_factory=dict)
    consistency: dict[str, Any] = field(default_factory=dict)
    migrations: dict[str, Any] = field(default_factory=dict)
    schema_drift: dict[str, Any] = field(default_factory=dict)
    row_counts: dict[str, int] = field(default_factory=dict)
    sequences: list[SequenceState] = field(default_factory=list)
    representative_objects: list[dict[str, Any]] = field(default_factory=list)
    content_inventory: dict[str, Any] = field(default_factory=dict)
    governance: dict[str, Any] = field(default_factory=dict)
    included_assets: list[AssetRef] = field(default_factory=list)
    excluded_state: list[dict[str, Any]] = field(default_factory=list)
    storage: dict[str, Any] = field(default_factory=dict)
    encryption: dict[str, Any] = field(default_factory=dict)
    retention: dict[str, Any] = field(default_factory=dict)
    #: Who created this backup and why -- ``{"created_by": "restore"|"operator",
    #: "restored_backup_id": ...}``.
    #:
    #: A pre-restore safety backup is otherwise indistinguishable from a routine
    #: one: same class, same id shape, and the only link back to the restore is
    #: an English sentence in ``timestamps.note``. That backup is the last copy
    #: of production if a restore degrades, so "is this a safety backup" needs a
    #: structured answer rather than a regex.
    #:
    #: Empty on every manifest written before this field existed; readers must
    #: treat absence as "unknown", never as "routine".
    provenance: dict[str, Any] = field(default_factory=dict)
    timestamps: dict[str, Any] = field(default_factory=dict)
    signature: dict[str, Any] = field(default_factory=lambda: {"algorithm": "none"})

    def to_payload(self) -> dict[str, Any]:
        """Render the manifest, rejecting any secret-bearing content."""
        payload: dict[str, Any] = {
            "manifest_schema_version": self.manifest_schema_version,
            "backup_id": self.backup_id,
            "backup_class": self.backup_class,
            "status": self.status,
            "tool": self.tool,
            "target_identity": self.target_identity,
            "postgres": self.postgres,
            "consistency": self.consistency,
            "migrations": self.migrations,
            "schema_drift": self.schema_drift,
            "row_counts": self.row_counts,
            "sequences": [seq.to_payload() for seq in self.sequences],
            "representative_objects": self.representative_objects,
            "content_inventory": self.content_inventory,
            "governance": self.governance,
            "included_assets": [asset.to_payload() for asset in self.included_assets],
            "excluded_state": self.excluded_state,
            "storage": self.storage,
            "encryption": self.encryption,
            "retention": self.retention,
            "provenance": self.provenance,
            "timestamps": self.timestamps,
            "signature": self.signature,
        }
        assert_no_secrets(payload)
        return payload

    def to_bytes(self) -> bytes:
        """Return the canonical bytes to write to ``manifest.json``."""
        return canonical_bytes(self.to_payload())

    def checksum(self) -> str:
        """Return the SHA-256 stored in the detached ``manifest.sha256``."""
        return sha256_hex(self.to_bytes())

    def asset(self, name: str) -> Optional[AssetRef]:
        """Return the named asset reference, if the manifest records one."""
        for candidate in self.included_assets:
            if candidate.name == name:
                return candidate
        return None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BackupManifest":
        return cls(
            backup_id=str(payload["backup_id"]),
            backup_class=str(payload["backup_class"]),
            status=str(payload.get("status", "complete")),
            manifest_schema_version=int(
                payload.get("manifest_schema_version", MANIFEST_SCHEMA_VERSION)
            ),
            tool=dict(payload.get("tool") or {}),
            target_identity=dict(payload.get("target_identity") or {}),
            postgres=dict(payload.get("postgres") or {}),
            consistency=dict(payload.get("consistency") or {}),
            migrations=dict(payload.get("migrations") or {}),
            schema_drift=dict(payload.get("schema_drift") or {}),
            row_counts={
                str(k): int(v) for k, v in (payload.get("row_counts") or {}).items()
            },
            sequences=[
                SequenceState.from_payload(item)
                for item in (payload.get("sequences") or [])
            ],
            representative_objects=list(payload.get("representative_objects") or []),
            content_inventory=dict(payload.get("content_inventory") or {}),
            governance=dict(payload.get("governance") or {}),
            included_assets=[
                AssetRef.from_payload(item)
                for item in (payload.get("included_assets") or [])
            ],
            excluded_state=list(payload.get("excluded_state") or []),
            storage=dict(payload.get("storage") or {}),
            encryption=dict(payload.get("encryption") or {}),
            retention=dict(payload.get("retention") or {}),
            provenance=dict(payload.get("provenance") or {}),
            timestamps=dict(payload.get("timestamps") or {}),
            signature=dict(payload.get("signature") or {"algorithm": "none"}),
        )

    @classmethod
    def from_bytes(cls, raw: bytes) -> "BackupManifest":
        return cls.from_payload(json.loads(raw.decode("utf-8")))


def sign_manifest(
    payload: dict[str, Any],
    *,
    mode: str = "none",
    kms_key_arn: str = "",
) -> dict[str, Any]:
    """Produce the manifest ``signature`` block.

    v1 ships SHA-256 integrity only. The field is designed now so that adding a
    KMS implementation later changes this function and its verify counterpart
    without touching the manifest shape or any caller.
    """
    normalized = (mode or "none").strip().lower()
    if normalized == "none":
        return {
            "algorithm": "none",
            "value": payload_checksum(signable_payload(payload)),
        }
    if normalized == "kms":
        raise NotImplementedError(
            "KMS manifest signing is designed but not implemented in v1; "
            "set backup.signing.mode to 'none'."
        )
    raise ValueError(f"Unsupported manifest signing mode: {mode!r}")


def verify_manifest_signature(
    payload: dict[str, Any],
    signature: Optional[dict[str, Any]],
) -> bool:
    """Verify a manifest ``signature`` block against its payload."""
    block = signature or {}
    algorithm = str(block.get("algorithm", "none")).strip().lower()
    if algorithm == "none":
        recorded = block.get("value")
        if not recorded:
            # v1 manifests may predate the value field; integrity is still
            # covered by the detached manifest.sha256.
            return True
        return str(recorded) == payload_checksum(signable_payload(payload))
    return False


def signature_scheme(
    payload: dict[str, Any],
    signature: Optional[dict[str, Any]],
) -> str:
    """Classify how a recorded signature relates to its payload.

    Returns ``"valid"``, ``"legacy"``, or ``"invalid"``.

    ``legacy`` exists because the original producer hashed the payload while
    ``signature`` still held its default. Those manifests are intact -- their
    bytes are covered by the detached ``manifest.sha256`` -- but they can never
    satisfy the corrected check. Treating them as invalid would make every
    backup taken before the fix fail verification and therefore fail restore
    preflight, turning a cosmetic defect into unrestorable artifacts.
    """
    block = signature or {}
    recorded = block.get("value")
    if str(block.get("algorithm", "none")).strip().lower() != "none" or not recorded:
        return "valid" if verify_manifest_signature(payload, signature) else "invalid"
    if str(recorded) == payload_checksum(signable_payload(payload)):
        return "valid"
    legacy_payload = dict(payload)
    legacy_payload["signature"] = {"algorithm": "none"}
    if str(recorded) == payload_checksum(legacy_payload):
        return "legacy"
    return "invalid"


__all__ = [
    "BACKUP_CLASSES",
    "BACKUP_CLASS_FULL",
    "BACKUP_CLASS_PROVIDER_SNAPSHOT",
    "BACKUP_CLASS_TEMPLATE_PACK",
    "CONSISTENCY_BEST_EFFORT",
    "CONSISTENCY_SNAPSHOT",
    "MANIFEST_SCHEMA_VERSION",
    "AssetRef",
    "BackupManifest",
    "SequenceState",
    "assert_credential_free_uri",
    "assert_no_secrets",
    "canonical_bytes",
    "canonical_json",
    "find_secret_paths",
    "payload_checksum",
    "sha256_file",
    "sha256_hex",
    "signable_payload",
    "sign_manifest",
    "signature_scheme",
    "verify_manifest_signature",
]
