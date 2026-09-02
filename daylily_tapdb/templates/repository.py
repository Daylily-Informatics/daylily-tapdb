"""Canonical repository-owned template packs and provenance receipts.

These packs are source artifacts for an owning application repository.  They
are deliberately separate from :mod:`daylily_tapdb.backup` template-pack
backups, which are database recovery artifacts with a different lifecycle.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select

from daylily_tapdb.models.template import generic_template
from daylily_tapdb.templates.loader import find_tapdb_core_config_dir, seed_templates

REPOSITORY_PACK_FORMAT = "tapdb.repository-template-pack/v1"
RECEIPT_FORMAT = "tapdb.repository-template-receipt/v1"
REQUIRED_FIELDS = (
    "name",
    "polymorphic_discriminator",
    "category",
    "type",
    "subtype",
    "version",
    "instance_prefix",
)
SEEDABLE_FIELDS = REQUIRED_FIELDS + (
    "instance_polymorphic_identity",
    "validator_ref",
    "bstatus",
    "json_addl",
    "json_addl_schema",
    "is_singleton",
)
FORBIDDEN_FIELDS = frozenset(
    {
        "uid",
        "euid",
        "euid_prefix",
        "euid_seq",
        "machine_uuid",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "created_dt",
        "modified_dt",
        "is_deleted",
        "password",
        "secret",
        "secret_arn",
        "connection_string",
    }
)
_SENSITIVE_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "private_key",
    "access_key",
    "connection_string",
)


@dataclass(frozen=True)
class RepositoryImportResult:
    """Validation or import result with no ORM objects attached."""

    dry_run: bool
    template_count: int
    inserted: int
    skipped: int
    prefixes_validated: tuple[str, ...]
    checksum_sha256: str


def canonical_json_bytes(payload: Any) -> bytes:
    """Return stable UTF-8 JSON suitable for source control and checksums."""

    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            separators=(",", ": "),
        )
        + "\n"
    ).encode("utf-8")


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_value(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)


def _assert_no_sensitive_keys(value: Any, *, location: str) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            if any(part in normalized for part in _SENSITIVE_KEY_PARTS):
                raise ValueError(f"{location} contains sensitive key: {key}")
            _assert_no_sensitive_keys(nested, location=f"{location}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _assert_no_sensitive_keys(nested, location=f"{location}[{index}]")


def serialize_template(row: Any) -> dict[str, Any]:
    """Serialize exactly the fields accepted by the supported seed loader."""

    payload: dict[str, Any] = {}
    for field in SEEDABLE_FIELDS:
        value = getattr(row, field, None)
        if field in REQUIRED_FIELDS:
            value = str(value or "")
        elif field == "is_singleton":
            value = bool(value)
        elif field == "json_addl":
            value = _json_value(value if value is not None else {})
        elif field == "json_addl_schema":
            value = _json_value(value) if value is not None else None
        elif value is not None:
            value = str(value)
        payload[field] = value
    _assert_no_sensitive_keys(payload.get("json_addl"), location="json_addl")
    _assert_no_sensitive_keys(
        payload.get("json_addl_schema"), location="json_addl_schema"
    )
    return payload


def template_key(template: dict[str, Any]) -> tuple[str, str, str, str]:
    return tuple(
        str(template.get(field) or "")
        for field in ("category", "type", "subtype", "version")
    )  # type: ignore[return-value]


def build_repository_pack(rows: Iterable[Any]) -> dict[str, Any]:
    """Build a deterministic repository pack with no database identities."""

    templates = [serialize_template(row) for row in rows]
    templates.sort(key=template_key)
    return {"format": REPOSITORY_PACK_FORMAT, "templates": templates}


def _active_owned_templates(
    session: Any,
    *,
    domain_code: str,
    issuer_app_code: str,
    template_euid: str | None = None,
) -> list[Any]:
    """Return the exact active rows eligible for a repository pack."""

    stmt = select(generic_template).where(
        generic_template.domain_code == str(domain_code),
        generic_template.issuer_app_code == str(issuer_app_code),
        generic_template.is_deleted.is_(False),
    )
    if template_euid:
        stmt = stmt.where(generic_template.euid == str(template_euid).strip())
    rows = list(session.execute(stmt).scalars())
    if not rows:
        if template_euid:
            raise LookupError(f"template not found: {template_euid}")
        raise LookupError("no active owned templates found for repository export")
    return rows


def repository_pack_bytes(
    session: Any,
    *,
    domain_code: str,
    issuer_app_code: str,
    template_euid: str | None = None,
) -> bytes:
    """Build canonical attachment bytes without writing server-side files."""

    rows = _active_owned_templates(
        session,
        domain_code=domain_code,
        issuer_app_code=issuer_app_code,
        template_euid=template_euid,
    )
    return canonical_json_bytes(build_repository_pack(rows))


def _absolute_pack_path(path: str | Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise ValueError("repository pack path must be absolute")
    if candidate.suffix.lower() != ".json":
        raise ValueError("repository pack path must name a .json file")
    resolved_parent = candidate.parent.resolve(strict=True)
    if not resolved_parent.is_dir():
        raise ValueError("repository pack parent must be an existing directory")
    return resolved_parent / candidate.name


def receipt_path(pack_path: str | Path) -> Path:
    path = _absolute_pack_path(pack_path)
    return path.with_name(f"{path.stem}.receipt.json")


def _path_occupied(path: Path) -> bool:
    """Treat dangling symlinks as collisions as well as existing files."""

    return path.exists() or path.is_symlink()


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_new(path: Path, content: bytes, *, mode: int = 0o644) -> None:
    if _path_occupied(path):
        raise FileExistsError(f"refusing to overwrite existing file: {path}")
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(temp_name)
    linked = False
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), mode)
            os.fsync(handle.fileno())
        if _path_occupied(path):
            raise FileExistsError(f"refusing to overwrite existing file: {path}")
        os.link(temp_path, path, follow_symlinks=False)
        linked = True
        _fsync_directory(path.parent)
    except Exception:
        if linked:
            path.unlink(missing_ok=True)
            _fsync_directory(path.parent)
        raise
    finally:
        temp_path.unlink(missing_ok=True)


def _load_registry_claims(
    registry_path: str | Path,
    *,
    domain_code: str,
    prefixes: Iterable[str],
) -> tuple[str, str, dict[str, Any]]:
    path = Path(registry_path).expanduser().resolve(strict=True)
    raw = path.read_bytes()
    payload = json.loads(raw)
    ownership = payload.get("ownership")
    if not isinstance(ownership, dict):
        raise ValueError("prefix registry is missing an ownership object")
    domain_claims = ownership.get(domain_code)
    if not isinstance(domain_claims, dict):
        raise ValueError(f"prefix registry has no claims for domain {domain_code!r}")
    claims: dict[str, Any] = {}
    for prefix in sorted(set(prefixes)):
        claim = domain_claims.get(prefix)
        if not isinstance(claim, dict):
            raise ValueError(
                f"prefix registry has no {domain_code!r}/{prefix!r} ownership claim"
            )
        claims[prefix] = claim
    return (
        hashlib.sha256(raw).hexdigest(),
        str(payload.get("version") or ""),
        claims,
    )


def export_repository_pack(
    session: Any,
    pack_path: str | Path,
    *,
    domain_code: str,
    issuer_app_code: str,
    prefix_registry_path: str | Path,
    actor: str,
    template_euid: str | None = None,
) -> dict[str, Any]:
    """Export active owned templates, atomically, with a provenance sidecar."""

    path = _absolute_pack_path(pack_path)
    sidecar = receipt_path(path)
    if _path_occupied(path) or _path_occupied(sidecar):
        collision = path if _path_occupied(path) else sidecar
        raise FileExistsError(f"refusing to overwrite existing file: {collision}")

    rows = _active_owned_templates(
        session,
        domain_code=domain_code,
        issuer_app_code=issuer_app_code,
        template_euid=template_euid,
    )

    pack = build_repository_pack(rows)
    pack_bytes = canonical_json_bytes(pack)
    checksum = hashlib.sha256(pack_bytes).hexdigest()
    prefixes = [str(item["instance_prefix"]).upper() for item in pack["templates"]]
    registry_checksum, registry_version, claims = _load_registry_claims(
        prefix_registry_path,
        domain_code=str(domain_code),
        prefixes=prefixes,
    )
    _assert_no_sensitive_keys(claims, location="prefix_registry.claims")
    exported_at = datetime.now(UTC).isoformat()
    receipt = {
        "format": RECEIPT_FORMAT,
        "operation": "export",
        "actor": str(actor or "").strip(),
        "exported_at": exported_at,
        "domain_code": str(domain_code),
        "issuer_app_code": str(issuer_app_code),
        # Store only the adjacent artifact name. Repository packs and their
        # receipts are source-controlled artifacts and must remain verifiable
        # after a checkout moves to a different absolute path.
        "repository_pack": path.name,
        "content_sha256": checksum,
        "template_count": len(rows),
        "templates": [
            {
                "stored_euid": str(row.euid),
                "template_key": list(template_key(serialize_template(row))),
                "created_dt": (
                    row.created_dt.isoformat()
                    if getattr(row, "created_dt", None)
                    else None
                ),
                "modified_dt": (
                    row.modified_dt.isoformat()
                    if getattr(row, "modified_dt", None)
                    else None
                ),
            }
            for row in sorted(
                rows, key=lambda item: template_key(serialize_template(item))
            )
        ],
        "prefix_registry": {
            "sha256": registry_checksum,
            "version": registry_version,
            "claims": claims,
        },
    }
    _assert_no_sensitive_keys(receipt, location="receipt")
    _atomic_write_new(path, pack_bytes)
    try:
        _atomic_write_new(sidecar, canonical_json_bytes(receipt), mode=0o444)
    except Exception:
        path.unlink(missing_ok=True)
        _fsync_directory(path.parent)
        raise
    return receipt


def read_repository_pack(pack_path: str | Path) -> tuple[Path, dict[str, Any], bytes]:
    path = _absolute_pack_path(pack_path)
    if not path.is_file():
        raise FileNotFoundError(f"repository pack not found: {path}")
    raw = path.read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("repository pack root must be a JSON object")
    validate_repository_pack(payload)
    return path, payload, raw


def _verified_receipt(
    path: Path,
    raw: bytes,
    *,
    domain_code: str,
    issuer_app_code: str,
) -> dict[str, Any]:
    sidecar = receipt_path(path)
    if not sidecar.is_file():
        raise FileNotFoundError(f"repository receipt not found: {sidecar}")
    try:
        receipt = json.loads(sidecar.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"repository receipt is unreadable: {sidecar}") from exc
    if not isinstance(receipt, dict) or receipt.get("format") != RECEIPT_FORMAT:
        raise ValueError(f"repository receipt format must be {RECEIPT_FORMAT!r}")
    if receipt.get("repository_pack") != path.name:
        raise ValueError("repository receipt pack path does not match")
    if receipt.get("content_sha256") != hashlib.sha256(raw).hexdigest():
        raise ValueError("repository receipt checksum does not match pack content")
    if receipt.get("domain_code") != str(domain_code):
        raise ValueError("repository receipt domain does not match target")
    if receipt.get("issuer_app_code") != str(issuer_app_code):
        raise ValueError("repository receipt owner does not match target")
    return receipt


def validate_repository_pack(payload: dict[str, Any]) -> None:
    if payload.get("format") != REPOSITORY_PACK_FORMAT:
        raise ValueError(f"repository pack format must be {REPOSITORY_PACK_FORMAT!r}")
    templates = payload.get("templates")
    if not isinstance(templates, list) or not templates:
        raise ValueError("repository pack must contain a non-empty templates array")
    seen: set[tuple[str, str, str, str]] = set()
    for index, item in enumerate(templates):
        if not isinstance(item, dict):
            raise ValueError(f"templates[{index}] must be an object")
        forbidden = sorted(FORBIDDEN_FIELDS.intersection(item))
        if forbidden:
            raise ValueError(
                f"templates[{index}] contains forbidden field(s): {', '.join(forbidden)}"
            )
        missing = [field for field in REQUIRED_FIELDS if not str(item.get(field) or "")]
        if missing:
            raise ValueError(
                f"templates[{index}] missing required field(s): {', '.join(missing)}"
            )
        unknown = sorted(set(item).difference(SEEDABLE_FIELDS))
        if unknown:
            raise ValueError(
                f"templates[{index}] contains unsupported field(s): {', '.join(unknown)}"
            )
        _assert_no_sensitive_keys(
            item.get("json_addl"), location=f"templates[{index}].json_addl"
        )
        _assert_no_sensitive_keys(
            item.get("json_addl_schema"),
            location=f"templates[{index}].json_addl_schema",
        )
        key = template_key(item)
        if key in seen:
            raise ValueError(f"duplicate template identity: {'/'.join(key)}")
        seen.add(key)


def _validate_import_claims(
    payload: dict[str, Any],
    *,
    domain_code: str,
    owner_repo_name: str,
    domain_registry_path: str | Path,
    prefix_registry_path: str | Path,
) -> tuple[str, ...]:
    domain_payload = json.loads(Path(domain_registry_path).expanduser().read_text())
    if str(domain_code) not in (domain_payload.get("domains") or {}):
        raise ValueError(f"domain registry has no domain {domain_code!r}")
    prefixes = tuple(
        sorted(
            {
                str(item["instance_prefix"]).strip().upper()
                for item in payload["templates"]
            }
        )
    )
    _checksum, _version, claims = _load_registry_claims(
        prefix_registry_path, domain_code=str(domain_code), prefixes=prefixes
    )
    for prefix, claim in claims.items():
        claimant = str(
            claim.get("issuer_app_code")
            or claim.get("owner_repo_name")
            or claim.get("repo_name")
            or ""
        )
        if claimant != str(owner_repo_name):
            raise ValueError(
                f"prefix {prefix!r} is claimed by {claimant!r}, not {owner_repo_name!r}"
            )
    return prefixes


def import_repository_pack(
    session: Any,
    pack_path: str | Path,
    *,
    domain_code: str,
    owner_repo_name: str,
    domain_registry_path: str | Path,
    prefix_registry_path: str | Path,
    dry_run: bool = True,
) -> RepositoryImportResult:
    """Validate or import a pack through the supported governed seed loader."""

    path, payload, raw = read_repository_pack(pack_path)
    _verified_receipt(
        path,
        raw,
        domain_code=domain_code,
        issuer_app_code=owner_repo_name,
    )
    prefixes = _validate_import_claims(
        payload,
        domain_code=domain_code,
        owner_repo_name=owner_repo_name,
        domain_registry_path=domain_registry_path,
        prefix_registry_path=prefix_registry_path,
    )
    existing_by_key = {
        template_key(serialize_template(row)): serialize_template(row)
        for row in session.execute(
            select(generic_template).where(
                generic_template.domain_code == str(domain_code),
                generic_template.issuer_app_code == str(owner_repo_name),
            )
        ).scalars()
    }
    skipped = 0
    pending: list[dict[str, Any]] = []
    for item in payload["templates"]:
        current = existing_by_key.get(template_key(item))
        if current is None:
            pending.append(item)
        elif current == item:
            skipped += 1
        else:
            raise ValueError(
                "repository template conflicts with stored identity: "
                + "/".join(template_key(item))
            )
    inserted = 0
    if not dry_run and pending:
        summary = seed_templates(
            session,
            pending,
            overwrite=False,
            core_config_dir=find_tapdb_core_config_dir(),
            domain_code=str(domain_code),
            owner_repo_name=str(owner_repo_name),
            domain_registry_path=Path(domain_registry_path),
            prefix_registry_path=Path(prefix_registry_path),
        )
        inserted = summary.inserted
        skipped += summary.skipped
    return RepositoryImportResult(
        dry_run=bool(dry_run),
        template_count=len(payload["templates"]),
        inserted=inserted,
        skipped=skipped,
        prefixes_validated=prefixes,
        checksum_sha256=hashlib.sha256(raw).hexdigest(),
    )


def repository_inventory(
    session: Any,
    pack_path: str | Path,
    *,
    domain_code: str,
    issuer_app_code: str,
) -> dict[str, Any]:
    """Report pending/backed-up/failed status for each stored template."""

    path = _absolute_pack_path(pack_path)
    rows = list(
        session.execute(
            select(generic_template).where(
                generic_template.domain_code == str(domain_code),
                generic_template.issuer_app_code == str(issuer_app_code),
                generic_template.is_deleted.is_(False),
            )
        ).scalars()
    )
    pack_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    pack_error: str | None = None
    if path.exists():
        try:
            _path, pack, raw = read_repository_pack(path)
            _verified_receipt(
                path,
                raw,
                domain_code=domain_code,
                issuer_app_code=issuer_app_code,
            )
            pack_by_key = {template_key(item): item for item in pack["templates"]}
        except Exception as exc:
            pack_error = str(exc)
    elif receipt_path(path).exists():
        pack_error = f"repository receipt exists without pack: {receipt_path(path)}"
    items = []
    for row in sorted(rows, key=lambda item: template_key(serialize_template(item))):
        serialized = serialize_template(row)
        current = pack_by_key.get(template_key(serialized))
        if pack_error:
            status = "failed"
        elif current == serialized:
            status = "backed-up"
        else:
            status = "pending"
        items.append(
            {
                "stored_euid": str(row.euid),
                "template_key": list(template_key(serialized)),
                "status": status,
            }
        )
    return {
        "format": "tapdb.repository-template-inventory/v1",
        "repository_pack": str(path),
        "status": "failed" if pack_error else "ok",
        "error": pack_error,
        "items": items,
        "counts": {
            status: sum(1 for item in items if item["status"] == status)
            for status in ("pending", "backed-up", "failed")
        },
    }


__all__ = [
    "FORBIDDEN_FIELDS",
    "REPOSITORY_PACK_FORMAT",
    "RepositoryImportResult",
    "build_repository_pack",
    "canonical_json_bytes",
    "export_repository_pack",
    "import_repository_pack",
    "read_repository_pack",
    "repository_inventory",
    "repository_pack_bytes",
    "serialize_template",
    "template_key",
    "validate_repository_pack",
]
