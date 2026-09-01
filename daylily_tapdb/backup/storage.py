"""Storage backends for backup artifacts.

Layout (no central index file -- ``list`` discovers backups by scanning for
``manifest.json`` keys, so a hand-copied backup directory is still listable and
a corrupt index can never hide a good backup)::

    <root>/<client_id>/<database_name>/<backup_class>/<backup_id>/
        manifest.json
        manifest.sha256
        <artifact>
    <root>/<client_id>/<database_name>/rehearsals/<backup_id>/<ts>.json

``LocalStorageBackend`` exposes ``local_path`` so ``pg_dump`` can write straight
to its final destination. Remote backends return ``None`` there, which tells the
service to dump to a staging file and upload.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Iterable, Optional, Protocol, runtime_checkable
from urllib.parse import urlparse

from daylily_tapdb.backup.manifest import assert_credential_free_uri

MANIFEST_KEY = "manifest.json"
MANIFEST_CHECKSUM_KEY = "manifest.sha256"
REHEARSALS_SEGMENT = "rehearsals"


def _clean_segment(value: str, *, field_name: str) -> str:
    """Validate one path segment: no separators, no traversal, non-empty."""
    text = str(value or "").strip().strip("/")
    if not text:
        raise ValueError(f"{field_name} must not be empty")
    if "/" in text or "\\" in text:
        raise ValueError(f"{field_name} must not contain path separators: {value!r}")
    if text in {".", ".."} or text.startswith(".."):
        raise ValueError(f"{field_name} must not traverse paths: {value!r}")
    return text


def normalize_key(key: str) -> str:
    """Normalize a storage key, rejecting absolute paths and traversal."""
    text = str(key or "").strip()
    if not text:
        raise ValueError("storage key must not be empty")
    if text.startswith("/") or text.startswith("\\"):
        raise ValueError(f"storage key must be relative: {key!r}")
    parts = [part for part in text.replace("\\", "/").split("/") if part]
    if any(part == ".." for part in parts):
        raise ValueError(f"storage key must not traverse paths: {key!r}")
    if not parts:
        raise ValueError("storage key must not be empty")
    return "/".join(parts)


def backup_prefix(
    client_id: str,
    database_name: str,
    backup_class: str,
    backup_id: str,
) -> str:
    """Return the storage prefix holding one backup's manifest and artifact."""
    return "/".join(
        (
            _clean_segment(client_id, field_name="client_id"),
            _clean_segment(database_name, field_name="database_name"),
            _clean_segment(backup_class, field_name="backup_class"),
            _clean_segment(backup_id, field_name="backup_id"),
        )
    )


def database_prefix(client_id: str, database_name: str) -> str:
    """Return the storage prefix covering every backup for one target."""
    return "/".join(
        (
            _clean_segment(client_id, field_name="client_id"),
            _clean_segment(database_name, field_name="database_name"),
        )
    )


def rehearsal_key(
    client_id: str,
    database_name: str,
    backup_id: str,
    stamp: str,
) -> str:
    """Return the key for one rehearsal evidence document."""
    return "/".join(
        (
            database_prefix(client_id, database_name),
            REHEARSALS_SEGMENT,
            _clean_segment(backup_id, field_name="backup_id"),
            f"{_clean_segment(stamp, field_name='stamp')}.json",
        )
    )


@runtime_checkable
class StorageBackend(Protocol):
    """The storage surface the backup service depends on."""

    def put_bytes(self, key: str, data: bytes) -> None: ...

    def get_bytes(self, key: str) -> bytes: ...

    def put_file(self, key: str, source: Path) -> None: ...

    def get_file(self, key: str, destination: Path) -> Path: ...

    def exists(self, key: str) -> bool: ...

    def list_keys(self, prefix: str = "") -> list[str]: ...

    def list_sizes(self, prefix: str = "") -> dict[str, int]: ...

    def deletion_capability(self) -> dict[str, Any]: ...

    def delete(self, key: str) -> None: ...

    def delete_prefix(self, prefix: str) -> None: ...

    def local_path(self, key: str) -> Optional[Path]: ...

    def describe(self) -> dict[str, Any]: ...


class LocalStorageBackend:
    """Filesystem-backed storage rooted at a directory."""

    scheme = "file"

    def __init__(self, root: Path):
        self.root = Path(root).expanduser().resolve()

    def _resolve(self, key: str) -> Path:
        return self.root / normalize_key(key)

    def put_bytes(self, key: str, data: bytes) -> None:
        target = self._resolve(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Unique per writer. A name derived from the target collides between
        # concurrent writers of the same key: the loser's `os.replace` finds
        # its staged file already moved and raises FileNotFoundError. Receipt
        # mirroring writes the same `head.json` from every writer, so this is
        # reachable rather than theoretical -- `write_receipt` uses `mkstemp`
        # for exactly this reason.
        handle, tmp_name = tempfile.mkstemp(
            dir=target.parent, prefix=f".{target.name}.", suffix=".tmp"
        )
        tmp = Path(tmp_name)
        try:
            with os.fdopen(handle, "wb") as stream:
                stream.write(data)
            os.replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)

    def get_bytes(self, key: str) -> bytes:
        return self._resolve(key).read_bytes()

    def put_file(self, key: str, source: Path) -> None:
        target = self._resolve(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        source = Path(source)
        if source.resolve() == target:
            return
        tmp = target.with_name(f".{target.name}.tmp")
        shutil.copyfile(source, tmp)
        os.replace(tmp, target)

    def get_file(self, key: str, destination: Path) -> Path:
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        source = self._resolve(key)
        if source.resolve() != destination.resolve():
            shutil.copyfile(source, destination)
        return destination

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def list_keys(self, prefix: str = "") -> list[str]:
        base = self.root if not prefix else self._resolve(prefix)
        if not base.exists():
            return []
        keys: list[str] = []
        for path in sorted(base.rglob("*")):
            if path.is_file() and not path.name.startswith("."):
                keys.append(path.relative_to(self.root).as_posix())
        return keys

    def list_sizes(self, prefix: str = "") -> dict[str, int]:
        """Return ``{key: size_in_bytes}`` for everything under ``prefix``.

        Exists so a caller can tell "the artifact is present" from "the
        artifact is the size it claims to be". A truncated or zero-byte upload
        satisfies the first and not the second, and it is the realistic
        corruption mode for a partial sync or an interrupted transfer.
        """
        base = self.root if not prefix else self._resolve(prefix)
        if not base.exists():
            return {}
        return {
            path.relative_to(self.root).as_posix(): path.stat().st_size
            for path in sorted(base.rglob("*"))
            if path.is_file() and not path.name.startswith(".")
        }

    def delete(self, key: str) -> None:
        target = self._resolve(key)
        if target.exists():
            target.unlink()

    def delete_prefix(self, prefix: str) -> None:
        target = self._resolve(prefix)
        if target.is_dir():
            shutil.rmtree(target)

    def local_path(self, key: str) -> Optional[Path]:
        """Return where a key lives on disk, creating nothing.

        A getter must have no side effects: read-only operations like `plan`
        call this, and creating the storage tree would make them fail their own
        "never mutates" guarantee. Callers that intend to write use
        ``put_file``/``put_bytes``, which create parents themselves.
        """
        return self._resolve(key)

    def describe(self) -> dict[str, Any]:
        return {"backend": "local", "uri": f"file://{self.root}"}

    def deletion_capability(self) -> dict[str, Any]:
        """A filesystem unlink genuinely frees the bytes."""
        return {
            "reclaims": True,
            "reason": "local filesystem: unlink frees the bytes",
            "versioning": None,
            "object_lock": False,
        }


class S3StorageBackend:
    """S3-backed storage.

    boto3 is imported lazily on first use, matching ``cli/aurora.py``: importing
    this module -- which the CLI does on every invocation -- must never require
    AWS libraries or credentials.
    """

    scheme = "s3"

    def __init__(self, bucket: str, prefix: str = ""):
        self.bucket = _clean_segment(bucket, field_name="bucket")
        self.prefix = normalize_key(prefix) if prefix else ""
        self._client: Any = None

    def _full_key(self, key: str) -> str:
        normalized = normalize_key(key)
        return f"{self.prefix}/{normalized}" if self.prefix else normalized

    def _delimited_prefix(self, prefix: str) -> str:
        """Return the S3 ``Prefix`` for a listing, bounded at a path segment.

        S3 prefix matching is a plain string compare, so listing ``acme/orders``
        without a trailing delimiter *also* returns ``acme/orders-staging`` and
        ``acme/orders-archive`` -- a different target's backups, surfacing in
        this target's listing, status page and GUI.

        Only the trailing segment is exposed (``client_id`` is always followed
        by ``/``), so the reach is precisely: another target under the same
        client whose ``database_name`` starts with this one's. That is an
        ordinary naming convention, not a pathological one.

        ``LocalStorageBackend.list_keys`` walks a real directory and has a real
        boundary, so no local-fixture test can distinguish the two forms -- the
        stubbed-client test asserting this literal argument is the only guard.
        """
        full = self._full_key(prefix) if prefix else self.prefix
        if not full:
            # No bucket-level prefix and no argument: list the whole bucket.
            # "" is already unbounded, and "/" would match nothing.
            return ""
        return full if full.endswith("/") else f"{full}/"

    def _ensure_client(self) -> Any:
        if self._client is None:
            try:
                import boto3  # noqa: PLC0415 -- deliberate lazy import
            except ImportError as exc:  # pragma: no cover - env dependent
                raise RuntimeError(
                    "S3 backup storage requires boto3. Install it or set "
                    "backup.storage.uri to a file:// destination."
                ) from exc
            self._client = boto3.client("s3")
        return self._client

    def put_bytes(self, key: str, data: bytes) -> None:
        self._ensure_client().put_object(
            Bucket=self.bucket, Key=self._full_key(key), Body=data
        )

    def get_bytes(self, key: str) -> bytes:
        response = self._ensure_client().get_object(
            Bucket=self.bucket, Key=self._full_key(key)
        )
        return response["Body"].read()

    def put_file(self, key: str, source: Path) -> None:
        self._ensure_client().upload_file(
            str(Path(source)), self.bucket, self._full_key(key)
        )

    def get_file(self, key: str, destination: Path) -> Path:
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_client().download_file(
            self.bucket, self._full_key(key), str(destination)
        )
        return destination

    def exists(self, key: str) -> bool:
        """Return whether the object exists.

        Only a genuine "not found" is reported as False. An expired token or a
        denied HeadObject is re-raised rather than silently reported as absent
        -- the service treats absence as "safe to write here", so swallowing an
        auth failure could overwrite a real backup.
        """
        client = self._ensure_client()
        try:
            client.head_object(Bucket=self.bucket, Key=self._full_key(key))
        except Exception as exc:
            response = getattr(exc, "response", None) or {}
            code = str(response.get("Error", {}).get("Code", ""))
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
                return False
            raise
        return True

    def list_keys(self, prefix: str = "") -> list[str]:
        client = self._ensure_client()
        full_prefix = self._delimited_prefix(prefix)
        paginator = client.get_paginator("list_objects_v2")
        keys: list[str] = []
        strip = f"{self.prefix}/" if self.prefix else ""
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for item in page.get("Contents", []) or []:
                key = str(item["Key"])
                keys.append(
                    key[len(strip) :] if strip and key.startswith(strip) else key
                )
        return sorted(keys)

    def list_sizes(self, prefix: str = "") -> dict[str, int]:
        """Return ``{key: size_in_bytes}`` for everything under ``prefix``.

        Free: ``list_objects_v2`` already returns ``Size`` for every key, so
        this is the same request ``list_keys`` makes with a field kept instead
        of discarded. No ``HeadObject`` per asset.
        """
        client = self._ensure_client()
        full_prefix = self._delimited_prefix(prefix)
        paginator = client.get_paginator("list_objects_v2")
        strip = f"{self.prefix}/" if self.prefix else ""
        sizes: dict[str, int] = {}
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for item in page.get("Contents", []) or []:
                key = str(item["Key"])
                trimmed = key[len(strip) :] if strip and key.startswith(strip) else key
                sizes[trimmed] = int(item.get("Size") or 0)
        return sizes

    def delete(self, key: str) -> None:
        self._ensure_client().delete_object(Bucket=self.bucket, Key=self._full_key(key))

    def delete_prefix(self, prefix: str) -> None:
        for key in self.list_keys(prefix):
            self.delete(key)

    def local_path(self, key: str) -> Optional[Path]:
        """No local path exists; the service stages to a temp file and uploads."""
        _ = key
        return None

    def deletion_capability(self) -> dict[str, Any]:
        """Report whether deleting an object here actually frees anything.

        Two bucket properties make a ``DeleteObject`` not mean what it says,
        and each must be distinguished from *not knowing*:

        * **Versioning.** ``Enabled`` writes a delete marker and keeps the
          object. ``Suspended`` is not ``Disabled`` -- a delete still writes a
          null-version marker, and every non-null version written while
          versioning was on is retained. Treating ``Status != "Enabled"`` as
          safe is the natural implementation and is wrong for exactly the
          objects that matter. A never-versioned bucket returns no ``Status``
          key at all, not ``"Disabled"``.
        * **Object Lock.** The bucket carries an externally declared retention
          policy that a TapDB-side prune can only conflict with.

        A denied probe returns ``reclaims: None`` -- *unknown*, not *absent*.
        Mapping ``AccessDenied`` onto ``object_lock: False`` is the obvious
        shortcut and it silently converts a no-escape refusal into a pass on a
        bucket that may well be locked.

        Deliberately not called by ``describe()``, which is embedded in every
        manifest and runs on every plan: two API calls and two IAM permissions
        on the create path to serve a delete-path concern is the wrong trade.
        """
        client = self._ensure_client()

        def _code(exc: Exception) -> str:
            response = getattr(exc, "response", None) or {}
            return str(response.get("Error", {}).get("Code", ""))

        def _denied(exc: Exception) -> bool:
            response = getattr(exc, "response", None) or {}
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            return _code(exc) in {"AccessDenied", "AccessDeniedException"} or (
                status == 403
            )

        def _unknown(reason: str, versioning: Any = None) -> dict[str, Any]:
            return {
                "reclaims": None,
                "reason": reason,
                "versioning": versioning,
                "object_lock": None,
            }

        try:
            response = client.get_bucket_versioning(Bucket=self.bucket)
            versioning = response.get("Status")
        except Exception as exc:  # noqa: BLE001
            if _denied(exc):
                return _unknown("bucket versioning could not be read (denied)")
            return _unknown(f"bucket versioning could not be read: {exc}")

        try:
            client.get_object_lock_configuration(Bucket=self.bucket)
            object_lock = True
        except Exception as exc:  # noqa: BLE001
            if _code(exc) == "ObjectLockConfigurationNotFoundError":
                object_lock = False
            elif _denied(exc):
                return _unknown(
                    "Object Lock configuration could not be read (denied)",
                    versioning=versioning,
                )
            else:
                return _unknown(
                    f"Object Lock configuration could not be read: {exc}",
                    versioning=versioning,
                )

        versioned = versioning in ("Enabled", "Suspended")
        if object_lock:
            reason = "Object Lock is configured; retention is governed outside TapDB"
        elif versioned:
            reason = (
                f"bucket versioning is {versioning}; a delete writes a marker and "
                "reclaims nothing. Use an S3 lifecycle "
                "NoncurrentVersionExpiration rule instead."
            )
        else:
            reason = "unversioned bucket without Object Lock: a delete frees the bytes"

        return {
            "reclaims": not object_lock and not versioned,
            "reason": reason,
            "versioning": versioning,
            "object_lock": object_lock,
        }

    def describe(self) -> dict[str, Any]:
        base = f"s3://{self.bucket}"
        return {
            "backend": "s3",
            "uri": f"{base}/{self.prefix}" if self.prefix else base,
        }


def default_storage_uri(config_dir: Path) -> str:
    """Return the default ``file://`` storage URI under the config directory."""
    return f"file://{Path(config_dir).expanduser().resolve() / 'backups'}"


def build_storage_backend(uri: str, *, config_dir: Optional[Path] = None) -> Any:
    """Construct a storage backend from a configured URI.

    An empty URI falls back to ``<config_dir>/backups``. Credential-bearing
    URIs are rejected here rather than at use time so a bad config fails at
    load, not mid-backup.
    """
    text = assert_credential_free_uri(uri or "")
    if not text:
        if config_dir is None:
            raise ValueError(
                "backup.storage.uri is unset and no config_dir was supplied"
            )
        text = default_storage_uri(config_dir)

    parsed = urlparse(text)
    scheme = (parsed.scheme or "file").lower()

    if scheme == "file":
        raw = f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path
        if not raw:
            raise ValueError(f"file:// storage URI has no path: {uri!r}")
        return LocalStorageBackend(Path(raw))
    if scheme == "s3":
        if not parsed.netloc:
            raise ValueError(f"s3:// storage URI has no bucket: {uri!r}")
        return S3StorageBackend(parsed.netloc, parsed.path.strip("/"))
    if not parsed.scheme:
        return LocalStorageBackend(Path(text))
    raise ValueError(
        f"Unsupported backup storage scheme {scheme!r}; expected 'file' or 's3'"
    )


def discover_backup_prefixes(keys: Iterable[str]) -> list[str]:
    """Return the prefix of every backup found in a key listing."""
    prefixes: list[str] = []
    for key in keys:
        normalized = key.replace("\\", "/")
        if normalized.endswith(f"/{MANIFEST_KEY}"):
            prefixes.append(normalized[: -(len(MANIFEST_KEY) + 1)])
        elif normalized == MANIFEST_KEY:
            prefixes.append("")
    return sorted(set(prefixes))


__all__ = [
    "MANIFEST_CHECKSUM_KEY",
    "MANIFEST_KEY",
    "REHEARSALS_SEGMENT",
    "LocalStorageBackend",
    "S3StorageBackend",
    "StorageBackend",
    "backup_prefix",
    "build_storage_backend",
    "database_prefix",
    "default_storage_uri",
    "discover_backup_prefixes",
    "normalize_key",
    "rehearsal_key",
]
