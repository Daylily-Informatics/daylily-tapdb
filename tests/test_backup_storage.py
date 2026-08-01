"""Storage backends: key hygiene, layout, and local round-trips."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from daylily_tapdb.backup.storage import (
    MANIFEST_KEY,
    LocalStorageBackend,
    S3StorageBackend,
    backup_prefix,
    build_storage_backend,
    database_prefix,
    default_storage_uri,
    discover_backup_prefixes,
    normalize_key,
    rehearsal_key,
)


def test_backup_prefix_matches_documented_layout():
    prefix = backup_prefix("acme", "prod", "full", "full-20260727T000000Z")

    assert prefix == "acme/prod/full/full-20260727T000000Z"


def test_database_prefix_covers_all_classes_for_one_target():
    assert database_prefix("acme", "prod") == "acme/prod"


def test_rehearsal_key_matches_documented_layout():
    key = rehearsal_key("acme", "prod", "full-1", "20260727T101500Z")

    assert key == "acme/prod/rehearsals/full-1/20260727T101500Z.json"


@pytest.mark.parametrize(
    "segment",
    ["", "   ", "with/slash", "..", "../escape", "back\\slash"],
)
def test_path_segments_reject_traversal_and_separators(segment):
    with pytest.raises(ValueError):
        backup_prefix(segment, "prod", "full", "id")


@pytest.mark.parametrize("key", ["/absolute", "a/../../etc/passwd", "", "   "])
def test_normalize_key_rejects_unsafe_keys(key):
    with pytest.raises(ValueError):
        normalize_key(key)


def test_normalize_key_collapses_redundant_separators():
    assert normalize_key("a//b/c") == "a/b/c"
    assert normalize_key("a\\b") == "a/b"


def test_local_backend_roundtrips_bytes_and_files(tmp_path: Path):
    backend = LocalStorageBackend(tmp_path / "root")
    backend.put_bytes("acme/prod/full/one/manifest.json", b'{"a":1}')

    assert backend.exists("acme/prod/full/one/manifest.json")
    assert backend.get_bytes("acme/prod/full/one/manifest.json") == b'{"a":1}'

    source = tmp_path / "dump.bin"
    source.write_bytes(b"dump-bytes")
    backend.put_file("acme/prod/full/one/tapdb.dump", source)

    fetched = backend.get_file(
        "acme/prod/full/one/tapdb.dump", tmp_path / "fetched.bin"
    )
    assert fetched.read_bytes() == b"dump-bytes"


def test_local_backend_writes_are_atomic_leaving_no_temp_files(tmp_path: Path):
    backend = LocalStorageBackend(tmp_path / "root")
    backend.put_bytes("a/b/c/manifest.json", b"{}")

    directory = (tmp_path / "root" / "a" / "b" / "c").resolve()
    leftovers = [p.name for p in directory.iterdir() if p.name.startswith(".")]

    assert leftovers == []


def test_local_backend_lists_keys_and_ignores_hidden_files(tmp_path: Path):
    backend = LocalStorageBackend(tmp_path / "root")
    backend.put_bytes("acme/prod/full/one/manifest.json", b"{}")
    backend.put_bytes("acme/prod/full/two/manifest.json", b"{}")
    (tmp_path / "root" / "acme" / "prod" / ".hidden").write_bytes(b"x")

    keys = backend.list_keys("acme/prod")

    assert keys == [
        "acme/prod/full/one/manifest.json",
        "acme/prod/full/two/manifest.json",
    ]


def test_local_backend_list_keys_is_empty_for_missing_prefix(tmp_path: Path):
    backend = LocalStorageBackend(tmp_path / "root")

    assert backend.list_keys("nothing/here") == []


def test_local_backend_delete_and_delete_prefix(tmp_path: Path):
    backend = LocalStorageBackend(tmp_path / "root")
    backend.put_bytes("acme/prod/full/one/manifest.json", b"{}")
    backend.put_bytes("acme/prod/full/one/tapdb.dump", b"x")

    backend.delete("acme/prod/full/one/tapdb.dump")
    assert not backend.exists("acme/prod/full/one/tapdb.dump")

    backend.delete_prefix("acme/prod/full/one")
    assert backend.list_keys("acme/prod") == []


def test_local_backend_exposes_a_local_path(tmp_path: Path):
    backend = LocalStorageBackend(tmp_path / "root")
    path = backend.local_path("acme/prod/full/one/tapdb.dump")

    assert path is not None
    assert path == (tmp_path / "root" / "acme/prod/full/one/tapdb.dump").resolve()


def test_local_path_creates_nothing(tmp_path: Path):
    # Read-only operations call this while reporting on a destination; a getter
    # that mkdirs would make `plan` violate its own never-mutates guarantee.
    backend = LocalStorageBackend(tmp_path / "root")

    backend.local_path("acme/prod/full/one/tapdb.dump")

    assert not (tmp_path / "root").exists()


def test_discover_backup_prefixes_finds_backups_without_an_index():
    keys = [
        "acme/prod/full/one/manifest.json",
        "acme/prod/full/one/tapdb.dump",
        "acme/prod/full/two/manifest.json",
        "acme/prod/rehearsals/one/20260727T101500Z.json",
    ]

    assert discover_backup_prefixes(keys) == [
        "acme/prod/full/one",
        "acme/prod/full/two",
    ]


def test_build_storage_backend_resolves_file_uri(tmp_path: Path):
    backend = build_storage_backend(f"file://{tmp_path}/store")

    assert isinstance(backend, LocalStorageBackend)
    assert backend.root == (tmp_path / "store").resolve()


def test_build_storage_backend_defaults_under_config_dir(tmp_path: Path):
    backend = build_storage_backend("", config_dir=tmp_path)

    assert isinstance(backend, LocalStorageBackend)
    assert backend.root == (tmp_path / "backups").resolve()
    assert default_storage_uri(tmp_path).endswith("/backups")


def test_build_storage_backend_requires_config_dir_when_uri_is_empty():
    with pytest.raises(ValueError, match="config_dir"):
        build_storage_backend("")


def test_build_storage_backend_resolves_s3_uri_without_importing_boto3():
    sys.modules.pop("boto3", None)
    backend = build_storage_backend("s3://my-bucket/tapdb/backups")

    assert isinstance(backend, S3StorageBackend)
    assert backend.bucket == "my-bucket"
    assert backend.prefix == "tapdb/backups"
    assert backend.local_path("anything") is None
    # Constructing the backend must not drag in the AWS SDK.
    assert "boto3" not in sys.modules


def test_build_storage_backend_rejects_credential_bearing_uri():
    with pytest.raises(ValueError, match="must not embed credentials"):
        build_storage_backend("s3://key:secret@bucket/prefix")


@pytest.mark.parametrize("uri", ["ftp://host/path", "https://host/path"])
def test_build_storage_backend_rejects_unsupported_schemes(uri):
    with pytest.raises(ValueError, match="Unsupported backup storage scheme"):
        build_storage_backend(uri)


def test_s3_uri_without_bucket_is_rejected():
    with pytest.raises(ValueError, match="no bucket"):
        build_storage_backend("s3:///just/a/path")


def test_manifest_key_constant_is_what_discovery_scans_for():
    assert MANIFEST_KEY == "manifest.json"
