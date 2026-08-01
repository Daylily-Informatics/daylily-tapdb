"""S3 backend behaviour, exercised against a fake client.

The real boto3 client is never constructed. These tests exist because key
composition and error classification are exactly the kind of logic that ships
broken when the only coverage is "it imports".
"""

from __future__ import annotations

from pathlib import Path

import pytest

from daylily_tapdb.backup.storage import S3StorageBackend


class FakeClientError(Exception):
    """Stands in for botocore.exceptions.ClientError."""

    def __init__(self, code: str, status: int = 400):
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class FakeS3Client:
    def __init__(self, *, objects=None, head_error=None):
        self.objects: dict[str, bytes] = dict(objects or {})
        self.head_error = head_error
        self.calls: list[tuple] = []

    def put_object(self, *, Bucket, Key, Body):
        self.calls.append(("put_object", Bucket, Key))
        self.objects[Key] = Body

    def get_object(self, *, Bucket, Key):
        self.calls.append(("get_object", Bucket, Key))

        class _Body:
            def __init__(self, data):
                self._data = data

            def read(self):
                return self._data

        return {"Body": _Body(self.objects[Key])}

    def upload_file(self, filename, bucket, key):
        self.calls.append(("upload_file", bucket, key))
        self.objects[key] = Path(filename).read_bytes()

    def download_file(self, bucket, key, filename):
        self.calls.append(("download_file", bucket, key))
        Path(filename).write_bytes(self.objects[key])

    def head_object(self, *, Bucket, Key):
        self.calls.append(("head_object", Bucket, Key))
        if self.head_error is not None:
            raise self.head_error
        if Key not in self.objects:
            raise FakeClientError("404", status=404)
        return {"ContentLength": len(self.objects[Key])}

    def delete_object(self, *, Bucket, Key):
        self.calls.append(("delete_object", Bucket, Key))
        self.objects.pop(Key, None)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self.objects
        calls = self.calls

        class _Paginator:
            def paginate(self, *, Bucket, Prefix):
                # Recorded so a test can assert the literal Prefix. ``startswith``
                # below is not a simplification -- S3 prefix matching really is a
                # string compare, which is the whole reason the delimiter matters.
                calls.append(("list_objects_v2", Bucket, Prefix))
                contents = [
                    {"Key": key, "Size": len(objects[key])}
                    for key in sorted(objects)
                    if key.startswith(Prefix)
                ]
                return [{"Contents": contents}]

        return _Paginator()


def _backend(prefix="tapdb/backups", **kwargs) -> S3StorageBackend:
    backend = S3StorageBackend("my-bucket", prefix)
    backend._client = FakeS3Client(**kwargs)
    return backend


def test_keys_are_composed_under_the_configured_prefix():
    backend = _backend()
    backend.put_bytes("acme/prod/full/one/manifest.json", b"{}")

    assert "tapdb/backups/acme/prod/full/one/manifest.json" in backend._client.objects


def test_keys_are_bare_when_no_prefix_is_configured():
    backend = _backend(prefix="")
    backend.put_bytes("acme/prod/full/one/manifest.json", b"{}")

    assert "acme/prod/full/one/manifest.json" in backend._client.objects


def test_bytes_roundtrip():
    backend = _backend()
    backend.put_bytes("a/manifest.json", b'{"x":1}')

    assert backend.get_bytes("a/manifest.json") == b'{"x":1}'


def test_file_roundtrip(tmp_path: Path):
    backend = _backend()
    source = tmp_path / "dump.bin"
    source.write_bytes(b"dump-bytes")

    backend.put_file("a/tapdb.dump", source)
    fetched = backend.get_file("a/tapdb.dump", tmp_path / "out" / "fetched.bin")

    assert fetched.read_bytes() == b"dump-bytes"


def test_list_keys_strips_the_configured_prefix():
    backend = _backend()
    backend.put_bytes("acme/prod/full/one/manifest.json", b"{}")
    backend.put_bytes("acme/prod/full/two/manifest.json", b"{}")

    assert backend.list_keys("acme/prod") == [
        "acme/prod/full/one/manifest.json",
        "acme/prod/full/two/manifest.json",
    ]


def test_exists_is_true_for_a_present_object():
    backend = _backend()
    backend.put_bytes("a/manifest.json", b"{}")

    assert backend.exists("a/manifest.json") is True


def test_exists_is_false_for_a_genuine_404():
    assert _backend().exists("a/missing.json") is False


@pytest.mark.parametrize(
    "error",
    [
        FakeClientError("ExpiredToken", status=400),
        FakeClientError("AccessDenied", status=403),
        FakeClientError("InvalidAccessKeyId", status=403),
    ],
)
def test_exists_reraises_auth_failures_instead_of_reporting_absent(error):
    # Reporting "absent" here would tell the service it is safe to write, which
    # could overwrite a real backup.
    backend = _backend(head_error=error)

    with pytest.raises(FakeClientError):
        backend.exists("a/manifest.json")


def test_delete_and_delete_prefix():
    backend = _backend()
    backend.put_bytes("acme/prod/full/one/manifest.json", b"{}")
    backend.put_bytes("acme/prod/full/one/tapdb.dump", b"x")

    backend.delete("acme/prod/full/one/tapdb.dump")
    assert backend.exists("acme/prod/full/one/tapdb.dump") is False

    backend.delete_prefix("acme/prod/full/one")
    assert backend.list_keys("acme/prod") == []


def test_describe_reports_the_uri_without_credentials():
    assert _backend().describe() == {
        "backend": "s3",
        "uri": "s3://my-bucket/tapdb/backups",
    }
    assert _backend(prefix="").describe()["uri"] == "s3://my-bucket"


def test_unsafe_keys_are_rejected_before_any_client_call():
    backend = _backend()

    with pytest.raises(ValueError):
        backend.put_bytes("../escape", b"x")
    assert backend._client.calls == []


def test_missing_boto3_produces_an_actionable_error(monkeypatch):
    import builtins

    backend = S3StorageBackend("my-bucket")
    real_import = builtins.__import__

    def _no_boto3(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError("No module named 'boto3'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_boto3)

    with pytest.raises(RuntimeError, match="requires boto3"):
        backend.put_bytes("a/manifest.json", b"{}")


# ---------------------------------------------------------------------------
# Prefix boundary
#
# S3 prefix matching is a plain string compare, so a listing prefix without a
# trailing delimiter leaks a sibling target's objects into this target's
# results. ``LocalStorageBackend`` walks a real directory tree and has a real
# boundary, so no local-fixture test can tell the two forms apart -- these are
# the only guard.
# ---------------------------------------------------------------------------


def test_listing_a_target_does_not_leak_a_sibling_targets_backups():
    """``acme/orders`` must not return ``acme/orders-staging``.

    Only the trailing segment is unbounded (``client_id`` is always followed by
    ``/``), so the exposure is exactly: another target under the same client
    whose ``database_name`` starts with this one's. An ordinary naming
    convention, not a pathological one.
    """
    backend = _backend()
    backend.put_bytes("acme/orders/full/one/manifest.json", b"{}")
    backend.put_bytes("acme/orders-staging/full/two/manifest.json", b"{}")
    backend.put_bytes("acme/orders-archive/full/three/manifest.json", b"{}")

    keys = backend.list_keys("acme/orders")

    assert keys == ["acme/orders/full/one/manifest.json"]


def test_listing_prefix_is_delimited():
    """Pin the literal ``Prefix`` argument, not just the filtered result.

    The behavioural test above passes only because the fake paginator models
    S3's string-compare faithfully. This one fails the moment the delimiter is
    dropped, regardless of how any fake filters.
    """
    backend = _backend()
    backend.list_keys("acme/orders")

    assert ("list_objects_v2", "my-bucket", "tapdb/backups/acme/orders/") in (
        backend._client.calls
    )


def test_bucket_level_prefix_is_also_delimited():
    """The same trap one level up: prefix ``tapdb`` must not match ``tapdb2``."""
    backend = _backend(prefix="tapdb")
    backend.list_keys()

    assert ("list_objects_v2", "my-bucket", "tapdb/") in backend._client.calls


def test_listing_an_unprefixed_bucket_stays_unbounded():
    """With no bucket prefix and no argument, list everything.

    ``""`` is already unbounded; ``"/"`` would match nothing at all, which would
    turn "list the whole bucket" into "find no backups" -- silently, since an
    empty listing is indistinguishable from an empty bucket.
    """
    backend = _backend(prefix="")
    backend.put_bytes("acme/orders/full/one/manifest.json", b"{}")

    assert backend.list_keys() == ["acme/orders/full/one/manifest.json"]
    assert ("list_objects_v2", "my-bucket", "") in backend._client.calls


def test_delete_prefix_inherits_the_boundary():
    """``delete_prefix`` iterates ``list_keys``, so it must not reach a sibling.

    Harmless for ``create_backup``'s cleanup path only because backup ids are
    fixed-length, so no id string-prefixes another -- but the guarantee should
    not rest on that.
    """
    backend = _backend()
    backend.put_bytes("acme/orders/full/one/manifest.json", b"{}")
    backend.put_bytes("acme/orders-staging/full/two/manifest.json", b"{}")

    backend.delete_prefix("acme/orders")

    assert "tapdb/backups/acme/orders/full/one/manifest.json" not in (
        backend._client.objects
    )
    assert "tapdb/backups/acme/orders-staging/full/two/manifest.json" in (
        backend._client.objects
    )


def test_list_sizes_reports_real_object_sizes():
    """`list_objects_v2` already returns Size; keeping it costs no extra call.

    Health compares these against the sizes a manifest records, so a backend
    that reported a constant would silently turn "the artifact is truncated"
    into "the artifact is fine".
    """
    backend = _backend()
    backend.put_bytes("acme/prod/full/one/manifest.json", b"{}")
    backend.put_bytes("acme/prod/full/one/tapdb.dump", b"0123456789")

    sizes = backend.list_sizes("acme/prod")

    assert sizes == {
        "acme/prod/full/one/manifest.json": 2,
        "acme/prod/full/one/tapdb.dump": 10,
    }


def test_list_sizes_respects_the_prefix_boundary():
    """Same delimiter guarantee as `list_keys` -- it shares the helper."""
    backend = _backend()
    backend.put_bytes("acme/orders/full/one/manifest.json", b"{}")
    backend.put_bytes("acme/orders-staging/full/two/manifest.json", b"{}")

    assert list(backend.list_sizes("acme/orders")) == [
        "acme/orders/full/one/manifest.json"
    ]
