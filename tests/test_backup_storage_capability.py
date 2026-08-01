"""``deletion_capability`` — probed against botocore's real S3 service model.

Deliberately **not** written against ``FakeS3Client``. Everything else in
``test_backup_storage_s3.py`` uses that fake, and it is right for key
composition, but these assertions are about an AWS API contract nobody in this
repo controls, and the response shapes are exactly where a hand-written fake
drifts:

* ``get_bucket_versioning`` returns ``{}`` with **no** ``Status`` key for a
  bucket that was never versioned — not ``{"Status": "Disabled"}``. A fake that
  always supplies ``Status`` hides a ``KeyError`` and, worse, hides the fact
  that ``None`` is a distinct state.
* ``get_object_lock_configuration`` **raises** for a bucket without Object Lock
  rather than returning a dict, and the error code is
  ``ObjectLockConfigurationNotFoundError``. A fake that returns a dict never
  exercises the branch, and getting it backwards turns "Object Lock present"
  into an unnoticed pass.

``Stubber`` validates both the request parameters and the response shape
against botocore's own model, so a response this file invents that S3 could not
actually produce fails here rather than in production.

The consequence of getting any of this wrong is that ``backup prune`` deletes
objects believing it frees space when it does not, or proceeds against a bucket
whose retention policy is declared outside TapDB.
"""

from __future__ import annotations

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from daylily_tapdb.backup.storage import S3StorageBackend


@pytest.fixture
def backend():
    """A backend wired to a stubbed client built from the real S3 model."""
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    stub = Stubber(client)
    storage = S3StorageBackend("my-bucket", "tapdb")
    storage._client = client
    stub.activate()
    yield storage, stub
    stub.deactivate()


def _client_error(code: str, status: int = 400) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "GetObjectLockConfiguration",
    )


def test_a_plain_bucket_reclaims(backend):
    """No versioning, no Object Lock: a delete frees the bytes."""
    storage, stub = backend
    # A never-versioned bucket really does return an empty document.
    stub.add_response("get_bucket_versioning", {}, {"Bucket": "my-bucket"})
    stub.add_client_error(
        "get_object_lock_configuration",
        service_error_code="ObjectLockConfigurationNotFoundError",
    )

    capability = storage.deletion_capability()

    assert capability["reclaims"] is True
    assert capability["versioning"] is None
    assert capability["object_lock"] is False


def test_versioning_enabled_does_not_reclaim(backend):
    storage, stub = backend
    stub.add_response(
        "get_bucket_versioning", {"Status": "Enabled"}, {"Bucket": "my-bucket"}
    )
    stub.add_client_error(
        "get_object_lock_configuration",
        service_error_code="ObjectLockConfigurationNotFoundError",
    )

    capability = storage.deletion_capability()

    assert capability["reclaims"] is False
    assert capability["versioning"] == "Enabled"
    assert "NoncurrentVersionExpiration" in capability["reason"]


def test_versioning_suspended_also_does_not_reclaim(backend):
    """``Suspended`` is not ``Disabled``, and the difference is not cosmetic.

    Under ``Suspended`` a DELETE still writes a null-version delete marker, and
    every non-null version written while versioning was on is retained. So it
    reclaims nothing for exactly the objects most likely to matter. Checking
    ``Status == "Enabled"`` is the natural implementation and it is wrong.
    """
    storage, stub = backend
    stub.add_response(
        "get_bucket_versioning", {"Status": "Suspended"}, {"Bucket": "my-bucket"}
    )
    stub.add_client_error(
        "get_object_lock_configuration",
        service_error_code="ObjectLockConfigurationNotFoundError",
    )

    capability = storage.deletion_capability()

    assert capability["reclaims"] is False
    assert capability["versioning"] == "Suspended"


def test_object_lock_configured_does_not_reclaim(backend):
    storage, stub = backend
    stub.add_response("get_bucket_versioning", {}, {"Bucket": "my-bucket"})
    stub.add_response(
        "get_object_lock_configuration",
        {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}},
        {"Bucket": "my-bucket"},
    )

    capability = storage.deletion_capability()

    assert capability["reclaims"] is False
    assert capability["object_lock"] is True
    assert "Object Lock" in capability["reason"]


def test_a_denied_object_lock_probe_is_unknown_not_absent(backend):
    """The distinction the whole design rests on.

    Without ``s3:GetBucketObjectLockConfiguration`` the probe fails. Mapping
    that onto ``object_lock: False`` is the obvious shortcut and it converts a
    no-escape refusal into a silent pass on a bucket that may well be locked.
    Unknown must stay unknown, so the caller can refuse or demand an explicit
    override.
    """
    storage, stub = backend
    stub.add_response("get_bucket_versioning", {}, {"Bucket": "my-bucket"})
    stub.add_client_error(
        "get_object_lock_configuration",
        service_error_code="AccessDenied",
        http_status_code=403,
    )

    capability = storage.deletion_capability()

    assert capability["reclaims"] is None
    assert capability["object_lock"] is None
    assert "denied" in capability["reason"]


def test_a_denied_versioning_probe_is_unknown(backend):
    """Denied versioning must not degrade to "unversioned".

    The second stubbed response is the point of the test. Without it, code that
    swallowed the denial and carried on would still land in the unknown branch
    -- because the *next* probe would fail for want of a stub -- and the test
    would pass on broken code. Queuing a successful Object Lock response means
    a swallowed denial reaches `reclaims: True`, which is the wrong answer this
    is meant to catch.
    """
    storage, stub = backend
    stub.add_client_error(
        "get_bucket_versioning",
        service_error_code="AccessDenied",
        http_status_code=403,
    )
    stub.add_client_error(
        "get_object_lock_configuration",
        service_error_code="ObjectLockConfigurationNotFoundError",
    )

    capability = storage.deletion_capability()

    assert capability["reclaims"] is None
    assert capability["versioning"] is None


def test_an_unexpected_probe_error_is_unknown_not_reclaimable(backend):
    """Fail closed. Any unrecognised failure must not read as "safe to delete"."""
    storage, stub = backend
    stub.add_response("get_bucket_versioning", {}, {"Bucket": "my-bucket"})
    stub.add_client_error(
        "get_object_lock_configuration", service_error_code="InternalError"
    )

    capability = storage.deletion_capability()

    assert capability["reclaims"] is None


def test_local_storage_reclaims(tmp_path):
    from daylily_tapdb.backup.storage import LocalStorageBackend

    capability = LocalStorageBackend(tmp_path).deletion_capability()

    assert capability["reclaims"] is True
    assert capability["object_lock"] is False


def test_the_probe_is_not_called_by_describe():
    """``describe()`` is embedded in every manifest and runs on every plan.

    Two API calls and two IAM permissions on the *create* path to serve a
    *delete* path concern is the wrong trade -- and it would make taking a
    backup fail on a bucket whose Object Lock configuration is not readable.

    Asserted by making the probe explosive rather than by starving a stub:
    ``deletion_capability`` catches broad exceptions by design, so an unstubbed
    call would be swallowed and ``describe()`` would look innocent.
    """
    storage = S3StorageBackend("my-bucket", "tapdb")

    def _explode():
        raise AssertionError("describe() must not probe deletion capability")

    storage.deletion_capability = _explode  # type: ignore[method-assign]

    assert storage.describe() == {"backend": "s3", "uri": "s3://my-bucket/tapdb"}
