"""Manifest canonical serialization, checksums, and secret hygiene."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from daylily_tapdb.backup.manifest import (
    AssetRef,
    BackupManifest,
    SequenceState,
    assert_credential_free_uri,
    assert_no_secrets,
    canonical_bytes,
    canonical_json,
    find_credential_uris,
    find_secret_paths,
    payload_checksum,
    sha256_file,
    sha256_hex,
    sign_manifest,
    signable_payload,
    signature_scheme,
    verify_manifest_signature,
)


def _manifest() -> BackupManifest:
    return BackupManifest(
        backup_id="full-20260727T000000Z-abc123",
        backup_class="full",
        target_identity={
            "client_id": "acme",
            "database_name": "prod",
            "schema_name": "tapdb_prod",
            "target_label": "acme/prod/tapdb_prod@tapdb",
        },
        row_counts={"generic_instance": 12, "audit_log": 40},
        sequences=[
            SequenceState(name="wx_instance_seq", last_value=17, is_called=True),
        ],
        included_assets=[
            AssetRef(name="tapdb.dump", bytes=1024, sha256="ab" * 32),
        ],
        # Non-default on purpose. ``to_payload`` and ``from_payload`` are two
        # hand-written enumerations, and the round-trip test compares
        # ``to_payload()`` against ``to_payload()`` -- so a field added to one
        # and forgotten in the other still round-trips *if its default is
        # empty*. Only a non-default value makes the omission visible.
        provenance={
            "created_by": "restore",
            "restored_backup_id": "full-20260726T000000Z-def456",
        },
    )


def test_canonical_json_is_sorted_and_stable():
    payload = {"b": 1, "a": {"d": 2, "c": 3}}
    text = canonical_json(payload)

    assert text == json.dumps(payload, indent=2, sort_keys=True)
    assert text.index('"a"') < text.index('"b"')
    # Key order in the source dict must not change the bytes.
    assert canonical_bytes(payload) == canonical_bytes({"a": {"c": 3, "d": 2}, "b": 1})


def test_manifest_checksum_matches_written_bytes():
    manifest = _manifest()

    assert manifest.checksum() == sha256_hex(manifest.to_bytes())
    assert manifest.checksum() == payload_checksum(manifest.to_payload())


def test_manifest_roundtrips_through_payload():
    manifest = _manifest()
    restored = BackupManifest.from_bytes(manifest.to_bytes())

    assert restored.to_payload() == manifest.to_payload()
    assert restored.checksum() == manifest.checksum()
    assert restored.sequences[0].last_value == 17
    assert restored.asset("tapdb.dump") is not None
    assert restored.asset("missing.dump") is None


def test_editing_a_manifest_changes_its_checksum():
    manifest = _manifest()
    before = manifest.checksum()
    manifest.row_counts["generic_instance"] = 13

    assert manifest.checksum() != before


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({"db_password": "hunter2"}, ["db_password"]),
        ({"target": {"secret_arn": "arn:aws:x"}}, ["target.secret_arn"]),
        ({"a": [{"api_token": "t"}]}, ["a[0].api_token"]),
        ({"creds": {"credential_id": 1}}, ["creds.credential_id"]),
        ({"env": {"PGPASSWORD": "x"}}, ["env.PGPASSWORD"]),
    ],
)
def test_find_secret_paths_flags_secret_shaped_keys(payload, expected):
    assert find_secret_paths(payload) == expected


def test_key_matching_ignores_values():
    # A hostname or ARN as a *value* is legitimate manifest content.
    payload = {"host": "db.internal", "kms_key_arn": "arn:aws:kms:...:key/abc"}

    assert find_secret_paths(payload) == []


def test_credential_bearing_uri_values_are_caught():
    # The one deliberate exception to key-only matching: a URI carrying
    # userinfo is unambiguously a secret, and it hides under innocuous keys.
    payload = {
        "storage": {"uri": "s3://AKIAEXAMPLE:secret123@bucket/prefix"},
        "receipt_mirror": {"uri": "s3://audit-bucket/receipts"},
    }

    assert find_credential_uris(payload) == ["storage.uri"]


def test_clean_uri_values_are_not_flagged():
    payload = {
        "storage": {"uri": "file:///var/lib/tapdb/backups"},
        "mirror": {"uri": "s3://bucket/prefix"},
        "identity": {"arn": "arn:aws:rds:us-west-2:1:cluster/x"},
        "host": "db.internal:5432",
    }

    assert find_credential_uris(payload) == []


def test_manifest_refuses_to_render_a_credential_bearing_uri():
    manifest = _manifest()
    manifest.storage = {"uri": "s3://AKIAEXAMPLE:secret123@bucket/prefix"}

    with pytest.raises(ValueError, match="credential-bearing URIs"):
        manifest.to_payload()


def test_asset_ref_from_file_records_size_and_checksum(tmp_path: Path):
    artifact = tmp_path / "tapdb.dump"
    artifact.write_bytes(b"dump-bytes")

    ref = AssetRef.from_file(artifact, content_type="application/x-pgdump")

    assert ref.name == "tapdb.dump"
    assert ref.bytes == 10
    assert ref.sha256 == sha256_hex(b"dump-bytes")
    assert ref.content_type == "application/x-pgdump"


def test_asset_ref_from_file_accepts_an_override_name(tmp_path: Path):
    artifact = tmp_path / "staged.tmp"
    artifact.write_bytes(b"x")

    assert AssetRef.from_file(artifact, name="tapdb.dump").name == "tapdb.dump"


def test_to_payload_refuses_secret_bearing_manifest():
    manifest = _manifest()
    manifest.target_identity["secret_arn"] = "arn:aws:secretsmanager:x"

    with pytest.raises(ValueError, match="secret_arn"):
        manifest.to_payload()


def test_assert_no_secrets_accepts_a_clean_payload():
    assert_no_secrets(_manifest().to_payload()) is None


@pytest.mark.parametrize(
    "uri",
    [
        "s3://user:pass@bucket/prefix",
        "postgres://admin:hunter2@host/db",
        "file://someone@host/path",
    ],
)
def test_credential_bearing_uris_are_rejected(uri):
    with pytest.raises(ValueError, match="must not embed credentials"):
        assert_credential_free_uri(uri)


@pytest.mark.parametrize(
    "uri",
    ["", "s3://bucket/prefix", "file:///var/lib/tapdb/backups", "/plain/path"],
)
def test_credential_free_uris_are_accepted(uri):
    assert assert_credential_free_uri(uri) == uri.strip()


def test_sha256_file_streams_large_content(tmp_path: Path):
    target = tmp_path / "artifact.bin"
    payload = b"x" * (3 * 1024 * 1024 + 7)
    target.write_bytes(payload)

    assert sha256_file(target, chunk_size=4096) == sha256_hex(payload)


def test_signature_stub_is_sha256_and_verifies():
    payload = _manifest().to_payload()
    signature = sign_manifest(payload, mode="none")

    assert signature["algorithm"] == "none"
    # A signature covers the payload *minus itself*. Asserting against the
    # bare payload passed only because it described a shape that never exists
    # on disk: the producer writes the signature back into the manifest, so a
    # stored manifest could never re-hash to that value.
    assert signature["value"] == payload_checksum(signable_payload(payload))
    assert verify_manifest_signature(payload, signature) is True


def test_signature_verification_fails_on_altered_payload():
    payload = _manifest().to_payload()
    signature = sign_manifest(payload, mode="none")
    payload["row_counts"]["generic_instance"] = 999

    assert verify_manifest_signature(payload, signature) is False


def test_kms_signing_is_designed_but_not_implemented():
    with pytest.raises(NotImplementedError, match="KMS"):
        sign_manifest(_manifest().to_payload(), mode="kms")


def test_unknown_signing_mode_is_rejected():
    with pytest.raises(ValueError, match="Unsupported"):
        sign_manifest(_manifest().to_payload(), mode="pgp")


# ---------------------------------------------------------------------------
# provenance
# ---------------------------------------------------------------------------


def test_provenance_is_covered_by_the_signature():
    """Set provenance *before* signing, or every safety backup fails verify.

    ``sign_manifest`` hashes the payload as it exists at that instant. Assigning
    ``manifest.provenance`` afterwards -- the natural shape when the safety
    backup is created inside the restore flow -- puts the field in the stored
    bytes but outside the signature, so ``signature_scheme`` returns "invalid",
    ``backup verify`` fails, and restore preflight refuses the one backup that
    is the last copy of production.
    """
    manifest = _manifest()
    manifest.signature = sign_manifest(manifest.to_payload(), mode="none")

    assert signature_scheme(manifest.to_payload(), manifest.signature) == "valid"

    # The failure mode itself: mutate after signing and the scheme flips.
    manifest.provenance = {"created_by": "operator"}
    assert signature_scheme(manifest.to_payload(), manifest.signature) == "invalid"


def test_manifests_written_before_provenance_existed_still_load():
    """Absence is a third state, and must not read as "routine"."""
    payload = _manifest().to_payload()
    del payload["provenance"]

    restored = BackupManifest.from_payload(payload)

    assert restored.provenance == {}
    assert restored.provenance.get("created_by") is None
