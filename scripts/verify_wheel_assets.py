"""Verify release metadata and the runtime assets shipped in one wheel."""

from __future__ import annotations

import argparse
import email
import zipfile
from pathlib import Path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-version", default=None)
    return parser


def main() -> None:
    args = _parser().parse_args()
    wheels = sorted(Path("dist").glob("*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one wheel in dist, found {len(wheels)}")
    with zipfile.ZipFile(wheels[0]) as wheel:
        names = set(wheel.namelist())
        metadata_names = [
            name for name in names if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise SystemExit(
                f"expected one wheel METADATA file, found {len(metadata_names)}"
            )
        metadata = email.message_from_bytes(wheel.read(metadata_names[0]))

    if metadata.get("Name") != "daylily-tapdb":
        raise SystemExit(f"unexpected package name: {metadata.get('Name')!r}")
    version = str(metadata.get("Version") or "")
    if args.expected_version and version != args.expected_version:
        raise SystemExit(
            f"wheel version {version!r} does not match {args.expected_version!r}"
        )
    requirements = metadata.get_all("Requires-Dist") or []
    if "meridian-euid==0.4.8" not in requirements:
        raise SystemExit("wheel does not require exact meridian-euid==0.4.8")

    required = (
        "daylily_tapdb/advisory_locks.py",
        "daylily_tapdb/migration_identity.py",
        "daylily_tapdb/runtime_info.py",
        "daylily_tapdb/security_context.py",
        "daylily_tapdb/services/object_operations.py",
        "daylily_tapdb/templates/repository.py",
        "daylily_tapdb/web/dag_v2.py",
        "docs/consumer-discoverability-guide.md",
        "schema/tapdb_schema.sql",
        "schema/rls.sql",
        "schema/migrations/20260902_010000_natural_identity_and_owner_uniqueness.sql",
        "schema/migrations/20260902_010100_legacy_outbox_message_conversion.sql",
        "schema/migrations/20260902_020000_force_rls_and_audit_attribution.sql",
    )
    missing = [
        item for item in required if not any(name.endswith(item) for name in names)
    ]
    if missing:
        raise SystemExit("wheel missing: " + ", ".join(missing))
    print(f"verified {wheels[0].name}: version={version}, meridian-euid==0.4.8")


if __name__ == "__main__":
    main()
