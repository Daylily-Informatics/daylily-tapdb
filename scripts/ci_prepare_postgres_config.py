"""Create the explicit PostgreSQL runtime configuration used by CI."""

from __future__ import annotations

import json
import os
from pathlib import Path

import yaml


def main() -> None:
    config_path = Path(os.environ["TAPDB_TEST_CONFIG"]).resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    domain_registry = config_path.parent / "domain_code_registry.json"
    prefix_registry = config_path.parent / "prefix_ownership_registry.json"
    domain_registry.write_text(
        json.dumps({"version": "0.4.0", "domains": {"Z": {"name": "ci"}}}) + "\n",
        encoding="utf-8",
    )
    prefix_registry.write_text(
        json.dumps(
            {
                "version": "0.4.0",
                "ownership": {
                    "Z": {
                        prefix: {"issuer_app_code": "daylily-tapdb"}
                        for prefix in (
                            "ADT",
                            "EDG",
                            "GSE",
                            "GVR",
                            "MSG",
                            "SYS",
                            "TPX",
                            "XRF",
                        )
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    example_path = (
        Path(__file__).resolve().parents[1] / "config" / "tapdb-config-example.yaml"
    )
    payload = yaml.safe_load(example_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("example TAPDB config root must be a mapping")
    meta = payload.get("meta")
    target = payload.get("target")
    if not isinstance(meta, dict) or not isinstance(target, dict):
        raise RuntimeError("example TAPDB config must contain meta and target mappings")
    meta.update(
        {
            "config_version": 4,
            "client_id": "ci",
            "database_name": "release",
            "owner_repo_name": "daylily-tapdb",
            "domain_registry_path": str(domain_registry),
            "prefix_ownership_registry_path": str(prefix_registry),
        }
    )
    target.update(
        {
            "engine_type": "local",
            "host": "localhost",
            "port": 5432,
            "ui_port": 18911,
            "domain_code": "Z",
            "user": "tapdb_runtime",
            "password": "runtime-password",
            "tenant_id": "",
            "allow_global_claims": False,
            "operator": {
                "user": "tapdb_operator",
                "password": "operator-password",
                "secret_arn": "",
                "iam_auth": False,
            },
            "database": "tapdb",
            "schema_name": "tapdb_ci_release",
        }
    )
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    config_path.chmod(0o600)


if __name__ == "__main__":
    main()
