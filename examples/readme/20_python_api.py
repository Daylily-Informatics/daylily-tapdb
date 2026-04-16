#!/usr/bin/env python3
"""Minimal TAPDB public-API example for the README."""

from __future__ import annotations

import json
import os
from pathlib import Path

from daylily_tapdb import InstanceFactory, TAPDBConnection, TemplateManager
from daylily_tapdb.cli.db_config import get_db_config_for_env

TEMPLATE_CODE = "MSG/message/webhook_event/1.0/"


def _default_config_path() -> Path:
    explicit = os.environ.get("TAPDB_DOCS_CONFIG")
    if explicit:
        return Path(explicit).expanduser().resolve()

    workdir = Path(os.environ.get("TAPDB_DOCS_WORKDIR", "~/.tapdb-docs")).expanduser()
    client_id = os.environ.get("TAPDB_DOCS_CLIENT_ID", "docs")
    database_name = os.environ.get("TAPDB_DOCS_DATABASE_NAME", "demo")
    return (
        workdir
        / ".config"
        / "tapdb"
        / client_id
        / database_name
        / "tapdb-config.yaml"
    ).resolve()


def main() -> None:
    config_path = _default_config_path()
    env_name = os.environ.get("TAPDB_DOCS_ENV", "dev")
    instance_name = os.environ.get(
        "TAPDB_DOCS_INSTANCE_NAME",
        "README Webhook Event",
    )

    if not config_path.exists():
        raise SystemExit(
            f"Config not found at {config_path}. "
            "Run examples/readme/10_bootstrap_local.sh first or set TAPDB_DOCS_CONFIG."
        )

    cfg = get_db_config_for_env(env_name, config_path=config_path)
    domain_code = os.environ.get("TAPDB_DOCS_DOMAIN_CODE", str(cfg["domain_code"]))
    owner_repo_name = os.environ.get(
        "TAPDB_DOCS_OWNER_REPO_NAME",
        str(cfg["owner_repo_name"]),
    )
    conn = TAPDBConnection(
        db_hostname=f"{cfg['host']}:{cfg['port']}",
        db_user=cfg["user"],
        db_pass=cfg["password"],
        db_name=cfg["database"],
        app_username="tapdb_readme_example",
        domain_code=domain_code,
        owner_repo_name=owner_repo_name,
    )

    template_manager = TemplateManager()
    factory = InstanceFactory(template_manager, domain_code=domain_code)

    with conn:
        with conn.session_scope(commit=True) as session:
            template = template_manager.get_template(
                session,
                TEMPLATE_CODE,
                domain_code=domain_code,
            )
            if template is None:
                raise SystemExit(f"Template not found: {TEMPLATE_CODE}")

            instance = factory.create_instance(
                session,
                template_code=TEMPLATE_CODE,
                name=instance_name,
                properties={
                    "event_type": "docs.readme.example",
                    "aggregate_euid": template.euid,
                    "payload": {"source": "README Python API example"},
                    "metadata": {"owner_repo_name": owner_repo_name},
                },
                create_children=False,
            )

            payload = {
                "config_path": str(config_path),
                "env": env_name,
                "template_code": TEMPLATE_CODE,
                "template_euid": template.euid,
                "instance_uid": int(instance.uid),
                "instance_euid": instance.euid,
                "instance_name": instance.name,
                "domain_code": instance.domain_code,
                "owner_repo_name": owner_repo_name,
                "issuer_app_code": instance.issuer_app_code,
            }

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
