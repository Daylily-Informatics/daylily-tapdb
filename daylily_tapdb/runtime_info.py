"""One sanitized runtime-information payload for every TapDB surface."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import shutil

# Fixed argv only; shell execution is never used.
import subprocess  # nosec B404
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import yaml

from daylily_tapdb import __version__
from daylily_tapdb.backup.engine import sanitized_libpq_environment
from daylily_tapdb.cli.context import TapdbContext
from daylily_tapdb.cli.db_config import get_config_path, get_db_config


def _git(repo: Path, *args: str) -> str | None:
    git = shutil.which("git")
    if not git:
        return None
    try:
        # Executable and argv are locally controlled.
        result = subprocess.run(  # nosec B603
            [git, "-C", str(repo), *args],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    return (result.stdout or "").strip()


def _package_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _safe_uri(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlsplit(raw)
    if not parsed.scheme:
        return None
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _bucket(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlsplit(value)
    return parsed.netloc if parsed.scheme == "s3" and parsed.netloc else None


def _ui_pid(path: Path) -> tuple[int | None, bool]:
    try:
        pid = int(path.read_text().strip())
        os.kill(pid, 0)
        return pid, True
    except (OSError, ValueError):
        return None, False


def _database_probe(cfg: dict[str, Any]) -> tuple[str, str | None]:
    psql = shutil.which("psql")
    if not psql:
        return "unknown", "psql is not installed"
    env = sanitized_libpq_environment()
    env["PGCONNECT_TIMEOUT"] = "3"
    if cfg.get("password"):
        env["PGPASSWORD"] = str(cfg["password"])
    if cfg.get("hostaddr"):
        env["PGHOSTADDR"] = str(cfg["hostaddr"])
    command = [
        psql,
        "-X",
        "-q",
        "-t",
        "-A",
        "-w",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        str(cfg["host"]),
        "-p",
        str(cfg["port"]),
        "-U",
        str(cfg["user"]),
        "-d",
        str(cfg["database"]),
        "-c",
        "select current_setting('server_version');",
    ]
    try:
        # Executable and argv are locally controlled.
        result = subprocess.run(  # nosec B603
            command,
            capture_output=True,
            text=True,
            env=env,
            timeout=5,
            check=False,
        )
    except Exception as exc:
        return "error", type(exc).__name__
    if result.returncode != 0:
        return "error", f"psql exit {result.returncode}"
    return "ok", (result.stdout or "").strip() or None


def build_runtime_info(
    *,
    config_path: str | Path | None = None,
    probe_database: bool = True,
    resolved_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a stable, secret-free status payload from one explicit config."""

    effective_path = get_config_path(config_path=config_path)
    cfg = (
        dict(resolved_config)
        if resolved_config is not None
        else get_db_config(config_path=effective_path)
    )
    if not effective_path.is_file():
        raise FileNotFoundError(f"TapDB config file does not exist: {effective_path}")
    raw = effective_path.read_bytes()
    root = yaml.safe_load(raw) or {}
    if not isinstance(root, dict):
        raise RuntimeError("TapDB config root must be a mapping")
    meta_value = root.get("meta")
    meta: dict[str, Any] = meta_value if isinstance(meta_value, dict) else {}
    backup_value = root.get("backup")
    backup: dict[str, Any] = backup_value if isinstance(backup_value, dict) else {}
    storage_value = backup.get("storage")
    storage_cfg: dict[str, Any] = (
        storage_value if isinstance(storage_value, dict) else {}
    )
    dag_value = root.get("dag_v2")
    dag_cfg: dict[str, Any] = dag_value if isinstance(dag_value, dict) else {}

    client_id = str(cfg["client_id"]).strip()
    database_name = str(cfg["database_name"]).strip()
    if not client_id or not database_name:
        raise RuntimeError(
            "resolved config requires exact non-empty client_id and database_name"
        )
    ctx = TapdbContext(
        client_id=client_id,
        database_name=database_name,
        explicit_config_path=effective_path,
    )
    ui_pid_path = ctx.ui_dir() / "ui.pid"
    repo = Path(__file__).resolve().parent.parent
    tag = _git(repo, "describe", "--tags", "--exact-match", "HEAD")
    commit = _git(repo, "rev-parse", "HEAD")
    branch = _git(repo, "branch", "--show-current")
    git_status = _git(repo, "status", "--porcelain")
    dirty = None if git_status is None else bool(git_status)
    probe_fields = ("host", "port", "user", "database")
    can_probe = all(str(cfg.get(field) or "").strip() for field in probe_fields)
    if probe_database and can_probe:
        db_status, db_version = _database_probe(cfg)
    elif probe_database:
        db_status, db_version = "unknown", "incomplete database target"
    else:
        db_status, db_version = "not_probed", None
    backup_uri = _safe_uri(storage_cfg.get("uri"))
    other_uris = [
        _safe_uri(value)
        for key, value in sorted(storage_cfg.items())
        if key.endswith("uri") and key != "uri"
    ]
    storage_uris = [item for item in [backup_uri, *other_uris] if item]
    ui_pid, ui_running = _ui_pid(ui_pid_path)
    service_id = str(dag_cfg.get("service_id") or "").strip() or None
    dag_status = str(dag_cfg.get("status") or "").strip() or (
        "configured" if service_id else "not_configured"
    )

    payload: dict[str, Any] = {
        "format": "tapdb.runtime-info/v1",
        "package": {
            "name": "daylily-tapdb",
            "version": __version__,
        },
        "python": {
            "version": sys.version.split()[0],
            "implementation": sys.implementation.name,
        },
        "meridian": {
            "package": "meridian-euid",
            "version": _package_version("meridian-euid"),
        },
        "git": {
            "tag": tag,
            "commit": commit,
            "branch": branch,
            "dirty": dirty,
        },
        "config": {
            "path": str(effective_path),
            "exists": True,
            "sha256": hashlib.sha256(raw).hexdigest() if raw else None,
            "config_version": meta.get("config_version"),
            "target": "explicit",
        },
        "database": {
            "engine_type": cfg.get("engine_type"),
            "host": cfg.get("host"),
            "port": cfg.get("port"),
            "database": cfg.get("database"),
            "schema_name": cfg.get("schema_name"),
            "status": db_status,
            "server_version": db_version,
        },
        "scope": {
            "client_id": client_id or None,
            "database_name": database_name or None,
            "domain_code": cfg.get("domain_code"),
            "owner_repo_name": cfg.get("owner_repo_name"),
        },
        "storage": {
            "uris": storage_uris,
            "s3_buckets": sorted(
                {bucket for uri in storage_uris if (bucket := _bucket(uri))}
            ),
            "aws_profile": cfg.get("aws_profile") or None,
            "region": cfg.get("region") or None,
        },
        "ui": {
            "status": "running" if ui_running else "stopped",
            "running": ui_running,
            "pid": ui_pid,
            "port": cfg.get("ui_port"),
        },
        "dag": {
            "status": dag_status,
            "service_id": service_id,
            "eligible": bool(service_id and dag_status in {"configured", "eligible"}),
        },
    }
    rendered = json.loads(json.dumps(payload, sort_keys=True))
    if not isinstance(rendered, dict):  # pragma: no cover - payload is fixed above
        raise RuntimeError("runtime information payload must be an object")
    return rendered


__all__ = ["build_runtime_info"]
