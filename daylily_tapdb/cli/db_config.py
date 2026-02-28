"""DB connection config loader for CLI.

Config search order:
1) TAPDB_CONFIG_PATH (explicit override)
2) ~/.config/tapdb/tapdb-config.yaml
3) ./config/tapdb-config.yaml (repo-local)

Notes:
- We intentionally support JSON content inside the .yaml file (valid YAML 1.2),
  so this works without adding a hard dependency on PyYAML.
- If PyYAML is installed, we will load real YAML.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "tapdb" / "tapdb-config.yaml"
REPO_CONFIG_PATH = Path.cwd() / "config" / "tapdb-config.yaml"


def get_config_path() -> Path:
    """Return the first config path to consider (existing preferred).

    This is used for derived paths (e.g., logs directory). If no config exists
    in any search location, this returns the default user config path.
    """
    paths = get_config_paths()
    for p in paths:
        if p.exists():
            return p
    return paths[0]


def get_config_paths() -> list[Path]:
    """Return the ordered list of config paths the CLI will search."""
    override = os.environ.get("TAPDB_CONFIG_PATH")
    if override:
        return [Path(override).expanduser()]
    return [DEFAULT_CONFIG_PATH, REPO_CONFIG_PATH]


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")

    try:
        import yaml  # type: ignore

        data = yaml.safe_load(raw)
    except ModuleNotFoundError:
        # JSON is valid YAML 1.2, so allow a JSON-formatted config file without PyYAML.
        data = json.loads(raw)

    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(
            f"Config root must be a mapping/dict, got {type(data).__name__}"
        )
    return data


def load_config() -> dict[str, Any]:
    """Load the config file if present; return {} if missing."""
    for path in get_config_paths():
        if path.exists():
            return _load_yaml_or_json(path)
    return {}


def get_db_config_for_env(env_name: str) -> dict[str, str]:
    """Resolve DB config for an environment name (dev/test/prod/aurora_*).

    Resolution order (highest precedence first):
    1) TAPDB_<ENV>_* environment variables
    2) PG* environment variables
    3) Config file (searched in order via get_config_paths())
    4) hard defaults

    Supported file shapes:
    - {"dev": {host, port, user, password, database}, "prod": {...}}
    - {"environments": {"dev": {...}, "prod": {...}}}

    The returned dict always contains an ``engine_type`` key:
    - ``"local"`` (default) for standard PostgreSQL environments
    - ``"aurora"`` when the config file sets ``engine_type: aurora``

    Aurora environments additionally return ``region``,
    ``cluster_identifier``, ``iam_auth``, and ``ssl`` keys.
    """

    env_key = env_name.lower()
    env_prefix = f"TAPDB_{env_key.upper()}_"

    file_cfg: dict[str, Any] = {}
    root = load_config()
    if "environments" in root and isinstance(root.get("environments"), dict):
        file_cfg = root["environments"].get(env_key, {}) or {}
    else:
        file_cfg = root.get(env_key, {}) or {}

    def _file_str(key: str) -> str | None:
        val = file_cfg.get(key)
        if val is None:
            return None
        return str(val)

    engine_type = os.environ.get(
        f"{env_prefix}ENGINE_TYPE", _file_str("engine_type") or "local"
    )

    cfg: dict[str, str] = {
        "engine_type": engine_type,
        "host": os.environ.get(
            f"{env_prefix}HOST",
            os.environ.get("PGHOST", _file_str("host") or "localhost"),
        ),
        "port": os.environ.get(
            f"{env_prefix}PORT", os.environ.get("PGPORT", _file_str("port") or "5432")
        ),
        "user": os.environ.get(
            f"{env_prefix}USER",
            os.environ.get(
                "PGUSER", _file_str("user") or os.environ.get("USER", "postgres")
            ),
        ),
        "password": os.environ.get(
            f"{env_prefix}PASSWORD",
            os.environ.get("PGPASSWORD", _file_str("password") or ""),
        ),
        "database": os.environ.get(
            f"{env_prefix}DATABASE", _file_str("database") or f"tapdb_{env_key}"
        ),
    }

    if engine_type == "aurora":
        cfg["region"] = os.environ.get(
            f"{env_prefix}REGION", _file_str("region") or "us-west-2"
        )
        cfg["cluster_identifier"] = os.environ.get(
            f"{env_prefix}CLUSTER_IDENTIFIER",
            _file_str("cluster_identifier") or "",
        )
        cfg["iam_auth"] = os.environ.get(
            f"{env_prefix}IAM_AUTH", _file_str("iam_auth") or "true"
        )
        cfg["ssl"] = os.environ.get(f"{env_prefix}SSL", _file_str("ssl") or "true")

    return cfg
