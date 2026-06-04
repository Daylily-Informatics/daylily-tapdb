"""Foreground container entrypoint for TapDB admin."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping


def _required_env(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for TapDB admin container startup.")
    return value


def _required_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"TAPDB_ADMIN_PORT must be an integer, got {value!r}."
        ) from exc
    if port <= 0 or port > 65535:
        raise RuntimeError(f"TAPDB_ADMIN_PORT must be between 1 and 65535, got {port}.")
    return port


def _tls_mode(env: Mapping[str, str]) -> str:
    mode = _required_env(env, "TAPDB_ADMIN_TLS_MODE").lower()
    if mode not in {"http", "https"}:
        raise RuntimeError("TAPDB_ADMIN_TLS_MODE must be one of: http, https.")
    if mode == "http":
        http_context = str(env.get("TAPDB_ADMIN_HTTP_CONTEXT") or "").strip()
        if http_context != "local-compose":
            raise RuntimeError(
                "TAPDB_ADMIN_TLS_MODE=http is only allowed for local Compose. "
                "Set TAPDB_ADMIN_HTTP_CONTEXT=local-compose for that runtime."
            )
    return mode


def build_admin_server_argv(env: Mapping[str, str] | None = None) -> list[str]:
    """Build the explicit admin-server argv from container environment."""

    source = os.environ if env is None else env
    config_path = _required_env(source, "TAPDB_CONFIG_PATH")
    host = _required_env(source, "TAPDB_ADMIN_HOST")
    port = _required_port(_required_env(source, "TAPDB_ADMIN_PORT"))
    tls_mode = _tls_mode(source)

    argv = [
        sys.executable,
        "-m",
        "daylily_tapdb.cli.admin_server",
        "--config",
        config_path,
        "--host",
        host,
        "--port",
        str(port),
        "--tls-mode",
        tls_mode,
    ]
    if tls_mode == "https":
        argv.extend(
            [
                "--ssl-keyfile",
                _required_env(source, "TAPDB_ADMIN_TLS_KEYFILE"),
                "--ssl-certfile",
                _required_env(source, "TAPDB_ADMIN_TLS_CERTFILE"),
            ]
        )
    return argv


def main() -> None:
    argv = build_admin_server_argv()
    # Re-exec uses the current Python executable with fixed admin-server argv.
    os.execv(sys.executable, argv)  # nosec B606


if __name__ == "__main__":
    main()
