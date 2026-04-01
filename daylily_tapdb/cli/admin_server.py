"""Explicit-context TAPDB admin server runner.

This wrapper exists so ``tapdb ui start`` can start the admin UI in a child
process without relying on ambient TAPDB_* shell state.
"""

from __future__ import annotations

import argparse
import os

from daylily_tapdb.cli.context import set_cli_context

_CONFIG_ENV = "DAYLILY_TAPDB_ACTIVE_CONFIG"
_ENV_ENV = "DAYLILY_TAPDB_ACTIVE_ENV"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m daylily_tapdb.cli.admin_server",
        description="Start the TAPDB admin UI with explicit TapDB context.",
    )
    parser.add_argument("--config", required=True, help="TapDB config file path")
    parser.add_argument("--env", required=True, help="TapDB env name")
    parser.add_argument("--host", required=True, help="UI bind host")
    parser.add_argument("--port", required=True, type=int, help="UI bind port")
    parser.add_argument("--ssl-keyfile", required=True, help="TLS key file")
    parser.add_argument("--ssl-certfile", required=True, help="TLS cert file")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    os.environ[_CONFIG_ENV] = args.config
    os.environ[_ENV_ENV] = args.env
    set_cli_context(config_path=args.config, env_name=args.env)

    import uvicorn

    uvicorn.run(
        "daylily_tapdb.cli.admin_server:build_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
        ssl_keyfile=args.ssl_keyfile,
        ssl_certfile=args.ssl_certfile,
    )


def build_app():
    config_path = (os.environ.get(_CONFIG_ENV) or "").strip()
    env_name = (os.environ.get(_ENV_ENV) or "").strip()
    if not config_path or not env_name:
        raise RuntimeError(
            "Explicit TapDB admin context is missing. "
            "Start the UI through `tapdb --config <path> --env <name> ui start`."
        )
    set_cli_context(config_path=config_path, env_name=env_name)
    from admin.main import app

    return app


if __name__ == "__main__":
    main()
