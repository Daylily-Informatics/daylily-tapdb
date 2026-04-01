"""Explicit-context TAPDB admin server runner."""

from __future__ import annotations

import argparse
import importlib
import json
import os
from pathlib import Path

from daylily_tapdb.cli.context import resolve_context, set_cli_context

_CONTEXT_FILENAME = "context.json"


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


def _context_file_path() -> Path:
    return Path.cwd() / _CONTEXT_FILENAME


def _write_context_file(*, config_path: str, env_name: str, host: str, port: int) -> Path:
    ctx = resolve_context(
        require_keys=True,
        config_path=config_path,
        env_name=env_name,
        allow_namespace_fallback=False,
    )
    ui_dir = ctx.ui_dir(env_name)
    ui_dir.mkdir(parents=True, exist_ok=True)
    context_file = ui_dir / _CONTEXT_FILENAME
    context_file.write_text(
        json.dumps(
            {
                "config_path": str(Path(config_path).expanduser().resolve()),
                "env_name": env_name,
                "host": host,
                "port": port,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return context_file


def _read_context_file() -> dict[str, object]:
    context_file = _context_file_path()
    if not context_file.exists():
        raise RuntimeError(
            "Explicit TapDB admin context is missing. "
            "Start the UI through `tapdb --config <path> --env <name> ui start`."
        )
    raw = json.loads(context_file.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"Invalid TAPDB admin context file: {context_file}")
    return raw


def load_admin_app(*, config_path: str, env_name: str):
    """Build the TAPDB admin FastAPI app for an explicit config/env."""
    set_cli_context(config_path=config_path, env_name=env_name)
    admin_main = importlib.import_module("admin.main")
    admin_main = importlib.reload(admin_main)
    return admin_main.app


def main() -> None:
    args = _build_parser().parse_args()
    context_file = _write_context_file(
        config_path=args.config,
        env_name=args.env,
        host=args.host,
        port=args.port,
    )
    os.chdir(context_file.parent)
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
    context = _read_context_file()
    config_path = str(context.get("config_path") or "").strip()
    env_name = str(context.get("env_name") or "").strip()
    if not config_path or not env_name:
        raise RuntimeError("TapDB admin context file is incomplete.")
    return load_admin_app(config_path=config_path, env_name=env_name)


if __name__ == "__main__":
    main()
