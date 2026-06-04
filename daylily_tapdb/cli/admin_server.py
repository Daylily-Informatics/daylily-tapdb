"""Explicit-context TAPDB admin server runner."""

from __future__ import annotations

import argparse
import importlib
import json
import os
from pathlib import Path
from typing import Any

from daylily_tapdb.cli.context import resolve_context, set_cli_context

_CONTEXT_FILENAME = "context.json"
_TLS_MODES = {"http", "https"}
_HTTP_CONTEXT_ENV = "TAPDB_ADMIN_HTTP_CONTEXT"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m daylily_tapdb.cli.admin_server",
        description="Start the TAPDB admin UI with explicit TapDB context.",
    )
    parser.add_argument("--config", required=True, help="TapDB config file path")
    parser.add_argument("--host", required=True, help="UI bind host")
    parser.add_argument("--port", required=True, type=int, help="UI bind port")
    parser.add_argument(
        "--tls-mode",
        choices=sorted(_TLS_MODES),
        default=None,
        help="Explicit admin TLS mode; or set TAPDB_ADMIN_TLS_MODE=http|https",
    )
    parser.add_argument("--ssl-keyfile", default=None, help="TLS key file")
    parser.add_argument("--ssl-certfile", default=None, help="TLS cert file")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    return parser


def _context_file_path() -> Path:
    return Path.cwd() / _CONTEXT_FILENAME


def _write_context_file(
    *,
    config_path: str,
    host: str,
    port: int,
    tls_mode: str,
) -> Path:
    ctx = resolve_context(
        require_keys=True,
        config_path=config_path,
    )
    ui_dir = ctx.ui_dir()
    ui_dir.mkdir(parents=True, exist_ok=True)
    context_file = ui_dir / _CONTEXT_FILENAME
    context_file.write_text(
        json.dumps(
            {
                "config_path": str(Path(config_path).expanduser().resolve()),
                "target": "explicit",
                "host": host,
                "port": port,
                "tls_mode": tls_mode,
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
            "Start the UI through `tapdb --config <path> ui start`."
        )
    raw = json.loads(context_file.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"Invalid TAPDB admin context file: {context_file}")
    return raw


def load_admin_app(*, config_path: str):
    """Build the TAPDB admin FastAPI app for an explicit config target."""
    set_cli_context(config_path=config_path)
    admin_main = importlib.import_module("admin.main")
    admin_main = importlib.reload(admin_main)
    admin_main.app.state.tapdb_admin_module = admin_main
    from daylily_tapdb.admin_health import install_tapdb_admin_health_routes

    install_tapdb_admin_health_routes(
        admin_main.app,
        config_path=config_path,
    )
    return admin_main.app


def _resolve_tls_mode(explicit_mode: str | None) -> str:
    raw = explicit_mode or os.environ.get("TAPDB_ADMIN_TLS_MODE")
    mode = str(raw or "").strip().lower()
    if not mode:
        raise RuntimeError(
            "TAPDB_ADMIN_TLS_MODE is required and must be one of: http, https."
        )
    if mode not in _TLS_MODES:
        raise RuntimeError(
            f"Invalid TAPDB_ADMIN_TLS_MODE {raw!r}; expected one of: http, https."
        )
    if mode == "http":
        http_context = str(os.environ.get(_HTTP_CONTEXT_ENV) or "").strip()
        if http_context != "local-compose":
            raise RuntimeError(
                "TAPDB_ADMIN_TLS_MODE=http is only allowed for local Compose. "
                f"Set {_HTTP_CONTEXT_ENV}=local-compose for that runtime."
            )
    return mode


def _uvicorn_tls_kwargs(
    *,
    tls_mode: str,
    ssl_keyfile: str | None,
    ssl_certfile: str | None,
) -> dict[str, str]:
    if tls_mode == "http":
        if ssl_keyfile or ssl_certfile:
            raise RuntimeError("HTTP admin mode must not include TLS key/cert files.")
        return {}

    raw_keyfile = str(ssl_keyfile or "").strip()
    raw_certfile = str(ssl_certfile or "").strip()
    if not raw_keyfile or not raw_certfile:
        raise RuntimeError(
            "HTTPS admin mode requires --ssl-keyfile and --ssl-certfile."
        )
    key_path = Path(raw_keyfile).expanduser()
    cert_path = Path(raw_certfile).expanduser()
    missing = [str(path) for path in (key_path, cert_path) if not path.exists()]
    if missing:
        raise RuntimeError(
            "HTTPS admin mode requires existing TLS file(s): " + ", ".join(missing)
        )
    return {"ssl_keyfile": str(key_path), "ssl_certfile": str(cert_path)}


def run_admin_server(args: Any) -> None:
    tls_mode = _resolve_tls_mode(getattr(args, "tls_mode", None))
    context_file = _write_context_file(
        config_path=args.config,
        host=args.host,
        port=args.port,
        tls_mode=tls_mode,
    )
    os.chdir(context_file.parent)
    set_cli_context(config_path=args.config)
    tls_kwargs = _uvicorn_tls_kwargs(
        tls_mode=tls_mode,
        ssl_keyfile=getattr(args, "ssl_keyfile", None),
        ssl_certfile=getattr(args, "ssl_certfile", None),
    )

    import uvicorn

    uvicorn.run(
        "daylily_tapdb.cli.admin_server:build_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
        **tls_kwargs,
    )


def main() -> None:
    args = _build_parser().parse_args()
    try:
        run_admin_server(args)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc


def build_app():
    context = _read_context_file()
    config_path = str(context.get("config_path") or "").strip()
    if not config_path:
        raise RuntimeError("TapDB admin context file is incomplete.")
    from daylily_tapdb.web import create_tapdb_web_app

    return create_tapdb_web_app(config_path=config_path)


if __name__ == "__main__":
    main()
