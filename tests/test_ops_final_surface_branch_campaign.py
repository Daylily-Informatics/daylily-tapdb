"""Behavior coverage for small runtime, bridge, and snapshot surfaces."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import daylily_tapdb.backup.snapshots as snapshots
import daylily_tapdb.container_entry as container_entry
import daylily_tapdb.web.bridge as bridge


def test_snapshot_client_and_json_coercion(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    calls: list[tuple[str, str]] = []
    fake_boto3 = SimpleNamespace(
        client=lambda name, region_name: calls.append((name, region_name)) or sentinel
    )
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    assert snapshots._rds_client("us-east-1") is sentinel
    assert calls == [("rds", "us-east-1")]

    value = {
        "time": datetime(2026, 1, 1, tzinfo=UTC),
        "items": [None, 1, object()],
    }
    converted = snapshots._jsonable(value)
    assert converted["time"] == "2026-01-01T00:00:00+00:00"
    assert converted["items"][0:2] == [None, 1]
    assert isinstance(converted["items"][2], str)


def test_snapshot_cluster_identifier_config_fallback() -> None:
    assert (
        snapshots._cluster_identifier(
            {"cluster_identifier": " target-cluster "},
            {"provider_snapshots_cluster_identifier": ""},
        )
        == "target-cluster"
    )


def test_container_port_tls_and_environment_errors() -> None:
    with pytest.raises(RuntimeError, match="integer"):
        container_entry._required_port("abc")
    for value in ("0", "65536"):
        with pytest.raises(RuntimeError, match="between 1 and 65535"):
            container_entry._required_port(value)

    with pytest.raises(RuntimeError, match="one of"):
        container_entry._tls_mode({"TAPDB_ADMIN_TLS_MODE": "smtp"})
    with pytest.raises(RuntimeError, match="local Compose"):
        container_entry._tls_mode({"TAPDB_ADMIN_TLS_MODE": "http"})


def test_container_build_uses_os_environ_and_main_execs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = {
        "TAPDB_CONFIG_PATH": "/config/tapdb.yaml",
        "TAPDB_ADMIN_HOST": "127.0.0.1",
        "TAPDB_ADMIN_PORT": "8911",
        "TAPDB_ADMIN_TLS_MODE": "http",
        "TAPDB_ADMIN_HTTP_CONTEXT": "local-compose",
    }
    monkeypatch.setattr(container_entry.os, "environ", env)
    argv = container_entry.build_admin_server_argv()
    assert argv[-2:] == ["--tls-mode", "http"]

    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        container_entry.os,
        "execv",
        lambda executable, arguments: calls.append((executable, arguments)),
    )
    container_entry.main()
    assert calls == [(container_entry.sys.executable, argv)]


def test_bridge_url_and_user_normalization_branches() -> None:
    request = SimpleNamespace()
    assert bridge.resolve_bridge_url(None, request) == ""
    assert (
        bridge.resolve_bridge_url(lambda _request: " /dynamic ", request) == "/dynamic"
    )
    assert bridge.resolve_bridge_url(" /static ", request) == "/static"

    assert bridge.normalize_host_user(None) is None
    assert bridge.normalize_host_user({}) is None
    actual = bridge.normalize_host_user(
        {
            "username": " Alice ",
            "role": "owner",
            "name": "Alice Example",
            "is_active": 0,
            "require_password_change": 1,
        }
    )
    assert actual == {
        "uid": "alice",
        "username": "alice",
        "email": "alice",
        "display_name": "Alice Example",
        "role": "user",
        "is_active": False,
        "require_password_change": True,
    }


def test_bridge_shell_and_extra_context_filters() -> None:
    request = SimpleNamespace()
    inactive = bridge.resolve_host_shell(None, request)
    assert inactive["active"] is False

    host = bridge.TapdbHostBridge(
        service_name=" ",
        app_name=" ",
        home_url=lambda _request: "/host",
        logout_url=None,
        nav_links=(
            bridge.TapdbHostNavLink(" Valid ", " /valid "),
            bridge.TapdbHostNavLink("", "/missing-label"),
            bridge.TapdbHostNavLink("missing-href", ""),
        ),
        extra_stylesheets=(" /static/a.css ", ""),
    )
    shell = bridge.resolve_host_shell(host, request)
    assert shell["service_name"] == "tapdb"
    assert shell["app_name"] == "TAPDB"
    assert shell["logout_url"] == ""
    assert shell["nav_links"] == [{"label": "Valid", "href": "/valid"}]
    assert shell["extra_stylesheets"] == ["/static/a.css"]

    assert bridge.resolve_host_context(None, request) == {}
    assert (
        bridge.resolve_host_context(
            bridge.TapdbHostBridge(extra_context=lambda _request: "invalid"), request
        )
        == {}
    )
    assert bridge.resolve_host_context(
        bridge.TapdbHostBridge(extra_context=lambda _request: {"host": True}), request
    ) == {"host": True}
