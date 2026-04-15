from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from admin.db_metrics import (
    MetricsRow,
    TSVMetricsWriter,
    current_metrics_path,
    two_week_period_start_utc,
)
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context


def _write_config(path: Path, *, queue_max: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    domain_registry = path.parent / "domain_code_registry.json"
    prefix_registry = path.parent / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test-localhost"}}}\n',
        encoding="utf-8",
    )
    prefix_registry.write_text(
        (
            '{"version":"0.4.0","ownership":{"Z":{"TPX":{"issuer_app_code":"daylily-tapdb"},'
            '"EDG":{"issuer_app_code":"daylily-tapdb"},'
            '"ADT":{"issuer_app_code":"daylily-tapdb"},'
            '"SYS":{"issuer_app_code":"daylily-tapdb"},'
            '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
        ),
        encoding="utf-8",
    )
    path.write_text(
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: alpha\n"
        "  database_name: beta\n"
        "  owner_repo_name: daylily-tapdb\n"
        f"  domain_registry_path: {domain_registry}\n"
        f"  prefix_ownership_registry_path: {prefix_registry}\n"
        "admin:\n"
        "  footer:\n"
        "    repo_url: https://example.com/tapdb\n"
        "  session:\n"
        "    secret: secret123\n"
        "  auth:\n"
        "    mode: tapdb\n"
        "    disabled_user:\n"
        "      email: tapdb-admin@localhost\n"
        "      role: admin\n"
        "    shared_host:\n"
        "      session_secret: shared-secret\n"
        "      session_cookie: session\n"
        "      session_max_age_seconds: 1209600\n"
        "  cors:\n"
        "    allowed_origins: []\n"
        "  ui:\n"
        "    tls:\n"
        "      cert_path: ''\n"
        "      key_path: ''\n"
        "  metrics:\n"
        "    enabled: true\n"
        f"    queue_max: {queue_max}\n"
        "    flush_seconds: 3600\n"
        "  db_pool_size: 5\n"
        "  db_max_overflow: 10\n"
        "  db_pool_timeout: 30\n"
        "  db_pool_recycle: 1800\n"
        "environments:\n"
        "  dev:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: '5533'\n"
        "    ui_port: '8911'\n"
        "    domain_code: Z\n"
        "    user: tapdb\n"
        "    password: ''\n"
        "    database: tapdb_dev\n",
        encoding="utf-8",
    )
    return path


def test_two_week_period_start_groups_iso_weeks():
    dt = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    assert two_week_period_start_utc(dt) == datetime(2025, 12, 29, tzinfo=timezone.utc)

    dt = datetime(2026, 1, 12, 12, 0, tzinfo=timezone.utc)
    assert two_week_period_start_utc(dt) == datetime(2026, 1, 12, tzinfo=timezone.utc)


def test_tsv_writer_writes_header_and_rows(tmp_path):
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "alpha" / "beta" / "tapdb-config.yaml",
        queue_max=100,
    )
    clear_cli_context()
    set_cli_context(config_path=cfg_path, env_name="dev")

    writer = TSVMetricsWriter("dev")
    writer.enqueue(
        MetricsRow(
            ts_utc="2026-01-01T00:00:00+00:00",
            duration_ms="1.234",
            ok="1",
            op="SELECT",
            table_hint="generic_instance",
            path="/",
            method="GET",
            username="admin",
            rowcount="1",
            error_type="",
        )
    )
    writer.stop(timeout=1.0)

    path = current_metrics_path("dev")
    text = path.read_text(encoding="utf-8")
    assert text.startswith(
        "ts_utc\tduration_ms\tok\top\ttable_hint\tpath\tmethod\tusername\trowcount\terror_type"
    )
    assert "SELECT" in text
    assert "\tgeneric_instance\t" in text
    clear_cli_context()


def test_tsv_writer_drops_when_queue_full(tmp_path):
    cfg_path = _write_config(
        tmp_path / ".config" / "tapdb" / "alpha" / "beta" / "tapdb-config.yaml",
        queue_max=1,
    )
    clear_cli_context()
    set_cli_context(config_path=cfg_path, env_name="dev")

    writer = TSVMetricsWriter("dev")
    time.sleep(0.05)
    row = MetricsRow(
        ts_utc="2026-01-01T00:00:00+00:00",
        duration_ms="0.100",
        ok="1",
        op="SELECT",
        table_hint="generic_instance",
        path="/",
        method="GET",
        username="admin",
        rowcount="1",
        error_type="",
    )
    for _ in range(50):
        writer.enqueue(row)
    assert writer.dropped_count() > 0
    writer.stop(timeout=1.0)
    clear_cli_context()
