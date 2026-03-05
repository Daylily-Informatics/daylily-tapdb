from __future__ import annotations

import time
from datetime import datetime, timezone

from admin.db_metrics import (
    MetricsRow,
    TSVMetricsWriter,
    current_metrics_path,
    two_week_period_start_utc,
)


def test_two_week_period_start_groups_iso_weeks():
    # 2026-01-05 is ISO week 2, grouped with week 1 starting 2025-12-29.
    dt = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    assert two_week_period_start_utc(dt) == datetime(2025, 12, 29, tzinfo=timezone.utc)

    # 2026-01-12 is ISO week 3, grouped with week 3 starting 2026-01-12.
    dt = datetime(2026, 1, 12, 12, 0, tzinfo=timezone.utc)
    assert two_week_period_start_utc(dt) == datetime(2026, 1, 12, tzinfo=timezone.utc)


def test_tsv_writer_writes_header_and_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("TAPDB_DB_METRICS_DIR", str(tmp_path))
    monkeypatch.setenv("TAPDB_DB_METRICS_FLUSH_SECS", "3600")
    monkeypatch.setenv("TAPDB_DB_METRICS_QUEUE_MAX", "100")

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
    assert text.startswith("ts_utc\tduration_ms\tok\top\ttable_hint\tpath\tmethod\tusername\trowcount\terror_type")
    assert "SELECT" in text
    assert "\tgeneric_instance\t" in text


def test_tsv_writer_drops_when_queue_full(tmp_path, monkeypatch):
    monkeypatch.setenv("TAPDB_DB_METRICS_DIR", str(tmp_path))
    monkeypatch.setenv("TAPDB_DB_METRICS_FLUSH_SECS", "3600")
    monkeypatch.setenv("TAPDB_DB_METRICS_QUEUE_MAX", "1")

    writer = TSVMetricsWriter("dev")
    time.sleep(0.05)  # let background thread enter sleep loop
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

