"""Contracts for the changed-module branch-coverage release gate."""

from __future__ import annotations

import subprocess

import pytest

from scripts.verify_changed_coverage import (
    changed_python_modules,
    coverage_failures,
)


def test_changed_python_modules_keeps_only_production_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = iter(
        [
            "daylily_tapdb/runtime_info.py\nREADME.md\ntests/test_x.py\nadmin/main.py\n",
            "daylily_tapdb/modified.py\n",
            "admin/staged.py\n",
            "daylily_tapdb/new_surface.py\ndocs/new.md\n",
        ]
    )

    def _run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 0, stdout=next(outputs), stderr="")

    monkeypatch.setattr(subprocess, "run", _run)

    assert changed_python_modules("origin/main") == [
        "admin/main.py",
        "admin/staged.py",
        "daylily_tapdb/modified.py",
        "daylily_tapdb/new_surface.py",
        "daylily_tapdb/runtime_info.py",
    ]


def test_coverage_failures_enforces_present_numeric_branch_aware_percent() -> None:
    report = {
        "files": {
            "daylily_tapdb/good.py": {"summary": {"percent_covered": 90.0}},
            "daylily_tapdb/low.py": {"summary": {"percent_covered": 89.999}},
            "admin/no_percent.py": {"summary": {}},
        }
    }

    measured, failures = coverage_failures(
        report,
        [
            "daylily_tapdb/good.py",
            "daylily_tapdb/low.py",
            "admin/no_percent.py",
            "admin/missing.py",
        ],
        minimum=90.0,
    )

    assert measured == [
        ("daylily_tapdb/good.py", 90.0),
        ("daylily_tapdb/low.py", 89.999),
    ]
    assert failures == [
        "daylily_tapdb/low.py: 90.00% is below 90.00%",
        "admin/no_percent.py: coverage percentage is unavailable",
        "admin/missing.py: absent from coverage report",
    ]


def test_coverage_failures_rejects_malformed_report() -> None:
    with pytest.raises(ValueError, match="files mapping"):
        coverage_failures({}, [], minimum=90.0)
