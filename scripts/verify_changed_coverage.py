"""Require branch-aware coverage for every changed production Python module."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

SOURCE_ROOTS = ("daylily_tapdb/", "admin/")


def _git_lines(*args: str) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def changed_python_modules(base: str) -> list[str]:
    """Return changed and untracked production modules relative to ``base``."""

    # Deleted modules cannot appear in a coverage report and are verified by
    # separate import/package-asset contracts. Restrict this gate to code that
    # will actually ship in the candidate tree.
    changed = set(
        _git_lines("diff", "--name-only", "--diff-filter=ACMR", f"{base}...HEAD")
    )
    changed.update(_git_lines("diff", "--name-only", "--diff-filter=ACMR"))
    changed.update(_git_lines("diff", "--cached", "--name-only", "--diff-filter=ACMR"))
    changed.update(_git_lines("ls-files", "--others", "--exclude-standard"))
    return sorted(
        path
        for path in changed
        if path.endswith(".py") and path.startswith(SOURCE_ROOTS)
    )


def coverage_failures(
    report: dict,
    modules: list[str],
    *,
    minimum: float,
) -> tuple[list[tuple[str, float]], list[str]]:
    """Return measured modules and release-blocking coverage failures."""

    files = report.get("files")
    if not isinstance(files, dict):
        raise ValueError("coverage report is missing the files mapping")

    measured: list[tuple[str, float]] = []
    failures: list[str] = []
    for module in modules:
        record = files.get(module)
        if not isinstance(record, dict) or not isinstance(record.get("summary"), dict):
            failures.append(f"{module}: absent from coverage report")
            continue
        raw_percent = record["summary"].get("percent_covered")
        if not isinstance(raw_percent, (int, float)):
            failures.append(f"{module}: coverage percentage is unavailable")
            continue
        percent = float(raw_percent)
        measured.append((module, percent))
        if percent < minimum:
            failures.append(f"{module}: {percent:.2f}% is below {minimum:.2f}%")
    return measured, failures


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/main", help="Git comparison ref")
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("coverage.json"),
        help="coverage.py JSON report",
    )
    parser.add_argument("--minimum", type=float, default=90.0)
    return parser


def main() -> None:
    args = _parser().parse_args()
    modules = changed_python_modules(args.base)
    report = json.loads(args.report.read_text(encoding="utf-8"))
    measured, failures = coverage_failures(
        report,
        modules,
        minimum=args.minimum,
    )

    for module, percent in measured:
        print(f"{module}: {percent:.2f}%")
    if failures:
        raise SystemExit("changed-module coverage failed:\n" + "\n".join(failures))
    print(
        f"changed-module coverage passed for {len(modules)} module(s) "
        f"at >= {args.minimum:.2f}%"
    )


if __name__ == "__main__":
    main()
