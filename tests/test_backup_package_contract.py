"""Package-level contracts for daylily_tapdb.backup.

These guard properties that are easy to break silently while adding modules:
the package must stay importable without AWS libraries or a database, and its
public surface must stay honest.
"""

from __future__ import annotations

import ast
import importlib
import subprocess
import sys
from pathlib import Path

import daylily_tapdb.backup as backup_pkg


def _discover_modules() -> list[str]:
    """Enumerate the package's modules from disk.

    Deliberately not a hand-maintained list: that version went stale three
    times as modules were added, and each time the tests below passed
    vacuously against whatever had been forgotten.
    """
    package_dir = Path(backup_pkg.__file__).parent
    return sorted(
        f"daylily_tapdb.backup.{path.stem}"
        for path in package_dir.glob("*.py")
        if path.stem != "__init__"
    )


MODULES = _discover_modules()


def test_module_discovery_found_the_package():
    # Guards the guard: an empty list would make every test below vacuous.
    assert len(MODULES) >= 9
    assert "daylily_tapdb.backup.service" in MODULES


def test_every_module_is_importable():
    for name in MODULES:
        assert importlib.import_module(name) is not None


def test_every_name_in_all_actually_exists():
    missing = [name for name in backup_pkg.__all__ if not hasattr(backup_pkg, name)]

    assert missing == []


def _group_key(name: str) -> tuple[int, str]:
    """Order by the repo's __all__ convention: constants, classes, functions.

    Matches daylily_tapdb/validation/__init__.py, which lists
    DEFAULT_VALIDATOR_REF, then Assessment/Finding, then assess_evidence...
    """
    if name.isupper():
        return (0, name)
    if name[0].isupper():
        return (1, name)
    return (2, name)


def test_all_follows_the_repo_grouping_convention():
    assert backup_pkg.__all__ == sorted(backup_pkg.__all__, key=_group_key)


def test_all_is_free_of_duplicates():
    assert len(backup_pkg.__all__) == len(set(backup_pkg.__all__))


def test_every_module_contributes_to_the_package_surface():
    # A module added but never re-exported is easy to miss -- engine.py was.
    for name in MODULES:
        module = importlib.import_module(name)
        exported = set(getattr(module, "__all__", []))
        if not exported:
            continue
        assert exported & set(backup_pkg.__all__), (
            f"{name} exports {sorted(exported)[:3]}... but none reach the package"
        )


def test_importing_the_package_does_not_require_boto3():
    # This package is imported on every CLI invocation. Pulling in the AWS SDK
    # at module scope would tax every `tapdb` command and break environments
    # that have no AWS libraries installed at all.
    code = (
        "import sys; import daylily_tapdb.backup; "
        "sys.exit(1 if 'boto3' in sys.modules else 0)"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True)

    assert result.returncode == 0, "importing daylily_tapdb.backup pulled in boto3"


def test_importing_the_package_does_not_require_a_database_driver_connection():
    # Importing must never attempt to connect to anything.
    code = "import daylily_tapdb.backup as b; assert b.BACKUP_CLASS_FULL == 'full'"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True)

    assert result.returncode == 0, result.stderr.decode()[:400]


def _imported_roots(module_name: str) -> set[str]:
    """Top-level packages a module imports, read from its AST.

    Parsed rather than grepped: the modules discuss typer and FastAPI in their
    docstrings precisely because staying free of them is the point, and a text
    search cannot tell the prose from an import.
    """
    path = Path(backup_pkg.__file__).parent / f"{module_name.rsplit('.', 1)[-1]}.py"
    roots: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    return roots


#: Frameworks that belong to a surface. The service layer exists so that the
#: CLI, the admin API and the GUI share one implementation; the moment one of
#: these appears inside the package, a surface has started to leak into it.
SURFACE_FRAMEWORKS = {"typer", "fastapi", "starlette", "jinja2", "click", "rich"}


def test_no_module_imports_a_surface_framework():
    """The plan's central claim: one service, and the surfaces are adapters.

    Without this check the claim is enforced only by review. It is exactly the
    kind of boundary that erodes one convenient import at a time.
    """
    offenders = {
        name: sorted(_imported_roots(name) & SURFACE_FRAMEWORKS)
        for name in MODULES
        if _imported_roots(name) & SURFACE_FRAMEWORKS
    }

    assert not offenders, f"surface frameworks leaked into the service: {offenders}"


def test_only_the_engine_shells_out():
    """Every external `pg_dump`/`pg_restore` call goes through one module.

    Concentrating them is what makes the redaction, timeout and error handling
    reviewable in a single place instead of copied per call site.
    """
    shelling = sorted(name for name in MODULES if "subprocess" in _imported_roots(name))

    assert shelling == ["daylily_tapdb.backup.engine"], shelling


def test_errors_all_share_one_base_so_callers_can_catch_broadly():
    subclasses = [
        backup_pkg.BackupNotFoundError,
        backup_pkg.BackupVerificationError,
        backup_pkg.BackupVersionMismatchError,
        backup_pkg.RestoreConfirmationError,
        backup_pkg.RestoreStageStaleError,
        backup_pkg.BackupPolicyBlockedError,
    ]

    for cls in subclasses:
        assert issubclass(cls, backup_pkg.BackupError)
