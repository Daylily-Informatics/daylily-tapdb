"""Deployment-registered validators shared by Python, CLI, and HTTP.

Entry points are installed, trusted application code. Never accept a module or
function name from an HTTP payload or correction-plan file.
"""

from importlib.metadata import entry_points
from typing import Callable


def load_owner_validators(
    explicit: dict[str, Callable] | None = None,
) -> dict[str, Callable]:
    result = {}
    for entry in entry_points(group="daylily_tapdb.correction_validators"):
        if entry.name in result:
            raise ValueError("duplicate installed correction validator: " + entry.name)
        validator = entry.load()
        if not callable(validator):
            raise ValueError("correction validator must be callable: " + entry.name)
        result[entry.name] = validator
    for owner, validator in (explicit or {}).items():
        if not isinstance(owner, str) or not owner.strip() or not callable(validator):
            raise ValueError(
                "explicit owner validator requires an exact owner and callable"
            )
        if owner in result and result[owner] is not validator:
            raise ValueError("conflicting correction validators: " + owner)
        result[owner] = validator
    return result
