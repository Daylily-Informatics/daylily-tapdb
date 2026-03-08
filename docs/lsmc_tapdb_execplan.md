# LSMC TapDB Execution Plan

## Goal

Converge `daylily-tapdb` on one supported action system and one shared identifier posture for the LSMC beta contract.

## Decisions

- `action_definition` is the only supported action payload shape for action templates.
- `materialize_actions()` must ignore templates that do not expose a valid `action_definition`; no legacy `action_template` fallback remains.
- `ActionDispatcher` remains the shared execution surface for repo-specific handlers.
- Shared EUID helpers remain focused on formatting and validation of opaque Meridian-style EUIDs.
- TapDB will not present app-local prefix registration as a shared contract for Atlas, Bloom, or Ursa.

## Worklist

1. Remove the legacy `action_template` fallback in `daylily_tapdb/factory/instance.py`.
2. Update tests to assert that missing or invalid `action_definition` entries are skipped rather than silently accepted.
3. Replace the mutable `EUIDConfig` registry API with a read-only canonical prefix catalog suitable for documentation and validation only.
4. Update `tests/test_euid.py` and `tests/conftest.py` to match the tightened EUID surface.
5. Update README examples so TapDB only documents the canonical action pattern and no longer advertises application-defined prefix registration.
6. Keep existing outbox behavior unchanged, but call it out as supported shared substrate.

## Validation

- `pytest tests/test_instance_factory_unit.py tests/test_euid.py tests/test_outbox_integration.py tests/test_outbox_sql.py tests/test_cli.py tests/test_integration.py`
- `ruff check daylily_tapdb tests`

## Breaking Changes

- Templates that still use `json_addl.action_template` without `json_addl.action_definition` will no longer materialize actions.
- `EUIDConfig.register_prefix()` and related app-local prefix mutation behavior are removed from the supported shared API.
