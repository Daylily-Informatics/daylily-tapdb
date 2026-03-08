# LSMC TapDB Breaking Changes

## Action System

- `json_addl.action_definition` is now mandatory for action template materialization.
- TapDB no longer falls back to `json_addl.action_template`.
- Tests, docs, and examples that treat the legacy fallback as valid are removed.

## Identifier Governance

- Shared EUID helpers no longer advertise application-defined prefix registration as the normal contract.
- Calling code must treat EUIDs as opaque identifiers; business behavior must not be derived from prefix parsing.

## Impact On App Repos

### Bloom

- Bloom action templates must expose `action_definition` only.
- Bloom cannot rely on TapDB preserving retired action compatibility during action import or materialization.

### Atlas

- Atlas must treat EUIDs as opaque and must not introduce logic that depends on app-local prefix registration through TapDB.

### Ursa

- Ursa must treat EUIDs as opaque and must not depend on custom prefix registration behavior from TapDB.
