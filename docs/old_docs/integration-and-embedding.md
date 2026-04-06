# Integration and Embedding

TAPDB can run as a standalone substrate, or it can be embedded inside a larger application. The current codebase supports both patterns, but the responsibilities stay sharply divided.

## What TAPDB Owns

TAPDB owns the substrate layer:

- polymorphic template and instance persistence
- lineage and traversal
- audit history
- transactional outbox and inbox handling
- admin UI mounting and auth plumbing
- CLI-driven database lifecycle

The current object model and relationships are implemented in [`daylily_tapdb/models`](../daylily_tapdb/models) and the outbox/inbox helpers in [`daylily_tapdb/outbox`](../daylily_tapdb/outbox).

## Admin Mounting

The admin UI can be mounted into another FastAPI app using the loader in [`daylily_tapdb.cli.admin_server`](../daylily_tapdb/cli/admin_server.py).

The current pattern is:

```python
from fastapi import FastAPI
from daylily_tapdb.cli.admin_server import load_admin_app

app = FastAPI()
tapdb_admin = load_admin_app(
    config_path="/abs/path/to/tapdb-config.yaml",
    env_name="dev",
)
app.mount("/tapdb", tapdb_admin)
```

The UI assumes HTTPS for login and callback flows. The supported mounting and auth guidance is also summarized in [`docs/tapdb_gui_inclusion.md`](./tapdb_gui_inclusion.md).

## Auth Modes

Current TAPDB admin auth modes are configured in TAPDB config and handled by the admin server:

- `tapdb` for TAPDB-native auth
- `shared_host` for host-app session sharing
- `disabled` for local development or diagnostics

The config example in [`config/tapdb-config-example.yaml`](../config/tapdb-config-example.yaml) shows the expected shape. The current docs and code treat `disabled` as non-production only.

Practical guidance:

- Use `tapdb` when TAPDB should own its own login flow.
- Use `shared_host` when a parent app already has a session system and can provide the expected payload.
- Use `disabled` only when the parent app already enforces access or when you are working locally.

## Client Repository Responsibilities

Client repos should own:

- domain semantics
- domain template packs
- workflow semantics
- integration adapters
- business-specific tests and fixtures

TapDB should not become the place where domain meaning lives. It is the persistence and object model substrate, not the application authority.

That split is consistent with the current core config pack in [`daylily_tapdb/core_config`](../daylily_tapdb/core_config) and with the template loading policy in [`daylily_tapdb/templates/loader.py`](../daylily_tapdb/templates/loader.py).

## Extension Boundaries

Use TAPDB as the substrate, but keep domain behavior in the owning application repo.

TapDB is a good fit for:

- reusable object models
- lineage-based relationships
- versioned templates
- audit and outbox persistence
- shared admin/runtime management

Application repos should own:

- customer or lab workflow semantics
- domain-specific template packs
- external integrations
- access policy beyond TAPDB substrate rules
- user-facing behavior that is not generic enough to belong here

That line matters because TAPDB has been refactored into a reusable base layer. The docs should explain the base layer clearly, but not blur the boundary back into application logic.

## What Not To Outsource

Application repos should not outsource these responsibilities to TAPDB:

- the meaning of domain objects
- the business meaning of EUIDs
- the choice of workflow states
- the shape of domain-specific JSON payloads
- the orchestration of external APIs

TAPDB can store, validate, and expose those objects. It should not decide what they mean.

## Related Materials

- [`docs/runtime-and-cli.md`](./runtime-and-cli.md)
- [`docs/tapdb_gui_inclusion.md`](./tapdb_gui_inclusion.md)
