# Repository Review

This memo captures the documentation-relevant findings from the post-refactor review of `daylily-tapdb`.

## Findings

### 1. The root README was missing at review time

At the start of this documentation rebuild, `pyproject.toml` still declared `README.md` as the package readme, and `docs/README.md` still treated `../README.md` as the canonical top-level entry point, but the file itself was not present in the repository.

That created a real documentation gap:

- package metadata points at a missing file
- the docs index points at a missing file
- new contributors do not get a single canonical overview

### 2. The docs index was stale

[`docs/README.md`](./README.md) still references historical migration notes and the missing root README, but it does not yet reflect the refactored documentation surface.

The docs index should be an entry point into:

- a root README
- runtime and CLI docs
- identity/scoping docs
- template authoring docs
- integration/embedding docs

### 3. Meridian terminology and example drift existed

Current TAPDB code and the Meridian 0.3.2 spec require domain-aware EUID language, but older examples and some repo lore still use stale forms like `sandbox` terminology or non-canonical EUID examples.

The current code in [`daylily_tapdb/euid.py`](../daylily_tapdb/euid.py) is explicit about:

- production vs domain-scoped validation
- uppercase-only canonical form
- checksum validation
- `MERIDIAN_DOMAIN_CODE` runtime resolution

Docs should mirror that current model and avoid examples that imply UUID-like or metadata-bearing identifiers.

### 4. CLI and config contract drift existed

The current CLI is config-first and namespace-scoped, but the existing documentation surface is too thin to make that obvious.

The codebase currently expects:

- explicit `--config`
- explicit `--env`
- metadata-driven namespace resolution
- runtime layout under `~/.config/tapdb/<client>/<database>/<env>/`

That needs to be documented as the operational contract, not as an implementation detail.

### 5. Historical and planning cruft needed demotion

The refactor left behind historical material that is useful for context but not for day-to-day use.

The documentation should clearly separate:

- current operational guidance
- architecture explanations
- historical or planning notes

Anything in the historical bucket should be explicitly labeled as background, not as current behavior.

## Documentation Direction

The rebuilt docs should present TAPDB as:

- a reusable substrate
- a polymorphic object model
- a lineage-first persistence layer
- a CLI-managed runtime

They should not present TAPDB as a domain application in its own right.

## Evidence Base

This review is grounded in current code, tests, and docs, including:

- [`daylily_tapdb/cli/context.py`](../daylily_tapdb/cli/context.py)
- [`daylily_tapdb/cli/__init__.py`](../daylily_tapdb/cli/__init__.py)
- [`daylily_tapdb/euid.py`](../daylily_tapdb/euid.py)
- [`schema/tapdb_schema.sql`](../schema/tapdb_schema.sql)
- [`tests/test_cli.py`](../tests/test_cli.py)
- [`tests/test_pg_integration.py`](../tests/test_pg_integration.py)
- [`tests/test_euid.py`](../tests/test_euid.py)
- [`docs/README.md`](./README.md)
