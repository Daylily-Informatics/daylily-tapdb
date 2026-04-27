# TAPDB Docs

This directory holds the support material for the TAPDB substrate. The root [README.md](../README.md) is the canonical entry point for new readers and the quickest path to the current mental model.

## Start Here

- [../README.md](../README.md): overview, philosophy, quickstart, and core mental model
- [architecture.md](architecture.md): deeper architecture, object model, and write-path explanation
- [tapdb_gui_inclusion.md](tapdb_gui_inclusion.md): admin GUI embedding and auth-mode guidance

## Current Focus

- TAPDB is a reusable substrate, not a domain repo.
- The current codebase is organized around templates, instances, lineage, audit, outbox, inbox, and explicit scoping.
- Meridian terminology in the docs should use `domain` and `domain_code`, not older sandbox language.
- CLI examples should reflect the current `tapdb --config ... --env ...` namespace model.

## Deep Dives

The refactor review and the deeper doc set live in this directory as separate support files:

- [identity-and-scoping.md](identity-and-scoping.md)
- [template-authoring.md](template-authoring.md)
- [runtime-and-cli.md](runtime-and-cli.md)
- [integration-and-embedding.md](integration-and-embedding.md)
- [repository-review.md](repository-review.md)

Those files are the right place for detailed references, policy notes, and implementation guidance that would clutter the root README.

Historical or planning material also lives here. Treat these as background, not
as the current operator quickstart:

- [tapdb_0600_refactor_overview_what_was_done.md](tapdb_0600_refactor_overview_what_was_done.md)
- [tapdb_meridian_041_refactor.md](tapdb_meridian_041_refactor.md)
- [specs_and_plans/](specs_and_plans/)

## Reading Order

1. Read the root README for the framing and the top-level quickstart.
2. Read [architecture.md](architecture.md) for the structural model and write path.
3. Read the deeper docs when you need a focused topic such as identity, template packs, runtime, or integration boundaries.
