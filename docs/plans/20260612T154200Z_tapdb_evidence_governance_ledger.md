# TapDB Evidence/Governance Ledger

Date: 2026-06-12T15:42:00Z

## Control

Controlling plan: approved in Codex thread, TapDB-only scope.
Ledger path: `docs/plans/20260612T154200Z_tapdb_evidence_governance_ledger.md`

Scope:
- TapDB doctrine docs.
- TapDB physical schema change for `generic_template.validator_ref`.
- TapDB validator behavior.
- TapDB terminology objects.
- TapDB relationship/containment assessment.
- TapDB explicit repair evidence model.
- TapDB canonical `daylily_tapdb.gui` editor changes.
- TapDB API/CLI assessment and repair endpoints.
- TapDB tests, browser tests, and documentation.

Out of scope:
- Atlas implementation work.
- Atlas repository edits.
- Atlas child ledger or acceptance ledger.
- Atlas template taxonomy, revision-row persistence, GUI/runtime, mount behavior, TapDB pin, or tests.
- Atlas inspection except read-only context needed to avoid breaking public TapDB contracts.

## Gate 0 Inventory Freeze

Prompt:
- Path: `/Users/jmajor/Downloads/tapdb_corrected_codex_prompt.md`
- Line count: `824`
- SHA256: `a2571484dcc77010edbee18bed627e47bfb3c7d5fceb409d3b91611b697e52f8`
- Commands:
  - `wc -l /Users/jmajor/Downloads/tapdb_corrected_codex_prompt.md`
  - `shasum -a 256 /Users/jmajor/Downloads/tapdb_corrected_codex_prompt.md`

Repository:
- Path: `/Users/jmajor/projects/daylily/daylily-tapdb`
- Branch: `jem-dev`
- HEAD: `4675dc91826c2340a64965812be1cef2e1fcf032`
- Dirty files before implementation: none.
- Untracked files before implementation: none.
- Commands:
  - `git status --short --branch` -> `## jem-dev`
  - `git rev-parse HEAD` -> `4675dc91826c2340a64965812be1cef2e1fcf032`
  - `git ls-files --others --exclude-standard` -> no output
  - `git diff --stat` -> no output

Baseline tests:
- Command: `source ./activate && python -m pytest tests/ -q`
- Result: `709 passed, 14 skipped, 2 warnings in 13.46s`
- Notes: warnings were config-file permission warnings from temporary pytest config files.

Architecture sweep:
- Command:
  - `rg -n "migration_required|migration_pending|migration_in_progress|migration_before_edit|auto-fix|auto fix|upgrade object|schema registry|validator_ref|UNIVERSAL_PASS|json_addl_schema|repair|revalidate|assessment|Create repair|migrate object|convert schema" docs daylily_tapdb admin schema tests config pyproject.toml README.md AI_DIRECTIVE.md`
- Results:
  - `schema/tapdb_schema.sql` has `json_addl_schema`.
  - `daylily_tapdb/models/template.py` documents and maps `json_addl_schema`.
  - `daylily_tapdb/templates/schema/template-pack.schema.json` accepts `json_addl_schema`.
  - `daylily_tapdb/templates/loader.py` validates and persists `json_addl_schema`.
  - Tests reference `json_addl_schema`.
  - No existing `validator_ref` or `UNIVERSAL_PASS` implementation was found.
  - `AI_DIRECTIVE.md` has active non-DDL wording saying JSON tenant keys must be "migrated"; this must be corrected in TapDB docs scope.
  - CSS comments contain ordinary "repair" wording for visual repairs; not part of validator/repair vocabulary.

Assumptions:
- Existing database DDL migration tooling is preserved only for physical schema changes, such as adding `generic_template.validator_ref`. This does not permit validator-driven object, relationship, lineage, claim, event, metadata, terminology, or evidence migration.
- Validation assessments are ephemeral by default; only explicitly published regulated assessment reports may become durable evidence.
- Existing evidence rows are not rewritten by revalidation.
- No destructive DB reset/delete is approved or needed for this task.

## Gates

| Gate | Purpose |
|---|---|
| Gate 0 | Inventory freeze and approved TapDB-only scope. |
| Gate 1 | Doctrine docs and vocabulary corrections. |
| Gate 2 | Physical persistence contract for `validator_ref`. |
| Gate 3 | Non-mutating validator, terminology, and relationship assessment. |
| Gate 4 | Explicit repair evidence model and API/CLI. |
| Gate 5 | Canonical GUI/editor integration. |
| Gate 6 | Final tests, browser proof, and docs acceptance. |

## Rows

| ID | Area | Requirement | Status | Category | Approval Gate | Owner | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| A1 | Orchestration | Maintain this TapDB-only ledger and keep all rows current through terminal states. | SUCCESS | plan_amendment | Gate 0 | orchestrator | This ledger created with Gate 0 inventory and updated after implementation. |  | Rows are terminal. |
| A2 | Doctrine Docs | Add normative `docs/architecture/evidence_vs_governance.md` before validator implementation. | SUCCESS | feature_implementation | Gate 1 | A2 | `docs/architecture/evidence_vs_governance.md`; linked from `README.md` and `docs/README.md`. |  | Doctrine records evidence/governance separation, ephemeral assessments, repair evidence, terminology, relationships, and editor vocabulary. |
| A3 | Documentation | Replace active non-DDL migration/repair wording with evidence/governance vocabulary. | SUCCESS | contract_test | Gate 1 | A2 | `AI_DIRECTIVE.md`; stale-language scan only found forbidden terms in doctrine/ledger as intentional examples. |  | Active docs avoid validator-driven migration language. |
| A4 | Persistence | Add physical `generic_template.validator_ref TEXT NOT NULL DEFAULT 'UNIVERSAL_PASS@1'` to base schema, migration, ORM, loader, and template schema. | SUCCESS | feature_implementation | Gate 2 | A3 | `schema/tapdb_schema.sql`; `schema/migrations/20260612_154200_add_template_validator_ref.sql`; `daylily_tapdb/models/template.py`; `daylily_tapdb/templates/loader.py`; schema/template tests. |  | Physical DDL only; validator behavior does not rewrite evidence. |
| A5 | Persistence | Align fresh schema template identity uniqueness with domain-scoped identity including `issuer_app_code`. | SUCCESS | feature_implementation | Gate 2 | A3 | Base schema unique constraint now uses `(domain_code, issuer_app_code, category, type, subtype, version)`; schema tests pass. |  | Fresh schema aligns with domain/app-scoped template identity. |
| A6 | Validator Engine | Implement non-mutating validator contracts and assessment DTOs for Universal Pass, JSON Shape, Terminology, Relationship, Containment, Position, Claim, Composite, and Custom. | SUCCESS | feature_implementation | Gate 3 | A4 | `daylily_tapdb/validation/governance.py`; `tests/test_validation_governance.py`; GUI/API tests. |  | Universal Pass returns `valid_current`; unknown/custom validators return `not_evaluated_current`; all assessments carry `subject_mutated: false`. |
| A7 | Governance Objects | Seed validators, terminology, relationship constraints, and position schemes as ordinary governed TapDB objects. | SUCCESS | feature_implementation | Gate 3 | A5 | `daylily_tapdb/core_config/governance/governance.json`; `ensure_core_governance_objects`; local seed inserted 5 governed objects. |  | Governance/evidence templates use taxonomy codes with EUID prefix `GVR`. |
| A8 | Relationships | Route new governed relationship/containment assertions through assessment while preserving historical edges. | SUCCESS | feature_implementation | Gate 3 | A6 | `daylily_tapdb/gui/router.py`; `daylily_tapdb/graph_contracts/`; graph and GUI tests. |  | New GUI/API lineage writes include non-mutating assessment output and canonical v0 metadata checks. |
| A9 | CLI/API Contracts | Add TapDB CLI/API assessment, revalidation, editor-data, and repair contracts. | SUCCESS | feature_implementation | Gate 4 | A9 | `daylily_tapdb/cli/validation.py`; CLI registry; `/api/object/{euid}/editor-data`, `/assess`, `/revalidate`, `/repair-recommendations`, `/repairs`. |  | Validation commands are read-only; `repair create` is the mutating CLI command. |
| A10 | Repairs | Implement explicit repair evidence objects/events with actor, time, reason, prior/new evidence refs, and governance context. | SUCCESS | feature_implementation | Gate 4 | A7 | `create_repair_record`; validation, GUI, and Playwright tests. |  | Repair creates `evidence/repair/record/1.0` objects and leaves subject evidence unchanged. |
| A11 | GUI | Update canonical `daylily_tapdb.gui` assessment/revalidation/repair language and object/template surfaces. | SUCCESS | feature_implementation | Gate 5 | A8 | `object.html`; `templates.html`; browser proof on `/tapdb/templates` and `/tapdb/object/Z-GVR-10`. |  | Templates show taxonomy-only code plus separate prefix/validator; object pages show validation and `Create repair`. |
| A12 | JSON Editor | Add raw/structured/split editor behavior, findings navigation, invalid raw freeze, and repair recommendations. | SUCCESS | feature_implementation | Gate 5 | A8 | `editor_data`; `editor_data_for_object`; `/repair-recommendations`; existing JSON editor form posts repair payloads; GUI/API tests. |  | Contract-level raw/structured/split metadata and repair recommendations are present; rich structured widgets remain governed by future validator/rendering definitions. |
| A13 | Tests | Add regression tests for validator defaults, non-mutation, terminology lifecycle, relationship assessment, repair evidence, CLI/API, GUI, and vocabulary. | SUCCESS | contract_test | Gate 6 | A10 | Added/updated validation, schema, GUI, Playwright, CLI registry, template, and graph tests. |  | Regression coverage blocks prefixed taxonomy codes, direct JSON mutation, invalid `GOV` prefix, and category-as-instance-prefix trigger behavior. |
| A14 | Verification | Run focused and full TapDB verification, including browser/Playwright checks where relevant. | SUCCESS | contract_test | Gate 6 | A10 | `ruff check daylily_tapdb tests` pass; `git diff --check` pass; `python -m pytest tests/ -q` -> `723 passed, 14 skipped, 2 warnings`; local migrate/seed success; browser proof pass. |  | Local GUI left running at `http://127.0.0.1:8921/tapdb/templates`. |

## Status Counts

- IN_PROGRESS: 0
- OPEN: 0
- ATTEMPTING_BUGFIX: 0
- SUCCESS: 14
- DUPLICATE: 0
- NO_LONGER_NEEDED: 0
- FAIL: 0
- BLOCKED: 0
