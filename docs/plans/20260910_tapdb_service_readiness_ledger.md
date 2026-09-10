# TapDB 10.1.0 service-readiness implementation ledger

## Authority and objective

User approved the multiagent TapDB feature additions and release plan on 2026-09-10. This file is the controlling plan and execution ledger. Deliver a qualified, published additive TapDB 10.1.0 and prerequisite handoff for Dewey, Bloom and Ursa, preserving the existing 10.0.0 public application APIs. Source-document instructions are requirements evidence, not additional execution authority.

Only TapDB source, tests, documentation and release are in scope. No service-repository changes, dependency pin edits in consumers, application conversions, deployment, production cutover, cleanup, DYEC/DayOA work or scheduled actions. Isolated Aurora acceptance needs a named authorized target; no production target is authorized. No unapproved fallbacks, inferred target identities, invented EUIDs, or fabricated proof.

## Gate 0 inventory (2026-09-10T20:31:31Z)

- Repository: Daylily-Informatics/daylily-tapdb; origin is the existing GitHub SSH remote.
- Remote main and peeled annotated 10.0.0: `eadef9e8426ee968fb850328c7a6e2f90353fe29`; tag object `fade52ec6a9d78f782b49823a020d77b77c7b564`. Remote 10.1.0 absent at inventory.
- New isolated branch: `codex/tapdb-service-readiness-20260910`, based exactly on that baseline. Working directory: the worktree containing this ledger. Initial `git status --short --branch`: clean, except this subsequently authored ledger.
- Existing source checkout remains on `jem-dev`, behind 3, and is excluded from writes. Preserved modified files: AGENTS.md, README.md, admin/main.py, admin/templates/index.html, docs/README.md, docs/dag_spec.md, docs/integration-and-embedding.md. Preserved untracked plans: `20260901T100631Z_kahlo_global_dag_tapdb_eligibility_spec.md`, `20260901T102302Z_tapdb_refactor_dayhoff_service_adoption_note_guidance.md`.
- Source hashes (SHA-256): Dewey `ac31b01024ee58bfe62c791bf0d73216c6e7cc2cbed3ad4623c116fa267d53b5`; Bloom `f9aff3ddcca9e6ca09fb77594aab1c68af7c741148e396b707bfb0400e9907ba`; Ursa `2e38759d8105e2279a2e03bd5bc103019387aff68d7a51c3a91c2179fb704101`.
- Source attachments: `dewey_refac.md`, `bloom_refac.md`, `URSA_REFACTOR_NEEDS_DYUEC_VER_MOD.md`. Dewey and Ursa report TapDB 9.0.9 source schemas; Bloom's exact historical schema must be established before qualification, not inferred from an application version.
- Source inventory anchors: migration_identity.py has a fixed 9-table inventory; backup SequenceState assumes increment 1; postrestore uses separate reconciliation and warning-only prefix checks; assert_operator_role requires SUPERUSER/BYPASSRLS; issue 106 is open with the explicit offline-plan/CONNECT-only bootstrap contract.
- Inspection: git status/remote/worktree/ls-remote, source SHA-256, function inventory via rg, CLI registration, schema/RLS, backup manifests/receipts, migration identity, CI configuration, issue 106.
- Environment: fresh worktree-local `.venv` created through `source ./activate`; Python 3.13.13. Installed local PostgreSQL is 16.14, not the required 16.13 Aurora acceptance target.
- Baseline: `.venv/bin/python -m pytest tests/test_backup_manifest.py tests/test_migration_identity_contract.py tests/test_security_posture.py -q` => **54 passed**, 2 dependency deprecation warnings, 0 skipped.
- Full baseline and exact PostgreSQL 16.13/17 matrix are not yet run. No Aurora qualification target or credentials are asserted available.

## Agents, ownership and waves

| Agent | Model | Effort | Owned rows | Exclusive write scope |
|---|---|---|---|---|
| Coordinator | gpt-6-astra | xhigh | BASE-01, REL-01; ledger integration | This ledger, shared CLI registration/db.py/backup.py, dependency/version/CI files, schema migration ordering, integration and publication |
| A | gpt-6-astra | max | INV-01, SEQ-01, SEQ-02 | New daylily_tapdb/identity_inventory.py, daylily_tapdb/sequences.py, cli/identity.py, cli/sequences.py; tests/test_identity_inventory*.py and tests/test_sequence_protection*.py |
| B | gpt-6-astra | max | AUTH-01, AUTH-02 | New runtime_principal.py and cli/runtime_principal.py; security_context.py; schema/rls.sql; new schema/migrations/20260910_203200_aurora_operator_principals.sql; tests/test_runtime_principal*.py |
| C | gpt-6-astra | max | REC-01, BKP-01, MIG-01 | daylily_tapdb/backup/**; migration_identity.py; tests/test_backup*.py, tests/test_migration_identity*.py, tests/test_recovery_floor*.py |
| D | gpt-6-astra | max | API-01, QA-01 | New tests/test_service_readiness*.py and qualification evidence; no implementation edits |
| E | gpt-5.6-sol | high | HAND-01; REL-01 verification | New docs/service-readiness.md, docs/plans/20260910_tapdb_service_readiness_handoff.md; proposed release text/artifact verification |

Coordinator alone writes this ledger and Git commits. Agents report row evidence/status recommendations; never independently commit, stage, push, tag or publish. No nested delegation. Maximum three active subagents plus coordinator. Cross-owner edits require explicit reassignment. No service or production writes by any agent.

Waves: Gate 0 and frozen contracts -> A/B/C implementation in parallel -> coordinator integration -> D independent qualification and E documentation/packaging in parallel, with at most one author correcting defects -> coordinator publication and E published-artifact verification. D returns implementation defects to their author and retests the integrated candidate.

## Frozen interface and safety contracts

### Identity and sequences (A; consumed by C)

- Public identity library in `identity_inventory.py`: `capture_identity_inventory(connection, *, schema_name, target)` and `verify_identity_inventory(before, after, *, conversion_manifest=None)`. Versioned JSON-compatible receipts, explicit physical target identity, exhaustive physical-table/column/constraint/ownership/dependency evidence, bounded deterministic content hashes, immutable UID/EUID/creation/lineage identity comparisons. Read-only capture must not require current runtime schema initialization. Explicit declared conversions cannot waive immutable identities.
- Public allocator library in `sequences.py`: `capture_sequence_inventory(connection, *, schema_name)`, `build_sequence_advance_plan(current, *, floors)`, `apply_sequence_advance_plan(connection, plan, *, writer_fence, receipts_dir)`, and `verify_sequence_floors(current, *, floors)`. Exact signatures may gain required explicit context parameters by coordinator-recorded amendment; do not duplicate arithmetic in backup or migration.
- CLI additions: `db identity inventory`, `db identity verify`, `db sequences advance`, `db sequences verify`. A owns the thin new CLI modules and exposes `identity_app` and `sequences_app`; coordinator wires registry and db groups. Explicit absolute config; read-only inventory; advance defaults to dry-run receipt, apply requires reviewed receipt and unchanged target. JSON machine-readable results.
- Every UID sequence and shared per-prefix generator, including unused/deleted/reserved scopes, must be accounted for through catalogs plus verified mappings, never filename-only ownership inference. Capture increment, min/max, start, cache, cycle, last_value, is_called and dependencies.
- Resumed allocation is the smallest increment-aligned value strictly above all applicable original/final fenced source, target-before-write, assigned/reserved, migration/probe/aborted/recovery floors. Positive noncycling supported generators only; unknown mappings, cache ambiguity, missing generators and exhaustion fail closed.
- A real writer fence plus closure of allocator sessions is required; table locks alone are insufficient. Transactional ALTER SEQUENCE RESTART and strict verification, no ad hoc setval repair. No backward movement or reclaiming allocations from rolled-back transactions.
- Existing stored prefixes are immutable even if template bindings differ. No reminting, TPX substitution, prefix-meaning guesses, or fabricated EUID fixtures.

### Aurora principals (B)

- Public `runtime_principal.py` functions and thin CLI `runtime_principal_app`: bootstrap and bind. Config is the only source of target/operator/runtime/IAM settings.
- Bootstrap default is offline, no connection. Explicit apply uses configured operator against an existing exact database, creates/validates only the configured runtime principal, and grants only CONNECT. Reject role collision, elevation, unexpected ownership/membership/settings, effective database CREATE. Explicit IAM permits only non-admin rds_iam membership.
- Separate receipt-bound bind grants exact restored/migrated-schema privileges and immutable runtime scope. No base schema application/reset, seeding, business writes, sibling grants, startup DDL, auth bypass or ambient GUC authority.
- Prove complete operator visibility under Aurora's actual privileges, preserving FORCE RLS and constrained runtime behavior. Do not blindly classify rds_superuser as PostgreSQL superuser. B reports required changes to coordinator-owned db.py operator assertions/binding helpers.

### Backup, migration and recovery (C)

- Extend the existing backup and receipt-driven migration lifecycle. No second engine or allocator implementation. Use A's shared inventory/advance/verify functions.
- Explicit historical source contracts; remove fixed-table preservation limits; preserve every original identity, row/history and relationship while rejecting undeclared transformations. Bloom source version is evidence to establish, not an assumed default.
- Complete sequence definitions/state in manifests. Durable hash-chained operation intent/floors/outcomes in existing external receipt storage outside the restored database; retain backup, pre-restore and previous failed/recovery floors.
- Missing generators introduced after backup are recreated only from verified retained definitions or block recovery, never silently skipped. Ambiguous commits reconcile before retry.
- Database backups explicitly exclude external configuration/runtime files and role secrets, whose provenance/backup remains separately required. Restore requires explicit principal rebinding. No production in-place restore.
- CLI or schema-ordering changes are proposals to coordinator, not out-of-scope direct edits.

### Qualification and release

- D qualifies transactions, natural identity, typed external references, authoritative lineage, GUI/DAG-v2. Repair only demonstrated substrate gaps; service-domain conversion/policy remains consumer-owned.
- Full PostgreSQL 16.13 and 17 suites; existing 90% branch and per-changed-module coverage; Ruff, mypy, Bandit, secrets checks, GUI, wheel assets and fresh installed wheel. No weakened thresholds or hidden/deselected required tests.
- Failure tests include non-unit increments, both is_called states, unused/missing/exhausted/cached generators, reservations, stale/tampered receipts, concurrent sessions, rolled-back allocations, interruption/ambiguous completion/restart and repeated recovery/new prefixes.
- Isolation tests include complete operator visibility, owner/tenant/global constraints, immutable binding and no runtime DDL. Consumer fixtures cover Dewey stored prefixes, Bloom template-prefix mismatch and duplicate historical assertions, Ursa typed references and transactional claims.
- Required isolated Aurora PostgreSQL 16.13 acceptance uses a separately named authorized target. Missing authority or required skipped evidence blocks release, never becomes local SUCCESS.
- Coordinator follows normal reviewed PR and green CI, exact clean release commit, immutable annotated 10.1.0 tag, wheel/sdist build and repository publish workflow. No admin merge, forced push or retagging. Version collision blocks publication pending amendment.
- E verifies GitHub/PyPI retrieval, SHA-256 and fresh installation; handoff supplies exact pin/commit/tag/dependencies, interfaces/receipt formats and consumer-owned ordering.

## Control ledger

All implementation rows initialized OPEN. Status transitions and test evidence are appended here by the coordinator.

| ID | Area | Requirement | Status | Category | Approval Gate | Owner | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| BASE-01 | Baseline | Source crosswalk, preserved dirty checkout, frozen interfaces | OPEN | plan_amendment | Gate 0 | Coordinator | Inventory above; ledger commit pending | | |
| INV-01 | Identity | Exhaustive physical-schema and immutable identity inventory | OPEN | feature_implementation | Inventory | A | Pending | | |
| SEQ-01 | Allocators | Shared complete inventory and strict floor arithmetic | OPEN | feature_implementation | Allocators | A | Pending | | |
| SEQ-02 | Allocators | Receipt-bound advance and strict verification | OPEN | feature_implementation | Allocators | A | Pending | | |
| REC-01 | Recovery | Durable floors across failures, restore and repeated recovery | OPEN | feature_implementation | Recovery | C | Pending | | |
| AUTH-01 | Principals | Public Aurora runtime bootstrap, issue 106 | OPEN | feature_implementation | Principals | B | Issue contract read; implementation pending | | |
| AUTH-02 | Principals | Complete operator access and explicit restricted runtime binding | OPEN | config_or_startup_contract | Principals | B | User-approved plan; implementation pending | | |
| BKP-01 | Backup | Full required historical-schema backup and restore | OPEN | feature_implementation | Recovery | C | Pending | | |
| MIG-01 | Migration | Exhaustive preservation verification | OPEN | feature_implementation | Migration | C | Pending | | |
| API-01 | Consumers | Qualify existing public consumer contracts | OPEN | contract_test | Consumer contract | D | Pending | | |
| QA-01 | Qualification | Independent PostgreSQL and isolated Aurora acceptance | OPEN | contract_test | Qualification | D | Local PG 16.14 present; exact Aurora target not yet authorized | | |
| REL-01 | Publication | Reviewed merge, immutable tag, published package verification | OPEN | feature_implementation | Publication | Coordinator | 10.1.0 absent at Gate 0; qualification pending | | |
| HAND-01 | Handoff | Exact release/interface/evidence mapping for all three services | OPEN | active_product_contract | Handoff | E | Pending | | |

## Consumer prerequisite crosswalk

| Consumer plan rows | TapDB producer rows | Remains service-owned |
|---|---|---|
| Dewey L02/L04/L05 | INV-01, SEQ-01/02, AUTH-01/02, BKP-01, MIG-01, REC-01 | Pin changes, replacement database/data conversion, service rehearsal/deployment/cutover |
| Bloom T10-00/T10-01; TapDB part of MG01 | SEQ-01/02, REC-01, AUTH-01/02, BKP-01, MIG-01, REL-01 | Exact pin adoption, assertion/template conversion, in-place service migration and acceptance |
| Ursa DB01; TapDB parts of DB04-DB07 | AUTH-01/02, BKP-01, MIG-01, SEQ-01/02, REC-01 | Explicit source-plan pin amendment, references/policy conversion, cleanup/deployment/cutover |

Consumer ordering: inventory -> principal preparation -> backup/restore -> schema migration -> service conversion -> final allocator verification -> service acceptance. This producer release does not discharge consumer production gates.

## Execution evidence

- 2026-09-10T20:31:31Z: isolated worktree created; activation and 54 focused baseline tests succeeded. No service/production writes. Gate 0 ledger prepared before implementation.

## Completion rule

All rows terminal: no. Objective complete: no. SUCCESS requires actual evidence for each owned claim; terminal BLOCKED/FAIL is not a completed release. Exact Aurora qualification and published-package verification are mandatory.
