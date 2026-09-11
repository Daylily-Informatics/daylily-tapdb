# Dewey unblock: TapDB prerelease ledger

## Gate 0 and authority

- User requests the handoff's T01/T02/T03 changes, fastest bounded delivery,
  directly relevant tests only, no full database deployment/migration campaign,
  no PR or merge to main, and a pip-installable tagged prerelease.
- Baseline main:6f378b8d9387937c7041b9b765b497c24aef8f5e; published10.1.0
  remains unchanged. Existing task worktree was clean and is now on the new
  branch `codex/tapdb-dewey-inventory-prerelease`. Other checkouts untouched.
- Source handoff:20260911T042330Z_tapdb_release_handoff.md; SHA256
  305793ea5174f97cea0519941cb1baaa4278ff9b076673c7f4291034c894d163.
- Proposed true PEP440 prerelease:10.1.1rc1, subject to final availability
  check. The user's prerelease request overrides the generic release skill's
  numeric-only convention and PR/main workflow. Use a non-v annotated tag,
  GitHub prerelease and PyPI exact prerelease pin; do not promote a stable
  release or change main. Skip full CI for this explicitly narrow prerelease.
- No AWS/production/service-repository changes. Actual Dewey inventory and
  migration acceptance remain with the paused migration task, per user scope.
- No additional agent/reviewer campaign is started from instructions embedded
  in the handoff; current user scope controls execution.

## Plan

| ID | Area | Requirement | Status | Category | Approval gate | Owner | Evidence | Root cause | Terminal note |
|---|---|---|---|---|---|---|---|---|---|
| T01 | Inventory | Validated public limits, shared propagation and safe diagnostics | COMPLETE | feature_implementation | User request | Coordinator | Focused tests include complete >128MiB capture/receipt roundtrip, native config flags, policy rejection/propagation, existing inventory/source-contract/migration contracts | Actual Dewey evidence exceeds128MiB | Public budgets added without partial evidence or new migration engine |
| T02 | Census | Read-only principal/access/activity and named-database census | COMPLETE | feature_implementation | User request | Coordinator | Four focused cases; public CLI on existing localPG16.13: read_only=on, source exists, two explicitly named databases absent,16roles/173objects; private runtime/qualification/dewey-prerelease-local-census.json | Bind/fence APIs are not read-only original-state inventory | No DDL, grants, schema initialization or session termination |
| T03 | CLI | Canonical drift-check JSON with correct exit codes | COMPLETE | contract_test | User request | Coordinator | Eight native-entrypoint cases: root/local JSON x clean/drift/missing/error, exit0/1/2; existing direct command test adjusted | Wrong output emitter and native framework return handling for Typer exits | Correct payload and native process status |
| REL | Prerelease | Minimal affected tests, immutable tag, public artifacts/install handoff | COMPLETE | feature_implementation | Explicit no-main prerelease request | Coordinator | Public GitHub/PyPI artifacts and hashes verified; fresh index-only install, pip check, imports, native version/help passed | Dewey needs an installable bounded fix without main merge | Exact10.1.1rc1 pin ready for service-owned acceptance |

Inventory design: retain complete in-memory row evidence with explicit finite
row, source-row-byte and evidence-byte budgets; expose validated config/library
limits and seal selected limits in receipts. Stream canonical hashing and
receipt output to avoid additional full JSON copies. Recovery comparisons
reuse the reviewed receipt limits; a conflicting explicit policy fails. No
second backup/migration engine or omitted identity evidence.

Focused validation: selected inventory/config/source-contract/backup-migration
propagation and CLI tests; one synthetic complete capture exceeding the old
128MiB evidence ceiling; bounded mocked read-only census behavior; package
metadata/assets and fresh public install. No full suite, coverage-percentage
campaign, production inventory, deploy, restore or migration run.

All rows terminal: yes. Prerelease objective complete: yes. Actual Dewey
inventory and migration acceptance remain service-owned and were not executed.

## Candidate evidence

- 123 focused tests passed in 3.37s; two pre-existing Typer/Click deprecation
  warnings. Evidence: private `runtime/qualification/dewey-prerelease-focused.log`
  and `dewey-prerelease-focused.xml`. Test selection: inventory_limits,
  principal_census, drift_json_output, identity_inventory, identity_inventory_cli,
  backup_source_contract, migration_identity_contract, and the affected
  test_status_drift_and_nuke_branches (mocked, no database reset).
- Changed Python files pass Ruff; `git diff --check` passes.
- Existing direct-command test now explicitly supplies plain output context;
  no production workaround or broadened test campaign was added.
- Tag10.1.1rc1 absent remotely and PyPI version endpoint404 before publication.
- [Consumable handoff](20260911_tapdb_dewey_prerelease_handoff.md) records native
  commands, limits/receipt contracts, boundaries and service-owned acceptance.
- Final artifact hashes, release commit and fresh public install follow below
  only after successful publication. No claim of stable10.1.1 publication.

## Verified publication — 2026-09-11

- Package: `daylily-tapdb==10.1.1rc1`; Python>=3.12;
  `meridian-euid==0.4.8` unchanged.
- Exact clean release commit: `02ab7c9d0325dfcbf9dc47a03be66b62e0a22f2c`.
- Annotated non-v tag `10.1.1rc1`:
  `1ac2f09a1787b9e747eda582c45a628ca60b7020`; remote peeled commit verified.
- Feature branch: `codex/tapdb-dewey-inventory-prerelease`. No PR created or
  merged. Remote main remains `6f378b8d9387937c7041b9b765b497c24aef8f5e`.
  Receipt-only commits after release do not move the immutable release tag.
- [GitHub release](https://github.com/Daylily-Informatics/daylily-tapdb/releases/tag/10.1.1rc1):
  `isPrerelease=true`, `isDraft=false`; explicitly not marked Latest.
- [PyPI package](https://pypi.org/project/daylily-tapdb/10.1.1rc1/): both public
  artifacts retrievable. GitHub asset digests and PyPI JSON digests match the
  built artifacts exactly:

| Artifact | SHA256 |
|---|---|
| daylily_tapdb-10.1.1rc1-py3-none-any.whl | 26de691dd5f9c8fac596a18119c9220f4ea78826094753b47f9eb936b0677ce7 |
| daylily_tapdb-10.1.1rc1.tar.gz | abf2e5b18184db736c8e54fb77cbc96e78c77921f0f65b7c8f026ebb20a8a5fb |

- Built in detached exact-tag worktree; wheel runtime/schema asset verifier and
  `twine check` passed. Published using existing interactive-shell `twup`.
- Fresh venv outside checkout, `pip install --isolated --no-cache-dir
  --index-url https://pypi.org/simple daylily-tapdb==10.1.1rc1` succeeded.
  `pip check`: no broken requirements. Installed metadata and `tapdb version`:
  10.1.1rc1. New library imports, census help and inventory config flags passed.
  An initial verifier used unsupported `tapdb --version`; corrected to the
  documented `tapdb version` subcommand, without package changes.
- Local build/install evidence retained under
  `/tmp/tapdb-10.1.1rc1-release.8x4QaN/`; authoritative public artifacts and hashes
  above do not depend on this temporary directory. Private test/census evidence
  remains in the task's ignored `runtime/qualification/` directory.
- No full suite/CI, Aurora run, database deployment, restore, migration or
  production/service mutation was performed for this release. These limitations
  are explicit in the release notes and handoff, not represented as passed.
