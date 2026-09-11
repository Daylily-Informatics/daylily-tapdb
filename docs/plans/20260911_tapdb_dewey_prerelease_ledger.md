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
| T01 | Inventory | Validated public limits, shared propagation and safe diagnostics | IN_PROGRESS | feature_implementation | User request | Coordinator | Baseline fixed250000rows/8MiBrow/128MiBevidence; shared capture callers inventoried | Actual Dewey evidence exceeds128MiB | |
| T02 | Census | Read-only principal/access/activity and named-database census | OPEN | feature_implementation | User request | Coordinator | Public historical census absent | Bind/fence APIs are not read-only original-state inventory | |
| T03 | CLI | Canonical drift-check JSON with correct exit codes | OPEN | contract_test | User request | Coordinator | Existing print_text JSON suppressed by global JSON | Wrong output emitter | |
| REL | Prerelease | Minimal affected tests, immutable tag, public artifacts/install handoff | OPEN | feature_implementation | Explicit no-main prerelease request | Coordinator | Pending | | |

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

All rows terminal: no. Objective complete: no.
