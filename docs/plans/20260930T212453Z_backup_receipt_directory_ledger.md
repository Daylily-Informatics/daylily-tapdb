# TapDB backup receipt-directory release

## Current continuation state

Objective: release the bounded native backup receipt-directory fix from the
newest published TapDB source, then provide an explicit Dayhoff fleet adoption
handoff. The user approved: “proceed with the new tapdb release from the current
max ver tapdb and we will migrate all of the dayhoff services to use this new
tapdb.” This supersedes the prior hold on this specific native fix. It does not
authorize unrelated TapDB changes, destructive restoration or journal repair.

Baseline verified 2026-09-30: GitHub numeric tags and PyPI both identify 11.0.0
as newest. Annotated tag peels to `6d84bec6c3bb0c728b8c469938e9550f592178b5`.
Reused clean checkout `/Users/jmajor/projects/mega_dayhoff/repos_work/tapdb-scoped-template-import-20260927`;
new branch `codex/tapdb-backup-journal-11-0-1` starts at documentation follow-up
`6b3e0b7`, whose application source equals 11.0.0. No competing active TapDB
writer was found. 11.0.1 was rechecked unused before its immutable tag was made.

Scope: an explicit `backup.receipts_directory` setting, native configuration
CLI support, one shared directory validator/resolver, focused regression tests,
documentation and GitHub/PyPI publication. Existing omission retains the
existing documented location; explicitly malformed overrides fail. Retained
recovery-family checks, descriptors, history, schema, attribution and identifier
allocation are unchanged. No copying or rewriting old receipts.

Source and release are approved. Focused temporary-files/mocked checks were
explicitly approved by the reply “Approve these focused checks”; **19 passed**
in 1.16 seconds (Python 3.13.13, two existing Typer/Click deprecation warnings).
Two earlier failures were mistakes in the new chain-verifier test call and
assertion, corrected without changing the existing verifier. No PR/merge, production test,
database migration or service deployment in this package milestone. The user's
fleet intention is recorded; verify consumer contracts before a rollout. A
10-to-11 migration requires attributable writers, native adoption and bindings,
not simply new pins. Existing Atlas/Bloom images need no rebuild for operator-
only use; embedding the new version fleet-wide requires new service releases.

Completed: native shared directory validation, config update CLI, config loader
and backup service routing; malformed paths fail before backup planning reaches
storage. Existing API/GUI adapters consume that same server-owned setting.
The retained family validation and receipt writer are unchanged. Documentation
and the scoped Dayhoff source inventory/handoff are prepared. `git diff --check`
passed. No schema assets, dependencies, production configs or data changed.

Published **11.0.1** at `c7e1ae366068c58478c28007c2d11929cf392c16`,
annotated tag and feature branch pushed. GitHub release contains wheel and
source archive; both PyPI hashes match. The publisher's immediate download probe
failed during index propagation after successful upload; a later normal pip
download succeeded and its hash matches. No repeat upload or tag movement.
Atlas's controlling ledger/addendum are reconciled and pushed at
`5ba9edf` on `codex/atlas-tapdb11-completion`; unrelated work was preserved.

Next within this package scope: none. Future operator installation and consumer
adoption follow the handoff; no live-service changes happened in this patch
release. Evidence:
`20260930T212453Z_backup_receipt_directory_evidence/qualification.json`;
`20260930T212453Z_backup_receipt_directory_evidence/publication.json`;
[fleet handoff](20260930_tapdb_11_0_1_dayhoff_handoff.md).

Prior evidence: Atlas repository
`docs/plans/20260930_native_backup_receipt_directory_addendum.md` and its
`resume-native-recovery-preflight-20260930.json`. Both retained families are
valid; backup planning rejected their non-default journal paths. TapDB 11's
completed 38-check package qualification remains historical evidence, not
qualification of this patch or any consumer deployment.

## Ledger

| ID | Area / requirement | Status | Category | Gate | Owner | Evidence | Root cause | Terminal note |
|---|---|---|---|---|---|---|---|---|
| J01 | Freeze newest source and release scope | SUCCESS | plan_amendment | G0 | Lead | Remote tags, PyPI, clean checkout, source diff | | 11.0.0 source baseline; no production claim |
| J02 | Shared explicit receipt-directory configuration | SUCCESS | feature_implementation | G1 approved | Lead | backup/service.py; backup/receipts.py; cli/db_config.py | Hardcoded backup receipt path | Native setting implemented; no family or journal rewriting |
| J03 | CLI and API/GUI share the same setting | SUCCESS | active_product_contract | G1 approved | Lead | Native config update and existing adapter resolution; CLI/API chain test | | Server-owned path, no request override |
| J04 | Focused temporary-files/mocked regressions | SUCCESS | contract_test | G2 approved | Lead | qualification.json; 19 checks passed | Two test-authoring errors corrected | No DB/AWS calls; no broad suite |
| J05 | Immutable tag, wheel/sdist, GitHub/PyPI publication | SUCCESS | plan_amendment | G3 approved | Lead | publication.json; 11.0.1 at c7e1ae366068c58478c28007c2d11929cf392c16 | Initial simple-index propagation delay | Both artifacts verified; public download succeeded; no tag movement |
| J06 | Dayhoff fleet handoff and Atlas ledger reconciliation | SUCCESS | plan_amendment | G1 | Lead | Fleet handoff; Atlas ledger commit 5ba9edf pushed | | Per-service adoption remains separate and unperformed |

Counts: **6 SUCCESS, 0 OPEN**. All package ledger rows terminal: **yes**.
Package objective complete: **yes**. Fleet migration complete: **no**.
No PR/merge, database adoption, service deployment or production test occurred.
