# TapDB10.1.6 release and Bloom recovery proposal

Status: source proposal only. No tag, package build/publication, install, fence, migration apply or test has run. Root owns review, the final tagged release and Bloom maintenance.

Update13:52Z: the coordinator approved the explicit observed-original-ACL-restoration and bounded validation proposals. Their source candidate is now prepared; see`20260918T135200Z_restoration_performance_candidate.md`. This earlier document retains the pre-implementation diagnosis. No native operation or package release followed that source authorization.

## Source and operator package

The three reviewed authorization files remain frozen at the source hashes in`20260918T123300Z_candidate_assets.json`. Latest remote numeric10.1.x release remains10.1.5, peeled commit`1566aa345f5d85826b0637660fbd4dac21242e49`; the read-only remote tag check during this preparation found no10.1.6. Recheck exact absence immediately before tagging. Version comes from setuptools_scm: commit the final reviewed allowlist, create an immutable annotated numeric`10.1.6` tag on that clean commit and verify the peeled commit/tag type. Do not change a published tag or merge branches.

Build only the final tagged wheel on EC2 using the supported Python package build command. Retain its SHA256, metadata version and packaged SQL hashes, and require the migration file SHA to equal the accepted native preview. Install that exact wheel into the existing operator venv at`/home/ubuntu/bloom_ops/tapdb101-20260911/venv` with no dependency changes. This release has no dependency delta. No PyPI publication is required for a local, exact-wheel operator installation; publication may be separately requested. The existing Bloom10.0.6 image, TapDB consumer wheel, credential, grants and runtime binding stay unchanged. Never rebind the corrected native routine with older operator tooling.

## Native migration contract

`daylily_tapdb/cli/db.py:1750` loads the existing accepted preflight with its native checksum validator. The receipt is not bound to an operator package version. It is bound to exact target/inventories, all pending SQL hashes, mapping/family/journal authority and strict native comparison. Therefore no extra standalone preview is required solely after installation of matching10.1.6 assets. If the target or recovery evidence changes during proper fence reconciliation, a new preflight may be required; the native mismatch must not be ignored.

`migration_identity.py:1192–1212` verifies the fence, locks all target tables ACCESS EXCLUSIVE, recomputes the native preflight and compares all fields before SQL. Later stages verify preservation, retained sequence floors and post-commit identity, then release only through the native retained-session protocol. There is no automatic fence timeout. PostgreSQL control-session values at13:32Z were statement_timeout=0, lock_timeout=0, idle_in_transaction_session_timeout=1d. Do not enter a42-minute fenced scan merely to update one function.

The eventual native command shape, **not currently runnable because the historical epoch is unresolved**, is:

```bash
sudo /home/ubuntu/bloom_ops/tapdb101-20260911/venv/bin/tapdb \
  --config /home/ubuntu/bloom_ops/tapdb101-20260911/operator.yaml \
  --client-id bloom --database-name bloom-day db schema migrate --apply \
  --preflight-receipt /home/ubuntu/bloom_ops/kahlo-resolver-20260918T123300Z/native-preview.json \
  --receipt /home/ubuntu/bloom_ops/kahlo-resolver-20260918T123300Z/native-apply.json \
  --sequence-mappings /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/source-sequence-mappings.json \
  --recovery-family /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/recovery-family.json \
  --receipts-dir /home/ubuntu/bloom_ops/tapdb101-20260911/journal \
  --establish-writer-fence \
  --control-config /home/ubuntu/bloom_ops/tapdb101-20260911/control-operator.yaml \
  --provider-contract /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/aurora-provider-contract.json
```

An exact successful new native quarantine receipt would also be required for quarantine promotion. No path or success receipt is invented here. Application must use a freshly reviewed valid receipt after historical recovery. The original native preview remains valuable evidence, not permission to override a mismatch.

## Historical epoch: concrete blocker before application

Fresh evidence is in`20260918T133201Z_native_fence_owner_read.json`. The current ACL is exactly restored to the old epoch's original ACL, the DB is open and the instantaneous census is clear. Nevertheless the native journal ends with unresolved takeover intent`000033-20260913T090834Z`; native quarantine was reached at`000036`, and no native release was recorded.

Existing owner routes:

1. `db sequences reconcile --fence-intent-receipt-id 000033-20260913T090834Z` calls`build_writer_fence_takeover_plan` (`sequence_fence.py:756`). Atlines826–833, an open takeover must still have operator-only effective CONNECT. The fresh restored normal ACL fails that requirement. This conclusion is from exact source plus owner evidence; no failing live takeover plan was run merely to repeat the guard.
2. `--quarantine-receipt` on native acquire requires the exact latest journal-bound quarantine and matching current quarantine ACL (`sequence_fence.py:1220–1258`); the current original ACL differs.
3. `--release-intent-receipt-id` accepts only a real release_intent with committed allocator evidence. The takeover/quarantine-open receipt cannot be relabeled as one.

Concrete next decision: the TapDB owner needs to review an explicit, audited recovery of this observed original-ACL restoration. It must retain exact target and epoch, unchanged original ACL, physical identity, all original floors, current complete session/worker visibility, native allocation safety and the truthful historical outcome. It must not invent a successful old migration or silently reopen normal access. No source or database change for this additional recovery capability is authorized by this proposal. Root is coordinating the required maintenance boundary.

## Bounded repeated-family validation proposal — no edits

Locations: `backup/recovery.py:180` (`recovery_family_state`), especially191–210 and374–389; `migration_identity.py:469` (`build_migration_preflight`), especially540–543,553 and630–632.

The observed journal has38 receipts and36 embedded declarations that equal the one exact canonical family. The function first validates that family and its complete rooted hash chains, then line210 fully revalidates those same roots for each identical embedded declaration. Four such calls plus three standalone reads produce155 scans of508,553,329bytes each.

Proposed narrow diff:

1. Keep the initial full family validator. For an embedded declaration **exactly equal** to that validated canonical object, reuse only its already-established descriptor validation and required-root membership. Different declarations still run the existing validator before any foreign-family exclusion or changed-family rejection. Every receipt, chain edge, origin/member, floor, intent/outcome and pending-state check still runs.
2. During the existing full history read, retain each root's verified head and exact ordered receipt IDs. Before returning, re-read and fully verify each root again, then require identical head and IDs. Added/removed/reordered/changed/corrupt receipts or head changes fail. This is a bounded invocation-local proof, not a global or persistent cache.
3. In `build_migration_preflight`, retain start heads only after full family validation and require identical heads at the end, after its existing native validations. This rejects changes between its repeated native stages. Do not add these transient heads to the persisted comparable receipt, which intentionally excludes the application's own legitimate nonallocator fence events between separate operations.

For the observed unchanged one-root journal this changes each38-scan state call to3 scans, approximately15 total scans (~7.6GB), plus cheap start/end head reads. The rough duration estimate is about4 minutes from the measured42-minute original; this is not runtime validation or a promised maintenance duration. No global cache, TTL, default family, journal pruning, alternative root, caller-injected reviewed state or skipped epoch check is proposed. Root requested prioritizing the actual historical fence contract before approving this source expansion.
