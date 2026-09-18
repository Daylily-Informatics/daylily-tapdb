# TapDB10.1.6: observed restoration and bounded journal validation

Created2026-09-18T13:52:00Z. Status: source candidate for coordinator review. No release, package build/install, native preview of these new functions, reconciliation, fence, database application or tests have run. The coordinator explicitly authorized these two additions after reviewing the retained native evidence and proposal. This document supplements the authorization ledger; it does not change its production receipts.

## Demonstrated state and exact recovery scope

The fresh native receipt`20260918T133201Z_native_fence_owner_read.json` identifies Bloom's still unresolved takeover intent`000033-20260913T090834Z`, its completed native quarantine, the exact already restored original ACL, open database gate and clear instantaneous census. Family`d869afb8-47a7-41dd-813a-f24d0ca96fa8` has no pending allocator/recovery operation. These are different states: allocator pending=0 did not settle the writer-fence epoch. No successful old migration or release is inferred.

The new explicit native mode is`db sequences reconcile --observe-original-acl-restoration --fence-intent-receipt-id ...`. It is not an automatic path used by ordinary takeover, acquire, migration or release. An explicit original recovery family is required. The public implementation is`daylily_tapdb.sequences.reconcile_original_acl_restoration`; the CLI exports it through the existing owner command.

The mode requires all of the following before producing a plan or appending its audit receipt:

1. Complete original and family hash chains; the exact latest unresolved takeover intent; exactly one real completed native quarantine receipt bound to that intent, target, physical database, original ACL, family and journal.
2. Exact configured target, original explicit sequence mappings, same physical database/server, original authenticated operator/control database, current supported provider identity and unchanged roles/provider evidence during observation.
3. A gate that is already open and the complete original ACL already restored, including its default-ACL representation. It never issues ALTER DATABASE, GRANT, REVOKE, nextval, setval or object mutation.
4. Explicitly verified read-only repeatable-read control/target transactions (SHOW checks on the target and every fresh control observation; both isolation values are sealed in the receipt), native complete statistics visibility, only the observation's exact target backend, no prepared transactions or enabled subscriptions, and supported worker/extension inventory. The control check starts before the target connection; a fresh control transaction repeats identity/ACL/provider and census checks after capture.
5. Terminal recovery/family state; every retained local/family floor and generator definition; complete current sequence inventory; successful native floor verification; a second matching inventory; unchanged fully reverified family and source journals.
6. On apply, exact equality with the sealed reviewed plan. Any changed allocator, role, ACL, provider or journal requires a new explicit review.

Apply appends one native hash-chained`original_acl_restoration_reconciled` event to the existing journal. It preserves every older receipt and floor. The event states`old_operation_outcome=unknown`, `establishes_writer_fence=false`, and`requires_new_review=true`. The journal timestamp and exact observing backend are retained. `active_epoch` accepts only the new sealed event bound to the exact earlier takeover checksum/target/physical identity/family, with verified floor and read-only observation fields.

This is an observation of already restored access, not writer exclusion. Normal access is open, so no promise of continued inactivity follows from a clear instantaneous census. The later function migration must obtain a separately reviewed native writer fence and repeat all apply-time checks. Pending or unsafe allocator state cannot be resolved by this mode; the observation fails instead.

## Prepared native commands; not executed

These commands require the final reviewed10.1.6 operator package and a new unused receipt path. They do not require a Bloom container change for the authorization function repair. The coordinator owns admission/drain coordination, review, installation and any later apply.

```bash
sudo /home/ubuntu/bloom_ops/tapdb101-20260911/venv/bin/tapdb \
  --config /home/ubuntu/bloom_ops/tapdb101-20260911/operator.yaml \
  --client-id bloom --database-name bloom-day db sequences reconcile \
  --control-config /home/ubuntu/bloom_ops/tapdb101-20260911/control-operator.yaml \
  --receipts-dir /home/ubuntu/bloom_ops/tapdb101-20260911/journal \
  --fence-intent-receipt-id 000033-20260913T090834Z \
  --observe-original-acl-restoration \
  --sequence-mappings /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/source-sequence-mappings.json \
  --recovery-family /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/recovery-family.json \
  --provider-contract /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/aurora-provider-contract.json \
  --receipt /home/ubuntu/bloom_ops/kahlo-resolver-20260918T123300Z/original-acl-restoration-plan.json
```

After explicit coordinator acceptance of that real plan, the same command may add`--apply --preflight-receipt .../original-acl-restoration-plan.json` and replace`--receipt` with the new`.../original-acl-restoration-result.json`. No plan/result at those paths is claimed to exist. The command appends the observed event only; it does not perform the function migration. Readback must show the exact new native receipt and no active fence epoch, while all ACL/actor/token/template/runtime values remain unchanged. The subsequent fresh migration preflight may differ from the earlier13:15 receipt because new observed inventory/floor evidence is now retained. Native comparison, not an old hash assertion, controls reuse.

## Journal validation implementation

`backup/recovery.py:recovery_family_state` still validates the canonical family and all rooted chains first. Only an embedded descriptor exactly equal to that fully validated object avoids a redundant descriptor validation; a different descriptor still runs the original full validation before any exclusion or mismatch. Every receipt, physical member, intent/outcome, pending operation, definition and floor is processed. Each rooted chain is captured with its head and ordered receipt IDs and fully reread/verified at the end. Added, missing, reordered, changed or corrupt journal content fails.

`migration_identity.py:build_migration_preflight` now captures fully verified heads and ordered receipt IDs before any database inventory capture, then fully rereads/verifies them after all native scope/generator checks. Any change during that invocation fails. The transient guard is not added to the persisted comparable receipt: the application's own legitimate nonallocator fence events between separate invocations remain subject to the existing native floor/member comparison.

No global cache, TTL, retained injected state, alternative journal, receipt pruning or integrity bypass was added. For the previously observed unchanged one-root38-receipt journal, each family-state pass falls from38 scans to3. The previously155-scan preflight becomes approximately17 full scans after the new enclosing start/end guards, about8.65GB of repeated journal reads rather than78.83GB. This is a source-derived count, not a measured runtime result. The original42m07.68s duration cannot be claimed as the new duration, and the final release still needs its accepted native observation.

## Source ledger

| Row | State | Evidence or remaining gate |
| --- | --- | --- |
| R0 | SUCCESS | Existing owner evidence demonstrates restored original ACL and unresolved native epoch; original receipts unchanged |
| R1 | IN_PROGRESS | Explicit observation-only reconciliation and exact epoch recognition; final isolation-guard review and release gate pending |
| R2 | IN_PROGRESS | Canonical descriptor dedup plus complete start/end chain/head guards; final isolation-guard review and release gate pending |
| R3 | OPEN | Final annotated10.1.6 release and exact operator wheel installation |
| R4 | OPEN | Native read-only restoration plan under final source; real plan review |
| R5 | OPEN | Authorized journal-only observation apply/readback |
| R6 | OPEN | Fresh accepted native function migration preflight/fence/apply and existing-reader acceptance |

The original authorization SQL migration is byte-unchanged. The exact eight-file source allowlist and child-document hashes are in`20260918T123300Z_source_allowlist.json`. Source ready is not released, installed, applied or accepted.
