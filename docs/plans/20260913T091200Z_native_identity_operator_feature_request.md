# Feature request: bounded native identity rollout and aborted-migration recovery

Context: Ursa/Bloom identity repair on2026-09-13. User requested no further TapDB releases and a minimal integration path. TapDB10.1.4 identity migration was aborted by physical-preservation checks because the new table and FK back-reference schema effects lacked declarations.10.1.5 publishes corrected declarations; runtime SQL/catalog/resolver are unchanged. No further package release is authorized in this effort.

## Requested owning capabilities

1. Expose operator CLI arguments for an explicit runtime configuration plus operator credential configuration, preserving the immutable runtime config identity. Current native Python composition is possible but the CLI encourages an operator-path identity mismatch.
2. Native additive-migration authoring should declare new tables and FK parent-schema effects together or provide a bounded declaration-generation command. Preserve strict row/identity checks; do not silently accept schema changes.
3. Provide progress/phase/elapsed-time output for full identity hashing during preflight, apply and abort recovery. Current operations can run for several minutes with no output.
4. Provide an explicit receipt-bound availability-restoration operation after a rolled-back migration, preserving original ACL, retained allocator floors and recovery epoch, without requiring users to infer the sequences-advance recovery workflow. No raw ACL override or guard bypass.

## Current workaround

Use existing native `db sequences reconcile` to recover the original failed fence; use a reviewed monotonic `db sequences advance` plan/apply against actual quarantine retained floors when restoring prior service availability. Use the public native runtime-principal API with the existing Bloom helper's exact runtime/operator configuration composition for binding and identity access. This is an operational composition, not a compatibility reader or a weakened authorization policy.

Built Bloom10.0.6 with TapDB10.1.4 is source-compatible with a successful10.1.5 migration because runtime/schema assets are identical. Do not rebuild it solely for corrected migration declaration metadata.

No implementation or further release is implied by this feature request.

## Observed latency cause and manual restoration requirement

At09:27:56UTC the interrupted read-only planner stack was inside `require_family_member` → `recovery_family_state` → `validate_recovery_family` → `_history` → `verify_receipt_chain` → repeated receipt checksums. It was not performing a new required data mutation. A previously completed quarantine receipt already retained the sequence inventory and all floors. Avoid recursively revalidating every historical receipt multiple times inside the same operation when a successful preservation receipt is explicitly trusted. Preserve epoch/identity checks without rescanning the full historical chain.

The user explicitly waived repeated preservation checks and asked for a minimal workaround. The retained result requires56of66sequence next-values to advance by1; discarding these floors is not an acceptable shortcut. A bounded manual restoration must apply only those monotonic floors, restore exactly the recorded original ACL, and emit a truthful manual receipt. Native epoch reconciliation remains an explicit upstream follow-up; do not forge a native release receipt or claim the journal was reconciled by manual work.
