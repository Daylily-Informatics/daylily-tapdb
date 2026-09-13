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
