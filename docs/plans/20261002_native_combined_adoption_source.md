# Combined adoption and retained floors — local source correction

## Current continuation state

Local source only, based on published11.0.2 commit `9acb8bf7c72f8c95941d133cb9127c014c3fa015`. Controlling fleet disposition is Dayhoff review round59; root owns the fleet ledger. The original six services were restored by the coordinator without adoption/apply/binding. This document does not authorize a release, install, outage or native execution.

The patch supports one reviewed operation: advance above all original retained/family reservations, then establish adoption and allocate exactly the two bundled templates and their audit records in the same transaction under the existing physical writer fence. SQL assets, bundled templates, model/context/CAS/binding/history interfaces and ordinary allocator behavior remain unchanged. No version bump, dependency change or consumer source/image change is included.

Focused checks are authored in `tests/test_combined_adoption_floors.py`; existing adoption checks receive the required explicit journal argument. No tests, lint, coverage, imports, package installations or production/database operations have been run for this patch. Independent source review is ongoing; source implementation is not runtime validation.

## Public contract

`plan_adoption(connection, cfg, *, receipts_dir, recovery_family=None)` requires the exact existing canonical original journal, including an explicitly selected empty first journal. It returns a sealed `format` and `schema_version` of `tapdb.integrity-adoption/v2`.

The original `target`, `tables`, `catalog`, `sequences`, `assets` and `new_core_templates` fields remain. New fields are:

| Field | Meaning |
| --- | --- |
| `history` | Exact original journal, explicit family or null, every declared root's head and receipt checksums, complete historical floors and inventory seals |
| `allocator_plan` | Existing sealed `tapdb-sequence-advance/v1` with the minimal aligned values above all applicable prior/assigned/allocated floors |
| `allocation_paths` | Actual template/audit UID identity dependencies and scoped EUID prefix bindings; aggregate maximum calls for the two exact definitions |
| `allocation_reservation` | Sealed `tapdb-sequence-allocation-reservation/v1`; finite call bounds and durable reservation ceilings, including full sequence cache blocks |
| `allocation_surface` | Sealed `tapdb-adoption-allocation-surface/v1`; validated preexisting template/audit table/type/default/index/constraint/rule/access-method metadata |

`capture_allocation_surface(connection, cfg)` is a public read-only preflight callable. Keep the complete catalog receipt in private operational evidence where appropriate; a sanitized projection can retain its seal, schema, relation names and column/index/constraint counts. It performs no DDL, grants or row writes. Capture under the existing explicit operator transaction/context; do not invent alternative credentials/config paths.

CLI planning requires `integrity-adopt plan --receipts-dir /absolute/original-journal --output /absolute/new-plan.json`, optionally `--recovery-family /absolute/original-family.json`. The existing apply command uses the plan's sealed exact family/journal. Old snapshot-only v1 plans are rejected; no compatibility fallback or automatic plan conversion exists.

`adopt_integrity(cfg, control_cfg, *, plan, receipts_dir, provider_contract=None)` retains its separate target/control sessions. Success is a sealed `format` and `schema_version` `tapdb.integrity-adoption-receipt/v2`, retaining `epoch`, `templates`, `plan_sha256`, `runtime_rebind_required`, `consumer_write_context_required`, `fence_released` and `allocator_receipt`, and adding `allocation_catalog_sha256` and the acquired `fence_sha256`.

## Ordering and proof

1. Revalidate the exact historical checkpoint before acquiring the native fence. After acquisition, permit only the exact two receipts produced by that acquired fence on the original root; every other root and the original receipt prefix must remain unchanged. Re-read actual complete retained/family floors and definitions, preventing a resealed plan from omitting history.
2. Lock the existing domain/audit tables. Revalidate the full original snapshot and sealed allocation surface before any advance or adoption DDL. Recompute the native advance plan and allocation reservation; stale source/history/identity/mapping/limits/capacity stops.
3. Apply the native minimal advance before any bundled template/audit identity allocation. Its original `sequence_advance` intent additionally retains a finite `native_allocation_reservation` for every permitted template/audit generator. Shared prefixes sum their calls; counts come from exactly two bundled definitions. Cache blocks and non-unit increments are included.
4. Install unchanged native assets, runtime grants and history baseline. Verify preexisting record/audit digests and exact authorized advanced sequence state. Validate the canonical installed runtime catalog and bounded insert surface before seeding. The unchanged loader may only insert or skip the exact two definitions; governance object creation stays disabled.
5. `finalize_sequence_allocation` captures the actual post-template inventory. It requires the exact journaled original pending intent/plan/family/operation, unchanged definitions/ownership/dependencies/physical identity, permitted mapping continuity, and counts implied by the actual inserted/skipped summary. It verifies prior-floor dominance and actual assigned/high-water values within the pre-recorded cache-aware reservation. It preserves original intent and plan references, records the adoption epoch and preallocation receipt hash, and emits a final pending result. The pre-template pending result cannot be acknowledged as committed for a reserved native allocation.
6. Commit that one transaction, record the exact final native committed allocator receipt, then use the existing fresh inventory/session/provider/original ACL release checks. Fence release compares the actual final inventory, not a pre-template snapshot.

The operation's own reservation may equal or exceed its current next value. It is a conservative reservation for a **future maintenance attempt**, not a prior floor that this same operation must immediately exceed again. Prior floors are never removed or reinterpreted. Ordinary issuance and the standalone native sequence operation retain their existing semantics.

## Supported boundary

This bounded path requires existing, unambiguous WX/WSX/XX/AY/MSG generators for the unchanged schema statements and GVR/XRF for the bundled definitions. All six captured fleet plans already contain them. A missing historical generator or absent fixed/bundled prefix fails during read-only planning; this patch does not add generator creation/recreation or birth/rollback reconciliation. The generic loader's existing ensure-prefix implementation remains unchanged; it can verify/annotate these existing generators without consuming sequence values.

Only ordinary native heap template/audit tables with supported built-in scalar types and literal/native time defaults are supported. Custom/domain types, partition routing, custom expression functions/operators/index operator classes/access methods, rules or extra canonical trigger paths stop before unbounded allocation. These are supported-input limits, not permission to remove application objects or rewrite schema.

## Failure and recovery

Any failure after acquisition retains the closed native writer fence. No implicit retry/reopen occurs. A database rollback does not erase external intent or prove sequence reservations were never exposed. Before template insertion, the native intent already covers all permitted allocations and cache blocks; recovery readers retain those original floors on failure or ambiguity.

If insertion/finalization fails, do not mark the pre-template pending result committed. If commit acknowledgement is lost, do not infer rollback. If commit succeeds but external outcome or fence release fails, the database may already have the adoption epoch; repeated adoption remains prohibited. Use existing native quarantine/observed reconciliation and exact retained evidence. The original sequence intent ID, advance-plan hash, target, physical identity and family remain usable by current recovery readers. This patch does not add a cleanup path or manufacture a terminal outcome.

Publishing/installing a new operator-only patch and executing its combined plans require the coordinator's separate release scope and final independent review. Existing11.0.2 consumer images can be reused only after the reviewer confirms unchanged runtime contracts/assets; this document does not claim deployment or acceptance.
