# Operator-only compact audit preservation — proposed 10.1.8

Coordinator release review, 2026-09-27T00:51Z: source propagation, strict attribution/catalog comparison and finite audit/non-audit limits reviewed. Exact Bloom policy/dependency evidence corroborates PostgreSQL rendering; schema assets are unchanged from10.1.7. Seven focused pure cases reused; no broad checks. Remote tag10.1.8 absent and PyPI10.1.8 returned404 before release. Publish operator-only10.1.8 under existing necessary-revision authority; application images remain10.1.7. Publication is not database migration or deployed acceptance. QEO administrator session is now available; scoped token creation awaits browser action-time confirmation.

## Current continuation state

Implementation is ready for root's bounded source review. Bloom's owning read-only audit catalog now corroborates the exact policy deparse and catalog transition; no further source change was necessary. Source is ready for root's final review/release. No commit, tag, publication, installation, full inventory scan or database mutation has been performed by this agent. Services remain on 10.1.7. Root owns release and production; database cutover remains gated on complete credentials and native receipts. Source uses the existing `codex/tapdb-reviewed-scope-correction-20260926` branch based on released 10.1.7 (`5ab19eb08ce6195c8e108199f911326e0b200042`). No schema SQL/assets, runtime, API wire, allocator or application pins changed. Version is setuptools_scm tag-derived; the generated ignored `_version.py` is not an authored release change.

Owning input: Atlas evidence `kahlo-audit-catalog-closure.json`, SHA256 `c37d22b677a3b289dfe390dbcc8cb7d5c397d43edee9d40487fe66ca9ec566a7`, captured read-only by root. Five applied, fourteen pending, zero unknown applied assets. Audit has its complete existing columns, ordinary nonpartitioned BIGINT uid primary key, only the EUID INSERT trigger, no policies, RLS disabled. The exact pending expanded hashes are retained in native `audit_inventory.REVIEWED_ASSETS`.

Six focused pure tests passed in 0.18s. Root then requested one narrowly scoped propagation case; that new test alone passed in 0.50s, exercising real CLI target resolution, source contract, current recapture, interim/postflight physical verification and committed recovery finalization with only database/journal IO mocked. Seven focused cases now have passing evidence; no full-suite rerun. Initial run was five pass/one fail: unchanged-content comparison admitted changed summary counters; fixed to require exact summary equality without a transformation. No PostgreSQL, broad inventory/recovery suite, container or network tests. Further checks require reporting their exact scope first. One deployed corrective cycle remains the user limit.

## Concrete change

New explicit `inventory_mode: audit_log_ordered_digest/v1` produces `tapdb-identity-inventory/audit-digest-v1`. It is selected only by the operator config root or public capture target mapping. Original full-row format is unchanged; there is no automatic switching, receipt conversion, table exclusion or fallback. 10.1.7 consumers reject the new format.

Only audit_log streams to an ordered, length-framed whole-row digest. Every column contributes, and uid must strictly increase. The raw and expected-after digests are calculated together: the expected digest changes only changed_by when the SQL expression `changed_by IS NULL OR trim(changed_by)=''` says to use the named historical attribution constant. SQL computes this predicate, avoiding a Python/SQL whitespace disagreement. The final **raw** digest must equal the approved expected digest, with identical row count and unchanged scope/deletion counters. Unexpected added/deleted rows or any other value change fail. Original/current and postflight/reconciliation comparisons require exact raw summary equality.

Full catalog metadata remains captured, with policy roles added for compact audit. The exact transition permits changed_by nullable→NOT NULL, enabled/forced RLS and exactly the native audit scope policy plus exact-owner operator policy. Only those policies' catalog dependencies may differ; all other table metadata must match. No broad audit schema waiver is used. Migration preflight admits the exact fourteen expanded assets, or zero pending assets for final/no-op/reconciliation state; partial/different closures fail. Normal tables still use native per-row transformation evidence. Source contracts require no special adapter: they retain the new inventory and call the same now format-aware native verifier.

`max_rows`, `max_row_bytes` and `max_receipt_bytes` remain explicit finite native limits. New mode additionally caps streamed audit source at 16 GiB. Audit uses a 64-row server cursor and constant retained digest state. Non-audit evidence remains memory-resident under its existing limit; total process RSS is not asserted or measured locally. The observed audit count 2,239,149 therefore requires an explicit total row limit above that count, including every other table; the default 250,000 cannot pass. Keep the 128 MiB ordinary evidence cap unless a separate measured non-audit receipt proves a justified finite adjustment. Do not simply raise the original full audit receipt cap.

## Files

- `daylily_tapdb/audit_inventory.py`: one audit-only digest/counter/capture/catalog/closure contract, about 230 lines including exact reviewed hashes.
- `daylily_tapdb/identity_inventory.py`: explicit format dispatch, compact audit capture, recorded limits/usage, receipt comparison, policy-role capture, mode propagation with reviewed limits.
- `daylily_tapdb/migration_identity.py`: compact audit projection without fabricated empty rows, retained scope checks, exact pending closure, shared physical verification of audit.
- `daylily_tapdb/cli/db_config.py`, `cli/identity.py`, `cli/db.py`: root config selection and target propagation; no application config changes.
- `tests/test_audit_inventory_compact.py`: seven pure tests for attribution/whitespace/value preservation, order/duplicates/catalog denial, native verifier/mode propagation, scope projection, evidence shape/counters, exact expanded closure admission.
- Earlier `20260927T014500Z_kahlo_audit_catalog_closure.py` and its handoff are the completed diagnostic helper artifacts, not a runtime dependency of the amendment.

## Root execution handoff

After root review/release/install of operator 10.1.8 only, set the exact private Kahlo operator YAML root:

```yaml
inventory_mode: audit_log_ordered_digest/v1
inventory_limits:
  max_rows: 3000000
  max_row_bytes: 8388608
  max_receipt_bytes: 134217728
```

This is an explicit reviewed finite ceiling, not an assertion that the current total count is three million. Existing source receipt failures stay preserved. Do not change the runtime YAML or application image. Use native `tapdb --config <exact operator.yaml> identity inventory --source-version 9.0.9 --receipt <new absolute source receipt>` with exact reviewed sequence mappings and recovery-family arguments from the controlling plan. Native CLI emits full evidence JSON: redirect stdout/stderr to new private files and report only the saved receipt digest/path and bounded counts. The prior 10.1.7 inventory helper intentionally rejects another version and does not propagate this new mode; do not run it unchanged or silently relax its checks.

Carry the successful source receipt through native `db schema migrate --dry-run` and reviewed fence/recovery/apply commands. The configured mode propagates from source/preflight receipts through current/interim/postflight/finalization. No repeated full row dictionaries are allocated for audit. Before any mutation root must inspect exact full-table counts, transformation count, bounded resource observations, pending assets, catalog and sequence evidence. No database cutover is authorized by this handoff itself.

The critical deployed check is the real native precommit whole-audit comparison and exact PostgreSQL catalog/policy deparse comparison, followed by the native committed recovery receipt. Pure tests do not prove server cursor behavior. Exact PostgreSQL policy deparse text is now corroborated against root-supplied Bloom catalog evidence; Kahlo precommit enforcement remains required. Any mismatch must fail and preserve the existing recovery/fence state; do not weaken the policy comparator or infer committed success. No separate PostgreSQL rehearsal/full suite is prescribed.

## Owning PostgreSQL catalog corroboration

Root supplied `bloom-audit-catalog-reference.json`, SHA256 `09eed6b9b81b5af2cd2fd8875e978cd1a1be81d1ac50b64bdb49b71f5da1647d`, read-only without audit row access. Direct comparison against the candidate `_policies` function matches both complete policy dictionaries exactly, including expressions, commands, permissiveness and roles. Bloom has 38 catalog dependencies; Kahlo source has 33. The five additions are exactly the two canonical policy table dependencies plus the three scope-policy column dependencies.

For additional source corroboration, substitute only Bloom's schema identifier with Kahlo's identifier in the saved catalog, then run the candidate full catalog transition comparator against the saved Kahlo source catalog: it passes. No other field normalization or waiver was used. This is comparison of existing receipts, not a database mutation, inventory scan or PostgreSQL rehearsal. Candidate source was unchanged and no additional tests were run. The actual Kahlo post-migration catalog must still pass the same native comparator before commit.
