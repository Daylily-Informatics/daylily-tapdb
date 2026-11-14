# Kahlo audit catalog closure handoff

Prepared for root execution only. Local verification: Python AST parse, no imports, tests or live calls. Native implementation waits for this owning evidence.

Helper: `20260927T014500Z_kahlo_audit_catalog_closure.py`
SHA256: `e249ae9efa330b3d9da26443a239c58184cefb9f72421242ea0a0201d5a34065`

Stage under `/home/ubuntu/atlas-globaldag-20260926-operator/`. The existing inventory helper must also be staged under that root at its exact reviewed SHA256 `f00b40c13e6da6169747c6bca90b78b0b199b889a0453759e5e4351c338623cc`; this helper imports only its host/config/package guards, never its main or inventory routines.

Root command from the designated EC2 host as ubuntu, using a new receipt name:

```bash
/home/ubuntu/atlas-globaldag-20260926-operator/venv/bin/python \
  /home/ubuntu/atlas-globaldag-20260926-operator/20260927T014500Z_kahlo_audit_catalog_closure.py \
  --inventory-helper /home/ubuntu/atlas-globaldag-20260926-operator/20260927T000100Z_native_inventory_preflight.py \
  --receipt-name kahlo-audit-catalog-closure-20260927T014500Z
```

The filenames shown are explicit staging destinations, not discovery or fallback paths. If the prior helper is already staged elsewhere inside the same root, pass its actual verified path. Do not overwrite a prior receipt.

Capture is native operator `REPEATABLE READ`, `read_only=True`, with transaction read-only readback, 30-second per-statement timeout and 3-second lock timeout. It calls existing native migration asset expansion/tracking/catalog routines and adds a catalog-only `pg_policy.polroles` query. No audit SELECT, row count, source inventory, sequence inventory, preflight, migration, fence, role or database mutation is exposed. The only database value rows read are migration tracking filename/applied_at; other reads are PostgreSQL catalogs and physical identity. Expanded SQL is packaged code, never executed. Temporary transaction settings do not alter persistent database state.

Private receipt includes complete expanded packaged migration assets/hashes, exact applied/pending names, unmatched historical filenames (reported, not silently waived), audit column/constraint/index/trigger/policy/dependency metadata including policy roles, target identity and config hashes. It contains no audit row contents or credentials. It is diagnostic closure evidence, not a native migration preflight or authorization receipt. Stdout is bounded to counts, digest and receipt path. On failure preserve `failure.json`; do not infer no pending migrations.
