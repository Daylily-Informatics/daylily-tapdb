# Native DAG query and return-reference release

Controlling ledger: Kahlo `docs/plans/20261015T075127Z_seqnas_performance_annotations_ledger.md`, rows K13 and K15. This receipt supports that ledger; it does not replace it.

Baseline: branch `codex/tapdb-backup-journal-11-0-1`, commit e3a2e2f410c5fe25b25e680707dbd0093bfdbd7e, release11.0.4. The affected graph/query files are identical to deployed11.0.2 before this change. The user explicitly requested faster native queries, credential repair, and blocking symmetric foreign-pointer traversal cycles.

Source changes:

- Fetch active adjacency in batches of128 through the caller's scoped session, eager-load endpoints, and reuse the outgoing rows for exact XRF projections. Preserve scope/tenant, lineage provenance, cycle, missing-endpoint and soft-delete checks.
- Count each queued physical node once. Native DAG and embedded object-graph routes use this reader. Standalone in-memory payload construction remains available.
- Select only search-result fields and execute blocking native DB handlers in FastAPI's worker pool.
- Visit each exact service/EUID once per federation request, including already returned owned nodes; retain failed branches without repeated retries.
- Preserve local lineage cycle rejection. Replace cycle-closing foreign-reference bridges with visible nonexpanding boundaries that retain exact target and assertion provenance. Report the number and warnings explicitly.
- Give empty transport exceptions a meaningful error type.

Existing production observations: Ursa search9.369seconds; execution graph at depth4/max500 exceeded35seconds. After-release measurements remain pending. QEO's credential is application-owned and repaired separately in Kahlo configuration.

Python AST parsing and diff whitespace review performed. Tests and lint have not run; the supplied policy requires a second explicit request for execution. No synthetic production objects, schema migration, destructive operation, or fleet-wide deployment is part of this change. Consumers selected for this release are Kahlo and the Ursa API; their other processes remain pinned.
