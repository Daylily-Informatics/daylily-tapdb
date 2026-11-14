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

Release correction:11.0.5 was published but never built into a consumer. Source review found two reader arguments attached to the GUI wrapper instead of its direct object builders;11.0.6 corrects these call sites before consumer adoption. No production change occurred from11.0.5.

## Performance follow-up: 11.0.7

After11.0.6, Ursa graph improved from >35s to8.2-8.4s; search remained9.0-9.45s. User now targets>=50% improvement/Ursa<=4s, prioritizing Dewey and Kahlo. QEO performance is excluded.

Native read-only profiling found63,831 JSON decodes for~215 retained graph nodes and4.5s scans of each~78k-row search table. The EC2 host has spare CPU/memory. Narrow adjacency reads retain endpoint identity/scope, then load full payloads only for retained nodes/edges and outbound canonical XRF assertions/templates. Every read remains in the requesting RLS session; no persistent graph/authorization cache.

The two instance/lineage read policies now resolve immutable domain/owner scope with uncorrelated scalar subqueries. Tenant checks, all WITH CHECK expressions, role resolution, and policy roles remain unchanged. The exact packaged security-catalog contract changes with the SQL asset. Native `tapdb db schema apply` is required for each selected existing service schema; no raw database edits, new privileges, synthetic records or blanket fleet rollout. Packaged schema assets are otherwise identical between11.0.2 and11.0.6. Consumers: Dewey, Ursa API and Kahlo; other workers/images stay pinned.

AST syntax and diff whitespace review completed. Tests remain unexecuted under the supplied twice-requested policy. Runtime before/after observation and user acceptance remain separate gates in the controlling Kahlo ledger.
