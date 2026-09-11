# TapDB 10.1.0 human review packet

Reviewer: user / `iamh2o` (volunteered; review not yet performed).
Candidate under review: `c1d23f170371c6330ae438fd8772a634cfe3438b`.
Scope: PostgreSQL/Aurora 16.13. PG17 deferred under
[issue 107](https://github.com/Daylily-Informatics/daylily-tapdb/issues/107).
This packet and the ownership amendment do not change runtime code.

## 1. Immediate decision: review the allocator correction

The independent reviewer found that an unqualified sequence reference could
select a temporary sequence instead of the persistent allocator. The persistent
high-water state could then cease to represent identifiers actually issued.
The recorded local16.13 failure is in
`runtime/qualification/d-auth16-temp2.xml`; production was not tested or changed.

Review these existing implementation files:

- [Canonical allocator functions](../../schema/allocator_functions.sql):
  persistent sequences and template relations are explicitly qualified by the
  trigger table's schema; sequence invocation uses `pg_catalog.nextval`;
  function search paths are pinned to the managed schema, `pg_catalog`, then
  `pg_temp`. Missing persistent generators fail rather than selecting another.
- [Migration](../../schema/migrations/20260910_233000_pin_managed_allocator_resolution.sql):
  installs the shared function asset; declares only affected trigger metadata.
- [Base schema](../../schema/tapdb_schema.sql): retains the matching allocator
  definitions for fresh installations.

Review questions:

1. Does the correction consistently resolve allocation and lookup operations
   to the intended schema, including when another schema has matching names?
2. Does the migration preserve existing identifiers, stored prefixes, sequence
   state, function interfaces and application privileges?
3. Are there any concrete defects or missing evidence that prevent acceptance?

Please return findings or a code-review verdict tied to the candidate commit.
Code-review approval alone is not full release or Aurora acceptance.

## 2. Evidence already available

| Evidence | What it establishes | What it does not establish |
|---|---|---|
| `runtime/qualification/b-allocator-resolution-pg16.xml` | Correction author reports 204 passing exact16.13 checks | Independent retest of the final candidate |
| `runtime/qualification/root-878b9a0-pg16-partial.xml` | 2955 passing ordinary PG16 cases on the earlier frozen candidate | Full suite: two authorization modules were explicitly unrun |
| `runtime/qualification/e-package-20260911T001928Z/` | Earlier candidate builds and installs in fresh core and GUI environments | Publication or fresh installation of a public release |
| `runtime/qualification/c-retained-outcome16-MtoTN6/test.xml` | One retained-session lost-receipt recovery scenario passes | Full-suite/coverage acceptance; its command failed the narrow-scope coverage gate |

Raw runtime evidence is retained locally, not committed. The
[controlling ledger](20260910_tapdb_service_readiness_ledger.md) records source
hashes, prior findings, ownership and the complete implementation history.

## 3. Remaining acceptance is explicit

- Independent correction verification and required consumer/API qualification:
  **pending human review/evidence**, not a passed AI gate.
- Complete final-candidate PG16 suite: **pending**, including the two previously
  unrun authorization modules. No tests are silently removed or marked passed.
- Exact isolated Aurora16.13 acceptance: **pending**; existing author preparation
  is not final acceptance. No production target is in scope.
- Changed-module90 coverage: **not met**. Diagnostic recovery86.72% and
  service89.57%; no threshold exception is approved by volunteering to review.
- PR/CI/reviewed merge, annotated tag, publication and fresh public install:
  **pending**. No `10.1.0` release claim is authorized by this packet.

## 4. Review disposition

Record reviewer identity, candidate commit, reviewed scope, findings, evidence
paths and explicit decisions. Separate code-review approval from test results,
coverage exceptions and release authorization. Any runtime change requires
affected evidence to be refreshed. The original AI screening remains recorded;
this handoff neither clears it nor instructs a workaround through another tool.
