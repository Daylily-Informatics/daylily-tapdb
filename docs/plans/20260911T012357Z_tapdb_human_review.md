# TapDB 10.1.0 human review packet

Historical pre-release review packet. The user subsequently accepted the
confidential independent acceptance and explicitly authorized administrative
CI/review bypass. TapDB10.1.0 is now published with verified public artifacts
and a successful fresh installation; see the
[final handoff](20260910_tapdb_service_readiness_handoff.md). Pending-release
statements below describe the earlier review checkpoint, not current blockers.
No confidential results are attached, and no full CI pass is claimed.

Reviewer disposition: the user states that the independent acceptance is
sufficient to proceed and its results are sensitive. Accepted by user
attestation; no sensitive results are requested or reproduced in this packet.
This is separate from formal GitHub review approval and observed CI results.
Current runtime/test candidate: **`24075ba7628d38502538b1897e09ae3cb58e21a7`**.
The user's "proceed" disposition was for earlier TEMP candidate17d0ca0.
Subsequent bounded changes fix Aurora provider lookup/preload compatibility,
add actionable census refusal details, and isolate disposable test servers from
automatic vacuum. Production code is identical to package-verified
`aeb5ac079959d4d74160de3b537e49a3013135b3`;24075ba adds only test isolation and
ledger evidence. Independent acceptance is now user-attested, not implied from
those author results.
Scope: PostgreSQL/Aurora 16.13. PG17 deferred under
[issue 107](https://github.com/Daylily-Informatics/daylily-tapdb/issues/107).
The earlier "proceed" disposition is superseded by the explicit confidential
independent-acceptance disposition above. It is not a GitHub approval or a
claim that the outstanding CI suite was run locally.

## 1. Immediate decision: review allocator and `TEMP` confinement

The independent reviewer found that an unqualified sequence reference could
select a temporary sequence instead of the persistent allocator. The persistent
high-water state could then cease to represent identifiers actually issued.
The recorded local16.13 failure is in
`runtime/qualification/d-auth16-temp2.xml`; production was not tested or changed.
The schema-qualified allocator correction remains required. The later
user-requested confinement amendment additionally makes the receipt-bound bind
revoke database `TEMP` from `PUBLIC` and the configured runtime principal,
preserving pre-existing effective operator `TEMP` through an explicit reviewed
grant only when the plan shows it would otherwise be lost. Bootstrap remains
CONNECT-only.

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
- [Runtime-principal binding](../../daylily_tapdb/runtime_principal.py): review
  exact database ACL/effective-`TEMP` inventory, the planned and applied
  `PUBLIC`/runtime revocations, any explicit operator preservation grant, and
  post-apply proof that runtime effective `TEMP` is false. In the sealed
  receipts, inspect `database_access`, `database_revokes`,
  `operator_database_grants`, `runtime_session_requirement`, and the result's
  `runtime_temp_denied`; apply must use `RESTRICT` rather than cascading
  dependent grants outside the reviewed scope.
- [Runtime-principal CLI](../../daylily_tapdb/cli/runtime_principal.py): review
  that bind exposes the database-wide impact and mandatory runtime-session
  restart without changing CONNECT-only bootstrap or terminating sessions.

Review questions:

1. Does the correction consistently resolve allocation and lookup operations
   to the intended schema, including when another schema has matching names?
2. Does the migration preserve existing identifiers, stored prefixes, sequence
   state, function interfaces and application privileges?
3. Are there any concrete defects or missing evidence that prevent acceptance?
4. Does the reviewed plan make clear that revoking `PUBLIC TEMP` affects every
   role relying on that implicit grant, and refuse a silent compatibility
   allowance for applications that require temporary tables or sequences?
5. Does apply verify runtime effective `TEMP=false` without claiming that it
   terminated old sessions or removed temporary objects that already existed?

The user has supplied the acceptance disposition. The historical review
questions above are retained for context, not a request to disclose sensitive
findings. Ordinary CI and publication evidence remain separate.

## 2. Evidence already available

| Evidence | What it establishes | What it does not establish |
|---|---|---|
| `runtime/qualification/b-allocator-resolution-pg16.xml` | Correction author reports 204 passing exact16.13 checks | Independent retest of the final candidate |
| `runtime/qualification/root-878b9a0-pg16-partial.xml` | 2955 passing ordinary PG16 cases on the earlier frozen candidate | Full suite: two authorization modules were explicitly unrun |
| `runtime/qualification/e-package-20260911T001928Z/` | Earlier candidate builds and installs in fresh core and GUI environments | Publication or fresh installation of a public release |
| `runtime/qualification/c-retained-outcome16-MtoTN6/test.xml` | One retained-session lost-receipt recovery scenario passes and contributes to measured recovery coverage of 86.72% | Full-suite, functional, or independent acceptance |
| `runtime/qualification/root-temp-cli-contract.xml` | 53 root CLI, core, and release-contract checks pass for the amendment | Principal author evidence, human verdict, or frozen-candidate/Aurora acceptance |
| `runtime/qualification/b-temp-denial-final-pg16.xml` | 134 author cases pass on exact16.13, zero skips; TEMP denial, operator preservation, ordinary allocation and exact role names | Human or independent acceptance of the final frozen candidate |
| `runtime/qualification/root-24075ba-pg16-partial.xml` | 2964PASS, zero errors/skips, aggregate94.73%; all changed-module gates pass with the two approved exceptions | Complete suite: the two independent authorization modules remain unrun |
| `runtime/qualification/aurora-68595f8-migration-result.json` and `aurora-68595f8-temp-plan.result.json` | Actual isolated Aurora migration/recovery committed, fence released, TEMP bind applied with runtime TEMPfalse and operator TEMPtrue; separate fresh sessions reject TEMP table/sequence creation | Independent consumer/recovery acceptance or service deployment |
| `runtime/qualification/e-package-aeb5ac0-rjx4UT/` | Current unchanged production payload builds and fresh core wheel installation passes | Public publication; final sdist/test-configuration hash |

Raw runtime evidence is retained locally, not committed. The
[controlling ledger](20260910_tapdb_service_readiness_ledger.md) records source
hashes, prior findings, ownership and the complete implementation history.

## 3. Remaining acceptance is explicit

- Independent correction verification and required consumer/API qualification:
  **accepted by user attestation**; sensitive results are not attached and no
  completed AI review is claimed.
- Complete final-candidate PG16 suite: **pending**, including the two previously
  unrun authorization modules. No tests are silently removed or marked passed.
- Exact isolated Aurora16.13 independent acceptance: **accepted by user
  attestation**, separately from the recorded corrected migration and
  TEMP-binding author checks. No production target is in scope.
- Runtime `TEMP` confinement: **implemented, author-tested; independent
  acceptance user-attested**. Service
  adoption must close/recreate existing runtime sessions; no automatic
  termination or existing-object removal claim is authorized.
- Coverage exceptions: **approved by the user** for exactly
  `daylily_tapdb/backup/recovery.py` at measured 86.72% and
  `daylily_tapdb/backup/service.py` at 89.57%. Numeric reports remain required;
  aggregate coverage and every other changed production module retain the 90%
  floor. These exceptions do not waive functional or acceptance gates.
- PR/CI/reviewed merge, annotated tag, publication and fresh public install:
  **pending**. No `10.1.0` release claim is authorized by this packet.

## 4. Review disposition

Record the user's acceptance decision without requesting or publishing the
sensitive findings. Separate user attestation from observed test results and
formal GitHub review; the two coverage exceptions above are already approved
and do not require a reviewer waiver. Any runtime change requires
affected evidence to be refreshed. The original AI screening remains recorded;
this handoff neither clears it nor instructs a workaround through another tool.
