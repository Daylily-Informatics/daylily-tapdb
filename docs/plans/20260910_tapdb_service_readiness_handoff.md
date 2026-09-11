# TapDB 10.1.0 service-readiness release handoff

Status date: 2026-09-11

## Release status

TapDB **10.1.0 is published and verified** on GitHub and PyPI. Fresh public
installation passes. Independent acceptance is user-attested, with
sensitive results intentionally kept outside Git. The owner explicitly
authorized the administrative merge: CI and formal GitHub review were waived,
**not passed**. No tests or CI workflow were removed or rewritten.

Install the exact release using `python -m pip install 'daylily-tapdb==10.1.0'`.
Choose `daylily-tapdb[aurora]==10.1.0` or
`daylily-tapdb[aurora,gui]==10.1.0` when those extras are needed.

| Claim | Evidence |
|---|---|
| Release PR | [#108](https://github.com/Daylily-Informatics/daylily-tapdb/pull/108), administratively merged by `iamh2o` |
| Release commit / peeled tag | `9db1abb4525f2594ebdbf2a307eb8b49aa51883d` |
| Annotated tag | `10.1.0`; tag object `d8d36584a5338d7256aba0300d299d53a5c12217` |
| GitHub release | [10.1.0](https://github.com/Daylily-Informatics/daylily-tapdb/releases/tag/10.1.0), public, not prerelease |
| PyPI publication | [daylily-tapdb10.1.0](https://pypi.org/project/daylily-tapdb/10.1.0/), existing publisher returned success |
| Wheel | `daylily_tapdb-10.1.0-py3-none-any.whl`;659225bytes |
| Wheel SHA-256 | `f46cb2abfccb3000b9f9443ea00045e83fb96f6b320d2aeac0ad800ea47e8801` |
| Source archive | `daylily_tapdb-10.1.0.tar.gz`;1321143bytes |
| Source SHA-256 | `a582f902ff6df6a66c05ff293cef3c5cbca84e9b6de9977737d25fe0e11cc978` |
| Build/metadata/assets | Clean exact-tag build; Twine and packaged schema/migration/interface asset verification pass; GitHub asset digests match |
| Fresh public install | E verifies fresh Python3.13.13 public-index-only/no-cache core installation, pip check, installed metadata10.1.0, public imports/CLI help and RECORD-backed schema/migration assets; FastAPI absent from core |
| Independent acceptance | Accepted by user attestation; confidential results not attached |
| CI / formal GitHub review | Owner-authorized administrative bypass; no passing CI or approving GitHub review claimed |
| Local regression | Exact16.13 partial suite2964PASS, aggregate94.73%; all33 changed-module gates pass with approved recovery86.72% / backup-service89.57% numeric exceptions |
| Local test limitation | Two independent authorization modules not run locally; no full-suite claim |
| Aurora author evidence | Isolated migration/recovery committed; writer fence released; runtime binding and fresh-session TEMP-denial checks pass |
| PG17 | Deferred under [issue#107](https://github.com/Daylily-Informatics/daylily-tapdb/issues/107); not qualified |
| Production/service adoption | No service pins, conversions, deployments, production databases or AWS resources changed by this release |

The release's runtime/test payload matches candidate
`24075ba7628d38502538b1897e09ae3cb58e21a7`; production payload matches
`aeb5ac079959d4d74160de3b537e49a3013135b3`. Subsequent commits record release
authority and evidence, not new runtime behavior.

Public verification evidence is retained locally under
`runtime/qualification/e-public-10.1.0-H50z50/`: tag verification, PyPI JSON,
direct public wheel/sdist downloads and hashes, GitHub release digests,
fresh-public-install and fresh-public-smoke logs. The public-index-only install
used pip `--isolated --no-cache-dir --index-url https://pypi.org/simple` outside
the checkout, with ambient alternate indexes disabled. Both PyPI artifacts
are unyanked and their downloaded hashes equal the immutable artifact table.
No database/security test campaign or GUI rerun was added for publication.

Tagged/package documentation retains its pre-publication status text; this
current handoff and the GitHub release notes supply the final publication
receipts without modifying the immutable tag or artifacts.

## Service prerequisite completion

| Service rows | TapDB delivery | Still service-owned |
|---|---|---|
| Dewey L02/L04/L05 | Identity/allocator inventory, principal preparation, full backup/restore, migration preservation and durable floors in10.1.0 | Exact pin adoption, conversion, service rehearsal, runtime/config recovery artifacts, deployment/cutover |
| Bloom T10-00/T10-01; TapDB portion of MG01 | Shared allocator/principal/recovery interfaces and historical-shape preservation in10.1.0 | Assertion/template conversion, exact pin, service migration and acceptance |
| Ursa DB01; TapDB portions of DB04–DB07 | Principal, backup/migration and allocator/recovery prerequisites in10.1.0 | Source-plan pin amendment, references/policy conversion, cleanup policy and deployment |

Public interfaces, receipt fields, dependencies, historical source crosswalk
and exact adoption ordering follow. This producer release does not certify
any service's production recovery set or cutover.

## Compatibility and dependency contract

- Python: `>=3.12`.
- Release qualification targets: exact community PostgreSQL `16.13` and
  isolated Aurora PostgreSQL `16.13`.
- PostgreSQL `17.11` author and partial-regression evidence is retained below,
  but PostgreSQL 17 qualification is deferred and not part of the 10.1.0
  release claim. Deferred and unqualified does not mean unsupported.
- Required core dependencies at the current source checkpoint:
  `sqlalchemy>=2.0`, `psycopg2-binary>=2.9`, `pydantic`,
  `jsonschema>=4.0`, `typer`, `rich`, `pyyaml`, `uuid6>=2024.1.12`,
  `cli-core-yo==2.1.1`, and `meridian-euid==0.4.8`.
- Optional extras remain `aurora`, `api`, `gui`, `cli`, and `dev`; the tagged
  wheel metadata is the final dependency authority and must be recorded above.
- Existing TapDB 10.0.0 public application APIs for objects, typed external
  references, lineage, DAG v2, federation, and GUI embedding are intended to
  remain available. Independent frozen-candidate qualification must verify that
  claim before release.
- The pre-existing 10.0 local-development Unix-socket path-length selection is
  preserved and excluded from this readiness change. It is not a recovery
  target-discovery mechanism or release-acceptance substitute; new recovery and
  fencing require explicit TCP target and control identities.

## New public service-readiness interfaces

The operator guide is `docs/service-readiness.md`. The expected public Python
interfaces are:

- `daylily_tapdb.identity_inventory.capture_identity_inventory(...)`
- `daylily_tapdb.identity_inventory.verify_identity_inventory(...)`
- `daylily_tapdb.sequences.capture_sequence_inventory(...)`
- `daylily_tapdb.sequences.build_sequence_advance_plan(...)`
- `daylily_tapdb.sequences.apply_sequence_advance_plan(...)`
- `daylily_tapdb.sequences.verify_sequence_floors(...)`
- `daylily_tapdb.runtime_principal.bootstrap_runtime_principal(...)`
- `daylily_tapdb.runtime_principal.bind_runtime_principal(...)`
- `daylily_tapdb.backup.capture_source_contract(...)`
- the existing backup plan/create/verify/restore/rehearsal service functions.

The expected public CLI interfaces are:

- `tapdb db identity inventory|verify`
- `tapdb db sequences advance|verify|reconcile`
- `tapdb db runtime-principal bootstrap|bind`
- the existing `tapdb backup` lifecycle with explicit source contract,
  recovery family, target, writer-fence, recovery-source, control-config,
  provider-contract, and quarantine evidence where required.

Expected version markers are:

- `tapdb-source-contract/v1`
- `tapdb-identity-inventory/v1`
- `tapdb-identity-verification/v1`
- `tapdb-sequence-inventory/v1`
- `tapdb-sequence-advance/v1`
- `tapdb-sequence-apply/v1`
- `tapdb-sequence-verification/v1`
- `tapdb-writer-fence/v1`
- `tapdb-writer-fence-takeover/v1`
- `tapdb-writer-quarantine-verification/v1`
- `tapdb-recovery-floor/v1`
- `tapdb-recovery-family/v1`
- `tapdb-runtime-principal/v1`
- `tapdb-migration-receipt/v1`
- backup manifest schema version `2`.

All exact interfaces and markers must be re-read from the tagged installed
wheel. This draft cannot freeze them before independent qualification and the
reviewed release commit.

The runtime-principal bind plan/result must expose the exact
`database_access` inventory (`acl`, `runtime_connect`, `runtime_temp`,
`operator_temp`, and `operator_temp_after_revokes`), two explicit
`database_revokes` entries for `PUBLIC` and the configured runtime role with
`privileges: ["TEMPORARY"]`, and any required explicit operator preservation in
`operator_database_grants`. It must also carry
`runtime_session_requirement.action` as
`close_and_recreate_pre_binding_runtime_sessions`, with `verified: false` and
`performed_by_bind: false`. The applied result must bind the unchanged plan
through `plan_sha256`, report `runtime_temp_denied: true`, and preserve the same
session requirement. These are sealed public-interface receipts, not fields for
an operator to invent. `runtime_temp_denied` certifies the effective privilege
check only; it does not certify session closure or removal of existing temporary
objects. Apply uses `RESTRICT`; dependent grants must fail rather than cascade
outside the reviewed scope.

## Source crosswalk

| Consumer plan | Exact source evidence | TapDB prerequisite | Service-owned remainder |
|---|---|---|---|
| Dewey L02/L04/L05 | Historical TapDB `9.0.9`; service must capture actual source/package/config/physical inventory | Identity and allocator inventory, principal lifecycle, backup/restore, migration preservation, recovery floors | Pin, replacement database, service conversion, rehearsal, deployment, cutover, cleanup, acceptance |
| Bloom T10-00/T10-01 and TapDB part of MG01 | Source commit `948bc487415899eb50f024f2d421066a061f0a48`; project pin `9.0.10`; lock resolves TapDB commit `12351b832276d35228527c3c9596e5d60e7c7824`; `9.0.10` schema assets equal `9.0.9` | Allocator/recovery/principal/backup/migration and published 10.1.0 proof | Pin adoption, assertion/template conversion, in-place service migration, deployment, cleanup, acceptance |
| Ursa DB01 and TapDB parts of DB04–DB07 | Historical TapDB `9.0.9`; service must capture actual source/package/config/physical inventory | Principal, backup/migration, allocator/recovery | Source-plan pin amendment, typed-reference/policy conversion, deployment, cutover, cleanup, acceptance |

The required service ordering is:

1. Inventory the exact source and external recovery artifacts.
2. Prepare the constrained target runtime principal.
3. Capture, verify, and restore the backup under explicit recovery authority.
4. Apply the receipt-bound schema migration.
5. Perform the service-owned data/config/dependency conversion.
6. Verify identity preservation, complete retained allocator floors, session
   closure, receipt-bound runtime binding, and effective runtime `TEMP=false`
   from newly created service connections.
7. Run service-owned acceptance and deployment/cutover gates.

Before step 6 apply, the service owner must stop new runtime work and close all
existing connections or pool sessions for the configured runtime principal.
TapDB does not terminate sessions automatically or claim existing temporary
objects were removed. The bind plan must disclose the exact database ACL,
effective `TEMP`, revocation from `PUBLIC` and the runtime principal, and any
explicit operator grant. Revoking `PUBLIC TEMP` affects other roles on the same
database that relied on that implicit grant. Applications that require
temporary tables or sequences need an explicit design change; no compatibility
allowance is inferred.

## Evidence available for an independent reviewer

These are development/author receipts, not release acceptance:

| Evidence | Result and boundary |
|---|---|
| `runtime/qualification/a-tests.xml` and `a-coverage.json` | Author gate: 368 tests passed on exact PostgreSQL 16.13, zero skipped; aggregate branch-aware coverage 93.96%; changed modules above 90%. Does not replace independent authorization acceptance. |
| B author matrix | After the allocator-resolution correction, 204 tests passed on each of exact PostgreSQL 16.13 and 17.11 with zero skips; catalog coverage was 100% and principal coverage 99%. This is author evidence, not the frozen-candidate result. |
| `runtime/qualification/c-backup-recovery63.xml` | Author/integration checkpoint: 63 exact-16.13 backup and repeated-replacement recovery tests passed. |
| `runtime/qualification/root-backup-surfaces-integration5.xml` | Root integration checkpoint: 168 backup API/HTTP/GUI/surface cases passed on exact PostgreSQL 16.13. |
| `runtime/qualification/d-auth16-temp2.xml` | Independent P1 finding: a temporary sequence could redirect an unqualified managed allocator helper. This is failure evidence, not acceptance. The allocator correction remains required despite the later `TEMP`-denial amendment. |
| Isolated Aurora plan/apply receipts | Historical author binding used an exact qualification-only PostgreSQL 16.13 database. The later CONNECT-aware plan/result hashes were recorded in the controlling ledger. Those receipts observed effective `PUBLIC TEMP` and predate the requested denial, so they do not establish the amended confinement contract. Production remained unchanged. |
| `runtime/qualification/root-temp-cli-contract.xml` | Root integration checkpoint: 53 CLI, core, and release-contract checks passed for the `TEMP` amendment. This is not principal author evidence, a human verdict, or frozen-candidate/Aurora acceptance. |
| `runtime/qualification/e-package-20260910T234815Z/` | Pre-final-documentation SCM-pretend `10.1.0` source-snapshot diagnostic: build, Twine, wheel-asset verification, and sdist inspection passed. Wheel SHA-256 `25f03f754754968ecd257146c981e58e9cf029c1a78d8d1434cf98d484bdcb55`; sdist SHA-256 `3f79552d613f712f9b21dc1b5361e98e15561b7b38022e37a6a8e9c75b17d9d9`. Embedded allocator and migration hashes matched the source. The final filename/crosswalk documentation corrections followed this build. These are not release artifacts and do not fill the immutable slots above. |
| `runtime/qualification/e-package-20260911T000027Z/` | Exact clean candidate commit `c3eee0e8e2aa26ad69ab1c24ad5b58c0dcf8c9f3`, tree `8fa980b87934eb578c44692bfe9ed5dc61087674`, archived before build. Core-only and separate GUI-extra fresh public-index installation, pip check, required imports/assets and outside-checkout CLI help pass. FastAPI is absent from core. Wheel SHA-256 `54095f722d089bd39ff73d9560270c926ad83588f656dac4a9a7fe81b547a4da`; sdist `b9a350546b576fa9ac87308d6e395d2dc2911613041ebcf4ee33a51cacbdd464`. Candidate-only proof, not publication; later ledger/test changes are not covered by these artifact hashes. |

The subsequent independent real-PostgreSQL turn ended in a tooling
cybersecurity-risk refusal. Do not bypass or reproduce the blocked work, count
the refusal as a passed gate, or replace independent evidence with author
evidence. An authorized independent reviewer must qualify the final frozen
candidate through an allowed environment.

Public PyPI metadata currently reports `meridian-euid==0.4.8` as present and
not yanked, with wheel SHA-256
`c36e96b36c78da427b200bda7f76c23bd7db74dbe32b54a473134e2f1f61b946`,
matching the repository lock. The final local diagnostic installed the
candidate wheel and all core dependencies into a fresh Python 3.13 environment
using `pip --isolated install --no-cache-dir --index-url
https://pypi.org/simple <candidate-wheel>`, with ambient index variables unset
and pip configuration disabled. Installed 10.1.0 metadata, public Python
imports, and installed documentation/schema/migration assets passed.

The earlier core-install CLI smoke failed because importing `web.runtime`
eagerly imported optional FastAPI-backed web exports. Candidate `c3eee0e`
corrects that import boundary without adding dependencies or replacing the
existing public web implementations. E independently verified a bare-wheel
fresh install with FastAPI absent and all required CLI help commands passing;
the separate GUI-extra install also passed. The exact candidate package
receipts above resolve PKG-01 only. The partial PostgreSQL 16.13 run passed its
executed tests but is not the complete or independent release-scope
qualification; the failing PostgreSQL 17.11 run is retained as deferred
evidence. Independent PostgreSQL 16.13 and Aurora 16.13 acceptance remain
unavailable. Publication requires fresh-install proof from the eventual
published wheel, not this unreleased diagnostic artifact.

## Historical acceptance checklist (superseded by recorded release exception)

The following pre-exception checklist is retained as history. Independent
acceptance is now user-attested; the complete-CI/formal-review release gate was
explicitly waived by the owner. These historical requirements must not be
misread as completed test results or current outstanding release approvals.

The reviewer must first record one clean commit and confirm no file changes
during the entire matrix. Use the repository's checked-in CI workflow and
qualification builder as the command authority.

Required evidence:

1. `git status --short --branch`, exact commit, diff against the 10.0.0
   baseline, and candidate version metadata.
2. Exact PostgreSQL `16.13` binaries built by
   `scripts/build_qualification_postgres.sh` with the checked-in source hash.
3. The complete `python -m pytest tests -q` matrix with no deselection, no
   failures, no errors, and no unexpected skips.
4. Aggregate branch coverage at least 90% and at least 90% for each changed
   production module via `scripts/verify_changed_coverage.py`, except the two
   approved 10.1.0 numeric reports: `daylily_tapdb/backup/recovery.py` 86.72%
   and `daylily_tapdb/backup/service.py` 89.57%. Missing reports or exceptions
   for any other module are not approved.
5. Ruff check/format, configured mypy, Bandit, and detect-secrets results.
6. Independent allocator, family-root, writer-fence, session-closure,
   principal-confinement (including recreated-session effective `TEMP=false`),
   historical-source, backup/recovery, migration, API/GUI, typed-reference,
   lineage, and DAG-v2 acceptance.
7. Independent isolated Aurora 16.13 acceptance against only the separately
   authorized qualification target, including the corrected managed allocator
   resolution, reviewed database-wide `PUBLIC TEMP` impact, any explicit
   operator `TEMP` preservation, and runtime temporary-object denial.
8. A local candidate build with `SETUPTOOLS_SCM_PRETEND_VERSION=10.1.0`, Twine
   validation, `scripts/verify_wheel_assets.py --expected-version 10.1.0`, and
   a fresh installed-wheel smoke. This is candidate evidence only until the
   reviewed merge and annotated tag exist.

The exact frozen commit must then pass the protected PR checks before the
coordinator may merge, tag, build release artifacts, or publish.

PostgreSQL 17.11 qualification is explicitly deferred from this release. Its
recorded failures remain open evidence for later qualification; neither this
release nor this handoff claims PostgreSQL 17 acceptance or lack of runtime
support.

## Historical publication checklist

The release record above supersedes this original checklist. In particular,
the owner authorized an administrative merge instead of item1's normal gate.

The release coordinator, not this draft's author, owns these ordered actions:

1. Merge a reviewed, green PR without admin override.
2. Fast-forward local `main` to the exact remote merge commit and confirm a
   clean worktree.
3. Create and push an annotated bare tag `10.1.0` on that exact commit; verify
   tag object type and peeled commit. Never move a published tag.
4. Build wheel and sdist from the exact tag in a clean output directory.
5. Record filenames, sizes, SHA-256 values, and Twine metadata validation.
6. Publish through the established repository workflow.
7. Verify GitHub and PyPI independently; index visibility alone is insufficient.
8. Install `daylily-tapdb==10.1.0` without cache into a fresh environment.
9. Verify installed metadata, public imports/CLI help, schema assets, migrations,
   docs, and exact artifact hash provenance.
10. Replace every `PENDING` slot in this handoff with owning-system evidence.

Only then may the handoff say “TapDB 10.1.0 is published and available for
service qualification.” It still may not say that Dewey, Bloom, or Ursa has
converted, deployed, or passed service acceptance.

## Completion statement

- All controlling ledger rows terminal: **yes**, under the recorded owner
  acceptance/CI-review release amendments.
- TapDB10.1.0 release objective complete: **yes**, with the explicit CI/review
  waiver and user-attested independent acceptance; no full CI pass claimed.
- Production changed: **no**.
- Consumer repositories or pins changed: **no**.
- Publication approval exercised: **yes**, including explicit administrative
  CI/review exception. Final public-install verification passes.
