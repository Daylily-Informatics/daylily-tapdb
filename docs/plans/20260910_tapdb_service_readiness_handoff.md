# DRAFT: TapDB 10.1.0 service-readiness release handoff

Status date: 2026-09-10

## Status

**Candidate only. Not released. Not independently accepted.** This draft is a
slot-controlled handoff for a future exact TapDB 10.1.0 release. It is not an
installation instruction and must not be used as evidence that 10.1.0 exists on
GitHub, PyPI, or any service.

| Claim | Current state | Required terminal evidence |
|---|---|---|
| Source implementation | Implementation prepared; no independently qualified frozen candidate | Reviewed PR merge commit on `main` |
| Independent PostgreSQL qualification | Incomplete; current independent turn ended in a tooling refusal | Authorized independent rerun against one frozen commit on exact PostgreSQL 16.13 and 17.11 |
| Isolated Aurora PostgreSQL 16.13 acceptance | Author preparation/binding evidence exists; independent acceptance incomplete | Independent exact-target acceptance receipt against the frozen commit |
| Full suite and coverage | Not green/frozen | Zero-failure, zero-unexpected-skip full suites and aggregate plus changed-module branch coverage at or above 90% |
| CI | Not run on a reviewed release commit | Green protected-branch PR checks for quality, security, both PostgreSQL jobs, and build/install smoke |
| Tag | Not created | Immutable annotated bare tag `10.1.0`, peeled to the exact release commit |
| Wheel and sdist | Not release-built | Clean-build names and SHA-256 values from the tagged commit |
| Publication | Not performed | GitHub and PyPI retrieval plus metadata receipts |
| Fresh public installation | Not performed | No-cache installation and CLI/Python/schema-asset smoke from the published wheel |
| Consumer conversion/deployment | Out of TapDB release scope | Service-owned evidence for Dewey, Bloom, and Ursa |

No production service or database was changed by preparation of this handoff.

## Immutable release slots

Fill these only from the owning system after each event succeeds. Do not copy a
candidate checkout hash into a release slot.

| Field | Value |
|---|---|
| Project | `daylily-tapdb` |
| Candidate version | `10.1.0` |
| Baseline published version | `10.0.0` |
| Baseline release commit | `eadef9e8426ee968fb850328c7a6e2f90353fe29` |
| Baseline annotated tag object | `fade52ec6a9d78f782b49823a020d77b77c7b564` |
| Candidate branch | `codex/tapdb-service-readiness-20260910` |
| Preparatory ledger checkpoint | `868687d36d7467f27e8341a4813b0b17757b3aa2` (historical ledger-only checkpoint; not a release candidate) |
| Reviewed PR | **PENDING** |
| Reviewed merge commit | **PENDING** |
| CI run | **PENDING** |
| Annotated tag object | **PENDING** |
| Peeled `10.1.0^{}` commit | **PENDING** |
| GitHub release | **PENDING** |
| Wheel filename | **PENDING** |
| Wheel SHA-256 | **PENDING** |
| Sdist filename | **PENDING** |
| Sdist SHA-256 | **PENDING** |
| PyPI JSON/index observation | **PENDING** |
| Fresh-install environment and command receipt | **PENDING** |
| Installed `daylily-tapdb` version | **PENDING** |
| Installed wheel `RECORD` / schema-asset verification | **PENDING** |

The release is not complete while any required field is `PENDING`.

## Candidate compatibility and dependency contract

- Python: `>=3.12`.
- Release qualification targets: exact PostgreSQL `16.13` and `17.11`.
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
   closure, and receipt-bound runtime binding.
7. Run service-owned acceptance and deployment/cutover gates.

## Evidence available for an independent reviewer

These are development/author receipts, not release acceptance:

| Evidence | Result and boundary |
|---|---|
| `runtime/qualification/a-tests.xml` and `a-coverage.json` | Author gate: 368 tests passed on exact PostgreSQL 16.13, zero skipped; aggregate branch-aware coverage 93.96%; changed modules above 90%. Does not replace independent authorization acceptance. |
| B author matrix | After the allocator-resolution correction, 204 tests passed on each of exact PostgreSQL 16.13 and 17.11 with zero skips; catalog coverage was 100% and principal coverage 99%. This is author evidence, not the frozen-candidate result. |
| `runtime/qualification/c-backup-recovery63.xml` | Author/integration checkpoint: 63 exact-16.13 backup and repeated-replacement recovery tests passed. |
| `runtime/qualification/root-backup-surfaces-integration5.xml` | Root integration checkpoint: 168 backup API/HTTP/GUI/surface cases passed on exact PostgreSQL 16.13. |
| `runtime/qualification/d-auth16-temp2.xml` | Independent P1 finding: a temporary sequence could redirect an unqualified managed allocator helper. This is failure evidence, not acceptance. |
| Isolated Aurora plan/apply receipts | Author binding used an exact qualification-only PostgreSQL 16.13 database. The later CONNECT-aware plan/result hashes were recorded in the controlling ledger. Effective `PUBLIC TEMP` made independent temporary-object shadowing proof mandatory. Production remained unchanged. |
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
receipts above resolve PKG-01 only. The ordinary PostgreSQL regression runs
still failed, and independent/Aurora acceptance remains unavailable. Publication
requires fresh-install proof from the eventual published wheel, not this
unreleased diagnostic artifact.

## Independent frozen-candidate acceptance

The reviewer must first record one clean commit and confirm no file changes
during the entire matrix. Use the repository's checked-in CI workflow and
qualification builder as the command authority.

Required evidence:

1. `git status --short --branch`, exact commit, diff against the 10.0.0
   baseline, and candidate version metadata.
2. Exact PostgreSQL `16.13` and `17.11` binaries built by
   `scripts/build_qualification_postgres.sh` with the checked-in source hashes.
3. The complete `python -m pytest tests -q` matrix with no deselection, no
   failures, no errors, and no unexpected skips.
4. Aggregate branch coverage at least 90% and at least 90% for each changed
   production module via `scripts/verify_changed_coverage.py`.
5. Ruff check/format, configured mypy, Bandit, and detect-secrets results.
6. Independent allocator, family-root, writer-fence, session-closure,
   principal-confinement, historical-source, backup/recovery, migration,
   API/GUI, typed-reference, lineage, and DAG-v2 acceptance.
7. Independent isolated Aurora 16.13 acceptance against only the separately
   authorized qualification target, including the corrected managed allocator
   resolution and runtime temporary-object case.
8. A local candidate build with `SETUPTOOLS_SCM_PRETEND_VERSION=10.1.0`, Twine
   validation, `scripts/verify_wheel_assets.py --expected-version 10.1.0`, and
   a fresh installed-wheel smoke. This is candidate evidence only until the
   reviewed merge and annotated tag exist.

The exact frozen commit must then pass the protected PR checks before the
coordinator may merge, tag, build release artifacts, or publish.

## Publication proof required after merge

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

- All controlling ledger rows terminal: **no**.
- TapDB 10.1.0 objective complete: **no**.
- Production changed: **no**.
- Consumer repositories or pins changed: **no**.
- Publication approval exercised: **no**.
