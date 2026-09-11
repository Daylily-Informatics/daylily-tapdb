# TapDB 10.1.0

Shared TapDB prerequisites for Dewey, Bloom and Ursa, preserving the10.0.0
public application APIs. Install the exact package with
`python -m pip install 'daylily-tapdb==10.1.0'` (Python>=3.12).
Consumers needing Aurora or the canonical GUI must select the corresponding
`aurora` / `gui` extras explicitly.

## Included

- Exhaustive physical identity and allocator inventory, including historical
  schemas and preserved stored prefixes.
- Receipt-bound sequence advancement and verification with durable external
  recovery floors, shared by migration and restore.
- Historical-schema backup/restore and migration preservation verification.
- Offline runtime-principal bootstrap and explicit receipt-bound Aurora
  binding. Binding denies runtime temporary-object creation; operator access
  is preserved as declared by the reviewed plan.
- Corrected core-only CLI packaging without mandatory GUI dependencies.

## Release disposition

PR[#108](https://github.com/Daylily-Informatics/daylily-tapdb/pull/108) was
administratively merged under explicit owner authorization. CI and formal
GitHub review were waived, **not passed**. Independent acceptance is
user-attested; sensitive results are not included in this release.

Retained author evidence: exact PostgreSQL16.13 partial suite2964PASS,
aggregate branch coverage94.73%, changed-module gate passing with the two
approved numeric exceptions. Two independent authorization modules were not
run locally; this is not a full-suite claim. Isolated Aurora migration/recovery
and runtime TEMP binding author checks passed. PostgreSQL17 qualification is
deferred under [issue#107](https://github.com/Daylily-Informatics/daylily-tapdb/issues/107).

## Immutable artifacts

| Field | Value |
|---|---|
| Release commit | `9db1abb4525f2594ebdbf2a307eb8b49aa51883d` |
| Annotated tag | `10.1.0` |
| Tag object | `d8d36584a5338d7256aba0300d299d53a5c12217` |
| Wheel | `daylily_tapdb-10.1.0-py3-none-any.whl` |
| Wheel SHA-256 | `f46cb2abfccb3000b9f9443ea00045e83fb96f6b320d2aeac0ad800ea47e8801` |
| Source archive | `daylily_tapdb-10.1.0.tar.gz` |
| Source SHA-256 | `a582f902ff6df6a66c05ff293cef3c5cbca84e9b6de9977737d25fe0e11cc978` |

## Service adoption

The [operator guide](https://github.com/Daylily-Informatics/daylily-tapdb/blob/main/docs/service-readiness.md)
documents public CLI/Python interfaces and receipts. The
[three-service handoff](https://github.com/Daylily-Informatics/daylily-tapdb/blob/main/docs/plans/20260910_tapdb_service_readiness_handoff.md)
maps Dewey L02/L04/L05; Bloom T10-00/T10-01 and the TapDB portion of MG01;
Ursa DB01 and the TapDB portions of DB04–DB07.

Public verification passed: both PyPI downloads match the hashes above; GitHub
asset digests match; a fresh public-index-only/no-cache Python3.13.13 core
installation passes pip check, version/import/CLI checks and installed
schema/migration-asset verification. Tagged/package documentation retains
pre-publication status text; the current handoff and these release notes supply
the final receipts without changing immutable artifacts.

Ordering: inventory; principal preparation; backup/restore; schema migration;
service-owned conversion; final identity/allocator verification and runtime
binding; recreate runtime sessions; service acceptance. Restore requires
explicit principal rebinding. A database dump does not include per-service
TapDB config, registries, credentials/principal artifacts, runtime files or
the external recovery journal; services must preserve those separately.

Revoking database TEMP from PUBLIC also affects other roles relying on that
implicit grant. Existing runtime sessions must be closed/recreated during
adoption; TapDB does not terminate them automatically.

This release changes no service pins, service data, deployments or production
databases. Those adoption and cutover decisions remain service-owned.
