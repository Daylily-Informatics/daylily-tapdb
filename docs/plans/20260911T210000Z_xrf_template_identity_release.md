# TapDB 10.1.3: preserve loaded template definitions

User explicitly authorized a fast, targeted fix for GitHub issue #111, an
annotated Git tag and PyPI publication only. No PR, branch push, main merge,
broad/exhaustive tests, infrastructure changes, or consumer deployment.

Gate 0: clean feature branch codex/tapdb-tenant-allowlist-20260911 at
db3d4af827fbda8674ea505034c9e6d4f26a0204 (published 10.1.2). Remote tags showed
10.1.2 as the latest release. Issue #111 describes a loaded historical XRF
definition colliding with changed semantics at the same template identity.
The source fixture is copied exactly from tag 9.0.10.

| ID | Work | Status | Evidence |
|---|---|---|---|
| T01 | Declare distinct XRF template identity; preserve originals | SUCCESS | New coordinate only in core JSON; no historical row rewrite or relabel |
| T02 | Native selection and authorization | SUCCESS | Writer/read/replay/search/audit/GUI derive exact identity from declared pack; lineage scope contains no version-value rule |
| T03 | Loaded-definition immutability | SUCCESS | Loader refuses semantic overwrite and preserves retired/deleted lifecycle state; docs separate schema/allocator migration from explicit application object conversion |
| T04 | Targeted validation | SUCCESS | 14 distinct selected checks passed after fixture corrections; no broad suite or coverage run |
| T05 | Annotated tag, package and PyPI | IN_PROGRESS | Next: clean commit, tag 10.1.3, build and twup, exact pip download |

The user's correction controls the implementation: version is only a numeric
coordinate in a template identifier. No major/minor meaning, version ordering,
automatic latest selection, compatibility inference, or authorization meaning
is assigned. The role-specific core JSON files explicitly select definitions;
Python derives their exact codes from that data. A separate test uses numeric
7.25 to prove the coordinate is not hardcoded. The new pack's distinct value is
2.0; opaque reference data retains its unchanged coordinate.

The lineage rule still checks active visible endpoints, exact domain/owner,
source tenant, explicit approved-global-link metadata, reference type and opaque
public-global scope. Removing a numeric version condition does not bypass any
scope or ownership check. One native schema migration installs that rule; it
does not mutate templates, instances, lineage, audit or allocators.

Validation: nine existing selected lifecycle/reverse-search/opaque/parser,
graph, PostgreSQL endpoint-scope, core-pack and canonical security-catalog
checks passed. Five final selected checks passed in 1.54s: live/retired
historical definitions and bound-instance/audit preservation; public additive
XRF creation/read/replay/search/graph and allocator floor; overwrite rejection;
uninterpreted numeric identity; GUI readiness. Initial collection failed on a
test import. Subsequent fixture failures were timezone formatting, attempting
to bind an instance after retiring its template, a missing default validator,
old GUI template data, and a graph response-key/import typo. Only those affected
checks were rerun. Logs: /tmp/tapdb111-targeted-01.log,
/tmp/tapdb111-targeted-02.log, /tmp/tapdb111-preservation-detail.log,
/tmp/tapdb111-targeted-failures-03.log, /tmp/tapdb111-targeted-failures-04.log.

Ursa's already running conversion uses immutable image/TapDB 10.1.2 and is not
blocked by #111: it has no existing Ursa-owned template at the new-to-Ursa XRF
coordinates. It preserves its historical foreign-owner templates. This release
does not change that running container, Ursa's pin, or its final image.
