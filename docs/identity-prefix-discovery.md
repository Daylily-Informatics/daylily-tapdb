# Scoped identity-prefix discovery

`tapdb --config /absolute/operator-config.yaml db identity inventory --prefix BG`
reads a single prefix in the explicitly configured database and schema. It uses
the native operator read-only transaction; it does not initialize a schema,
allocate an EUID, change a sequence, or grant namespace ownership.

The `tapdb-prefix-usage/v1` result reports:

- Stored identities, including soft-deleted rows, grouped by native owner.
- Template instance-prefix and native identity-prefix bindings.
- Historical audit references to the requested domain/prefix.
- Matching native sequence state, including its cached reservation position.
- Target and authenticated physical database identity.

Known external allocation journals can be included with repeated
`--allocation-receipts-dir /absolute/native-journal-directory` arguments. The
native journal reader retains matching reservation floors regardless of a
receipt's success/failure status. No object properties or unrelated table rows
are exported.

An optional `--receipt /absolute/new-result.json` stores the result using the
existing receipt writer. This scoped result is deliberately not a complete
identity-preservation inventory and cannot be used as one. Source-version,
sequence-mapping, and recovery-family capture flags are mutually exclusive with
`--prefix`.

Availability across a Meridian domain requires the explicit relevant database
targets and known allocation journals, together with its owning registry. A
database with no matching rows does not establish global availability, and an
unregistered prefix does not erase historical allocations. Existing assignments,
generators, bindings, or reservation floors require disposition before a new
independent database begins issuing that domain/prefix.
