# Shared graph node error boundaries

User direction: apply the unsupported-reference leaf pattern to other per-node
errors while preserving valid graph branches. This extends the October 15
unsupported-reference work; it does not interpret or repair legacy records.

Native projection isolates malformed node properties, presentation and XRF
data. Invalid relationships, unavailable endpoints and cycles have explicit
diagnostics. Federation quarantines malformed or conflicting projections,
preserves completed graph content on a later deadline, and keeps upstream
failure boundaries. Kahlo and the native viewer label partial results and
provide reasons. Error payloads withhold untrusted properties and use fixed
messages. Root authentication, envelope/service identity, scope enforcement
and database failures remain mandatory errors.

Regression source covers malformed siblings with valid branches, invalid
canonical references, missing endpoints, conflicting nodes, cycles and a
deadline after the root loads. Python AST, JavaScript syntax and whitespace
were reviewed. No regression execution, lint, coverage or CI campaign was run.
No schema, template, data, credential or workflow changes are included.

Release candidate: immutable annotated TapDB 11.0.9. The controlling Kahlo
ledger tracks K31 and separate Kahlo/Dewey deployment evidence. Ursa adoption
belongs to the OWY chat; this work does not deploy/restart Ursa or resume OWY.
