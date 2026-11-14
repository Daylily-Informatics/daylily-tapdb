# Unsupported external references in native and federated DAGs

Controlling execution ledger: Kahlo `docs/plans/20261015T075127Z_seqnas_performance_annotations_ledger.md`.

User direction, 2026-10-15: older external-reference formats are unsupported. Encountering one must preserve a visible, non-traversed boundary and allow valid graph branches to load. Apply this to every object and producer; no EUID-specific correction or existing-data conversion.

Gate 0: TapDB11.0.7/b04257f551442bf2aa9074e9103720a89574f073. Production Ursa14.1.92 rejects historical typed XRF1.0 nodes in the reported graph. The current packaged XRF contract is2.0. Retain canonical relationship, RLS, scope, cycle, digest, and capacity validation.

The shared native reader recognizes unsupported versions of the two reserved external-reference template families. It emits only the authorized local record identity, classifier and timestamps, with empty properties/reference collections and an explicit `traversal_boundary`. It neither decodes old target fields nor expands that node through local adjacency or another service. Incoming persisted lineage can display the boundary; outgoing assertions are omitted. Root, object-detail, rooted DAG and visible-scope DAG use the same policy.

The federation client validates this boundary contract, refuses targets/outgoing edges on boundary records, retains valid branches and emits a warning and boundary count. Shared and Kahlo viewers label these nodes **Unsupported reference / Not traversed**. Unrelated malformed data and authorization failures remain errors. No schema, template, stored-object, credential or workflow changes.

Focused regression source covers mixed branches, shared old-reference hubs, root boundaries without adjacency reads, visible-scope behavior, property withholding, continued valid foreign jumps, and rejection of scope violations or an expanding boundary. Python AST, JavaScript syntax and whitespace reviewed. New regression cases have not been executed; prior graph14case evidence does not establish this behavior.

Release target: immutable annotated TapDB11.0.8, then exact dependency adoption by consuming producers. The Ursa deployment remains coordinated with the separate active runner repair; the shared source release does not itself change production. OWY remains paused.
