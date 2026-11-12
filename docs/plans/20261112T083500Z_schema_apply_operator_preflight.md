# Current continuation state

Objective: complete the explicitly approved Bloom-only native schema refresh needed for the11.0.7 instance/lineage read-policy optimization, then load the approved DAG template pack. Bloom11.0.1/TapDB11.0.10 is online; no sibling service changes.

Failure: native `tapdb db schema apply` checked database existence with the runtime login against `postgres`, swallowed its connection failure and reported that the known-existing Bloom database was absent. No schema change was applied. The native seed operator preflight had already authenticated to the exact Bloom database/schema; its later policy-contract failure identified the required refresh.

Repair: schema apply now reuses the existing operator target/schema preflight. It connects to the configured target, verifies operator authority for an existing schema, and reports connection/authority failure without mislabeling it as absence. Preserve existing schema application, immutable principal binding, grants and transaction controls. No raw SQL, credential rewrite, authentication bypass or new fallback.

Release target:11.0.11, based on11.0.10. Deploy this CLI package only into the approved isolated EC2 operator environment. Bloom's running image remains11.0.1 withTapDB11.0.10; the schema assets are unchanged between the two TapDB patches. No tests were run under the user's release policy.

Authority: user explicitly selected “Approve Bloom-only native refresh”; existing approvals cover native loader, isolated CLI bootstrap and the Bloom refactor release. Source repair follows the intended native command after its failure.

Next: publish11.0.11; install into /home/ubuntu/bloom-dag-release-20261112/tapdb-11.0.10-venv using approved existing pip --python; rerun native schema apply with /home/ubuntu/bloom_ops/tapdb101-20260911/operator.yaml; then native seed with the explicit parent example-pack directory and skip-existing. Trust native terminal results.
