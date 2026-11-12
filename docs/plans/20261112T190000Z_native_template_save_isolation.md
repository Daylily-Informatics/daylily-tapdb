# Current continuation state

Objective: prepare the native TapDB template builder fix requested after the Bloom labware demonstration pack could not be saved.

Authority: user selected “Prepare the native builder fix,” then explicitly approved “Release and deploy to Bloom day” on November 12. Package release and a Bloom-only final tagged EC2 image/deployment are now authorized. No tests, PR, merge, sibling-service deployment, credential changes or runtime hot patches are authorized.

Preparation base: TapDB 11.0.2, consumed by Bloom day 10.1.1. Prepared commit8ddcdb8 remains on codex/template-save-isolation-20261112. Release base: published11.0.9, with the identical scoped fix applied on codex/template-save-isolation-11-0-10, preserving later allocator and graph fixes. Intended release11.0.10. The separate Bloom pack is committed and pushed at265aed2 on codex/bloom-ursa-api-guide-20261030.

Observed failure: the native form validated the 26-template pack but Save rolled back with “Capture requires an active REPEATABLE READ or SERIALIZABLE transaction.” The separately approved CLI loader stopped at operator preflight because the live runtime target has no operator configuration. No new templates were committed.

Fix prepared: RuntimeDBConnection.session_scope accepts an explicit per-transaction isolation level, applied via Session.connection before transaction-context SQL. The native template Save route selects REPEATABLE READ. The cached Engine and SessionFactory defaults are not changed. Authentication, actor attribution, template immutability, commit/rollback ownership, and the existing capture requirement remain intact. Other callers retain their current isolation behavior.

Source diagnosis: session_scope begins a virtual transaction, then executes context SQL; template seeding's identity inventory capture requires REPEATABLE READ or SERIALIZABLE. The isolation must therefore be selected before context SQL, not after entering the yielded session. SQLAlchemy documents this per-transaction Session.connection(execution_options=...) pattern at https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#setting-isolation-for-individual-transactions.

Validation: source-grounded preparation only. Tests and runtime trials were not run under the user's no-tests policy. Production remains unchanged, so the live builder fix and demo cohort remain pending.

Next: publish11.0.10, pin Bloom to that package, create the next unused annotated Bloom release, build its final image on i-07df3a933e4839f52/us-west-2, and deploy Bloom only. Then repeat the originally authorized template Save operation and create the demonstration cohort through native owning controls. Do not use a runtime role as an operator or bypass the capture requirement.
