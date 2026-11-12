# Current continuation state

Objective: prepare the native TapDB template builder fix requested after the Bloom labware demonstration pack could not be saved.

Authority: user selected “Prepare the native builder fix” on November 12. This authorizes source preparation. No package release, deployment, runtime hot patch, credential/config change, tests, PR, or merge is in scope.

Base: TapDB 11.0.2, currently consumed by Bloom day 10.1.1. Branch codex/template-save-isolation-20261112. The separate Bloom pack is committed and pushed at 265aed2 on codex/bloom-ursa-api-guide-20261030.

Observed failure: the native form validated the 26-template pack but Save rolled back with “Capture requires an active REPEATABLE READ or SERIALIZABLE transaction.” The separately approved CLI loader stopped at operator preflight because the live runtime target has no operator configuration. No new templates were committed.

Fix prepared: RuntimeDBConnection.session_scope accepts an explicit per-transaction isolation level, applied via Session.connection before transaction-context SQL. The native template Save route selects REPEATABLE READ. The cached Engine and SessionFactory defaults are not changed. Authentication, actor attribution, template immutability, commit/rollback ownership, and the existing capture requirement remain intact. Other callers retain their current isolation behavior.

Source diagnosis: session_scope begins a virtual transaction, then executes context SQL; template seeding's identity inventory capture requires REPEATABLE READ or SERIALIZABLE. The isolation must therefore be selected before context SQL, not after entering the yielded session. SQLAlchemy documents this per-transaction Session.connection(execution_options=...) pattern at https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#setting-isolation-for-individual-transactions.

Validation: source-grounded preparation only. Tests and runtime trials were not run under the user's no-tests policy. Production remains unchanged, so the live builder fix and demo cohort remain pending.

Next: release and deploy this TapDB fix to the selected Bloom service only after requested. Then repeat the originally authorized template Save operation and create the demonstration cohort through native owning controls. Do not use a runtime role as an operator or bypass the capture requirement.
