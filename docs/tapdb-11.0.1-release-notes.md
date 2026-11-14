# TapDB 11.0.1

Native backup planning previously rejected valid sealed recovery families whose
retained journals were outside `<config_dir>/backups/receipts`. Operators can now
set `backup.receipts_directory` through
`tapdb --config PATH config update --backup-receipts-directory DIRECTORY`.

The setting selects an existing absolute canonical local directory. Explicit
blank/null, missing directories, files, relative paths and aliases fail. Omission
retains the established default. Native backup operations and their CLI/API/GUI
adapters use the same configured directory. Recovery-family membership checks,
receipt hashes, retained history and recovery floors remain enforced.

No schema, identifier, allocator, dependency or attribution-contract changes are
included. This patch alone does not require service container rebuilds for
operator-only backup use. Embedding the new pin in consumers does require their
own releases. Migrating consumers from 10.x still requires the complete
[11.x adoption contract](tapdb-11-integrity.md).

Validation: **19 focused checks passed**, using temporary files and mocked
capture only. They establish configuration rejection, retained-family matching,
corrupt-chain rejection and CLI/API appends to the same original chain. No
database, AWS, live-service, broad-suite or production acceptance test was run.
Two existing Typer/Click deprecation warnings remain.

See [backup configuration](backup-and-recovery.md#retained-receipt-journals-1101),
[the controlling ledger](plans/20260930T212453Z_backup_receipt_directory_ledger.md)
and [Dayhoff rollout prerequisites](plans/20260930_tapdb_11_0_1_dayhoff_handoff.md).
No consumer deployment or migration is included in this package release.
