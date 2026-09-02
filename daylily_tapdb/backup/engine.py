"""PostgreSQL client invocation: command builders, TOC parsing, version gates.

Every ``pg_dump``/``pg_restore``/``psql`` invocation in the backup subsystem is
constructed here, so no surface builds its own. Two decisions are load-bearing:

* **Schema-scoped, not table-scoped.** ``--schema`` captures every table,
  sequence, function, trigger, index, and policy the schema contains, including
  ones added after this code was written. Enumerating tables is precisely the
  bug this subsystem replaces.
* **Custom format.** ``-Fc`` is what makes ``pg_restore --list`` (inventory
  without a database) and ``pg_restore -f /dev/null`` (full corruption read
  without a target) possible, and it is the reason a corrupt backup can be
  rejected *before* anything is mutated.

Connection targeting is passed as flags rather than environment, because
``psql``, ``pg_dump``, and ``pg_restore`` accept ``-h/-p/-U/-d`` identically.
Credentials and TLS come from the environment -- see
``AuroraSchemaDeployer.client_env`` for the Aurora path.
"""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from daylily_tapdb.backup.errors import BackupVersionMismatchError

ENGINE_LOCAL = "local"
ENGINE_COMPOSE = "compose"
ENGINE_AURORA = "aurora"

DUMP_FORMAT_CUSTOM = "custom"
DEFAULT_ARTIFACT_NAME = "tapdb.dump"


def sanitized_libpq_environment() -> dict[str, str]:
    """Copy the process environment without ambient libpq/TapDB controls."""

    return {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith(("PG", "TAPDB_"))
    }


#: Archive entry descriptions emitted by pg_dump.
#:
#: Several are prefixes of others -- "SEQUENCE SET" of "SEQUENCE", "TABLE
#: ATTACH" of "TABLE", "OPERATOR CLASS" of "OPERATOR". Matching the shorter one
#: first does not merely mislabel the kind: the leftover word is then read as
#: the *schema*, which would inject a bogus name into ``schema_names_seen()``
#: -- the check that proves a dump was schema-scoped. The tuple is therefore
#: sorted longest-first at import so correctness never depends on hand-ordering
#: this list.
_RAW_TOC_TAGS: tuple[str, ...] = (
    "TEXT SEARCH CONFIGURATION",
    "TEXT SEARCH DICTIONARY",
    "TEXT SEARCH TEMPLATE",
    "MATERIALIZED VIEW DATA",
    "TEXT SEARCH PARSER",
    "MATERIALIZED VIEW",
    "PROCEDURAL LANGUAGE",
    "SEQUENCE OWNED BY",
    "PUBLICATION TABLE",
    "OPERATOR FAMILY",
    "OPERATOR CLASS",
    "SECURITY LABEL",
    "EVENT TRIGGER",
    "FOREIGN TABLE",
    "FK CONSTRAINT",
    "INDEX ATTACH",
    "TABLE ATTACH",
    "LARGE OBJECT",
    "SEQUENCE SET",
    "ROW SECURITY",
    "USER MAPPING",
    "DEFAULT ACL",
    "SUBSCRIPTION",
    "TABLE DATA",
    "PUBLICATION",
    "SHELL TYPE",
    "CONSTRAINT",
    "COLLATION",
    "AGGREGATE",
    "STATISTICS",
    "TRANSFORM",
    "EXTENSION",
    "PROCEDURE",
    "OPERATOR",
    "FUNCTION",
    "SEQUENCE",
    "COMMENT",
    "TRIGGER",
    "DEFAULT",
    "SERVER",
    "SCHEMA",
    "POLICY",
    "DOMAIN",
    "INDEX",
    "TABLE",
    "BLOBS",
    "VIEW",
    "TYPE",
    "CAST",
    "RULE",
    "BLOB",
    "ACL",
)

_TOC_TAGS: tuple[str, ...] = tuple(sorted(_RAW_TOC_TAGS, key=len, reverse=True))

_TOC_ENTRY = re.compile(r"^(\d+);\s+(\d+)\s+(\d+)\s+(.*)$")
_HEADER_FIELD = re.compile(r"^;\s{4,}([^:]+):\s*(.*)$")
_VERSION_TEXT = re.compile(r"(\d+)(?:\.(\d+))?")


@dataclass(frozen=True)
class TocEntry:
    """One entry from a custom-format archive's table of contents."""

    dump_id: int
    table_oid: int
    oid: int
    kind: str
    schema: Optional[str]
    name: str
    owner: Optional[str]

    def to_payload(self) -> dict[str, Any]:
        return {
            "dump_id": self.dump_id,
            "kind": self.kind,
            "schema": self.schema,
            "name": self.name,
        }


@dataclass
class ArchiveInventory:
    """What a dump actually contains, read from its table of contents.

    This is the evidence for issue #89 item 6: the archive itself proves which
    schema was captured, independent of what the manifest claims.
    """

    entries: list[TocEntry] = field(default_factory=list)
    header: dict[str, str] = field(default_factory=dict)
    unparsed: list[str] = field(default_factory=list)

    def counts_by_kind(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for entry in self.entries:
            counts[entry.kind] = counts.get(entry.kind, 0) + 1
        return dict(sorted(counts.items()))

    def schema_names_seen(self) -> list[str]:
        """Return every schema the archive touches.

        A schema-scoped dump must yield exactly one. More than one means the
        capture was not scoped the way the manifest says it was.
        """
        names: set[str] = set()
        for entry in self.entries:
            if entry.schema and entry.schema != "-":
                names.add(entry.schema)
            elif entry.kind == "SCHEMA":
                names.add(entry.name)
        return sorted(names)

    def names_of_kind(self, kind: str) -> list[str]:
        return sorted(entry.name for entry in self.entries if entry.kind == kind)

    @property
    def source_server_version(self) -> Optional[str]:
        return self.header.get("Dumped from database version")

    @property
    def dumped_by_version(self) -> Optional[str]:
        return self.header.get("Dumped by pg_dump version")

    @property
    def archive_format(self) -> Optional[str]:
        return self.header.get("Format")

    def to_payload(self) -> dict[str, Any]:
        return {
            "entry_count": len(self.entries),
            "counts_by_kind": self.counts_by_kind(),
            "schema_names_seen": self.schema_names_seen(),
            "format": self.archive_format,
            "source_server_version": self.source_server_version,
            "dumped_by_version": self.dumped_by_version,
            "unparsed_lines": len(self.unparsed),
        }


def _split_toc_rest(rest: str) -> tuple[str, Optional[str], str, Optional[str]]:
    """Split the post-oid remainder into (kind, schema, name, owner).

    Naive tokenizing does not work here: real entries include
    ``FUNCTION tapdb_prod meridian_generate_euid(text, bigint) postgres``,
    where the *name* contains a space. So the tag is matched from a known set,
    the next token is the schema, the final token is the owner, and everything
    between them is the name.
    """
    kind = ""
    for tag in _TOC_TAGS:
        if rest == tag or rest.startswith(tag + " "):
            kind = tag
            break

    if not kind:
        # Unknown tag -- a future PostgreSQL release, most likely. Take the
        # leading run of all-caps words rather than just the first word, so a
        # multi-word tag is not silently split with its tail read as a schema
        # name. (This heuristic would over-consume an all-caps schema name, but
        # it only ever runs for tags absent from the list above, which covers
        # everything pg_dump emits today.)
        words = rest.split()
        leading = []
        for word in words:
            if word.isupper() and word.isalpha():
                leading.append(word)
            else:
                break
        if not leading:
            head, _, tail = rest.partition(" ")
            return head, None, tail.strip(), None
        kind = " ".join(leading)

    remainder = rest[len(kind) :].strip()
    parts = remainder.split()
    if not parts:
        return kind, None, "", None
    if len(parts) == 1:
        return kind, None, parts[0], None

    schema = parts[0]
    owner = parts[-1] if len(parts) > 2 else None
    name = " ".join(parts[1:-1]) if len(parts) > 2 else parts[1]
    return kind, (None if schema == "-" else schema), name, owner


def parse_toc(text: str) -> ArchiveInventory:
    """Parse ``pg_restore --list`` output into an inventory."""
    inventory = ArchiveInventory()
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line.strip():
            continue
        if line.startswith(";"):
            match = _HEADER_FIELD.match(line)
            if match:
                inventory.header[match.group(1).strip()] = match.group(2).strip()
            continue
        match = _TOC_ENTRY.match(line)
        if not match:
            inventory.unparsed.append(line)
            continue
        kind, schema, name, owner = _split_toc_rest(match.group(4).strip())
        inventory.entries.append(
            TocEntry(
                dump_id=int(match.group(1)),
                table_oid=int(match.group(2)),
                oid=int(match.group(3)),
                kind=kind,
                schema=schema,
                name=name,
                owner=owner,
            )
        )
    return inventory


def connection_args(
    cfg: Mapping[str, Any],
    *,
    database: Optional[str] = None,
    user: Optional[str] = None,
) -> list[str]:
    """Return the ``-h/-p/-U/-d`` flags shared by every libpq client."""
    return [
        "-h",
        str(cfg["host"]),
        "-p",
        str(cfg["port"]),
        "-U",
        str(user or cfg["user"]),
        "-d",
        str(database or cfg["database"]),
    ]


def client_env(cfg: Mapping[str, Any]) -> dict[str, str]:
    """Return the subprocess environment for this target's engine type.

    Aurora delegates to ``AuroraSchemaDeployer.client_env`` so dumps and
    restores inherit the same IAM/Secrets-Manager credential and
    ``sslmode=verify-full`` TLS that every other TAPDB database command uses.
    Closing that gap is the point: today's backup command hand-builds a raw
    ``pg_dump`` and cannot reach an Aurora target at all.
    """
    engine_type = str(cfg.get("engine_type") or ENGINE_LOCAL).lower()

    if engine_type == ENGINE_AURORA:
        from daylily_tapdb.aurora.schema_deployer import AuroraSchemaDeployer

        iam_auth = str(cfg.get("iam_auth", "true")).lower() in ("true", "1", "yes")
        # No default region. An IAM auth token is minted *for a region*, so a
        # silent fallback would quietly authenticate against a cluster in a
        # region the operator never named -- the one failure mode that region
        # constraints exist to prevent. `get_db_config` already requires
        # `target.region` for aurora; this closes the path for any caller that
        # builds cfg by hand.
        region = str(cfg.get("region") or "").strip()
        if not region:
            raise ValueError(
                "Aurora targets require an explicit 'region'; refusing to guess "
                "which region to mint an auth token for."
            )
        resolved = AuroraSchemaDeployer.client_env(
            host=str(cfg["host"]),
            port=int(cfg["port"]),
            user=str(cfg["user"]),
            region=region,
            iam_auth=iam_auth,
            secret_arn=cfg.get("secret_arn") or None,
            password=cfg.get("password") or None,
            hostaddr=cfg.get("hostaddr") or None,
        )
        env_vars = sanitized_libpq_environment()
        for key in ("PGPASSWORD", "PGSSLMODE", "PGSSLROOTCERT"):
            if resolved.get(key):
                env_vars[key] = str(resolved[key])
        hostaddr = str(cfg.get("hostaddr") or "").strip()
        if hostaddr:
            env_vars["PGHOSTADDR"] = hostaddr
        return env_vars

    env_vars = sanitized_libpq_environment()
    password = cfg.get("password")
    if password:
        env_vars["PGPASSWORD"] = str(password)
    hostaddr = cfg.get("hostaddr")
    if hostaddr:
        env_vars["PGHOSTADDR"] = str(hostaddr).strip()
    return env_vars


def build_pg_dump_command(
    cfg: Mapping[str, Any],
    *,
    schema_name: str,
    output_path: Path,
    snapshot: Optional[str] = None,
    database: Optional[str] = None,
) -> list[str]:
    """Build the schema-scoped custom-format dump command.

    ``--no-owner``/``--no-acl`` keep cluster-scoped role state out of the
    artifact, which is what lets a backup restore onto an isolated target whose
    role set differs from production.
    """
    cmd = [
        "pg_dump",
        *connection_args(cfg, database=database),
        "--schema",
        schema_name,
        "--format",
        DUMP_FORMAT_CUSTOM,
        "--no-owner",
        "--no-acl",
        "--file",
        str(output_path),
    ]
    if snapshot:
        cmd.extend(["--snapshot", snapshot])
    return cmd


def build_pg_restore_list_command(archive_path: Path) -> list[str]:
    """Build the command that reads an archive's TOC without a database."""
    return ["pg_restore", "--list", str(archive_path)]


def build_pg_restore_verify_command(archive_path: Path) -> list[str]:
    """Build the deep-verify command: decompress and read every data block.

    Restoring to ``/dev/null`` touches no database yet forces the archive to be
    read end to end, which is what makes "a corrupted backup is rejected before
    the target is mutated" true rather than aspirational.
    """
    return ["pg_restore", "--file", os.devnull, str(archive_path)]


#: ``CREATE POLICY <name> ON <table> [AS ...] [FOR ...] [TO <roles>] ...``
#: The role list ends at USING, WITH CHECK, or the statement terminator.
#: ``[^;]*?`` rather than ``.*?``: with DOTALL and no boundary, a CREATE
#: POLICY that has no TO clause scanned forward past its own terminator into a
#: following ``GRANT ... TO role`` and reported that role as required by the
#: policy. Confining the match to a single statement fixes it.
_POLICY_TO_CLAUSE = re.compile(
    r"CREATE\s+POLICY\b[^;]*?\bTO\s+([^;]*?)(?=\bUSING\b|\bWITH\s+CHECK\b|;)",
    re.IGNORECASE | re.DOTALL,
)

#: Roles that always exist and are never granted; not worth checking for.
_BUILT_IN_ROLE_TOKENS = frozenset({"public", "current_user", "session_user"})


def build_pg_restore_sql_command(
    archive_path: Path,
    *,
    section: Optional[str] = None,
) -> list[str]:
    """Build a command that renders an archive back to SQL on stdout.

    Used to inspect statements the table of contents does not expose -- the
    TOC lists that a POLICY exists, but not which roles it grants to.
    """
    cmd = ["pg_restore", "--file", "-"]
    if section:
        cmd.extend(["--section", section])
    cmd.append(str(archive_path))
    return cmd


def policy_roles(sql_text: str) -> set[str]:
    """Return the roles named by ``CREATE POLICY`` statements.

    Roles are cluster-scoped and deliberately excluded from the artifact, so a
    policy naming a role the target lacks fails partway through a restore.
    Extracting them lets that failure happen in preflight instead.

    ``PUBLIC`` and the session keywords are filtered out: they always resolve
    and are not grantable roles.
    """
    roles: set[str] = set()
    for match in _POLICY_TO_CLAUSE.finditer(sql_text or ""):
        for token in match.group(1).split(","):
            name = token.strip().strip('"').strip()
            if name and name.lower() not in _BUILT_IN_ROLE_TOKENS:
                roles.add(name)
    return roles


def build_pg_restore_command(
    cfg: Mapping[str, Any],
    *,
    archive_path: Path,
    database: str,
    single_transaction: bool = True,
    no_owner: bool = True,
    exit_on_error: bool = True,
) -> list[str]:
    """Build the restore command for a target database."""
    cmd = ["pg_restore", *connection_args(cfg, database=database)]
    if single_transaction:
        # Implies --exit-on-error; a failed restore leaves nothing behind.
        cmd.append("--single-transaction")
    elif exit_on_error:
        cmd.append("--exit-on-error")
    if no_owner:
        cmd.extend(["--no-owner", "--no-acl"])
    cmd.append(str(archive_path))
    return cmd


def build_psql_command(
    cfg: Mapping[str, Any],
    *,
    sql: Optional[str] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
) -> list[str]:
    """Build a quiet, fail-fast psql command for maintenance statements."""
    cmd = [
        "psql",
        "-X",
        "-q",
        "-t",
        "-A",
        *connection_args(cfg, database=database, user=user),
        "-v",
        "ON_ERROR_STOP=1",
    ]
    if sql:
        cmd.extend(["-c", sql])
    return cmd


@dataclass(frozen=True)
class CommandResult:
    """Outcome of one client subprocess."""

    ok: bool
    stdout: str
    stderr: str
    returncode: int

    @property
    def output(self) -> str:
        return (self.stdout + self.stderr).strip()


def run_command(
    cmd: Sequence[str],
    *,
    env: Optional[Mapping[str, str]] = None,
    timeout: Optional[float] = None,
) -> CommandResult:
    """Run a client command, translating a missing binary into a clear error."""
    try:
        completed = subprocess.run(  # noqa: S603 - argv is built here, never a shell
            list(cmd),
            capture_output=True,
            text=True,
            env=dict(env) if env is not None else None,
            timeout=timeout,
        )
    except FileNotFoundError:
        binary = cmd[0] if cmd else "?"
        return CommandResult(
            ok=False,
            stdout="",
            stderr=(
                f"{binary} not found. Install the PostgreSQL client tools "
                "(they must be at least as new as the server)."
            ),
            returncode=127,
        )
    except subprocess.TimeoutExpired:
        return CommandResult(
            ok=False,
            stdout="",
            stderr=f"{cmd[0]} timed out after {timeout}s",
            returncode=124,
        )
    return CommandResult(
        ok=completed.returncode == 0,
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
        returncode=completed.returncode,
    )


def parse_version_major(text: Optional[str]) -> Optional[int]:
    """Return the major version from a version string.

    Handles ``16.14``, ``16.14 (Homebrew)``, ``pg_dump (PostgreSQL) 16.14``,
    and pre-10 forms like ``9.6.24`` (major 9).
    """
    if not text:
        return None
    match = _VERSION_TEXT.search(str(text))
    if not match:
        return None
    return int(match.group(1))


def server_version_major(server_version_num: Optional[int]) -> Optional[int]:
    """Return the major version encoded in ``server_version_num``.

    The encoding is ``major * 10000 + minor`` on modern PostgreSQL (160014 ->
    16) and ``major * 10000 + minor * 100 + patch`` before 10 (90624 -> 9), so
    integer division by 10000 is correct for both.
    """
    if server_version_num is None:
        return None
    return int(server_version_num) // 10000


def client_version(binary: str) -> Optional[str]:
    """Return a client binary's reported version, or None if unavailable."""
    result = run_command([binary, "--version"])
    return result.stdout.strip() if result.ok else None


def assert_dump_client_is_new_enough(
    *,
    client_version_text: Optional[str],
    server_version_text: Optional[str],
) -> None:
    """Refuse to dump with a client older than the server.

    An older ``pg_dump`` cannot represent everything a newer server holds, and
    PostgreSQL itself refuses the combination. Checking here turns that into a
    clear message at plan time rather than a cryptic failure mid-backup.
    """
    client_major = parse_version_major(client_version_text)
    server_major = parse_version_major(server_version_text)
    if client_major is None or server_major is None:
        return
    if client_major < server_major:
        raise BackupVersionMismatchError(
            f"pg_dump {client_major} is older than the server "
            f"({server_major}); dumping would be incomplete or refused.",
            detail={
                "pg_dump_major": client_major,
                "server_major": server_major,
            },
        )


def assert_restore_target_is_new_enough(
    *,
    source_server_version: Optional[str],
    target_server_version: Optional[str],
) -> None:
    """Refuse to restore a newer dump onto an older server.

    PostgreSQL supports restoring forward, never backward. Catching this in
    preflight is what keeps a doomed restore from touching the target at all.
    """
    source_major = parse_version_major(source_server_version)
    target_major = parse_version_major(target_server_version)
    if source_major is None or target_major is None:
        return
    if target_major < source_major:
        raise BackupVersionMismatchError(
            f"Backup was taken from PostgreSQL {source_major} but the restore "
            f"target runs {target_major}; restoring backward is not supported.",
            detail={
                "source_major": source_major,
                "target_major": target_major,
            },
        )


__all__ = [
    "DEFAULT_ARTIFACT_NAME",
    "DUMP_FORMAT_CUSTOM",
    "ENGINE_AURORA",
    "ENGINE_COMPOSE",
    "ENGINE_LOCAL",
    "ArchiveInventory",
    "CommandResult",
    "TocEntry",
    "assert_dump_client_is_new_enough",
    "assert_restore_target_is_new_enough",
    "build_pg_dump_command",
    "build_pg_restore_command",
    "build_pg_restore_list_command",
    "build_pg_restore_verify_command",
    "build_psql_command",
    "client_env",
    "client_version",
    "connection_args",
    "parse_toc",
    "parse_version_major",
    "run_command",
    "server_version_major",
]
