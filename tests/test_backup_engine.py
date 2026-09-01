"""Command construction, TOC parsing, and version gates.

The TOC fixture is genuine ``pg_restore --list`` output from a real
custom-format dump of the TAPDB schema, not a hand-written approximation --
which matters, because real entries contain shapes a plausible-looking fixture
would miss (function names with spaces, multi-word tags).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from daylily_tapdb.backup.engine import (
    ENGINE_AURORA,
    ENGINE_LOCAL,
    ArchiveInventory,
    assert_dump_client_is_new_enough,
    assert_restore_target_is_new_enough,
    build_pg_dump_command,
    build_pg_restore_command,
    build_pg_restore_list_command,
    build_pg_restore_verify_command,
    build_psql_command,
    client_env,
    connection_args,
    parse_toc,
    parse_version_major,
    run_command,
    server_version_major,
)
from daylily_tapdb.backup.errors import BackupVersionMismatchError

FIXTURE = Path(__file__).parent / "fixtures" / "pg_restore_toc_full.txt"

LOCAL_CFG = {
    "engine_type": ENGINE_LOCAL,
    "host": "localhost",
    "port": "5432",
    "user": "tapdb",
    "password": "s3cret",
    "database": "tapdb_shared",
    "schema_name": "tapdb_prod",
}


@pytest.fixture(scope="module")
def inventory() -> ArchiveInventory:
    return parse_toc(FIXTURE.read_text())


# --------------------------------------------------------------------------
# TOC parsing
# --------------------------------------------------------------------------


def test_every_entry_line_parses(inventory: ArchiveInventory):
    assert inventory.unparsed == []
    assert len(inventory.entries) == 186


def test_counts_match_the_real_schema_census(inventory: ArchiveInventory):
    counts = inventory.counts_by_kind()

    # Ground truth from a real dump of schema/tapdb_schema.sql.
    assert counts["TABLE"] == 9
    assert counts["FUNCTION"] == 22
    assert counts["TRIGGER"] == 16


def test_function_names_containing_spaces_are_parsed_whole(
    inventory: ArchiveInventory,
):
    # The case a hand-written fixture would have missed: pg_dump prints
    # overloaded signatures with a space after the comma.
    names = inventory.names_of_kind("FUNCTION")

    assert "meridian_generate_euid(text, bigint)" in names
    assert "meridian_generate_euid(text, bigint, text)" in names
    # The two overloads are distinct entries, not one mangled name.
    assert len([n for n in names if n.startswith("meridian_generate_euid")]) == 2


def test_multi_word_tags_are_not_split(inventory: ArchiveInventory):
    counts = inventory.counts_by_kind()

    assert counts["SEQUENCE SET"] == 12
    assert counts["TABLE DATA"] == 9
    assert counts["ROW SECURITY"] == 8
    assert counts["FK CONSTRAINT"] == 5
    # "SEQUENCE SET" must never be read as a SEQUENCE named "SET".
    assert "SET" not in inventory.names_of_kind("SEQUENCE")


def test_sequence_entries_cover_all_three_classes(inventory: ArchiveInventory):
    sequences = inventory.names_of_kind("SEQUENCE")

    assert "wx_instance_seq" in sequences  # static library
    assert "generic_instance_uid_seq" in sequences  # IDENTITY-backed
    assert len(sequences) == 12


def test_policies_are_captured(inventory: ArchiveInventory):
    # RLS policies ride along in a schema-scoped dump; preflight must then
    # confirm the roles they name exist on the target.
    assert inventory.counts_by_kind()["POLICY"] == 8


def test_schema_scoping_is_provable_from_the_archive(inventory: ArchiveInventory):
    # Issue #89 item 6: the dump itself proves what was captured.
    assert inventory.schema_names_seen() == ["tapdb_prod"]


def test_header_fields_are_parsed(inventory: ArchiveInventory):
    assert inventory.archive_format == "CUSTOM"
    assert inventory.source_server_version.startswith("16.")
    assert inventory.dumped_by_version.startswith("16.")


def test_schema_entry_has_no_parent_schema(inventory: ArchiveInventory):
    entry = next(e for e in inventory.entries if e.kind == "SCHEMA")

    assert entry.schema is None  # printed as "-" by pg_restore
    assert entry.name == "tapdb_prod"


def test_payload_is_json_shaped(inventory: ArchiveInventory):
    payload = inventory.to_payload()

    assert payload["entry_count"] == 186
    assert payload["schema_names_seen"] == ["tapdb_prod"]
    assert payload["unparsed_lines"] == 0


def test_empty_toc_parses_to_an_empty_inventory():
    result = parse_toc("")

    assert result.entries == []
    assert result.counts_by_kind() == {}
    assert result.schema_names_seen() == []


def test_garbage_lines_are_collected_not_silently_dropped():
    result = parse_toc("this is not a toc line\n1; 2 3 TABLE public t owner\n")

    assert len(result.entries) == 1
    assert result.unparsed == ["this is not a toc line"]


def test_multiple_schemas_are_visible_when_present():
    text = "1; 2 3 TABLE alpha t o\n2; 2 4 TABLE beta u o\n"

    assert parse_toc(text).schema_names_seen() == ["alpha", "beta"]


@pytest.mark.parametrize(
    "line, kind, schema, name",
    [
        # Each of these shares a prefix with a shorter tag. Matching the
        # shorter one would read the leftover word as the schema, injecting a
        # bogus name into schema_names_seen().
        (
            "1; 2 3 OPERATOR CLASS public my_ops owner",
            "OPERATOR CLASS",
            "public",
            "my_ops",
        ),
        (
            "1; 2 3 OPERATOR FAMILY public my_fam owner",
            "OPERATOR FAMILY",
            "public",
            "my_fam",
        ),
        (
            "1; 2 3 TABLE ATTACH public part_2026 owner",
            "TABLE ATTACH",
            "public",
            "part_2026",
        ),
        (
            "1; 2 3 INDEX ATTACH public idx_2026 owner",
            "INDEX ATTACH",
            "public",
            "idx_2026",
        ),
        (
            "1; 2 3 SEQUENCE SET tapdb wx_instance_seq owner",
            "SEQUENCE SET",
            "tapdb",
            "wx_instance_seq",
        ),
        ("1; 2 3 SHELL TYPE public mytype owner", "SHELL TYPE", "public", "mytype"),
    ],
)
def test_prefix_shadowing_tags_are_matched_longest_first(line, kind, schema, name):
    entry = parse_toc(line).entries[0]

    assert (entry.kind, entry.schema, entry.name) == (kind, schema, name)


def test_shadowing_tags_do_not_pollute_schema_names_seen():
    # The failure that motivated the longest-match fix: a mis-parsed tag adds a
    # fake schema, breaking the isolation proof for reasons unrelated to scope.
    text = "1; 2 3 TABLE tapdb_prod t o\n2; 2 4 OPERATOR CLASS tapdb_prod ops o\n"

    assert parse_toc(text).schema_names_seen() == ["tapdb_prod"]


def test_tag_list_is_ordered_longest_first():
    from daylily_tapdb.backup.engine import _TOC_TAGS

    lengths = [len(tag) for tag in _TOC_TAGS]

    assert lengths == sorted(lengths, reverse=True)


def test_an_unknown_multi_word_tag_is_not_split_into_a_fake_schema():
    # A tag from some future PostgreSQL release.
    entry = parse_toc("1; 2 3 QUANTUM INDEX public thing owner").entries[0]

    assert entry.kind == "QUANTUM INDEX"
    assert entry.schema == "public"
    assert entry.name == "thing"


# --------------------------------------------------------------------------
# Command construction
# --------------------------------------------------------------------------


def test_connection_args_are_the_shared_flags():
    assert connection_args(LOCAL_CFG) == [
        "-h",
        "localhost",
        "-p",
        "5432",
        "-U",
        "tapdb",
        "-d",
        "tapdb_shared",
    ]


def test_connection_args_accept_overrides():
    args = connection_args(LOCAL_CFG, database="restore_target", user="admin")

    assert "restore_target" in args
    assert "admin" in args


def test_dump_is_schema_scoped_and_custom_format(tmp_path: Path):
    cmd = build_pg_dump_command(
        LOCAL_CFG, schema_name="tapdb_prod", output_path=tmp_path / "a.dump"
    )

    assert cmd[0] == "pg_dump"
    assert ["--schema", "tapdb_prod"] == cmd[
        cmd.index("--schema") : cmd.index("--schema") + 2
    ]
    assert ["--format", "custom"] == cmd[
        cmd.index("--format") : cmd.index("--format") + 2
    ]
    assert "--no-owner" in cmd and "--no-acl" in cmd
    # The bug this whole subsystem replaces: never enumerate tables.
    assert "-t" not in cmd and "--table" not in cmd


def test_dump_passes_the_snapshot_when_one_was_exported(tmp_path: Path):
    cmd = build_pg_dump_command(
        LOCAL_CFG,
        schema_name="tapdb_prod",
        output_path=tmp_path / "a.dump",
        snapshot="00000003-0000001B-1",
    )

    assert ["--snapshot", "00000003-0000001B-1"] == cmd[-2:]


def test_dump_omits_snapshot_when_unavailable(tmp_path: Path):
    cmd = build_pg_dump_command(
        LOCAL_CFG, schema_name="tapdb_prod", output_path=tmp_path / "a.dump"
    )

    assert "--snapshot" not in cmd


def test_restore_list_needs_no_database(tmp_path: Path):
    cmd = build_pg_restore_list_command(tmp_path / "a.dump")

    assert cmd[:2] == ["pg_restore", "--list"]
    assert "-d" not in cmd


def test_deep_verify_reads_the_archive_without_a_target(tmp_path: Path):
    cmd = build_pg_restore_verify_command(tmp_path / "a.dump")

    assert "-d" not in cmd
    assert "--file" in cmd
    assert cmd[cmd.index("--file") + 1] in ("/dev/null", "nul")


def test_restore_uses_a_single_transaction_by_default(tmp_path: Path):
    cmd = build_pg_restore_command(
        LOCAL_CFG, archive_path=tmp_path / "a.dump", database="restore_target"
    )

    assert "--single-transaction" in cmd
    assert "restore_target" in cmd
    assert "--no-owner" in cmd


def test_restore_without_single_transaction_still_exits_on_error(tmp_path: Path):
    cmd = build_pg_restore_command(
        LOCAL_CFG,
        archive_path=tmp_path / "a.dump",
        database="t",
        single_transaction=False,
    )

    assert "--single-transaction" not in cmd
    assert "--exit-on-error" in cmd


def test_sql_render_command_targets_stdout(tmp_path: Path):
    from daylily_tapdb.backup.engine import build_pg_restore_sql_command

    cmd = build_pg_restore_sql_command(tmp_path / "a.dump", section="post-data")

    assert cmd[:2] == ["pg_restore", "--file"]
    assert cmd[2] == "-"
    assert ["--section", "post-data"] == cmd[3:5]
    assert "-d" not in cmd  # reads the archive, never a database


# --------------------------------------------------------------------------
# Policy role extraction
#
# The roles a policy grants to are not in the table of contents -- only in the
# statements. Since roles are excluded from the artifact by --no-acl, a policy
# naming a role the target lacks fails partway through a restore, so preflight
# has to read them out.
# --------------------------------------------------------------------------


def test_a_policy_with_no_to_clause_requires_no_roles():
    from daylily_tapdb.backup.engine import policy_roles

    sql = (
        "CREATE POLICY generic_instance_domain_isolation ON tapdb.generic_instance\n"
        "    USING ((domain_code = tapdb_current_domain_code()));\n"
    )

    # This is TAPDB's own shape: policies apply to PUBLIC.
    assert policy_roles(sql) == set()


def test_a_single_role_is_extracted():
    from daylily_tapdb.backup.engine import policy_roles

    sql = (
        "CREATE POLICY tenant_isolation ON public.orders FOR ALL TO app_user "
        "USING ((tenant_id = current_tenant()));"
    )

    assert policy_roles(sql) == {"app_user"}


def test_multiple_roles_are_extracted():
    from daylily_tapdb.backup.engine import policy_roles

    sql = (
        "CREATE POLICY p ON t FOR SELECT TO app_user, readonly_user, "
        "reporting USING (true);"
    )

    assert policy_roles(sql) == {"app_user", "readonly_user", "reporting"}


def test_quoted_role_names_are_unwrapped():
    from daylily_tapdb.backup.engine import policy_roles

    sql = 'CREATE POLICY p ON t TO "Mixed Case Role" USING (true);'

    assert policy_roles(sql) == {"Mixed Case Role"}


def test_public_and_session_keywords_are_not_roles():
    from daylily_tapdb.backup.engine import policy_roles

    sql = (
        "CREATE POLICY a ON t TO PUBLIC USING (true);"
        "CREATE POLICY b ON t TO current_user USING (true);"
        "CREATE POLICY c ON t TO session_user USING (true);"
    )

    # These always resolve and are not grantable roles.
    assert policy_roles(sql) == set()


def test_a_with_check_clause_terminates_the_role_list():
    from daylily_tapdb.backup.engine import policy_roles

    sql = "CREATE POLICY p ON t FOR INSERT TO writer WITH CHECK ((x > 0));"

    assert policy_roles(sql) == {"writer"}


def test_roles_are_collected_across_many_policies():
    from daylily_tapdb.backup.engine import policy_roles

    sql = "\n".join(
        [
            "CREATE POLICY a ON t1 TO alpha USING (true);",
            "CREATE POLICY b ON t2 USING (true);",
            "CREATE POLICY c ON t3 FOR UPDATE TO beta, gamma USING (true);",
        ]
    )

    assert policy_roles(sql) == {"alpha", "beta", "gamma"}


def test_a_to_outside_a_policy_is_not_mistaken_for_a_role():
    from daylily_tapdb.backup.engine import policy_roles

    sql = "GRANT SELECT ON t TO someone;\nALTER TABLE t OWNER TO other;"

    assert policy_roles(sql) == set()


def test_empty_input_yields_no_roles():
    from daylily_tapdb.backup.engine import policy_roles

    assert policy_roles("") == set()
    assert policy_roles(None) == set()


def test_psql_command_is_quiet_and_fails_fast():
    cmd = build_psql_command(LOCAL_CFG, sql="SELECT 1")

    assert cmd[0] == "psql"
    assert "-X" in cmd and "ON_ERROR_STOP=1" in cmd
    assert cmd[-2:] == ["-c", "SELECT 1"]


# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------


def test_local_env_carries_the_password():
    env = client_env(LOCAL_CFG)

    assert env["PGPASSWORD"] == "s3cret"
    assert "PGSSLMODE" not in env


def test_local_env_omits_password_when_unset():
    env = client_env({**LOCAL_CFG, "password": ""})

    assert "PGPASSWORD" not in env


def test_local_env_pins_hostaddr_when_configured():
    env = client_env({**LOCAL_CFG, "hostaddr": "10.0.0.7"})

    assert env["PGHOSTADDR"] == "10.0.0.7"


def test_aurora_env_delegates_to_the_shared_deployer(monkeypatch):
    from daylily_tapdb.aurora import schema_deployer as deployer_mod

    captured = {}

    def _fake_client_env(**kwargs):
        captured.update(kwargs)
        return {"PGPASSWORD": "iam-token", "PGSSLMODE": "verify-full"}

    monkeypatch.setattr(
        deployer_mod.AuroraSchemaDeployer, "client_env", _fake_client_env
    )

    env = client_env(
        {
            **LOCAL_CFG,
            "engine_type": ENGINE_AURORA,
            "region": "us-west-2",
            "iam_auth": "true",
            "hostaddr": "10.1.2.3",
        }
    )

    # Dumps and restores must inherit the same auth + TLS as every other
    # TAPDB database command -- the gap the legacy backup command left open.
    assert env["PGSSLMODE"] == "verify-full"
    assert captured["iam_auth"] is True
    assert captured["hostaddr"] == "10.1.2.3"
    assert captured["region"] == "us-west-2"
    # client_env supplies credentials only; targeting stays in argv.
    assert "database" not in captured


# --------------------------------------------------------------------------
# Versions
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("16.14", 16),
        ("16.14 (Homebrew)", 16),
        ("pg_dump (PostgreSQL) 16.14", 16),
        ("9.6.24", 9),
        ("17beta1", 17),
        (None, None),
        ("no digits here", None),
    ],
)
def test_parse_version_major(text, expected):
    assert parse_version_major(text) == expected


@pytest.mark.parametrize(
    "num, expected", [(160014, 16), (90624, 9), (110002, 11), (None, None)]
)
def test_server_version_major(num, expected):
    assert server_version_major(num) == expected


def test_dump_refuses_a_client_older_than_the_server():
    with pytest.raises(BackupVersionMismatchError) as excinfo:
        assert_dump_client_is_new_enough(
            client_version_text="pg_dump (PostgreSQL) 15.6",
            server_version_text="16.14",
        )

    assert excinfo.value.code == "version_mismatch"
    assert excinfo.value.detail["server_major"] == 16


def test_dump_accepts_a_newer_or_equal_client():
    assert_dump_client_is_new_enough(
        client_version_text="pg_dump (PostgreSQL) 16.14",
        server_version_text="16.14",
    )
    assert_dump_client_is_new_enough(
        client_version_text="pg_dump (PostgreSQL) 17.0",
        server_version_text="16.14",
    )


def test_restore_refuses_to_go_backward_a_major_version():
    with pytest.raises(BackupVersionMismatchError, match="restoring backward"):
        assert_restore_target_is_new_enough(
            source_server_version="16.14", target_server_version="15.6"
        )


def test_restore_accepts_forward_and_equal_targets():
    assert_restore_target_is_new_enough(
        source_server_version="15.6", target_server_version="16.14"
    )
    assert_restore_target_is_new_enough(
        source_server_version="16.14", target_server_version="16.14"
    )


def test_version_gates_stay_quiet_when_a_version_is_unknown():
    # Unknown is not the same as mismatched; refusing here would block
    # restores for a cosmetic reason.
    assert_restore_target_is_new_enough(
        source_server_version=None, target_server_version="16.14"
    )
    assert_dump_client_is_new_enough(
        client_version_text="pg_dump", server_version_text=None
    )


# --------------------------------------------------------------------------
# Subprocess wrapper
# --------------------------------------------------------------------------


def test_missing_binary_is_reported_actionably():
    result = run_command(["tapdb-definitely-not-a-real-binary", "--version"])

    assert not result.ok
    assert result.returncode == 127
    assert "not found" in result.stderr


def test_successful_command_captures_stdout():
    result = run_command(["echo", "hello"])

    assert result.ok
    assert result.stdout.strip() == "hello"


def test_failing_command_is_not_ok():
    result = run_command(["false"])

    assert not result.ok
    assert result.returncode != 0


# ---------------------------------------------------------------------------
# aurora snapshot endpoint pinning (plan section 3.2)
# ---------------------------------------------------------------------------


def test_snapshot_pinning_uses_the_client_resolved_address_not_the_servers(
    tmp_path, monkeypatch
):
    """Pin to what *this machine* resolves, never to the server's own address.

    Found by running against a real Aurora cluster: `inet_server_addr()`
    returns the backend's VPC-internal address (172.31.x.x), and pinning
    PGHOSTADDR to it made pg_dump fail with "Network is unreachable" from any
    client outside the VPC. The goal is only that the session and the dump
    reach the same backend; resolving the endpoint on the client side achieves
    that while staying routable from wherever the client actually is.

    Every unit test passed with the broken version, because none of them
    connects from outside a VPC.
    """
    from daylily_tapdb.backup import service

    captured: dict = {}

    def _fake_run(cmd, env=None, **kwargs):
        captured["env"] = dict(env or {})
        raise RuntimeError("stop after the env is built")

    monkeypatch.setattr(service.engine, "run_command", _fake_run)
    monkeypatch.setattr(service, "_client_resolved_address", lambda host: "203.0.113.9")

    with pytest.raises(RuntimeError):
        service._run_dump(
            {
                "engine_type": "aurora",
                "database": "d",
                "user": "u",
                "host": "cluster.us-east-1.rds.amazonaws.com",
                "port": "5432",
                "region": "us-east-1",
                "iam_auth": "false",
                "password": "x",
            },
            schema_name="s",
            artifact=tmp_path / "a.dump",
            snapshot="00000008-00000B84-1",
            # What the *server* reports -- must NOT be used to connect.
            backend={"address": "172.31.80.180", "port": 5432},
        )

    assert captured["env"].get("PGHOSTADDR") == "203.0.113.9", (
        "pinned to the server's self-reported address instead of the "
        "client-resolved one"
    )
    assert captured["env"].get("PGHOSTADDR") != "172.31.80.180"


def test_a_local_snapshot_dump_is_not_pinned(tmp_path, monkeypatch):
    """A local connection always reaches the same postmaster.

    Pinning there buys nothing and actively broke backups: `inet_server_addr()`
    renders as `::1/128`, which PGHOSTADDR rejects with
    "could not parse network address".
    """
    from daylily_tapdb.backup import service

    captured: dict = {}

    def _fake_run(cmd, env=None, **kwargs):
        captured["env"] = dict(env or {})
        raise RuntimeError("stop after the env is built")

    monkeypatch.setattr(service.engine, "run_command", _fake_run)
    with pytest.raises(RuntimeError):
        service._run_dump(
            {
                "engine_type": "local",
                "database": "d",
                "user": "u",
                "host": "localhost",
                "port": "5432",
            },
            schema_name="s",
            artifact=tmp_path / "a.dump",
            snapshot="00000003-0000000A-1",
            backend={"address": "::1", "port": 5432},
        )

    assert "PGHOSTADDR" not in captured["env"]


def test_an_aurora_target_without_a_region_is_refused():
    """No silent region default when minting an IAM auth token.

    The fallback was `us-west-2`. An auth token is minted *for a region*, so a
    default quietly authenticates against a cluster the operator never named --
    precisely what a "this region only" constraint exists to prevent.
    """
    from daylily_tapdb.backup import engine as engine_mod

    with pytest.raises(ValueError, match="explicit 'region'"):
        engine_mod.client_env(
            {
                "engine_type": "aurora",
                "host": "cluster.example.rds.amazonaws.com",
                "port": 5432,
                "user": "tapdb_admin",
                "region": "",
            }
        )


def test_snapshot_pinning_does_not_clobber_a_configured_hostaddr(tmp_path, monkeypatch):
    """A tunnelled private cluster must stay reachable.

    `hostaddr` is how a private Aurora cluster is reached through an SSM or SSH
    tunnel: `host` keeps the real endpoint so `verify-full` still validates the
    certificate, while `hostaddr` points at the local tunnel. Snapshot pinning
    overwriting it with the backend's VPC-internal address makes the dump
    unreachable from outside the VPC.
    """
    from daylily_tapdb.backup import service

    captured: dict = {}

    def _fake_run(cmd, env=None, **kwargs):
        captured["env"] = dict(env or {})
        raise RuntimeError("stop after the env is built")

    monkeypatch.setattr(service.engine, "run_command", _fake_run)
    with pytest.raises(RuntimeError):
        service._run_dump(
            {
                "engine_type": "aurora",
                "database": "d",
                "user": "u",
                "host": "real.cluster.us-east-1.rds.amazonaws.com",
                "port": "5432",
                "region": "us-east-1",
                "iam_auth": "false",
                "password": "x",
                "hostaddr": "127.0.0.1",
            },
            schema_name="s",
            artifact=tmp_path / "a.dump",
            snapshot="00000003-0000000A-1",
            backend={"address": "10.0.1.7", "port": 5432},
        )

    assert captured["env"].get("PGHOSTADDR") == "127.0.0.1", (
        "snapshot pinning overrode the operator's tunnel address"
    )
