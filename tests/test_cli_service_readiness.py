"""Registration policies for the public service-readiness lifecycle."""

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import yaml
from typer.testing import CliRunner

from daylily_tapdb.cli import framework_app
from daylily_tapdb.cli._registry_v2 import policy_for_command
from daylily_tapdb.cli.context import clear_cli_context
from daylily_tapdb.cli.db_config import get_db_config


@pytest.fixture(autouse=True)
def isolated_cli_context():
    clear_cli_context()
    yield
    clear_cli_context()


@pytest.mark.parametrize(
    ("group", "command", "mutating"),
    [
        ("db/identity", "inventory", False),
        ("db/identity", "verify", False),
        ("db/sequences", "verify", False),
        ("db/sequences", "advance", True),
        ("db/runtime-principal", "bootstrap", True),
        ("db/runtime-principal", "bind", True),
    ],
)
def test_readiness_registration_policies(group, command, mutating):
    policy = policy_for_command(group, command)
    assert policy.supports_json is True
    assert policy.mutates_state is mutating
    assert policy.supports_dry_run is mutating
    assert policy.interactive is False


def _aurora_config_args(tmp_path):
    config_path = tmp_path / "tapdb-config.yaml"
    domain = tmp_path / "domains.json"
    prefixes = tmp_path / "prefixes.json"
    certificate = tmp_path / "ca.pem"
    domain.write_text('{"version":"0.4.0","domains":{"Z":{"name":"test"}}}')
    prefixes.write_text('{"version":"0.4.0","ownership":{"Z":{}}}')
    certificate.write_text("test certificate fixture; no connection is made\n")
    return config_path, [
        "--config",
        str(config_path),
        "db-config",
        "init",
        "--client-id",
        "qualification",
        "--database-name",
        "readiness",
        "--owner-repo-name",
        "daylily-tapdb",
        "--domain-code",
        "Z",
        "--domain-registry-path",
        str(domain),
        "--prefix-ownership-registry-path",
        str(prefixes),
        "--engine-type",
        "aurora",
        "--host",
        "database.example.invalid",
        "--hostaddr",
        "127.0.0.1",
        "--port",
        "55434",
        "--server-port",
        "5432",
        "--ui-port",
        "18911",
        "--user",
        "readiness_runtime",
        "--database",
        "readiness_test",
        "--schema-name",
        "readiness",
        "--operator-user",
        "readiness_operator",
        "--operator-secret-arn",
        "arn:aws:secretsmanager:us-west-2:000000000000:secret:operator",
        "--region",
        "us-west-2",
        "--aws-profile",
        "qualification",
        "--cluster-identifier",
        "readiness-cluster",
        "--no-iam-auth",
        "--secret-arn",
        "arn:aws:secretsmanager:us-west-2:000000000000:secret:runtime",
        "--ssl",
        "verify-full",
        "--sslrootcert",
        str(certificate),
        "--safety-tier",
        "shared",
        "--destructive-operations",
        "blocked",
    ]


def test_aurora_config_init_and_update_preserve_explicit_contract(tmp_path):
    config_path, args = _aurora_config_args(tmp_path)
    runner = CliRunner()
    result = runner.invoke(framework_app, args)
    assert result.exit_code == 0, result.output
    cfg = get_db_config(config_path=config_path)
    assert cfg["region"] == "us-west-2"
    assert cfg["cluster_identifier"] == "readiness-cluster"
    assert cfg["iam_auth"] == "false"
    assert cfg["ssl"] == "verify-full"
    assert cfg["hostaddr"] == "127.0.0.1"
    assert cfg["server_port"] == "5432"
    assert cfg["operator_configured"] is True
    result = runner.invoke(
        framework_app,
        [
            "--config",
            str(config_path),
            "db-config",
            "update",
            "--region",
            "us-east-2",
            "--cluster-identifier",
            "readiness-east",
            "--aws-profile",
            "qualification-east",
            "--iam-auth",
        ],
    )
    assert result.exit_code == 0, result.output
    target = yaml.safe_load(config_path.read_text())["target"]
    assert target["region"] == "us-east-2"
    assert target["cluster_identifier"] == "readiness-east"
    assert target["iam_auth"] == "true"
    assert target["aws_profile"] == "qualification-east"


def test_registered_principal_bootstrap_is_offline_and_dry_run_vetoes_apply(
    tmp_path, monkeypatch
):
    import daylily_tapdb.runtime_principal as principal

    config_path, args = _aurora_config_args(tmp_path)
    runner = CliRunner()
    assert runner.invoke(framework_app, args).exit_code == 0
    monkeypatch.setattr(
        principal,
        "_operator_engine",
        lambda *args, **kwargs: pytest.fail("offline plan must not connect"),
    )
    for flags in ([], ["--dry-run"]):
        result = runner.invoke(
            framework_app,
            [
                "--config",
                str(config_path),
                *flags,
                "db",
                "runtime-principal",
                "bootstrap",
                *(["--apply"] if flags else []),
            ],
        )
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["status"] == "planned"


@pytest.mark.parametrize("group", ["identity", "sequences", "runtime-principal"])
def test_service_readiness_groups_are_registered_in_real_cli(tmp_path, group):
    config_path, args = _aurora_config_args(tmp_path)
    runner = CliRunner()
    assert runner.invoke(framework_app, args).exit_code == 0
    result = runner.invoke(
        framework_app, ["--config", str(config_path), "db", group, "--help"]
    )
    assert result.exit_code == 0, result.output
    assert group in result.output


def test_aurora_psql_keeps_setup_outside_create_database_transaction(
    tmp_path, monkeypatch
):
    from daylily_tapdb.aurora.schema_deployer import AuroraSchemaDeployer

    observed = []
    monkeypatch.setattr(
        AuroraSchemaDeployer,
        "client_env",
        lambda **kwargs: observed.append(kwargs) or {},
    )
    monkeypatch.setattr(
        "subprocess.run",
        lambda argv, **kwargs: (
            observed.append(argv)
            or type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()
        ),
    )
    ok, _ = AuroraSchemaDeployer.run_psql(
        host="database.example.invalid",
        port=55434,
        user="operator",
        database="postgres",
        region="us-west-2",
        profile="explicit",
        sslrootcert=str(tmp_path / "ca.pem"),
        setup_sql="SELECT 1",
        sql='CREATE DATABASE "qualification"',
    )
    assert ok
    assert observed[0]["profile"] == "explicit"
    assert observed[0]["sslrootcert"] == str(tmp_path / "ca.pem")
    assert observed[1][-4:] == [
        "-c",
        "SELECT 1",
        "-c",
        'CREATE DATABASE "qualification"',
    ]


@pytest.mark.parametrize(
    "missing",
    ["--region", "--cluster-identifier", "--ssl", "--sslrootcert", "--no-iam-auth"],
)
def test_aurora_config_missing_required_field_does_not_write(tmp_path, missing):
    config_path, args = _aurora_config_args(tmp_path)
    index = args.index(missing)
    del args[index : index + (1 if missing == "--no-iam-auth" else 2)]
    result = CliRunner().invoke(framework_app, args)
    assert result.exit_code != 0
    assert not config_path.exists()


def test_aurora_config_rejects_unverified_tls(tmp_path):
    config_path, args = _aurora_config_args(tmp_path)
    args[args.index("--ssl") + 1] = "require"
    result = CliRunner().invoke(framework_app, args)
    assert result.exit_code != 0
    assert not config_path.exists()


@pytest.mark.parametrize("outcome", ["success", "abort", "ambiguous_commit"])
@pytest.mark.parametrize("bound_family", [False, True])
def test_migrate_retains_session_and_reconciles_only_known_outcomes(
    tmp_path, monkeypatch, outcome, bound_family
):
    import daylily_tapdb.cli.db as db
    import daylily_tapdb.migration_identity as migration
    import daylily_tapdb.sequences as sequences

    config_path, args = _aurora_config_args(tmp_path)
    runner = CliRunner()
    assert runner.invoke(framework_app, args).exit_code == 0
    control_path = tmp_path / "control.yaml"
    control_path.write_text("explicit control fixture\n")
    reviewed_path = tmp_path / "reviewed.json"
    reviewed = {
        "receipt_version": migration.RECEIPT_VERSION,
        "sequence_inventory": {"reviewed": True},
    }
    family = {"reviewed": "family"} if bound_family else None
    provider = {"reviewed": "provider"} if bound_family else None
    quarantine = {"reviewed": "quarantine"} if bound_family else None
    extra_args = []
    if bound_family:
        reviewed["recovery_family"] = family
        for name, value in (
            ("recovery-family", family),
            ("provider-contract", provider),
            ("quarantine-receipt", quarantine),
        ):
            evidence_path = tmp_path / f"{name}.json"
            evidence_path.write_text(json.dumps(value))
            extra_args.extend(["--" + name, str(evidence_path)])
    reviewed["evidence_sha256"] = migration._sha256(reviewed)
    reviewed_path.write_text(json.dumps(reviewed))
    receipt_path = tmp_path / "result.json"
    events = []

    class ApplyError(ValueError):
        recovery_intent = {"intent": "retained"}
        sequence_result = {"status": "pending"}

    class Session:
        transactions = 0

        @contextmanager
        def begin(self):
            self.transactions += 1
            number = self.transactions
            events.append(f"begin-{number}")
            try:
                yield
            except Exception:
                events.append(f"rollback-{number}")
                raise
            if outcome == "ambiguous_commit" and number == 1:
                events.append("commit-ambiguous")
                raise RuntimeError("commit outcome unknown")
            events.append(f"commit-{number}")

    session = Session()
    control = object()

    @contextmanager
    def operator_session(cfg, **kwargs):
        is_control = cfg.get("is_control", False)
        events.append("control-open" if is_control else "target-open")
        yield control if is_control else session
        events.append("control-close" if is_control else "target-close")

    def control_config(*, config_path):
        assert config_path == control_path
        return {"is_control": True}

    def acquire(
        connection,
        *,
        control_connection,
        inventory,
        receipts_dir,
        provider_contract,
        quarantine_receipt,
        recovery_family,
    ):
        assert connection is session and control_connection is control
        assert inventory == {"reviewed": True}
        assert receipts_dir == tmp_path / "durable"
        assert provider_contract == provider and quarantine_receipt == quarantine
        assert recovery_family == family
        events.append("gate-closed")
        return {"fence": {"closed": True}}

    migration_result = SimpleNamespace(
        receipt={"applied_migrations": [], "allocator_changes": [{"advanced": True}]}
    )

    def apply(connection, **kwargs):
        assert connection is session
        assert kwargs["writer_fence"] == {"closed": True}
        events.append("apply")
        if outcome == "abort":
            raise ApplyError("known failure")
        return migration_result

    def finalize(connection, result, **kwargs):
        assert connection is session and result is migration_result
        assert "commit-1" in events
        events.append("finalize-success")
        return {"allocator_result": {"status": "committed"}}

    def abort(connection, **kwargs):
        assert connection is session
        assert "rollback-1" in events
        assert kwargs["recovery_intent"] == {"intent": "retained"}
        assert kwargs["sequence_result"] == {"status": "pending"}
        events.append("finalize-abort")
        return {"status": "aborted_floors_retained"}

    def release(connection, gate, *, result, receipts_dir, control_connection):
        assert connection is session and "commit-2" in events
        assert control_connection is control and "control-close" not in events
        assert result == {"status": "committed"}
        events.append("gate-opened")
        return {"status": "released"}

    monkeypatch.setattr(db, "operator_session", operator_session)
    monkeypatch.setattr(db, "get_db_config", control_config)
    monkeypatch.setattr(
        db, "_get_db_config", lambda env: get_db_config(config_path=config_path)
    )
    monkeypatch.setattr(db, "apply_migration_preflight", apply)
    monkeypatch.setattr(db, "finalize_migration_recovery", finalize)
    monkeypatch.setattr(db, "finalize_migration_abort", abort)
    monkeypatch.setattr(db, "_log_operation", lambda *args: None)
    monkeypatch.setattr(sequences, "acquire_database_writer_fence", acquire)
    monkeypatch.setattr(sequences, "release_database_writer_fence", release)
    result = runner.invoke(
        framework_app,
        [
            "--config",
            str(config_path),
            "db",
            "schema",
            "migrate",
            "--apply",
            "--preflight-receipt",
            str(reviewed_path),
            "--receipt",
            str(receipt_path),
            "--receipts-dir",
            str(tmp_path / "durable"),
            "--establish-writer-fence",
            "--control-config",
            str(control_path),
            *extra_args,
        ],
    )
    assert events[:3] == [
        "target-open",
        "control-open",
        "gate-closed",
    ], (result.output, result.exception)
    if outcome == "success":
        assert result.exit_code == 0, result.output
        assert "true no-op" not in result.output
        assert "allocators" not in result.output or "advanced" in result.output
        assert events[3:] == [
            "begin-1",
            "apply",
            "commit-1",
            "begin-2",
            "finalize-success",
            "commit-2",
            "gate-opened",
            "control-close",
            "target-close",
        ]
        assert (
            json.loads(receipt_path.read_text())["principal_binding_required"] is True
        )
    elif outcome == "abort":
        assert result.exit_code == 1, result.output
        assert "gate-opened" not in events
        assert "finalize-abort" in events
        assert (
            json.loads(receipt_path.read_text())["database_may_remain_closed"] is True
        )
    else:
        assert result.exit_code == 1, result.output
        assert "commit-ambiguous" in events
        assert "finalize-abort" not in events and "finalize-success" not in events
        assert "gate-opened" not in events
        assert not receipt_path.exists()
