"""Tests for AuroraSchemaDeployer — mocked subprocess/psql calls."""

from unittest.mock import MagicMock, patch

import pytest

from daylily_tapdb.aurora.schema_deployer import AuroraSchemaDeployer

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def aurora_kwargs():
    """Common kwargs for AuroraSchemaDeployer methods."""
    return {
        "host": "my-cluster.cluster-abc123.us-west-2.rds.amazonaws.com",
        "port": 5432,
        "user": "tapdb_admin",
        "database": "tapdb_aurora_dev",
        "region": "us-west-2",
    }


@pytest.fixture
def mock_ca_bundle(tmp_path):
    """Patch ensure_ca_bundle to return a temp path."""
    ca = tmp_path / "rds-ca-bundle.pem"
    ca.write_text("FAKE CA BUNDLE")
    with patch(
        "daylily_tapdb.aurora.schema_deployer.AuroraConnectionBuilder.ensure_ca_bundle",
        return_value=ca,
    ):
        yield ca


@pytest.fixture
def mock_iam_token():
    """Patch IAM token generation."""
    with patch(
        "daylily_tapdb.aurora.schema_deployer.AuroraConnectionBuilder.get_iam_auth_token",
        return_value="mock-iam-token-abc123",
    ) as m:
        yield m


@pytest.fixture
def mock_secret_password():
    """Patch Secrets Manager password retrieval."""
    with patch(
        "daylily_tapdb.aurora.schema_deployer.AuroraConnectionBuilder.get_secret_password",
        return_value="mock-secret-password",
    ) as m:
        yield m


# ---------------------------------------------------------------------------
# _build_psql_env
# ---------------------------------------------------------------------------


class TestBuildPsqlEnv:
    def test_explicit_signing_port_preserves_transport(
        self, aurora_kwargs, mock_iam_token, mock_ca_bundle
    ):
        aurora_kwargs["port"] = 55434
        cmd, env = AuroraSchemaDeployer._build_psql_env(
            **aurora_kwargs,
            server_port=5432,
            profile="qualification-profile",
            hostaddr="127.0.0.1",
        )
        mock_iam_token.assert_called_once_with(
            region=aurora_kwargs["region"],
            host=aurora_kwargs["host"],
            port=5432,
            user=aurora_kwargs["user"],
            profile="qualification-profile",
        )
        assert cmd[cmd.index("-p") + 1] == "55434"
        assert env["PGHOSTADDR"] == "127.0.0.1"

    @pytest.mark.parametrize("server_port", [0, 65536, True, "5432", 5432.0])
    def test_invalid_signing_port_refused_before_auth(
        self, aurora_kwargs, mock_iam_token, mock_ca_bundle, server_port
    ):
        with pytest.raises(ValueError, match="server_port must be an integer"):
            AuroraSchemaDeployer._build_psql_env(
                **aurora_kwargs, server_port=server_port
            )
        mock_iam_token.assert_not_called()

    def test_iam_auth(self, aurora_kwargs, mock_iam_token, mock_ca_bundle):
        cmd, env = AuroraSchemaDeployer._build_psql_env(
            **aurora_kwargs,
            iam_auth=True,
        )
        mock_iam_token.assert_called_once()
        assert env["PGPASSWORD"] == "mock-iam-token-abc123"
        assert env["PGSSLMODE"] == "verify-full"
        assert env["PGSSLROOTCERT"] == str(mock_ca_bundle)
        assert "-h" in cmd
        assert aurora_kwargs["host"] in cmd

    def test_secret_arn(self, aurora_kwargs, mock_secret_password, mock_ca_bundle):
        cmd, env = AuroraSchemaDeployer._build_psql_env(
            **aurora_kwargs,
            iam_auth=False,
            secret_arn="arn:aws:secretsmanager:us-west-2:123:secret:foo",
        )
        mock_secret_password.assert_called_once()
        assert env["PGPASSWORD"] == "mock-secret-password"

    def test_explicit_password(self, aurora_kwargs, mock_ca_bundle):
        cmd, env = AuroraSchemaDeployer._build_psql_env(
            **aurora_kwargs,
            iam_auth=False,
            password="explicit-pw",
        )
        assert env["PGPASSWORD"] == "explicit-pw"

    def test_no_auth_raises(self, aurora_kwargs, mock_ca_bundle):
        with pytest.raises(ValueError, match="requires iam_auth"):
            AuroraSchemaDeployer._build_psql_env(
                **aurora_kwargs,
                iam_auth=False,
            )


# ---------------------------------------------------------------------------
# run_psql
# ---------------------------------------------------------------------------


class TestRunPsql:
    def test_sql_success(self, aurora_kwargs, mock_iam_token, mock_ca_bundle):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="1\n",
                stderr="",
            )
            ok, out = AuroraSchemaDeployer.run_psql(
                **aurora_kwargs,
                sql="SELECT 1",
            )
        assert ok is True
        assert out == "1"
        # Verify -c flag was used
        call_args = mock_run.call_args[0][0]
        assert "-c" in call_args
        assert "SELECT 1" in call_args

    def test_file_success(
        self, aurora_kwargs, mock_iam_token, mock_ca_bundle, tmp_path
    ):
        schema = tmp_path / "schema.sql"
        schema.write_text("CREATE TABLE test (id INT);")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="",
                stderr="",
            )
            ok, out = AuroraSchemaDeployer.run_psql(
                **aurora_kwargs,
                file=schema,
            )
        assert ok is True
        call_args = mock_run.call_args[0][0]
        assert "-f" in call_args

    def test_failure(self, aurora_kwargs, mock_iam_token, mock_ca_bundle):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="ERROR: connection refused",
            )
            ok, out = AuroraSchemaDeployer.run_psql(
                **aurora_kwargs,
                sql="SELECT 1",
            )
        assert ok is False
        assert "connection refused" in out

    def test_psql_not_found(self, aurora_kwargs, mock_iam_token, mock_ca_bundle):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            ok, out = AuroraSchemaDeployer.run_psql(
                **aurora_kwargs,
                sql="SELECT 1",
            )
        assert ok is False
        assert "psql not found" in out


# ---------------------------------------------------------------------------
# deploy_schema
# ---------------------------------------------------------------------------


class TestDeploySchema:
    def test_deploy_forwards_signing_port(
        self, aurora_kwargs, mock_iam_token, mock_ca_bundle, tmp_path
    ):
        aurora_kwargs["port"] = 55434
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            ok, _ = AuroraSchemaDeployer.deploy_schema(
                **aurora_kwargs,
                schema_file=tmp_path / "schema.sql",
                server_port=5432,
                profile="qualification-profile",
            )
        assert ok
        assert mock_iam_token.call_args.kwargs["port"] == 5432
        assert mock_iam_token.call_args.kwargs["profile"] == "qualification-profile"
        cmd = mock_run.call_args.args[0]
        assert cmd[cmd.index("-p") + 1] == "55434"

    def test_deploy_success(
        self, aurora_kwargs, mock_iam_token, mock_ca_bundle, tmp_path
    ):
        schema = tmp_path / "tapdb_schema.sql"
        schema.write_text("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="",
                stderr="",
            )
            ok, out = AuroraSchemaDeployer.deploy_schema(
                **aurora_kwargs,
                schema_file=schema,
            )
        assert ok is True
        # Verify SSL env vars were set
        call_env = mock_run.call_args[1]["env"]
        assert call_env["PGSSLMODE"] == "verify-full"

    def test_deploy_failure(
        self, aurora_kwargs, mock_iam_token, mock_ca_bundle, tmp_path
    ):
        schema = tmp_path / "tapdb_schema.sql"
        schema.write_text("INVALID SQL;")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="ERROR: syntax error",
            )
            ok, out = AuroraSchemaDeployer.deploy_schema(
                **aurora_kwargs,
                schema_file=schema,
            )
        assert ok is False
        assert "syntax error" in out


# ---------------------------------------------------------------------------
# Integration with db.py _run_psql aurora branch
# ---------------------------------------------------------------------------


class TestDbRunPsqlAuroraBranch:
    """Verify _run_psql delegates to AuroraSchemaDeployer for aurora."""

    def test_aurora_delegation(self, mock_iam_token, mock_ca_bundle):
        """When engine_type=aurora, _run_psql should use AuroraSchemaDeployer."""
        from daylily_tapdb.cli.db import Environment, _run_psql

        aurora_cfg = {
            "engine_type": "aurora",
            "host": "aurora.cluster.us-west-2.rds.amazonaws.com",
            "port": "5432",
            "user": "tapdb_admin",
            "password": "",
            "database": "tapdb_aurora_dev",
            "region": "us-west-2",
            "iam_auth": "true",
            "ssl": "true",
        }

        with patch("daylily_tapdb.cli.db._get_db_config", return_value=aurora_cfg):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="42\n",
                    stderr="",
                )
                ok, out = _run_psql(Environment.target, sql="SELECT 42")

        assert ok is True
        assert out == "42"
        # Verify SSL was set in env
        call_env = mock_run.call_args[1]["env"]
        assert call_env["PGSSLMODE"] == "verify-full"

    def test_local_no_delegation(self):
        """When engine_type=local, _run_psql should NOT use AuroraSchemaDeployer."""
        from daylily_tapdb.cli.db import Environment, _run_psql

        local_cfg = {
            "engine_type": "local",
            "host": "localhost",
            "port": "5432",
            "user": "daylily",
            "password": "",
            "database": "tapdb_dev",
        }

        with patch("daylily_tapdb.cli.db._get_db_config", return_value=local_cfg):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="1\n",
                    stderr="",
                )
                ok, out = _run_psql(Environment.target, sql="SELECT 1")

        assert ok is True
        # Verify PGSSLMODE was NOT set (local mode)
        call_env = mock_run.call_args[1]["env"]
        assert (
            call_env.get("PGSSLMODE") is None
            or call_env.get("PGSSLMODE") != "verify-full"
        )
