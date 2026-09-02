"""Behavior coverage for legacy Aurora CLI control flow."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer
import yaml

import daylily_tapdb.aurora.stack_manager as stack_manager_mod
import daylily_tapdb.cli.aurora as aurora


class _Manager:
    create_result = {"outputs": {}}
    status_result = {"status": "CREATE_IN_PROGRESS", "outputs": {}}
    stacks_result: dict[str, object] = {}
    error_at: str | None = None

    def __init__(self, *, region: str) -> None:
        if self.error_at == "init":
            raise RuntimeError("manager init failed")
        self.region = region

    def initiate_create_stack(self, config):
        if self.error_at == "initiate":
            raise RuntimeError("initiate failed")
        return {"stack_name": f"tapdb-{config.cluster_identifier}"}

    def create_stack(self, config, callback=None):
        if callback:
            callback("CREATE_IN_PROGRESS", 1.6)
        if self.error_at == "create":
            raise RuntimeError("create failed")
        return self.create_result

    def delete_stack(self, stack_name: str, *, retain_networking: bool):
        if self.error_at == "delete":
            raise RuntimeError("delete failed")
        return {
            "status": "DELETE_COMPLETE",
            "stack": stack_name,
            "retain": retain_networking,
        }

    def get_stack_status(self, stack_name: str):
        if self.error_at == "status":
            raise RuntimeError("status failed")
        return self.status_result

    def detect_existing_resources(self, *, region: str):
        if self.error_at == "list":
            raise RuntimeError("list failed")
        return self.stacks_result


@pytest.fixture(autouse=True)
def _boundaries(monkeypatch: pytest.MonkeyPatch):
    _Manager.create_result = {"outputs": {}}
    _Manager.status_result = {"status": "CREATE_IN_PROGRESS", "outputs": {}}
    _Manager.stacks_result = {}
    _Manager.error_at = None
    monkeypatch.setattr(stack_manager_mod, "AuroraStackManager", _Manager)
    monkeypatch.setattr(aurora, "_ensure_boto3", lambda: object())
    monkeypatch.setattr(
        aurora,
        "get_db_config",
        lambda: {"cluster_identifier": "unit-cluster", "database": "unit_db"},
    )


def _write_config(path: Path, root: object) -> Path:
    path.write_text(yaml.safe_dump(root, sort_keys=False), encoding="utf-8")
    return path


def _valid_root() -> dict[str, object]:
    return {
        "meta": {"config_version": 4},
        "target": {
            "database": "unit_db",
            "schema_name": "unit_schema",
            "user": "unit_user",
            "iam_auth": "true",
            "ui_port": "8911",
        },
    }


def test_boto3_import_error_and_target_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.undo()
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(typer.Exit) as exc:
        aurora._ensure_boto3()
    assert exc.value.exit_code == 1

    monkeypatch.setattr(aurora, "get_db_config", lambda: {"database": " fallback "})
    assert aurora._target_cluster_identifier() == "fallback"
    monkeypatch.setattr(aurora, "get_db_config", lambda: {})
    with pytest.raises(RuntimeError, match="cluster_identifier"):
        aurora._target_cluster_identifier()
    assert aurora._stack_name_for_target("abc") == "tapdb-abc"


def test_public_ip_detection_retries_and_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = iter(
        [
            OSError("offline"),
            SimpleNamespace(
                __enter__=lambda self: self,
                __exit__=lambda *args: False,
                read=lambda: b"203.0.113.9\n",
            ),
        ]
    )

    class _Response:
        def __init__(self, value):
            self.value = value

        def __enter__(self):
            if isinstance(self.value, Exception):
                raise self.value
            return self.value

        def __exit__(self, *_args):
            return False

    def _open(*_args, **_kwargs):
        value = next(responses)
        if isinstance(value, Exception):
            raise value
        return _Response(value)

    monkeypatch.setattr(aurora.urllib.request, "urlopen", _open)
    assert aurora._detect_caller_public_ip() == "203.0.113.9"

    monkeypatch.setattr(
        aurora.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(
            SimpleNamespace(read=lambda: b"2001:db8::1")
        ),
    )
    with pytest.raises(RuntimeError, match="Unable to resolve"):
        aurora._detect_caller_public_ip()


@pytest.mark.parametrize(
    "root, message",
    [
        ([], "invalid TapDB config"),
        ({"meta": {}}, "explicit target config"),
        ({"meta": {}, "target": {}}, "target.database"),
        ({"target": _valid_root()["target"]}, "meta mapping"),
    ],
)
def test_update_config_rejects_invalid_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    root: object,
    message: str,
) -> None:
    path = _write_config(tmp_path / "tapdb.yaml", root)
    monkeypatch.setattr("daylily_tapdb.cli.db_config.get_config_paths", lambda: (path,))
    with pytest.raises(RuntimeError, match=message):
        aurora._update_config_file("db.example", "5432", "us-west-2")


def test_update_config_requires_file_and_preserves_optional_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "missing" / "tapdb.yaml"
    monkeypatch.setattr("daylily_tapdb.cli.db_config.get_config_paths", lambda: (path,))
    with pytest.raises(RuntimeError, match="explicit target config"):
        aurora._update_config_file("db.example", "5432", "us-west-2")

    root = _valid_root()
    root["target"].update(  # type: ignore[union-attr]
        {"cluster_identifier": "old", "secret_arn": "old-secret"}
    )
    _write_config(path, root)
    aurora._update_config_file("db.example", "6432", "us-east-1")
    actual = yaml.safe_load(path.read_text(encoding="utf-8"))["target"]
    assert actual["cluster_identifier"] == "old"
    assert actual["secret_arn"] == "old-secret"
    assert actual["iam_auth"] == "true"


def test_create_input_errors_and_background_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(aurora, "get_db_config", lambda: {})
    with pytest.raises(typer.Exit) as exc:
        aurora.aurora_create(
            region="x",
            instance_class="c",
            engine_version="1",
            vpc_id="",
            cidr=None,
            cost_center="c",
            project=None,
            publicly_accessible=False,
            no_iam_auth=False,
            no_deletion_protection=False,
            background=False,
        )
    assert exc.value.exit_code == 1

    monkeypatch.setattr(aurora, "get_db_config", lambda: {"database": "db"})
    monkeypatch.setattr(
        aurora,
        "_resolve_ingress_cidr",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("cidr failed")),
    )
    with pytest.raises(typer.Exit) as exc:
        aurora.aurora_create(
            region="x",
            instance_class="c",
            engine_version="1",
            vpc_id="",
            cidr=None,
            cost_center="c",
            project=None,
            publicly_accessible=True,
            no_iam_auth=False,
            no_deletion_protection=False,
            background=False,
        )
    assert exc.value.exit_code == 1

    monkeypatch.setattr(aurora, "_resolve_ingress_cidr", lambda *_args: "0.0.0.0/0")
    _Manager.error_at = "initiate"
    with pytest.raises(typer.Exit) as exc:
        aurora.aurora_create(
            region="x",
            instance_class="c",
            engine_version="1",
            vpc_id="",
            cidr=None,
            cost_center="c",
            project="p",
            publicly_accessible=True,
            no_iam_auth=True,
            no_deletion_protection=True,
            background=True,
        )
    assert exc.value.exit_code == 1


def test_create_endpoint_and_secret_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    updates: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        aurora,
        "_update_config_file",
        lambda *args, **kwargs: updates.append((*args, kwargs)),
    )
    _Manager.create_result = {
        "outputs": {
            "ClusterEndpoint": "db.example",
            "ClusterPort": "6432",
            "SecretArn": "secret",
        }
    }
    aurora.aurora_create(
        region="us-west-2",
        instance_class="c",
        engine_version="1",
        vpc_id="v",
        cidr="10.0.0.0/8",
        cost_center="c",
        project=None,
        publicly_accessible=False,
        no_iam_auth=False,
        no_deletion_protection=False,
        background=False,
    )
    assert updates and updates[0][0:3] == ("db.example", "6432", "us-west-2")

    _Manager.create_result = {"outputs": {}}
    aurora.aurora_create(
        region="x",
        instance_class="c",
        engine_version="1",
        vpc_id="",
        cidr="10/8",
        cost_center="c",
        project="p",
        publicly_accessible=False,
        no_iam_auth=False,
        no_deletion_protection=False,
        background=False,
    )
    assert len(updates) == 1


def test_delete_confirmation_rds_and_manager_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import boto3
    import rich.prompt

    monkeypatch.setattr(rich.prompt.Confirm, "ask", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Exit) as exc:
        aurora.aurora_delete(region="x", retain_networking=True, force=False)
    assert exc.value.exit_code == 0

    monkeypatch.setattr(rich.prompt.Confirm, "ask", lambda *_args, **_kwargs: True)
    rds = SimpleNamespace(
        modify_db_cluster=lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("protected")
        )
    )
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kwargs: rds)
    aurora.aurora_delete(region="x", retain_networking=False, force=False)

    monkeypatch.setattr(
        boto3,
        "client",
        lambda *_args, **_kwargs: SimpleNamespace(
            modify_db_cluster=lambda **_kwargs: None
        ),
    )
    _Manager.error_at = "delete"
    with pytest.raises(typer.Exit) as exc:
        aurora.aurora_delete(region="x", retain_networking=True, force=True)
    assert exc.value.exit_code == 1


@pytest.mark.parametrize(
    "status",
    ["CREATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE", "CREATE_FAILED"],
)
def test_status_human_colors_and_outputs(status: str) -> None:
    _Manager.status_result = {"status": status, "outputs": {"ClusterEndpoint": "db"}}
    aurora.aurora_status(region="x", as_json=False)


def test_status_and_connect_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    _Manager.error_at = "status"
    with pytest.raises(typer.Exit):
        aurora.aurora_status(region="x", as_json=False)
    with pytest.raises(typer.Exit):
        aurora.aurora_connect(region="x", user="u", database="db", export=False)

    _Manager.error_at = None
    _Manager.status_result = {"status": "CREATE_COMPLETE", "outputs": {}}
    with pytest.raises(typer.Exit):
        aurora.aurora_connect(region="x", user="u", database="db", export=False)

    monkeypatch.setattr(
        aurora, "get_db_config", lambda: {"cluster_identifier": "c", "database": ""}
    )
    with pytest.raises(typer.Exit):
        aurora.aurora_connect(region="x", user="u", database=None, export=False)


def test_connect_human_and_list_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    _Manager.status_result = {
        "status": "CREATE_COMPLETE",
        "outputs": {"ClusterEndpoint": "db.example", "ClusterPort": "6432"},
    }
    aurora.aurora_connect(region="x", user="u", database="db", export=False)

    rendered: list[object] = []
    monkeypatch.setattr(aurora, "print_renderable", rendered.append)
    _Manager.stacks_result = {
        "ok": {
            "status": "CREATE_COMPLETE",
            "outputs": {"ClusterEndpoint": "db"},
            "tags": {"lsmc-cost-center": "c"},
        },
        "roll": {"status": "UPDATE_ROLLBACK_COMPLETE"},
        "bad": {"status": "CREATE_FAILED"},
    }
    aurora.aurora_list(region="x", as_json=False)
    assert len(rendered) == 1

    _Manager.stacks_result = {}
    aurora.aurora_list(region="x", as_json=False)
    _Manager.error_at = "list"
    with pytest.raises(typer.Exit):
        aurora.aurora_list(region="x", as_json=False)
