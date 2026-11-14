"""Receipt-driven composition of native migration commands; no database engine."""

from __future__ import annotations

import fcntl
import json
import os
import re
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any, Mapping

from daylily_tapdb.backup.receipts import Actor, read_receipts, write_receipt
from daylily_tapdb.identity_inventory import seal_receipt, validate_receipt
from daylily_tapdb.security_context import Attribution

VERSION = "tapdb-migration-run/v1"
OPERATION = "migration_run"
ORDER = (
    "quiesce", "source_capture", "backup", "archive", "create_target",
    "schema_apply", "schema_migrate", "allocator_advance", "templates",
    "runtime_bind", "app_bootstrap", "activate",
)
NATIVE = {
    "source_capture": (("db", "identity", "inventory"),),
    "backup": (("backup", "create"),),
    "archive": (("db", "archive", "fence"), ("db", "archive", "retain")),
    "create_target": (("db", "create"),),
    "schema_apply": (("db", "schema", "apply"),),
    "schema_migrate": (("db", "schema", "migrate"),),
    "allocator_advance": (("db", "sequences", "advance"),),
    "templates": (("templates", "import"), ("db", "data", "seed")),
    "runtime_bind": (("db", "runtime-principal", "bind"),),
}


def _path(value: Any, *, directory: bool = False) -> Path:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise ValueError("Operator paths must be explicit and absolute")
    path = Path(value)
    if not (path.is_dir() if directory else path.is_file()):
        raise ValueError(f"Required operator path is missing: {path}")
    return path


def prepare_run(definition: Mapping[str, Any], *, target: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve preparation requirements before any service or database command."""
    required = {
        "run_id", "mode", "stop_after", "execution_backend", "operator_argv", "working_directory", "log_directory",
        "aws_profile", "aws_config_file", "attribution", "stages",
    }
    if set(definition) != required:
        raise ValueError("Migration definition requires exactly: " + ", ".join(sorted(required)))
    run_id = definition["run_id"]
    if not isinstance(run_id, str) or not re.fullmatch(r"[a-z0-9_]{1,80}", run_id):
        raise ValueError("run_id must be 1-80 lowercase letters, digits or underscores")
    if definition["mode"] not in {"fresh", "preserved"}:
        raise ValueError("mode must explicitly be fresh or preserved")
    if definition["stop_after"] not in {"runtime_bind", "app_bootstrap", "activate"}:
        raise ValueError("stop_after must explicitly select runtime_bind, app_bootstrap or activate")
    for key in ("execution_backend", "aws_profile"):
        if not isinstance(definition[key], str) or not definition[key].strip():
            raise ValueError(f"{key} is required")
    _path(definition["working_directory"], directory=True)
    logs = _path(definition["log_directory"], directory=True)
    if not os.access(logs, os.W_OK):
        raise ValueError(f"Native log directory is not writable: {logs}")
    _path(definition["aws_config_file"])
    operator_argv = definition["operator_argv"]
    if not isinstance(operator_argv, list) or not operator_argv or any(not isinstance(arg, str) or not arg for arg in operator_argv):
        raise ValueError("operator_argv must explicitly select the native TapDB executable or container invocation")
    _path(operator_argv[0])
    attribution = Attribution(**json.loads(_path(definition["attribution"]).read_text()))
    stages = definition["stages"]
    if not isinstance(stages, list) or not stages:
        raise ValueError("stages must be an explicit ordered list")
    kinds = []
    for stage in stages:
        if not isinstance(stage, dict) or set(stage) != {"kind", "commands"}:
            raise ValueError("Each stage requires kind and commands")
        kind = stage["kind"]
        if kind not in ORDER or kind in kinds:
            raise ValueError("Unknown or repeated migration stage")
        kinds.append(kind)
        commands = stage["commands"]
        if not isinstance(commands, list) or not commands:
            raise ValueError("Each stage needs explicit commands")
        for command in commands:
            native = kind in NATIVE
            fields = {"argv", "config"} if native else {"argv"}
            if not isinstance(command, dict) or set(command) != fields:
                raise ValueError(f"{kind} command requires exactly {sorted(fields)}")
            argv = command["argv"]
            if not isinstance(argv, list) or not argv or any(not isinstance(arg, str) or not arg for arg in argv):
                raise ValueError("argv must be a nonempty string array; no shell syntax")
            if native:
                from daylily_tapdb.cli.db_config import get_db_config

                get_db_config(config_path=_path(command["config"]))
                if not any(tuple(argv[:len(prefix)]) == prefix for prefix in NATIVE[kind]):
                    raise ValueError(f"{kind} must invoke its native TapDB command")
                if "--config" in argv or "--attribution" in argv:
                    raise ValueError("Native config/attribution come from the explicit prepared fields")
                if tuple(argv[:3]) == ("db", "archive", "fence") and "--apply" not in argv:
                    raise ValueError("The archive fence command must explicitly select --apply")
            else:
                _path(argv[0])
                if kind == "app_bootstrap" and not any(
                    argv[i:i + 2] == ["--attribution", definition["attribution"]]
                    for i in range(len(argv) - 1)
                ):
                    raise ValueError("Application bootstrap must explicitly forward --attribution")
        if kind in {"runtime_bind", "schema_migrate", "allocator_advance"}:
            final = commands[-1]["argv"]
            if "--apply" not in final or "--dry-run" in final:
                raise ValueError(f"{kind} must end with native --apply")
    if kinds != sorted(kinds, key=ORDER.index):
        raise ValueError("Stages must follow the documented migration lifecycle")
    if kinds[-1] != definition["stop_after"]:
        raise ValueError("The last stage must equal the explicitly selected stop_after boundary")
    required_kinds = {"quiesce", "backup", "runtime_bind"}
    if ORDER.index(definition["stop_after"]) >= ORDER.index("app_bootstrap"):
        required_kinds.add("app_bootstrap")
    if definition["stop_after"] == "activate":
        required_kinds.add("activate")
    if definition["mode"] == "fresh":
        required_kinds |= {"archive", "create_target", "schema_apply", "allocator_advance", "templates"}
    else:
        required_kinds.add("schema_migrate")
        if {"archive", "create_target", "allocator_advance"} & set(kinds):
            raise ValueError("Preserved migration uses the existing database and native migration allocator controls")
    if not required_kinds <= set(kinds):
        raise ValueError(f"Missing lifecycle stages: {sorted(required_kinds - set(kinds))}")
    return seal_receipt({
        "schema_version": VERSION, "tapdb_version": version("daylily-tapdb"),
        "target": dict(target), "definition": dict(definition),
        "actor": attribution.actor_subject,
    })


def _history(directory: Path, plan: Mapping[str, Any]) -> dict[int, dict[str, Any]]:
    states: dict[int, dict[str, Any]] = {}
    for receipt in read_receipts(directory):
        if receipt.operation != OPERATION or receipt.detail.get("run_id") != plan["definition"]["run_id"]:
            continue
        if receipt.detail.get("plan_sha256") != plan["sha256"]:
            raise ValueError("run_id already belongs to a different prepared plan")
        states[int(receipt.detail["command_index"])] = receipt.detail
    return states


def run_migration(plan: Mapping[str, Any], *, target: Mapping[str, Any], receipts_dir: Path) -> dict[str, Any]:
    """Resume at the first unfinished command, trusting each native exit result.

    A process interrupted after launch has an unknown outcome. It requires an
    owning-command reconciliation receipt; it is never automatically repeated.
    Successful allocator commands are never replayed after later-stage failure.
    """
    validate_receipt(plan, VERSION)
    _path(str(receipts_dir), directory=True)
    if plan["target"] != dict(target) or plan["tapdb_version"] != version("daylily-tapdb"):
        raise ValueError("Migration plan requires its exact configured target and TapDB version")
    definition = plan["definition"]
    actor = Actor(surface="cli", username=plan["actor"])
    lock_path = receipts_dir / ("migration_" + definition["run_id"] + ".lock")
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        states = _history(receipts_dir, plan)
        index = 0
        for stage in definition["stages"]:
            for command in stage["commands"]:
                state = states.get(index, {})
                if state.get("phase") == "succeeded":
                    index += 1
                    continue
                if state.get("phase") in {"running", "unknown"}:
                    raise ValueError(f"Command {index} has an unknown outcome; use migration-run reconcile with its native result")
                attempt = int(state.get("attempt", 0)) + 1
                stem = f"{definition['run_id']}_{index:03d}_{attempt:03d}"
                stdout = Path(definition["log_directory"]) / (stem + ".stdout")
                stderr = Path(definition["log_directory"]) / (stem + ".stderr")
                argv = list(command["argv"])
                if stage["kind"] in NATIVE:
                    argv = [*definition["operator_argv"], "--config", command["config"],
                            "--attribution", definition["attribution"], *argv]
                detail = {
                    "run_id": definition["run_id"], "plan_sha256": plan["sha256"],
                    "target": plan["target"], "command_index": index, "stage": stage["kind"],
                    "attempt": attempt, "stdout": str(stdout), "stderr": str(stderr),
                }
                env = dict(os.environ, AWS_PROFILE=definition["aws_profile"],
                           AWS_CONFIG_FILE=definition["aws_config_file"])
                print(f"Migration run: {stage['kind']} command {index}, attempt {attempt}", file=sys.stderr, flush=True)
                with stdout.open("xb") as out, stderr.open("xb") as err:
                    write_receipt(receipts_dir, operation=OPERATION, status="intent", actor=actor,
                                  detail={**detail, "phase": "running"})
                    try:
                        process = subprocess.Popen(argv, cwd=definition["working_directory"], env=env,
                                                   stdin=None, stdout=out, stderr=subprocess.PIPE)
                    except OSError as exc:
                        write_receipt(receipts_dir, operation=OPERATION, status="failed", actor=actor,
                                      detail={**detail, "phase": "failed", "launch_error": type(exc).__name__})
                        raise
                    # Relay progress emitted by the native command; no polling or probes.
                    for line in iter(process.stderr.readline, b""):
                        err.write(line)
                        err.flush()
                        sys.stderr.write(line.decode("utf-8", errors="replace"))
                        sys.stderr.flush()
                    returncode = process.wait()
                phase = "succeeded" if returncode == 0 else "failed"
                # A stable result path lets later stages consume a successful
                # native result even if earlier attempts failed. Keep all logs.
                if returncode == 0:
                    os.link(stdout, Path(definition["log_directory"]) / f"{definition['run_id']}_{index:03d}.stdout")
                write_receipt(receipts_dir, operation=OPERATION, status=phase, actor=actor,
                              detail={**detail, "phase": phase, "returncode": returncode})
                if returncode:
                    return {**detail, "phase": phase, "returncode": returncode}
                index += 1
        return {"run_id": definition["run_id"], "phase": "succeeded", "completed_commands": index,
                "stopped_after": definition["stop_after"]}


def reconcile_run(plan: Mapping[str, Any], *, receipts_dir: Path, result_path: Path) -> dict[str, Any]:
    """Record an explicit owning-system reconciliation; never replay a write.

    The supplied result identifies the command and native evidence URI. It must
    state succeeded or retryable after the owning command has reconciled any
    allocator/fence ambiguity. This result does not itself reconcile a database.
    """
    validate_receipt(plan, VERSION)
    _path(str(receipts_dir), directory=True)
    result = json.loads(_path(str(result_path)).read_text())
    if set(result) != {"plan_sha256", "command_index", "disposition", "native_receipt"}:
        raise ValueError("Reconciliation requires plan_sha256, command_index, disposition, native_receipt")
    if result["plan_sha256"] != plan["sha256"] or result["disposition"] not in {"succeeded", "retryable"}:
        raise ValueError("Reconciliation must explicitly identify this plan and its owning outcome")
    native_result = json.loads(_path(result["native_receipt"]).read_text())
    lock_path = receipts_dir / ("migration_" + plan["definition"]["run_id"] + ".lock")
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = _history(receipts_dir, plan).get(result["command_index"])
        if not state or state["phase"] == "succeeded":
            raise ValueError("Only an unfinished command can be reconciled")
        if result["disposition"] == "succeeded":
            kind = state["stage"]
            if kind == "allocator_advance":
                validate_receipt(native_result, "tapdb-sequence-apply/v1")
                succeeded = native_result.get("phase") == "committed"
            elif kind == "schema_migrate":
                succeeded = native_result.get("recovery_completion", {}).get("status") == "committed"
            elif kind == "runtime_bind":
                succeeded = native_result.get("operation") == "bind" and native_result.get("status") == "applied"
            elif kind == "archive":
                succeeded = native_result.get("phase") in {"acquired", "archived_retained"}
            elif kind == "backup":
                succeeded = native_result.get("dry_run") is False and bool(native_result.get("receipt_id"))
            else:
                succeeded = native_result.get("returncode") == 0
            if not succeeded:
                raise ValueError("The supplied native result does not record successful completion of this stage")
        detail = {**state, "phase": "succeeded" if result["disposition"] == "succeeded" else "failed",
                  "reconciliation": str(result_path), "native_receipt": result["native_receipt"]}
        write_receipt(receipts_dir, operation=OPERATION, status="succeeded",
                      actor=Actor(surface="cli", username=plan["actor"]), detail=detail)
    return detail
