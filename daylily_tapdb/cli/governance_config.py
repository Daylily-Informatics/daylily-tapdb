"""Bounded canonical-config update for explicit Meridian authorization."""

from __future__ import annotations

import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Mapping

import yaml

from daylily_tapdb.cli.context import resolve_context
from daylily_tapdb.cli.db_config import (
    _build_db_config_from_section,
    _validate_target_meta_for_context,
)
from daylily_tapdb.governance import GovernanceAuthorization


def _metadata_node(raw: str) -> yaml.MappingNode:
    # Rewriting an anchored metadata object could also change an unrelated
    # alias. Ambiguous YAML is rejected rather than normalized or expanded.
    if any(isinstance(token, (yaml.AnchorToken, yaml.AliasToken)) for token in yaml.scan(raw)):
        raise ValueError("Governance config updates require YAML without anchors or aliases")
    root = yaml.compose(raw, Loader=yaml.SafeLoader)
    if not isinstance(root, yaml.MappingNode):
        raise ValueError("TapDB config must be an object")
    metadata = [value for key, value in root.value if key.value == "meta"]
    if len(metadata) != 1 or not isinstance(metadata[0], yaml.MappingNode):
        raise ValueError("TapDB config requires one explicit meta object")
    names = [key.value for key, _value in metadata[0].value]
    if len(names) != len(set(names)) or "<<" in names:
        raise ValueError("TapDB meta keys must be explicit and unique")
    return metadata[0]


def _with_authorization(raw: str, authorization: GovernanceAuthorization) -> str:
    """Replace only the authorization span; preserve every other input byte."""
    meta = _metadata_node(raw)
    encoded = json.dumps(authorization.to_dict(), ensure_ascii=True)
    newline = "\r\n" if "\r\n" in raw else "\n"
    current = [(key, value) for key, value in meta.value if key.value == "governance_authorization"]
    if current:
        key, value = current[0]
        if isinstance(value, (yaml.MappingNode, yaml.SequenceNode)) and not value.flow_style:
            # The collection's end mark includes comments/whitespace before the
            # next key. Stop at the last value so those unrelated bytes survive.
            leaf = value
            while isinstance(leaf, (yaml.MappingNode, yaml.SequenceNode)) and not leaf.flow_style and leaf.value:
                leaf = leaf.value[-1][1] if isinstance(leaf, yaml.MappingNode) else leaf.value[-1]
            end = leaf.end_mark.index
            separator = ""
            if isinstance(leaf, yaml.ScalarNode) and leaf.style in ("|", ">"):
                if end < len(raw):
                    separator = newline + " " * leaf.end_mark.column
                elif raw.endswith("\n"):
                    separator = newline
            replacement = "governance_authorization: " + encoded + separator
            return raw[:key.start_mark.index] + replacement + raw[end:]
        return raw[:value.start_mark.index] + encoded + raw[value.end_mark.index:]
    if meta.flow_style:
        offset = meta.end_mark.index - 1
        if raw[offset] != "}":
            raise ValueError("Cannot locate the explicit meta mapping boundary")
        separator = ", " if meta.value and not raw[:offset].rstrip().endswith(",") else ""
        addition = separator + '"governance_authorization": ' + encoded
        return raw[:offset] + addition + raw[offset:]
    if not meta.value:
        raise ValueError("TapDB config requires explicit namespace metadata")
    offset = meta.end_mark.index
    indent = " " * meta.value[0][0].start_mark.column
    separator = "" if raw[:offset].endswith("\n") else newline
    addition = separator + indent + "governance_authorization: " + encoded + newline
    return raw[:offset] + addition + raw[offset:]


def _without_authorization(root: dict) -> dict:
    return {
        **root,
        "meta": {key: value for key, value in root["meta"].items() if key != "governance_authorization"},
    }


def set_governance_authorization(
    config_path: str | Path,
    value: GovernanceAuthorization | Mapping[str, object],
) -> Path:
    """Validate and atomically replace only explicit authorization metadata.

    This operation deliberately does not require unrelated admin UI mappings
    and never connects to the database. Missing or incorrect authorization and
    an invalid resulting target fail before replacement.
    """
    authorization = GovernanceAuthorization.from_value(value)
    if authorization is None:
        raise ValueError("governance_authorization must be an explicit object")
    path = Path(config_path).expanduser().resolve(strict=True)
    original = path.read_bytes()
    original_stat = path.stat()
    raw = original.decode("utf-8")
    rendered = _with_authorization(raw, authorization)
    root = yaml.safe_load(rendered)
    previous = yaml.safe_load(raw)
    if _without_authorization(root) != _without_authorization(previous):
        raise ValueError("Governance update would change unrelated config values")
    ctx = resolve_context(config_path=path, require_keys=True)
    _validate_target_meta_for_context(root, ctx, path)
    target = root.get("target")
    if not isinstance(target, dict) or not target:
        raise ValueError("TapDB config requires an explicit target object")
    _build_db_config_from_section(
        ctx=ctx, root=root, resolved_config_path=path, file_cfg=target,
        section_name="target", target_name="target",
    )
    descriptor, temporary = tempfile.mkstemp(prefix=".tapdb-governance-", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(rendered.encode("utf-8"))
            stream.flush()
            temporary_stat = os.fstat(stream.fileno())
            if (temporary_stat.st_uid, temporary_stat.st_gid) != (original_stat.st_uid, original_stat.st_gid):
                os.fchown(stream.fileno(), original_stat.st_uid, original_stat.st_gid)
            os.fchmod(stream.fileno(), stat.S_IMODE(original_stat.st_mode))
            os.fsync(stream.fileno())
        current_stat = path.stat()
        stat_fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_mtime_ns", "st_ctime_ns")
        if path.read_bytes() != original or any(
            getattr(current_stat, name) != getattr(original_stat, name)
            for name in stat_fields
        ):
            raise RuntimeError("TapDB config changed during governance authorization update")
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return path
