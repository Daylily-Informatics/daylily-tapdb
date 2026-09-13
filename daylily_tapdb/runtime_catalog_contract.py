"""Packaged, positive security authority for runtime schema binding.

Live hashes are receipt evidence, not authorization. Routine source and metadata
are compared with this distribution's SQL assets; policies retain their exact
PostgreSQL expression structure and triggers retain their complete firing shape.
Unknown historical objects are not part of this grant authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from importlib.metadata import distribution
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit


class RuntimeCatalogContractError(RuntimeError):
    """The packaged or live security contract cannot be proven canonical."""


def _asset_path(name: str) -> Path:
    package = distribution("daylily-tapdb")
    direct = json.loads(package.read_text("direct_url.json") or "{}")
    if direct.get("dir_info", {}).get("editable") is True:
        source = urlsplit(direct["url"])
        if source.scheme != "file" or source.netloc:
            raise RuntimeCatalogContractError(
                "Editable security assets require a local distribution"
            )
        root = Path(unquote(source.path)).resolve()
        if root != Path(__file__).resolve().parents[1]:
            raise RuntimeCatalogContractError(
                "Editable security assets do not match the imported distribution"
            )
        path = root / "schema" / name
    else:
        matches = [
            item
            for item in package.files or []
            if str(item).endswith("/schema/" + name) or str(item) == "schema/" + name
        ]
        if len(matches) != 1:
            raise RuntimeCatalogContractError(
                "Installed distribution must own one exact security asset: " + name
            )
        path = Path(str(package.locate_file(matches[0]))).resolve()
    if not path.is_file():
        raise RuntimeCatalogContractError("Packaged security asset is missing: " + name)
    return path


def security_assets() -> dict[str, str]:
    """Read only the imported distribution's assets, never cwd discovery."""
    return {
        name: _asset_path(name).read_text(encoding="utf-8")
        for name in ("tapdb_schema.sql", "rls.sql", "allocator_functions.sql", "runtime_identity_authorization.sql")
    }


def expression_tokens(expression: str) -> tuple[str, ...]:
    """Ignore whitespace only outside quoted values; retain every operator/parens."""
    token = re.compile(
        r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|[A-Za-z_][A-Za-z0-9_$]*|[0-9]+|::|>=|<=|<>|!=|[().=,+*/<>-]"
    )
    result = []
    offset = 0
    for match in token.finditer(expression):
        if expression[offset : match.start()].strip():
            raise RuntimeCatalogContractError("Unrecognized policy expression token")
        result.append(match.group())
        offset = match.end()
    if expression[offset:].strip():
        raise RuntimeCatalogContractError("Unrecognized policy expression token")
    return tuple(result)


def policy_expressions(schema_sql: str) -> dict[str, tuple[str, str]]:
    """Pinned PostgreSQL 16/17 deparse contract under search_path=pg_catalog.

    Parentheses, order, operators, names and quoted values are significant.
    Fresh packaged-schema tests establish correspondence to canonical rls.sql.
    """
    s = schema_sql
    scope = f"(domain_code = {s}.tapdb_current_domain_code()) AND (issuer_app_code = {s}.tapdb_current_owner_repo_name())"
    read_tenant = (
        f"((tenant_id IS NULL) OR (tenant_id = ANY ({s}.tapdb_allowed_tenant_ids())))"
    )
    write_tenant = f"((tenant_id = ANY ({s}.tapdb_allowed_tenant_ids())) OR ((tenant_id IS NULL) AND (({s}.tapdb_current_tenant_id() IS NULL) OR {s}.tapdb_allow_global_rows())))"
    result = {
        name: (f"({scope} AND {read_tenant})", f"({scope} AND {write_tenant})")
        for name in (
            "generic_template",
            "generic_instance",
            "generic_instance_lineage",
            "audit_log",
            "outbox_event",
            "inbox_message",
        )
    }
    result["tapdb_identity_prefix_config"] = (f"({scope})", f"({scope})")
    event_scope = scope.replace("(domain_code", "(event.domain_code").replace(
        "(issuer_app_code", "(event.issuer_app_code"
    )
    event_read = read_tenant.replace("tenant_id IS", "event.tenant_id IS").replace(
        "(tenant_id =", "(event.tenant_id ="
    )
    event_write = write_tenant.replace("tenant_id IS", "event.tenant_id IS").replace(
        "(tenant_id =", "(event.tenant_id ="
    )
    head = f"(EXISTS ( SELECT 1 FROM {s}.outbox_event event WHERE ((event.id = outbox_event_attempt.outbox_event_id) AND {event_scope} AND "
    result["outbox_event_attempt"] = (
        head + event_read + ")))",
        head + event_write + ")))",
    )
    message = f"(EXISTS ( SELECT 1 FROM {s}.generic_instance message WHERE (message.uid = tapdb_legacy_outbox_mapping.message_uid)))"
    event = f"(EXISTS ( SELECT 1 FROM {s}.outbox_event event WHERE ((event.id = tapdb_legacy_outbox_mapping.old_outbox_id) AND "
    result["tapdb_legacy_outbox_mapping"] = (
        f"({message} AND {event}(event.message_uid = tapdb_legacy_outbox_mapping.message_uid)))))",
        f"({message} AND {event}((event.message_uid IS NULL) OR (event.message_uid = tapdb_legacy_outbox_mapping.message_uid))))))",
    )
    return result


_FUNCTION = re.compile(
    r"^CREATE OR REPLACE FUNCTION (?P<name>\w+)\((?P<arguments>[^)]*)\)\s+RETURNS\s+(?P<before>.*?)\$\$(?P<source>.*?)\$\$(?P<after>[^;]*);",
    re.M | re.S,
)
_TRIGGER = re.compile(
    r"^CREATE TRIGGER (?P<name>\w+)\s+(?P<timing>BEFORE|AFTER)\s+(?P<events>[A-Z\s]+?) ON (?P<relation>\w+)\s+FOR EACH ROW EXECUTE FUNCTION (?P<function>\w+)\(\);",
    re.M,
)
_TYPES = {
    "TEXT": (25, "text"),
    "BIGINT": (20, "bigint"),
    "JSONB": (3802, "jsonb"),
    "BOOLEAN": (16, "boolean"),
    "UUID": (2950, "uuid"),
    "UUID[]": (2951, "uuid[]"),
    "VOID": (2278, "void"),
    "TRIGGER": (2279, "trigger"),
    "CHAR": (1042, "character"),
}


def _validate_allocator_copies(assets: dict[str, str]) -> None:
    """Require one byte-identical raw-SQL copy of the shared allocator asset."""
    canonical = assets["allocator_functions.sql"]
    if (
        not canonical.startswith("-- tapdb-managed-allocator-functions:start\n")
        or not canonical.endswith("-- tapdb-managed-allocator-functions:end\n")
        or assets["tapdb_schema.sql"].count(canonical) != 1
    ):
        raise RuntimeCatalogContractError(
            "Canonical allocator asset differs from its exact inline base-schema copy"
        )
    signatures = {
        "tapdb_get_identity_prefix": "entity_name TEXT",
        "set_generic_template_euid": "",
        "set_generic_instance_euid": "",
        "set_generic_instance_lineage_euid": "",
        "set_audit_log_euid": "",
    }
    canonical_definitions = list(_FUNCTION.finditer(canonical))
    if (
        len(canonical_definitions) != len(signatures)
        or {match["name"]: match["arguments"] for match in canonical_definitions}
        != signatures
    ):
        raise RuntimeCatalogContractError(
            "Canonical allocator routine signatures differ from the supported API"
        )
    for name in ("tapdb_schema.sql", "rls.sql"):
        definitions = [
            match.group()
            for match in _FUNCTION.finditer(assets[name])
            if match["name"] in signatures
        ]
        expected = (
            [match.group() for match in canonical_definitions]
            if name == "tapdb_schema.sql"
            else []
        )
        if definitions != expected:
            raise RuntimeCatalogContractError(
                "Duplicate allocator definitions differ from canonical packaged authority"
            )


def canonical_security_contract(schema_name: str, schema_sql: str) -> dict[str, Any]:
    assets = security_assets()
    _validate_allocator_copies(assets)
    functions: dict[tuple[str, str], dict[str, Any]] = {}
    triggers: dict[tuple[str, str], dict[str, Any]] = {}
    pinned = set()
    for block in re.findall(
        r"DO \$tapdb_pin_[a-z_]+\$(.*?)END;\s*\$tapdb_pin_[a-z_]+\$;",
        assets["rls.sql"] + "\n" + assets["allocator_functions.sql"] + "\n" + assets["runtime_identity_authorization.sql"],
        re.S,
    ):
        names = re.search(
            r"FOREACH function_name IN ARRAY ARRAY\[(.*?)\] LOOP", block, re.S
        )
        if names is None:
            raise RuntimeCatalogContractError(
                "Unsupported packaged function search-path contract"
            )
        pinned.update(re.findall(r"'([a-z_]+)'", names[1]))
    # Allocators are checked byte-for-byte above, then parsed once from the
    # standalone base copy. Do not let last-wins parsing conceal copy drift.
    for source in (assets["tapdb_schema.sql"], assets["rls.sql"], assets["runtime_identity_authorization.sql"]):
        matches = list(_FUNCTION.finditer(source))
        if len(matches) != len(
            re.findall(r"^CREATE OR REPLACE FUNCTION ", source, re.M)
        ):
            raise RuntimeCatalogContractError("Unsupported packaged routine definition")
        for match in matches:
            values = match.groupdict()
            metadata = values["before"] + values["after"]
            language = re.search(r"\bLANGUAGE (sql|plpgsql)\b", metadata, re.I)
            if language is None:
                raise RuntimeCatalogContractError(
                    "Unsupported packaged routine language"
                )
            arguments = []
            argument_types = []
            argument_names = []
            for argument in values["arguments"].split(","):
                if not argument.strip():
                    continue
                name, type_name = argument.strip().split()
                oid, rendered = _TYPES[type_name.upper()]
                arguments.append(f"{name} {rendered}")
                argument_types.append(str(oid))
                argument_names.append(name)
            name = values["name"]
            argument_sql = ", ".join(arguments)
            functions[(name, argument_sql)] = {
                "name": name,
                "arguments": argument_sql,
                "argument_types": " ".join(argument_types),
                "argument_names": argument_names or None,
                "argument_modes": None,
                "all_argument_types": None,
                "default_count": 0,
                "default_expression": None,
                "variadic_type": 0,
                "return_type": _TYPES[values["before"].split()[0].upper()][0],
                "language": language[1].lower(),
                "kind": "f",
                "source": values["source"],
                "binary": None,
                "sql_body": None,
                "transform_types": None,
                "security_definer": "SECURITY DEFINER" in metadata,
                "strict": bool(re.search(r"\bSTRICT\b", metadata)),
                "volatility": "i"
                if "IMMUTABLE" in metadata
                else "s"
                if "STABLE" in metadata
                else "v",
                "parallel": "u",
                "leakproof": False,
                "returns_set": False,
                "support": 0,
                "cost": 100.0,
                "rows": 0.0,
                "settings": [f"search_path={schema_sql}, pg_catalog, pg_temp"]
                if name in pinned
                else None,
            }
        trigger_matches = list(_TRIGGER.finditer(source))
        if len(trigger_matches) != len(re.findall(r"^CREATE TRIGGER ", source, re.M)):
            raise RuntimeCatalogContractError("Unsupported packaged trigger definition")
        for match in trigger_matches:
            values = match.groupdict()
            event_bits = sum(
                {"INSERT": 4, "DELETE": 8, "UPDATE": 16}[event.strip()]
                for event in values.pop("events").split("OR")
            )
            timing = values.pop("timing")
            triggers[(values["relation"], values["name"])] = {
                **values,
                "function_schema": schema_name,
                "function_arguments": "",
                "enabled": "O",
                "type": 1 + (2 if timing == "BEFORE" else 0) + event_bits,
                "when": None,
                "columns": "",
                "arguments_hex": "",
                "constraint_oid": 0,
                "deferrable": False,
                "initially_deferred": False,
                "old_table": None,
                "new_table": None,
            }
    return {
        "asset_sha256": {
            name: hashlib.sha256(source.encode()).hexdigest()
            for name, source in assets.items()
        },
        "functions": functions,
        "triggers": triggers,
        "policies": policy_expressions(schema_sql),
    }


def validate_managed_security(
    contract: dict[str, Any],
    *,
    owner: str,
    owner_oid: int,
    policies: list[dict[str, Any]],
    functions: list[dict[str, Any]],
    triggers: list[dict[str, Any]],
    managed_tables: set[str],
) -> list[dict[str, Any]]:
    """Return only positively identified canonical routines eligible for grants."""
    if any({"name", "arguments"} - set(row) for row in functions) or any(
        {"relation", "name"} - set(row) for row in policies + triggers
    ):
        raise RuntimeCatalogContractError(
            "Live security catalog identity metadata is incomplete"
        )
    expected_policies = {
        (table, "tapdb_operator_access")
        for table in managed_tables - {"_tapdb_migrations"}
    }
    expected_policies.update(
        (table, table + "_scope_isolation") for table in contract["policies"]
    )
    actual_policies = [row for row in policies if row["relation"] in managed_tables]
    if {(row["relation"], row["name"]) for row in actual_policies} != expected_policies:
        raise RuntimeCatalogContractError(
            "Unexpected or missing runtime/operator RLS policies"
        )
    for row in actual_policies:
        operator = row["name"] == "tapdb_operator_access"
        expected = (
            ("true", "true") if operator else contract["policies"][row["relation"]]
        )
        if (
            row["roles"] != (f"{{{owner_oid}}}" if operator else "{0}")
            or row["permissive"] is not True
            or row["command"] != "*"
        ):
            raise RuntimeCatalogContractError(
                "Runtime/operator policy authority differs from canonical metadata"
            )
        if any(
            not isinstance(row.get(field), str)
            or expression_tokens(row[field]) != expression_tokens(value)
            for field, value in zip(("using", "with_check"), expected, strict=True)
        ):
            raise RuntimeCatalogContractError(
                "Runtime/operator policy authority differs from canonical expressions"
            )
    actual_functions = {(row["name"], row["arguments"]): row for row in functions}
    routine_grants = []
    for key, expected in contract["functions"].items():
        routine_row = actual_functions.get(key)
        if routine_row is None:
            raise RuntimeCatalogContractError(
                f"Current canonical RLS migration routine is missing: {key}"
            )
        if key[0] == "tapdb_resolve_user_authorization" and routine_row.get("public_execute") is not False:
            raise RuntimeCatalogContractError("Canonical identity projection must deny PUBLIC EXECUTE")
        if (
            set(expected) - set(routine_row)
            or routine_row["owner"] != owner
            or any(routine_row.get(field) != value for field, value in expected.items())
        ):
            raise RuntimeCatalogContractError(
                f"Canonical routine body or security metadata differs: {key}"
            )
        routine_grants.append(routine_row)
    actual_triggers = {
        (row["relation"], row["name"]): row
        for row in triggers
        if row["relation"] in managed_tables
    }
    if set(actual_triggers) != set(contract["triggers"]):
        raise RuntimeCatalogContractError(
            "Managed trigger inventory differs from canonical security contract"
        )
    for key, expected in contract["triggers"].items():
        if set(expected) - set(actual_triggers[key]) or any(
            actual_triggers[key].get(field) != value
            for field, value in expected.items()
        ):
            raise RuntimeCatalogContractError(
                f"Canonical immutable runtime-scope trigger or managed trigger differs: {key}"
            )
    return routine_grants
