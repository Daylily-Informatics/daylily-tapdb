"""Aurora (AWS RDS) CLI commands for TAPDB.

Provides ``tapdb aurora`` subcommand group for managing Aurora PostgreSQL
clusters via CloudFormation.
"""

from __future__ import annotations

import ipaddress
import json
import os
import stat
import urllib.request
from typing import Optional

import typer
from cli_core_yo import ccyo_out
from rich.console import Console
from rich.table import Table

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.cli.output import print_renderable

console = Console()

aurora_app = typer.Typer(help="Aurora PostgreSQL cluster management commands")
_DEFAULT_PRIVATE_INGRESS_CIDR = "10.0.0.0/8"
_PUBLIC_IP_RESOLUTION_URLS = (
    "https://checkip.amazonaws.com",
    "https://api.ipify.org",
)


def _ensure_boto3():
    """Import boto3, raising a clear CLI error if missing."""
    try:
        import boto3  # noqa: F401

        return boto3
    except ImportError:
        ccyo_out.error(
            "boto3 is required for Aurora commands.\n"
            "  Install with: pip install 'daylily-tapdb'"
        )
        raise typer.Exit(1)


def _target_cluster_identifier() -> str:
    cfg = get_db_config()
    cluster_identifier = str(
        cfg.get("cluster_identifier") or cfg.get("database") or ""
    ).strip()
    if not cluster_identifier:
        raise RuntimeError(
            "TapDB target.cluster_identifier or target.database is required "
            "for Aurora commands."
        )
    return cluster_identifier


def _stack_name_for_target(cluster_identifier: str) -> str:
    """Derive CloudFormation stack name from the explicit target config."""
    return f"tapdb-{cluster_identifier}"


def _detect_caller_public_ip() -> str:
    """Return the caller's current public IPv4 address."""
    for url in _PUBLIC_IP_RESOLUTION_URLS:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:  # nosec B310
                candidate = response.read().decode("utf-8").strip()
            parsed = ipaddress.ip_address(candidate)
            if parsed.version != 4:
                raise RuntimeError(
                    "Aurora public ingress auto-resolution requires an IPv4 address."
                )
            return str(parsed)
        except Exception:
            continue
    raise RuntimeError(
        "Unable to resolve the current public IPv4 address for Aurora ingress. "
        "Pass --cidr explicitly."
    )


def _resolve_ingress_cidr(
    cidr: str | None,
    publicly_accessible: bool,
    *,
    public_ip_resolver=None,
) -> str:
    """Return the effective ingress CIDR for the Aurora security group."""
    if cidr:
        return cidr
    if publicly_accessible:
        resolver = public_ip_resolver or _detect_caller_public_ip
        return f"{resolver()}/32"
    return _DEFAULT_PRIVATE_INGRESS_CIDR


def _update_config_file(
    endpoint: str,
    port: str,
    region: str,
    cluster_identifier: str | None = None,
    secret_arn: str | None = None,
) -> None:
    """Update TAPDB config file with Aurora endpoint info.

    Uses the active explicit TapDB config path / namespace context.
    """
    from daylily_tapdb.cli.db_config import get_config_paths

    config_path = get_config_paths()[0]
    config_path.parent.mkdir(parents=True, exist_ok=True)

    existing: dict = {}
    if config_path.exists():
        raw = config_path.read_text(encoding="utf-8")
        try:
            import yaml  # type: ignore

            existing = yaml.safe_load(raw) or {}
        except ModuleNotFoundError:
            existing = json.loads(raw) if raw.strip() else {}

    if not isinstance(existing, dict):
        raise RuntimeError(f"invalid TapDB config: {config_path}")
    target = existing.get("target")
    if not isinstance(target, dict):
        raise RuntimeError(f"TapDB explicit target config is required: {config_path}")
    meta = existing.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    meta["config_version"] = 4
    existing["meta"] = meta

    existing["target"] = {
        **target,
        "engine_type": "aurora",
        "host": endpoint,
        "port": port,
        "database": str(target.get("database") or "").strip(),
        "schema_name": str(target.get("schema_name") or "").strip(),
        "user": str(target.get("user") or "tapdb_admin"),
        "region": region,
        "cluster_identifier": cluster_identifier
        or str(target.get("cluster_identifier") or target.get("database") or ""),
        "iam_auth": "false" if secret_arn else str(target.get("iam_auth") or "true"),
        "secret_arn": secret_arn or str(target.get("secret_arn") or ""),
        "ssl": "true",
        "ui_port": str(target.get("ui_port") or "8911"),
    }

    try:
        import yaml  # type: ignore

        config_path.write_text(
            yaml.dump(existing, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )
    except ModuleNotFoundError:
        config_path.write_text(json.dumps(existing, indent=2) + "\n", encoding="utf-8")

    os.chmod(config_path, stat.S_IRUSR | stat.S_IWUSR)  # 0600
    ccyo_out.print_text(f"  Config updated: [dim]{config_path}[/dim]")


@aurora_app.command("create")
def aurora_create(
    region: str = typer.Option("us-west-2", "--region", "-r", help="AWS region"),
    instance_class: str = typer.Option(
        "db.r6g.large", "--instance-class", help="RDS instance class"
    ),
    engine_version: str = typer.Option(
        "16.6", "--engine-version", help="Aurora PostgreSQL engine version"
    ),
    vpc_id: str = typer.Option("", "--vpc-id", help="VPC ID to deploy into"),
    cidr: Optional[str] = typer.Option(
        None,
        "--cidr",
        help=(
            "Ingress CIDR for security group. Defaults to the current public IP "
            "(/32) when --publicly-accessible is set, otherwise 10.0.0.0/8."
        ),
    ),
    cost_center: str = typer.Option(
        "global", "--cost-center", help="LSMC cost center tag"
    ),
    project: Optional[str] = typer.Option(
        None, "--project", help="LSMC project tag (default: tapdb-<region>)"
    ),
    publicly_accessible: bool = typer.Option(
        False,
        "--publicly-accessible/--no-publicly-accessible",
        help="Whether the DB instance is publicly accessible (default: False)",
    ),
    no_iam_auth: bool = typer.Option(
        False, "--no-iam-auth", help="Disable IAM database authentication"
    ),
    no_deletion_protection: bool = typer.Option(
        False,
        "--no-deletion-protection",
        help="Disable deletion protection (default: deletion protection ON)",
    ),
    background: bool = typer.Option(
        False, "--background", help="Fire-and-forget: initiate creation and exit"
    ),
):
    """Create an Aurora PostgreSQL cluster via CloudFormation."""
    try:
        cluster_identifier = _target_cluster_identifier()
    except RuntimeError as exc:
        ccyo_out.error(str(exc))
        raise typer.Exit(1)

    try:
        resolved_cidr = _resolve_ingress_cidr(cidr, publicly_accessible)
    except RuntimeError as exc:
        ccyo_out.error(str(exc))
        raise typer.Exit(1)

    if resolved_cidr == "0.0.0.0/0" and publicly_accessible:
        ccyo_out.warning(
            "⚠️  WARNING: Creating publicly accessible cluster open to "
            "all IPs (0.0.0.0/0). Consider restricting --cidr to your IP.",
            err=True,
        )

    _ensure_boto3()

    from daylily_tapdb.aurora.config import AuroraConfig
    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    proj = project or f"tapdb-{region}"
    tags = {"lsmc-cost-center": cost_center, "lsmc-project": proj}

    config = AuroraConfig(
        region=region,
        cluster_identifier=cluster_identifier,
        instance_class=instance_class,
        engine_version=engine_version,
        vpc_id=vpc_id,
        cidr=resolved_cidr,
        iam_auth=not no_iam_auth,
        publicly_accessible=publicly_accessible,
        deletion_protection=not no_deletion_protection,
        tags=tags,
    )

    stack_name = _stack_name_for_target(cluster_identifier)
    ccyo_out.print_text(
        "\n[bold cyan]━━━ Aurora Create (explicit target) ━━━[/bold cyan]"
    )
    ccyo_out.print_text(f"  Stack:    {stack_name}")
    ccyo_out.print_text(f"  Cluster:  {cluster_identifier}")
    ccyo_out.print_text(f"  Region:   {region}")
    ccyo_out.print_text(f"  Instance: {instance_class}")
    ccyo_out.print_text(f"  Engine:   PostgreSQL {engine_version}")
    ccyo_out.print_text(f"  IAM Auth: {config.iam_auth}")
    ccyo_out.print_text(f"  VPC:      {vpc_id or '(auto-discover default)'}")
    ccyo_out.print_text(f"  Ingress:  {resolved_cidr}")
    ccyo_out.print_text("")

    try:
        mgr = AuroraStackManager(region=region)

        if background:
            # Fire-and-forget: start creation, don't wait
            initiated = mgr.initiate_create_stack(config)
            ccyo_out.success(
                f"Stack creation initiated (stack: {initiated['stack_name']})."
            )
            ccyo_out.detail("Check progress with: [cyan]tapdb aurora status[/cyan]")
            return
        else:
            # Block with live progress using rich status spinner
            from rich.status import Status

            status_display = Status("Creating...", console=console, spinner="dots")

            def _progress_callback(status: str, elapsed: float) -> None:
                status_display.update(
                    f"Creating... ({elapsed:.0f}s elapsed) — Status: {status}"
                )

            status_display.start()
            try:
                result = mgr.create_stack(config, callback=_progress_callback)
            finally:
                status_display.stop()
    except RuntimeError as exc:
        ccyo_out.error(f"Stack creation failed: {exc}")
        raise typer.Exit(1)

    outputs = result.get("outputs", {})
    endpoint = outputs.get("ClusterEndpoint", "")
    port = outputs.get("ClusterPort", "5432")

    ccyo_out.success(f"\nStack {stack_name} created.")
    if endpoint:
        ccyo_out.print_text(f"  Endpoint: [cyan]{endpoint}[/cyan]")
        ccyo_out.print_text(f"  Port:     {port}")
        _update_config_file(
            endpoint,
            port,
            region,
            cluster_identifier=cluster_identifier,
            secret_arn=outputs.get("SecretArn"),
        )

    if outputs.get("SecretArn"):
        ccyo_out.print_text(f"  Secret:   {outputs['SecretArn']}")


@aurora_app.command("delete")
def aurora_delete(
    region: str = typer.Option("us-west-2", "--region", "-r", help="AWS region"),
    retain_networking: bool = typer.Option(
        True,
        "--retain-networking/--no-retain-networking",
        help="Retain VPC security group and subnet group (default: retain)",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
):
    """Delete an Aurora CloudFormation stack."""
    _ensure_boto3()
    try:
        cluster_identifier = _target_cluster_identifier()
    except RuntimeError as exc:
        ccyo_out.error(str(exc))
        raise typer.Exit(1)

    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    stack_name = _stack_name_for_target(cluster_identifier)

    if not force:
        from rich.prompt import Confirm

        ccyo_out.warning(f"\nThis will delete stack {stack_name} in {region}.")
        if retain_networking:
            ccyo_out.print_text(
                "  Networking resources (SG, subnet group) will be retained."
            )
        else:
            ccyo_out.error("  All resources including networking will be deleted.")
        if not Confirm.ask("Proceed?", default=False):
            ccyo_out.print_text("[dim]Cancelled.[/dim]")
            raise typer.Exit(0)

    ccyo_out.print_text(
        "\n[bold cyan]━━━ Aurora Delete (explicit target) ━━━[/bold cyan]"
    )
    ccyo_out.print_text(f"  Stack:  {stack_name}")
    ccyo_out.print_text(f"  Cluster: {cluster_identifier}")
    ccyo_out.print_text(f"  Region: {region}")
    ccyo_out.print_text(f"  Retain networking: {retain_networking}")
    ccyo_out.print_text("")

    try:
        import boto3

        mgr = AuroraStackManager(region=region)

        # Disable deletion protection before stack deletion
        cluster_id = cluster_identifier
        try:
            rds = boto3.client("rds", region_name=region)
            rds.modify_db_cluster(
                DBClusterIdentifier=cluster_id,
                DeletionProtection=False,
                ApplyImmediately=True,
            )
            ccyo_out.print_text(
                f"  [dim]Disabled deletion protection on cluster {cluster_id}[/dim]"
            )
        except Exception as exc:
            ccyo_out.print_text(
                f"  [dim]Could not disable deletion protection: {exc}[/dim]"
            )

        result = mgr.delete_stack(stack_name, retain_networking=retain_networking)
    except RuntimeError as exc:
        ccyo_out.error(f"Stack deletion failed: {exc}")
        raise typer.Exit(1)

    ccyo_out.success(f"Stack {stack_name} deleted (status: {result['status']}).")


@aurora_app.command("status")
def aurora_status(
    region: str = typer.Option("us-west-2", "--region", "-r", help="AWS region"),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show Aurora stack status, endpoint, and outputs."""
    _ensure_boto3()
    try:
        cluster_identifier = _target_cluster_identifier()
    except RuntimeError as exc:
        ccyo_out.error(str(exc))
        raise typer.Exit(1)

    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    stack_name = _stack_name_for_target(cluster_identifier)

    try:
        mgr = AuroraStackManager(region=region)
        info = mgr.get_stack_status(stack_name)
    except RuntimeError as exc:
        ccyo_out.error(f"{exc}")
        raise typer.Exit(1)

    if as_json:
        ccyo_out.emit_json(info)
        return

    status = info["status"]
    color = "green" if "COMPLETE" in status and "ROLLBACK" not in status else "yellow"
    if "FAILED" in status:
        color = "red"

    ccyo_out.print_text(
        "\n[bold cyan]━━━ Aurora Status (explicit target) ━━━[/bold cyan]"
    )
    ccyo_out.print_text(f"  Stack:  {stack_name}")
    ccyo_out.print_text(f"  Cluster: {cluster_identifier}")
    ccyo_out.print_text(f"  Region: {region}")
    ccyo_out.print_text(f"  Status: [{color}]{status}[/{color}]")

    outputs = info.get("outputs", {})
    if outputs:
        ccyo_out.print_text("\n  [bold]Outputs:[/bold]")
        for key, val in outputs.items():
            ccyo_out.print_text(f"    {key}: [cyan]{val}[/cyan]")


@aurora_app.command("connect")
def aurora_connect(
    region: str = typer.Option("us-west-2", "--region", "-r", help="AWS region"),
    user: str = typer.Option("tapdb_admin", "--user", "-u", help="Database user"),
    database: str = typer.Option(
        None,
        "--database",
        "-d",
        help="Database name (default: target.database)",
    ),
    export: bool = typer.Option(
        False, "--export", "-e", help="Print export statements for shell"
    ),
):
    """Print or export connection info for the explicit Aurora target."""
    _ensure_boto3()
    try:
        cfg = get_db_config()
        cluster_identifier = _target_cluster_identifier()
    except RuntimeError as exc:
        ccyo_out.error(str(exc))
        raise typer.Exit(1)

    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    db_name = database or str(cfg.get("database") or "").strip()
    if not db_name:
        ccyo_out.error("target.database is required for Aurora connection output.")
        raise typer.Exit(1)
    stack_name = _stack_name_for_target(cluster_identifier)

    try:
        mgr = AuroraStackManager(region=region)
        info = mgr.get_stack_status(stack_name)
    except RuntimeError as exc:
        ccyo_out.error(f"{exc}")
        raise typer.Exit(1)

    outputs = info.get("outputs", {})
    endpoint = outputs.get("ClusterEndpoint", "")
    port = int(outputs.get("ClusterPort", "5432"))

    if not endpoint:
        ccyo_out.error(
            f"No endpoint found for stack {stack_name}. Is the cluster fully created?"
        )
        raise typer.Exit(1)

    if export:
        ccyo_out.print_text(f"export PGHOST={endpoint}")
        ccyo_out.print_text(f"export PGPORT={port}")
        ccyo_out.print_text(f"export PGDATABASE={db_name}")
        ccyo_out.print_text(f"export PGUSER={user}")
        ccyo_out.print_text("export PGSSLMODE=verify-full")
    else:
        ccyo_out.print_text(
            "\n[bold cyan]━━━ Aurora Connect (explicit target) ━━━[/bold cyan]"
        )
        ccyo_out.print_text(f"  Endpoint: [cyan]{endpoint}[/cyan]")
        ccyo_out.print_text(f"  Port:     {port}")
        ccyo_out.print_text(f"  Database: {db_name}")
        ccyo_out.print_text(f"  User:     {user}")
        ccyo_out.print_text("  SSL:      verify-full")
        ccyo_out.print_text(
            f"\n  Connection URL (IAM auth):\n"
            f"  [dim]postgresql+psycopg2://{user}:<iam-token>@{endpoint}:{port}/{db_name}"
            f"?sslmode=verify-full[/dim]"
        )


@aurora_app.command("list")
def aurora_list(
    region: str = typer.Option(
        "us-west-2", "--region", "-r", help="AWS region to scan"
    ),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """List all tapdb Aurora stacks."""
    _ensure_boto3()

    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    try:
        mgr = AuroraStackManager(region=region)
        stacks = mgr.detect_existing_resources(region=region)
    except RuntimeError as exc:
        ccyo_out.error(f"{exc}")
        raise typer.Exit(1)

    if as_json:
        ccyo_out.emit_json(stacks)
        return

    if not stacks:
        ccyo_out.print_text(f"[dim]No tapdb Aurora stacks found in {region}.[/dim]")
        return

    table = Table(title=f"TAPDB Aurora Stacks ({region})")
    table.add_column("Stack", style="cyan")
    table.add_column("Status")
    table.add_column("Endpoint")
    table.add_column("Cost Center", style="dim")

    for name, info in stacks.items():
        status = info.get("status", "")
        is_ok = "COMPLETE" in status and "ROLLBACK" not in status
        color = "green" if is_ok else "yellow"
        if "FAILED" in status:
            color = "red"
        endpoint = info.get("outputs", {}).get("ClusterEndpoint", "-")
        cost = info.get("tags", {}).get("lsmc-cost-center", "-")
        table.add_row(name, f"[{color}]{status}[/{color}]", endpoint, cost)

    print_renderable(table)
