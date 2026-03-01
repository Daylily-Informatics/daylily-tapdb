"""Aurora (AWS RDS) CLI commands for TAPDB.

Provides ``tapdb aurora`` subcommand group for managing Aurora PostgreSQL
clusters via CloudFormation.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()

aurora_app = typer.Typer(help="Aurora PostgreSQL cluster management commands")


def _ensure_boto3():
    """Import boto3, raising a clear CLI error if missing."""
    try:
        import boto3  # noqa: F401

        return boto3
    except ImportError:
        console.print(
            "[red]✗[/red] boto3 is required for Aurora commands.\n"
            "  Install with: [cyan]pip install 'daylily-tapdb[aurora]'[/cyan]"
        )
        raise typer.Exit(1)


def _stack_name_for_env(env: str) -> str:
    """Derive CloudFormation stack name from environment name."""
    return f"tapdb-{env}"


def _update_config_file(
    env: str,
    endpoint: str,
    port: str,
    region: str,
    cluster_identifier: str | None = None,
) -> None:
    """Update TAPDB config file with Aurora endpoint info.

    Honors TAPDB_CONFIG_PATH override and TAPDB_DATABASE_NAME scoping.
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

    if "environments" not in existing:
        existing["environments"] = {}

    env_cfg = existing["environments"].get(env, {}) or {}
    existing["environments"][env] = {
        **env_cfg,
        "engine_type": "aurora",
        "host": endpoint,
        "port": port,
        "database": f"tapdb_{env}",
        "user": "tapdb_admin",
        "region": region,
        "cluster_identifier": cluster_identifier or env,
        "iam_auth": "true",
        "ssl": "true",
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
    console.print(f"  Config updated: [dim]{config_path}[/dim]")


@aurora_app.command("create")
def aurora_create(
    env: str = typer.Argument(..., help="Environment name (e.g. dev, staging, prod)"),
    region: str = typer.Option("us-west-2", "--region", "-r", help="AWS region"),
    instance_class: str = typer.Option(
        "db.r6g.large", "--instance-class", help="RDS instance class"
    ),
    engine_version: str = typer.Option(
        "16.6", "--engine-version", help="Aurora PostgreSQL engine version"
    ),
    vpc_id: str = typer.Option("", "--vpc-id", help="VPC ID to deploy into"),
    cidr: str = typer.Option(
        "10.0.0.0/8", "--cidr", help="Ingress CIDR for security group"
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
    if cidr == "0.0.0.0/0" and publicly_accessible:
        console.print(
            "[yellow]⚠️  WARNING: Creating publicly accessible cluster open to "
            "all IPs (0.0.0.0/0). Consider restricting --cidr to your IP.[/yellow]",
            err=True,
        )

    _ensure_boto3()

    from daylily_tapdb.aurora.config import AuroraConfig
    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    proj = project or f"tapdb-{region}"
    tags = {"lsmc-cost-center": cost_center, "lsmc-project": proj}

    config = AuroraConfig(
        region=region,
        cluster_identifier=env,
        instance_class=instance_class,
        engine_version=engine_version,
        vpc_id=vpc_id,
        iam_auth=not no_iam_auth,
        publicly_accessible=publicly_accessible,
        deletion_protection=not no_deletion_protection,
        tags=tags,
    )

    stack_name = _stack_name_for_env(env)
    console.print(f"\n[bold cyan]━━━ Aurora Create ({env}) ━━━[/bold cyan]")
    console.print(f"  Stack:    {stack_name}")
    console.print(f"  Region:   {region}")
    console.print(f"  Instance: {instance_class}")
    console.print(f"  Engine:   PostgreSQL {engine_version}")
    console.print(f"  IAM Auth: {config.iam_auth}")
    console.print(f"  VPC:      {vpc_id or '(auto-discover default)'}")
    console.print()

    try:
        mgr = AuroraStackManager(region=region)

        if background:
            # Fire-and-forget: start creation, don't wait
            initiated = mgr.initiate_create_stack(config)
            console.print(
                f"[green]✓[/green] Stack creation initiated "
                f"(stack: [bold]{initiated['stack_name']}[/bold])."
            )
            console.print(
                f"  Check progress with: "
                f"[cyan]tapdb aurora status {env}[/cyan]"
            )
            return
        else:
            # Block with live progress using rich status spinner
            from rich.status import Status

            status_display = Status(
                "Creating...", console=console, spinner="dots"
            )

            def _progress_callback(status: str, elapsed: float) -> None:
                status_display.update(
                    f"Creating... ({elapsed:.0f}s elapsed) — "
                    f"Status: {status}"
                )

            status_display.start()
            try:
                result = mgr.create_stack(config, callback=_progress_callback)
            finally:
                status_display.stop()
    except RuntimeError as exc:
        console.print(f"[red]✗[/red] Stack creation failed: {exc}")
        raise typer.Exit(1)

    outputs = result.get("outputs", {})
    endpoint = outputs.get("ClusterEndpoint", "")
    port = outputs.get("ClusterPort", "5432")

    console.print(f"\n[green]✓[/green] Stack [bold]{stack_name}[/bold] created.")
    if endpoint:
        console.print(f"  Endpoint: [cyan]{endpoint}[/cyan]")
        console.print(f"  Port:     {port}")
        _update_config_file(env, endpoint, port, region)

    if outputs.get("SecretArn"):
        console.print(f"  Secret:   {outputs['SecretArn']}")


@aurora_app.command("delete")
def aurora_delete(
    env: str = typer.Argument(..., help="Environment name"),
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

    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    stack_name = _stack_name_for_env(env)

    if not force:
        from rich.prompt import Confirm

        console.print(
            f"\n[yellow]⚠[/yellow]  This will delete stack [bold]{stack_name}[/bold] "
            f"in [bold]{region}[/bold]."
        )
        if retain_networking:
            console.print("  Networking resources (SG, subnet group) will be retained.")
        else:
            console.print(
                "  [red]All resources[/red] including networking will be deleted."
            )
        if not Confirm.ask("Proceed?", default=False):
            console.print("[dim]Cancelled.[/dim]")
            raise typer.Exit(0)

    console.print(f"\n[bold cyan]━━━ Aurora Delete ({env}) ━━━[/bold cyan]")
    console.print(f"  Stack:  {stack_name}")
    console.print(f"  Region: {region}")
    console.print(f"  Retain networking: {retain_networking}")
    console.print()

    try:
        import boto3

        mgr = AuroraStackManager(region=region)

        # Disable deletion protection before stack deletion
        cluster_id = env
        try:
            rds = boto3.client("rds", region_name=region)
            rds.modify_db_cluster(
                DBClusterIdentifier=cluster_id,
                DeletionProtection=False,
                ApplyImmediately=True,
            )
            console.print(
                f"  [dim]Disabled deletion protection on cluster {cluster_id}[/dim]"
            )
        except Exception as exc:
            console.print(
                f"  [dim]Could not disable deletion protection: {exc}[/dim]"
            )

        result = mgr.delete_stack(stack_name, retain_networking=retain_networking)
    except RuntimeError as exc:
        console.print(f"[red]✗[/red] Stack deletion failed: {exc}")
        raise typer.Exit(1)

    console.print(
        f"[green]✓[/green] Stack [bold]{stack_name}[/bold] deleted "
        f"(status: {result['status']})."
    )


@aurora_app.command("status")
def aurora_status(
    env: str = typer.Argument(..., help="Environment name"),
    region: str = typer.Option("us-west-2", "--region", "-r", help="AWS region"),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show Aurora stack status, endpoint, and outputs."""
    _ensure_boto3()

    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    stack_name = _stack_name_for_env(env)

    try:
        mgr = AuroraStackManager(region=region)
        info = mgr.get_stack_status(stack_name)
    except RuntimeError as exc:
        console.print(f"[red]✗[/red] {exc}")
        raise typer.Exit(1)

    if as_json:
        console.print_json(json.dumps(info))
        return

    status = info["status"]
    color = "green" if "COMPLETE" in status and "ROLLBACK" not in status else "yellow"
    if "FAILED" in status:
        color = "red"

    console.print(f"\n[bold cyan]━━━ Aurora Status ({env}) ━━━[/bold cyan]")
    console.print(f"  Stack:  {stack_name}")
    console.print(f"  Region: {region}")
    console.print(f"  Status: [{color}]{status}[/{color}]")

    outputs = info.get("outputs", {})
    if outputs:
        console.print("\n  [bold]Outputs:[/bold]")
        for key, val in outputs.items():
            console.print(f"    {key}: [cyan]{val}[/cyan]")


@aurora_app.command("connect")
def aurora_connect(
    env: str = typer.Argument(..., help="Environment name"),
    region: str = typer.Option("us-west-2", "--region", "-r", help="AWS region"),
    user: str = typer.Option("tapdb_admin", "--user", "-u", help="Database user"),
    database: str = typer.Option(
        None,
        "--database",
        "-d",
        help="Database name (default: tapdb_<env>)",
    ),
    export: bool = typer.Option(
        False, "--export", "-e", help="Print export statements for shell"
    ),
):
    """Print or export connection info for an Aurora environment."""
    _ensure_boto3()

    from daylily_tapdb.aurora.stack_manager import AuroraStackManager

    db_name = database or f"tapdb_{env}"
    stack_name = _stack_name_for_env(env)

    try:
        mgr = AuroraStackManager(region=region)
        info = mgr.get_stack_status(stack_name)
    except RuntimeError as exc:
        console.print(f"[red]✗[/red] {exc}")
        raise typer.Exit(1)

    outputs = info.get("outputs", {})
    endpoint = outputs.get("ClusterEndpoint", "")
    port = int(outputs.get("ClusterPort", "5432"))

    if not endpoint:
        console.print(
            f"[red]✗[/red] No endpoint found for stack {stack_name}. "
            "Is the cluster fully created?"
        )
        raise typer.Exit(1)

    if export:
        console.print(f"export PGHOST={endpoint}")
        console.print(f"export PGPORT={port}")
        console.print(f"export PGDATABASE={db_name}")
        console.print(f"export PGUSER={user}")
        console.print("export PGSSLMODE=verify-full")
    else:
        console.print(f"\n[bold cyan]━━━ Aurora Connect ({env}) ━━━[/bold cyan]")
        console.print(f"  Endpoint: [cyan]{endpoint}[/cyan]")
        console.print(f"  Port:     {port}")
        console.print(f"  Database: {db_name}")
        console.print(f"  User:     {user}")
        console.print("  SSL:      verify-full")
        console.print(
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
        console.print(f"[red]✗[/red] {exc}")
        raise typer.Exit(1)

    if as_json:
        console.print_json(json.dumps(stacks, default=str))
        return

    if not stacks:
        console.print(f"[dim]No tapdb Aurora stacks found in {region}.[/dim]")
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

    console.print(table)
