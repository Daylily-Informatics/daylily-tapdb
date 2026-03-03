"""
TAPDB User Management CLI.

Commands for managing TAPDB application users.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from daylily_tapdb import TAPDBConnection
from daylily_tapdb.cli.db import Environment
from daylily_tapdb.cli.db_config import get_db_config_for_env
from daylily_tapdb.passwords import hash_password as _hash_password
from daylily_tapdb.user_store import (
    create_or_get,
    list_users,
    set_active,
    set_password_hash,
    set_role,
    soft_delete,
)

user_app = typer.Typer(help="User management commands")
console = Console()


def _open_connection(env: Environment, *, app_username: str) -> TAPDBConnection:
    cfg = get_db_config_for_env(env.value)
    engine_type = (cfg.get("engine_type") or "local").strip().lower()
    iam_auth = (cfg.get("iam_auth") or "true").strip().lower() in (
        "true",
        "1",
        "yes",
        "on",
    )
    region = (cfg.get("region") or "us-west-2").strip()
    return TAPDBConnection(
        db_hostname=f"{cfg['host']}:{cfg['port']}",
        db_user=cfg["user"],
        db_pass=cfg.get("password") or None,
        db_name=cfg["database"],
        engine_type=engine_type,
        region=region,
        iam_auth=iam_auth,
        app_username=app_username,
    )


def _format_date(value: object, *, include_time: bool = False) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M" if include_time else "%Y-%m-%d")
    raw = str(value).strip()
    if not raw:
        return "-"
    # Accept ISO strings persisted in json_addl.
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %H:%M" if include_time else "%Y-%m-%d")
    except Exception:
        return raw


@user_app.command("list")
def user_list(
    env: Environment = typer.Argument(..., help="Target environment"),
    show_inactive: bool = typer.Option(
        False, "--inactive", "-i", help="Include inactive users"
    ),
):
    """List all users."""
    try:
        with _open_connection(env, app_username="tapdb_user_cli") as conn:
            with conn.session_scope() as session:
                users = list_users(session, include_inactive=show_inactive)
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to list users: {e}")
        raise typer.Exit(1)

    if not users:
        console.print("[dim]No users found[/dim]")
        return

    table = Table(title=f"TAPDB Users ({env.value})")
    table.add_column("Username", style="cyan")
    table.add_column("Email")
    table.add_column("Display Name")
    table.add_column("Role", style="bold")
    table.add_column("Active")
    table.add_column("Created")
    table.add_column("Last Login")

    for user in users:
        role_style = "green" if user.role == "admin" else ""
        active_icon = "✓" if user.is_active else "✗"
        table.add_row(
            user.username,
            user.email or "",
            user.display_name or "",
            f"[{role_style}]{user.role}[/{role_style}]" if role_style else user.role,
            active_icon,
            _format_date(user.created_dt),
            _format_date(user.last_login_dt, include_time=True),
        )

    console.print(table)


@user_app.command("add")
def user_add(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Option(..., "--username", "-u", help="Username (unique)"),
    role: str = typer.Option("user", "--role", "-r", help="Role: admin or user"),
    email: Optional[str] = typer.Option(None, "--email", "-e", help="Email address"),
    display_name: Optional[str] = typer.Option(
        None, "--name", "-n", help="Display name"
    ),
    password: Optional[str] = typer.Option(
        None, "--password", "-p", help="Password (optional)"
    ),
):
    """Add a new user."""
    if role not in ("admin", "user"):
        console.print(f"[red]✗[/red] Invalid role: {role}. Must be 'admin' or 'user'")
        raise typer.Exit(1)

    pw_hash = None
    if password:
        try:
            pw_hash = _hash_password(password)
        except RuntimeError as e:
            console.print(f"[red]✗[/red] {e}")
            console.print("  Install with: [cyan]pip install 'passlib[bcrypt]'[/cyan]")
            raise typer.Exit(1)

    try:
        with _open_connection(env, app_username=username) as conn:
            with conn.session_scope(commit=True) as session:
                _, created = create_or_get(
                    session,
                    login_identifier=username,
                    email=email,
                    display_name=display_name,
                    role=role,
                    is_active=True,
                    require_password_change=False,
                    password_hash=pw_hash,
                    cognito_username=email or username,
                )
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to create user: {e}")
        raise typer.Exit(1)

    if not created:
        console.print(f"[red]✗[/red] User '{username}' already exists")
        raise typer.Exit(1)

    console.print(
        f"[green]✓[/green] Created user"
        f" [cyan]{username}[/cyan]"
        f" with role [bold]{role}[/bold]"
    )


@user_app.command("set-role")
def user_set_role(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username to modify"),
    role: str = typer.Argument(..., help="New role: admin or user"),
):
    """Set user role (admin or user)."""
    if role not in ("admin", "user"):
        console.print(f"[red]✗[/red] Invalid role: {role}. Must be 'admin' or 'user'")
        raise typer.Exit(1)
    try:
        with _open_connection(env, app_username=username) as conn:
            with conn.session_scope(commit=True) as session:
                updated = set_role(session, username, role)
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to set role: {e}")
        raise typer.Exit(1)

    if not updated:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)

    console.print(
        f"[green]✓[/green] Set [cyan]{username}[/cyan] role to [bold]{role}[/bold]"
    )


@user_app.command("deactivate")
def user_deactivate(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username to deactivate"),
):
    """Deactivate a user (soft disable)."""
    try:
        with _open_connection(env, app_username=username) as conn:
            with conn.session_scope(commit=True) as session:
                updated = set_active(session, username, False)
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to deactivate user: {e}")
        raise typer.Exit(1)
    if not updated:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)
    console.print(f"[green]✓[/green] Deactivated user [cyan]{username}[/cyan]")


@user_app.command("activate")
def user_activate(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username to activate"),
):
    """Activate a user."""
    try:
        with _open_connection(env, app_username=username) as conn:
            with conn.session_scope(commit=True) as session:
                updated = set_active(session, username, True)
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to activate user: {e}")
        raise typer.Exit(1)
    if not updated:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)
    console.print(f"[green]✓[/green] Activated user [cyan]{username}[/cyan]")


@user_app.command("set-password")
def user_set_password(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username"),
    password: str = typer.Option(
        ...,
        "--password",
        "-p",
        prompt=True,
        hide_input=True,
        confirmation_prompt=True,
        help="New password",
    ),
):
    """Set user password."""
    try:
        pw_hash = _hash_password(password)
    except RuntimeError as e:
        console.print(f"[red]✗[/red] {e}")
        console.print("  Install with: [cyan]pip install 'passlib[bcrypt]'[/cyan]")
        raise typer.Exit(1)

    try:
        with _open_connection(env, app_username=username) as conn:
            with conn.session_scope(commit=True) as session:
                updated = set_password_hash(
                    session,
                    username,
                    pw_hash,
                    require_password_change=None,
                )
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to set password: {e}")
        raise typer.Exit(1)

    if not updated:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Password updated for [cyan]{username}[/cyan]")


@user_app.command("delete")
def user_delete(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username to delete"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Delete a user (soft delete)."""
    if not force:
        confirm = typer.confirm(f"Permanently delete user '{username}'?")
        if not confirm:
            console.print("[dim]Cancelled[/dim]")
            raise typer.Exit(0)

    try:
        with _open_connection(env, app_username=username) as conn:
            with conn.session_scope(commit=True) as session:
                deleted = soft_delete(session, username)
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to delete user: {e}")
        raise typer.Exit(1)

    if not deleted:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Deleted user [cyan]{username}[/cyan]")
