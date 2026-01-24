"""
TAPDB User Management CLI.

Commands for managing TAPDB application users.
"""
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from daylily_tapdb.cli.db import Environment, _get_db_config, _run_psql
from daylily_tapdb.passwords import hash_password as _hash_password

user_app = typer.Typer(help="User management commands")
console = Console()

@user_app.command("list")
def user_list(
    env: Environment = typer.Argument(..., help="Target environment"),
    show_inactive: bool = typer.Option(False, "--inactive", "-i", help="Include inactive users"),
):
    """List all users."""
    where = "" if show_inactive else "WHERE is_active = TRUE"
    sql = f"""
        SELECT username, email, display_name, role, is_active, 
               to_char(created_dt, 'YYYY-MM-DD') as created,
               to_char(last_login_dt, 'YYYY-MM-DD HH24:MI') as last_login
        FROM tapdb_user {where}
        ORDER BY username
    """
    success, output = _run_psql(env, sql=sql)
    
    if not success:
        console.print(f"[red]✗[/red] Failed to list users: {output}")
        raise typer.Exit(1)
    
    # Parse output and display as table
    lines = [l.strip() for l in output.strip().split("\n") if l.strip() and not l.startswith("(")]
    
    if len(lines) <= 2:  # Header + separator only
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
    
    for line in lines[2:]:  # Skip header and separator
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 7:
            role_style = "green" if parts[3] == "admin" else ""
            active_icon = "✓" if parts[4].strip().lower() in ("t", "true") else "✗"
            table.add_row(
                parts[0], parts[1], parts[2],
                f"[{role_style}]{parts[3]}[/{role_style}]" if role_style else parts[3],
                active_icon, parts[5], parts[6] or "-"
            )
    
    console.print(table)


@user_app.command("add")
def user_add(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Option(..., "--username", "-u", help="Username (unique)"),
    role: str = typer.Option("user", "--role", "-r", help="Role: admin or user"),
    email: Optional[str] = typer.Option(None, "--email", "-e", help="Email address"),
    display_name: Optional[str] = typer.Option(None, "--name", "-n", help="Display name"),
    password: Optional[str] = typer.Option(None, "--password", "-p", help="Password (optional)"),
):
    """Add a new user."""
    if role not in ("admin", "user"):
        console.print(f"[red]✗[/red] Invalid role: {role}. Must be 'admin' or 'user'")
        raise typer.Exit(1)
    
    # Hash password if provided
    pw_hash = None
    if password:
        try:
            pw_hash = _hash_password(password)
        except RuntimeError as e:
            console.print(f"[red]✗[/red] {e}")
            console.print("  Install with: [cyan]pip install 'passlib[bcrypt]'[/cyan]")
            raise typer.Exit(1)
    pw_sql = f"'{pw_hash}'" if pw_hash else "NULL"
    email_sql = f"'{email}'" if email else "NULL"
    name_sql = f"'{display_name}'" if display_name else "NULL"
    
    sql = f"""
        INSERT INTO tapdb_user (username, email, display_name, role, password_hash)
        VALUES ('{username}', {email_sql}, {name_sql}, '{role}', {pw_sql})
        RETURNING username, role
    """
    
    success, output = _run_psql(env, sql=sql)
    
    if not success:
        if "duplicate key" in output.lower():
            console.print(f"[red]✗[/red] User '{username}' already exists")
        else:
            console.print(f"[red]✗[/red] Failed to create user: {output}")
        raise typer.Exit(1)
    
    console.print(f"[green]✓[/green] Created user [cyan]{username}[/cyan] with role [bold]{role}[/bold]")


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
    
    sql = f"UPDATE tapdb_user SET role = '{role}', modified_dt = NOW() WHERE username = '{username}' RETURNING username"
    success, output = _run_psql(env, sql=sql)
    
    if not success or "UPDATE 0" in output:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)
    
    console.print(f"[green]✓[/green] Set [cyan]{username}[/cyan] role to [bold]{role}[/bold]")


@user_app.command("deactivate")
def user_deactivate(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username to deactivate"),
):
    """Deactivate a user (soft disable)."""
    sql = f"UPDATE tapdb_user SET is_active = FALSE, modified_dt = NOW() WHERE username = '{username}' RETURNING username"
    success, output = _run_psql(env, sql=sql)

    if not success or "UPDATE 0" in output:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Deactivated user [cyan]{username}[/cyan]")


@user_app.command("activate")
def user_activate(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username to activate"),
):
    """Activate a user."""
    sql = f"UPDATE tapdb_user SET is_active = TRUE, modified_dt = NOW() WHERE username = '{username}' RETURNING username"
    success, output = _run_psql(env, sql=sql)

    if not success or "UPDATE 0" in output:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Activated user [cyan]{username}[/cyan]")


@user_app.command("set-password")
def user_set_password(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username"),
    password: str = typer.Option(..., "--password", "-p", prompt=True, hide_input=True,
                                  confirmation_prompt=True, help="New password"),
):
    """Set user password."""
    try:
        pw_hash = _hash_password(password)
    except RuntimeError as e:
        console.print(f"[red]✗[/red] {e}")
        console.print("  Install with: [cyan]pip install 'passlib[bcrypt]'[/cyan]")
        raise typer.Exit(1)
    sql = f"UPDATE tapdb_user SET password_hash = '{pw_hash}', modified_dt = NOW() WHERE username = '{username}' RETURNING username"
    success, output = _run_psql(env, sql=sql)

    if not success or "UPDATE 0" in output:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Password updated for [cyan]{username}[/cyan]")


@user_app.command("delete")
def user_delete(
    env: Environment = typer.Argument(..., help="Target environment"),
    username: str = typer.Argument(..., help="Username to delete"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Permanently delete a user."""
    if not force:
        confirm = typer.confirm(f"Permanently delete user '{username}'?")
        if not confirm:
            console.print("[dim]Cancelled[/dim]")
            raise typer.Exit(0)

    sql = f"DELETE FROM tapdb_user WHERE username = '{username}' RETURNING username"
    success, output = _run_psql(env, sql=sql)

    if not success or "DELETE 0" in output:
        console.print(f"[red]✗[/red] User '{username}' not found")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Deleted user [cyan]{username}[/cyan]")

