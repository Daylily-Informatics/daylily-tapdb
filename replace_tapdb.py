import re
import sys
import os

def process(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    if "from cli_core_yo import output" not in content:
        content = content.replace("import typer\n", "import typer\nfrom cli_core_yo import output\n")

    # Error replacements
    content = re.sub(r'console\.print\([^"]*?\[red\]✗\[/red\]  ([^"]*)"\)\n\s*raise typer\.Exit\(1\)', r'output.abort("\1")', content)
    content = re.sub(r'console\.print\(f"[^"]*?\[red\]✗\[/red\]  ([^"]*)"\)\n\s*raise typer\.Exit\(1\)', r'output.abort(f"\1")', content)

    # Success replacements
    content = re.sub(r'console\.print\([^"]*?\[green\]✓\[/green\]  ([^"]*)"\)', r'output.success("\1")', content)
    content = re.sub(r'console\.print\(f"[^"]*?\[green\]✓\[/green\]  ([^"]*)"\)', r'output.success(f"\1")', content)

    # Replace remaining console.print with output.info
    content = re.sub(r'console\.print\(', 'output.info(', content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

process("daylily_tapdb/cli/__init__.py")
print("Done")
