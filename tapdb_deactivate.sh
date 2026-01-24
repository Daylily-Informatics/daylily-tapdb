#!/bin/sh
# TAPDB Environment Deactivation Script
# Usage: source tapdb_deactivate.sh
#
# This script:
#   1. Stops any running local PostgreSQL instances
#   2. Removes tab completion
#   3. Deactivates the virtual environment

# Colors (POSIX-safe)
_tapdb_red='\033[0;31m'
_tapdb_green='\033[0;32m'
_tapdb_cyan='\033[0;36m'
_tapdb_yellow='\033[0;33m'
_tapdb_bold='\033[1m'
_tapdb_reset='\033[0m'

printf "${_tapdb_bold}${_tapdb_cyan}━━━ TAPDB Deactivation ━━━${_tapdb_reset}\n"

if [ -z "${VIRTUAL_ENV:-}" ]; then
    printf "${_tapdb_yellow}⚠${_tapdb_reset} No virtual environment is active.\n"
    unset _tapdb_red _tapdb_green _tapdb_cyan _tapdb_yellow _tapdb_bold _tapdb_reset
    return 0 2>/dev/null || exit 0
fi

# Stop local PostgreSQL instances if running
_tapdb_find_repo_root() {
    _d="$(pwd)"
    while [ "$_d" != "/" ]; do
        if [ -f "$_d/pyproject.toml" ] && [ -d "$_d/daylily_tapdb" ]; then
            printf "%s" "$_d"
            return 0
        fi
        _d="$(dirname "$_d")"
    done
    return 1
}

_tapdb_repo_root="$(_tapdb_find_repo_root)"
if [ -z "${_tapdb_repo_root}" ]; then
    _tapdb_repo_root="$(pwd)"
fi

_tapdb_have_pg_ctl=0
if command -v pg_ctl >/dev/null 2>&1; then
    _tapdb_have_pg_ctl=1
fi

for _tapdb_env in dev test; do
    _tapdb_pg_dir="${_tapdb_repo_root}/postgres_data/${_tapdb_env}"
    if [ -d "$_tapdb_pg_dir" ] && [ -f "${_tapdb_pg_dir}/postmaster.pid" ]; then
        if [ "${_tapdb_have_pg_ctl}" -eq 1 ]; then
            printf "${_tapdb_yellow}►${_tapdb_reset} Stopping local PostgreSQL (${_tapdb_env})...\n"
            pg_ctl -D "$_tapdb_pg_dir" stop -m fast >/dev/null 2>&1
            if [ $? -eq 0 ]; then
                printf "${_tapdb_green}✓${_tapdb_reset} PostgreSQL (${_tapdb_env}) stopped.\n"
            fi
        else
            printf "${_tapdb_yellow}⚠${_tapdb_reset} pg_ctl not found; cannot stop local PostgreSQL (${_tapdb_env}).\n"
        fi
    fi
done

# Remove tab completion
_tapdb_shell="unknown"
if [ -n "${ZSH_VERSION:-}" ]; then
    _tapdb_shell="zsh"
elif [ -n "${BASH_VERSION:-}" ]; then
    _tapdb_shell="bash"
elif [ -n "${SHELL:-}" ]; then
    _tapdb_shell="$(basename "$SHELL")"
fi
if [ "$_tapdb_shell" = "zsh" ]; then
    # Remove zsh completion function if defined
    if type _TAPDB_COMPLETE >/dev/null 2>&1; then
        unfunction _TAPDB_COMPLETE 2>/dev/null
    fi
    if type _tapdb_completion >/dev/null 2>&1; then
        unfunction _tapdb_completion 2>/dev/null
    fi
elif [ "$_tapdb_shell" = "bash" ]; then
    # Remove bash completion
    complete -r tapdb 2>/dev/null
fi
printf "${_tapdb_green}✓${_tapdb_reset} Tab completion removed.\n"

# Deactivate virtual environment
printf "${_tapdb_cyan}►${_tapdb_reset} Deactivating .venv...\n"
if command -v deactivate >/dev/null 2>&1; then
    deactivate
    printf "${_tapdb_green}✓${_tapdb_reset} TAPDB deactivated.\n"
else
    printf "${_tapdb_yellow}⚠${_tapdb_reset} Could not find deactivate() function; your shell may not support venv deactivation here.\n"
fi

printf "${_tapdb_bold}${_tapdb_cyan}━━━━━━━━━━━━━━━━━━━━━━━━━━${_tapdb_reset}\n"

# Cleanup temp vars
unset _tapdb_repo_root _tapdb_shell _tapdb_env _tapdb_pg_dir _tapdb_have_pg_ctl
unset _tapdb_find_repo_root
unset _tapdb_red _tapdb_green _tapdb_cyan _tapdb_yellow _tapdb_bold _tapdb_reset

