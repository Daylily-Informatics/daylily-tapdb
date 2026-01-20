#!/bin/sh
# TAPDB Environment Deactivation Script
# Usage: source tapdb_deactivate.sh
#
# This script:
#   1. Stops any running local PostgreSQL instances
#   2. Removes tab completion
#   3. Deactivates the conda environment

# Colors (POSIX-safe)
_tapdb_red='\033[0;31m'
_tapdb_green='\033[0;32m'
_tapdb_cyan='\033[0;36m'
_tapdb_yellow='\033[0;33m'
_tapdb_bold='\033[1m'
_tapdb_reset='\033[0m'

printf "${_tapdb_bold}${_tapdb_cyan}━━━ TAPDB Deactivation ━━━${_tapdb_reset}\n"

# Check if we're in the TAPDB environment
if [ "$CONDA_DEFAULT_ENV" != "TAPDB" ]; then
    printf "${_tapdb_yellow}⚠${_tapdb_reset} TAPDB environment is not active.\n"
    unset _tapdb_red _tapdb_green _tapdb_cyan _tapdb_yellow _tapdb_bold _tapdb_reset
    return 0 2>/dev/null || exit 0
fi

# Stop local PostgreSQL instances if running
_tapdb_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
for _tapdb_env in dev test; do
    _tapdb_pg_dir="${_tapdb_script_dir}/postgres_data/${_tapdb_env}"
    if [ -d "$_tapdb_pg_dir" ] && [ -f "${_tapdb_pg_dir}/postmaster.pid" ]; then
        printf "${_tapdb_yellow}►${_tapdb_reset} Stopping local PostgreSQL (${_tapdb_env})...\n"
        pg_ctl -D "$_tapdb_pg_dir" stop -m fast >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            printf "${_tapdb_green}✓${_tapdb_reset} PostgreSQL (${_tapdb_env}) stopped.\n"
        fi
    fi
done

# Remove tab completion
_tapdb_shell="$(basename "$SHELL")"
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

# Deactivate conda environment
printf "${_tapdb_cyan}►${_tapdb_reset} Deactivating TAPDB environment...\n"
conda deactivate

if [ $? -eq 0 ]; then
    printf "${_tapdb_green}✓${_tapdb_reset} TAPDB deactivated.\n"
else
    printf "${_tapdb_red}✗${_tapdb_reset} Failed to deactivate environment.\n"
fi

printf "${_tapdb_bold}${_tapdb_cyan}━━━━━━━━━━━━━━━━━━━━━━━━━━${_tapdb_reset}\n"

# Cleanup temp vars
unset _tapdb_script_dir _tapdb_shell _tapdb_env _tapdb_pg_dir
unset _tapdb_red _tapdb_green _tapdb_cyan _tapdb_yellow _tapdb_bold _tapdb_reset

