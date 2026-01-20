#!/bin/sh
# TAPDB Environment Activation Script
# Usage: source tapdb_activate.sh
#
# This script:
#   1. Creates the TAPDB conda environment if it doesn't exist
#   2. Activates the environment
#   3. Installs the package if needed
#   4. Enables tab completion
#   5. Shows available CLI commands

# Colors (POSIX-safe)
_tapdb_red='\033[0;31m'
_tapdb_green='\033[0;32m'
_tapdb_cyan='\033[0;36m'
_tapdb_yellow='\033[0;33m'
_tapdb_bold='\033[1m'
_tapdb_reset='\033[0m'

# Find repo root (where this script lives)
_tapdb_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

printf "${_tapdb_bold}${_tapdb_cyan}━━━ TAPDB Activation ━━━${_tapdb_reset}\n"

# Check if conda is available
if ! command -v conda >/dev/null 2>&1; then
    printf "${_tapdb_red}✗${_tapdb_reset} conda not found. Please install miniconda/anaconda first.\n"
    return 1 2>/dev/null || exit 1
fi

# Check if TAPDB environment exists
if ! conda env list | grep -q "^TAPDB "; then
    printf "${_tapdb_yellow}⚠${_tapdb_reset} TAPDB environment not found. Creating...\n"
    
    if [ -f "${_tapdb_script_dir}/tapdb_env.yaml" ]; then
        conda env create -n TAPDB -f "${_tapdb_script_dir}/tapdb_env.yaml"
        if [ $? -ne 0 ]; then
            printf "${_tapdb_red}✗${_tapdb_reset} Failed to create environment.\n"
            return 1 2>/dev/null || exit 1
        fi
        printf "${_tapdb_green}✓${_tapdb_reset} Environment created.\n"
    else
        printf "${_tapdb_red}✗${_tapdb_reset} tapdb_env.yaml not found in ${_tapdb_script_dir}\n"
        return 1 2>/dev/null || exit 1
    fi
fi

# Activate environment
printf "${_tapdb_cyan}►${_tapdb_reset} Activating TAPDB environment...\n"
eval "$(conda shell.$(basename "$SHELL") activate TAPDB)"

if [ $? -ne 0 ]; then
    printf "${_tapdb_red}✗${_tapdb_reset} Failed to activate environment.\n"
    return 1 2>/dev/null || exit 1
fi

# Check if tapdb CLI is installed
if ! command -v tapdb >/dev/null 2>&1; then
    printf "${_tapdb_yellow}⚠${_tapdb_reset} tapdb CLI not installed. Installing...\n"
    pip install -e "${_tapdb_script_dir}" -q
    if [ $? -ne 0 ]; then
        printf "${_tapdb_red}✗${_tapdb_reset} Failed to install tapdb.\n"
        return 1 2>/dev/null || exit 1
    fi
    printf "${_tapdb_green}✓${_tapdb_reset} tapdb installed.\n"
fi

# Enable tab completion for current shell session
_tapdb_shell="$(basename "$SHELL")"
if [ "$_tapdb_shell" = "zsh" ]; then
    eval "$(tapdb --show-completion zsh 2>/dev/null)"
elif [ "$_tapdb_shell" = "bash" ]; then
    eval "$(tapdb --show-completion bash 2>/dev/null)"
fi

# Show status
printf "${_tapdb_green}✓${_tapdb_reset} TAPDB activated ${_tapdb_cyan}($(tapdb version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo 'dev'))${_tapdb_reset}\n"
printf "\n"

# Print available commands dynamically
printf "${_tapdb_bold}Available Commands:${_tapdb_reset}\n"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg init <env>" "Initialize local PostgreSQL data dir"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg start-local <env>" "Start local PostgreSQL (dev/test)"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg stop-local <env>" "Stop local PostgreSQL"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg create <env>" "Create empty database"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg delete <env>" "Delete database (⚠️ destructive)"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg start|stop|status" "Manage system PostgreSQL"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db create <env>" "Initialize TAPDB schema"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db seed <env>" "Seed templates from config/"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db setup <env>" "Full setup (create+schema+seed)"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db status <env>" "Check schema status"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db nuke <env>" "Drop all tables (⚠️ destructive)"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db backup <env>" "Backup database"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb ui start|stop|status" "Manage admin UI server"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb --help" "Full help"
printf "\n"
printf "${_tapdb_bold}Environments:${_tapdb_reset} dev | test | prod\n"
printf "${_tapdb_bold}Quick Start:${_tapdb_reset}  tapdb pg init dev && tapdb pg start-local dev && tapdb db setup dev\n"
printf "\n"

# Show UI status if running
_tapdb_ui_status=$(tapdb ui status 2>/dev/null)
if echo "$_tapdb_ui_status" | grep -q "running"; then
    printf "${_tapdb_green}●${_tapdb_reset} UI Server: ${_tapdb_green}running${_tapdb_reset}\n"
    printf "  URL: ${_tapdb_cyan}http://127.0.0.1:8000${_tapdb_reset}\n"
else
    printf "${_tapdb_yellow}○${_tapdb_reset} UI Server: not running (start with: ${_tapdb_cyan}tapdb ui start${_tapdb_reset})\n"
fi

printf "${_tapdb_bold}${_tapdb_cyan}━━━━━━━━━━━━━━━━━━━━━━━━${_tapdb_reset}\n"

# Cleanup temp vars
unset _tapdb_script_dir _tapdb_shell _tapdb_ui_status
unset _tapdb_red _tapdb_green _tapdb_cyan _tapdb_yellow _tapdb_bold _tapdb_reset

