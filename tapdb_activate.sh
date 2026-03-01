#!/usr/bin/env bash
# TAPDB Environment Activation Script (pip + venv)
# Usage (bash/zsh): source ./tapdb_activate.sh [--smoke]
#
# This script:
#   1. Creates a local virtualenv at ./.venv (if needed)
#   2. Activates it for the current shell
#   3. Installs this repo in editable mode with dev+CLI+admin extras (if needed)
#   4. Enables tab completion (bash/zsh)
#   5. Shows helpful next commands
#
# Smoke mode:
#   - Skips pip install
#   - Skips shell completion
#   - Intended for non-interactive CI/smoke checks: `source ./tapdb_activate.sh --smoke`

# Guardrail: this file must be sourced (executing it cannot activate your current shell).
_tapdb__sourced=0
if [ -n "${BASH_VERSION:-}" ]; then
    if [ "${BASH_SOURCE[0]}" != "$0" ]; then
        _tapdb__sourced=1
    fi
elif [ -n "${ZSH_VERSION:-}" ]; then
    case "${ZSH_EVAL_CONTEXT:-}" in
        *:file) _tapdb__sourced=1 ;;
    esac
fi
if [ "${_tapdb__sourced}" -ne 1 ]; then
    printf 'ERROR: tapdb_activate.sh must be sourced (bash/zsh):\n  source ./tapdb_activate.sh\n' >&2
    exit 2
fi
unset _tapdb__sourced

# Colors (POSIX-safe)
_tapdb_red='\033[0;31m'
_tapdb_green='\033[0;32m'
_tapdb_cyan='\033[0;36m'
_tapdb_yellow='\033[0;33m'
_tapdb_bold='\033[1m'
_tapdb_reset='\033[0m'

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

printf "${_tapdb_bold}${_tapdb_cyan}━━━ TAPDB Activation ━━━${_tapdb_reset}\n"

# Flags
_tapdb_smoke="${TAPDB_ACTIVATE_SMOKE:-0}"
for _tapdb_arg in "$@"; do
    if [ "${_tapdb_arg}" = "--smoke" ]; then
        _tapdb_smoke=1
    fi
done

_tapdb_repo_root="$(_tapdb_find_repo_root)"
if [ -z "${_tapdb_repo_root}" ]; then
    printf "${_tapdb_red}✗${_tapdb_reset} Could not locate repo root (pyproject.toml).\n"
    printf "  Run this from the repository (or a subdir): ${_tapdb_cyan}. ./tapdb_activate.sh${_tapdb_reset}\n"
    return 1 2>/dev/null || exit 1
fi

_tapdb_venv_dir="${_tapdb_repo_root}/.venv"

_tapdb_python=""
if [ -n "${TAPDB_PYTHON:-}" ]; then
    _tapdb_python="$TAPDB_PYTHON"
elif command -v python3 >/dev/null 2>&1; then
    _tapdb_python="python3"
elif command -v python >/dev/null 2>&1; then
    _tapdb_python="python"
fi

if [ -z "${_tapdb_python}" ]; then
    printf "${_tapdb_red}✗${_tapdb_reset} No python interpreter found (python3/python).\n"
    return 1 2>/dev/null || exit 1
fi

if [ ! -f "${_tapdb_venv_dir}/bin/activate" ]; then
    printf "${_tapdb_yellow}⚠${_tapdb_reset} .venv not found. Creating...\n"
    "${_tapdb_python}" -m venv "${_tapdb_venv_dir}"
    if [ $? -ne 0 ]; then
        printf "${_tapdb_red}✗${_tapdb_reset} Failed to create virtual environment at ${_tapdb_venv_dir}\n"
        return 1 2>/dev/null || exit 1
    fi
    printf "${_tapdb_green}✓${_tapdb_reset} Virtual environment created.\n"
fi

printf "${_tapdb_cyan}►${_tapdb_reset} Activating .venv...\n"
source "${_tapdb_venv_dir}/bin/activate"
if [ -z "${VIRTUAL_ENV:-}" ]; then
    printf "${_tapdb_red}✗${_tapdb_reset} Failed to activate virtual environment.\n"
    return 1 2>/dev/null || exit 1
fi

# Determine expected tapdb executable from this venv.
# NOTE: `command -v tapdb` can resolve to a shell function/alias or a global
# install, which would cause us to skip installing into the venv.
_tapdb_bin_dir="${VIRTUAL_ENV:-${_tapdb_venv_dir}}/bin"
_tapdb_tapdb="${_tapdb_bin_dir}/tapdb"

# Check if tapdb CLI is installed in *this* venv
if [ ! -x "${_tapdb_tapdb}" ]; then
    if [ "${_tapdb_smoke}" = "1" ]; then
        printf "${_tapdb_yellow}⚠${_tapdb_reset} tapdb CLI not installed (smoke mode: skipping install).\n"
        printf "  Install with: ${_tapdb_cyan}(cd %s && python -m pip install -e \".[cli,admin,aurora,dev]\")${_tapdb_reset}\n" "${_tapdb_repo_root}"
    else
        printf "${_tapdb_yellow}⚠${_tapdb_reset} tapdb CLI not installed. Installing...\n"
        (cd "${_tapdb_repo_root}" && python -m pip install -e ".[cli,admin,aurora,dev]" -q)
        if [ $? -ne 0 ]; then
            printf "${_tapdb_red}✗${_tapdb_reset} Failed to install tapdb.\n"
            return 1 2>/dev/null || exit 1
        fi
        printf "${_tapdb_green}✓${_tapdb_reset} tapdb installed.\n"
    fi
fi

# Enable tab completion for current shell session (interactive shells only)
_tapdb_shell="unknown"
if [ -n "${ZSH_VERSION:-}" ]; then
    _tapdb_shell="zsh"
elif [ -n "${BASH_VERSION:-}" ]; then
    _tapdb_shell="bash"
elif [ -n "${SHELL:-}" ]; then
    _tapdb_shell="$(basename "$SHELL")"
fi
_tapdb_is_interactive=0
case "${-}" in
    *i*) _tapdb_is_interactive=1 ;;
esac

if [ "${_tapdb_smoke}" != "1" ] && [ "${_tapdb_is_interactive}" -eq 1 ] && [ -x "${_tapdb_tapdb}" ]; then
    if [ "${_tapdb_shell}" = "zsh" ]; then
        # In non-interactive zsh shells, compdef/compinit may not be loaded; avoid warnings.
        if ! command -v compdef >/dev/null 2>&1; then
            if command -v autoload >/dev/null 2>&1; then
                autoload -Uz compinit 2>/dev/null || true
                compinit -u 2>/dev/null || true
            fi
        fi
        if command -v compdef >/dev/null 2>&1; then
            eval "$("${_tapdb_tapdb}" --show-completion zsh 2>/dev/null)" || true
        fi
    elif [ "${_tapdb_shell}" = "bash" ]; then
        if command -v complete >/dev/null 2>&1; then
            eval "$("${_tapdb_tapdb}" --show-completion bash 2>/dev/null)" || true
        fi
    fi
fi

# Show status
if [ "${_tapdb_smoke}" = "1" ]; then
    printf "${_tapdb_green}✓${_tapdb_reset} TAPDB venv activated ${_tapdb_cyan}(smoke mode)${_tapdb_reset}\n"
elif [ -x "${_tapdb_tapdb}" ]; then
    printf "${_tapdb_green}✓${_tapdb_reset} TAPDB activated ${_tapdb_cyan}($(${_tapdb_tapdb} version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo 'dev'))${_tapdb_reset}\n"
else
    printf "${_tapdb_green}✓${_tapdb_reset} TAPDB venv activated ${_tapdb_cyan}(tapdb not installed)${_tapdb_reset}\n"
fi
printf "\n"

# Print available commands dynamically
printf "${_tapdb_bold}Available Commands:${_tapdb_reset}\n"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg init <env>" "Initialize local PostgreSQL data dir"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg start-local <env>" "Start local PostgreSQL (dev/test)"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg stop-local <env>" "Stop local PostgreSQL"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb pg start|stop|status" "Manage system PostgreSQL"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb bootstrap local" "One-command local bootstrap"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db create <env>" "Create empty database"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db delete <env>" "Delete database (⚠️ destructive)"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db schema apply <env>" "Apply TAPDB schema"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db data seed <env>" "Seed templates from config/"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db schema status <env>" "Check schema status"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db schema reset <env>" "Drop schema objects (⚠️ destructive)"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb db data backup <env>" "Backup database data"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb cognito setup <env>" "Setup Cognito pool + bind pool-id"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb cognito add-user <env> <email>" "Create Cognito user"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb ui start|stop|status" "Manage admin UI server"
printf "${_tapdb_cyan}%-32s${_tapdb_reset} %s\n" "tapdb --help" "Full help"
printf "\n"
printf "${_tapdb_bold}Environments:${_tapdb_reset} dev | test | prod\n"
printf "${_tapdb_bold}Quick Start:${_tapdb_reset}  export TAPDB_ENV=dev && tapdb bootstrap local\n"
printf "\n"

# Show UI status if running
if [ "${_tapdb_smoke}" != "1" ] && [ -x "${_tapdb_tapdb}" ]; then
    _tapdb_ui_status=$(${_tapdb_tapdb} ui status 2>/dev/null)
    if echo "$_tapdb_ui_status" | grep -q "running"; then
        printf "${_tapdb_green}●${_tapdb_reset} UI Server: ${_tapdb_green}running${_tapdb_reset}\n"
        printf "  URL: ${_tapdb_cyan}https://127.0.0.1:8911${_tapdb_reset}\n"
    else
        printf "${_tapdb_yellow}○${_tapdb_reset} UI Server: not running (start with: ${_tapdb_cyan}tapdb ui start${_tapdb_reset})\n"
    fi
fi

printf "${_tapdb_bold}${_tapdb_cyan}━━━━━━━━━━━━━━━━━━━━━━━━${_tapdb_reset}\n"

# Cleanup temp vars
unset _tapdb_repo_root _tapdb_venv_dir _tapdb_python _tapdb_shell _tapdb_ui_status
unset _tapdb_bin_dir _tapdb_tapdb
unset _tapdb_arg _tapdb_smoke _tapdb_is_interactive
unset _tapdb_find_repo_root
unset _tapdb_red _tapdb_green _tapdb_cyan _tapdb_yellow _tapdb_bold _tapdb_reset
