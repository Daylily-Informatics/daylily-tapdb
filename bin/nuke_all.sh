#!/usr/bin/env bash
# TAPDB "reset everything" helper.
#
# Defaults to a LOCAL reset:
#   - stop local TAPDB UI + local Postgres (best-effort)
#   - delete repo-local artifacts (.venv/, postgres_data/, caches, build output)
#   - delete TAPDB user state (~/.tapdb, ~/.config/tapdb)
#   - optionally remove a conda env (prompted)
#   - unset AWS env vars (does NOT delete AWS resources)
#
# Optional FULL deletion (explicit flags + double confirmation required):
#   - remote DB deletion via `tapdb pg delete <env>`
#   - AWS deletions (RDS/Secrets/S3) ONLY for explicit IDs you provide

# This script is intended to run from either bash or zsh.
#
# NOTE: If you invoke this script via `zsh bin/nuke_all.sh`, zsh's default word-splitting
# rules differ from sh/bash. Enable sh-like word splitting so loops over space-separated
# env vars behave consistently.
if [ -n "${ZSH_VERSION:-}" ]; then
  setopt SH_WORD_SPLIT 2>/dev/null || true
fi

set -u

_tapdb_usage() {
  cat <<'USAGE'
Usage:
  bash bin/nuke_all.sh [--local] [--dry-run] [--yes]
  bash bin/nuke_all.sh --full [--dry-run] [--yes]

  # (also works from zsh)
  zsh bin/nuke_all.sh [--local] [--dry-run] [--yes]
  zsh bin/nuke_all.sh --full [--dry-run] [--yes]

Modes:
  --local         Local reset only (default)
  --full          Full deletion: local reset + remote DB deletion + AWS deletion

Full deletion sub-flags (you can also use these without --full):
  --remote-db     Run `tapdb pg delete` for selected envs (default envs: dev test; prod requires explicit opt-in)
  --aws           Delete AWS resources for explicit IDs provided via env vars

Other flags:
  --conda-env NAME   Conda env name to remove (default: TAPDB; prompted)
  --no-conda         Skip conda env removal step entirely
  --dry-run          Print what would happen; do not delete/stop
  --yes              Skip interactive confirmations (NOT recommended for --full)
  --help             Show help

AWS deletion inputs (used when --aws is set):
  TAPDB_AWS_REGION                 (or AWS_REGION/AWS_DEFAULT_REGION)
  TAPDB_AWS_RDS_INSTANCE_IDS       space-separated DB instance identifiers
  TAPDB_AWS_SECRET_IDS             space-separated secret IDs/ARNs
  TAPDB_AWS_S3_BUCKETS             space-separated bucket names
  TAPDB_AWS_SKIP_FINAL_SNAPSHOT=1  if set, skip RDS final snapshots (dangerous)

Remote DB deletion inputs (used when --remote-db is set):
  TAPDB_NUKE_PG_ENVS               space-separated envs (default: dev test; include prod only if you really mean it)

Examples:
  bash bin/nuke_all.sh
  bash bin/nuke_all.sh --dry-run
  bash bin/nuke_all.sh --full --remote-db --aws

USAGE
}

_tapdb_info() { printf '%s\n' "$*"; }
_tapdb_warn() { printf '%s\n' "WARN: $*"; }
_tapdb_err() { printf '%s\n' "ERROR: $*" >&2; }

_tapdb_run() {
  # Usage: _tapdb_run <cmd...>
  if [ "${_tapdb_dry_run}" -eq 1 ]; then
    printf '+ %s\n' "$*"
    return 0
  fi
  "$@"
}

_tapdb_confirm() {
  # POSIX-y confirmation. Returns 0 if confirmed.
  if [ "${_tapdb_dry_run}" -eq 1 ]; then
    return 0
  fi
  if [ "${_tapdb_yes}" -eq 1 ]; then
    return 0
  fi
  printf '%s [y/N]: ' "$1"
  read -r _tapdb_ans || return 1
  case "${_tapdb_ans}" in
    y|Y|yes|YES) return 0 ;;
    *) return 1 ;;
  esac
}

_tapdb_double_confirm() {
  # For very destructive actions.
  if ! _tapdb_confirm "$1"; then
    return 1
  fi
  if ! _tapdb_confirm "SECOND CONFIRMATION: $1"; then
    return 1
  fi
  return 0
}

_tapdb_mode="local"
_tapdb_do_aws=0
_tapdb_do_remote_db=0
_tapdb_dry_run=0
_tapdb_yes=0
_tapdb_do_conda=1
_tapdb_conda_env="TAPDB"

while [ $# -gt 0 ]; do
  case "$1" in
    --local) _tapdb_mode="local" ;;
    --full) _tapdb_mode="full" ;;
    --aws) _tapdb_do_aws=1 ;;
    --remote-db) _tapdb_do_remote_db=1 ;;
    --dry-run) _tapdb_dry_run=1 ;;
    --yes) _tapdb_yes=1 ;;
    --conda-env)
      shift
      _tapdb_conda_env="${1:-}"
      ;;
    --no-conda) _tapdb_do_conda=0 ;;
    --help|-h) _tapdb_usage; exit 0 ;;
    *)
      _tapdb_err "Unknown arg: $1"
      _tapdb_usage
      exit 2
      ;;
  esac
  shift
done

if [ "${_tapdb_mode}" = "full" ]; then
  # full implies both, but still gated behind confirmations and required inputs
  _tapdb_do_aws=1
  _tapdb_do_remote_db=1
fi

_tapdb_repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "${_tapdb_repo_root}" || exit 1

if command -v git >/dev/null 2>&1; then
  _tapdb_git_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  if [ -n "${_tapdb_git_root}" ] && [ "${_tapdb_git_root}" != "${_tapdb_repo_root}" ]; then
    _tapdb_err "Refusing to run outside repo root."
    _tapdb_err "  script root: ${_tapdb_repo_root}"
    _tapdb_err "  git root:    ${_tapdb_git_root}"
    exit 2
  fi
fi

_tapdb_info "TAPDB reset: mode=${_tapdb_mode} (repo: ${_tapdb_repo_root})"

if [ "${_tapdb_do_remote_db}" -eq 1 ] || [ "${_tapdb_do_aws}" -eq 1 ]; then
  _tapdb_warn "FULL deletion can destroy remote infrastructure/data."
  if ! _tapdb_double_confirm "Proceed with destructive actions beyond local filesystem?"; then
    _tapdb_err "Aborted."
    exit 1
  fi
fi

# --- Stop services (best-effort) ---
if command -v tapdb >/dev/null 2>&1; then
  _tapdb_run tapdb ui stop || true
  _tapdb_run tapdb pg stop-local dev || true
  _tapdb_run tapdb pg stop-local test || true
else
  # Fallback: stop local PG clusters if pg_ctl is available.
  if command -v pg_ctl >/dev/null 2>&1; then
    for _tapdb_env in dev test; do
      _tapdb_pg_dir="${_tapdb_repo_root}/postgres_data/${_tapdb_env}"
      if [ -f "${_tapdb_pg_dir}/postmaster.pid" ]; then
        _tapdb_info "Stopping local PostgreSQL (${_tapdb_env}) via pg_ctl..."
        _tapdb_run pg_ctl -D "${_tapdb_pg_dir}" stop -m fast || true
      fi
    done
  fi
fi

# --- Local filesystem reset ---
if ! _tapdb_confirm "Delete local TAPDB files (.venv/, postgres_data/, caches, build output, ~/.tapdb, ~/.config/tapdb)?"; then
  _tapdb_err "Aborted."
  exit 1
fi

_tapdb_run rm -rf .venv postgres_data .pytest_cache .mypy_cache .ruff_cache build dist .coverage htmlcov
_tapdb_run find . -maxdepth 1 -name "*.egg-info" -prune -exec rm -rf {} +
_tapdb_run rm -rf "$HOME/.tapdb" "$HOME/.config/tapdb"

# --- Conda env removal (optional) ---
if [ "${_tapdb_do_conda}" -eq 1 ] && [ -n "${_tapdb_conda_env}" ]; then
  if command -v conda >/dev/null 2>&1; then
    if _tapdb_confirm "Remove conda env '${_tapdb_conda_env}'?"; then
      _tapdb_run conda env remove -y -n "${_tapdb_conda_env}" || true
    fi
  fi
fi

# --- Remote DB deletion (optional) ---
if [ "${_tapdb_do_remote_db}" -eq 1 ]; then
  _tapdb_pg_envs="${TAPDB_NUKE_PG_ENVS:-dev test}"
  if ! command -v tapdb >/dev/null 2>&1; then
    _tapdb_err "tapdb CLI not found; cannot run remote DB deletion."
    _tapdb_err "Activate your venv first: source ./tapdb_activate.sh"
    exit 1
  fi
  for _tapdb_env in ${_tapdb_pg_envs}; do
    if [ "${_tapdb_env}" = "prod" ]; then
      _tapdb_warn "PRODUCTION ENV SELECTED"
      if ! _tapdb_double_confirm "Run 'tapdb pg delete prod'?"; then
        _tapdb_warn "Skipping prod."
        continue
      fi
    fi
    _tapdb_warn "About to run: tapdb pg delete ${_tapdb_env}"
    if _tapdb_confirm "Delete configured database for env '${_tapdb_env}' via tapdb pg delete?"; then
      _tapdb_run tapdb pg delete "${_tapdb_env}"
    fi
  done
fi

# --- AWS deletion (optional) ---
if [ "${_tapdb_do_aws}" -eq 1 ]; then
  if ! command -v aws >/dev/null 2>&1; then
    _tapdb_err "aws CLI not found; cannot delete AWS resources."
    exit 1
  fi

  _tapdb_region="${TAPDB_AWS_REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}"
  if [ -z "${_tapdb_region}" ]; then
    _tapdb_err "Set TAPDB_AWS_REGION (or AWS_REGION/AWS_DEFAULT_REGION) before --aws."
    exit 1
  fi

  _tapdb_warn "AWS deletion is disabled unless you provide explicit IDs via env vars."
  _tapdb_warn "Region: ${_tapdb_region}"
  if ! _tapdb_double_confirm "Delete AWS resources in region ${_tapdb_region} for the explicit IDs you provided?"; then
    _tapdb_err "Aborted AWS deletion."
  else
    _tapdb_rds_ids="${TAPDB_AWS_RDS_INSTANCE_IDS:-}"
    _tapdb_secret_ids="${TAPDB_AWS_SECRET_IDS:-}"
    _tapdb_s3_buckets="${TAPDB_AWS_S3_BUCKETS:-}"
    _tapdb_skip_snapshot="${TAPDB_AWS_SKIP_FINAL_SNAPSHOT:-0}"

    if [ -n "${_tapdb_rds_ids}" ]; then
      for _tapdb_id in ${_tapdb_rds_ids}; do
        if [ "${_tapdb_skip_snapshot}" = "1" ]; then
          _tapdb_run aws --region "${_tapdb_region}" rds delete-db-instance \
            --db-instance-identifier "${_tapdb_id}" \
            --skip-final-snapshot \
            --delete-automated-backups
        else
          _tapdb_snap="tapdb-nuke-${_tapdb_id}-$(date -u +%Y%m%d%H%M%S)"
          _tapdb_run aws --region "${_tapdb_region}" rds delete-db-instance \
            --db-instance-identifier "${_tapdb_id}" \
            --final-db-snapshot-identifier "${_tapdb_snap}" \
            --delete-automated-backups
        fi
      done
    fi

    if [ -n "${_tapdb_secret_ids}" ]; then
      _tapdb_days="${TAPDB_AWS_SECRET_RECOVERY_DAYS:-7}"
      for _tapdb_id in ${_tapdb_secret_ids}; do
        _tapdb_run aws --region "${_tapdb_region}" secretsmanager delete-secret \
          --secret-id "${_tapdb_id}" \
          --recovery-window-in-days "${_tapdb_days}"
      done
    fi

    if [ -n "${_tapdb_s3_buckets}" ]; then
      for _tapdb_b in ${_tapdb_s3_buckets}; do
        _tapdb_run aws --region "${_tapdb_region}" s3 rm "s3://${_tapdb_b}" --recursive
        _tapdb_run aws --region "${_tapdb_region}" s3api delete-bucket --bucket "${_tapdb_b}"
      done
    fi
  fi
fi

# Unset AWS env vars (always; this does NOT delete AWS resources)
unset AWS_PROFILE AWS_DEFAULT_REGION AWS_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

_tapdb_info "Done."
