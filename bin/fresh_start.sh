#!/usr/bin/env bash
# Convenience helper to do a "fresh start" in one shot.
#
# This runs in its own process; it will NOT leave your current shell activated.
# For interactive dev work, prefer:
#   source ./tapdb_activate.sh

set -euo pipefail

_tapdb_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${_tapdb_repo_root}"

source ./tapdb_activate.sh
tapdb pg init dev
tapdb pg start-local dev
tapdb db setup dev
tapdb ui start
