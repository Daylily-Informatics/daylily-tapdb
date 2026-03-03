#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "error: not inside a git repository" >&2
  exit 1
fi

latest_tag="$(
  git for-each-ref refs/tags \
    --sort=-version:refname \
    --format='%(refname:strip=2)' \
  | grep -E '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z]+)?$' \
  | head -n 1
)"

if [[ -z "$latest_tag" ]]; then
  echo "error: no semantic version tags found (expected tags like 0.1.17)" >&2
  exit 1
fi

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/tapdb-release-build.XXXXXX")"
cleanup() {
  git worktree remove --force "$tmp_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Preparing detached worktree for tag: $latest_tag"
git worktree add --detach "$tmp_dir" "$latest_tag" >/dev/null

mkdir -p "$tmp_dir/dist"
find "$tmp_dir/dist" -mindepth 1 -maxdepth 1 -exec rm -rf {} +

echo "Building release artifacts for tag: $latest_tag"
(
  cd "$tmp_dir"
  python -m build "$@"
)

mkdir -p dist
find dist -mindepth 1 -maxdepth 1 -exec rm -rf {} +
cp "$tmp_dir"/dist/* dist/

shopt -s nullglob
artifacts=(dist/*)
if (( ${#artifacts[@]} == 0 )); then
  echo "error: build completed but no artifacts were copied to dist/" >&2
  exit 1
fi

echo "Built artifacts:"
for artifact in "${artifacts[@]}"; do
  echo "  - $artifact"
done

tag_named_artifacts=(dist/*"$latest_tag"*)
if (( ${#tag_named_artifacts[@]} == 0 )); then
  cat >&2 <<EOF
warning: artifact filenames do not include tag $latest_tag.
This can happen when historical tags resolve to a different version under
setuptools_scm (for example, lightweight vs annotated tag behavior).
EOF
fi
