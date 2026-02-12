#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-runtime}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

delete_contents() {
  local dir="$1"
  if [[ -d "$dir" ]]; then
    find "$dir" -mindepth 1 ! -name '.gitkeep' -delete
  fi
}

ensure_gitkeep() {
  local dir="$1"
  mkdir -p "$dir"
  if [[ ! -f "$dir/.gitkeep" ]]; then
    : > "$dir/.gitkeep"
  fi
}

case "$MODE" in
  runtime)
    delete_contents "data/runs"
    delete_contents "data/reports"
    delete_contents "data/lake/runs"
    ensure_gitkeep "data/runs"
    ensure_gitkeep "data/reports"
    ensure_gitkeep "data/lake"
    ;;
  all)
    delete_contents "data/runs"
    delete_contents "data/reports"
    delete_contents "data/lake/runs"
    delete_contents "data/lake/raw"
    delete_contents "data/lake/cleaned"
    delete_contents "data/lake/features"
    delete_contents "data/lake/trades"
    delete_contents "data/features"
    ensure_gitkeep "data"
    ensure_gitkeep "data/lake"
    ensure_gitkeep "data/runs"
    ensure_gitkeep "data/reports"
    ;;
  repo)
    # Repo-local runtime and cache cleanup.
    delete_contents "project/runs"
    find . -type f -name "*.pyc" -delete
    find . -type d -name "__pycache__" -empty -delete
    find project -type d -empty -delete
    delete_contents ".pytest_cache"
    ;;
  *)
    echo "Usage: $0 [runtime|all|repo]" >&2
    exit 1
    ;;
esac

echo "Clean completed: mode=$MODE"
