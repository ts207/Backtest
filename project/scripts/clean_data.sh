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

delete_dir() {
  local dir="$1"
  if [[ -d "$dir" ]]; then
    find "$dir" -mindepth 1 -delete
    rmdir "$dir" 2>/dev/null || true
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
    delete_dir ".pytest_cache"
    delete_dir ".mypy_cache"
    delete_dir ".ruff_cache"
    delete_dir "htmlcov"
    find . -type f \
      \( -name "*.pyc" -o -name "*.pyo" -o -name "*.tmp" -o -name "*.swp" \
         -o -name ".coverage" -o -name ".coverage.*" -o -name ".DS_Store" \) \
      -delete
    while IFS= read -r cache_dir; do
      find "$cache_dir" -mindepth 1 -delete
      rmdir "$cache_dir" 2>/dev/null || true
    done < <(find . -type d \( -name "__pycache__" -o -name ".ipynb_checkpoints" \))
    ;;
  *)
    echo "Usage: $0 [runtime|all|repo]" >&2
    exit 1
    ;;
esac

echo "Clean completed: mode=$MODE"
