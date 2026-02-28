#!/usr/bin/env bash
# clean_data.sh — fast data artifact cleanup using rm -rf + recreate
# Usage: clean_data.sh [runtime|all|repo]
set -euo pipefail

MODE="${1:-runtime}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

# Fast wipe: rm -rf the directory, then recreate it with a .gitkeep
wipe_dir() {
  local dir="$1"
  if [[ -d "$dir" ]]; then
    rm -rf "${dir:?}"
  fi
  mkdir -p "$dir"
  touch "$dir/.gitkeep"
}

# Wipe contents only, keeping the directory itself (for top-level dirs tracked by git)
wipe_contents() {
  local dir="$1"
  if [[ -d "$dir" ]]; then
    # Remove everything inside except the directory itself
    find "$dir" -mindepth 1 -maxdepth 1 ! -name '.gitkeep' \
      -exec rm -rf {} +
  fi
  mkdir -p "$dir"
  [[ -f "$dir/.gitkeep" ]] || touch "$dir/.gitkeep"
}

case "$MODE" in
  runtime)
    echo "Cleaning runtime artifacts (runs, reports, lake/runs, lake/trades, events)..."
    wipe_contents "data/runs"
    wipe_contents "data/reports"
    wipe_contents "data/events"
    if [[ -d "data/lake/runs" ]]; then
      rm -rf data/lake/runs
      mkdir -p data/lake/runs
    fi
    if [[ -d "data/lake/trades" ]]; then
      rm -rf data/lake/trades
      mkdir -p data/lake/trades
    fi
    ;;
  all)
    echo "Cleaning ALL data artifacts..."
    wipe_contents "data/runs"
    wipe_contents "data/reports"
    wipe_contents "data/events"
    for subdir in runs raw cleaned features trades; do
      if [[ -d "data/lake/$subdir" ]]; then
        rm -rf "data/lake/$subdir"
        mkdir -p "data/lake/$subdir"
      fi
    done
    if [[ -d "data/features" ]]; then
      rm -rf data/features
      mkdir -p data/features
    fi
    touch data/.gitkeep data/lake/.gitkeep data/runs/.gitkeep data/reports/.gitkeep
    ;;
  repo)
    echo "Cleaning repo-local runtime and cache artifacts..."
    if [[ -d "project/runs" ]]; then wipe_contents "project/runs"; fi
    for cache_dir in .pytest_cache .mypy_cache .ruff_cache htmlcov; do
      [[ -d "$cache_dir" ]] && rm -rf "$cache_dir" || true
    done
    find . -not -path './.venv/*' -not -path './.git/*' -type f \
      \( -name "*.pyc" -o -name "*.pyo" -o -name "*.tmp" -o -name "*.swp" \
         -o -name ".coverage" -o -name ".coverage.*" -o -name ".DS_Store" \
         -o -name "*:Zone.Identifier" \) \
      -delete
    find . -not -path './.venv/*' -not -path './.git/*' -type d \
      \( -name "__pycache__" -o -name ".ipynb_checkpoints" \) \
      -exec rm -rf {} + 2>/dev/null || true
    ;;
  *)
    echo "Usage: $0 [runtime|all|repo]" >&2
    exit 1
    ;;
esac

echo "Clean completed: mode=$MODE"
