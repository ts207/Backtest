#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-runtime}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

delete_contents() {
  local dir="$1"
  if [[ -d "$dir" ]]; then
    find "$dir" -mindepth 1 -delete
  fi
}

case "$MODE" in
  runtime)
    delete_contents "data/runs"
    delete_contents "data/reports"
    delete_contents "data/lake/runs"
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
    ;;
  *)
    echo "Usage: $0 [runtime|all]" >&2
    exit 1
    ;;
esac

echo "Clean completed: mode=$MODE"
