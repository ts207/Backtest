#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $0 --run_id RID [--workflow core|research|full] [--event_type EVENT] [--expect_promoted_audits 0|1]
USAGE
}

RUN_ID=""
WORKFLOW="core"
EVENT_TYPE="vol_shock_relaxation"
EXPECT_PROMOTED_AUDITS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run_id) RUN_ID="$2"; shift 2;;
    --workflow) WORKFLOW="$2"; shift 2;;
    --event_type) EVENT_TYPE="$2"; shift 2;;
    --expect_promoted_audits) EXPECT_PROMOTED_AUDITS="$2"; shift 2;;
    *) echo "Unknown argument: $1"; usage; exit 2;;
  esac
done

if [[ -z "$RUN_ID" ]]; then
  usage
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DATA_ROOT="${BACKTEST_DATA_ROOT:-$ROOT_DIR/data}"

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "[contract] missing file: $path" >&2
    return 1
  fi
}

require_any_file() {
  local a="$1"
  local b="$2"
  if [[ ! -f "$a" && ! -f "$b" ]]; then
    echo "[contract] missing both candidate files:" >&2
    echo "  - $a" >&2
    echo "  - $b" >&2
    return 1
  fi
}

require_dir() {
  local path="$1"
  if [[ ! -d "$path" ]]; then
    echo "[contract] missing directory: $path" >&2
    return 1
  fi
}

# Core artifact contract
if [[ "$WORKFLOW" == "core" || "$WORKFLOW" == "full" ]]; then
  require_file "$DATA_ROOT/runs/$RUN_ID/engine/metrics.json"
  require_any_file \
    "$DATA_ROOT/reports/vol_compression_expansion_v1/$RUN_ID/summary.json" \
    "$DATA_ROOT/reports/by_run/$RUN_ID/backtest/vol_compression_expansion_v1/summary.json"
fi

# Research artifact contract
if [[ "$WORKFLOW" == "research" || "$WORKFLOW" == "full" ]]; then
  require_file "$DATA_ROOT/reports/$EVENT_TYPE/$RUN_ID/${EVENT_TYPE}_summary.json"
  require_file "$DATA_ROOT/reports/phase2/$RUN_ID/$EVENT_TYPE/phase2_candidates.csv"
  require_file "$DATA_ROOT/reports/phase2/$RUN_ID/$EVENT_TYPE/promoted_candidates.json"
  if [[ "$EXPECT_PROMOTED_AUDITS" == "1" ]]; then
    require_dir "$DATA_ROOT/reports/promotion_audits/$RUN_ID/$EVENT_TYPE"
  fi
fi

echo "[contract] verification passed for run_id=$RUN_ID workflow=$WORKFLOW"
