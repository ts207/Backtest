#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage (preferred positional):
  $0 <run_id> <workflow: core|research|full> [phase2_event_type] [--expect-audits]

Flag form (backward compatible):
  $0 --run_id RID --workflow core|research|full [--event_type EVENT] [--expect_promoted_audits 0|1]
USAGE
}

RUN_ID=""
WORKFLOW="core"
EVENT_TYPE="vol_shock_relaxation"
EXPECT_AUDITS=0

if [[ $# -gt 0 && "$1" != --* ]]; then
  RUN_ID="${1:-}"; shift || true
  WORKFLOW="${1:-core}"; shift || true
  if [[ $# -gt 0 && "$1" != --* ]]; then
    EVENT_TYPE="$1"; shift
  fi
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run_id) RUN_ID="$2"; shift 2 ;;
    --workflow) WORKFLOW="$2"; shift 2 ;;
    --event_type) EVENT_TYPE="$2"; shift 2 ;;
    --expect_promoted_audits)
      EXPECT_AUDITS="$2"; shift 2 ;;
    --expect-audits)
      EXPECT_AUDITS=1; shift ;;
    *) echo "Unknown argument: $1"; usage; exit 2 ;;
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
  [[ -f "$path" ]] || { echo "[contract] missing file: $path" >&2; return 1; }
}

require_any_file() {
  local a="$1"
  local b="$2"
  [[ -f "$a" || -f "$b" ]] || {
    echo "[contract] missing both candidate files:" >&2
    echo "  - $a" >&2
    echo "  - $b" >&2
    return 1
  }
}

require_parquet_under() {
  local dir="$1"
  [[ -d "$dir" ]] || { echo "[contract] missing directory: $dir" >&2; return 1; }
  local found
  found="$(find "$dir" -type f -name '*.parquet' -print -quit)"
  [[ -n "$found" ]] || { echo "[contract] no parquet found under: $dir" >&2; return 1; }
}

IFS=',' read -r -a symbols <<< "${SYMBOLS_OVERRIDE:-BTCUSDT,ETHUSDT}"
meta_path="$DATA_ROOT/runs/$RUN_ID/run_meta.json"
if [[ -f "$meta_path" ]]; then
  symbols_csv="$(python3 - <<PY
import json
from pathlib import Path
p=Path('$meta_path')
obj=json.loads(p.read_text())
print(obj.get('symbols',''))
PY
)"
  if [[ -n "$symbols_csv" ]]; then
    IFS=',' read -r -a symbols <<< "$symbols_csv"
  fi
fi

for i in "${!symbols[@]}"; do
  symbols[$i]="$(echo "${symbols[$i]}" | xargs)"
done

if [[ "$WORKFLOW" == "core" || "$WORKFLOW" == "research" || "$WORKFLOW" == "full" ]]; then
  for sym in "${symbols[@]}"; do
    [[ -n "$sym" ]] || continue
    require_parquet_under "$DATA_ROOT/lake/runs/$RUN_ID/cleaned/perp/$sym/bars_15m"
    require_parquet_under "$DATA_ROOT/lake/runs/$RUN_ID/features/perp/$sym/15m/features_v1"
    require_parquet_under "$DATA_ROOT/lake/runs/$RUN_ID/context/funding_persistence/$sym"
  done
fi

if [[ "$WORKFLOW" == "core" || "$WORKFLOW" == "full" ]]; then
  require_file "$DATA_ROOT/runs/$RUN_ID/engine/metrics.json"
  require_any_file \
    "$DATA_ROOT/reports/vol_compression_expansion_v1/$RUN_ID/summary.md" \
    "$DATA_ROOT/reports/by_run/$RUN_ID/backtest/vol_compression_expansion_v1/summary.md"
fi

if [[ "$WORKFLOW" == "research" || "$WORKFLOW" == "full" ]]; then
  require_file "$DATA_ROOT/reports/$EVENT_TYPE/$RUN_ID/${EVENT_TYPE}_events.csv"
  require_file "$DATA_ROOT/reports/phase2/$RUN_ID/$EVENT_TYPE/promoted_candidates.json"
  if [[ "$EXPECT_AUDITS" == "1" ]]; then
    [[ -d "$DATA_ROOT/reports/promotion_audits/$RUN_ID/$EVENT_TYPE" ]] || { echo "[contract] missing directory: $DATA_ROOT/reports/promotion_audits/$RUN_ID/$EVENT_TYPE" >&2; exit 1; }
  fi
fi

echo "[contract] verification passed for run_id=$RUN_ID workflow=$WORKFLOW event=$EVENT_TYPE"
