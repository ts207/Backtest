#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[smoke] Running fast integration smoke tests"
pytest -q tests/test_run_all_phase2.py tests/test_organize_reports.py tests/test_check_overlay_promotion.py

echo "[smoke] Completed"
