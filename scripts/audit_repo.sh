#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[audit] Running test suite"
pytest -q

echo "[audit] Verifying modules compile"
python3 -m compileall -q project tests

echo "[audit] Git working tree summary"
git status --short

echo "[audit] Completed"
