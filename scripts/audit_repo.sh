#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[audit] Checking for unresolved merge conflicts"
if git diff --name-only --diff-filter=U | grep -q .; then
  echo "[audit] Unmerged paths detected" >&2
  git diff --name-only --diff-filter=U >&2
  exit 1
fi
if rg -n "^(<<<<<<<|=======|>>>>>>>)" README.md docs project tests scripts Makefile >/tmp/merge_markers.txt; then
  echo "[audit] Conflict markers detected:" >&2
  cat /tmp/merge_markers.txt >&2
  exit 1
fi

echo "[audit] Running test suite"
pytest -q

echo "[audit] Verifying modules compile"
python3 -m compileall -q project tests

echo "[audit] Scanning for TODO/FIXME/HACK markers"
rg -n "TODO|FIXME|HACK|XXX" project tests docs || true

if [[ "${RUN_SMOKE:-1}" == "1" ]]; then
  echo "[audit] Running smoke matrix"
  bash scripts/smoke.sh
fi

echo "[audit] Git working tree summary"
git status --short

echo "[audit] Completed"
