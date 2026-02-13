#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

python3 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt

# Default to in-repo data path unless explicitly overridden.
export BACKTEST_DATA_ROOT="${BACKTEST_DATA_ROOT:-${ROOT_DIR}/data}"
mkdir -p "${BACKTEST_DATA_ROOT}"

echo "Codex setup complete."
echo "Repo: ${ROOT_DIR}"
echo "Python: ${ROOT_DIR}/.venv/bin/python"
echo "BACKTEST_DATA_ROOT: ${BACKTEST_DATA_ROOT}"
