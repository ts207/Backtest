#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ ! -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  echo "Missing ${ROOT_DIR}/.venv/bin/python. Run .codex/setup.sh first." >&2
  exit 1
fi

export BACKTEST_DATA_ROOT="${BACKTEST_DATA_ROOT:-${ROOT_DIR}/data}"
mkdir -p "${BACKTEST_DATA_ROOT}"

echo "Codex maintenance complete."
echo "Repo: ${ROOT_DIR}"
echo "BACKTEST_DATA_ROOT: ${BACKTEST_DATA_ROOT}"
