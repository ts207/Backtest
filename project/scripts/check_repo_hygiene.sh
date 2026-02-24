#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

MAX_BYTES=$((5 * 1024 * 1024))
if [[ -n "${MAX_TRACKED_FILE_BYTES:-}" ]]; then
  MAX_BYTES="${MAX_TRACKED_FILE_BYTES}"
fi

fail=0

echo "[hygiene] checking forbidden tracked path patterns..."
present_tracked="$(mktemp)"
git ls-files | while IFS= read -r path; do
  if [[ -f "$path" ]]; then
    echo "$path"
  fi
done >"$present_tracked"

blocked_patterns=(
  '^data/reports/.+'
  '^data/runs/.+'
  '^data/lake/(cleaned|features|runs)/.+'
  '^debug\.log$'
  '^debug.*\.log$'
  '^debug.*\.txt$'
  '^diag_out\.txt$'
  '^ingest_run\.log$'
  '^nohup\.out$'
)
for pattern in "${blocked_patterns[@]}"; do
  if rg -n "$pattern" "$present_tracked" >/tmp/hygiene_blocked.txt; then
    rg -n '\.gitkeep$' /tmp/hygiene_blocked.txt >/dev/null || true
    filtered="$(mktemp)"
    rg -v '\.gitkeep$' /tmp/hygiene_blocked.txt >"$filtered" || true
    if [[ -s "$filtered" ]]; then
      echo "[hygiene] blocked tracked files matched pattern: $pattern"
      cat "$filtered"
      fail=1
    fi
    rm -f "$filtered"
  fi
done

rm -f /tmp/hygiene_blocked.txt "$present_tracked"

echo "[hygiene] checking sidecar metadata files..."
if find . -type f \( -name '*:Zone.Identifier' -o -name '*#Uf03aZone.Identifier' -o -name '*#Uf03aZone.Identifier:Zone.Identifier' \) | sed 's#^\./##' | tee /tmp/hygiene_sidecars.txt | rg -q '.'; then
  echo "[hygiene] sidecar metadata files detected"
  fail=1
fi

rm -f /tmp/hygiene_sidecars.txt

echo "[hygiene] checking tracked file size limits..."
while IFS= read -r path; do
  [[ -f "$path" ]] || continue
  bytes=$(wc -c <"$path")
  if [[ "$bytes" -gt "$MAX_BYTES" ]]; then
    echo "[hygiene] tracked file exceeds max size (${MAX_BYTES} bytes): $path ($bytes bytes)"
    fail=1
  fi
done < <(git ls-files)

if [[ "$fail" -ne 0 ]]; then
  echo "[hygiene] FAILED"
  exit 1
fi

echo "[hygiene] OK"
