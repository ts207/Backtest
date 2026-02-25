# Workflow

## Development Rules

1. Spec-first changes: update `spec/` contracts before implementation when behavior changes.
2. Test-first for new behavior and regressions.
3. Verify with evidence before completion claims.
4. Preserve PIT and manifest integrity invariants.

## Standard Sequence

1. Define scope and contracts.
2. Add/adjust tests.
3. Implement minimal changes.
4. Run verification commands.
5. Document behavior/contract updates.

## Required Verification

- `make check-hygiene`
- `make test-fast`
- targeted tests for touched modules

## Operational Guidance

- Use repo-local Python: `./.venv/bin/python`
- Export `BACKTEST_DATA_ROOT=$(pwd)/data`
- Avoid committing runtime artifacts under `data/runs` and `data/reports`.
