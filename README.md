# Backtest Repository Guide

## Environment
- Create a Python 3.10+ virtualenv in the repo root.
- Install runtime/dev deps: `./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt`.
- Point `BACKTEST_DATA_ROOT` at a writable data directory before running any pipeline (e.g., `export BACKTEST_DATA_ROOT=/abs/path/to/data`).

## Canonical Discovery Flow
1. `./.venv/bin/python project/pipelines/run_all.py` orchestrates ingest → clean → `features_v1` → context features → phase1 → phase2 → edge export → expectancy + robustness → strategy builder.
2. Pass the flags your run needs: `--run_id`, `--symbols BTCUSDT,ETHUSDT`, `--start`, `--end`, `--run_phase2_conditional 1`, `--phase2_event_type all`, `--run_strategy_builder 1`, etc.
3. Use `--skip_ingest_spot_ohlcv 1` if cross-venue spot data already exists; `cross_venue_desync` automatically pulls spot data otherwise.

## Outputs
- Canonical numeric artifacts land under `data/reports/…` (phase1, phase2, edge candidates, strategy builder, run tracking).
- Promoted edge list: `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`.
- Strategy outputs: `data/reports/strategy_builder/<run_id>/strategy_candidates.json` plus markdown summaries.
- Manual backtests use template IDs from the strategy candidates list and run with `project/pipelines/backtest/backtest_strategies.py --run_id … --symbols … --strategies <template> --force 1`.

## Reporting & Allocation
- The latest report stage writes `reports/vol_compression_expansion_v1/<run_id>/allocation_weights.{csv,json}` via `project/pipelines/report/capital_allocation.py`.
- Tests for the report stage live in `tests/test_capital_allocation.py`.

## Testing
- Run `./.venv/bin/pytest -q` to execute the current unit test suite.

## Docs
- Use the `docs/` files for stage-level explanations: `docs/discovery_pipeline.md` describes how phase1 → phase2 → strategy outputs are produced, and `docs/operator_runbook.md` walks through the standard orchestration commands plus manual backtest handoff. Treat those docs as the current source of truth and keep them synchronized with the pipeline code when changes land.
## Notes
- Ignore any markdown that references retired workflows; prefer the overview above combined with the docs and pipeline scripts in `project/pipelines/`.
