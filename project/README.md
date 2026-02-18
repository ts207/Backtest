# Project Package Guide

`project/` contains the executable runtime used by `project/pipelines/run_all.py`.

## Layout

- `pipelines/ingest`: raw market ingestion
- `pipelines/clean`: cleaned bar/funding datasets
- `pipelines/features`: feature and context construction
- `pipelines/research`: phase-1/phase-2 discovery and strategy preparation
- `pipelines/backtest`: strategy/blueprint execution
- `pipelines/eval`: walkforward split evaluation
- `pipelines/report`: final report generation
- `pipelines/alpha_bundle`: optional parallel signal pipeline
- `engine/`: runtime execution and pnl plumbing
- `strategies/`: strategy implementations, adapters, DSL interpreter
- `strategy_dsl/`: blueprint schema and policy mapping
- `features/`: reusable feature logic

## Data Contract

All runtime artifacts are rooted under `BACKTEST_DATA_ROOT`.

Key locations:
- Run manifests/logs: `data/runs/<run_id>/...`
- Lake raw: `data/lake/raw/...`
- Lake cleaned: `data/lake/cleaned/...`
- Lake features: `data/lake/features/...`
- Research reports: `data/reports/<stage>/<run_id>/...`
- Engine outputs: `data/runs/<run_id>/engine/...`
- Backtest metrics/trades: `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/...`
- Eval outputs: `data/reports/eval/<run_id>/...`
- Promotion outputs: `data/reports/promotions/<run_id>/...`
- Final report outputs: `data/reports/vol_compression_expansion_v1/<run_id>/...`

## Important Orchestration Defaults (`run_all.py`)

- `run_backtest=0`
- `run_walkforward_eval=0`
- `run_make_report=0`
- `run_strategy_blueprint_compiler=1`
- `run_strategy_builder=1`

So discovery runs are not equivalent to full backtest/eval/report runs unless those flags are enabled.

## Minimal Full-Path Enablement

To force end-to-end execution via orchestrator:
```bash
--run_edge_candidate_universe 1 \
--run_backtest 1 \
--run_walkforward_eval 1 \
--run_make_report 1
```

## Strategy DSL Notes

- Blueprints compile to `data/reports/strategy_blueprints/<run_id>/blueprints.jsonl`.
- The DSL interpreter executes trigger/confirmation/condition logic directly.
- Unknown trigger/confirmation names are hard-fail validation errors.

## Testing

```bash
./.venv/bin/pytest -q
```
