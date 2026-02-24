# Project Package Guide

`project/` contains runtime code executed by `project/pipelines/run_all.py`.

## Package Layout

- `pipelines/ingest`: raw market ingestion
- `pipelines/clean`: cleaned bar and funding alignment
- `pipelines/features`: `features_v1`, funding persistence context, market state context
- `pipelines/research`: Phase1 analyzers, event registry build, Phase2 discovery, bridge, blueprint compile
- `pipelines/backtest`: strategy/blueprint backtest
- `pipelines/eval`: walkforward evaluation
- `pipelines/report`: report generation
- `events/`: registry normalization and event flag generation
- `engine/`: execution/runtime
- `strategy_dsl/`: blueprint schema and compile/runtime contracts
- `features/`: reusable indicator and event-family feature logic

## Artifact Contracts

All runtime outputs are rooted at `BACKTEST_DATA_ROOT`.

- Stage manifests: `data/runs/<run_id>/`
- Lake raw/cleaned/features: `data/lake/...`
- Registry artifacts: `data/events/<run_id>/`
- Research outputs: `data/reports/<stage>/<run_id>/...`
- Blueprints: `data/reports/strategy_blueprints/<run_id>/blueprints.jsonl`

## Operational Defaults (run_all)

Key defaults that matter:

- `run_phase2_conditional=0`
- `run_backtest=0`
- `run_walkforward_eval=0`
- `run_make_report=0`
- `run_bridge_eval_phase2=1` (when Phase2 is enabled)

So a plain run is data/features/context oriented unless discovery/backtest flags are enabled.

## Integrity Notes

- Context/market-state funding coverage is fail-closed.
- Registry emits both impulse and active flags.
- Phase2 discovery enforces lagged entry and registry-first events.
