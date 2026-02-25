# Project Package Guide

`project/` contains runtime code executed by `project/pipelines/run_all.py`.

## Package Layout

- `pipelines/ingest`: raw data ingestion
- `pipelines/clean`: cleaned bars and alignment
- `pipelines/features`: feature/context builders
- `pipelines/research`: analyzers, registry, phase2, bridge, promotion, compile, builder
- `pipelines/backtest`: backtest execution
- `pipelines/eval`: walkforward evaluation
- `pipelines/report`: reporting
- `events/`: registry normalization and event flag construction
- `strategy_dsl/`: blueprint contracts and schemas

## Runtime Artifact Contracts

All outputs are rooted at `BACKTEST_DATA_ROOT`.

- Run artifacts: `data/runs/<run_id>/`
- Event artifacts: `data/events/<run_id>/`
- Stage reports: `data/reports/<stage>/<run_id>/...`
- Data lake: `data/lake/...`

## Manifest Model

- Run manifest: `data/runs/<run_id>/run_manifest.json`
- Stage manifests: `data/runs/<run_id>/<stage_or_instance>.json`
- Stage logs: `data/runs/<run_id>/<stage_or_instance>.log`

Event-specific stages use stage-instance naming to preserve per-event provenance.
