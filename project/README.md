# Project Package

This package contains the discovery-core runtime used by `project/pipelines/run_all.py`.

Active areas:
- `pipelines/ingest`
- `pipelines/clean`
- `pipelines/features`
- `pipelines/research`
- `pipelines/backtest`
- `pipelines/report`
- `pipelines/alpha_bundle` (experimental)
- `pipelines/_lib`
- `features/`
- `engine/`
- `strategies/`

Runtime data contract:
- root: `data/`
- manifests/logs: `data/runs/<run_id>/...`
- research outputs: `data/reports/<stage>/<run_id>/...`
- backtest outputs: `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/...`
- backtest reports: `data/reports/vol_compression_expansion_v1/<run_id>/...`
- alpha bundle artifacts: `data/feature_store/{signals,cross_section,regimes,alpha_bundle,...}`
- model registry artifacts: `data/model_registry/CombModelRidge_*.json`

Backtesting is optional and can be enabled from `project/pipelines/run_all.py` with:
- `--run_backtest 1`
- `--run_make_report 1`
- `--strategies <comma-separated ids>` (required; no default strategy is auto-selected)

AlphaBundle is additive and does not replace `run_all.py` orchestration. See:
- `docs/combined_model_1_3_architecture.md`
- `project/specs/combined_model_1_3_spec_v1.yaml`
