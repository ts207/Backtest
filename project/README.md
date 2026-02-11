# Project Package

This package contains the discovery-core runtime used by `project/pipelines/run_all.py`.

Active areas:
- `pipelines/ingest`
- `pipelines/clean`
- `pipelines/features`
- `pipelines/research`
- `pipelines/_lib`
- `features/`

Runtime data contract:
- root: `data/`
- manifests/logs: `data/runs/<run_id>/...`
- research outputs: `data/reports/<stage>/<run_id>/...`
- backtest outputs: `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/...`
- backtest reports: `data/reports/vol_compression_expansion_v1/<run_id>/...`

Backtesting is optional and can be enabled from `project/pipelines/run_all.py` with:
- `--run_backtest 1`
- `--run_make_report 1`
- `--strategies <comma-separated ids>` (required; no default strategy is auto-selected)
