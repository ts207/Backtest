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

Execution backtesting and portfolio allocation surfaces were intentionally removed to keep this repo discovery-only.
