# Data Strategy

## Policy

`data/` is runtime storage, not source control content.

## Keep in Git

- code (`project/`)
- tests (`tests/`)
- docs (`docs/`)
- configs/specs

## Do Not Commit

- raw exchange downloads (`data/lake/raw/...`)
- cleaned/features/trades artifacts (`data/lake/...`)
- manifests/logs/reports (`data/runs/...`, `data/reports/...`)

## Reproducibility Rules

- Treat `run_id` as immutable join key.
- Persist stage manifests in `data/runs/<run_id>/*.json`.
- Record the exact command and env (`BACKTEST_DATA_ROOT`, flags, symbols, dates).

## Retention Recommendation

Minimum snapshot for audit:
- `data/runs/<run_id>/`
- `data/reports/phase2/<run_id>/`
- `data/reports/strategy_blueprints/<run_id>/`
- if downstream run enabled:
  - `data/reports/eval/<run_id>/`
  - `data/reports/promotions/<run_id>/`
  - `data/reports/vol_compression_expansion_v1/<run_id>/`
