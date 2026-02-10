# Data Strategy

## Decision
`data/` remains local-only and is not committed to git.

## What stays in git
- Source code under `project/`
- Tests under `tests/`
- Documentation under `docs/`
- Small, deterministic config files and manifests

## What stays out of git
- Raw exchange downloads (`data/lake/raw/...`)
- Cleaned/feature parquet partitions (`data/lake/...`)
- Run outputs/logs/reports generated per run (`data/runs/...`, `data/reports/...`)

## Operational policy
- Treat `run_id` as the immutable join key across artifacts.
- Keep at least one durable external backup target for large artifacts (for example: S3-compatible object storage or NAS).
- If sharing results, export compressed run snapshots keyed by `run_id` instead of committing data files.

## Minimal backup scope recommendation
- `data/runs/<run_id>/` manifests and logs
- `data/reports/<strategy_or_research>/<run_id>/` summaries
- Any custom configs used for that run

This keeps the repository lightweight while preserving reproducibility and auditability.
