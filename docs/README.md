# Documentation Index

Documentation is split into two stable layers:

- `docs/spec/` for contracts, schemas, and interpretation locks.
- `docs/report/` for time-bound run analyses and audit reports.

## Core project docs

- `../README.md` — setup, workflows, and command entrypoints.
- `../ANALYSIS_BACKTEST_REPOSITORY.md` — architecture overview snapshot.

## Specification docs (`docs/spec/`)

- `spec/context_features.md` — funding persistence context contract and usage boundaries.
- `spec/vol_shock_relaxation.md` — Vol Shock → Relaxation event schema.
- `spec/auction_boundary_microstructure.md` — ABMA phase-1 analyzer skeleton and output contract.
- `spec/abma_binance_um_phase1.md` — ABMA v1 Binance UM input specification.
- `spec/abma_v2_funding_phase1_lock.md` — ABMA v2 funding interpretation lock.
- `spec/recommendations.md` — execution roadmap and definition-of-done checkpoints.

## Report docs (`docs/report/`)

- `report/full_project_audit_deep_report.md` — comprehensive technical audit with risks and prioritized actions.
- `report/latest_run_deep_report.md` — latest deep run analysis and prioritized actions.
- `report/project_report_detailed.md` — change-focused implementation report.

## Doc hygiene rules

- Keep specs evergreen and reproducible in `docs/spec/`.
- Keep run-specific and date-specific narratives in `docs/report/`.
- Prefer linking from README/index files instead of duplicating long content.
