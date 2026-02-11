# Documentation Index

Documentation is split into two stable layers:

- `docs/spec/` for contracts, schemas, and interpretation locks.
- `docs/report/` for run-bound analyses and audit narratives.

## Core project docs

- `../README.md` — setup, commands, output locations, and workflows.
- `../ANALYSIS_BACKTEST_REPOSITORY.md` — architecture overview snapshot.

## Specification docs (`docs/spec/`)

- `spec/context_features.md` — funding persistence context contract and usage boundaries.
- `spec/vol_shock_relaxation.md` — Vol Shock → Relaxation event schema.
- `spec/auction_boundary_microstructure.md` — ABMA phase-1 analyzer skeleton and output contract.
- `spec/abma_binance_um_phase1.md` — ABMA v1 Binance UM input specification.
- `spec/abma_v2_funding_phase1_lock.md` — ABMA v2 funding interpretation lock.
- `spec/recommendations.md` — execution roadmap and definition-of-done checkpoints.

## Report docs (`docs/report/`)

- `report/latest_run_deep_report.md` — latest deep run analysis and prioritized actions.
- `report/project_report_detailed.md` — change-focused implementation report.

## Conventions

- Specs: prefer `*_spec.md` for new contract files.
- Locks: use `*_lock.md`.
- Reports: use `report_<topic>_<run_id>.md` for run-bound narratives.
- Keep this index short and link outward.
