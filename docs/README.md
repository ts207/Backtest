# Documentation Index

This index groups project docs so navigation is fast and predictable.

## Core project docs

- `../README.md` — setup, quickstart, commands, and repo layout.
- `../ANALYSIS_BACKTEST_REPOSITORY.md` — high-level architecture + interpretation snapshot.

## Feature and strategy specifications

- `context_features.md` — funding persistence context contract and usage boundaries.
- `vol_shock_relaxation.md` — Vol Shock → Relaxation event schema.
- `auction_boundary_microstructure.md` — ABMA phase-1 analyzer skeleton and output contract.
- `abma_binance_um_phase1.md` — ABMA v1 Binance UM input specification.
- `abma_v2_funding_phase1_lock.md` — ABMA v2 funding interpretation lock.

## Research policy and recommendations

- `recommendations.md` — execution roadmap and definition-of-done checkpoints.

## Run and implementation reports

- `latest_run_deep_report.md` — latest deep run analysis and prioritized actions.
- `project_report_detailed.md` — change-focused implementation report.

## How to keep docs organized

- Put **specs/contracts** in files named `*_spec.md` or explicit domain names.
- Put **frozen interpretation locks** in files named `*_lock.md`.
- Put **time-bound reports** in files named `*report*.md` and include run IDs.
- Keep README files short and link outward to detailed docs instead of duplicating content.
