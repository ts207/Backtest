# Current Audit Baseline

Date: 2026-02-24

This document summarizes the current implemented integrity posture.

## Verified Strengths

- Stage wiring validation for Phase2 chain in orchestrator.
- Registry normalization with canonical event schema.
- Event flags include both impulse (`*_event`) and active-window (`*_active`) columns.
- Phase2 discovery applies multiplicity controls and fail-closed invariants.
- Funding alignment in feature/context paths uses PIT-safe joins with staleness limits.
- Manifest/provenance coverage is enforced in critical stages.

## Recently Hardened Contracts

1. Funding and context integrity
- Context and market-state builders now fail when funding coverage has gaps.
- Removed scale-guess behavior in market context; one canonical funding series is required.

2. Feature semantics
- `revision_lag_minutes` aligned to 5m bars.
- `spread_zscore` no longer aliases basis z-score.
- Basis missing data remains explicit (no false zero fill).

3. Phase2 timing correctness
- Registry events are preferred as Phase2 input.
- `enter_ts` is preferred over raw `timestamp` for event alignment.
- `entry_lag_bars >= 1` enforced to prevent same-bar fill leakage.

4. Repo hygiene and reliability
- Runtime log artifacts removed from tracked files and blocked by hygiene checks.
- Hypothesis generators now support parquet/csv fallback when `pyarrow` is unavailable.

## Remaining Risk Areas

- Some Phase1 analyzers still use heuristic thresholds and may require family-specific calibration discipline.
- Cost model realism depends on liquidity inputs; bridge gates reduce but do not eliminate this risk.
- Archived and legacy research scripts exist; operational runs should follow `run_all.py` entrypoints.

## Minimum Promotion-Grade Checklist

- Non-trivial event count in Phase1.
- Registry flags present for selected event family.
- Phase2 discoveries survive multiplicity and quality gates.
- Bridge tradability gates pass with stressed costs.
- Blueprint compile succeeds with executable conditions/actions.
- Walkforward holds under embargo and regime concentration limits.
