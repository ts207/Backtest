# Research Pipelines Index

Active discovery-core research scripts:

## Hypothesis generation
- `generate_hypothesis_queue.py`

## Phase1 event analyzers
- `analyze_vol_shock_relaxation.py`
- `analyze_vol_aftershock_window.py`
- `analyze_liquidity_absence_window.py`
- `analyze_liquidity_refill_lag_window.py`
- `analyze_directional_exhaustion_after_forced_flow.py`
- `analyze_cross_venue_desync.py`
- `analyze_funding_extreme_reversal_window.py`
- `analyze_funding_episode_events.py`
- `analyze_range_compression_breakout_window.py`
- `analyze_liquidity_vacuum.py`
- `analyze_liquidation_cascade.py`
- `analyze_oi_shock_events.py`
- `analyze_conditional_expectancy.py`

## Phase2 and candidate export
- `phase2_conditional_hypotheses.py`
- `export_edge_candidates.py`

## Expectancy validation
- `analyze_conditional_expectancy.py`
- `validate_expectancy_traps.py`

## Utility
- `generate_recommendations_checklist.py`

Notes:
- Write machine-readable outputs under `data/reports/<stage>/<run_id>/...`.
- Keep scripts deterministic and non-leaking by design.
