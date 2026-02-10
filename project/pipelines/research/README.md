# Research Pipelines Index

This folder contains ad-hoc and repeatable research workflows.

## Portfolio policy & validation
- `validate_multi_edge_portfolio.py`: applies promotion gates to multi-edge outputs.
- `sweep_portfolio_harshness.py`: runs threshold sweeps over validation gates.
- Shared metrics helpers live in `_lib/metrics.py`.

## Strategy feasibility / execution studies
- `strategies_cost_report.py`: strategy-level gross/cost feasibility report across symbols/timeframes.
  - Supports execution throttling studies via:
    - `--execution_decision_grid_bars`
    - `--execution_decision_grid_offset_bars`
    - `--execution_decision_grid_hours` (fallback convenience)

## Signal-analysis studies
- `analyze_vol_shock_relaxation.py`
- `analyze_vol_aftershock_window.py`
- `analyze_liquidity_absence_window.py`
- `analyze_liquidity_refill_lag_window.py`
- `analyze_directional_exhaustion_after_forced_flow.py`
- `analyze_cross_venue_desync.py`
- `analyze_conditional_expectancy.py`

## Edge candidate flow
- `export_edge_candidates.py`
- `check_overlay_promotion.py`
- `validate_expectancy_traps.py`

## Parameter sweep helpers
- `sweep_tsmom_band_bps.py`
- `sweep_tsmom_cooldown.py`

## Notes
- If you add a new research script, place reusable logic in `_lib/` when possible.
- Prefer writing machine-readable outputs (`.json`/`.csv`) under `data/runs/<run_id>/research/`.
