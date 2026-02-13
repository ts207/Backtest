# Strategy Builder Guide

`build_strategy_candidates.py` converts promoted edge candidates into manual-backtest strategy candidate artifacts.

## Inputs
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
- referenced phase2 source artifacts (`promoted_candidates.json` or `phase2_candidates.csv`)
- optional AlphaBundle score artifact (`data/feature_store/alpha_bundle/alpha_bundle_scores.parquet` or `.csv`)

## Command
```bash
./.venv/bin/python project/pipelines/research/build_strategy_candidates.py \
  --run_id edge_2020_2025_fresh \
  --symbols BTCUSDT,ETHUSDT \
  --top_k_per_event 2 \
  --max_candidates 20 \
  --include_alpha_bundle 1
```

## Outputs
- `data/reports/strategy_builder/<run_id>/strategy_candidates.json`
- `data/reports/strategy_builder/<run_id>/selection_summary.md`
- `data/reports/strategy_builder/<run_id>/manual_backtest_instructions.md`

## Candidate structure
Each candidate includes:
- source metadata (`event`, `condition`, `action`, `status`)
- ranking metrics (`edge_score`, `stability_proxy`, `selection_score`)
- translated risk controls (`entry_delay_bars`, `size_scale`, `block_entries`)
- routing metadata (`execution_family`, `base_strategy`)
- execution readiness fields (`backtest_ready`, `backtest_ready_reason`)
- manual backtest command template

## Intended usage
- Use strategy-builder outputs as a deterministic handoff for manual backtests.
- Keep track-level parity: AlphaBundle and mainline candidates must pass the same gate philosophy.

## Routing table (single source of truth)
The strategy builder routes event families with an explicit mapping:

| phase2 event family | execution_family | base_strategy |
|---|---|---|
| `vol_shock_relaxation` | `breakout_mechanics` | `vol_compression_v1` |
| `vol_aftershock_window` | `breakout_mechanics` | `vol_compression_v1` |
| `range_compression_breakout_window` | `breakout_mechanics` | `vol_compression_v1` |
| `liquidity_refill_lag_window` | `breakout_mechanics` | `vol_compression_v1` |
| `liquidity_absence_window` | `breakout_mechanics` | `vol_compression_v1` |
| `liquidity_vacuum` | `breakout_mechanics` | `vol_compression_v1` |
| `funding_extreme_reversal_window` | `carry_imbalance` | `carry_imbalance_v1` |
| `directional_exhaustion_after_forced_flow` | `exhaustion_overshoot` | `mean_reversion_exhaustion_v1` |
| `cross_venue_desync` | `spread_dislocation` | `spread_dislocation_v1` |

### Fallback behavior (fail closed)
If an event family is not in the routing table, strategy builder does **not** default to breakout:
- `execution_family` is set to `unmapped`
- `base_strategy` is set to `unmapped`
- `backtest_ready` is set to `false`
- `backtest_ready_reason` records the unknown event family
- candidate notes include the same routing failure reason for auditability
