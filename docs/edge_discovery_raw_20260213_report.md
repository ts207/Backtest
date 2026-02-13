# Edge Discovery Run Report: `edge_discovery_raw_20260213`

## Run configuration
- Command:
  ```bash
  ./.venv/bin/python project/pipelines/run_all.py \
    --run_id edge_discovery_raw_20260213 \
    --symbols BTCUSDT,ETHUSDT \
    --start 2020-06-01 \
    --end 2025-07-10 \
    --skip_ingest_ohlcv 1 \
    --skip_ingest_funding 1 \
    --skip_ingest_spot_ohlcv 1 \
    --enable_cross_venue_spot_pipeline 0 \
    --run_hypothesis_generator 1 \
    --run_phase2_conditional 1 \
    --phase2_event_type all \
    --run_edge_candidate_universe 1
  ```
- Data source mode: **from existing raw lake partitions** (ingest skipped).
- Symbols: `BTCUSDT`, `ETHUSDT`
- Date range: `2020-06-01` to `2025-07-10`
- Planned stages: `27` (completed)

## Primary result artifacts
- Edge candidate universe:
  - `data/reports/edge_candidates/edge_discovery_raw_20260213/edge_candidates_normalized.csv`
  - `data/reports/edge_candidates/edge_discovery_raw_20260213/edge_candidates_normalized.json`
- Strategy builder outputs:
  - `data/reports/strategy_builder/edge_discovery_raw_20260213/strategy_candidates.json`
  - `data/reports/strategy_builder/edge_discovery_raw_20260213/selection_summary.md`
  - `data/reports/strategy_builder/edge_discovery_raw_20260213/manual_backtest_instructions.md`
- Hypothesis generation outputs:
  - `data/reports/hypothesis_generator/edge_discovery_raw_20260213/summary.md`
  - `data/reports/hypothesis_generator/edge_discovery_raw_20260213/phase1_hypothesis_queue.csv`

## Event families with reports generated
- `vol_shock_relaxation`
- `liquidity_refill_lag_window`
- `liquidity_absence_window`
- `vol_aftershock_window`
- `directional_exhaustion_after_forced_flow`
- `cross_venue_desync`
- `liquidity_vacuum`
- `funding_extreme_reversal_window`
- `range_compression_breakout_window`

For each family, the run produced Phase 1 artifacts under:
- `data/reports/<event>/edge_discovery_raw_20260213/`

And Phase 2 promotion artifacts under:
- `data/reports/phase2/edge_discovery_raw_20260213/<event>/`

## Runtime notes
- Longest stages:
  - `analyze_liquidity_refill_lag_window` (~204.7s)
  - `analyze_directional_exhaustion_after_forced_flow` (~137.5s)
  - `analyze_liquidity_vacuum` (~42.1s)
- Pipeline completion status: `Pipeline run completed: edge_discovery_raw_20260213`
