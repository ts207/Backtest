# Operator Runbook (Unified Mainline + AlphaBundle)

## 0) Environment
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
export BACKTEST_DATA_ROOT=/abs/path/to/data
```

## 1) Canonical discovery + strategy-builder run (2020-2025)
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id edge_2020_2025_fresh \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --run_hypothesis_generator 1 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1 \
  --run_expectancy_analysis 1 \
  --run_expectancy_robustness 1 \
  --run_recommendations_checklist 1 \
  --run_strategy_builder 1
```

Cross-venue specifics:
- If phase2 includes `cross_venue_desync` (or `all`), `run_all.py` auto-runs:
  - `ingest_binance_spot_ohlcv_15m`
  - `build_cleaned_15m --market spot`
  - `build_features_v1 --market spot`
- To reuse preloaded spot data and skip downloads:
```bash
--skip_ingest_spot_ohlcv 1
```

Expected outputs:
- `data/runs/<run_id>/*.json|*.log`
- `data/lake/runs/<run_id>/context/funding_persistence/...`
- `data/lake/runs/<run_id>/context/market_state/...`
- `data/reports/phase2/<run_id>/<event_type>/...`
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
- `data/reports/expectancy/<run_id>/conditional_expectancy*.json`
- `data/reports/strategy_builder/<run_id>/*`

Execution order (high-level):
1. ingest -> clean -> features (perp, optional spot)
2. context build
3. phase1 family analyzers
4. phase2 conditional hypotheses
5. edge candidate export + expectancy checks
6. strategy builder handoff

## 2) Manual backtest after strategy builder
Backtests are not auto-triggered by default. Use strategy-builder outputs first, then run manual backtests.

Manual command template:
```bash
./.venv/bin/python project/pipelines/backtest/backtest_strategies.py \
  --run_id <manual_backtest_run_id> \
  --symbols BTCUSDT,ETHUSDT \
  --strategies vol_compression_v1 \
  --force 1
```

## 3) Strategy-builder direct run (optional)
```bash
./.venv/bin/python project/pipelines/research/build_strategy_candidates.py \
  --run_id edge_2020_2025_fresh \
  --symbols BTCUSDT,ETHUSDT \
  --top_k_per_event 2 \
  --max_candidates 20 \
  --include_alpha_bundle 1
```

Interpretation:
- `base_strategy` is event-family template mapping.
- `backtest_ready=true` means executable adapter exists now.
- `backtest_ready=false` means translate candidate into concrete strategy implementation first.

## 4) AlphaBundle parallel flow (equal policy)
Use AlphaBundle when multi-signal, cross-sectional research is required. Keep promotion gates equivalent to mainline.

Avoid AlphaBundle when your question is narrow and event-mechanism specific (single-family validation is faster and cleaner in mainline phase1/phase2).

Minimal path:
```bash
./.venv/bin/python project/pipelines/alpha_bundle/build_universe_snapshot.py \
  --run_id RUN \
  --universe_id top_liquid_v1 \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT \
  --bar_interval 15m \
  --adv_window_bars 96 \
  --adv_min_usd 1000000

./.venv/bin/python project/pipelines/alpha_bundle/build_alpha_signals_v2.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT \
  --bar_interval 15m

./.venv/bin/python project/pipelines/alpha_bundle/merge_signals.py \
  --run_id RUN \
  --signals_dir data/feature_store/signals

./.venv/bin/python project/pipelines/alpha_bundle/fit_orth_and_ridge.py \
  --run_id RUN \
  --signals_path data/feature_store/alpha_bundle/merged_signals.parquet \
  --label_path <labels_panel.parquet> \
  --signal_cols ts_momentum_multi,xs_momentum,mean_reversion_state,funding_carry_adjusted,onchain_flow_mc,orderflow_imbalance
```

## 5) Optional ingestion sensors
```bash
./.venv/bin/python project/pipelines/ingest/ingest_binance_um_liquidation_snapshot.py \
  --run_id edge_2020_2025_fresh \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31

./.venv/bin/python project/pipelines/ingest/ingest_binance_um_open_interest_hist.py \
  --run_id edge_2020_2025_fresh \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --period 5m
```

## Cleanup
```bash
project/scripts/clean_data.sh runtime
project/scripts/clean_data.sh all
project/scripts/clean_data.sh repo
```
