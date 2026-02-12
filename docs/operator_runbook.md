# Operator Runbook (Mainline + AlphaBundle)

This runbook covers:
1. Mainline discovery/backtest/report operations (`run_all.py`)
2. Experimental AlphaBundle research pipelines (`project/pipelines/alpha_bundle/*`)

## 0) Environment
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
export BACKTEST_DATA_ROOT=/abs/path/to/data
```

## 1) Build datasets and core features
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10
```

Expected outputs:
- `data/runs/<run_id>/*.json|*.log`
- `data/lake/runs/<run_id>/cleaned/...`
- `data/lake/runs/<run_id>/features/...`
- `data/lake/runs/<run_id>/context/funding_persistence/...`

## 2) Discovery chain with edge export
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_hypothesis_generator 1 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1 \
  --run_expectancy_analysis 1 \
  --run_expectancy_robustness 1
```

Expected outputs:
- `data/reports/hypothesis_generator/<run_id>/...`
- `data/reports/phase2/<run_id>/<event_type>/phase2_candidates.csv`
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
- `data/reports/expectancy/<run_id>/conditional_expectancy*.json`

## 3) Optional backtest + report (mainline only)
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --strategies vol_compression_v1 \
  --run_backtest 1 \
  --run_make_report 1
```

Expected outputs:
- `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/metrics.json`
- `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/equity_curve.csv`
- `data/reports/vol_compression_expansion_v1/<run_id>/summary.md`

## 4) Experimental AlphaBundle workflow
All AlphaBundle modules read cleaned data from `$BACKTEST_DATA_ROOT/lake/cleaned` by default.

1. Build universe snapshot:
```bash
./.venv/bin/python project/pipelines/alpha_bundle/build_universe_snapshot.py \
  --run_id RUN \
  --universe_id top_liquid_v1 \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT \
  --bar_interval 15m \
  --adv_window_bars 96 \
  --adv_min_usd 1000000
```

2. Build per-symbol signals:
```bash
./.venv/bin/python project/pipelines/alpha_bundle/build_alpha_signals_v2.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT \
  --bar_interval 15m
```

3. Optional cross-sectional and context enrichments:
```bash
./.venv/bin/python project/pipelines/alpha_bundle/build_xs_momentum.py \
  --run_id RUN \
  --universe_snapshot_path data/feature_store/universe_snapshots/universe_snapshot_top_liquid_v1.parquet

./.venv/bin/python project/pipelines/alpha_bundle/build_onchain_flow_signal.py \
  --run_id RUN \
  --onchain_path <onchain_flow.parquet>

./.venv/bin/python project/pipelines/alpha_bundle/build_regime_filter.py \
  --run_id RUN \
  --market_symbol BTCUSDT
```

4. Merge, fit, and apply:
```bash
./.venv/bin/python project/pipelines/alpha_bundle/merge_signals.py \
  --run_id RUN \
  --signals_dir data/feature_store/signals \
  --xs_momentum_path data/feature_store/signals/xs_momentum_top_liquid_v1.parquet \
  --onchain_flow_path data/feature_store/signals/onchain_flow_mc.parquet

./.venv/bin/python project/pipelines/alpha_bundle/fit_orth_and_ridge.py \
  --run_id RUN \
  --signals_path data/feature_store/alpha_bundle/merged_signals.parquet \
  --label_path <labels_panel.parquet> \
  --signal_cols ts_momentum_multi,xs_momentum,mean_reversion_state,funding_carry_adjusted,onchain_flow_mc,orderflow_imbalance

./.venv/bin/python project/pipelines/alpha_bundle/apply_alpha_bundle.py \
  --run_id RUN \
  --signals_path data/feature_store/alpha_bundle/merged_signals.parquet \
  --ridge_model_path <CombModelRidge_*.json> \
  --regime_path data/feature_store/regimes/vol_regime.parquet
```

## 5) Optional ingestion sensors
```bash
./.venv/bin/python project/pipelines/ingest/ingest_binance_um_liquidation_snapshot.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10

./.venv/bin/python project/pipelines/ingest/ingest_binance_um_open_interest_hist.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --period 5m
```

## Cleanup
```bash
project/scripts/clean_data.sh runtime
project/scripts/clean_data.sh all
project/scripts/clean_data.sh repo
```
