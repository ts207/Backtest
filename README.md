# Backtest (Discovery Core)

Discovery-only crypto research pipeline for BTC/ETH and related futures sensors.

## Scope
The active codebase supports:
- market data ingestion
- cleaned 15m canonical bars
- feature/context builds
- hypothesis generation
- Phase1/Phase2 event discovery
- edge candidate export
- conditional expectancy + robustness checks

Removed from active scope:
- strategy execution engine
- backtest portfolio allocation
- overlay contracts and promotion checks

## Setup
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

## Data root
Default data root is `data/` at repo root.

```bash
export BACKTEST_DATA_ROOT=/abs/path/to/data
```

## One-command discovery run
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

## Key outputs
- `data/runs/<run_id>/*.json|*.log`
- `data/reports/hypothesis_generator/<run_id>/...`
- `data/reports/phase2/<run_id>/<event_type>/...`
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
- `data/reports/expectancy/<run_id>/conditional_expectancy.json`
- `data/reports/expectancy/<run_id>/conditional_expectancy_robustness.json`

## Optional ingestion sensors
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

## Clean local artifacts
```bash
project/scripts/clean_data.sh runtime
project/scripts/clean_data.sh all
project/scripts/clean_data.sh repo
```


## Combined Model 1 + Model 3 (Institutional AlphaBundle)

This repo includes scaffolding for a deterministic, point-in-time safe **AlphaBundle** pipeline under `project/pipelines/alpha_bundle/` and an authoritative spec at `project/specs/combined_model_1_3_spec_v1.yaml`.

Quick start (single-symbol scaffold):

```bash
python project/pipelines/alpha_bundle/build_alpha_signals_v2.py \
  --run_id RUN \
  --symbol BTCUSDT \
  --bars_path <bars.parquet> \
  --funding_path <funding.parquet> \
  --oi_path <oi.parquet>

# Merge step is optional in single-symbol mode (signals parquet already contains required columns).
python project/pipelines/alpha_bundle/fit_orth_and_ridge.py \
  --run_id RUN \
  --signals_path data/feature_store/signals/signals_BTCUSDT.parquet \
  --label_path <labels.parquet> \
  --signal_cols ts_momentum_multi,mean_reversion_state,funding_carry_adjusted,orderflow_imbalance \
  --orth_method residualization

python project/pipelines/alpha_bundle/apply_alpha_bundle.py \
  --run_id RUN \
  --signals_path data/feature_store/signals/signals_BTCUSDT.parquet \
  --ridge_model_path <CombModelRidge_*.json>
```

These modules are intentionally minimal and intended to be extended to full universe snapshots, cross-sectional aggregates, regime gating, and portfolio optimization.

### Multi-crypto universe (recommended path)

Prereq: build cleaned 15m bars (this repo writes to `project/lake/cleaned/perp/<symbol>/bars_15m/...`).

1) Build PIT universe snapshots (per bar timestamp) using ADV proxy:

```bash
python project/pipelines/alpha_bundle/build_universe_snapshot.py \
  --run_id RUN \
  --universe_id top_liquid_v1 \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT \
  --bar_interval 15m \
  --adv_window_bars 96 \
  --adv_min_usd 1000000 \
  --start 2021-01-01T00:00:00Z \
  --end 2025-01-01T00:00:00Z
```

2) Build per-symbol alpha signals (outputs one parquet per symbol under `data/feature_store/signals/`):

```bash
python project/pipelines/alpha_bundle/build_alpha_signals_v2.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT \
  --bar_interval 15m
```

3) (Optional) Build cross-sectional features for any base feature column (e.g., cross-sectional rank of `z_tsmom_multi`).
   This writes a long table with cs_zscore/cs_rank per (ts_event,symbol).

Recommended: build cross-sectional momentum via the dedicated pipeline (rank-based):

```bash
python project/pipelines/alpha_bundle/build_xs_momentum.py \
  --run_id RUN \
  --universe_snapshot_path data/feature_store/universe_snapshots/universe_snapshot_top_liquid_v1.parquet \
  --bar_interval 15m
```

If you want cross-sectional stats for an arbitrary base feature, use:

```bash
python project/pipelines/alpha_bundle/build_cross_section_features.py \
  --run_id RUN \
  --universe_snapshot_path data/feature_store/universe_snapshots/universe_snapshot_top_liquid_v1.parquet \
  --base_feature_dir data/feature_store/signals \
  --base_feature_name ts_momentum_multi
```

4) (Optional) Build on-chain flow signal (requires an external OnChainFlow parquet/csv):

```bash
python project/pipelines/alpha_bundle/build_onchain_flow_signal.py \
  --run_id RUN \
  --onchain_path <onchain_flow.parquet> \
  --bar_interval 15m
```

5) (Optional) Build volatility regime gating series:

```bash
python project/pipelines/alpha_bundle/build_regime_filter.py \
  --run_id RUN \
  --market_symbol BTCUSDT \
  --bar_interval 15m
```

6) Merge signal components into one panel table:

```bash
python project/pipelines/alpha_bundle/merge_signals.py \
  --run_id RUN \
  --signals_dir data/feature_store/signals \
  --xs_momentum_path data/feature_store/signals/xs_momentum_top_liquid_v1.parquet \
  --onchain_flow_path data/feature_store/signals/onchain_flow_mc.parquet
```

7) Fit ridge (pooled over all (ts_event,symbol)) once you have labels in a panel format (ts_event,symbol,y):

```bash
python project/pipelines/alpha_bundle/fit_orth_and_ridge.py \
  --run_id RUN \
  --signals_path data/feature_store/alpha_bundle/merged_signals.parquet \
  --label_path <labels_panel.parquet> \
  --signal_cols ts_momentum_multi,xs_momentum,mean_reversion_state,funding_carry_adjusted,onchain_flow_mc,orderflow_imbalance \
  --orth_method residualization \
  --lambda_grid 1e-4,3e-4,1e-3,3e-3,1e-2 \
  --cv_blocks 6
```

8) Apply ridge to produce `alpha_bundle_scores.parquet`:

```bash
python project/pipelines/alpha_bundle/apply_alpha_bundle.py \
  --run_id RUN \
  --signals_path data/feature_store/alpha_bundle/merged_signals.parquet \
  --ridge_model_path <CombModelRidge_*.json> \
  --regime_path data/feature_store/regimes/vol_regime.parquet
```
