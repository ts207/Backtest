# Backtest (Discovery + Optional Execution Validation)

Crypto research pipeline for BTC/ETH and related futures sensors. The mainline runtime remains discovery-first with optional strategy backtesting/reporting, and now also includes experimental AlphaBundle (Combined Model 1 + 3) research modules.

## Scope
Primary runtime:
- market data ingestion
- cleaned 15m canonical bars
- feature/context builds
- hypothesis generation
- Phase1/Phase2 event discovery
- edge candidate export
- conditional expectancy + robustness checks
- optional single-strategy backtesting (`vol_compression_v1`)
- optional backtest report generation

Experimental runtime:
- `project/pipelines/alpha_bundle/*` for multi-signal alpha research and combination
- architecture spec at `project/specs/combined_model_1_3_spec_v1.yaml`

## Setup
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

## Data root
Default data root is `data/` at repo root (override with `BACKTEST_DATA_ROOT`).

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

## Optional backtest + report
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

## Key outputs
- `data/runs/<run_id>/*.json|*.log`
- `data/reports/hypothesis_generator/<run_id>/...`
- `data/reports/phase2/<run_id>/<event_type>/...`
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
- `data/reports/expectancy/<run_id>/conditional_expectancy.json`
- `data/reports/expectancy/<run_id>/conditional_expectancy_robustness.json`
- `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/metrics.json`
- `data/reports/vol_compression_expansion_v1/<run_id>/summary.md`
- `data/feature_store/*` (AlphaBundle feature/model outputs)

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

## Combined Model 1 + Model 3 (Experimental AlphaBundle)
The AlphaBundle stack is additive and does not replace the primary `run_all.py` discovery/backtest flow.

Quick start (single symbol):
```bash
./.venv/bin/python project/pipelines/alpha_bundle/build_alpha_signals_v2.py \
  --run_id RUN \
  --symbol BTCUSDT \
  --bars_path <bars.parquet> \
  --funding_path <funding.parquet> \
  --oi_path <oi.parquet>

./.venv/bin/python project/pipelines/alpha_bundle/fit_orth_and_ridge.py \
  --run_id RUN \
  --signals_path data/feature_store/signals/signals_BTCUSDT.parquet \
  --label_path <labels.parquet> \
  --signal_cols ts_momentum_multi,mean_reversion_state,funding_carry_adjusted,orderflow_imbalance \
  --orth_method residualization

./.venv/bin/python project/pipelines/alpha_bundle/apply_alpha_bundle.py \
  --run_id RUN \
  --signals_path data/feature_store/signals/signals_BTCUSDT.parquet \
  --ridge_model_path <CombModelRidge_*.json>
```

Multi-universe example:
```bash
./.venv/bin/python project/pipelines/alpha_bundle/build_universe_snapshot.py \
  --run_id RUN \
  --universe_id top_liquid_v1 \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT \
  --bar_interval 15m \
  --adv_window_bars 96 \
  --adv_min_usd 1000000 \
  --start 2021-01-01T00:00:00Z \
  --end 2025-01-01T00:00:00Z

./.venv/bin/python project/pipelines/alpha_bundle/build_alpha_signals_v2.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT \
  --bar_interval 15m

./.venv/bin/python project/pipelines/alpha_bundle/build_xs_momentum.py \
  --run_id RUN \
  --universe_snapshot_path data/feature_store/universe_snapshots/universe_snapshot_top_liquid_v1.parquet \
  --bar_interval 15m

./.venv/bin/python project/pipelines/alpha_bundle/merge_signals.py \
  --run_id RUN \
  --signals_dir data/feature_store/signals \
  --xs_momentum_path data/feature_store/signals/xs_momentum_top_liquid_v1.parquet \
  --onchain_flow_path data/feature_store/signals/onchain_flow_mc.parquet

./.venv/bin/python project/pipelines/alpha_bundle/fit_orth_and_ridge.py \
  --run_id RUN \
  --signals_path data/feature_store/alpha_bundle/merged_signals.parquet \
  --label_path <labels_panel.parquet> \
  --signal_cols ts_momentum_multi,xs_momentum,mean_reversion_state,funding_carry_adjusted,onchain_flow_mc,orderflow_imbalance \
  --orth_method residualization \
  --lambda_grid 1e-4,3e-4,1e-3,3e-3,1e-2 \
  --cv_blocks 6
```

See `docs/combined_model_1_3_architecture.md` for architecture details.

## Clean local artifacts
```bash
project/scripts/clean_data.sh runtime
project/scripts/clean_data.sh all
project/scripts/clean_data.sh repo
```
