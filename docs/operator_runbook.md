# Operator Runbook

## 0) Environment

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
export BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data
```

## 1) Discovery-Only Canonical Run

```bash
BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/run_all.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --run_hypothesis_generator 1 \
  --run_phase2_conditional 1 \
  --phase2_event_type all
```

Use this when you only want discovery/blueprint/builder artifacts.

## 2) Full End-to-End Run

```bash
BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/run_all.py \
  --run_id RUN_FULL \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1 \
  --run_backtest 1 \
  --run_walkforward_eval 1 \
  --run_make_report 1 \
  --blueprints_path data/reports/strategy_blueprints/RUN_FULL/blueprints.jsonl \
  --blueprints_top_k 10 \
  --blueprints_filter_event_type all \
  --fees_bps 4 \
  --slippage_bps 2 \
  --cost_bps 6
```

## 3) Skip network ingest when raw already exists

```bash
--skip_ingest_ohlcv 1 --skip_ingest_funding 1 --skip_ingest_spot_ohlcv 1 --force 0
```

## 4) Downstream Reruns for an existing run

```bash
BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/backtest/backtest_strategies.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT \
  --blueprints_path data/reports/strategy_blueprints/RUN/blueprints.jsonl \
  --blueprints_top_k 10 \
  --blueprints_filter_event_type all \
  --fees_bps 4 --slippage_bps 2 --cost_bps 6 --force 1

BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/eval/run_walkforward.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 --end 2025-12-31 \
  --embargo_days 0 --train_frac 0.6 --validation_frac 0.2 \
  --blueprints_path data/reports/strategy_blueprints/RUN/blueprints.jsonl \
  --blueprints_top_k 10 --blueprints_filter_event_type all \
  --fees_bps 4 --slippage_bps 2 --cost_bps 6 --force 1 \
  --config project/configs/pipeline.yaml --config project/configs/fees.yaml

BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/research/promote_blueprints.py --run_id RUN

BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/report/make_report.py --run_id RUN
```

## 5) Fast Triage Checklist

1. `data/runs/<run_id>/` has stage JSON manifests.
2. If no `backtest_strategies.json`, downstream never ran.
3. If strategy builder shows zero candidates, check edge export stage and checklist.
4. If promotion is empty, inspect `promotion_report.json` gates and `walkforward_summary.json`.

## 6) Safety

- Use a new `run_id` to avoid overwriting existing artifacts.
- Keep `BACKTEST_DATA_ROOT` explicit to avoid mixing datasets across directories.

## 7) Walkforward Strictness (Defaults)

Walkforward is fail-closed by default:

- Requires one execution source: `--strategies` or `--blueprints_path`
- Requires a `test` split window
- Fails if split artifacts are missing/invalid:
  - `metrics.json`
  - `strategy_returns_*.csv`
- Uses validated `cost_decomposition.net_alpha` for stressed split PnL
- Supports repeated `--config` passthrough so split backtests match canonical config behavior

## 8) Strategy-Prep Strictness (Defaults)

Strict mode is the default for strategy preparation:

- Compiler:
  - `--strategy_blueprint_ignore_checklist 0`
  - `--strategy_blueprint_allow_fallback 0`
  - `--strategy_blueprint_allow_non_executable_conditions 0`
- Builder:
  - `--strategy_builder_ignore_checklist 0`
  - `--strategy_builder_allow_non_promoted 0`
  - `--strategy_builder_allow_missing_candidate_detail 0`

Explicit override example:

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id RUN_OVERRIDE \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --strategy_blueprint_ignore_checklist 1 \
  --strategy_blueprint_allow_fallback 1 \
  --strategy_blueprint_allow_non_executable_conditions 1 \
  --strategy_builder_ignore_checklist 1 \
  --strategy_builder_allow_non_promoted 1 \
  --strategy_builder_allow_missing_candidate_detail 1 \
  --promotion_allow_fallback_evidence 1
```

## 9) Promotion Strictness (Defaults)

Promotion is fail-closed by default:

- Requires strategy-level walkforward evidence when blueprints exist (`walkforward_summary.json` with `per_strategy_split_metrics`)
- Fails if a blueprint has no matching strategy walkforward block
- Reads strategy return files with required columns; invalid/missing artifacts fail the stage

Fallback override (not recommended for production):

```bash
./.venv/bin/python project/pipelines/research/promote_blueprints.py \
  --run_id RUN \
  --allow_fallback_evidence 1
```

## 10) Report Strictness (Defaults)

Report generation is fail-closed by default:

- Requires valid backtest metrics in `lake/trades/backtests/vol_compression_expansion_v1/<run_id>/metrics.json`
- Requires `equity_curve.csv` with `equity` column
- Requires trade evidence from `trades_*.csv` or engine fallback (`strategy_returns_*.csv`)
- Requires numeric `metrics.cost_decomposition.net_alpha`

Fallback override for alternate backtest strategy dirs:

```bash
./.venv/bin/python project/pipelines/report/make_report.py \
  --run_id RUN \
  --allow_backtest_artifact_fallback 1
```
