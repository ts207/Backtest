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
  --phase2_event_type all \
  --phase2_min_ess 150 \
  --phase2_ess_max_lag 24 \
  --phase2_multiplicity_k 1.0 \
  --phase2_parameter_curvature_max_penalty 0.50 \
  --phase2_delay_grid_bars 0,4,8,16,30 \
  --phase2_min_delay_positive_ratio 0.60 \
  --phase2_min_delay_robustness_score 0.60
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
  --cost_bps 6 \
  --walkforward_drawdown_cluster_top_frac 0.10 \
  --walkforward_drawdown_tail_q 0.05 \
  --promotion_max_loss_cluster_len 64 \
  --promotion_max_cluster_loss_concentration 0.50 \
  --promotion_min_tail_conditional_drawdown_95 -0.20
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
  --fees_bps 4 --slippage_bps 2 --cost_bps 6 --force 1 \
  --clean_engine_artifacts 1

BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/eval/run_walkforward.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 --end 2025-12-31 \
  --embargo_days 0 --train_frac 0.6 --validation_frac 0.2 \
  --regime_max_share 0.80 \
  --blueprints_path data/reports/strategy_blueprints/RUN/blueprints.jsonl \
  --blueprints_top_k 10 --blueprints_filter_event_type all \
  --fees_bps 4 --slippage_bps 2 --cost_bps 6 --force 1 \
  --allow_unexpected_strategy_files 0 \
  --clean_engine_artifacts 1 \
  --config project/configs/pipeline.yaml --config project/configs/fees.yaml

BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/research/promote_blueprints.py \
  --run_id RUN \
  --regime_max_share 0.80

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
- Emits `per_strategy_regime_metrics` for promotion gates
- Emits `per_strategy_drawdown_cluster_metrics` for promotion drawdown-cluster gates

## 8) Strategy-Prep Strictness (Defaults)

Strict mode is the default for strategy preparation:

- Compiler:
  - `--strategy_blueprint_ignore_checklist 0`
  - `--strategy_blueprint_allow_fallback 0`
  - `--strategy_blueprint_allow_non_executable_conditions 0`
  - `--strategy_blueprint_allow_naive_entry_fail 0`
- Builder:
  - `--strategy_builder_ignore_checklist 0`
  - `--strategy_builder_allow_non_promoted 0`
  - `--strategy_builder_allow_missing_candidate_detail 0`
  - Merges evidence from:
    - `reports/promotions/<run_id>/promoted_blueprints.jsonl`
    - `reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
  - Source precedence on conflicts: promoted blueprint > edge candidate
- Naive-entry stage:
  - `--run_naive_entry_eval 1`
  - `--naive_min_trades 100`
  - `--naive_min_expectancy_after_cost 0.0`
  - `--naive_max_drawdown -0.25`

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
  --strategy_blueprint_allow_naive_entry_fail 1 \
  --run_naive_entry_eval 0 \
  --strategy_builder_ignore_checklist 1 \
  --strategy_builder_allow_non_promoted 1 \
  --strategy_builder_allow_missing_candidate_detail 1 \
  --promotion_allow_fallback_evidence 1
```

Execution convenience for checklist `KEEP_RESEARCH`:
- `run_all.py` now defaults `--auto_continue_on_keep_research 0` (fail-closed).
- If checklist decision is `KEEP_RESEARCH` and execution is requested, pipeline stops before execution stages by default.
- When checklist decision is `KEEP_RESEARCH` and execution is requested (`--run_backtest 1` or `--run_walkforward_eval 1`), orchestrator injects:
  - compiler: `--ignore_checklist 1 --allow_fallback_blueprints 1`
  - builder: `--ignore_checklist 1 --allow_non_promoted 1`
- The injected overrides only apply when explicitly enabling `--auto_continue_on_keep_research 1` (non-production diagnostics).
- Discovery-only runs keep strict strategy-prep defaults (no auto bypass).
- Run manifest trace fields:
  - `checklist_decision`
  - `auto_continue_applied`
  - `auto_continue_reason`
  - `execution_blocked_by_checklist`
  - `non_production_overrides`

## 9) Promotion Strictness (Defaults)

Promotion is fail-closed by default:

- Requires strategy-level walkforward evidence when blueprints exist (`walkforward_summary.json` with `per_strategy_split_metrics`)
- Fails if a blueprint has no matching strategy walkforward block
- Reads strategy return files with required columns; invalid/missing artifacts fail the stage
- Uses strategy-level walkforward regime evidence when available (train+validation regime consistency)
- Uses strategy-level drawdown clustering evidence when available:
  - `--max_loss_cluster_len` (default `64`)
  - `--max_cluster_loss_concentration` (default `0.50`)
  - `--min_tail_conditional_drawdown_95` (default `-0.20`)
- Enforces realized cost dominance guard on train/validation:
  - `--max_cost_ratio_train_validation` (default `0.60`)

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
- Trade evidence policy:
  - if `trades_*.csv` exist, report uses them as source-of-truth and requires row count to match `metrics.total_trades`
  - engine fallback is allowed only when `trades_*.csv` are absent
  - fallback is scoped to `metrics.metadata.strategy_ids` and fails if required strategy files are missing
- Requires numeric `metrics.cost_decomposition.net_alpha`

Fallback override for alternate backtest strategy dirs:

```bash
./.venv/bin/python project/pipelines/report/make_report.py \
  --run_id RUN \
  --allow_backtest_artifact_fallback 1
```

## 11) Mandatory Rebuild (Tier-1.5)

Historical runs produced before stale-artifact hardening are invalid for decisioning until rebuilt.

Invalidated example:
- `RUN_2022_2023`

Required rebuild pattern (production, strict defaults):

```bash
BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/run_all.py \
  --run_id RUN_2022_2023 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_backtest 1 \
  --run_walkforward_eval 1 \
  --run_make_report 1 \
  --force 1 \
  --clean_engine_artifacts 1 \
  --walkforward_allow_unexpected_strategy_files 0
```

Do not enable non-production overrides during rebuild (`--clean_engine_artifacts 0`, `--allow_unexpected_strategy_files 1`).
