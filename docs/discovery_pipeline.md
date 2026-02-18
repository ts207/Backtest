# Discovery Pipeline

## Purpose

The discovery pipeline converts raw market data into auditable strategy hypotheses and blueprints.

High-level chain:
1. ingest
2. clean
3. features + context
4. hypothesis queue generation (proposal layer)
5. phase-1 event analyzers
6. phase-2 conditional hypotheses
7. edge/blueprint/strategy handoff artifacts

## Orchestrator

Entry point:
- `project/pipelines/run_all.py``

Important defaults:
- `run_backtest=0`
- `run_walkforward_eval=0`
- `run_make_report=0`

So discovery can complete successfully without producing backtest/eval/promotion/final-report artifacts.

## Stage Groups

### A) Data foundation
- `ingest_binance_um_ohlcv_15m`
- `ingest_binance_um_funding`
- `ingest_binance_spot_ohlcv_15m` (auto-used when cross-venue phase2 is requested)
- `build_cleaned_15m` (+ `build_cleaned_15m_spot` if enabled)
- `build_features_v1` (+ `build_features_v1_spot` if enabled)
- `build_universe_snapshots`
- `build_context_features`
- `build_market_context`

### B) Research
- hypothesis queue (`generate_hypothesis_queue.py`)
- phase-1 family analyzers (`analyze_*`)
- phase-2 per-family conditional hypotheses (`phase2_conditional_hypotheses.py`)
- checklist (`generate_recommendations_checklist.py`)

Phase-2 now consumes `data/reports/hypothesis_generator/<run_id>/phase1_hypothesis_queue.*` when present and records matched hypothesis context per event family.

### C) Strategy preparation
- blueprint compiler (`compile_strategy_blueprints.py`)
- strategy builder (`build_strategy_candidates.py`)

### D) Optional downstream execution
- backtest (`backtest_strategies.py`)
- walkforward (`run_walkforward.py`)
- promotion (`promote_blueprints.py`)
- report (`make_report.py`)

Walkforward integrity defaults:
- fail-closed artifact validation (`metrics.json` + `strategy_returns_*.csv` required per split),
- required `test` split (train/validation only is treated as invalid),
- repeated `--config` passthrough to split backtests for config parity with canonical backtest runs.

Promotion integrity defaults:
- fail-closed evidence mode (`walkforward_summary.json` with `per_strategy_split_metrics` is required when blueprints exist),
- no silent per-blueprint fallback when strategy-level walkforward evidence is missing,
- override requires explicit `--promotion_allow_fallback_evidence 1` in `run_all.py` (or `--allow_fallback_evidence 1` in `promote_blueprints.py`).

Report integrity defaults:
- fail-closed backtest artifact contract (requires `metrics.json`, `equity_curve.csv`, and trade evidence),
- requires valid `metrics.cost_decomposition.net_alpha` (no implicit zero fallback),
- no silent backtest-directory fallback unless explicitly enabled (`--report_allow_backtest_artifact_fallback 1`).

## Artifact Expectations

### Discovery complete
- `data/runs/<run_id>/*.json|*.log`
- `data/reports/<event_family>/<run_id>/...`
- `data/reports/phase2/<run_id>/<event_family>/...`
- `data/reports/strategy_blueprints/<run_id>/...`
- `data/reports/strategy_builder/<run_id>/...`

### Full downstream enabled
- `data/runs/<run_id>/engine/strategy_returns_*.csv`
- `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/metrics.json`
- `data/reports/eval/<run_id>/walkforward_summary.json`
- `data/reports/promotions/<run_id>/promotion_report.json`
- `data/reports/vol_compression_expansion_v1/<run_id>/summary.json`

## Common Misread

"Run succeeded but no promotion/backtest/eval files" usually means:
- run used discovery defaults and downstream flags were not enabled.

## Practical checks

- Stage manifests exist under `data/runs/<run_id>/`.
- If `backtest_strategies.json` is missing, downstream was not run.
- If `build_strategy_candidates.json` reports `edge_rows_seen=0`, strategy builder had no edge universe input.
