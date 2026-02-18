# Discovery Pipeline

## Purpose

The discovery pipeline converts raw market data into auditable strategy hypotheses and blueprints.

High-level chain:
1. ingest
2. clean
3. features + context
4. hypothesis queue generation (proposal layer)
5. phase-1 event analyzers
6. canonical event registry build (`build_event_registry.py`)
7. phase-2 conditional hypotheses
8. bridge evaluation (tradability gates on executed after-cost metrics)
9. edge/blueprint/strategy handoff artifacts

## Orchestrator

Entry point:
- `project/pipelines/run_all.py`

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

Market-scoped manifest naming:
- perp manifests remain:
  - `data/runs/<run_id>/build_cleaned_15m.json`
  - `data/runs/<run_id>/build_features_v1.json`
- spot manifests are isolated:
  - `data/runs/<run_id>/build_cleaned_15m_spot.json`
  - `data/runs/<run_id>/build_features_v1_spot.json`

### B) Research
- hypothesis queue (`generate_hypothesis_queue.py`)
- phase-1 family analyzers (`analyze_*`)
- canonical event registry (`build_event_registry.py`)
- phase-2 per-family conditional hypotheses (`phase2_conditional_hypotheses.py`)
- bridge tradability evaluation (`bridge_evaluate_phase2.py`)
- naive-entry validation (`evaluate_naive_entry.py`)
- checklist (`generate_recommendations_checklist.py`)

Checklist auto-continue behavior:
- `run_all.py` default: `--auto_continue_on_keep_research 0` (fail-closed)
- If checklist decision is `KEEP_RESEARCH` and execution is requested:
  - default behavior: pipeline stops before execution stages
  - override behavior (`--auto_continue_on_keep_research 1`): orchestrator injects:
  - compiler: `--ignore_checklist 1 --allow_fallback_blueprints 1`
  - builder: `--ignore_checklist 1 --allow_non_promoted 1`
- Discovery-only runs remain strict (no auto-bypass).
- `run_manifest.json` records:
  - `checklist_decision`
  - `auto_continue_applied`
  - `auto_continue_reason`
  - `execution_blocked_by_checklist`
  - `non_production_overrides`

Phase-2 now consumes `data/reports/hypothesis_generator/<run_id>/phase1_hypothesis_queue.*` when present and records matched hypothesis context per event family.
Phase-2 Tier-1 hardening adds:
- registry-parity enforcement (phase1 counts must match `data/events/<run_id>/events.parquet`),
- cost-aware promotion fields:
  `after_cost_expectancy_per_trade`, `stressed_after_cost_expectancy_per_trade`,
  `turnover_proxy_mean`, `avg_dynamic_cost_bps`, `cost_ratio`,
  `cost_input_coverage`, `cost_model_valid`, `gate_cost_model_valid`,
- validation-only selection policy with dual labels:
  legacy (`validation_*`, `test_*`) + canonical aliases (`val_*`, `oos1_*`),
- multiplicity now keyed from validation p-values (`val_p_value_adj_bh`).
- ESS gate (`--min_ess`, `--ess_max_lag`)
- multiplicity penalty (`--multiplicity_k`) with `expectancy_after_multiplicity`
- strict multiplicity gate (`gate_multiplicity_strict`)

Phase-2 Tier-2 hardening adds:
- parameter-curvature robustness (`--parameter_curvature_max_penalty`) with neighborhood fields:
  `expectancy_left|center|right`, `curvature_penalty`, `gate_parameter_curvature`
- delay robustness (`--delay_grid_bars`, `--min_delay_positive_ratio`, `--min_delay_robustness_score`) with:
  `delay_expectancy_map`, `delay_positive_ratio`, `delay_robustness_score`, `gate_delay_robustness`

Bridge-first tradability hardening adds:
- strict candidate type split (`candidate_type`: `standalone` vs `overlay`) and `overlay_base_candidate_id`,
- bridge outputs:
  `bridge_train_after_cost_bps`, `bridge_validation_after_cost_bps`,
  `bridge_validation_stressed_after_cost_bps`, `bridge_validation_trades`,
  `bridge_effective_cost_bps_per_trade`, `bridge_gross_edge_bps_per_trade`,
- bridge gates:
  `gate_bridge_has_trades_validation`, `gate_bridge_after_cost_positive_validation`,
  `gate_bridge_after_cost_stressed_positive_validation`, `gate_bridge_edge_cost_ratio`,
  `gate_bridge_turnover_controls`, `gate_bridge_tradable`,
- executed ranking signal:
  `selection_score_executed` (used ahead of proxy quality score in downstream selection).

### C) Strategy preparation
- blueprint compiler (`compile_strategy_blueprints.py`)
- strategy builder (`build_strategy_candidates.py`)

Strategy-prep strictness now includes naive-entry contract:
- compiler requires `data/reports/naive_entry/<run_id>/naive_entry_validation.csv` by default,
- candidates must have `naive_pass=true` unless explicitly overridden with `--allow_naive_entry_fail 1`.
- compiler applies explicit action semantics overlays (`entry_gate_skip`, `risk_throttle_*`) and drops runtime-equivalent behavior duplicates.
- builder merges promoted blueprints and edge candidates with deterministic precedence:
  - promoted blueprint > edge candidate > alpha bundle,
  - then `selection_score` desc, then `strategy_candidate_id` asc.

### D) Optional downstream execution
- backtest (`backtest_strategies.py`)
- walkforward (`run_walkforward.py`)
- promotion (`promote_blueprints.py`)
- report (`make_report.py`)

Walkforward integrity defaults:
- fail-closed artifact validation (`metrics.json` + `strategy_returns_*.csv` required per split),
- required `test` split (train/validation only is treated as invalid),
- expected strategy set is enforced in strategy and blueprint mode; unexpected `strategy_returns_*.csv` fail by default (`--allow_unexpected_strategy_files 0`),
- split backtests clean run-local engine artifacts by default (`--clean_engine_artifacts 1`) when force rerunning,
- repeated `--config` passthrough to split backtests for config parity with canonical backtest runs.
- per-strategy regime evidence (`per_strategy_regime_metrics`) with `--regime_max_share`.
- per-strategy drawdown clustering evidence (`per_strategy_drawdown_cluster_metrics`) with:
  `max_loss_cluster_len`, `cluster_loss_concentration`, `tail_conditional_drawdown_95`.

Promotion integrity defaults:
- fail-closed evidence mode (`walkforward_summary.json` with `per_strategy_split_metrics` is required when blueprints exist),
- no silent per-blueprint fallback when strategy-level walkforward evidence is missing,
- override requires explicit `--promotion_allow_fallback_evidence 1` in `run_all.py` (or `--allow_fallback_evidence 1` in `promote_blueprints.py`).
- regime gate can consume walkforward strategy regime evidence directly (`regime_evidence_source=walkforward_strategy`).
- drawdown-cluster gates consume walkforward strategy evidence directly:
  `max_loss_cluster_len`, `max_cluster_loss_concentration`, `min_tail_conditional_drawdown_95`.
- realized train/validation cost ratio gate from strategy returns:
  `--max_cost_ratio_train_validation` (default `0.60`).

Report integrity defaults:
- fail-closed backtest artifact contract (requires `metrics.json`, `equity_curve.csv`, and trade evidence),
- deterministic evidence source:
  - `trades_*.csv` is primary if present,
  - engine fallback is only used when trade files are absent and is scoped to `metrics.metadata.strategy_ids`,
- strict trade-count check when trade files exist (`len(trades_*.csv rows) == metrics.total_trades`),
- requires valid `metrics.cost_decomposition.net_alpha` (no implicit zero fallback),
- no silent backtest-directory fallback unless explicitly enabled (`--report_allow_backtest_artifact_fallback 1`).

Historical-run policy:
- runs generated before Tier-1.5 artifact hygiene are invalid for decisioning until rebuilt with `--force 1`.
- example invalidated run: `RUN_2022_2023`.

## Artifact Expectations

### Discovery complete
- `data/runs/<run_id>/*.json|*.log`
- `data/runs/<run_id>/run_manifest.json` (git commit, data hash, feature schema provenance, config digest)
- `data/reports/<event_family>/<run_id>/...`
- `data/events/<run_id>/events.parquet`
- `data/events/<run_id>/event_flags.parquet`
- `data/reports/phase2/<run_id>/<event_family>/...`
- `data/reports/phase2/<run_id>/<event_family>/candidates_costed.jsonl`
- `data/reports/naive_entry/<run_id>/naive_entry_validation.csv|.json`
- `data/reports/strategy_blueprints/<run_id>/...`
- `data/reports/strategy_builder/<run_id>/...`

### Full downstream enabled
- `data/runs/<run_id>/engine/strategy_returns_*.csv`
- `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/metrics.json`
- `data/reports/eval/<run_id>/walkforward_summary.json`
- `data/reports/eval/<run_id>/selection_log.json`
- `data/reports/promotions/<run_id>/promotion_report.json`
- `data/reports/vol_compression_expansion_v1/<run_id>/summary.json`

## Common Misread

"Run succeeded but no promotion/backtest/eval files" usually means:
- run used discovery defaults and downstream flags were not enabled.

## Practical checks

- Stage manifests exist under `data/runs/<run_id>/`.
- If `backtest_strategies.json` is missing, downstream was not run.
- If `build_strategy_candidates.json` reports `edge_rows_seen=0`, strategy builder had no edge universe input.
