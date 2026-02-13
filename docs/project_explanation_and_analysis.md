# Project Explanation and Analysis

## 1) Executive Summary

This repository is a crypto research and backtesting platform focused on **event-driven edge discovery** before strategy deployment.  
Its core workflow is:

1. Ingest and normalize market datasets.
2. Build cleaned bars, base features, and context signals.
3. Run Phase 1 event analyzers (structure tests).
4. Run Phase 2 conditional hypothesis gates (conditions/actions).
5. Export edge candidates and validate expectancy/robustness.
6. Build strategy candidates and hand off to manual backtesting.

The system is strongest in:
- deterministic stage orchestration,
- event-level diagnostics and gating,
- strategy candidate generation from promoted research outputs,
- practical operator tooling and run-scoped artifacts.

The system is currently weaker in:
- explicit multiple-testing correction breadth (partially improved),
- full execution realism (maker/taker + impact dynamics),
- explicit portfolio-level risk cap enforcement,
- strict walk-forward/promotion parity controls.

## 2) Repository Structure and Responsibilities

Top-level:
- `project/`: core runtime code.
- `tests/`: pytest coverage for pipelines, adapters, IO contracts, and gates.
- `docs/`: operator and architecture references.
- `data/`: runtime lake/runs/reports outputs (mostly git-ignored).

Core code zones:
- `project/pipelines/ingest/`: exchange dataset ingestion (OHLCV, funding, liquidation, OI).
- `project/pipelines/clean/`: cleaned 15m bars + funding alignment.
- `project/pipelines/features/`: base features and context signals.
- `project/pipelines/research/`: phase analyzers, hypothesis generation, expectancy, robustness, checklist.
- `project/pipelines/backtest/`: strategy backtest orchestration.
- `project/pipelines/report/`: run report generation.
- `project/engine/`: strategy execution + PnL routines.
- `project/strategies/`: base strategies and adapters.
- `project/configs/`: pipeline and fee defaults.

## 3) End-to-End Runtime Architecture

### 3.1 Orchestration entrypoint
`project/pipelines/run_all.py` is the canonical orchestrator:
- defines CLI contract for run parameters (`run_id`, `symbols`, `start`, `end`, flags).
- appends stage scripts in deterministic order.
- propagates stage logs/manifests under `data/runs/<run_id>/`.
- supports optional research/backtest/report stages.
- auto-includes spot pipeline when cross-venue analysis is requested.

### 3.2 Data flow model
The runtime uses layered storage:
- Raw: `data/lake/raw/...` (exchange-origin tables).
- Cleaned: `data/lake/cleaned/...` (normalized bars + aligned funding).
- Features: `data/lake/features/...` and run-scoped context under `data/lake/runs/<run_id>/context/...`.
- Research outputs: `data/reports/...`.
- Backtest outputs: `data/lake/trades/backtests/...` + `data/runs/<run_id>/engine/...`.

This model gives:
- traceable stage boundaries,
- clear recompute behavior,
- artifact-level debuggability.

## 4) Pipeline Stage Analysis

### 4.1 Ingestion
Implemented ingestors include:
- perp OHLCV (`ingest_binance_um_ohlcv_15m.py`),
- spot OHLCV (`ingest_binance_spot_ohlcv_15m.py`),
- funding (`ingest_binance_um_funding.py`),
- liquidation snapshot,
- open interest history.

Key characteristics:
- archive-first retrieval with daily fallback.
- API fallback for funding/OI tails where needed.
- partition completeness checks.
- timestamp normalization and schema sanity.
- run manifests with per-symbol ingestion stats.

Strength:
- robust practical handling of Binance archive/API realities.

Risk:
- vendor schema/time semantics can still drift; centralized schema-version registry is limited.

### 4.2 Clean stage
`build_cleaned_15m.py`:
- enforces OHLCV/funding sanity checks,
- aligns funding to bar grid,
- tracks gap/missing metrics,
- persists per-month cleaned/funding partitions.

Strength:
- explicit handling of missing/constant/misaligned funding modes.

Risk:
- audit remains distributed in stage-level stats rather than a single hardened data-readiness gate artifact.

### 4.3 Features and context
`build_features_v1.py` computes baseline features:
- returns and volatility proxies,
- rolling range and percentile context,
- funding fields.

Context builders:
- `build_context_features.py` for funding persistence state.
- `build_market_context.py` for trend/vol/compression/funding regime labels.

Strength:
- stable, low-footprint feature baseline with run-scoped context.

Risk:
- advanced mechanism features (basis/borrow/execution microstructure) are partly externalized or optional.

### 4.4 Research and discovery
Research stack includes:
- phase-1 family analyzers,
- phase-2 conditional hypothesis gates,
- hypothesis queue generation,
- expectancy analysis,
- robustness/trap validation,
- recommendations checklist.

Phase 2 provides:
- condition/action generation,
- CI and regime stability gates,
- simplicity caps,
- promoted candidate outputs and manifests.

Strength:
- explicit go/no-go logic with explainable failure reasons.

Risk:
- promotion still depends heavily on heuristic/statistical gate design; external multiplicity safeguards needed across broader experiment sets.

### 4.5 Strategy candidate handoff
`build_strategy_candidates.py`:
- maps promoted edge events to strategy templates,
- emits ranked candidates with risk control hints,
- includes manual backtest commands,
- can optionally include AlphaBundle candidates.

Strength:
- clear bridge from research outputs to executable strategies.

Risk:
- backtest readiness varies by adapter/template availability.

### 4.6 Backtest and reporting
`backtest_strategies.py` + `engine/runner.py`:
- loads data/features/context,
- generates positions via registered strategies,
- computes portfolio metrics and trade logs,
- writes backtest artifacts and report inputs.

`make_report.py` consolidates:
- summary metrics,
- fee sensitivity,
- data quality and diagnostics,
- context segmentation and overlay bindings.

Recent hardening now includes:
- explicit cost decomposition payload,
- reproducibility metadata payload.

## 5) Strategy Layer Analysis

Current strategy registry includes:
- `vol_compression_v1`,
- event-family adapters (`liquidity_refill_lag_v1`, `liquidity_absence_gate_v1`, `forced_flow_exhaustion_v1`, `funding_extreme_reversal_v1`, `cross_venue_desync_v1`, `liquidity_vacuum_v1`).

`vol_compression_v1` behavior:
- compression entry rules,
- breakout confirmation,
- stop/target/time exits,
- optional adaptive trailing logic,
- one-trade-per-day guard.

Strength:
- deterministic and explainable position logic.

Risk:
- position sizing and portfolio constraints are still comparatively simple; not yet a full portfolio risk engine.

## 6) Quality, Testing, and Operational Maturity

Test suite:
- broad unit/integration checks across ingestion contracts, orchestration ordering, research gates, adapters, and utility behavior.
- repository standard command: `make test` (pytest).

Operational maturity signals:
- per-stage manifests and logs,
- deterministic stage wiring,
- run-scoped report artifacts,
- explicit gating documents and summaries.

Remaining operational risk:
- limited direct tests for full live-like execution behavior (latency/fill/queue realism),
- no built-in realtime monitor daemon for deployment drift.

## 7) Recent Requirement-Driven Improvements

Recently implemented high-risk/ROI improvements:

1. Multiplicity-aware expectancy gating (`R7`):
- Benjamini-Hochberg adjusted p-values added.
- expectancy evidence now requires adjusted significance.
- test-count and multiplicity metadata included in artifacts.

2. Reproducibility metadata (`R5`):
- backtest metrics now include config digest, git revision, and upstream snapshot hashes.

3. Cost decomposition (`R4`):
- backtest metrics/report now expose gross alpha, fees, slippage, impact, net alpha, and turnover.

These changes improve auditability, governance, and false-positive control with minimal structural disruption.

## 8) Strengths vs Risks (Concise Matrix)

Strengths:
- clear stage boundaries and artifact contracts,
- practical ingestion resilience,
- event-driven research discipline,
- usable strategy handoff workflow,
- solid test baseline.

Risks:
- partial execution realism,
- partial portfolio-level risk cap enforcement,
- incomplete full walk-forward promotion framework,
- ongoing maintenance burden for evolving exchange data contracts.

## 9) Recommended Technical Roadmap (Prioritized)

### Priority A (near-term, high leverage)
1. Formalize multiplicity accounting globally:
   - persistent experiment/test ledger across runs.
2. Promote reproducibility metadata to all research/report stages:
   - config digest + code revision + snapshot IDs everywhere.
3. Expand decomposition and cost realism:
   - maker/taker path + execution-mode assumptions in artifacts.

### Priority B (mid-term)
1. Portfolio risk engine:
   - leverage/exposure/liquidity caps with deterministic breach actions.
2. Walk-forward promotion contract:
   - explicit train/validation/test windows for promotion decisions.
3. Unified data-readiness gate:
   - single blocking artifact before expensive research/backtest runs.

### Priority C (longer-term)
1. Deployment-grade monitoring:
   - feed health, fill parity, cost drift, volatility/risk drift monitors.
2. Execution simulation upgrade:
   - latency/partial fills/queue-proxy and market-impact sensitivity.
3. Strategy template expansion:
   - funding carry delta-neutral and MEV-aware risk filters as first-class templates.

## 10) Conclusion

The project is already a strong research-first crypto backtesting platform with practical operational scaffolding.  
Its biggest opportunity is to evolve from robust **discovery infrastructure** into robust **deployment governance and execution realism**, while preserving the existing deterministic, artifact-driven workflow that makes iteration and audits tractable.

