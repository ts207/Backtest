# Gemini Project Context: Backtest

## Project Overview
**Backtest** is a high-performance, event-driven quantitative trading research and backtesting platform specialized for crypto markets (Binance Perpetual and Spot). It features a multi-phase discovery pipeline transitioning from raw market data ingestion to hypothesis generation, conditional analysis, strategy building, and walk-forward evaluation.

### Key Technologies
- **Language**: Python 3.x
- **Data Engineering**: Pandas, NumPy, PyArrow (Parquet storage)
- **Architecture**: Subprocess-driven orchestration, **Spec-First** design.
- **Testing**: Pytest with isolated data roots and automated spec validation.
- **Management**: **Conductor** spec-driven development framework.

### Core Architecture
- `project/pipelines/run_all.py`: Master orchestrator for 13+ pipeline stages.
- `project/engine/`: Core execution (P&L, risk, fills).
- `project/strategies/`: Strategy implementations and DSL interpreter.
- `spec/`: **Source of Truth** for concepts (`spec/concepts/`), features (`spec/features/`), events (`spec/events/`), and tests (`spec/tests/`).
- `data/`: Local artifact root (Gitignored). Defined by `BACKTEST_DATA_ROOT`.
- `conductor/`: Project management, tracks, and workflow definitions.

---

## Strategic Roadmap (Vertical Slices)

1.  **Slice 1: Data Layer**: Reliable 1m bars, ToB aggregates, and basis state.
2.  **Slice 2: PIT Features & Labels**: Feature construction with strict "Point-in-Time" guards.
3.  **Slice 3: Event Registry**: Detection of liquidity stress, vol shocks, and funding extremes.
4.  **Slice 4: Validation (Phase 2)**: Candidate generation with BH-FDR multiplicity control.
5.  **Slice 5: Bridge**: Executed-like simulation (bid/ask fills, latency, slippage) and **Performance Attribution**.
6.  **Slice 6: Strategy & Walkforward**: Robustness testing and parameter sweeps.
7.  **Slice 7: Portfolio**: Cluster-based allocation and portfolio execution simulation.

---

## Core Invariants & Mandates

### 1. Specs as Source of Truth (Mandatory)
- **Phase 2 Constraint**: Phase 2 must only read from `spec/concepts/*.yaml`, `spec/features/*.yaml`, `spec/events/*.yaml`, `spec/tests/*.yaml`, `spec/gates.yaml`, and `spec/multiplicity/families.yaml`.
- **Traceability**: Phase 2 reports MUST emit `spec_hashes`, `dataset_hash`, and `code_commit` to ensure results are bound to specific thresholds.

### 2. Event Quality Gate (Gate E-1)
Before entering Phase 2, every event type (e.g., `liquidity_vacuum`) must pass the **E-1 Gate**:
- **Prevalence**: Must be within [1, 500] events per 10k bars.
- **Join Rate**: Must achieve > 99% join success with PIT features and forward labels.
- **Artifact**: `event_quality_report.json` must be generated per symbol/run.

### 3. Phase 2 Candidate Contract
- **Family Definition (Option A)**: Multiplicity correction (BH-FDR) is applied at the `(event_type, rule_template, horizon)` level.
- **Minimal Templates**: Start with `mean_reversion` and `continuation` across `5m`, `15m`, and `60m` horizons.
- **Validation Gates**:
    - **Economic**: Positive after-cost expectancy under conservative sweeps.
    - **Stability**: Sign stability across time and regime slices.
    - **Multiplicity**: FDR discovery at `q <= 0.05`.
- **Required Outputs**: `candidates_raw.parquet`, `pvals.parquet`, `fdr.parquet`, `report.json`.

---

## Current Operational State

### Microstructure Calibration
The system is fully calibrated for **BTCUSDT**, **ETHUSDT**, and **SOLUSDT** (Feb 1–7, 2026).
- **Amihud Ratio**: Updated threshold > 0.5 (reflecting liquidity provision regimes).
- **VPIN**: Tests for correlation magnitude (abs > 0.1) rather than direction.

### Performance Attribution & Economic Evaluation
The platform now supports robust attribution of candidate performance to market regimes.
- **PIT Joiner**: Exact timestamp/symbol matching for candidates and features.
- **Regime Metrics**: Aggregated P&L, Sharpe Ratio, and **Max Drawdown** per volatility/liquidity regime.
- **Cost Model**: Binance-specific fee and slippage modeling integrated into the Bridge phase.

### Infrastructure Implemented
- `project/eval/attribution_joiner.py`: Joins Phase 2 candidates with PIT features.
- `project/eval/performance_attribution.py`: Calculates regime-specific performance metrics.
- `project/eval/cost_model.py`: Applies fee and slippage models to gross P&L.
- `project/pipelines/report/performance_attribution_report.py`: Exports attribution results to Parquet.
- `spec_qa_linter.py`: Enforces spec integrity.
- `validate_event_quality.py`: Implements Gate E-1.

---

## Building and Running

### Environment Setup
```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

### Key Commands
| Command | Description |
|:---|:---|
| `make run` | ingest + clean + features + context |
| `make discover-edges` | Full discovery: Phase 1 + Phase 2 + Export |
| `make check-hygiene` | Enforce repo hygiene and spec linter |
| `make test-fast` | Fast tests (excludes `@pytest.mark.slow`) |
| `/conductor:setup` | Re-initialize or check project management state |
| `/conductor:implement` | Select and implement a pending track |
