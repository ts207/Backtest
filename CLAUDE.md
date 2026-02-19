# Backtest

Crypto event-driven strategy discovery and backtesting system (Binance perp/spot, 5m candles).

## Environment

```bash
export BACKTEST_DATA_ROOT=/path/to/data   # Required - all artifacts live here
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
```

## Commands

```bash
# Tests
pytest -q                          # All tests
pytest -q -m "not slow"            # Fast profile (CI-equivalent)
pytest tests/test_sanity.py -v     # Single file

# Pipeline (via Makefile)
make run                           # ingest + clean + features + context
make discover-edges                # Full discovery (phase1 + phase2 + export)
make discover-edges-from-raw       # Discovery using existing raw data (skip ingest)
make test                          # Full test suite
make test-fast                     # Fast tests only
make check-hygiene                 # Enforce repo hygiene constraints

# Direct entry point
python project/pipelines/run_all.py \
  --run_id discovery_2020_2025 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 --end 2025-07-10
```

## Architecture

```
project/
├── pipelines/run_all.py      # MAIN ENTRY POINT - master orchestrator (1179 lines)
│   ├── ingest/               # Binance API → raw Parquet
│   ├── clean/                # Normalization
│   ├── features/             # Feature + context construction
│   ├── research/             # Phase-1 hypothesis gen, Phase-2 conditional analysis
│   ├── backtest/             # Strategy execution
│   ├── eval/                 # Walkforward splits
│   └── report/               # Promotion and reporting
├── engine/runner.py          # Execution engine (P&L, risk, fills)
├── strategies/               # Strategy implementations + DSL interpreter
├── strategy_dsl/             # Blueprint schema and contract validation
├── features/                 # Reusable feature logic
└── configs/                  # pipeline.yaml, fees.yaml, venue configs

tests/                        # 75+ test files, 269 functions
data/                         # Runtime artifacts (gitignored)
docs/                         # Operational docs and runbooks
```


# Gemini Project Context: Backtest

## Project Overview
**Backtest** is a high-performance, event-driven quantitative trading research and backtesting platform specialized for crypto markets (Binance Perpetual and Spot). It features a multi-phase discovery pipeline transitioning from raw market data ingestion to hypothesis generation, conditional analysis, strategy building, and walk-forward evaluation.

### Key Technologies
- **Language**: Python 3.x
- **Data Engineering**: Pandas, NumPy, PyArrow (Parquet storage)
- **Architecture**: Subprocess-driven orchestration, **Spec-First** design.
- **Testing**: Pytest with isolated data roots and automated spec validation.

### Core Architecture
- `project/pipelines/run_all.py`: Master orchestrator for 13+ pipeline stages.
- `project/engine/`: Core execution (P&L, risk, fills).
- `project/strategies/`: Strategy implementations and DSL interpreter.
- `spec/`: **Source of Truth** for concepts (`spec/concepts/`), features (`spec/features/`), events (`spec/events/`), and tests (`spec/tests/`).
- `data/`: Local artifact root (Gitignored). Defined by `BACKTEST_DATA_ROOT`.

---

## Strategic Roadmap (Vertical Slices)

1.  **Slice 1: Data Layer**: Reliable 1m bars, ToB aggregates, and basis state.
2.  **Slice 2: PIT Features & Labels**: Feature construction with strict "Point-in-Time" guards.
3.  **Slice 3: Event Registry**: Detection of liquidity stress, vol shocks, and funding extremes.
4.  **Slice 4: Validation (Phase 2)**: Candidate generation with BH-FDR multiplicity control.
5.  **Slice 5: Bridge**: Executed-like simulation (bid/ask fills, latency, slippage).
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

### 4. Atlas Verification Loop
- **Claim Mapping**: Atlas claims are linked to executable Test IDs via `claim_test_map.csv`.
- **Verification Log**: Every run must append to `claim_verification_log.parquet` to update claim status (`supported_in_env` vs `unsupported_in_env`).

### 5. Golden Baseline
The "Certification Batch" (Run ID: `certification_batch`, Jan 1-7 2024, BTC/ETH/SOL) serves as the regression baseline.
- **Golden Artifacts**: Stored in `golden/` directory.
    - `e1_report.json`: Gate E-1 results.
    - `phase2_fdr.parquet`: Phase 2 multiplicity results.
    - `claim_verification_log.parquet`: Atlas claim verification.
- **Contract Tests**: `tests/test_audit_compliance.py` verifies current run outputs against this baseline.

---

## Current Operational State

### Microstructure Calibration
The system is fully calibrated for **BTCUSDT**, **ETHUSDT**, and **SOLUSDT** (Feb 1–7, 2026).
- **Amihud Ratio**: Updated threshold > 0.5 (reflecting liquidity provision regimes).
- **VPIN**: Tests for correlation magnitude (abs > 0.1) rather than direction.

### Infrastructure Implemented
- `spec_qa_linter.py`: Enforces spec integrity.
- `validate_event_quality.py`: Implements Gate E-1.
- `phase2_candidate_discovery.py`: Implements spec-bound candidate discovery with BH-FDR.
- `verify_atlas_claims.py`: Automates Knowledge Atlas claim verification.

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
