# Backtest

Event-driven quantitative research platform for crypto perpetual markets.
Discovers, validates, and compiles tradable strategy blueprints from raw market data
through a multi-stage pipeline with point-in-time safety, multiplicity control, and walk-forward evaluation.

## What It Does

1. Ingests Binance USDT-M perpetual (and optional spot) OHLCV, funding, OI, and liquidation data.
2. Builds cleaned 5-minute bars with rigorous gap tracking and exact funding timestamp alignment (no smearing).
3. Engineers features (volatility, microstructure, funding, basis, context states).
4. Detects 56 event families across 10 analyzer categories.
5. Runs unified conditional hypothesis discovery (Phase 2) with BH-FDR multiplicity control, stratified by symbol.
6. Applies bridge tradability checks, candidate promotion, and edge registry tracking.
7. Executes expectancy analysis, recommendations profiling, and strategy builder / blueprint compilation.
8. Backtests compiled blueprints through a discrete-position engine and runs out-of-sample walk-forward evaluation regimes.

## Quick Start

```bash
# 1. Setup
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export BACKTEST_DATA_ROOT=$(pwd)/data

# 2. Run core pipeline (ingest + clean + features + events)
python project/pipelines/run_all.py \
  --run_id my_first_run \
  --symbols BTCUSDT \
  --start 2024-01-01 \
  --end 2024-06-30

# 3. Run full hybrid discovery (Phase 1 + 2 + Expectancy Analysis)
make discover-hybrid RUN_ID=discover_all SYMBOLS=BTCUSDT,ETHUSDT START=2024-01-01 END=2024-12-31

# 4. Run discovery + backtesting
make discover-hybrid-backtest RUN_ID=test_run SYMBOLS=BTCUSDT STRATEGIES=candidate_1

# 5. Run tests (Fast profile available)
make test-fast
```

## Pipeline Stages

```
raw market data (Binance perp/spot)
  |
  v
ingest_ohlcv_5m, ingest_funding, [ingest_spot, ingest_oi, ingest_liquidations]
  |
  v
build_cleaned_5m  -->  Cleaned5mBarsSchema validation (gap masked, flat prices avoided)
  |
  v
build_features_v1  -->  feature_schema_v1.json validation
  |
  v
build_funding_persistence, build_market_state, build_universe_snapshot
  |
  v
Phase 1 analyzers (analyze_*.py)  -->  raw event CSVs
  |
  v
build_event_registry  -->  events.parquet + event_flags.parquet
  |
  v
Phase 2 conditional discovery  -->  BH-FDR controlled, symbol-stratified candidate hypotheses
  |
  v
[expectancy_analysis], [bridge_evaluate_phase2]  -->  Economic viability & robustness scoring
  |
  v
promote_candidates, update_edge_registry  -->  promoted_candidates.parquet
  |
  v
compile_strategy_blueprints, build_strategies  -->  blueprints.jsonl + blueprints.yaml
  |
  v
[backtest_strategies]  -->  discrete engine PnL traces
  |
  v
[walkforward_eval]  -->  OOS validation & drawdown regime clustering
  |
  v
[make_report]  -->  performance attribution reports
```

Stages in brackets are optional and enabled via orchestrator CLI flags.

## Make Targets

| Target | Description |
|--------|-------------|
| `make run` | Ingest + clean + features + context |
| `make discover-edges` | Full discovery chain (all 56 event families) |
| `make discover-edges-from-raw` | Discovery using existing raw data (skip ingest) |
| `make discover-hybrid` | Discover edges + expectancy checks (research-speed mode) |
| `make discover-hybrid-backtest` | Discover hybrid + backtest + report |
| `make baseline STRATEGIES=...` | Core pipeline + backtest + report |
| `make test-fast` | Fast tests (excludes slow markers) |
| `make test` | Full test suite |
| `make compile` | Byte-compile all Python modules |
| `make check-hygiene` | Repository hygiene checks |
| `make clean-runtime` | Remove run/report artifacts |

## Output Layout

All outputs are rooted at `$BACKTEST_DATA_ROOT` (default: `./data`).

```
data/
  lake/
    raw/binance/perp/{symbol}/ohlcv_5m/      Raw OHLCV parquet
    raw/binance/perp/{symbol}/funding/        Raw funding rates
    cleaned/perp/{symbol}/bars_5m/            Cleaned 5m bars
    features/perp/{symbol}/5m/features_v1/   Engineered features
    runs/{run_id}/                            Run-scoped datasets
  events/{run_id}/
    events.parquet                            Canonical event registry
    event_flags.parquet                       Per-bar event flags
  reports/
    phase2/{run_id}/{event_type}/             Phase 2 candidates
    bridge_eval/{run_id}/{event_type}/        Bridge evaluation
    strategy_builder/{run_id}/                Built strategies
    strategy_blueprints/{run_id}/             Compiled blueprints
  runs/{run_id}/
    run_manifest.json                         Run-level provenance
    *.json                                    Stage manifests
    *.log                                     Stage logs
```

## Documentation

| Document | Purpose |
|----------|---------|
| [Getting Started](docs/GETTING_STARTED.md) | Setup, first run, and common workflows |
| [Architecture](docs/ARCHITECTURE.md) | Pipeline structure, data flow, and execution model |
| [Concepts](docs/CONCEPTS.md) | PIT safety, event semantics, discovery pipeline, robustness |
| [Spec-First Development](docs/SPEC_FIRST.md) | How specs govern behavior and how to extend the system |
| [Audit Baseline](docs/AUDIT.md) | Resolved and remaining audit findings |
| [Runbook](RUNBOOK.md) | Operational reference for running and debugging |
| [Research Playbook](RESEARCH_PLAYBOOK.md) | Research philosophy and methodology |

## Requirements

- Python 3.12+
- Dependencies: see `requirements.txt`
- OS: Linux / macOS / WSL
