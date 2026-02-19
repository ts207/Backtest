# Backtest: Quantitative Trading Research Platform

## 🛠️ Environment & Setup
```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

## 🚀 Key Commands
| Command | Description |
|:---|:---|
| `make run` | Ingest + Clean + Build Features |
| `make discover-edges` | Full Discovery: Phase 1 + Phase 2 + Export |
| `make test-fast` | Fast tests (excludes `@pytest.mark.slow`) |
| `make check-hygiene` | Enforce repo hygiene and spec linter |

## 🧠 Session Updates (Feb 19, 2026)

### 1. Atlas-Driven Discovery Loop
Implemented a two-stage deterministic planning workflow:
- **Global Planner**: `generate_candidate_templates.py` translates Atlas backlog into reusable templates and identifies missing specs (`spec_tasks.parquet`).
- **Per-Run Enumerator**: `generate_candidate_plan.py` generates a budgeted, symbol-expanded `candidate_plan.jsonl` with feasibility reporting.
- **Enforcement**: `phase2_candidate_discovery.py` now requires `--atlas_mode 1` to follow the candidate plan and records the plan hash for reproducibility.

### 2. Event: LIQUIDATION_CASCADE
- **Spec**: `spec/events/LIQUIDATION_CASCADE.yaml` (Liquidation spike + OI drop).
- **Analyzer**: `analyze_liquidation_cascade.py` implemented with severity bucketing.
- **Registry**: Registered in `project/events/registry.py`.

### 3. Integrity & Reporting Invariants
- **Cost Invariant**: `cost_config_digest` is now enforced in `Blueprint.lineage` and `compile_strategy_blueprints.py` selection logic.
- **Phase 2 Metrics**: Refined reporting to distinguish `discoveries_statistical` from `survivors_phase2`. Fixed FDR estimation (sum of q-values).
- **Bug Fixes**: 
    - Fixed `ts` vs `timestamp` column mismatch in liquidation ingestor.
    - Corrected `oi_delta_1h` window (12 bars for 1h @ 5m).
    - Prevented double-prepending of `PROJECT_ROOT` in config resolution.

### 4. Documentation
Created a comprehensive documentation suite in `docs/`:
- `ARCHITECTURE.md`: Pipeline orchestration and data flow.
- `CONCEPTS.md`: PIT, Multiplicity, and Blueprints.
- `GETTING_STARTED.md`: First run and Atlas mode guide.
- `SPEC_FIRST.md`: How to use YAML specs as the source of truth.

## 🏗️ Core Architecture
- `project/pipelines/run_all.py`: Master orchestrator for 16+ stages.
- `project/engine/`: Core execution (P&L, risk, fills).
- `project/strategy_dsl/`: Blueprint schema and contract validation (`contract_v1.py`).
- `spec/`: Source of Truth for concepts, features, and events.
- `atlas/`: Global planning artifacts (`candidate_templates.parquet`).

## 🛡️ Core Mandates
1.  **Specs as Source of Truth**: All logic must be spec-bound.
2.  **Point-in-Time (PIT) Integrity**: Strict guards against look-ahead bias.
3.  **Cost Reproducibility**: All discoveries must be bound to a `cost_config_digest`.
4.  **Fail-Closed Selection**: Selection must verify costs and statistical discovery.
