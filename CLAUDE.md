# Backtest: Quantitative Trading Research Platform

## 🛠️ Environment & Setup

```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

## 🚀 Key Commands

| Command | Description |
| :--- | :--- |
| `make run` | Ingest + Clean + Build Features |
| `make discover-edges` | Full Discovery: Phase 1 + Phase 2 + Export |
| `make test-fast` | Fast tests (excludes `@pytest.mark.slow`) |
| `make check-hygiene` | Enforce repo hygiene and spec linter |

## 🧠 Session Updates (Feb 23, 2026)

### A1: Execution Timeframe Wiring

- Fixed hardcoded `"15m"` path lookups in `runner.py` — all research data is at `"5m"`.
- Added `BARS_PER_YEAR` dict + `_DEFAULT_TIMEFRAME = "5m"` to `runner.py`.
- `run_engine()`, `backtest_strategies.py`, `run_walkforward.py`, `run_all.py` all now accept `--timeframe` / `--backtest_timeframe`.
- Sharpe annualization now uses `BARS_PER_YEAR.get(timeframe)` instead of hardcoded constant.

### B1 + B2 + PR-5: Audit Gate Fixes (completed)

- **B1** (F-1 + F-10): Fallback blueprints are now banned from OOS evaluation artifacts.
- **B2** (F-4): Walk-forward embargo default changed from 0 → 1 day.
- **PR-5** (F-8): Liquidation symbol map validated post-ingest; 31 unit tests added.

### Repo Cleanup (Feb 23)

- 216 tracked + 694 on-disk Zone.Identifier sidecar files removed.
- Dead code (unreachable `PATCH` blocks) removed from `runner.py` and `backtest_strategies.py`.
- `project/features/__init__.py` fixed to use relative imports.

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

## 🔧 Environment Quirks

- Use `source .venv/bin/activate && python -m pytest` — `python` is not in PATH
- Always exclude pre-existing failures: `--ignore=tests/pipelines/backtest/test_compile_fallback.py --ignore=tests/pipelines/research/test_compile_fallback.py`
- `project/features/__init__.py` must use relative imports (`from .module import`) — absolute `project.features.*` breaks subprocess context; bare `features.*` breaks pytest context

## 🧹 Zone.Identifier Cleanup (Windows NTFS sidecars)

- `git rm --cached` removes from index only; delete from disk with `find <dir> -name '*Zone.Identifier*' -delete`
- Use `git ls-files -z` (null-delimited) to handle unicode filenames in scripts — plain `git ls-files` output is shell-quoted and breaks subprocess calls
- `.gitignore` pattern must be `*Zone.Identifier` (glob) — `*#Uf03aZone.Identifier` is not a valid gitignore escape and does nothing

## 🏗️ Core Architecture

- `project/pipelines/run_all.py`: Master orchestrator for 30+ stages.
- `project/engine/`: Core execution (P&L, risk, fills).
- `project/strategy_dsl/`: Blueprint schema and contract validation (`contract_v1.py`).
- `spec/`: Source of Truth for concepts, features, and events.
- `atlas/`: Global planning artifacts (`candidate_templates.parquet`).

## 🛡️ Core Mandates

1. **Specs as Source of Truth**: All logic must be spec-bound.
2. **Point-in-Time (PIT) Integrity**: Strict guards against look-ahead bias.
3. **Cost Reproducibility**: All discoveries must be bound to a `cost_config_digest`.
4. **Fail-Closed Selection**: Selection must verify costs and statistical discovery.
