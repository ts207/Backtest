# Full Project Report: Backtest Repository

_Date: 2026-02-10_

## 1) Executive Summary


## Addendum (implemented follow-up actions)

The following recommendations from this report have now been implemented:
- **P0:** Added `docs/architecture_diagram.md` and `docs/operator_runbook.md`.
- **P2:** Added two additional baseline strategies and registered them:
  - `vol_compression_momentum_v1`
  - `vol_compression_reversion_v1`

---


Backtest is a Python-based quantitative research and backtesting system focused on crypto perpetual futures at 15-minute resolution. The repository is organized around a staged pipeline (`ingest -> clean -> features -> backtest -> report`) with optional research and portfolio-validation branches. The codebase has strong modular separation (pipelines, engine, strategies, analyzers, portfolio), explicit configuration support, and solid regression coverage. Test health is currently strong (`83 passed`).

Overall assessment:
- **Maturity:** Medium-high for research-grade workflows.
- **Reliability:** High in current tested behaviors.
- **Operational readiness:** Good for local/CI research workloads.
- **Primary risks:** data-source availability edge cases, pandas forward-compatibility warnings in selected paths, and single-strategy concentration in the default registry.

---

## 2) Repository Inventory

High-level inventory from filesystem scan:
- Python files: **104**
- Markdown docs: **19**
- YAML configs: **5**
- JSON edge specs: **3**

Major folders and role:
- `project/` (**136 files**) — implementation code.
- `tests/` (**62 files**) — regression and contract tests.
- `docs/` (**11 files**) — architecture, research notes, recommendations.
- `edges/` (**3 files**) — pinned overlay specifications.

---

## 3) Architecture Overview

### 3.1 Pipeline orchestration

The main orchestrator is `project/pipelines/run_all.py` and supports:
- Core stages: ingest OHLCV, ingest funding, clean, build features, build context features, backtest, report.
- Optional research branches:
  - Phase 1/2 research (`analyze_vol_shock_relaxation`, `phase2_conditional_hypotheses`)
  - Aftershock analysis
  - Edge-candidate export
- Optional multi-edge branch:
  - Portfolio backtest and validation with configurable modes.

Design strengths:
- Stage-level logs/manifests per `run_id`.
- CLI flags allow skipping ingest, forcing rebuilds, and toggling strictness around funding quality.
- Config overlays supported via repeated `--config` options.

### 3.2 Data model and IO strategy

The project uses a run-scoped data lake approach:
- Raw market data under `data/lake/raw/...`.
- Run-specific cleaned/features/context artifacts under `data/lake/runs/<run_id>/...`.
- Reports/manifests under `data/reports/...` and `data/runs/<run_id>/...`.

This pattern supports reproducibility and split-by-run analysis while keeping large data outside git.

### 3.3 Strategy and simulation engine

Core simulation lives in:
- `project/engine/runner.py`
- `project/engine/pnl.py`

Observations:
- Engine merges per-symbol features with context features (funding persistence context).
- Position and return paths are validated for timestamp correctness (UTC checks).
- Strategy lookup is pluggable through `project/strategies/registry.py`.

Current registered strategy set is intentionally minimal:
- `vol_compression_v1`.

### 3.4 Overlay and edge governance contracts

Two policy-oriented contract layers exist:
- `project/strategies/overlay_registry.py` validates pinned overlay specs in `edges/*.json`.
- `project/portfolio/edge_contract.py` validates edge definitions and enforces evidence requirements for APPROVED edges.

Notable governance controls:
- Pinned spec versioning.
- Allowed lifecycle states (`DRAFT`, `APPROVED`, etc.).
- Required objective/constraints/stability fields for promoted overlays.
- Required evidence metadata for approved edges.

### 3.5 Portfolio allocator

`project/portfolio/multi_edge_allocator.py` implements multi-edge allocation with modes:
- `equal_risk`
- `score_weighted`
- `constrained_optimizer`

Constraints include:
- Max drawdown, CVaR proxy, gross/net exposure caps.
- Single-symbol and per-edge risk controls.
- Turnover budgeting and optional return-tilt behavior.

---

## 4) Configuration & Operational Controls

Key configuration files:
- `project/configs/pipeline.yaml` — pipeline shape and multi-edge policy defaults.
- `project/configs/portfolio.yaml` — objective and constraints for portfolio allocation.
- `project/configs/fees.yaml` — baseline fee/slippage assumptions.

Operational controls exposed via CLI:
- Funding strictness toggles (`allow_missing_funding`, `allow_constant_funding`, timestamp rounding).
- Cost overrides (`fees_bps`, `slippage_bps`, `cost_bps`).
- Strategy selection and overlay injection.

This balance gives strong reproducibility while still supporting controlled experiment variance.

---

## 5) Testing and Quality Signals

Validation executed in this review:
- `python3 -m pytest -q`
- Result: **83 passed**, 0 failed.

Test suite coverage domains (from file inventory):
- Pipeline orchestration and diagnostics.
- Funding ingestion/persistence and feature-context correctness.
- Research analyzer mechanics and contracts.
- Overlay promotion checks and edge contract validation.
- Multi-edge allocation and run-all integration behavior.

Quality posture:
- Strong regression confidence for current architecture.
- Good alignment between governance contracts and tests.

---

## 6) Strengths

1. **Clear modular architecture** with explicit boundaries between ingestion, feature engineering, simulation, and research.
2. **Reproducibility-first workflow** using `run_id`, stage manifests, and run-scoped artifacts.
3. **Governance-oriented promotion model** for overlays and edges (status lifecycle + evidence requirements).
4. **Strong automated test baseline** for both mechanics and policy contracts.
5. **Config-driven extensibility** for research toggles and risk/cost assumptions.

---

## 7) Risks / Gaps

1. **Strategy concentration risk**
   - Default strategy registry currently exposes a single strategy (`vol_compression_v1`).
   - Portfolio and research pipelines are broader than current baseline strategy diversity.

2. **Potential forward-compatibility warnings (pandas evolution)**
   - Prior local audit doc flags deprecation/future-warning hot spots.
   - Not blocking today, but relevant for future dependency upgrades.

3. **Data-source boundary limitations**
   - Historical availability constraints (pre-late-2019 futures archives) require clamp behavior and manifest caveats.

4. **Operational complexity in optional branches**
   - `run_all.py` supports many toggles; this is powerful but increases config and runbook burden.

---

## 8) Recommendations (Prioritized)

### P0 (Immediate, low risk)
- Add a concise architecture diagram (`docs/`) showing stage artifacts and dependencies.
- Add a single “operator runbook” markdown with 3 reference workflows:
  - baseline run
  - research phase2 run
  - multi-edge validation run

### P1 (Near-term)
- Harden identified warning-prone pandas paths and add targeted unit tests that assert dtypes after fill/merge operations.
- Introduce smoke fixtures for “missing funding + strict mode” and “rounded timestamps mode” with explicit expected manifests.

### P2 (Medium-term)
- Expand strategy registry with at least one additional orthogonal baseline strategy to reduce concentration and improve portfolio-combination utility.
- Add benchmark timing/profiling snapshots for heavy stages (`ingest`, `features`, `backtest_multi_edge_portfolio`).

### P3 (Long-term)
- Consider introducing a machine-readable experiment registry (e.g., YAML index of run groups and hypotheses) to connect research promotion decisions directly to evidence runs.

---

## 9) Conclusion

The repository is well-structured, test-healthy, and already incorporates advanced governance concepts uncommon in many research backtest codebases (formal edge/overlay contracts with promotion checks). With modest investment in operator documentation, warning hardening, and strategy diversity, it can move from strong research infrastructure toward more production-like repeatability and maintainability.
