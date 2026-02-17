# Backtest Repository — Detailed Analysis and Current Status

## Executive Summary

This repository is in **good operational health**: test suite is green and architecture is clearly modular. The immediate engineering priority is **forward-compatibility hardening** (primarily pandas deprecations/future warnings), not emergency bug fixing.

**Current status at audit time**
- Latest local branch: `work`
- Latest commit before this update: `8f1d8dc` (`Add repository analysis audit report`)
- Automated validation: `71 passed, 0 failed` (with `45 warnings`)
- Scope size snapshot:
  - `71` files under `project/`
  - `25` test files under `tests/`
  - `6` docs files under `docs/`

## System Overview

The project implements an end-to-end crypto perpetual futures research/backtesting workflow.

### Main components

- `project/pipelines/`
  - Entry points for ingest, cleaning, features, research analysis, backtests, reports, and orchestrator (`run_all.py`).
- `project/engine/`
  - Simulation/portfolio logic and strategy-return aggregation.
- `project/strategies/`
  - Strategy definitions and overlay registry contracts.
- `project/analyzers/`
  - Event and hypothesis analyzers (ABMA v1/v2 funding-focused research).
- `project/features/`
  - Reusable feature modules and context derivations.
- `tests/`
  - Broad unit/integration-style checks spanning contracts, diagnostics, and run determinism.

### Data and run model

The repository is run-id centric:
- Raw and normalized lake data under `data/lake/...`
- Run-scoped artifacts under `data/lake/runs/<run_id>/...`
- Engine outputs and reports under `data/runs/...` and `data/reports/...`

This structure supports reproducibility and post-run audits.

## Pipeline Lifecycle (Operational View)

1. **Ingestion**
   - Pulls Binance UM OHLCV/funding (+ related funding-event streams).
   - Handles archive/API paths and persistence manifests.
2. **Cleaning / canonicalization**
   - Produces aligned 15m bars with quality diagnostics.
3. **Feature + context building**
   - Generates strategy and research features from cleaned bars/funding joins.
4. **Backtest execution**
   - Runs strategy logic through the engine and writes trades/equity/metrics.
5. **Research tracks (Phase 1/2)**
   - Mechanism validation and constrained conditional-hypothesis promotion gates.
6. **Reporting**
   - Aggregates run diagnostics and summary outputs.

## Latest Validation Snapshot

Command run:

```bash
python3 -m pytest -q
```

Result:
- **Passes:** 71
- **Failures:** 0
- **Warnings:** 45

Interpretation:
- Behavior is currently stable under existing regression checks.
- Technical debt is concentrated in warning-producing paths rather than failing logic.

## Warning Inventory and Risk

### 1) Deprecated dtype check (moderate future risk)
- File: `project/pipelines/_lib/validation.py`
- Signal: deprecated `is_datetime64tz_dtype` usage.
- Impact: future pandas upgrades can break timezone validation.

### 2) Silent downcasting behavior change risk
- File: `project/engine/runner.py`
- Signal: multiple `fillna(...).astype(...)` future warnings.
- Impact: future dtype behavior may subtly affect signal/control columns.

### 3) Concat dtype inference drift risk
- File: `project/pipelines/ingest/ingest_binance_um_funding.py`
- Signal: concat with empty/all-NA entries warning.
- Impact: funding table schema inference may drift across pandas versions.

## Practical Health Assessment

- **Reliability now:** High (green tests)
- **Maintainability:** Good, aided by modular boundaries and naming
- **Forward compatibility:** Medium risk (pandas behavior changes)
- **Research extensibility:** Strong (phase-gated pipelines and analyzers are already present)

## Priority Recommendations

### Immediate (next PR)
1. Replace deprecated timezone dtype check with pandas-supported approach.
2. Make `runner.py` dtype intent explicit before/after fill operations.
3. Guard concat paths with empty-frame normalization in funding ingest.

### Near-term (next 2–4 PRs)
1. Add warning-budget tracking in CI (or fail-on-warning for touched modules).
2. Add tiny schema contract tests around funding/engine intermediate columns.
3. Introduce a lightweight “compatibility matrix” job for pinned pandas versions.

### Medium-term
1. Add robustness benchmark runs across more symbols.
2. Expand run-summary artifacts with automated “edge stability” deltas between runs.
3. Add quality gates for report completeness and manifest consistency.

## Current Overview for Stakeholders

- The platform is **usable and stable today** for continued research.
- The main technical risk is **dependency evolution**, not immediate correctness failures.
- A targeted warning-reduction hardening sprint should materially reduce upgrade risk while preserving current behavior.
- After hardening, effort should shift to strategy-edge work: broader universe checks, robustness splits, and promotion-gate evidence depth.

## Suggested Definition of Done for “Status Hardening”

- `pytest -q` remains fully green.
- Warning count reduced substantially from current baseline.
- No schema drift in generated CSV/Parquet/manifests for representative run IDs.
- One short “upgrade readiness” note added to docs with pinned dependency expectations.
