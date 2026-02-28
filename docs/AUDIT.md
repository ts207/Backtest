# Audit Report

**Basis:** Static code inspection + live test run (`audit_test_run`, BTCUSDT Jan–Feb 2024) + full fast test suite. All 100% of fast tests pass (exit 0). Git commit: `a2a57b2`.

---

## Executive Summary

The pipeline is in a significantly improved state compared to initial assessments. All previously-identified **Critical** data integrity issues are resolved. The research pipeline correctly enforces PIT constraints, discrete funding, gap neutrality, spec hashing, and multiplicity control. The execution engine now has a dynamic cost model, volume-targeting risk allocator, MEV overlays, and double-lag protection.

**Remaining risk concentrations are in the execution realism and production operations layers**, not the data or research layers.

---

## Test Run Observations (`audit_test_run`)

| Observation | Evidence |
| :--- | :--- |
| Clean stage completed in **< 1 sec** for 9,216 bars (Jan–Feb 2024) | `build_cleaned_5m.json` |
| Funding scale auto-detected with confidence=1.0, scale=1 (decimal) | `build_cleaned_5m.log` |
| Features stage completed cleanly with run-scoped output paths | `build_features_v1.json` |
| **`build_context_features` correctly failed** with `funding_missing_pct=0.937` | `build_context_features.log` |
| `late_artifact_count: 0` — no artifacts written after pipeline cutoff | `run_manifest.json` |
| Spec hashes recorded for all 60+ event YAMLs plus concept schemas | `run_manifest.json` |
| `ontology_spec_hash` pinned deterministically | `run_manifest.json` |

The context feature failure is **expected and correct behavior** — the integrity gate rejects running context features on a window with insufficient funding coverage. This protects downstream hypothesis generation from contaminated inputs.

---

## Resolved Issues (Previously Critical/High)

| Component | Previous Status | Current Status | Evidence |
| :--- | :---: | :---: | :--- |
| **OHLC gap forward-fill** | Critical | ✅ Resolved | `build_cleaned_5m.py:254` — explicit comment `"Do NOT forward fill prices for gaps"`, `is_gap` mask used; prices are NaN across gaps |
| **Engine treats ffilled bars as valid** | High | ✅ Resolved | `runner.py:430-433` — `nan_ret_mask` forces positions flat on gap bars, logged as `forced_flat_bars` in diagnostics |
| **Funding smearing** | Critical | ✅ Resolved | `build_cleaned_5m.py:134-135` — `funding_rate_realized` set to funded value only on `timestamp == funding_event_ts`, zero otherwise |
| **ToB forward-fill without tolerance** | Critical | ✅ Resolved | `FUNDING_MAX_STALENESS = 8h` tolerance enforced in `_align_funding`; integrity gate in `build_context_features` rejects high missing rates |
| **ML standardization leakage** | Critical | ✅ Resolved | Scaling now enforced inside each split per code history |
| **Walk-forward splits by row fractions** | Medium | ✅ Resolved | Timestamp cutoffs with embargo days enforced (`walkforward_embargo_days`, default=1) |
| **Missing `ended_at` in manifest** | Low | ✅ Resolved | `run_manifest.json: ended_at` field present |
| **No non-production bypass tracking** | Low | ✅ Resolved | `non_production_overrides` list in manifest |

---

## Current Findings

### Engine & Execution

| Component | Severity | Finding |
| :--- | :---: | :--- |
| **Intrabar execution realism** | Medium | Close-to-close PnL only; no open-execution, VWAP, or stop-fill simulation. Signals fired at bar-close are assumed to transact at next-bar open implicitly via `execution_lag_bars`, but no adverse excursion within the bar is modeled. |
| **Event feature ffill (12 bars)** | Low | `runner.py:204` — a `# TODO` comment marks event feature ffill limit of 12 bars as configurable in future. Currently hardcoded; may over-extend event context for fast-decaying signals. |
| **Dynamic cost model coverage** | Low | `execution_model.py` correctly implements spread/vol/participation impact when `tob_coverage >= min_tob_coverage`. Falls back to static when ToB data unavailable. Calibration of weights (`spread_weight`, `volatility_weight`, etc.) is not yet data-driven. |
| **Double-lag guard** | ✅ Positive | `runner.py:391-395` — actively raises `ValueError` when `blueprint.delay_bars > 0` and `execution_lag_bars > 0` unless `allow_double_lag=1` is explicitly set. |

### Risk Allocator

| Component | Severity | Finding |
| :--- | :---: | :--- |
| **Vol targeting implemented** | ✅ Positive | `risk_allocator.py:76-81` — rolling vol targeting (`target_annual_vol`) scales positions dynamically. Uses 5760-bar (20-day at 5m) rolling window with 288-bar minimum. |
| **Drawdown de-risking implemented** | ✅ Positive | `risk_allocator.py:84-90` — drawdown factor clips allocation proportionally as drawdown approaches `max_drawdown_limit`. |
| **Correlation clustering** | High | No cross-strategy correlation adjustment. Simultaneous long positions across related symbols will silently accumulate correlated gross exposure up to `max_portfolio_gross`. |
| **Regime-conditional sizing** | Medium | No regime-dependent position caps. Macro bear/squeeze regimes are tracked in `vol_regime` context but not wired into the allocator. |

### Research & Discovery

| Component | Severity | Finding |
| :--- | :---: | :--- |
| **BH-FDR multiplicity control** | ✅ Positive | Phase 2 applies BH-FDR correction, stratified by symbol. |
| **Funding integrity gate** | ✅ Positive | Confirmed live: `build_context_features` correctly panics when `funding_missing_pct=0.937`. |
| **Ontology consistency pre-flight** | ✅ Positive | `ontology_consistency_audit.py` runs before any stage per `--run_ontology_consistency_audit 1` (default). |
| **Universe/delisting bias** | High | Universe snapshots loaded from `lake/metadata/universe_snapshots`, but no verified delisting history for expanding symbol sets. Survivorship bias risk if symbol universe grows beyond the 5 currently available. |
| **Discovery OOS disjointness** | Medium | Blueprint lineage includes discovery window parameters, but there is no automated code-level block preventing operators from re-using the same date range for both discovery and evaluation. |

### Data Layer

| Component | Severity | Finding |
| :--- | :---: | :--- |
| **Gap masking** | ✅ Positive | `is_gap=True` bars have NaN prices; volume zero-filled. Engine neutralizes positions on these bars. |
| **Funding** | ✅ Positive | Discrete event model. `funding_rate_realized` applied only at exact funding timestamps. |
| **Spot data QC** | Low | Spot OHLCV ingested but not subjected to the same gap-mask validation as perp bars; stale or sparse spot can contaminate basis features. |
| **Schema versioning** | ✅ Positive | `feature_schema_v1.json` and `v2.json` both declared; schema hash tracked in manifest. |

### Operations / Production

| Component | Severity | Finding |
| :--- | :---: | :--- |
| **Live monitoring** | High | No data freshness monitors, feature drift detection (PSI), or PnL attribution drift alerts. |
| **Kill switch** | High | No automated kill switch based on realized slippage deviation, anomalous PnL, or data health breaches. |
| **Incident playbooks** | Medium | `RUNBOOK.md` exists with operational guidance, but no automated alerting integration. |

---

## Maturity Scores (1–5)

| Dimension | Score | Rationale |
| :--- | :---: | :--- |
| Data integrity & timestamp correctness | **4** | Gap masking, discrete funding, tolerance-gated ToB, and funding integrity gates all confirmed working. |
| Research methodology | **4** | BH-FDR, symbol-stratified multiplicity, ontology hashing, and checklist execution guard all operational. |
| Backtesting & execution realism | **3** | Dynamic cost model and lag hardening added. Intrabar simulation still close-to-close only. |
| Risk management & portfolio construction | **3** | Vol targeting and drawdown de-risking present. Missing correlation clustering and regime-conditional sizing. |
| Robustness & stress testing | **2** | Some bootstrap in research analyzers; no Monte Carlo, scenario library, or regime-segmented OOS testing. |
| Production readiness & monitoring | **1** | Pre-flight audits present. No live monitoring, drift detection, or kill switch. |
| Governance & reproducibility | **5** | Spec hashes, ontology hash, git commit, input file hashes, late artifact audit, and `claim_map_hash` all in every manifest. |

---

## Priority Action Items

### Immediate (0–30 days)
1. **Correlation clustering in allocator** — cap cross-correlated simultaneous long exposure (e.g., BTC+ETH long at same time).
2. **Spot data QC** — apply the same gap masking and integrity gates to spot OHLCV as applied to perp bars.
3. **Event feature ffill limit** — make the 12-bar limit configurable per strategy via blueprint params.

### Medium-term (30–60 days)
1. **Regime-conditional position sizing** — wire `vol_regime` into the allocator to reduce gross exposure in high-vol/chop regimes.
2. **Discovery/OOS window enforcement** — add automated guard that compares blueprint discovery window against evaluation window and fails if they overlap.
3. **Intrabar execution modes** — add at minimum "next-open" fill to close the close-to-close optimism gap.

### Longer-term (60–90 days)
1. **Live monitoring** — data freshness, feature drift (PSI), realized vs. modeled slippage.
2. **Kill switch** — automated halt on anomalous slippage, PnL breach, or data health failure.
3. **Universe delisting history** — build a historical symbol eligibility table to prevent survivorship bias in multi-symbol sweeps.
