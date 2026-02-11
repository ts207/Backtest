# Full Project Report: Backtest Repository

_Date: 2026-02-10 (refreshed)_

## 1) Executive Summary

Backtest is a modular crypto-perpetual research and backtesting stack centered on a 15m data model and a run-scoped pipeline (`ingest -> clean -> features -> backtest -> report`).

Current high-level status:
- Test health is strong (**92 passed**).
- Strategy stack is now multi-family (trend, carry, reversion, vol-compression variants), not single-strategy.
- Portfolio controls are intentionally strict on drawdown, exposure, and turnover.
- The main practical question is calibration: **are constraints too harsh for edge realization after costs?**

Bottom line:
- The framework is robust enough to produce tradable candidates.
- The current risk gates are **conservative but not unreasonable** for institutional-style survivability.
- For pure return-seeking research, a staged “harshness sweep” should be added so constraints are validated empirically rather than accepted by default.

---

## 2) Current Strategy and Edge Inventory

### 2.1 Registered strategies (`project/strategies/registry.py`)
The active registry exposes six strategies:
1. `vol_compression_v1`
2. `vol_compression_momentum_v1`
3. `vol_compression_reversion_v1`
4. `tsmom_v1`
5. `funding_carry_v1`
6. `intraday_reversion_v1`

This is a healthy cross-family baseline for diversification experiments.

### 2.2 Portfolio edge set (`project/configs/portfolio.yaml`)
Configured APPROVED edges include:
- `vc_core`, `vc_momentum`, `vc_reversion` (vol-compression family)
- `tsmom_trend` (trend family)
- `carry_funding` (carry family)
- `intraday_reversion_core` (reversion family)

Notable governance quality:
- Each edge carries evidence metadata (`run_id`, `split`, `date_range`, `config_hash`).
- Family policy can enforce one edge per family to reduce intra-family crowding.
- Cost assumptions are explicit (fees/slippage), which improves realism.

---

## 3) “Too Harsh?” Constraint/Gate Review

### 3.1 Current guardrails
From `project/configs/portfolio.yaml`, key limits are:
- `max_drawdown_pct: 0.20`
- `cvar_1d_99_pct: 0.025`
- `gross_exposure_max: 1.50`
- `net_exposure_max: 0.60`
- `single_symbol_weight_max: 0.20`
- `turnover_budget_daily: 0.30`

With gates:
- `regime_sign_consistency_min: 0.80`
- `symbol_positive_pnl_ratio_min: 0.60`
- non-negative uplift / friction-floor thresholds.

### 3.2 Assessment
These settings are **conservative**, but broadly aligned with preventing overfit/high-turnover illusionary edges.

Where they may feel harsh:
- `sign_consistency >= 0.80` can reject valid cyclical or regime-rotating edges.
- `symbol_positive_pnl_ratio >= 0.60` can penalize concentrated-but-strong specialist strategies.
- `turnover_budget_daily = 0.30` can clip short-horizon alpha that requires frequent rebalancing.

### 3.3 Verdict
- For **promotion to production-like deployment**: constraints are appropriate.
- For **early discovery research**: likely somewhat harsh.

Recommended operating model:
- Keep current strict profile as **Production Profile**.
- Add a **Research Profile** with slightly relaxed gates for idea incubation, then require re-validation under Production Profile before approval.

---

## 4) Priority Improvements to Reach “Successful Trading Strategy”

### P0: Separate research-vs-production gate profiles
Create two config presets:
- `portfolio_research.yaml` (moderately relaxed)
- `portfolio_production.yaml` (current strict defaults)

Suggested research relaxations to test:
- `regime_sign_consistency_min`: 0.80 -> 0.70
- `symbol_positive_pnl_ratio_min`: 0.60 -> 0.50
- `turnover_budget_daily`: 0.30 -> 0.40

### P1: Run a formal harshness sensitivity sweep
Automate a sweep around gate thresholds and report:
- candidate survival rate
- net return vs drawdown frontier
- stability degradation curve

This converts “too harsh?” from opinion into measurable policy tradeoff.

### P2: Strengthen objective alignment
Given objective `net_total_return`, add reporting of:
- return per unit turnover
- return per unit drawdown
- tail event contribution decomposition

This ensures return maximization remains constrained and economically meaningful.

### P3: Improve dependency security baseline
Upgrade known vulnerable pins (at minimum):
- `requests >= 2.32.4`
- `pyarrow >= 17.0.0`

Then re-run full tests and audit.

---

## 5) Practical 30-Day Execution Plan

1. **Week 1**: Add research/production config split and harshness sweep script.
2. **Week 2**: Run sweep on TOP10 and BTC/ETH-only universes; publish frontier report.
3. **Week 3**: Promote only candidates that pass both relaxed discovery and strict re-validation.
4. **Week 4**: Lock one deployable multi-edge spec and monitor out-of-sample stability.

Success criteria:
- Positive constrained net return after costs.
- No material breach of drawdown/tail limits.
- Stable sign across major regimes and major symbols.

---

## 6) Conclusion

The project is no longer strategy-concentrated and already has a strong governance backbone for disciplined promotion. The current policy is not “wrongly harsh”; it is **production-conservative**. To maximize chance of finding successful strategies, add an explicit two-profile workflow and a quantitative harshness sweep, then require strict-profile re-validation before final approval.
