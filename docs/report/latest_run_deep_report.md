# Backtest Project Deep Analysis and Latest Run Report

## Executive Summary

This repository is a modular crypto-perpetual research/backtest stack with clear stage boundaries (ingest → clean → features → context features → backtest → report → research checklist), strong manifesting, and deterministic tests.

A fresh full pipeline run was executed locally (reusing existing raw data) with:

- `run_id`: `20260211_031950`
- symbols: `BTCUSDT, ETHUSDT`
- date window: `2024-01-01` to `2024-03-31`
- ingest skipped (raw data already present)

`vol_compression_expansion_v1` strategy verdict from the built-in checklist gate is **KEEP_RESEARCH** (strategy-layer result, not global run conclusion).

Primary strategy-layer reasons:

1. Net return is negative after costs (6 bps/side baseline).
2. Trade count is too low for confidence (11 vs threshold 20).
3. Stability checks are still missing in this strategy checklist artifact (separate Phase-2 event promotions may exist).

## Project Architecture Analysis

## 1) Orchestration and run model

- The top-level orchestrator (`project/pipelines/run_all.py`) constructs a run-scoped directory under `data/runs/<run_id>/` and executes stages in order.
- Stages are independently manifest-logged (`<stage>.json` + `<stage>.log`), which makes postmortem and reproducibility straightforward.
- Optional research and phase gates can be inserted conditionally (Phase 1, Phase 2, recommendations checklist).

Operational implication: this is a strong “experiment ledger” setup; it is easy to compare runs and audit stage-level failures.

## 2) Strategy and execution logic

- The default strategy is `vol_compression_v1`.
- Entry requires compression conditions (`rv_pct_2880`, range ratio constraints) plus breakout confirmation logic.
- Exit stack includes stop/target, time exit (`max_hold_bars`), volatility expansion exit, optional no-expansion timeout, and optional adaptive trailing stop.

Operational implication: logic is interpretable and rule-based, but may be over-constrained in the tested period (trade count is low).

## 3) Cost and risk framing

- Pipeline defaults imply round-trip cost assumptions via fee + slippage model.
- Report includes fee sensitivity table (0/2/5/6/10 bps per side), which is very useful for edge realism.
- Research checklist applies objective gates (positive net return, trade count floor, drawdown cap, stability evidence presence).

Operational implication: the framework is correctly optimized for “falsifiable research” instead of unconstrained curve fit.

## Latest Run: `20260211_031950`

Primary evidence artifacts for this run:

- Summary report: `data/reports/vol_compression_expansion_v1/20260211_031950/summary.md`
- Stage manifests/logs: `data/runs/20260211_031950/<stage>.json|.log`
- Checklist verdict: `data/runs/20260211_031950/research_checklist/checklist.md`
- Engine outputs: `data/runs/20260211_031950/engine/`

## 1) Stage health

All executed stages completed successfully:

- `build_cleaned_15m`: success
- `build_features_v1`: success
- `build_context_features`: success
- `backtest_vol_compression_v1`: success
- `make_report`: success
- `generate_recommendations_checklist`: executed with decision output

No stage-level failure was observed in manifests.

## 2) Data quality / coverage

Coverage and quality are strong for this run window:

- Missing OHLCV: 0.00% for both symbols in Jan/Feb/Mar 2024.
- Bad bars: 0 for both symbols.
- Funding event coverage: 100% for both symbols.
- Funding missing: 0.00%.
- Gap stats: 0 gap_count, 0 max_gap_len.

Interpretation: performance issues are unlikely to be explained by obvious data-integrity faults in this sample window.

## 3) Feature quality / usable sample

Feature diagnostics show a non-trivial warmup/availability loss:

- `rv_pct_2880` NaN rate: 34.05%
- `pct_rows_dropped`: 34.05%
- `missing_feature_bars` in engine diagnostics: 5,950 bars (34.05%)

Interpretation: about one-third of bars are not tradable due to feature readiness, constraining opportunity set and lowering event count.

## 4) Backtest performance snapshot

Combined metrics:

- Trades: 11
- Win rate: 54.55%
- Avg R: -0.05
- Ending equity: 991,272.08 (from 1,000,000 baseline)
- Max drawdown: -4.27%
- Annualized Sharpe: -0.37

By symbol:

- BTCUSDT: 6 trades, avg R +0.116
- ETHUSDT: 5 trades, avg R -0.242

Interpretation: positive hit rate but negative expectancy (average loss size/exit profile likely dominates).

## 5) Cost sensitivity

Net return by fee bps per side:

- 0.0 bps: +0.3872%
- 2.0 bps: -0.0328%
- 5.0 bps: -0.6628%
- 6.0 bps: -0.8728%
- 10.0 bps: -1.7128%

Interpretation:

- The edge is extremely thin before costs.
- Any realistic friction pushes outcome negative quickly.
- This is a red flag for production viability without strategy refinement.

## 6) Strategy checklist verdict (`vol_compression_expansion_v1` only)

Checklist decision for this strategy mapping: **KEEP_RESEARCH**.

Failed gates:

- `positive_net_return_after_costs`: FAIL
- `minimum_trade_count`: FAIL (11 < 20)
- `stability_checks_present`: FAIL

Passed gates:

- `max_drawdown_cap`: PASS
- `data_quality_missing_ohlcv`: PASS

Interpretation: the framework is correctly preventing premature promotion for the `vol_compression_v1` strategy checklist path.

## 7) Phase-2 event promotion snapshot (provided external evidence)

Additional evidence provided in review indicates multiple Phase-2 event candidates marked `PROMOTED` for the same `run_id` (`20260211_031950`) across several event families (e.g., `vol_shock_relaxation`, `directional_exhaustion_after_forced_flow`, `range_compression_breakout_window`, `liquidity_refill_lag_window`, `liquidity_absence_window`, `funding_extreme_reversal_window`, `vol_aftershock_window`).

- Provided source path pattern: `.../data/reports/phase2/20260211_031950/<event>/promoted_candidates.json` (review payload example pointed to `/home/tstuv/snap/Backtest/...`).

Key interpretation notes:

- These promotions are **event-level/conditional research artifacts**, not equivalent to direct promotion of `vol_compression_v1` strategy PnL readiness.
- Several promoted candidates show **negative `expected_return_proxy`** despite positive `edge_score` and `stability_proxy=1.0`, so promotion here should be interpreted as “mechanism candidate survived event gating,” not “production-positive net strategy return.”
- Governance should therefore keep two ledgers separate:
  1. event-level conditional promotions, and
  2. deployable strategy-level checklist readiness.

Recommended reconciliation action:

- Add a run-level reconciliation report that joins strategy checklist outcomes with Phase-2 promoted candidate stats (including cost-adjusted return proxy sign and event count), so reviewers can immediately see where event-level signal validity diverges from strategy-level profitability.


## 8) Selected next move: stability proxy audit implementation (Option 4)

Given reviewer feedback, the immediate implementation focus is **stability proxy scrutiny** before portfolio construction.

Implemented audit utility:

- `project/pipelines/research/audit_promoted_candidates.py`

Purpose:

- Detect suspicious stability saturation (`stability_proxy == 1.0` concentration).
- Detect duplicate/near-duplicate edge scores inside each event family.
- Collapse near-duplicate parameter variants (e.g., `_0`, `_1`) into mechanism-level canonical candidates and emit `collapsed_candidates.json`.
- Summarize promoted candidates by mechanism family (event-level clustering vs candidate IDs).
- Compute cross-family dependency snapshot when timestamps are available (`timestamp_jaccard`, co-firing rate within ±15m, optional proxy-correlation matrix).
- Emit machine-readable + markdown artifacts for review: `promotion_audit.json` and `promotion_audit.md`.

Important scope note:

- Current repository code for `phase2_conditional_hypotheses.py` does not natively define fields named `stability_proxy`, `edge_score`, or `expected_return_proxy`; these appear in external promotion payloads provided during review.
- Therefore, this audit is designed as a **post-processing guardrail** over promoted payloads, and is the safest immediate way to validate proxy behavior without assuming hidden metric semantics.

Usage example:

```bash
python3 project/pipelines/research/audit_promoted_candidates.py \
  --input /path/to/promoted_candidates.json \
  --out_dir data/reports/phase2/20260211_031950/_audit
```

Expected immediate value:

1. Flags whether `stability_proxy` is saturated/trivial.
2. Flags event-family duplicate edges and writes mechanism-collapsed candidates.
3. Quantifies cross-family co-firing/overlap before any optimizer decision.
4. Produces a mechanism-first summary table that supports the consolidation stage decision.


## 9) Layered run decision (contradiction fix)

To avoid conflating layers, run decisions should be produced separately:

- **Research layer (event-level):** `RESEARCH_PROMOTIONS_PRESENT` / `RESEARCH_EMPTY` / `RESEARCH_INCONCLUSIVE`
- **Strategy layer (per strategy module):** e.g., `VOL_COMPRESSION_KEEP_RESEARCH`

Implemented helper:

- `project/pipelines/research/build_layered_run_decision.py`

This writes:

- `data/reports/run_decisions/<run_id>/layered_decision.json`
- `data/reports/run_decisions/<run_id>/layered_decision.md`

Global deployability must not be claimed from a single strategy checklist. If only one strategy decision exists, the helper emits:

- `GLOBAL_DEPLOYABILITY_NOT_ASSESSED_SINGLE_STRATEGY`

This resolves the apparent contradiction by design: “research promotions exist” can coexist with “this one strategy mapping is KEEP_RESEARCH.”


## 10) Promoted-edge audit runner stage (implementation)

To operationalize strategy-level audits per promoted family (not just vol compression), a new runner is added:

- `project/pipelines/research/run_promoted_edge_audits.py`

What it does (minimal non-tunable mapping):

1. Reads promoted candidates from `data/reports/phase2/<run_id>/<event_type>/promoted_candidates.json`.
2. Loads phase-1 family events from `data/reports/<event_type>/<run_id>/*events.csv`.
3. Simulates fixed-rule trades per promoted candidate:
   - entry: first 15m bar open strictly after event end (`exit_ts`, fallback `enter_ts`)
   - exit: fixed horizon (`horizon_bars`, default 1 = 15m)
   - costs: pessimistic per-side fee+spread, no tunable filters
4. Writes per-family summaries under:
   - `data/reports/promotion_audits/<run_id>/<event_type>/summary.md`
   - `data/reports/promotion_audits/<run_id>/<event_type>/summary.json`

Pipeline integration:

- `project/pipelines/run_all.py` now supports `--run_promoted_edge_audits 1` and forwards audit knobs (`top_n`, horizon, pessimistic costs).

This closes the mapping gap: “research promotions exist” is now auditable as per-family strategy outcomes instead of being judged only through `vol_compression_expansion_v1`.

## Root-Cause Assessment (Why the run likely underperforms)

Most probable contributors, ranked:

1. **Cost fragility**: the strategy appears near break-even gross; realistic costs flip sign.
2. **Low event density**: only 11 trades in 3 months across 2 symbols reduces statistical confidence.
3. **Feature warmup burden**: 34% bar exclusion cuts effective sample and setup opportunities.
4. **Asymmetric symbol behavior**: ETH sample underperformed vs BTC in this run.
5. **Incomplete strategy-level stability evidence**: the checklist governance gate remains unmet for this strategy path even if some event-level promotions are available elsewhere.

## Prioritized Next Steps

## Priority 0 — Reproducibility and experiment hygiene (immediate)

1. Pin and record baseline run as control (`20260211_031950`).
2. Standardize a run matrix naming scheme (e.g., `YYYYMMDD_HHMMSS_vc1_<variant>`).
3. Keep checklist output mandatory in every trial.

## Priority 1 — Improve sample efficiency without overfitting

1. Reduce early-sample drop pressure:
   - Evaluate shorter long-horizon feature windows or delayed evaluation start with explicit reporting.
2. Increase event count safely:
   - Tune compression strictness and breakout confirmation in coarse grid, not micro-optimization.
3. Expand universe modestly:
   - Add 2–4 liquid perp symbols to increase independent events.

Success criterion: move median run trade count meaningfully above the checklist floor (20+) while preserving data quality.

## Priority 2 — Fix cost fragility

1. Re-test with execution-aware assumptions:
   - Time-of-day restrictions around spread-heavy windows.
   - Optional liquidity/tradeability filters.
2. Evaluate exit logic variants:
   - Compare fixed target/stop vs adaptive trailing vs timeout combinations.
3. Track per-trade gross edge decomposition:
   - gross expectancy, cost drag, and net expectancy by symbol/regime.

Success criterion: positive net return at 6 bps per-side in out-of-sample or split-consistent evaluation.

## Priority 3 — Satisfy stability governance

1. Run dedicated stability/phase analyses to populate missing checklist evidence.
2. Add temporal split consistency checks (year/quarter buckets where possible).
3. Add symbol-split consistency reporting for sign and effect size.

Success criterion: `stability_checks_present` gate turns PASS with explicit artifacts.

## Suggested 2-Week Execution Plan

Week 1:

- Run baseline + 6–10 coarse parameter variants over the same quarter.
- Add 2 additional liquid symbols.
- Produce comparative table: trades, net return at 6 bps, drawdown, Sharpe, checklist pass/fail.

Week 2:

- Narrow to top 2–3 robust candidates (not top PnL only).
- Execute longer-range backtests (multi-regime window) for survivors.
- Generate stability artifacts and reassess promotion readiness.

## Operational Risks to Track

- **Overfitting risk** from parameter sweeps without strict holdout discipline.
- **Liquidity/impact mismatch** if assumed costs understate live execution.
- **Regime dependency** where a setup works only in narrow volatility states.
- **False confidence** from low-trade samples.

## Bottom Line

The project infrastructure is solid and research-oriented, and the latest run confirms end-to-end integrity. However, the tested strategy configuration is not yet promotion-ready: it is cost-fragile, low-sample, and missing stability evidence. The immediate path is not deployment; it is structured hypothesis iteration focused on improving net expectancy robustness and passing governance gates.
