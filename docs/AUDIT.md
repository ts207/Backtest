## Executive summary (≤300 words)

This audit is based on the code in `Backtest-main (3).zip` (no run artifacts or dataset samples were provided). The repository implements a **spec-first, event→hypothesis→blueprint→engine backtest** factory primarily for **Binance USDT-M perpetuals** with partial **Binance spot** support (no evidence of Bybit/OKX/Deribit connectors). Core strengths are: (1) consistent use of **as-of backward joins** for optional time-series (reduces lookahead), (2) explicit **manifests/spec hashes** and some **ontology drift checks**, and (3) a coherent pipeline separation (ingest/clean/features/research/eval/backtest).

Primary high-severity issues are concentrated in the **data layer and realism layer**:

* **Gap handling** forward-fills OHLC across missing intervals, producing synthetic flat prices and suppressing volatility; the engine then treats those bars as valid returns, creating optimistic bias.
* **Funding alignment** spreads funding rates across bars using the *previous* funding timestamp via backward-asof; this misaligns realized funding economics by one interval and can materially distort PnL attribution.
* **Order book snapshots** are forward-filled without tolerance, creating stale quotes and contaminating spread/depth-based execution cost calibration.
* The “ML” pipeline (`alpha_bundle`) standardizes/orthogonalizes using full-sample statistics before time-split evaluation → classic leakage.
* Backtesting is **close-to-close**, with simplified transaction cost proxies and limited liquidity/impact realism; portfolio risk controls are basic (gross exposure caps, no correlation clustering/vol targeting), and production monitoring/kill-switch/drift detection is largely absent.

Net: research pipeline is structurally promising, but data integrity and execution realism issues must be fixed before any claims of robust edge or production readiness.

---

## Assumptions (explicit)

1. Evidence basis is **static code inspection** only; no exchange specs, fee tiers, or sample data were verified.
2. Markets supported in code: **Binance UM perps + Binance spot OHLCV**; “major exchanges” coverage is a target architecture, not implemented.
3. Strategy decisions are assumed to occur **after bar close** and execute with at least **one bar lag** unless configured otherwise.

---

## Detailed findings

| Component                        | Risk                                                                                                                                        |     Severity | Evidence                                                                                                                 | Recommendation                                                                                                                                                                                                                 |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | -----------: | ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Cleaning (bars)                  | **Implementation:** forward-filling OHLC across missing intervals creates synthetic flat prices → optimistic vol/Sharpe, hides data outages | **Critical** | `project/pipelines/clean/build_cleaned_5m.py:217–236` (ffill OHLC, volume=0, `is_gap` recorded but prices still filled)  | Do **not** ffill OHLC for missing market data; keep NaNs or “invalid bars”, and force **flat positions** across gaps. Add “gap embargo” (e.g., no trading for N bars after gaps).                                              |
| Cleaning (bars→engine)           | **Implementation:** engine treats ffilled bars as valid returns (ret not NaN) so gap periods contribute zero-return stability               |     **High** | `project/engine/pnl.py:10–33` (returns from `pct_change`, NaNs only if close NaN) + ffill upstream                       | Add explicit mask: if `is_gap==1` then returns invalid and positions forced to 0; or drop those timestamps from backtest.                                                                                                      |
| Funding alignment (clean)        | **Implementation:** realized funding spread uses **backward-asof** to prior funding timestamp → misaligned by one interval                  | **Critical** | `project/pipelines/clean/build_cleaned_5m.py:89–129` (`merge_asof(... direction="backward")` then divide by bars/event)  | Split into two series: **feature funding (last-known)** vs **realized funding cashflow**. For realized PnL: apply funding **only at funding timestamps** using position at that time (discrete cashflow), not smeared forward. |
| Funding economics (engine)       | **Structural:** per-bar funding model (continuous) differs from actual discrete funding payments and position timing                        |     **High** | `project/engine/pnl.py:50–72` (funding_pnl computed per bar)                                                             | Implement discrete funding events; validate against exchange rule: payment at funding time, position snapshot definition, sign convention.                                                                                     |
| ToB snapshots (1s)               | **Implementation:** quote stream forward-filled via merge_asof without tolerance → stale quotes during outages                              | **Critical** | `project/pipelines/clean/build_tob_snapshots_1s.py:74–84` (merge_asof backward, no tolerance; output becomes continuous) | Add tolerance (e.g., max quote age seconds) and mark invalid quotes; propagate gaps into aggregated ToB features and cost calibration.                                                                                         |
| ToB aggregation                  | **Implementation:** depth/imbalance features inherit stale quotes; potential divide-by-zero/inf handling unclear                            |     **High** | `project/pipelines/clean/build_tob_5m_agg.py` (derived fields from snapshots)                                            | Add validity masks, robust NaN/inf sanitization, and coverage metrics; refuse to calibrate costs when ToB coverage < threshold *and* quote age violations occur.                                                               |
| Feature merges                   | **Positive:** as-of backward joins reduce lookahead in optional series                                                                      |          Low | `project/pipelines/features/build_features_v1.py:129–135` (OI merge_asof backward)                                       | Keep pattern; extend to all external series with explicit tolerance and freshness checks.                                                                                                                                      |
| Feature quality under gaps       | **Implementation:** rolling vol/range computed on ffilled OHLC → regime features contaminated                                               |     **High** | `project/pipelines/features/build_features_v1.py:443–450` (rolling std/max/min on OHLC) + ffilled OHLC upstream          | Recompute features on **gap-clean** series; drop windows that overlap gaps; track “effective sample” per rolling feature.                                                                                                      |
| Basis (spot/perp)                | **Structural:** basis computed from spot close backward-asof; spot data quality not audited for outages/forks                               |       Medium | `project/pipelines/features/build_features_v1.py` (`_add_basis_features`) + spot ingestion exists                        | Add spot QC: trading halts, symbol mapping, stablecoin depegs; compute basis using mark price where appropriate; enforce timestamp consistency.                                                                                |
| Universe selection               | **Structural/implementation gap:** expects 15m bars path that is not built in visible pipeline; delistings not modeled                      |         High | `project/pipelines/_lib/universe.py:31–49` reads `bars_15m`                                                              | Implement a **historical universe** table (listed/unlisted dates, volume, exchange availability). Avoid survivorship by constructing constituents at each timestamp.                                                           |
| Labeling / targets               | **Structural:** discovery uses close-to-close forward return; intrabar risk, stops, execution price not modeled                             |       Medium | `project/pipelines/research/phase2_candidate_discovery.py` (entry/horizon uses closes)                                   | Provide alternative labels: open-to-open, VWAP, adverse excursion, time-to-hit; align label with execution model.                                                                                                              |
| Cross-validation discipline (ML) | **Implementation:** full-sample standardization/orthogonalization before time-split evaluation → leakage                                    | **Critical** | `project/pipelines/alpha_bundle/fit_orth_and_ridge.py:149–186` (global mean/std then time blocks)                        | Move scaling/orthogonalization **inside each split** (fit on train, apply to val/test). Store transformers per split.                                                                                                          |
| Multiple testing / selection     | **Structural:** large hypothesis space → data-snooping risk unless discovery/eval windows are enforced                                      |         High | Phase2 writes FDR artifacts; no enforced disjointness seen in blueprint lineage                                          | Enforce **hard separation**: discovery window → freeze candidates → evaluate on forward OOS. Write discovery start/end into blueprint lineage and block overlaps.                                                              |
| Walk-forward splitting           | **Implementation:** split labels by row fractions rather than fixed timestamp cutoffs; unstable when data coverage changes                  |       Medium | `project/pipelines/research/analyze_conditional_expectancy.py:153–197`                                                   | Use explicit timestamp cutoffs (e.g., by date) and store them. Add “purge/embargo” around boundaries.                                                                                                                          |
| Backtest return model            | **Structural:** close-to-close, position applied with lag; limited intrabar realism                                                         |       Medium | `project/engine/pnl.py:10–33` (pct_change close)                                                                         | Support OHLC simulation modes: next-open execution, bar-range slippage, stop/limit fill models.                                                                                                                                |
| Transaction costs                | **Structural:** proxy model uses spread/vol/quote_volume; lacks order-book impact and venue fee tiers                                       |         High | `project/engine/execution_model.py:9–55` + `project/configs/fees.yaml`                                                   | Calibrate per-symbol/per-regime costs from ToB; incorporate fee tiers, maker/taker, rebates; add impact model tied to depth and order size.                                                                                    |
| Liquidity constraints            | **Structural:** no explicit depth/participation constraints; turnover penalty only                                                          |         High | Engine costs derived from turnover; no max participation checks                                                          | Add max participation rate, min depth checks, minimum quote_volume, and reject trades that exceed constraints.                                                                                                                 |
| Latency                          | **Partial:** execution lag bars supported, but no stochastic latency / queue position                                                       |       Medium | `project/engine/runner.py` (execution_lag_bars shift)                                                                    | Add stochastic latency model and maker/taker decision; validate sensitivity to latency distribution.                                                                                                                           |
| Risk sizing                      | **Structural:** basic per-strategy weights and gross exposure caps; no vol targeting/correlation clustering                                 |         High | `project/engine/risk_allocator.py`                                                                                       | Implement vol targeting, correlation clustering, leverage caps by regime, drawdown-based de-risking, per-symbol exposure limits.                                                                                               |
| Tail risk controls               | **Structural:** limited stress testing; no scenario shocks / crash regimes beyond heuristics                                                |       Medium | “stressed cost multiplier” appears in eval/promote                                                                       | Add historical scenario library (crash, squeeze, funding spikes), EVT/Peaks-over-threshold checks, and portfolio tail constraints.                                                                                             |
| Robustness testing               | **Structural:** limited bootstrap in some research modules; no Monte Carlo for strategy selection                                           |       Medium | bootstrap functions exist in research analyzers; no portfolio MC                                                         | Add block bootstrap on returns/trades, parameter perturbation grid, and **regime-segmented** OOS tests.                                                                                                                        |
| Production monitoring            | **Structural gap:** no live monitoring, drift detection (beyond ontology), kill switch                                                      |         High | search hits show ontology drift only                                                                                     | Add: data freshness monitors, feature drift (PSI), PnL attribution drift, slippage drift, and automated kill-switch thresholds.                                                                                                |
| Governance / reproducibility     | **Positive:** stage manifests, spec hashes, terminal artifact audit                                                                         |          Low | `project/pipelines/run_all.py` manifest + late artifact audit                                                            | Keep; extend to include data snapshot hashes and discovery/eval window provenance.                                                                                                                                             |

---

## Red flags (top 10)

1. **OHLC forward-fill across missing data** (synthetic flat prices) → optimistic stability.
2. **Funding rate misalignment** (backward-asof to prior funding event, then smeared forward) → wrong realized funding PnL timing/magnitude.
3. **Order book snapshots forward-filled without tolerance** → stale spreads/depth contaminate cost calibration and execution features.
4. **ML standardization leakage** in `alpha_bundle` (global mean/std pre-split).
5. **No enforced discovery vs OOS separation** → selection bias remains unless the operator manually enforces windows.
6. **Backtest realism limited** (close-to-close, simplified fills, no participation constraints).
7. **Universe/delistings not modeled** → survivorship bias risk if expanding beyond hand-picked symbols.
8. **Execution cost model depends on fields that may be missing/contaminated** (spread/quote_volume from stale ToB).
9. **Walk-forward splits by row fractions** (unstable across dataset changes).
10. **Production readiness gaps** (no drift/health monitoring, kill switch, incident playbooks).

---

## Maturity score (1–5)

| Dimension                                                       | Score | Rationale                                                                                           |
| --------------------------------------------------------------- | ----: | --------------------------------------------------------------------------------------------------- |
| Data integrity & timestamp correctness                          |     2 | Ingestion checks exist, but gap handling + ToB staleness + funding alignment are high-impact flaws. |
| Research methodology (leakage control, multiplicity, selection) |     3 | Multiplicity controls exist; separation and split definitions need hardening.                       |
| Backtesting & execution realism                                 |     2 | Basic costs/lag supported; lacks depth/impact/participation and intrabar realism.                   |
| Risk management & portfolio construction                        |     2 | Exposure caps exist; missing vol targeting, correlation controls, drawdown/tail governance.         |
| Robustness & stress testing                                     |     2 | Some stress heuristics/bootstraps; lacks systematic MC/regime stress framework.                     |
| Production readiness & monitoring                               |     1 | Minimal live monitoring/drift/kill-switch infrastructure.                                           |
| Governance & reproducibility                                    |     4 | Manifests/spec hashes and drift checks are comparatively strong.                                    |

---

## Action roadmap (30/60/90 days)

### 0–30 days (stop major bias; make results interpretable)

1. **Fix gaps end-to-end:** stop OHLC ffill; introduce invalid-bar masking; force flat across gaps; add gap coverage reports.
2. **Fix funding:** implement discrete funding cashflows at funding timestamps; keep separate “last-known funding feature” series.
3. **Fix ToB staleness:** tolerance + quote-age validity; propagate validity into ToB aggregates and cost calibration.
4. **Add verification tests (automated):**

   * Gap audit: %invalid bars, max gap length, PnL sensitivity to dropping gap windows.
   * Funding reconciliation: compare discrete vs current smeared funding PnL on a sample month.
   * ToB freshness: distribution of quote ages; reject if age>threshold.
5. **Harden walk-forward splits:** timestamp cutoffs + embargo; persist in artifacts.

### 31–60 days (reduce selection bias; improve realism)

1. Enforce **discovery vs OOS** separation in code: blueprint lineage must include discovery window; evaluator blocks overlap.
2. Add **participation/depth constraints** and size-aware impact model (even if coarse).
3. Add **vol targeting + per-symbol exposure caps + drawdown de-risking** in risk allocator.
4. Repair `alpha_bundle` leakage: fit scalers/orthogonalizers per split; add time-series CV.

### 61–90 days (production-grade controls)

1. Implement monitoring: data freshness, feature drift (PSI), realized slippage vs model, PnL attribution drift, tail alerts.
2. Add kill-switch logic: max intraday loss, anomalous slippage, missing data triggers, regime-change safeguards.
3. Add robustness suite: block bootstrap, parameter perturbation, regime segmentation, scenario library (crash/squeeze/funding spikes).
4. Add historical universe and delisting metadata; remove survivorship bias if scaling symbol coverage.

---

## Verification tests to run next (highest information per hour)

1. **Gap realism test:** run backtests under (A) current ffill, (B) invalid-bar masking + forced flat; compare turnover, vol, and net PnL attribution deltas.
2. **Funding alignment test:** compute funding PnL via discrete cashflows and compare against current smeared method; quantify max per-day deviation.
3. **ToB freshness test:** measure quote age distribution; re-run cost calibration with freshness gating; compare estimated cost bands.
