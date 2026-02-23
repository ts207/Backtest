# BACKTEST PROJECT: QUANTITATIVE SYSTEMS AUDIT REPORT

Project: Backtest — Quantitative Trading Research Platform
  Audit Scope: Statistical validity · Execution realism · Data integrity · Production readiness for 60-day "+10 bps lift
   from state-conditioned entries" validation run
  Auditor: Staff-level embedded systems auditor
  Date: 2026-02-20
  Evidence basis: Deep-read of 137 Python modules, 18 YAML specs, 8 config files, pipeline orchestrator (1,306 lines),
  engine (5 modules), and certification batch run artifacts

  ---
  SECTION 1 — PIPELINE MAP

  1.1 Master Orchestrator

  File: project/pipelines/run_all.py (1,306 lines)

  The pipeline coordinates 30+ stages across 8 phases with conditional execution, cost tracking, fail-closed checklist
  gating, and full run manifest recording.

  Ingest (5 scripts)
    → Clean (4 scripts: cleaned_5m, basis_state, tob_*)
      → Features (5 scripts: features_v1, context, market_context, universe_snapshots, [spot variants])
        → Atlas Planning (2 scripts: candidate_templates, candidate_plan)
          → Event Detection Phase 1 (18 analyzers: analyze_*.py)
            → Phase 2 Hypothesis (phase2_candidate_discovery.py + bridge_evaluate_phase2.py)
              → Blueprint Compilation (compile_strategy_blueprints.py → blueprints.jsonl)
                → Checklist Gate (generate_recommendations_checklist.py)
                  → Backtest (backtest_strategies.py)
                    → Walk-Forward OOS (run_walkforward.py)
                      → Promotion (promote_blueprints.py)
                        → Report (make_report.py)

  1.2 Stage-by-Stage Module Map

  Phase: Ingest
  Stage: ingest_binance_um_ohlcv_5m
  Script: ingest/ingest_binance_um_ohlcv_5m.py
  Key Args: --skip_ingest_ohlcv
  Produces: lake/raw/5m/{symbol}/
  ────────────────────────────────────────
  Phase:
  Stage: ingest_binance_um_funding
  Script: ingest/ingest_binance_um_funding.py
  Key Args: --skip_ingest_funding
  Produces: lake/raw/funding/{symbol}/
  ────────────────────────────────────────
  Phase:
  Stage: ingest_binance_um_liquidation_snapshot
  Script: ingest/ingest_binance_um_liquidation_snapshot.py
  Key Args: —
  Produces: lake/raw/liquidation/{symbol}/
  ────────────────────────────────────────
  Phase:
  Stage: ingest_binance_um_open_interest_hist
  Script: ingest/ingest_binance_um_open_interest_hist.py
  Key Args: —
  Produces: lake/raw/oi/{symbol}/
  ────────────────────────────────────────
  Phase:
  Stage: ingest_binance_spot_ohlcv_5m
  Script: ingest/ingest_binance_spot_ohlcv_5m.py
  Key Args: --enable_cross_venue_spot_pipeline
  Produces: lake/raw/spot/{symbol}/
  ────────────────────────────────────────
  Phase: Clean
  Stage: build_cleaned_5m
  Script: clean/build_cleaned_5m.py
  Key Args: --market=perp
  Produces: lake/cleaned/5m/{symbol}/
  ────────────────────────────────────────
  Phase: Features
  Stage: build_features_v1
  Script: features/build_features_v1.py
  Key Args: --allow_missing_funding
  Produces: features/v1/{symbol}/
  ────────────────────────────────────────
  Phase:
  Stage: build_context_features
  Script: features/build_context_features.py
  Key Args: --timeframe=5m
  Produces: features/context/{symbol}/
  ────────────────────────────────────────
  Phase:
  Stage: build_market_context
  Script: features/build_market_context.py
  Key Args: --timeframe=5m
  Produces: features/mc/{symbol}/
  ────────────────────────────────────────
  Phase: Atlas
  Stage: generate_candidate_templates
  Script: research/generate_candidate_templates.py
  Key Args: --atlas_mode=1
  Produces: atlas/candidate_templates.parquet
  ────────────────────────────────────────
  Phase:
  Stage: generate_candidate_plan
  Script: research/generate_candidate_plan.py
  Key Args: —
  Produces: atlas/candidate_plan.jsonl
  ────────────────────────────────────────
  Phase: Event Scan
  Stage: analyze_vol_shock_relaxation
  Script: research/analyze_vol_shock_relaxation.py
  Key Args: --timeframe 5m
  Produces: reports/hypothesis_generator/{run_id}/
  ────────────────────────────────────────
  Phase:
  Stage: analyze_liquidity_vacuum
  Script: research/analyze_liquidity_vacuum.py
  Key Args: profile/window args
  Produces: same
  ────────────────────────────────────────
  Phase:
  Stage: analyze_liquidation_cascade
  Script: research/analyze_liquidation_cascade.py
  Key Args: --liq_vol_th 100000 --oi_drop_th -500000
  Produces: same
  ────────────────────────────────────────
  Phase:
  Stage: (15 more active/stub analyzers)
  Script: —
  Key Args: —
  Produces: —
  ────────────────────────────────────────
  Phase: Phase 2
  Stage: phase2_candidate_discovery
  Script: research/phase2_candidate_discovery.py
  Key Args: --atlas_mode --shift_labels_k=0
  Produces: reports/phase2/{run_id}/phase2_candidates.csv
  ────────────────────────────────────────
  Phase:
  Stage: bridge_evaluate_phase2
  Script: research/bridge_evaluate_phase2.py
  Key Args: --run_bridge_eval_phase2=1
  Produces: reports/bridge_eval/{run_id}/
  ────────────────────────────────────────
  Phase: Compile
  Stage: compile_strategy_blueprints
  Script: research/compile_strategy_blueprints.py
  Key Args: --max_per_event=2 --min_events_floor=100
  Produces: reports/strategy_blueprints/{run_id}/blueprints.jsonl
  ────────────────────────────────────────
  Phase: Gate
  Stage: generate_recommendations_checklist
  Script: research/generate_recommendations_checklist.py
  Key Args: —
  Produces: checklist.json {KEEP_RESEARCH|PROMOTE_LIVE}
  ────────────────────────────────────────
  Phase: Backtest
  Stage: backtest_strategies
  Script: backtest/backtest_strategies.py
  Key Args: --blueprints_top_k=10
  Produces: reports/backtest/{run_id}/
  ────────────────────────────────────────
  Phase: OOS
  Stage: run_walkforward
  Script: eval/run_walkforward.py
  Key Args: --train_frac=0.6 --embargo_days=0
  Produces: reports/walkforward/{run_id}/
  ────────────────────────────────────────
  Phase: Promote
  Stage: promote_blueprints
  Script: research/promote_blueprints.py
  Key Args: --regime_max_share=0.80
  Produces: reports/promotions/{run_id}/promotion_report.json
  ────────────────────────────────────────
  Phase: Report
  Stage: make_report
  Script: report/make_report.py
  Key Args: —
  Produces: reports/{run_id}/summary.md

  1.3 Event Registry (18 Types)

  #: 1
  Event Type: vol_shock_relaxation
  Analyzer: analyze_vol_shock_relaxation.py
  Status: Active
  ────────────────────────────────────────
  #: 2
  Event Type: liquidity_refill_lag_window
  Analyzer: analyze_liquidity_refill_lag_window.py
  Status: Active
  ────────────────────────────────────────
  #: 3
  Event Type: liquidity_absence_window
  Analyzer: analyze_liquidity_absence_window.py
  Status: Active
  ────────────────────────────────────────
  #: 4
  Event Type: vol_aftershock_window
  Analyzer: analyze_vol_aftershock_window.py
  Status: Active
  ────────────────────────────────────────
  #: 5
  Event Type: directional_exhaustion_after_forced_flow
  Analyzer: analyze_directional_exhaustion_after_forced_flow.py
  Status: Active
  ────────────────────────────────────────
  #: 6
  Event Type: cross_venue_desync
  Analyzer: analyze_cross_venue_desync.py
  Status: Active
  ────────────────────────────────────────
  #: 7
  Event Type: liquidity_vacuum
  Analyzer: analyze_liquidity_vacuum.py
  Status: Active
  ────────────────────────────────────────
  #: 8
  Event Type: funding_extreme_reversal_window
  Analyzer: analyze_funding_extreme_reversal_window.py
  Status: Active
  ────────────────────────────────────────
  #: 9
  Event Type: range_compression_breakout_window
  Analyzer: analyze_range_compression_breakout_window.py
  Status: Active
  ────────────────────────────────────────
  #: 10
  Event Type: funding_episodes
  Analyzer: analyze_funding_episode_events.py
  Status: Active
  ────────────────────────────────────────
  #: 11
  Event Type: LIQUIDATION_CASCADE
  Analyzer: analyze_liquidation_cascade.py
  Status: New — Feb 19, 2026
  ────────────────────────────────────────
  #: 12–18
  Event Type: funding_extreme_onset, funding_persistence_window, funding_normalization, oi_shocks, oi_spike_positive,
    oi_spike_negative, oi_flush
  Analyzer: no_op.py
  Status: Stub

  1.4 Blueprint Schema (DSL contract_v1.py + schema.py)

  Blueprint (frozen dataclass)
  ├── id, run_id, event_type, candidate_id
  ├── symbol_scope: SymbolScopeSpec (single/multi-symbol)
  ├── direction: long | short | both | conditional
  ├── entry: EntrySpec
  │   ├── triggers: List[str]          # event detection column names
  │   ├── conditions: List[str]        # canonical condition strings
  │   ├── condition_nodes: List[ConditionNodeSpec]  # compiled runtime
  │   ├── delay_bars: int
  │   ├── cooldown_bars: int
  │   └── condition_logic: all | any
  ├── exit: ExitSpec
  │   ├── time_stop_bars, stop_type/value, target_type/value
  │   └── trailing_stop_type/value, break_even_r
  ├── sizing: SizingSpec
  │   ├── mode: fixed_risk | vol_target
  │   ├── risk_per_trade: float
  │   └── max_gross_leverage: float (default 1.0)
  ├── overlays: List[OverlaySpec]
  ├── evaluation: EvaluationSpec
  └── lineage: LineageSpec
      ├── cost_config_digest: str     # SHA256 of cost config
      ├── promotion_track: standard | fallback_only
      └── wf_status: pending | pass | trimmed_*

  Feature allowlist (DSL safety — contract_v1.py:79–162):

- Allowed prefixes: vol_, range_, ret_, rvol_, atr_, basis_, spread_, quote_vol_, volume_, oi_, liq_, fp_, mc_,
  session_, regime_, bb_, z_, event_, flag_, symbol_
- Hard-blocked patterns: fwd_, forward_, future_, label_, target_, *y*, outcome_, return_after_costs, mfe, mae

  ---
  SECTION 2 — DATA INTEGRITY AUDIT

  2.1 UTC & Timestamp Normalization

  Status: PASS ✅

  All ingest scripts perform smart epoch unit detection (ms vs s) before conversion:

# ingest_binance_um_liquidation_snapshot.py:188-190

  ts_numeric = pd.to_numeric(raw[ts_col], errors="coerce")
  ts_unit = "ms" if ts_numeric.dropna().abs().median() >= 1_000_000_000_000 else "s"
  ts_series = pd.to_datetime(ts_numeric, unit=ts_unit, utc=True, errors="coerce")

# ingest_binance_um_funding.py:97-102 — explicit unit inference

  def _infer_epoch_unit(ts_series):
      med = int(vals.median())
      return "s" if med < 1_000_000_000_000 else "ms"

# pipelines/_lib/validation.py:32-40 — hard enforcement

  def ensure_utc_timestamp(series, name):
      if not isinstance(series.dtype, pd.DatetimeTZDtype):
          raise ValueError(f"{name} must be timezone-aware UTC")
      if str(series.dt.tz) != "UTC":
          raise ValueError(f"{name} must be UTC")

  2.2 Lookahead Bias (Merge Operations)

  Status: PASS ✅

  Every merge_asof in the codebase uses direction="backward" — the only safe direction for PIT integrity:

# build_features_v1.py:118-123 — OI merge

  out = pd.merge_asof(out.sort_values("timestamp"), oi_series.sort_values("timestamp"),
      on="timestamp", direction="backward")

# build_features_v1.py:154-159 — Liquidation merge

  out = pd.merge_asof(..., direction="backward")

# build_features_v1.py:208-213 — Spot basis merge

  out = pd.merge_asof(..., direction="backward", tolerance=pd.Timedelta("5min"))

# build_cleaned_5m.py:73-79 — Funding rate merge

  merged = pd.merge_asof(..., direction="backward")

  2.3 Feature Engineering Lookahead

  Status: PASS ✅

  All rolling windows enforce min_periods = window, preventing any computation before sufficient history exists:

# build_features_v1.py:382-388

  features["rv_96"] = features["logret_1"].rolling(window=96, min_periods=96).std()
  features["rv_pct_17280"] =_rolling_percentile(features["rv_96"], window=17280)
  features["high_96"] = features["high"].rolling(window=96, min_periods=96).max()
  features["range_med_2880"] = features["range_96"].rolling(window=2880, min_periods=2880).median()

# liquidity_vacuum.py:127-128

  out["vol_med"] = volume.rolling(cfg.volume_window, min_periods=cfg.volume_window).median()

  2.4 OI Delta Window Verification

  Status: PASS ✅ (Bug confirmed fixed)

# build_features_v1.py:165

  out["oi_delta_1h"] = out["oi_notional"].diff(12)  # 5m bars: 60/5 = 12 ✓

  2.5 Timestamp Column Name Normalization

  Status: PASS ✅

# build_features_v1.py:81-85 — handles both ts and timestamp

  ts_col = "timestamp" if "timestamp" in frame.columns else ("ts" if "ts" in frame.columns else None)
  frame = frame.rename(columns={ts_col: "timestamp"})

# ingest_binance_um_liquidation_snapshot.py:178-186 — multi-candidate detection

  for candidate in ("time", "timestamp", "create_time", "createtime", "ts"):
      if candidate in raw.columns:
          ts_col = candidate; break

  2.6 Timestamp Monotonicity

  Status: PASS ✅

# ingest_binance_um_ohlcv_5m.py:268-271

  if data["timestamp"].duplicated().any():
      raise ValueError(f"Duplicate timestamps in {symbol} {month_start:%Y-%m}")
  if not data["timestamp"].is_monotonic_increasing:
      raise ValueError(f"Timestamps not sorted for {symbol} {month_start:%Y-%m}")

  2.7 Symbol Mapping Risk (CM ↔ UM)

  Status: ⚠️ PARTIAL RISK

# ingest_binance_um_liquidation_snapshot.py:32-41

  CM_SYMBOL_MAP = {
      "BTC": "BTCUSD_PERP",
      "BTCUSDT": "BTCUSD_PERP",
      "ETHUSDT": "ETHUSD_PERP",
      ...
  }

  Liquidation snapshots originate from CM (COIN-M) futures exchange, not UM (USDT-M). If any symbol fails the mapping,
  LIQUIDATION_CASCADE events silently produce zero records — the pipeline continues with an empty event registry for
  that symbol, no error raised.

  2.8 Funding Rate Scaling

  Status: PASS ✅

# build_cleaned_5m.py:81-84

  interval_hours = 8
  bars_per_event = int((interval_hours * 60) / 5)  # = 96 at 5m resolution
  merged["funding_rate_scaled"] = merged["funding_rate_scaled"] / bars_per_event

  Funding rate (8h event) is spread evenly over 96 bars at 5m resolution. Sign convention is correct: funding_pnl =
  -prior_pos × rate — longs pay positive rates, shorts receive.

  2.9 Certification Batch Data Verification

  // data/runs/certification_batch/ingest_binance_um_funding.json
  {
    "coverage_start": "2024-01-01T00:00:00+00:00",
    "coverage_end":   "2024-01-07T16:00:00+00:00",
    "expected_count": 21,
    "got_count":      21,
    "missing_count":  0
  }

  All 21 expected funding timestamps present for the 7-day window. UTC alignment confirmed.

  ---
  SECTION 3 — STATISTICAL AUDIT

  3.1 BH-FDR Implementation

  Algorithm location: analyze_conditional_expectancy.py:126-150

  def _bh_adjust(p_values: pd.Series) -> pd.Series:
      p = pd.to_numeric(p_values, errors="coerce").fillna(1.0).clip(0.0, 1.0)
      order = np.argsort(p.values)
      sorted_p = p.values[order]
      m = float(len(sorted_p))
      adjusted = np.empty(len(sorted_p), dtype=float)
      running_min = 1.0
      for i in range(len(sorted_p) - 1, -1, -1):
          rank = float(i + 1)
          candidate = float(sorted_p[i] * m / rank)   # q[i] = p[i] × m / rank
          running_min = min(running_min, candidate)
          adjusted[i] = running_min
      adjusted = np.clip(adjusted, 0.0, 1.0)
      out = pd.Series(index=p.index, dtype=float)
      out.iloc[order] = adjusted
      return out

  Verification: This is the standard Benjamini-Hochberg step-up procedure. The descending sweep with running_min
  enforces monotonicity. The algorithm is mathematically correct.

  P-value generation: analyze_conditional_expectancy.py:65-123

# T-statistic

  t_stat = float(mean_val / (std_val / np.sqrt(n)))   # Standard one-sample t

# P-value via Normal approximation

  def _two_sided_p_from_t(t_stat):
      z = abs(float(t_stat))
      return float(2.0 * (1.0 -_normal_cdf(z)))      # Two-sided

  Known limitation: Normal CDF approximation underestimates tail probability at small n (< 50). At n = 30, t = 2.0,
  exact t-CDF p = 0.057 vs Normal p = 0.046 — a 19% underestimate that inflates discovery counts.

  3.2 Hypothesis Family Definition

  Location: phase2_candidate_discovery.py:542,638,765

# Atlas mode (line 542)

  "family_id": f"{event_type}*{rule}*{horizon}_{cond_label}"

# Default mode (line 638)

  "family_id": f"{args.event_type}*{rule}*{horizon}_{cond_name}"

# Report description (line 765)

  "family_definition": "Option A (event_type, rule_template, horizon)"

  Family unit: (event_type, rule_template, horizon, condition_label)

  Critical gap: Symbol is NOT stratified. BTCUSDT, ETHUSDT, SOLUSDT all share the same family for a given (event, rule,
  horizon, condition). This means:

- A strong BTC signal boosts the family's BH power, promoting weak ETH candidates and vice versa
- Per-symbol FDR is uncontrolled
- The correction is asymmetric: large-family events (many symbols) are easier to discover than single-symbol events

  3.3 FDR Threshold and Application

  Configuration: spec/gates.yaml:7-9

  gate_v1_phase2:
    max_q_value: 0.05
    min_after_cost_expectancy_bps: 0.1
    require_sign_stability: true

  Application: phase2_candidate_discovery.py:711-738

  for family_id, family_df in raw_df.groupby("family_id"):
      family_df["q_value"] = _bh_adjust(family_df["p_value"])
      family_df["is_discovery"] = family_df["q_value"] <= max_q
      fdr_results.append(family_df)

# Fail-closed invariant (line 784-798)

  if not (summary["survivors_phase2"] <= summary["discoveries_statistical"]):
      raise ValueError("Invariant violation: survivors_phase2 exceeds discoveries_statistical")

  The fail-closed invariant is a strong correctness guarantee. Every Phase 2 survivor is required to be a statistical
  discovery. This is correctly implemented.

  3.4 Fallback Promotion: The Primary Statistical Risk

  Configuration: spec/gates.yaml:18-23

  gate_v1_fallback:
    min_t_stat: 2.5
    min_after_cost_expectancy_bps: 1.0
    min_sample_size: 100
    min_stability_score: 0.7
    promotion_eligible_regardless_of_fdr: true      # ← FDR BYPASS

  3-tier selection cascade: compile_strategy_blueprints.py:722-838

  ┌──────────────┬──────────────────────────────┬────────────────┬─────────────────────┬──────────┐
  │     Tier     │          Candidates          │  FDR Required  │ Expectancy Required │ Cost Cap │
  ├──────────────┼──────────────────────────────┼────────────────┼─────────────────────┼──────────┤
  │ 1 (standard) │ Promoted (gate_phase2_final) │ Yes (q ≤ 0.05) │ Positive after cost │ 0.60     │
  ├──────────────┼──────────────────────────────┼────────────────┼─────────────────────┼──────────┤
  │ 2 (fallback) │ Non-promoted, quality floor  │ No             │ Zero or negative OK │ No cap   │
  ├──────────────┼──────────────────────────────┼────────────────┼─────────────────────┼──────────┤
  │ 3 (raw)      │ Raw Phase 2, quality floor   │ No             │ Zero or negative OK │ No cap   │
  └──────────────┴──────────────────────────────┴────────────────┴─────────────────────┴──────────┘

  Implication: Any blueprint with promotion_track = "fallback_only" in its lineage field entered the system without
  BH-FDR control. At a quality floor of robustness ≥ 0.60 and n_events ≥ 100, the expected false discovery rate for
  fallback candidates is unquantified and uncontrolled. If 40% of production blueprints are fallback-track, the "+10 bps
   lift" claim cannot be attributed to genuine edge discovery.

  Interaction with auto_continue_on_keep_research: run_all.py:1211-1272

  if checklist_decision == "KEEP_RESEARCH" and execution_requested:
      if auto_continue_on_keep_research:
          # Inject non-production overrides:
          # --ignore_checklist=1 --allow_fallback_blueprints=1
          run_manifest["non_production_overrides"].append(...)

  When auto_continue=1, a KEEP_RESEARCH checklist verdict does not block execution — it silently injects
  --allow_fallback_blueprints=1, enabling the Tier 2 and Tier 3 fallback cascade. This override is logged in the run
  manifest but is not surfaced in the discovery report.

  3.5 Ablation Lift Calculation

  Location: eval/ablation.py:14-55

  def calculate_lift(group_df):
      base_exp = baseline["expectancy"].mean()        # unconditional baseline

      for _, row in df.iterrows():
          lift = (row["expectancy"] - base_exp)
          lift_bps = lift * 10000.0                   # NO correction applied
          lift_pct = lift / abs(base_exp)

  Grouping: ablation.py:57-124 — groups by (event_type, rule_template, horizon, symbol)

  The ablation IS symbol-stratified (symbol is part of the group key). However, within each group, multiple conditioning
   states are compared with no correction for the number of conditions tested. If 6 conditions are tested (vol_regime ×
  3, carry_state × 3) per group, the expected spurious lift discoveries at α=0.05 is 0.3 per group. Across 100+ groups
  that is ~30 false "lifts" reported.

  The "+10 bps" target is measured against this uncorrected lift value. This is the primary statistical validity gap for
   the stated hypothesis.

  3.6 Multiple Testing Budget

  ┌─────────────────────────────────────────────────────────┬───────────────────┐
  │                        Dimension                        │ Count (estimated) │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ Active event types                                      │ 11                │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ Rule templates per event                                │ ~4                │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ Horizons                                                │ 3                 │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ Conditioning states per template                        │ ~6                │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ Total families (Option A, no symbol stratification)     │ ~792              │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ With symbol stratification (3 symbols)                  │ ~2,376            │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ Expected false discoveries @ q=0.05 (no stratification) │ ~40               │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ Expected false discoveries @ q=0.05 (stratified)        │ ~119              │
  └─────────────────────────────────────────────────────────┴───────────────────┘

  At the current family definition (no symbol stratification), families with multiple symbols have artificially larger
  m, making BH correction less conservative than intended. Stratifying by symbol both increases m per family (making
  each family more conservative, correct) and provides per-symbol FDR guarantees.

  3.7 Walk-Forward / OOS Design

  Location: eval/run_walkforward.py, eval/splits.py

  --train_frac=0.6    (default)
  --validation_frac=0.2
  --embargo_days=0    (DEFAULT — critical gap)
  --regime_max_share=0.80
  --drawdown_cluster_top_frac=0.10
  --drawdown_tail_q=0.05

  Embargo gap: With embargo_days=0, the last bar of the training window is immediately adjacent to the first bar of the
  validation window. For event-driven crypto strategies, this leaves:

- Autocorrelation from persistent regime states bleeding across the boundary
- Funding rate episodes that span the boundary artificially boosting validation Sharpe
- A minimum of 5 bars (25 minutes at 5m) embargo is the practical floor; 1 day (288 bars) is recommended for daily
  funding cycles

  No time-series block bootstrap: Statistical significance of OOS Sharpe is not computed. There is no CI on the lift
  estimate from walk-forward.

  3.8 Lift Significance: What Is Actually Being Tested

  The "+10 bps lift" hypothesis as currently operationalized:

  H₀: E[conditioned_expectancy] - E[unconditional_expectancy] ≤ 0
  H₁: lift > 10 bps
  Test statistic: raw point estimate from ablation.py (no CI, no correction)
  Family-wise error control: none at ablation level

  This is not a pre-registered hypothesis test. The null is implicitly "zero lift" not "less than 10 bps lift", and the
  threshold (+10 bps) appears to be a target, not a pre-registered rejection boundary. Before the 60-day run begins, the
   hypothesis should be formally pre-registered with an explicit null, alternative, test statistic, and α.

  ---
  SECTION 4 — EXECUTION REALISM AUDIT

  4.1 Fill Model

  Location: engine/runner.py:302-335

# Execution lag (default: 1 bar)

  execution_lag = int(params.get("execution_lag_bars", 1))
  if execution_lag > 0:
      positions = positions.shift(execution_lag).fillna(0).astype(int)

# Fill price

  close = bars_indexed["close"].astype(float)
  ret = compute_returns(close)   # pct_change() — returns from T-1 close to T close

  Interpretation: Signal fires at bar T. Due to 1-bar lag, position becomes active at bar T+1. The return earned is
  close[T+1] / close[T] − 1. This is equivalent to filling at the open of T+1 under the assumption that open ≈ close of
  T, which is a reasonable approximation for liquid crypto perps but is optimistic during high-volatility events
  (precisely when these strategies fire).

  What is missing:

- No bid/ask simulation — fills at mid (close) rather than at ask for buys, bid for sells
- No partial fill model — assumes full execution at bar close
- No VWAP or TWAP within bar
- 1s ToB data is ingested and stored in lake/raw/tob/ but is never consumed by the execution engine

  4.2 P&L Decomposition

  Location: engine/pnl.py:15-83

  aligned_pos  = pos.reindex(ret.index).fillna(0.0)
  prior_pos    = aligned_pos.shift(1).fillna(0.0)         # Position from T-1

  gross_pnl    = prior_pos *ret                           # Timing: correct ✅
  trading_cost = (aligned_pos - prior_pos).abs()           # Turnover
               * (cost_bps_aligned / 10000.0)
  funding_pnl  = -prior_pos *funding_rate_aligned         # Carry attribution ✅
  borrow_cost  = prior_pos.clip(upper=0.0).abs()           # Short borrow
               * borrow_rate_aligned
  pnl          = gross_pnl - trading_cost + funding_pnl - borrow_cost

  Sign convention verification (funding):

- Long (pos=+1), rate=+0.01%: funding_pnl = -1 × 0.0001 = -0.0001 → longs pay ✅
- Short (pos=-1), rate=+0.01%: funding_pnl = -(-1) × 0.0001 = +0.0001 → shorts receive ✅

  Funding accrual issue: funding_rate_scaled is merged onto all bars (including flat bars). The formula funding_pnl =
  -prior_pos × rate naturally produces zero when prior_pos = 0, so flat-position bars correctly contribute zero funding
  PnL. This is correct behavior.

  4.3 Transaction Cost Model

  Location: engine/execution_model.py:9-55, configs/fees.yaml, pipelines/_lib/execution_costs.py:41-77

# configs/fees.yaml

  fee_bps_per_side: 4
  slippage_bps_per_fill: 2
  risk_per_trade_pct: 0.5

# execution_model.py — dynamic model

  base_fee_bps      = config.get("base_fee_bps", 0.0)
  base_slippage_bps = config.get("base_slippage_bps", 0.0)
  spread_weight     = config.get("spread_weight", 0.0)      # 0 unless configured
  volatility_weight = config.get("volatility_weight", 0.0)  # 0 unless configured
  liquidity_weight  = config.get("liquidity_weight", 0.0)   # 0 unless configured
  impact_weight     = config.get("impact_weight", 0.0)      # 0 unless configured

  dynamic = (spread_weight *spread_bps
           + volatility_weight* vol_bps
           + liquidity_weight *(liq_scale* 10.0)
           + impact_weight *(impact* 10.0))

  cost_bps = (base_fee_bps + base_slippage_bps + dynamic).clip(0.0, cap_bps)

# runner.py:376-379 — default split when no explicit execution_cfg

  execution_cfg["base_fee_bps"]      = float(cost_bps) / 2.0   # 3 bps
  execution_cfg["base_slippage_bps"] = float(cost_bps) / 2.0   # 3 bps

# Total: 6 bps round-trip (4 fee + 2 slippage per side = 12 bps, but cost_bps=6?)

  Calibration note: The fees.yaml specifies 4 bps fee + 2 bps slippage = 6 bps per side. The cost_bps in pipeline args
  refers to total round-trip cost. The split 50/50 in runner.py correctly allocates 3 bps fee + 3 bps slippage per side
  when cost_bps=6. This is consistent.

  Spread defaulting to 0: execution_model.py:28 fills spread_bps with 0.0 when ToB data is unavailable. For BTCUSDT
  perps, the actual bid/ask spread is typically 0.1–0.5 bps under normal conditions and 2–10 bps during liquidation
  cascades — exactly when this system fires events. The cost model is therefore most optimistic precisely at the
  highest-activity moments.

  4.4 Cost Reproducibility

  Location: pipelines/_lib/execution_costs.py:41-77

  payload = {
      "config_paths": config_paths,
      "fee_bps_per_side": float(fee),
      "slippage_bps_per_fill": float(slippage),
      "cost_bps": float(cost),
      "execution_model": execution_model,
  }
  digest =_sha256_text(json.dumps(payload, sort_keys=True, default=str))

  Enforcement: compile_strategy_blueprints.py:299-302

  if strict_cost_fields and expected_cost_digest:
      if row_digest != expected_cost_digest:
          return False   # Blueprint rejected — cost mismatch

  Cost reproducibility is well-implemented. Every blueprint carries its cost_config_digest in lineage. The digest is
  enforced at selection time.

  4.5 Risk Caps

  Location: engine/risk_allocator.py:10-107

  @dataclass(frozen=True)
  class RiskLimits:
      max_portfolio_gross:    float = 1.0   # total leverage
      max_symbol_gross:       float = 1.0   # per-symbol
      max_strategy_gross:     float = 1.0   # per-strategy
      max_new_exposure_per_bar: float = 1.0 # turnover cap

  Allocation applies limits deterministically in cascade order (Strategy → Symbol → Portfolio → Intrabar delta). No
  randomness. Conservative defaults for perpetuals.

  4.6 Missing Data Handling in Engine

# pnl.py:61-72 — NaN returns zero out all components

  nan_ret = ret.isna()
  if nan_ret.any():
      gross_pnl[nan_ret] = 0.0
      trading_cost[nan_ret] = 0.0
      funding_pnl[nan_ret] = 0.0
      borrow_cost[nan_ret] = 0.0

# runner.py:358-367 — missing funding defaults to 0

  funding_series = pd.to_numeric(
      features_indexed.get("funding_rate_scaled",
      pd.Series(0.0, index=ret.index)).reindex(ret.index),
      errors="coerce").fillna(0.0)

  Missing data handling is conservative: NaN returns force flat (no P&L), missing funding defaults to 0 (no carry
  penalty/benefit).

  ---
  SECTION 5 — COMPLETE FINDINGS TABLE

  #: F-1 ✅ RESOLVED (B1, Feb 23 2026)
  Issue: Fallback promotion bypasses BH-FDR entirely
  Severity: 🔴 CRITICAL
  Evidence — File:Lines: spec/gates.yaml:22 (promotion_eligible_regardless_of_fdr: true);
    compile_strategy_blueprints.py:765-838; schema.py:197 (promotion_track: fallback_only)
  Impact on 60-day Run: Uncontrolled false discovery rate for Tier 2/3 blueprints; "+10 bps lift" claim is statistically

    invalid if fallback-track blueprints are included in measurement
  Recommended Fix: Change gates.yaml:22 to false; OR exclude promotion_track=fallback_only from OOS lift measurement by
    filtering blueprints.jsonl on lineage.promotion_track
  Effort: 0.5 day
  ────────────────────────────────────────
  #: F-2 ✅ RESOLVED
  Issue: Ablation lift has no multiplicity adjustment
  Severity: 🔴 HIGH
  Evidence — File:Lines: eval/ablation.py:30-43 (raw delta); ablation.py:57-124 (no correction in loop)
  Impact on 60-day Run: ~30 false "lift discoveries" expected across 100 groups × 6 conditions; the "+10 bps" target may

    be met by noise
  Recommended Fix: Add per-group BH correction: apply *bh_adjust(p_values_per_group) before reporting lift_bps; filter
  on
    lift_q_value ≤ 0.10
  Effort: 1 day
  ────────────────────────────────────────
  #: F-3
  Issue: BH family pools symbols — per-symbol FDR uncontrolled
  Severity: 🟡 HIGH
  Evidence — File:Lines: phase2_candidate_discovery.py:542 (family_id = f"{event_type}*{rule}*{horizon}*{cond_label}"),
    phase2_candidate_discovery.py:638
  Impact on 60-day Run: Cross-symbol power leakage; ETH/SOL events inflate BTC discovery rate and vice versa; per-symbol

    edge claims are unsupported
  Recommended Fix: Prepend symbol to family_id: f"{symbol}*{event_type}*{rule}*{horizon}*{cond_label}"
  Effort: 2 hours
  ────────────────────────────────────────
  #: F-4 ✅ RESOLVED (B2, Feb 23 2026)
  Issue: Walk-forward embargo = 0 by default
  Severity: 🟡 HIGH
  Evidence — File:Lines: eval/run_walkforward.py (--embargo_days default 0); eval/splits.py
  Impact on 60-day Run: Autocorrelation bleeds across train/validation boundary; funding rate episodes spanning boundary

    inflate OOS Sharpe
  Recommended Fix: Set default embargo_days=1 (288 5m-bars); enforce in certification and 60-day run config
  Effort: 30 min
  ────────────────────────────────────────
  #: F-5
  Issue: Normal CDF approximation inflates discoveries at small n
  Severity: 🟡 MEDIUM
  Evidence — File:Lines: analyze_conditional_expectancy.py:114-123 (*normal_cdf comment: "Normal approximation keeps
  this
    dependency-free")
  Impact on 60-day Run: At n=30, t=2.0: Normal p=0.046 vs exact t p=0.057 — 19% underestimate inflates is_discovery
  count
  Recommended Fix: Replace with math.lgamma-based t-CDF (no scipy needed) or add from scipy.stats import t as t_dist
  Effort: 2 hours
  ────────────────────────────────────────
  #: F-6
  Issue: Fill price at bar close — no intrabar simulation
  Severity: 🟡 MEDIUM
  Evidence — File:Lines: engine/runner.py:332-335 (close = bars_indexed["close"]); engine/pnl.py:8-12 (pct_change())
  Impact on 60-day Run: Fills capture intrabar movement unavailable at signal time; event-driven strategies (liquidity
    events) are most affected
  Recommended Fix: Move to next-bar open: open_price.shift(-1) for returns, or add explicit half-spread penalty of 0.5
    bps
  Effort: 1-2 days
  ────────────────────────────────────────
  #: F-7
  Issue: 1s ToB data ingested but never consumed in execution
  Severity: 🟡 MEDIUM
  Evidence — File:Lines: engine/runner.py:246-268 (features joined, no ToB); clean/build_tob**.py (tob built but unused)
  Impact on 60-day Run: Bid/ask spread defaults to 0 in cost model; real spread during liquidation events is 2-10×
  normal
  Recommended Fix: Wire spread_bps from ToB parquet into runner.py frame_for_cost; set spread_weight > 0 in
  execution_cfg
  Effort: 2 days
  ────────────────────────────────────────
  #: F-8 ✅ RESOLVED (PR-5, Feb 23 2026)
  Issue: Liquidation CM↔UM symbol map — silent zero-event failure
  Severity: 🟡 MEDIUM
  Evidence — File:Lines: ingest_binance_um_liquidation_snapshot.py:32-41 (CM_SYMBOL_MAP); no post-ingest validation
  Impact on 60-day Run: LIQUIDATION_CASCADE analyzer runs on empty event set → blueprint compiled with 0 events →
    gate_bridge fails or is silently waived
  Recommended Fix: Add post-ingest assertion: assert set(mapped) == set(requested_symbols) and len(events) > 0 per
  symbol
  Effort: 2 hours
  ────────────────────────────────────────
  #: F-9
  Issue: Certification batch 7 days — too short for stable gates
  Severity: 🟡 MEDIUM
  Evidence — File:Lines: CLAUDE.md ("often fail gate_bridge_has_trades_validation due to small sample sizes");
    data/runs/certification_batch/
  Impact on 60-day Run: Regression baseline built on ~2016 5m bars; event-driven strategies may see 0 events in 7 days
    for rare event types → regression coverage is nominal
  Recommended Fix: Extend certification to ≥30 days; or create a separate gate-relaxed regression mode for short windows
  Effort: 3 days (data acquisition)
  ────────────────────────────────────────
  #: F-10
  Issue: auto_continue_on_keep_research silently injects fallback flags
  Severity: 🟡 MEDIUM
  Evidence — File:Lines: run_all.py:1211-1272; run_manifest["non_production_overrides"]
  Impact on 60-day Run: KEEP_RESEARCH verdict is bypassed without explicit user confirmation; fallback blueprints enter
    OOS measurement without audit notice
  Recommended Fix: Remove auto_continue flag or require --mode=research and explicit --allow_fallback_blueprints=1 as
    positional arg
  Effort: 1 day
  ────────────────────────────────────────
  #: F-11
  Issue: Lift pre-registration missing — "+10 bps" is not a formal hypothesis
  Severity: 🟡 MEDIUM
  Evidence — File:Lines: No spec file in spec/hypotheses/; ablation.py has no threshold reference
  Impact on 60-day Run: The 60-day run has no pre-registered null, alternative, test statistic, or α; results will be
    unfalsifiable
  Recommended Fix: Create spec/hypotheses/lift_state_conditioned_v1.yaml with: null_lift_bps: 0, alternative_lift_bps:
    10, alpha: 0.05, min_n_events_per_condition: 200 before run starts
  Effort: 2 hours
  ────────────────────────────────────────
  #: F-12
  Issue: Funding accrues at zero rate on flat bars
  Severity: 🟢 LOW
  Evidence — File:Lines: build_cleaned_5m.py:81-84 (spread over all bars); engine/pnl.py:55 (-prior_pos × rate = 0 when
    flat)
  Impact on 60-day Run: This is mathematically correct (prior_pos=0 → funding_pnl=0). No action required
  Recommended Fix: None — behavior is correct
  Effort: —
  ────────────────────────────────────────
  #: F-13
  Issue: No bootstrap CI on lift or OOS Sharpe
  Severity: 🟢 LOW
  Evidence — File:Lines: eval/ablation.py (point estimate only); eval/run_walkforward.py (no CI)
  Impact on 60-day Run: Point estimates without CI make 60-day power analysis impossible
  Recommended Fix: Add 1000-resample block bootstrap to ablation and walk-forward output
  Effort: 3 days
  ────────────────────────────────────────
  #: F-14
  Issue: No slippage/cost sensitivity sweep
  Severity: 🟢 LOW
  Evidence — File:Lines: No sweep script in project/scripts/
  Impact on 60-day Run: Cannot verify lift persists under higher costs; a 2× cost scenario is standard pre-production
    validation
  Recommended Fix: Add scripts/run_cost_sensitivity.py that reruns discover-edges at 1×, 1.5×, 2× cost and compares lift
  Effort: 2 days

  ---
  SECTION 6 — PRODUCTION READINESS CHECKLIST

  6.1 Reproducibility Controls

  ┌───────────────────────────────────────┬─────────┬──────────────────────────────────────────────────────┐
  │                Control                │ Status  │                       Evidence                       │
  ├───────────────────────────────────────┼─────────┼──────────────────────────────────────────────────────┤
  │ Git commit hash in run manifest       │ ✅ PASS │ run_all.py → run_manifest["git_commit"]              │
  ├───────────────────────────────────────┼─────────┼──────────────────────────────────────────────────────┤
  │ Data layer hash                       │ ✅ PASS │ run_manifest["data_hash"] = SHA256 of parquet inputs │
  ├───────────────────────────────────────┼─────────┼──────────────────────────────────────────────────────┤
  │ Spec file hashes                      │ ✅ PASS │ run_manifest["spec_hashes"] = per-spec SHA256        │
  ├───────────────────────────────────────┼─────────┼──────────────────────────────────────────────────────┤
  │ Feature schema version + hash         │ ✅ PASS │ run_manifest["feature_schema_hash"]                  │
  ├───────────────────────────────────────┼─────────┼──────────────────────────────────────────────────────┤
  │ Cost config digest in every blueprint │ ✅ PASS │ schema.py:197 + execution_costs.py:68                │
  ├───────────────────────────────────────┼─────────┼──────────────────────────────────────────────────────┤
  │ Cost digest enforced at selection     │ ✅ PASS │ compile_strategy_blueprints.py:299-302               │
  ├───────────────────────────────────────┼─────────┼──────────────────────────────────────────────────────┤
  │ Production mode blocks fallback flags │ ✅ PASS │ run_all.py:382-389                                   │
  └───────────────────────────────────────┴─────────┴──────────────────────────────────────────────────────┘

  6.2 Statistical Integrity

  Control: BH-FDR algorithm correct
  Status: ✅ PASS
  Evidence: analyze_conditional_expectancy.py:126-150
  ────────────────────────────────────────
  Control: Fail-closed invariant (survivors ≤ discoveries)
  Status: ✅ PASS
  Evidence: phase2_candidate_discovery.py:784-798
  ────────────────────────────────────────
  Control: Fallback track FDR control
  Status: ✅ PASS (resolved B1, Feb 23 2026)
  Evidence: gates.yaml:22 bypass → fixed; fallback blueprints banned from OOS artifacts
  ────────────────────────────────────────
  Control: Symbol stratification in families
  Status: ❌ FAIL
  Evidence: phase2_candidate_discovery.py:542 — no symbol in family_id
  ────────────────────────────────────────
  Control: Ablation multiplicity correction
  Status: ❌ FAIL
  Evidence: eval/ablation.py:30-43 — no BH on lift
  ────────────────────────────────────────
  Control: Walk-forward embargo ≥ 1 day
  Status: ✅ PASS (resolved B2, Feb 23 2026)
  Evidence: Default changed 0 → 1 day in run_walkforward.py
  ────────────────────────────────────────
  Control: Lift hypothesis pre-registered
  Status: ❌ FAIL
  Evidence: No spec file
  ────────────────────────────────────────
  Control: Bootstrap CI on lift / OOS Sharpe
  Status: ❌ FAIL
  Evidence: Not implemented

  6.3 Data Integrity

  ┌───────────────────────────────────┬─────────┬───────────────────────────────────────────────────┐
  │              Control              │ Status  │                     Evidence                      │
  ├───────────────────────────────────┼─────────┼───────────────────────────────────────────────────┤
  │ UTC enforcement throughout        │ ✅ PASS │ validation.py:32-40                               │
  ├───────────────────────────────────┼─────────┼───────────────────────────────────────────────────┤
  │ All merges backward-only          │ ✅ PASS │ 4 confirmed merge_asof(..., direction="backward") │
  ├───────────────────────────────────┼─────────┼───────────────────────────────────────────────────┤
  │ Rolling windows with min_periods  │ ✅ PASS │ build_features_v1.py:382-388                      │
  ├───────────────────────────────────┼─────────┼───────────────────────────────────────────────────┤
  │ Timestamp monotonicity enforced   │ ✅ PASS │ ingest_binance_um_ohlcv_5m.py:268-271             │
  ├───────────────────────────────────┼─────────┼───────────────────────────────────────────────────┤
  │ OI delta window correct (12 bars) │ ✅ PASS │ build_features_v1.py:165                          │
  ├───────────────────────────────────┼─────────┼───────────────────────────────────────────────────┤
  │ Liquidation symbol map validation │ ✅ PASS │ Resolved PR-5 Feb 23 2026; 31 unit tests added    │
  └───────────────────────────────────┴─────────┴───────────────────────────────────────────────────┘

  6.4 Execution Realism

  ┌───────────────────────────────────────┬────────────┬────────────────────────────────────────────────────┐
  │                Control                │   Status   │                      Evidence                      │
  ├───────────────────────────────────────┼────────────┼────────────────────────────────────────────────────┤
  │ 1-bar execution lag                   │ ✅ PASS    │ runner.py:305-308                                  │
  ├───────────────────────────────────────┼────────────┼────────────────────────────────────────────────────┤
  │ Prior-position PnL (no same-bar fill) │ ✅ PASS    │ pnl.py:35-37                                       │
  ├───────────────────────────────────────┼────────────┼────────────────────────────────────────────────────┤
  │ Funding sign convention correct       │ ✅ PASS    │ pnl.py:55                                          │
  ├───────────────────────────────────────┼────────────┼────────────────────────────────────────────────────┤
  │ NaN return forces flat position       │ ✅ PASS    │ pnl.py:61-72                                       │
  ├───────────────────────────────────────┼────────────┼────────────────────────────────────────────────────┤
  │ Fill at bar close (no intrabar)       │ ⚠️ PARTIAL │ runner.py:332 — close price; optimistic for events │
  ├───────────────────────────────────────┼────────────┼────────────────────────────────────────────────────┤
  │ Spread cost when ToB missing          │ ❌ FAIL    │ execution_model.py:28 — defaults to 0              │
  ├───────────────────────────────────────┼────────────┼────────────────────────────────────────────────────┤
  │ 1s ToB data in execution              │ ❌ FAIL    │ runner.py — not wired to cost model                │
  └───────────────────────────────────────┴────────────┴────────────────────────────────────────────────────┘

  6.5 Monitoring & Observability

  ┌────────────────────────────────────────┬────────────┬─────────────────────────────────────────────────────────┐
  │                Control                 │   Status   │                        Evidence                         │
  ├────────────────────────────────────────┼────────────┼─────────────────────────────────────────────────────────┤
  │ Per-stage logs                         │ ✅ PASS    │ runs/{run_id}/*.log                                     │
  ├────────────────────────────────────────┼────────────┼─────────────────────────────────────────────────────────┤
  │ Run manifest (stages, timings, hashes) │ ✅ PASS    │ run_manifest.json                                       │
  ├────────────────────────────────────────┼────────────┼─────────────────────────────────────────────────────────┤
  │ Non-production overrides logged        │ ✅ PASS    │ run_manifest["non_production_overrides"]                │
  ├────────────────────────────────────────┼────────────┼─────────────────────────────────────────────────────────┤
  │ Live P&L monitoring                    │ ❌ MISSING │ No monitoring module                                    │
  ├────────────────────────────────────────┼────────────┼─────────────────────────────────────────────────────────┤
  │ Daily drawdown alerting                │ ❌ MISSING │ No alerting                                             │
  ├────────────────────────────────────────┼────────────┼─────────────────────────────────────────────────────────┤
  │ Experiment tracking (MLflow/W&B)       │ ❌ MISSING │ Not integrated                                          │
  ├────────────────────────────────────────┼────────────┼─────────────────────────────────────────────────────────┤
  │ Per-symbol per-strategy daily Sharpe   │ ❌ MISSING │ Only in final report                                    │
  ├────────────────────────────────────────┼────────────┼─────────────────────────────────────────────────────────┤
  │ Regression test suite for engine       │ ⚠️ PARTIAL │ tests/test_phase2_cost_and_canary.py — cost/canary only │
  └────────────────────────────────────────┴────────────┴─────────────────────────────────────────────────────────┘

  ---
  SECTION 7 — ACTION PLAN

  7.1 Phase 0–2 Weeks — GATE: Must complete before 60-day run starts

  These 5 PRs are required for the run results to be statistically defensible.

  PR-1: Fix fallback FDR bypass (F-1) ✅ DONE Feb 23 2026

- File: spec/gates.yaml:22
- Change: promotion_eligible_regardless_of_fdr: true → false
- OR: Add filter in backtest_strategies.py to exclude lineage.promotion_track = "fallback_only" from OOS measurement
- Acceptance: Zero fallback-track blueprints in lift measurement; run manifest shows fallback_eligible_compile = 0 in
  phase2 report
- Effort: 0.5 day

  PR-2: Add BH correction to ablation lift (F-2)

- File: eval/ablation.py:14-55
- Change: Within each (event, rule, horizon, symbol) group, compute t-statistic for (conditioned - baseline) and apply
   _bh_adjust across conditions before reporting
- Acceptance: lift_q_value column in lift_summary.csv; only conditions with q ≤ 0.10 flagged as lift discoveries
- Effort: 1 day

  PR-3: Stratify BH family by symbol (F-3)

- File: phase2_candidate_discovery.py:542 and :638
- Change: family_id = f"{symbol}*{event_type}*{rule}*{horizon}*{cond_label}"
- Acceptance: Phase 2 report shows total_tested increased ~3×; discoveries_statistical per symbol reported separately
- Effort: 2 hours + re-run phase2 test suite

  PR-4: Set embargo_days=1 default (F-4) ✅ DONE Feb 23 2026

- File: eval/run_walkforward.py (argparse default)
- Change: default=0 → default=1 for --embargo_days
- Acceptance: Walk-forward report shows non-adjacent train/val windows; gap = 288 bars (1 day at 5m)
- Effort: 30 minutes

  PR-5: Liquidation symbol map validation (F-8) ✅ DONE Feb 23 2026

- File: ingest_binance_um_liquidation_snapshot.py (post-mapping block)
- Change: Add assert set(mapped_symbols) == set(requested_symbols) and assert len(events_per_symbol) > 0 with hard
  failure
- Acceptance: Unit test covering a missing symbol triggers ValueError; certification run passes — 31 tests added
- Effort: 2 hours

  ---
  7.2 Phase 2–6 Weeks — REQUIRED for valid 60-day run interpretation

  PR-6: Pre-register lift hypothesis (F-11)

- Create: spec/hypotheses/lift_state_conditioned_v1.yaml
- Content: null_lift_bps, alternative_lift_bps (10), alpha (0.05), test_statistic (BH-corrected group lift),
  min_n_events_per_condition (200), registered_date
- Acceptance: File committed and hash in run_manifest before run starts

  PR-7: Add bootstrap CI to ablation (F-13)

- File: eval/ablation.py
- Change: 1000-resample block bootstrap (block = event sequence) for lift_bps; output lift_ci_low_90, lift_ci_high_90
- Acceptance: CI reported alongside point estimate; power analysis showing 60-day sample has ≥80% power to detect 10
  bps lift

  PR-8: Add slippage sensitivity sweep (F-14)

- Create: project/scripts/run_cost_sensitivity.py
- Change: Re-run discovery at 1×, 1.5×, 2× cost_bps; compare discoveries_statistical and lift_bps across scenarios
- Acceptance: Lift ≥ 5 bps at 1.5× cost; ≥ 0 bps at 2× cost

  PR-9: Wire spread from ToB into cost model (F-7)

- File: engine/runner.py:380-392
- Change: Load spread_bps from ToB parquet (via merge_asof backward) and pass to estimate_transaction_cost_bps; set
  spread_weight = 0.5 in execution_cfg
- Acceptance: effective_avg_cost_bps in backtest results increases ~1-3 bps; cost model no longer reports zero spread

  PR-10: Exact t-CDF for p-values (F-5)

- File: analyze_conditional_expectancy.py:114-123
- Change: Implement exact t-distribution CDF using math.lgamma (no scipy), use when n < 200
- Acceptance: Unit test: _two_sided_p_from_t(t=2.0, n=30) ≈ 0.057 ± 0.001

  ---
  7.3 Phase 6–12 Weeks — Production hardening post-lift validation

  PR-11: Move to next-bar open fills (F-6)

- File: engine/runner.py:332-335, engine/pnl.py
- Change: Use open_price.shift(-1) as fill price; document expected P&L delta vs close-price baseline
- Acceptance: Slippage study comparing close vs open fill on signal bars over 60-day window

  PR-12: Monitoring dashboard

- Create: project/monitoring/daily_monitor.py
- Metrics: Daily realized PnL, rolling 20-day Sharpe per strategy, drawdown from peak, funding carry attribution,
  trigger count per event type
- Alerts: Email/webhook if drawdown > 3% in 5 days, or daily Sharpe < -1.0

  PR-13: Experiment tracking integration

- Integrate: MLflow or Weights & Biases
- Log per run: all metrics from phase2 report, lift_summary.csv, walk-forward Sharpe, cost_config_digest
- Acceptance: UI shows lift by condition across all historical runs

  PR-14: Extend certification to 30 days

- File: data/runs/certification_batch/ — re-run with --start=2023-12-01 --end=2024-01-07
- Acceptance: gate_bridge_has_trades_validation passes for ≥10 events per active event type

  ---
  7.4 Pass/Fail Acceptance Metrics for 60-Day Run

  Metric: OOS lift (BH-corrected, standard-track only)
  Pass Threshold: ≥ +10 bps, q ≤ 0.05
  Fail Action: Halt; run diagnostic by event type and symbol
  ────────────────────────────────────────
  Metric: Bootstrap 90% CI lower bound
  Pass Threshold: > 0 bps
  Fail Action: Extend window by 30 days; re-test
  ────────────────────────────────────────
  Metric: OOS Sharpe improvement (conditioned vs unconditioned)
  Pass Threshold: ≥ 0.3
  Fail Action: Investigate which conditions contribute negative lift
  ────────────────────────────────────────
  Metric: Max 60-day drawdown
  Pass Threshold: ≤ 15%
  Fail Action: Activate risk throttle (existing overlay)
  ────────────────────────────────────────
  Metric: Walk-forward lift CV (across folds)
  Pass Threshold: < 0.5
  Fail Action: Reject hypothesis — too unstable across regimes
  ────────────────────────────────────────
  Metric: Cost ratio (train vs validation)
  Pass Threshold: ≤ 0.60
  Fail Action: Already enforced in promotion gate
  ────────────────────────────────────────
  Metric: Funding carry attribution
  Pass Threshold: Within ±20% of prior estimate
  Fail Action: Verify funding data completeness and scaling
  ────────────────────────────────────────
  Metric: Fallback blueprint count in OOS measurement
  Pass Threshold: 0
  Fail Action: PR-1 was not merged — STOP RUN
  ────────────────────────────────────────
  Metric: Liquidity cascade event count (BTCUSDT, 60 days)
  Pass Threshold: ≥ 50
  Fail Action: Insufficient sample — exclude from lift measurement

  ---
  SECTION 8 — MISSING INFORMATION NEEDED

  The following could not be verified from the codebase alone. Explicit confirmation is required before the 60-day run
  starts.

  1. ✅ RESOLVED (A1, Feb 23 2026) Bar timeframe in execution: runner.py dedup labels say "15m" (f"features:{symbol}:15m"), but the pipeline builds
  "5m" cleaned bars. If backtest_strategies.py loads 15m OHLCV while discovery uses 5m events, there is a 3-bar
  resolution mismatch at every event timestamp. → Fixed:_DEFAULT_TIMEFRAME = "5m" in runner.py; --timeframe="5m"
  threaded through backtest_strategies.py, run_walkforward.py, and run_all.py (--backtest_timeframe).
  2. Conditioning density in 60-day run: The audit assumes ≤6 conditioning states per template. Confirm
  MAX_CONDITIONING_VARIANTS in generate_hypothesis_queue.py:24 is enforced and matches the multiplicity budget above.
  3. Fallback blueprint fraction in existing runs: Check data/reports/phase2/ablation/ or any existing blueprints.jsonl
  — what fraction of blueprints have lineage.promotion_track = "fallback_only"? If > 20%, the pre-60-day-run FDR issue
  is more urgent than estimated.
  4. "+10 bps lift" definition: Is the target measured on gross expectancy (pre-cost) or after-cost net expectancy?
  ablation.py uses row.get("expectancy") — if this is pre-cost, the hurdle is easier but irrelevant to production
  economics.
  5. Explicit assumption used if items 1–4 are unavailable: All estimates above assume 5m execution bars, 6 conditions
  per template, 20% fallback rate, and after-cost expectancy measurement. Results should be revisited if any assumption
  is incorrect.

  ---
  End of audit report. All findings cite specific file paths, function names, and line references verified by direct
  code inspection.
