# Mechanism Audit And New Candidate Mechanisms

## Current Phase-1 Mechanisms In Pipeline

The following event families are currently integrated through phase1 -> phase2 -> candidate export:

1. `vol_shock_relaxation`
2. `liquidity_refill_lag_window`
3. `liquidity_absence_window`
4. `vol_aftershock_window`
5. `directional_exhaustion_after_forced_flow`
6. `cross_venue_desync`
7. `liquidity_vacuum`
8. `funding_extreme_reversal_window`
9. `range_compression_breakout_window`

## Coverage Assessment

Broadly covered:

1. Volatility shocks and post-shock decay.
2. Liquidity scarcity/refill and range behavior.
3. Cross-venue misalignment.
4. Directional exhaustion proxies.
5. Single-point funding stress.

Under-covered:

1. Funding path dependence (state over multiple funding cycles).
2. Regime transitions in volatility of volatility.
3. Multi-asset relative-value dislocations.
4. Structural crowding or concentration proxies.
5. Event interactions (compound mechanisms) with explicit joint anchors.

## New Candidate Mechanisms (Not Yet Implemented)

Each candidate below is intentionally named as a concrete event family for future phase1 analyzers.

1. `funding_sign_flip_shock_window`
Definition: abrupt funding sign flip with large absolute jump, then measure reversal/continuation hazard.
Primary fields: `funding_rate_scaled`, `ret_1`, `rv_96`.

2. `funding_carry_crowding_window`
Definition: prolonged same-sign elevated funding followed by crowding unwind window.
Primary fields: rolling funding sign streak, funding magnitude percentile.

3. `vol_of_vol_transition_window`
Definition: transition from low vol-of-vol to high vol-of-vol state and conditional tail behavior.
Primary fields: rolling std of `rv_96`, `ret_1`.

4. `range_regime_flip_window`
Definition: compression regime followed by expansion regime confirmation across multiple bars.
Primary fields: `range_96`, `range_med_*`, breakout confirmation count.

5. `tail_cluster_decay_window`
Definition: clustered large return events and subsequent hazard decay profile.
Primary fields: `ret_1` tail indicator runs, post-cluster hazard curve.

6. `session_handoff_dislocation_window`
Definition: price/rv dislocation around major session handoff times (UTC 00/08/16) with control matching by hour.
Primary fields: `timestamp`, `rv_96`, `ret_1`.

7. `cross_symbol_beta_break_window`
Definition: symbol return residual vs dynamic market beta exceeds threshold then mean-reversion window.
Primary fields: per-symbol returns + market proxy return.

8. `funding_price_divergence_window`
Definition: funding pressure and price direction diverge materially, test unwind probability.
Primary fields: `funding_rate_scaled`, rolling return trend.

## Priority Order For Build-Out

1. `funding_sign_flip_shock_window`
2. `funding_carry_crowding_window`
3. `vol_of_vol_transition_window`
4. `cross_symbol_beta_break_window`

## Minimal Acceptance Criteria For Any New Mechanism

1. Explicit non-overlapping event definition.
2. Matched controls with documented matching keys.
3. Phase1 artifacts: events, controls, matched deltas, hazards, phase/sign stability, summary.
4. Phase2 gating pass on CI and no schema-specific assumptions.
5. Included in `run_all` phase2 map and `export_edge_candidates` chain.
