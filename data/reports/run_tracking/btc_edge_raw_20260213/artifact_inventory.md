# Run Tracking: btc_edge_raw_20260213

## Command

`RUN_ID=btc_edge_raw_20260213 SYMBOLS=BTCUSDT START=2020-01-01 END=2025-12-31 make discover-edges-from-raw`

## Artifact Counts

- Report artifacts: **122**
- Runtime logs/metadata: **25**

## Top Edge Candidates (by edge_score)

| event | candidate_id | edge_score | n_events |
|---|---|---:|---:|
| vol_shock_relaxation | vol_shock_relaxation_0 | 0.441674 | 97 |
| vol_shock_relaxation | vol_shock_relaxation_1 | 0.441674 | 97 |
| liquidity_refill_lag_window | liquidity_refill_lag_window_0 | 0.156217 | 211 |
| liquidity_refill_lag_window | liquidity_refill_lag_window_1 | 0.156217 | 211 |
| range_compression_breakout_window | range_compression_breakout_window_0 | 0.124099 | 189 |

## Key Output Paths

- `data/reports/edge_candidates/btc_edge_raw_20260213/edge_candidates_normalized.csv`
- `data/runs/btc_edge_raw_20260213/export_edge_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/<event>/phase2_candidates.csv`

## Report Artifacts

- `data/reports/cross_venue_desync/btc_edge_raw_20260213/cross_venue_desync_controls.csv`
- `data/reports/cross_venue_desync/btc_edge_raw_20260213/cross_venue_desync_events.csv`
- `data/reports/cross_venue_desync/btc_edge_raw_20260213/cross_venue_desync_hazards.csv`
- `data/reports/cross_venue_desync/btc_edge_raw_20260213/cross_venue_desync_matched_deltas.csv`
- `data/reports/cross_venue_desync/btc_edge_raw_20260213/cross_venue_desync_phase_stability.csv`
- `data/reports/cross_venue_desync/btc_edge_raw_20260213/cross_venue_desync_sign_stability.csv`
- `data/reports/cross_venue_desync/btc_edge_raw_20260213/cross_venue_desync_summary.json`
- `data/reports/cross_venue_desync/btc_edge_raw_20260213/cross_venue_desync_summary.md`
- `data/reports/directional_exhaustion_after_forced_flow/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow_controls.csv`
- `data/reports/directional_exhaustion_after_forced_flow/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow_events.csv`
- `data/reports/directional_exhaustion_after_forced_flow/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow_hazards.csv`
- `data/reports/directional_exhaustion_after_forced_flow/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow_matched_deltas.csv`
- `data/reports/directional_exhaustion_after_forced_flow/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow_phase_stability.csv`
- `data/reports/directional_exhaustion_after_forced_flow/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow_sign_stability.csv`
- `data/reports/directional_exhaustion_after_forced_flow/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow_summary.json`
- `data/reports/directional_exhaustion_after_forced_flow/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow_summary.md`
- `data/reports/edge_candidates/btc_edge_raw_20260213/edge_candidates_normalized.csv`
- `data/reports/edge_candidates/btc_edge_raw_20260213/edge_candidates_normalized.json`
- `data/reports/funding_extreme_reversal_window/btc_edge_raw_20260213/funding_extreme_reversal_window_controls.csv`
- `data/reports/funding_extreme_reversal_window/btc_edge_raw_20260213/funding_extreme_reversal_window_events.csv`
- `data/reports/funding_extreme_reversal_window/btc_edge_raw_20260213/funding_extreme_reversal_window_hazards.csv`
- `data/reports/funding_extreme_reversal_window/btc_edge_raw_20260213/funding_extreme_reversal_window_matched_deltas.csv`
- `data/reports/funding_extreme_reversal_window/btc_edge_raw_20260213/funding_extreme_reversal_window_phase_stability.csv`
- `data/reports/funding_extreme_reversal_window/btc_edge_raw_20260213/funding_extreme_reversal_window_sign_stability.csv`
- `data/reports/funding_extreme_reversal_window/btc_edge_raw_20260213/funding_extreme_reversal_window_summary.json`
- `data/reports/funding_extreme_reversal_window/btc_edge_raw_20260213/funding_extreme_reversal_window_summary.md`
- `data/reports/hypothesis_generator/btc_edge_raw_20260213/dataset_introspection.json`
- `data/reports/hypothesis_generator/btc_edge_raw_20260213/fusion_hypotheses.json`
- `data/reports/hypothesis_generator/btc_edge_raw_20260213/hypothesis_candidates.json`
- `data/reports/hypothesis_generator/btc_edge_raw_20260213/phase1_hypothesis_queue.csv`
- `data/reports/hypothesis_generator/btc_edge_raw_20260213/phase1_hypothesis_queue.jsonl`
- `data/reports/hypothesis_generator/btc_edge_raw_20260213/summary.md`
- `data/reports/hypothesis_generator/btc_edge_raw_20260213/template_hypotheses.json`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_controls.csv`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_events.csv`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_hazards.csv`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_interpretation.md`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_matched_deltas.csv`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_phase_stability.csv`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_sign_stability.csv`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_summary.json`
- `data/reports/liquidity_absence_window/btc_edge_raw_20260213/liquidity_absence_window_summary.md`
- `data/reports/liquidity_refill_lag_window/btc_edge_raw_20260213/liquidity_refill_lag_window_controls.csv`
- `data/reports/liquidity_refill_lag_window/btc_edge_raw_20260213/liquidity_refill_lag_window_events.csv`
- `data/reports/liquidity_refill_lag_window/btc_edge_raw_20260213/liquidity_refill_lag_window_hazards.csv`
- `data/reports/liquidity_refill_lag_window/btc_edge_raw_20260213/liquidity_refill_lag_window_matched_deltas.csv`
- `data/reports/liquidity_refill_lag_window/btc_edge_raw_20260213/liquidity_refill_lag_window_phase_stability.csv`
- `data/reports/liquidity_refill_lag_window/btc_edge_raw_20260213/liquidity_refill_lag_window_sign_stability.csv`
- `data/reports/liquidity_refill_lag_window/btc_edge_raw_20260213/liquidity_refill_lag_window_summary.json`
- `data/reports/liquidity_refill_lag_window/btc_edge_raw_20260213/liquidity_refill_lag_window_summary.md`
- `data/reports/liquidity_vacuum/btc_edge_raw_20260213/liquidity_vacuum_calibration.csv`
- `data/reports/liquidity_vacuum/btc_edge_raw_20260213/liquidity_vacuum_controls.csv`
- `data/reports/liquidity_vacuum/btc_edge_raw_20260213/liquidity_vacuum_summary.json`
- `data/reports/phase2/btc_edge_raw_20260213/cross_venue_desync/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/cross_venue_desync/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/cross_venue_desync/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/cross_venue_desync/promoted_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/directional_exhaustion_after_forced_flow/promoted_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/funding_extreme_reversal_window/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/funding_extreme_reversal_window/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/funding_extreme_reversal_window/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/funding_extreme_reversal_window/promoted_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_absence_window/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_absence_window/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_absence_window/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_absence_window/promoted_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_refill_lag_window/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_refill_lag_window/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_refill_lag_window/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_refill_lag_window/promoted_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_vacuum/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_vacuum/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_vacuum/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/liquidity_vacuum/promoted_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/range_compression_breakout_window/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/range_compression_breakout_window/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/range_compression_breakout_window/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/range_compression_breakout_window/promoted_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/vol_aftershock_window/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/vol_aftershock_window/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/vol_aftershock_window/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/vol_aftershock_window/promoted_candidates.json`
- `data/reports/phase2/btc_edge_raw_20260213/vol_shock_relaxation/phase2_candidates.csv`
- `data/reports/phase2/btc_edge_raw_20260213/vol_shock_relaxation/phase2_manifests.json`
- `data/reports/phase2/btc_edge_raw_20260213/vol_shock_relaxation/phase2_summary.md`
- `data/reports/phase2/btc_edge_raw_20260213/vol_shock_relaxation/promoted_candidates.json`
- `data/reports/range_compression_breakout_window/btc_edge_raw_20260213/range_compression_breakout_window_controls.csv`
- `data/reports/range_compression_breakout_window/btc_edge_raw_20260213/range_compression_breakout_window_events.csv`
- `data/reports/range_compression_breakout_window/btc_edge_raw_20260213/range_compression_breakout_window_hazards.csv`
- `data/reports/range_compression_breakout_window/btc_edge_raw_20260213/range_compression_breakout_window_matched_deltas.csv`
- `data/reports/range_compression_breakout_window/btc_edge_raw_20260213/range_compression_breakout_window_phase_stability.csv`
- `data/reports/range_compression_breakout_window/btc_edge_raw_20260213/range_compression_breakout_window_sign_stability.csv`
- `data/reports/range_compression_breakout_window/btc_edge_raw_20260213/range_compression_breakout_window_summary.json`
- `data/reports/range_compression_breakout_window/btc_edge_raw_20260213/range_compression_breakout_window_summary.md`
- `data/reports/universe/btc_edge_raw_20260213/universe_membership.json`
- `data/reports/universe/btc_edge_raw_20260213/universe_membership.md`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_conditional_hazards.csv`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_controls.csv`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_events.csv`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_hazards.csv`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_matched_deltas.csv`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_phase_stability.csv`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_placebo_deltas.csv`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_re_risk_note.md`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_sign_stability.csv`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_summary.json`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_summary.md`
- `data/reports/vol_aftershock_window/btc_edge_raw_20260213/vol_aftershock_window_window_sensitivity.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_controls.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_events.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_hazard_auc.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_hazards.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_matched_deltas.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_phase_stability.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_sanity.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_sign_stability.csv`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_summary.json`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_summary.md`
- `data/reports/vol_shock_relaxation/btc_edge_raw_20260213/vol_shock_relaxation_thresholds.csv`

## Runtime Logs & Stage Metadata

- `data/runs/btc_edge_raw_20260213/analyze_liquidity_vacuum.log`
- `data/runs/btc_edge_raw_20260213/analyze_vol_shock_relaxation.log`
- `data/runs/btc_edge_raw_20260213/build_cleaned_15m.json`
- `data/runs/btc_edge_raw_20260213/build_cleaned_15m.log`
- `data/runs/btc_edge_raw_20260213/build_context_features.json`
- `data/runs/btc_edge_raw_20260213/build_context_features.log`
- `data/runs/btc_edge_raw_20260213/build_features_v1.json`
- `data/runs/btc_edge_raw_20260213/build_features_v1.log`
- `data/runs/btc_edge_raw_20260213/build_market_context.json`
- `data/runs/btc_edge_raw_20260213/build_market_context.log`
- `data/runs/btc_edge_raw_20260213/build_universe_snapshots.json`
- `data/runs/btc_edge_raw_20260213/build_universe_snapshots.log`
- `data/runs/btc_edge_raw_20260213/export_edge_candidates.json`
- `data/runs/btc_edge_raw_20260213/export_edge_candidates.log`
- `data/runs/btc_edge_raw_20260213/generate_hypothesis_queue.json`
- `data/runs/btc_edge_raw_20260213/generate_hypothesis_queue.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_cross_venue_desync.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_directional_exhaustion_after_forced_flow.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_funding_extreme_reversal_window.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_liquidity_absence_window.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_liquidity_refill_lag_window.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_liquidity_vacuum.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_range_compression_breakout_window.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_vol_aftershock_window.log`
- `data/runs/btc_edge_raw_20260213/phase2_conditional_hypotheses_vol_shock_relaxation.log`
