# Phase 2 Conditional Edge Hypothesis

Run ID: `vsr_long`
Event type: `vol_shock_relaxation`
Decision: **PROMOTE**
Phase 1 pass required: `True`
Phase 1 status: `freeze`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 17 (cap=20)
- Conditions evaluated: 17
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 120
- Simplicity gate pass: True

## Top candidates
 condition       condition_desc          action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all          fail_reasons
session_eu enter hour in [8,15] entry_gate_skip  entry_gating           41 matched_controls_excess           -0.459827             -0.508859              -0.396012               -0.018841                 -0.034056                  -0.006732              -1.0000                 True                True             -1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
session_eu enter hour in [8,15] risk_throttle_0 risk_throttle           41 matched_controls_excess           -0.459827             -0.506927              -0.399579               -0.018841                 -0.032098                  -0.006786              -1.0000                 True                True             -1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
session_eu enter hour in [8,15]        delay_30        timing           41 matched_controls_excess           -0.457844             -0.507515              -0.400241               -0.005888                 -0.010936                  -0.002281              -0.3125                 True                True             -1,-1                  True                   True                   True               True       True      True                      

## Explicit failures
     condition          action  sample_size                                        fail_reasons
           all       no_action          111 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all entry_gate_skip          111                               gate_f_exposure_guard
           all risk_throttle_0          111                               gate_f_exposure_guard
           all         delay_0          111 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT       no_action           55 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT entry_gate_skip           55                               gate_f_exposure_guard
symbol_BTCUSDT risk_throttle_0           55                               gate_f_exposure_guard
symbol_BTCUSDT         delay_0           55 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_ETHUSDT       no_action           56 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_ETHUSDT entry_gate_skip           56                               gate_f_exposure_guard

## Promoted
          condition                    condition_desc   action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all fail_reasons
         session_eu              enter hour in [8,15] delay_30        timing           41 matched_controls_excess           -0.457844             -0.507515              -0.400241               -0.005888                 -0.010936                  -0.002281              -0.3125                 True                True             -1,-1                  True                   True                   True               True       True      True             
fractional_age_0_33 t_rv_peak / duration_bars <= 0.33 delay_30        timing           80 matched_controls_excess           -0.439123             -0.479882              -0.394266               -0.004968                 -0.008077                  -0.002413              -0.3125                 True                True             -1,-1                  True                   True                   True               True       True      True             