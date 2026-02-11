# Phase 2 Conditional Edge Hypothesis

Run ID: `20260211_031950`
Event type: `liquidity_refill_lag_window`
Decision: **PROMOTE**
Phase 1 pass required: `True`
Phase 1 status: `unknown`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 3 (cap=20)
- Conditions evaluated: 3
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 24
- Simplicity gate pass: True

## Top candidates
     condition    condition_desc          action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all          fail_reasons
symbol_BTCUSDT symbol == BTCUSDT         delay_8        timing          210 matched_controls_excess           -0.115202             -0.141229              -0.088824               -0.001831                 -0.002108                  -0.001592            -0.083333                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True                      
symbol_BTCUSDT symbol == BTCUSDT        delay_30        timing          210 matched_controls_excess           -0.115202             -0.143473              -0.088809               -0.006868                 -0.007770                  -0.005918            -0.312500                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True                      
symbol_BTCUSDT symbol == BTCUSDT risk_throttle_0 risk_throttle          210 matched_controls_excess           -0.115202             -0.141132              -0.090121               -0.021976                 -0.025122                  -0.019158            -1.000000                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard

## Explicit failures
     condition                action  sample_size                                        fail_reasons
           all             no_action          387 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all       entry_gate_skip          387                               gate_f_exposure_guard
           all       risk_throttle_0          387                               gate_f_exposure_guard
           all               delay_0          387 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all reenable_at_half_life          387                               gate_f_exposure_guard
symbol_BTCUSDT             no_action          210               gate_a_ci,gate_b_time,gate_d_friction
symbol_BTCUSDT       entry_gate_skip          210                               gate_f_exposure_guard
symbol_BTCUSDT       risk_throttle_0          210                               gate_f_exposure_guard
symbol_BTCUSDT               delay_0          210               gate_a_ci,gate_b_time,gate_d_friction
symbol_BTCUSDT reenable_at_half_life          210                               gate_f_exposure_guard

## Promoted
     condition    condition_desc   action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all fail_reasons
symbol_BTCUSDT symbol == BTCUSDT  delay_8        timing          210 matched_controls_excess           -0.115202             -0.141229              -0.088824               -0.001831                 -0.002108                  -0.001592            -0.083333                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             
symbol_BTCUSDT symbol == BTCUSDT delay_30        timing          210 matched_controls_excess           -0.115202             -0.143473              -0.088809               -0.006868                 -0.007770                  -0.005918            -0.312500                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             