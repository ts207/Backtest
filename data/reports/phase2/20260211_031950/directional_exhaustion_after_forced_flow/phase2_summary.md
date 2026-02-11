# Phase 2 Conditional Edge Hypothesis

Run ID: `20260211_031950`
Event type: `directional_exhaustion_after_forced_flow`
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
symbol_ETHUSDT symbol == ETHUSDT         delay_8        timing          108 matched_controls_excess           -0.135713             -0.173084              -0.097063               -0.002091                 -0.002545                  -0.001695            -0.083333                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True                      
symbol_ETHUSDT symbol == ETHUSDT        delay_30        timing          108 matched_controls_excess           -0.135713             -0.178359              -0.098786               -0.007842                 -0.009411                  -0.006372            -0.312500                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True                      
symbol_ETHUSDT symbol == ETHUSDT risk_throttle_0 risk_throttle          108 matched_controls_excess           -0.135713             -0.177766              -0.096526               -0.025094                 -0.030397                  -0.020316            -1.000000                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard

## Explicit failures
     condition                action  sample_size                                        fail_reasons
           all             no_action          211 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all       entry_gate_skip          211                               gate_f_exposure_guard
           all       risk_throttle_0          211                               gate_f_exposure_guard
           all               delay_0          211 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all reenable_at_half_life          211                               gate_f_exposure_guard
symbol_BTCUSDT             no_action          103               gate_a_ci,gate_b_time,gate_d_friction
symbol_BTCUSDT       entry_gate_skip          103                               gate_f_exposure_guard
symbol_BTCUSDT       risk_throttle_0          103                               gate_f_exposure_guard
symbol_BTCUSDT               delay_0          103               gate_a_ci,gate_b_time,gate_d_friction
symbol_BTCUSDT reenable_at_half_life          103                               gate_f_exposure_guard

## Promoted
     condition    condition_desc   action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all fail_reasons
symbol_ETHUSDT symbol == ETHUSDT  delay_8        timing          108 matched_controls_excess           -0.135713             -0.173084              -0.097063               -0.002091                 -0.002545                  -0.001695            -0.083333                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             
symbol_ETHUSDT symbol == ETHUSDT delay_30        timing          108 matched_controls_excess           -0.135713             -0.178359              -0.098786               -0.007842                 -0.009411                  -0.006372            -0.312500                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             