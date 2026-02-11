# Phase 2 Conditional Edge Hypothesis

Run ID: `20260211_031950`
Event type: `vol_aftershock_window`
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
symbol_BTCUSDT symbol == BTCUSDT risk_throttle_0 risk_throttle          580 matched_controls_excess           -0.066253             -0.079604              -0.053881               -0.011070                 -0.012038                  -0.010183                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
symbol_BTCUSDT symbol == BTCUSDT entry_gate_skip  entry_gating          580 matched_controls_excess           -0.066253             -0.079630              -0.053901               -0.011070                 -0.012043                  -0.010143                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
           all        all events entry_gate_skip  entry_gating         1176 matched_controls_excess           -0.058372             -0.067353              -0.049503               -0.012081                 -0.012875                  -0.011312                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard

## Explicit failures
     condition          action  sample_size                                        fail_reasons
           all       no_action         1176 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all entry_gate_skip         1176                               gate_f_exposure_guard
           all risk_throttle_0         1176                               gate_f_exposure_guard
           all         delay_0         1176 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all         delay_8         1176                                     gate_d_friction
symbol_BTCUSDT       no_action          580               gate_a_ci,gate_b_time,gate_d_friction
symbol_BTCUSDT entry_gate_skip          580                               gate_f_exposure_guard
symbol_BTCUSDT risk_throttle_0          580                               gate_f_exposure_guard
symbol_BTCUSDT         delay_0          580               gate_a_ci,gate_b_time,gate_d_friction
symbol_ETHUSDT       no_action          596               gate_a_ci,gate_b_time,gate_d_friction

## Promoted
     condition    condition_desc                action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all fail_reasons
symbol_BTCUSDT symbol == BTCUSDT reenable_at_half_life        timing          580 matched_controls_excess           -0.039187             -0.049848              -0.028813               -0.004861                 -0.005412                  -0.004311            -0.434698                 True                True  -1,0,-1,-1,-1,-1                  True                   True                   True               True       True      True             
symbol_BTCUSDT symbol == BTCUSDT     risk_throttle_0.5 risk_throttle          580 matched_controls_excess           -0.033127             -0.039518              -0.026716               -0.005535                 -0.006035                  -0.005052            -0.500000                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             