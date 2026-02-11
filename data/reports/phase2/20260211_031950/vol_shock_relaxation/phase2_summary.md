# Phase 2 Conditional Edge Hypothesis

Run ID: `20260211_031950`
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
session_eu enter hour in [8,15] entry_gate_skip  entry_gating           47 matched_controls_excess           -0.464458             -0.503763              -0.421355               -0.011768                 -0.018171                  -0.006063              -1.0000                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
session_eu enter hour in [8,15] risk_throttle_0 risk_throttle           47 matched_controls_excess           -0.464458             -0.503739              -0.417710               -0.011768                 -0.018100                  -0.006196              -1.0000                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
session_eu enter hour in [8,15]        delay_30        timing           47 matched_controls_excess           -0.451968             -0.494038              -0.397050               -0.003678                 -0.005503                  -0.001993              -0.3125                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True                      

## Explicit failures
     condition          action  sample_size                                        fail_reasons
           all       no_action          155 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all entry_gate_skip          155                               gate_f_exposure_guard
           all risk_throttle_0          155                               gate_f_exposure_guard
           all         delay_0          155 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT       no_action           78 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT entry_gate_skip           78                               gate_f_exposure_guard
symbol_BTCUSDT risk_throttle_0           78                               gate_f_exposure_guard
symbol_BTCUSDT         delay_0           78 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_ETHUSDT       no_action           77 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_ETHUSDT entry_gate_skip           77                               gate_f_exposure_guard

## Promoted
 condition       condition_desc                action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all fail_reasons
session_eu enter hour in [8,15]              delay_30        timing           47 matched_controls_excess           -0.451968             -0.494038              -0.397050               -0.003678                 -0.005503                  -0.001993            -0.312500                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             
session_eu enter hour in [8,15] reenable_at_half_life        timing           47 matched_controls_excess           -0.443231             -0.487518              -0.391529               -0.001785                 -0.002719                  -0.000866            -0.130098                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             