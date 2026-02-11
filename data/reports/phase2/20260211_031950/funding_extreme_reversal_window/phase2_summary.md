# Phase 2 Conditional Edge Hypothesis

Run ID: `20260211_031950`
Event type: `funding_extreme_reversal_window`
Decision: **PROMOTE**
Phase 1 pass required: `True`
Phase 1 status: `unknown`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 11 (cap=20)
- Conditions evaluated: 11
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 64
- Simplicity gate pass: True

## Top candidates
     condition    condition_desc                action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all          fail_reasons
symbol_BTCUSDT symbol == BTCUSDT       risk_throttle_0 risk_throttle           55 matched_controls_excess           -0.070939             -0.113900              -0.034747               -0.000491                 -0.005525                   0.004225                 -1.0                 True                True        -1,-1,0,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
symbol_BTCUSDT symbol == BTCUSDT reenable_at_half_life        timing           55 matched_controls_excess           -0.070939             -0.114184              -0.034721               -0.000491                 -0.005545                   0.003858                 -1.0                 True                True        -1,-1,0,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
symbol_BTCUSDT symbol == BTCUSDT       entry_gate_skip  entry_gating           55 matched_controls_excess           -0.070939             -0.114180              -0.032571               -0.000491                 -0.005774                   0.004046                 -1.0                 True                True        -1,-1,0,-1                  True                   True                  False               True      False     False gate_f_exposure_guard

## Explicit failures
     condition                action  sample_size                                        fail_reasons
           all             no_action          112 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all       entry_gate_skip          112                               gate_f_exposure_guard
           all               delay_0          112 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT             no_action           55 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT       entry_gate_skip           55                               gate_f_exposure_guard
symbol_BTCUSDT       risk_throttle_0           55                               gate_f_exposure_guard
symbol_BTCUSDT               delay_0           55 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT reenable_at_half_life           55                               gate_f_exposure_guard
symbol_ETHUSDT             no_action           57 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_ETHUSDT       entry_gate_skip           57                               gate_f_exposure_guard

## Promoted
condition condition_desc                action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all fail_reasons
      all     all events       risk_throttle_0 risk_throttle          112 matched_controls_excess           -0.058364             -0.086896              -0.033031               -0.001555                 -0.004812                   0.001582                 -1.0                 True                True       -1,-1,-1,-1                  True                   True                   True               True       True      True             
      all     all events reenable_at_half_life        timing          112 matched_controls_excess           -0.058364             -0.086742              -0.035049               -0.001555                 -0.004657                   0.001750                 -1.0                 True                True       -1,-1,-1,-1                  True                   True                   True               True       True      True             