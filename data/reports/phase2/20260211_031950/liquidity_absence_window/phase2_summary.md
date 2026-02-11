# Phase 2 Conditional Edge Hypothesis

Run ID: `20260211_031950`
Event type: `liquidity_absence_window`
Decision: **PROMOTE**
Phase 1 pass required: `True`
Phase 1 status: `unknown`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 6 (cap=20)
- Conditions evaluated: 6
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 48
- Simplicity gate pass: True

## Top candidates
 condition       condition_desc                action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all          fail_reasons
session_eu enter hour in [8,15]       risk_throttle_0 risk_throttle          213 matched_controls_excess           -0.123737             -0.153685              -0.095418               -0.005577                 -0.006429                  -0.004765                 -1.0                 True                True    -1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
session_eu enter hour in [8,15] reenable_at_half_life        timing          213 matched_controls_excess           -0.123737             -0.154990              -0.095269               -0.005577                 -0.006495                  -0.004729                 -1.0                 True                True    -1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
session_eu enter hour in [8,15]       entry_gate_skip  entry_gating          213 matched_controls_excess           -0.123737             -0.149890              -0.095834               -0.005577                 -0.006511                  -0.004763                 -1.0                 True                True    -1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard

## Explicit failures
     condition                action  sample_size                                        fail_reasons
           all             no_action          739 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all       entry_gate_skip          739                               gate_f_exposure_guard
           all       risk_throttle_0          739                               gate_f_exposure_guard
           all               delay_0          739 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all               delay_8          739                                     gate_d_friction
           all reenable_at_half_life          739                               gate_f_exposure_guard
symbol_BTCUSDT             no_action          372               gate_a_ci,gate_b_time,gate_d_friction
symbol_BTCUSDT       entry_gate_skip          372               gate_d_friction,gate_f_exposure_guard
symbol_BTCUSDT     risk_throttle_0.5          372                                     gate_d_friction
symbol_BTCUSDT       risk_throttle_0          372               gate_d_friction,gate_f_exposure_guard

## Promoted
 condition       condition_desc            action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all fail_reasons
session_eu enter hour in [8,15] risk_throttle_0.5 risk_throttle          213 matched_controls_excess           -0.061869             -0.076479              -0.048561               -0.002788                 -0.003256                  -0.002396              -0.5000                 True                True    -1,-1,-1,-1,-1                  True                   True                   True               True       True      True             
session_eu enter hour in [8,15]          delay_30        timing          213 matched_controls_excess           -0.051924             -0.071639              -0.033175               -0.001743                 -0.002008                  -0.001481              -0.3125                 True                True    -1,-1,-1,-1,-1                  True                   True                   True               True       True      True             