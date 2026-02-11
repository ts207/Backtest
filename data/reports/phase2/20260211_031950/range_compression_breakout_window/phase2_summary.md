# Phase 2 Conditional Edge Hypothesis

Run ID: `20260211_031950`
Event type: `range_compression_breakout_window`
Decision: **PROMOTE**
Phase 1 pass required: `True`
Phase 1 status: `unknown`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 11 (cap=20)
- Conditions evaluated: 11
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 88
- Simplicity gate pass: True

## Top candidates
     condition    condition_desc                action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all          fail_reasons
vol_regime_low vol_regime == low       risk_throttle_0 risk_throttle          732 matched_controls_excess           -0.130576             -0.146939              -0.115991                0.000998                  0.000052                   0.001880                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
vol_regime_low vol_regime == low reenable_at_half_life        timing          732 matched_controls_excess           -0.130576             -0.146956              -0.113889                0.000998                  0.000042                   0.001898                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard
vol_regime_low vol_regime == low       entry_gate_skip  entry_gating          732 matched_controls_excess           -0.130576             -0.145260              -0.116040                0.000998                  0.000065                   0.001964                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                   True                  False               True      False     False gate_f_exposure_guard

## Explicit failures
     condition                action  sample_size                                        fail_reasons
           all             no_action         1451 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
           all               delay_0         1451 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT             no_action          714 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_BTCUSDT               delay_0          714 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_ETHUSDT             no_action          737 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_ETHUSDT       entry_gate_skip          737                               gate_f_exposure_guard
symbol_ETHUSDT       risk_throttle_0          737                               gate_f_exposure_guard
symbol_ETHUSDT               delay_0          737 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction
symbol_ETHUSDT reenable_at_half_life          737                               gate_f_exposure_guard
bull_bear_bear             no_action          702 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction

## Promoted
 condition        condition_desc          action action_family  sample_size           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_d_friction_floor  gate_f_exposure_guard  gate_e_simplicity  gate_pass  gate_all fail_reasons
session_us enter hour in [16,23] entry_gate_skip  entry_gating          561 matched_controls_excess           -0.115793             -0.132330              -0.099020                0.000118                 -0.001283                   0.001356                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             
session_us enter hour in [16,23] risk_throttle_0 risk_throttle          561 matched_controls_excess           -0.115793             -0.132879              -0.100337                0.000118                 -0.001108                   0.001543                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                   True                   True               True       True      True             