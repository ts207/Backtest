# Phase 2 Conditional Edge Hypothesis

Run ID: `btc_edge_raw_20260213`
Event type: `liquidity_absence_window`
Decision: **PROMOTE**
Phase 1 pass required: `True`
Phase 1 status: `unknown`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 5 (cap=20)
- Conditions evaluated: 5
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 40
- Simplicity gate pass: True
- Regime stability required splits: 2

## Top candidates
 condition       condition_desc                action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all          fail_reasons
session_eu enter hour in [8,15]       risk_throttle_0 risk_throttle          102             70                  11            21 matched_controls_excess           -0.220847             -0.270513              -0.172594               -0.004588                 -0.005744                  -0.003526                 -1.0                 True                True    -1,-1,-1,-1,-1                  True                     1                       1                   True                  0.004588               0.004588                    0.004588               0.004588          0.216259                  False                True               True                      -0.372477                -0.172621    -3.173445      0.001506             0.015064                      True               True      False     False gate_f_exposure_guard
session_eu enter hour in [8,15]       entry_gate_skip  entry_gating          102             70                  11            21 matched_controls_excess           -0.220847             -0.275235              -0.176452               -0.004588                 -0.005848                  -0.003436                 -1.0                 True                True    -1,-1,-1,-1,-1                  True                     1                       1                   True                  0.004588               0.004588                    0.004588               0.004588          0.216259                  False                True               True                      -0.372477                -0.172621    -3.173445      0.001506             0.015064                      True               True      False     False gate_f_exposure_guard
session_eu enter hour in [8,15] reenable_at_half_life        timing          102             70                  11            21 matched_controls_excess           -0.220847             -0.270017              -0.170129               -0.004588                 -0.005788                  -0.003533                 -1.0                 True                True    -1,-1,-1,-1,-1                  True                     1                       1                   True                  0.004588               0.004588                    0.004588               0.004588          0.216259                  False                True               True                      -0.372477                -0.172621    -3.173445      0.001506             0.015064                      True               True      False     False gate_f_exposure_guard

## Explicit failures
     condition                action  sample_size                                                                                   fail_reasons
           all             no_action          378 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all       entry_gate_skip          378                                                                          gate_f_exposure_guard
           all       risk_throttle_0          378                                                                          gate_f_exposure_guard
           all               delay_0          378 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all               delay_8          378                                                              gate_d_friction,gate_multiplicity
           all              delay_30          378                                                                              gate_multiplicity
           all reenable_at_half_life          378                                                                          gate_f_exposure_guard
symbol_BTCUSDT             no_action          378 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT       entry_gate_skip          378                                                            gate_c_regime,gate_f_exposure_guard
symbol_BTCUSDT     risk_throttle_0.5          378                                                                                  gate_c_regime

## Promoted
 condition       condition_desc            action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all fail_reasons
session_eu enter hour in [8,15] risk_throttle_0.5 risk_throttle          102             70                  11            21 matched_controls_excess           -0.110423             -0.133471              -0.085237               -0.002294                 -0.002898                  -0.001799                 -0.5                 True                True    -1,-1,-1,-1,-1                  True                     1                       1                   True                  0.004588               0.004588                    0.004588               0.002294          0.108129                   True                True               True                      -0.186238                -0.086310    -3.173445      0.001506             0.015064                      True               True       True      True             
       all           all events risk_throttle_0.5 risk_throttle          378            226                  76            76 matched_controls_excess           -0.030358             -0.038494              -0.022322               -0.002427                 -0.002769                  -0.002101                 -0.5                 True                True    -1,-1,-1,-1,-1                  True                     1                       1                   True                  0.004854               0.004854                    0.004854               0.002427          0.027931                   True                True               True                      -0.028290                -0.024101    -2.797286      0.005153             0.017178                      True               True       True      True             