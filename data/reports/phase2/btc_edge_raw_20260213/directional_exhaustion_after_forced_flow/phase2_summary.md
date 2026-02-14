# Phase 2 Conditional Edge Hypothesis

Run ID: `btc_edge_raw_20260213`
Event type: `directional_exhaustion_after_forced_flow`
Decision: **FREEZE**
Phase 1 pass required: `True`
Phase 1 status: `unknown`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 2 (cap=20)
- Conditions evaluated: 2
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 16
- Simplicity gate pass: True
- Regime stability required splits: 2

## Top candidates
condition condition_desc          action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all                            fail_reasons
      all     all events entry_gate_skip  entry_gating          105             63                  21            21 matched_controls_excess           -0.108119             -0.146872              -0.069817               -0.021771                 -0.025130                  -0.018676            -1.000000                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.021771               0.021771                    0.021771               0.021771          0.086349                  False                True               True                      -0.247825                -0.054217    -1.618913      0.105466             0.140621                      True              False      False     False gate_f_exposure_guard,gate_multiplicity
      all     all events risk_throttle_0 risk_throttle          105             63                  21            21 matched_controls_excess           -0.108119             -0.147289              -0.069450               -0.021771                 -0.025475                  -0.018650            -1.000000                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.021771               0.021771                    0.021771               0.021771          0.086349                  False                True               True                      -0.247825                -0.054217    -1.618913      0.105466             0.140621                      True              False      False     False gate_f_exposure_guard,gate_multiplicity
      all     all events         delay_8        timing          105             63                  21            21 matched_controls_excess           -0.108119             -0.147053              -0.069247               -0.001814                 -0.002083                  -0.001529            -0.083333                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.021771               0.021771                    0.021771               0.001814          0.106305                   True                True               True                      -0.247825                -0.054217    -1.618913      0.105466             0.140621                      True              False       True     False                       gate_multiplicity

## Explicit failures
     condition                action  sample_size                                                                                   fail_reasons
           all             no_action          105 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all       entry_gate_skip          105                                                        gate_f_exposure_guard,gate_multiplicity
           all     risk_throttle_0.5          105                                                                              gate_multiplicity
           all       risk_throttle_0          105                                                        gate_f_exposure_guard,gate_multiplicity
           all               delay_0          105 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all               delay_8          105                                                                              gate_multiplicity
           all              delay_30          105                                                                              gate_multiplicity
           all reenable_at_half_life          105                                                        gate_f_exposure_guard,gate_multiplicity
symbol_BTCUSDT             no_action          105 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT       entry_gate_skip          105                                          gate_c_regime,gate_f_exposure_guard,gate_multiplicity

## Promoted
None