# Phase 2 Conditional Edge Hypothesis

Run ID: `btc_edge_raw_20260213`
Event type: `liquidity_refill_lag_window`
Decision: **PROMOTE**
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
condition condition_desc          action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all          fail_reasons
      all     all events entry_gate_skip  entry_gating          211            126                  42            43 matched_controls_excess           -0.156217             -0.186749              -0.125858               -0.021903                 -0.024837                  -0.019151            -1.000000                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.021903               0.021903                    0.021903               0.021903          0.134314                  False                True               True                      -0.168895                -0.174577    -4.818481      0.000001             0.000002                      True               True      False     False gate_f_exposure_guard
      all     all events risk_throttle_0 risk_throttle          211            126                  42            43 matched_controls_excess           -0.156217             -0.188333              -0.126362               -0.021903                 -0.025074                  -0.018843            -1.000000                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.021903               0.021903                    0.021903               0.021903          0.134314                  False                True               True                      -0.168895                -0.174577    -4.818481      0.000001             0.000002                      True               True      False     False gate_f_exposure_guard
      all     all events         delay_8        timing          211            126                  42            43 matched_controls_excess           -0.156217             -0.185686              -0.128612               -0.001825                 -0.002091                  -0.001576            -0.083333                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.021903               0.021903                    0.021903               0.001825          0.154392                   True                True               True                      -0.168895                -0.174577    -4.818481      0.000001             0.000002                      True               True       True      True                      

## Explicit failures
     condition                action  sample_size                                                                                   fail_reasons
           all             no_action          211 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all       entry_gate_skip          211                                                                          gate_f_exposure_guard
           all       risk_throttle_0          211                                                                          gate_f_exposure_guard
           all               delay_0          211 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all reenable_at_half_life          211                                                                          gate_f_exposure_guard
symbol_BTCUSDT             no_action          211 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT       entry_gate_skip          211                                                            gate_c_regime,gate_f_exposure_guard
symbol_BTCUSDT     risk_throttle_0.5          211                                                                                  gate_c_regime
symbol_BTCUSDT       risk_throttle_0          211                                                            gate_c_regime,gate_f_exposure_guard
symbol_BTCUSDT               delay_0          211 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity

## Promoted
condition condition_desc   action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all fail_reasons
      all     all events  delay_8        timing          211            126                  42            43 matched_controls_excess           -0.156217             -0.185686              -0.128612               -0.001825                 -0.002091                  -0.001576            -0.083333                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.021903               0.021903                    0.021903               0.001825          0.154392                   True                True               True                      -0.168895                -0.174577    -4.818481      0.000001             0.000002                      True               True       True      True             
      all     all events delay_30        timing          211            126                  42            43 matched_controls_excess           -0.156217             -0.187814              -0.127869               -0.006845                 -0.007850                  -0.005932            -0.312500                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.021903               0.021903                    0.021903               0.006845          0.149372                   True                True               True                      -0.168895                -0.174577    -4.818481      0.000001             0.000002                      True               True       True      True             