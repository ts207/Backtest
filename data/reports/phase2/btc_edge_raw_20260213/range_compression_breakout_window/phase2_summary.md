# Phase 2 Conditional Edge Hypothesis

Run ID: `btc_edge_raw_20260213`
Event type: `range_compression_breakout_window`
Decision: **PROMOTE**
Phase 1 pass required: `True`
Phase 1 status: `unknown`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 10 (cap=20)
- Conditions evaluated: 10
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 80
- Simplicity gate pass: True
- Regime stability required splits: 2

## Top candidates
     condition    condition_desc                action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all fail_reasons
vol_regime_mid vol_regime == mid reenable_at_half_life        timing          189            135                  21            33 matched_controls_excess           -0.124099             -0.156449              -0.093930               -0.000011                 -0.002017                   0.002104                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     2                       2                   True                  0.009674               0.000011                    0.004843               0.000011          0.124088                   True                True               True                       -0.12677                -0.128075    -3.280738      0.001035             0.002367                      True               True       True      True             
vol_regime_mid vol_regime == mid       risk_throttle_0 risk_throttle          189            135                  21            33 matched_controls_excess           -0.124099             -0.152844              -0.094316               -0.000011                 -0.002113                   0.002122                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     2                       2                   True                  0.009674               0.000011                    0.004843               0.000011          0.124088                   True                True               True                       -0.12677                -0.128075    -3.280738      0.001035             0.002367                      True               True       True      True             
vol_regime_mid vol_regime == mid       entry_gate_skip  entry_gating          189            135                  21            33 matched_controls_excess           -0.124099             -0.156519              -0.096567               -0.000011                 -0.002145                   0.002102                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     2                       2                   True                  0.009674               0.000011                    0.004843               0.000011          0.124088                   True                True               True                       -0.12677                -0.128075    -3.280738      0.001035             0.002367                      True               True       True      True             

## Explicit failures
     condition    action  sample_size                                                                                   fail_reasons
           all no_action          682 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all   delay_0          682 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all   delay_8          682                                                                              gate_multiplicity
symbol_BTCUSDT no_action          682 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT   delay_0          682 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT   delay_8          682                                                                              gate_multiplicity
bull_bear_bear no_action          345 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
bull_bear_bear   delay_0          345 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
bull_bear_bear   delay_8          345                                                     gate_oos_validation_test,gate_multiplicity
bull_bear_bull no_action          337 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity

## Promoted
     condition    condition_desc          action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all fail_reasons
vol_regime_mid vol_regime == mid entry_gate_skip  entry_gating          189            135                  21            33 matched_controls_excess           -0.124099             -0.156519              -0.096567               -0.000011                 -0.002145                   0.002102                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     2                       2                   True                  0.009674               0.000011                    0.004843               0.000011          0.124088                   True                True               True                       -0.12677                -0.128075    -3.280738      0.001035             0.002367                      True               True       True      True             
vol_regime_mid vol_regime == mid risk_throttle_0 risk_throttle          189            135                  21            33 matched_controls_excess           -0.124099             -0.152844              -0.094316               -0.000011                 -0.002113                   0.002122                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     2                       2                   True                  0.009674               0.000011                    0.004843               0.000011          0.124088                   True                True               True                       -0.12677                -0.128075    -3.280738      0.001035             0.002367                      True               True       True      True             