# Phase 2 Conditional Edge Hypothesis

Run ID: `btc_edge_raw_20260213`
Event type: `vol_aftershock_window`
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
     condition    condition_desc          action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all                        fail_reasons
           all        all events entry_gate_skip  entry_gating          588            352                 118           118 matched_controls_excess           -0.066962             -0.080369              -0.053939               -0.011002                 -0.011990                  -0.010012                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.011002               0.011002                    0.011002               0.011002          0.055961                  False                True               True                      -0.062821                -0.067386    -4.457712      0.000008             0.000022                      True               True      False     False               gate_f_exposure_guard
           all        all events risk_throttle_0 risk_throttle          588            352                 118           118 matched_controls_excess           -0.066962             -0.081538              -0.054551               -0.011002                 -0.011982                  -0.010069                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.011002               0.011002                    0.011002               0.011002          0.055961                  False                True               True                      -0.062821                -0.067386    -4.457712      0.000008             0.000022                      True               True      False     False               gate_f_exposure_guard
symbol_BTCUSDT symbol == BTCUSDT entry_gate_skip  entry_gating          588            352                 118           118 matched_controls_excess           -0.066962             -0.080735              -0.054253               -0.011002                 -0.012022                  -0.010025                 -1.0                 True                True -1,-1,-1,-1,-1,-1                 False                     0                       0                   True                  0.011002               0.011002                    0.011002               0.011002          0.055961                  False                True               True                      -0.062821                -0.067386    -4.457712      0.000008             0.000022                      True               True      False     False gate_c_regime,gate_f_exposure_guard

## Explicit failures
     condition            action  sample_size                                                                                   fail_reasons
           all         no_action          588 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all   entry_gate_skip          588                                                                          gate_f_exposure_guard
           all   risk_throttle_0          588                                                                          gate_f_exposure_guard
           all           delay_0          588 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all           delay_8          588                                                              gate_d_friction,gate_multiplicity
symbol_BTCUSDT         no_action          588 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT   entry_gate_skip          588                                                            gate_c_regime,gate_f_exposure_guard
symbol_BTCUSDT risk_throttle_0.5          588                                                                                  gate_c_regime
symbol_BTCUSDT   risk_throttle_0          588                                                            gate_c_regime,gate_f_exposure_guard
symbol_BTCUSDT           delay_0          588 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity

## Promoted
condition condition_desc                action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all fail_reasons
      all     all events reenable_at_half_life        timing          588            352                 118           118 matched_controls_excess           -0.039780             -0.051157              -0.029278               -0.004846                 -0.005431                  -0.004287            -0.436366                 True                True  -1,0,-1,-1,-1,-1                  True                     1                       1                   True                  0.011002               0.011002                    0.011002               0.004846          0.034934                   True                True               True                       -0.04277                -0.029450    -2.701645      0.006900             0.011047                      True               True       True      True             
      all     all events     risk_throttle_0.5 risk_throttle          588            352                 118           118 matched_controls_excess           -0.033481             -0.040601              -0.027083               -0.005501                 -0.006000                  -0.005047            -0.500000                 True                True -1,-1,-1,-1,-1,-1                  True                     1                       1                   True                  0.011002               0.011002                    0.011002               0.005501          0.027980                   True                True               True                       -0.03141                -0.033693    -4.457712      0.000008             0.000022                      True               True       True      True             