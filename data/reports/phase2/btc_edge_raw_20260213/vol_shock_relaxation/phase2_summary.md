# Phase 2 Conditional Edge Hypothesis

Run ID: `btc_edge_raw_20260213`
Event type: `vol_shock_relaxation`
Decision: **PROMOTE**
Phase 1 pass required: `True`
Phase 1 status: `freeze`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 16 (cap=20)
- Conditions evaluated: 16
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 112
- Simplicity gate pass: True
- Regime stability required splits: 2

## Top candidates
     condition      condition_desc          action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all          fail_reasons
bull_bear_bear   bull_bear == bear risk_throttle_0 risk_throttle           75             42                  14            19 matched_controls_excess           -0.471705             -0.499503              -0.441431               -0.010576                 -0.017038                  -0.005928                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     2                       2                   True                  0.017388               0.010576                    0.013982               0.010576          0.461129                  False                True               True                      -0.467253                -0.469918   -18.177729  0.000000e+00             0.000000                      True               True      False     False gate_f_exposure_guard
bull_bear_bear   bull_bear == bear entry_gate_skip  entry_gating           75             42                  14            19 matched_controls_excess           -0.471705             -0.499549              -0.440299               -0.010576                 -0.017299                  -0.006111                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     2                       2                   True                  0.017388               0.010576                    0.013982               0.010576          0.461129                  False                True               True                      -0.467253                -0.469918   -18.177729  0.000000e+00             0.000000                      True               True      False     False gate_f_exposure_guard
  session_asia enter hour in [0,7] entry_gate_skip  entry_gating           42             24                   8            10 matched_controls_excess           -0.444931             -0.493545              -0.388753               -0.006556                 -0.010072                  -0.003448                 -1.0                 True                True -1,-1,-1,-1,-1,-1                  True                     3                       2                   True                  0.012064               0.006556                    0.009310               0.006556          0.438375                  False                True               True                      -0.508943                -0.367866    -4.902222  9.475856e-07             0.000002                      True               True      False     False gate_f_exposure_guard

## Explicit failures
     condition          action  sample_size                                                                                   fail_reasons
           all       no_action          147 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all entry_gate_skip          147                                                                          gate_f_exposure_guard
           all risk_throttle_0          147                                                                          gate_f_exposure_guard
           all         delay_0          147 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT       no_action          147 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT entry_gate_skip          147                                                                          gate_f_exposure_guard
symbol_BTCUSDT risk_throttle_0          147                                                                          gate_f_exposure_guard
symbol_BTCUSDT         delay_0          147 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
bull_bear_bear       no_action           75 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
bull_bear_bear entry_gate_skip           75                                                                          gate_f_exposure_guard

## Promoted
     condition     condition_desc                action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all fail_reasons
age_bucket_0_8 t_rv_peak in [0,8] reenable_at_half_life        timing           97             59                  16            22 matched_controls_excess           -0.441674             -0.474018              -0.403919               -0.000957                 -0.001890                  -0.000363            -0.121993                 True                True -1,-1,-1,-1,-1,-1                  True                     3                       2                   True                  0.013592               0.006404                    0.009998               0.000957          0.440718                   True                True               True                      -0.471658                -0.424483   -11.290604           0.0                  0.0                      True               True       True      True             
age_bucket_0_8 t_rv_peak in [0,8]              delay_30        timing           97             59                  16            22 matched_controls_excess           -0.441674             -0.473594              -0.403482               -0.002001                 -0.003599                  -0.000961            -0.312500                 True                True -1,-1,-1,-1,-1,-1                  True                     3                       2                   True                  0.013592               0.006404                    0.009998               0.002001          0.439673                   True                True               True                      -0.471658                -0.424483   -11.290604           0.0                  0.0                      True               True       True      True             