# Phase 2 Conditional Edge Hypothesis

Run ID: `btc_edge_raw_20260213`
Event type: `funding_extreme_reversal_window`
Decision: **FREEZE**
Phase 1 pass required: `True`
Phase 1 status: `unknown`
Phase 1 structure pass: `True`

## Counts
- Conditions generated: 10 (cap=20)
- Conditions evaluated: 10
- Actions generated: 8 (cap=9)
- Actions evaluated: 8
- Candidate rows evaluated: 40
- Simplicity gate pass: True
- Regime stability required splits: 2

## Top candidates
     condition    condition_desc                action action_family  sample_size  train_samples  validation_samples  test_samples           baseline_mode  delta_adverse_mean  delta_adverse_ci_low  delta_adverse_ci_high  delta_opportunity_mean  delta_opportunity_ci_low  delta_opportunity_ci_high  delta_exposure_mean  gate_a_ci_separated  gate_b_time_stable gate_b_year_signs  gate_c_regime_stable  gate_c_stable_splits  gate_c_required_splits  gate_d_friction_floor  opportunity_forward_mean  opportunity_tail_mean  opportunity_composite_mean  opportunity_cost_mean  net_benefit_mean  gate_f_exposure_guard  gate_g_net_benefit  gate_e_simplicity  validation_delta_adverse_mean  test_delta_adverse_mean  test_t_stat  test_p_value  test_p_value_adj_bh  gate_oos_validation_test  gate_multiplicity  gate_pass  gate_all                            fail_reasons
bull_bear_bull bull_bear == bull       risk_throttle_0 risk_throttle           54             33                  11            10 matched_controls_excess           -0.056231             -0.096223              -0.025577               -0.002204                 -0.007141                   0.002402                 -1.0                 True                True          -1,-1,-1                  True                     2                       2                   True                  0.014384               0.002204                    0.008294               0.002204          0.054027                  False                True               True                      -0.058611                -0.163196    -2.191909      0.028386             0.089394                      True              False      False     False gate_f_exposure_guard,gate_multiplicity
bull_bear_bull bull_bear == bull reenable_at_half_life        timing           54             33                  11            10 matched_controls_excess           -0.056231             -0.093910              -0.025029               -0.002204                 -0.007052                   0.002471                 -1.0                 True                True          -1,-1,-1                  True                     2                       2                   True                  0.014384               0.002204                    0.008294               0.002204          0.054027                  False                True               True                      -0.058611                -0.163196    -2.191909      0.028386             0.089394                      True              False      False     False gate_f_exposure_guard,gate_multiplicity
bull_bear_bull bull_bear == bull       entry_gate_skip  entry_gating           54             33                  11            10 matched_controls_excess           -0.056231             -0.092024              -0.026345               -0.002204                 -0.006763                   0.001987                 -1.0                 True                True          -1,-1,-1                  True                     2                       2                   True                  0.014384               0.002204                    0.008294               0.002204          0.054027                  False                True               True                      -0.058611                -0.163196    -2.191909      0.028386             0.089394                      True              False      False     False gate_f_exposure_guard,gate_multiplicity

## Explicit failures
     condition                action  sample_size                                                                                   fail_reasons
           all             no_action           56 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all       entry_gate_skip           56                                                        gate_f_exposure_guard,gate_multiplicity
           all     risk_throttle_0.5           56                                                                              gate_multiplicity
           all       risk_throttle_0           56                                                        gate_f_exposure_guard,gate_multiplicity
           all               delay_0           56 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
           all               delay_8           56                                                                              gate_multiplicity
           all              delay_30           56                                                                              gate_multiplicity
           all reenable_at_half_life           56                                                        gate_f_exposure_guard,gate_multiplicity
symbol_BTCUSDT             no_action           56 gate_a_ci,gate_b_time,gate_c_regime,gate_d_friction,gate_oos_validation_test,gate_multiplicity
symbol_BTCUSDT       entry_gate_skip           56                                                        gate_f_exposure_guard,gate_multiplicity

## Promoted
None