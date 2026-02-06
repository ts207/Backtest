# Vol Shock -> Relaxation Verification

Run ID: `sanity_fp_report`
Decision: **FREEZE**

## 1) Sanity & Coverage
 symbol  event_count  median_time_to_relax  median_duration  min_duration  max_duration  cooldown_overlap_violations  non_degenerate_count  time_to_relax_in_bounds
BTCUSDT            1                  61.0             62.0            62            62                            0                 False                     True
ETHUSDT            1                  49.0             50.0            50            50                            0                 False                     True

## 2) Matched-baseline deltas + CIs
split_name split_value                   metric  n  delta_mean  delta_ci_low  delta_ci_high        status
       all         ALL        relaxed_within_96  2    0.000000      0.000000       0.000000 includes_zero
       all         ALL            auc_excess_rv  2    0.622598      0.537358       0.707837      positive
       all         ALL       rv_decay_half_life  2   22.000000     20.000000      24.000000      positive
       all         ALL secondary_shock_within_h  2    1.000000      1.000000       1.000000      positive
       all         ALL             range_pct_96  2    0.040850     -0.026067       0.107767 includes_zero
    symbol     BTCUSDT        relaxed_within_96  1    0.000000      0.000000       0.000000 includes_zero
    symbol     BTCUSDT            auc_excess_rv  1    0.707837      0.707837       0.707837      positive
    symbol     BTCUSDT       rv_decay_half_life  1   24.000000     24.000000      24.000000      positive
    symbol     BTCUSDT secondary_shock_within_h  1    1.000000      1.000000       1.000000      positive
    symbol     BTCUSDT             range_pct_96  1    0.107767      0.107767       0.107767      positive
    symbol     ETHUSDT        relaxed_within_96  1    0.000000      0.000000       0.000000 includes_zero
    symbol     ETHUSDT            auc_excess_rv  1    0.537358      0.537358       0.537358      positive
    symbol     ETHUSDT       rv_decay_half_life  1   20.000000     20.000000      20.000000      positive
    symbol     ETHUSDT secondary_shock_within_h  1    1.000000      1.000000       1.000000      positive
    symbol     ETHUSDT             range_pct_96  1   -0.026067     -0.026067      -0.026067      negative
 bull_bear        bear        relaxed_within_96  1    0.000000      0.000000       0.000000 includes_zero
 bull_bear        bear            auc_excess_rv  1    0.707837      0.707837       0.707837      positive
 bull_bear        bear       rv_decay_half_life  1   24.000000     24.000000      24.000000      positive
 bull_bear        bear secondary_shock_within_h  1    1.000000      1.000000       1.000000      positive
 bull_bear        bear             range_pct_96  1    0.107767      0.107767       0.107767      positive
 bull_bear        bull        relaxed_within_96  1    0.000000      0.000000       0.000000 includes_zero
 bull_bear        bull            auc_excess_rv  1    0.537358      0.537358       0.537358      positive
 bull_bear        bull       rv_decay_half_life  1   20.000000     20.000000      20.000000      positive
 bull_bear        bull secondary_shock_within_h  1    1.000000      1.000000       1.000000      positive
 bull_bear        bull             range_pct_96  1   -0.026067     -0.026067      -0.026067      negative
vol_regime         low        relaxed_within_96  2    0.000000      0.000000       0.000000 includes_zero
vol_regime         low            auc_excess_rv  2    0.622598      0.537358       0.707837      positive
vol_regime         low       rv_decay_half_life  2   22.000000     20.000000      24.000000      positive
vol_regime         low secondary_shock_within_h  2    1.000000      1.000000       1.000000      positive
vol_regime         low             range_pct_96  2    0.040850     -0.026067       0.107767 includes_zero

## 3) Hazard AUC + Phase stability
 symbol         outcome  auc_hazard  auc_hazard_control  auc_hazard_delta
BTCUSDT range_expansion         1.0            2.200000         -1.200000
BTCUSDT secondary_shock         1.0            0.000000          1.000000
ETHUSDT range_expansion         1.0            1.916667         -0.916667
ETHUSDT secondary_shock         1.0            0.000000          1.000000

     split status  spread_t_rv_peak  spread_t_half_life  spread_t_relax  fractional_stable
    symbol   pass               2.0                 4.0            12.0               True
 bull_bear   pass               2.0                 4.0            12.0               True
vol_regime   pass               0.0                 0.0             0.0               True

## 4) Year sign stability
                  metric year_signs  sign_flip
       relaxed_within_96          0      False
           auc_excess_rv          1      False
      rv_decay_half_life          1      False
secondary_shock_within_h          1      False
            range_pct_96          1      False