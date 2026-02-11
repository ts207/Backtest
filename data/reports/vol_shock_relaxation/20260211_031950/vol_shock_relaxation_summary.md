# Vol Shock -> Relaxation Verification

Run ID: `20260211_031950`
Decision: **FREEZE**

## 1) Sanity & Coverage
 symbol  event_count  median_time_to_relax  median_duration  min_duration  max_duration  cooldown_overlap_violations  non_degenerate_count  time_to_relax_in_bounds
BTCUSDT           78                  38.5             39.5            15           147                            0                  True                     True
ETHUSDT           77                  34.0             35.0            12           144                            0                  True                     True

## 2) Matched-baseline deltas + CIs
split_name split_value                   metric   n  delta_mean  delta_ci_low  delta_ci_high        status
       all         ALL        relaxed_within_96 155    0.000000      0.000000       0.000000 includes_zero
       all         ALL            auc_excess_rv 155    0.241294      0.206582       0.276973      positive
       all         ALL       rv_decay_half_life 155   10.061935      9.385774      10.766548      positive
       all         ALL secondary_shock_within_h 155    0.781935      0.713548       0.841355      positive
       all         ALL             range_pct_96 155    0.049339      0.040489       0.059830      positive
    symbol     BTCUSDT        relaxed_within_96  78    0.000000      0.000000       0.000000 includes_zero
    symbol     BTCUSDT            auc_excess_rv  78    0.228164      0.190850       0.267713      positive
    symbol     BTCUSDT       rv_decay_half_life  78   10.430769      9.437885      11.477051      positive
    symbol     BTCUSDT secondary_shock_within_h  78    0.782051      0.692308       0.866731      positive
    symbol     BTCUSDT             range_pct_96  78    0.046807      0.035788       0.058129      positive
    symbol     ETHUSDT        relaxed_within_96  77    0.000000      0.000000       0.000000 includes_zero
    symbol     ETHUSDT            auc_excess_rv  77    0.254594      0.197053       0.317555      positive
    symbol     ETHUSDT       rv_decay_half_life  77    9.688312      8.594481      10.641688      positive
    symbol     ETHUSDT secondary_shock_within_h  77    0.781818      0.683117       0.870195      positive
    symbol     ETHUSDT             range_pct_96  77    0.051904      0.037577       0.067308      positive
 bull_bear        bear        relaxed_within_96  97    0.000000      0.000000       0.000000 includes_zero
 bull_bear        bear            auc_excess_rv  97    0.255355      0.209458       0.304820      positive
 bull_bear        bear       rv_decay_half_life  97    9.855670      8.921340      10.815309      positive
 bull_bear        bear secondary_shock_within_h  97    0.777320      0.692732       0.861856      positive
 bull_bear        bear             range_pct_96  97    0.051523      0.039190       0.064827      positive
 bull_bear        bull        relaxed_within_96  58    0.000000      0.000000       0.000000 includes_zero
 bull_bear        bull            auc_excess_rv  58    0.217777      0.167530       0.277356      positive
 bull_bear        bull       rv_decay_half_life  58   10.406897      9.255000      11.572586      positive
 bull_bear        bull secondary_shock_within_h  58    0.789655      0.679224       0.889655      positive
 bull_bear        bull             range_pct_96  58    0.045688      0.031224       0.061846      positive
vol_regime        high        relaxed_within_96  43    0.000000      0.000000       0.000000 includes_zero
vol_regime        high            auc_excess_rv  43    0.372533      0.297915       0.459255      positive
vol_regime        high       rv_decay_half_life  43   10.688372      9.436977      11.954070      positive
vol_regime        high secondary_shock_within_h  43    0.786047      0.665116       0.902442      positive
vol_regime        high             range_pct_96  43    0.077799      0.054977       0.101788      positive
vol_regime         low        relaxed_within_96 112    0.000000      0.000000       0.000000 includes_zero
vol_regime         low            auc_excess_rv 112    0.190907      0.160165       0.226875      positive
vol_regime         low       rv_decay_half_life 112    9.821429      8.897991      10.700134      positive
vol_regime         low secondary_shock_within_h 112    0.780357      0.707143       0.855357      positive
vol_regime         low             range_pct_96 112    0.038413      0.029499       0.048163      positive

## 3) Hazard AUC + Phase stability
 symbol         outcome  auc_hazard  auc_hazard_control  auc_hazard_delta
BTCUSDT range_expansion    3.059005            1.451621          1.607384
BTCUSDT secondary_shock    1.447928            0.012887          1.435041
ETHUSDT range_expansion    2.343687            2.305379          0.038308
ETHUSDT secondary_shock    1.511397            0.023623          1.487774

     split status  spread_t_rv_peak  spread_t_half_life  spread_t_relax  fractional_stable
    symbol   pass               1.0                 0.0             4.5               True
 bull_bear   pass               1.0                 0.0             6.0               True
vol_regime   pass               0.0                 0.0             2.0               True

## 4) Year sign stability
                  metric  year_signs  sign_flip
       relaxed_within_96 0,0,0,0,0,0      False
           auc_excess_rv 1,1,1,1,1,1      False
      rv_decay_half_life 1,1,1,1,1,1      False
secondary_shock_within_h 1,1,1,1,1,1      False
            range_pct_96 1,1,1,1,1,1      False