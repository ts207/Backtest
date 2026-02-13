# Vol Shock -> Relaxation Verification

Run ID: `btc_edge_raw_20260213`
Decision: **FREEZE**

## 1) Sanity & Coverage
 symbol  event_count  median_time_to_relax  median_duration  min_duration  max_duration  cooldown_overlap_violations  non_degenerate_count  time_to_relax_in_bounds
BTCUSDT          147                  33.0             34.0            13           147                            0                  True                     True

## 2) Matched-baseline deltas + CIs
split_name split_value                   metric   n  delta_mean  delta_ci_low  delta_ci_high        status
       all         ALL        relaxed_within_96 147    0.000000      0.000000       0.000000 includes_zero
       all         ALL            auc_excess_rv 147    0.176627      0.152622       0.204410      positive
       all         ALL       rv_decay_half_life 147    9.906122      9.141361      10.744728      positive
       all         ALL secondary_shock_within_h 147    0.825850      0.772755       0.881633      positive
       all         ALL             range_pct_96 147    0.036319      0.028373       0.044443      positive
    symbol     BTCUSDT        relaxed_within_96 147    0.000000      0.000000       0.000000 includes_zero
    symbol     BTCUSDT            auc_excess_rv 147    0.176627      0.152622       0.204410      positive
    symbol     BTCUSDT       rv_decay_half_life 147    9.906122      9.141361      10.744728      positive
    symbol     BTCUSDT secondary_shock_within_h 147    0.825850      0.772755       0.881633      positive
    symbol     BTCUSDT             range_pct_96 147    0.036319      0.028373       0.044443      positive
 bull_bear        bear        relaxed_within_96  75    0.000000      0.000000       0.000000 includes_zero
 bull_bear        bear            auc_excess_rv  75    0.201671      0.165129       0.240519      positive
 bull_bear        bear       rv_decay_half_life  75    9.677333      8.503533      10.737267      positive
 bull_bear        bear secondary_shock_within_h  75    0.896000      0.837333       0.949333      positive
 bull_bear        bear             range_pct_96  75    0.044589      0.033465       0.058742      positive
 bull_bear        bull        relaxed_within_96  72    0.000000      0.000000       0.000000 includes_zero
 bull_bear        bull            auc_excess_rv  72    0.150540      0.122189       0.180819      positive
 bull_bear        bull       rv_decay_half_life  72   10.144444      8.988681      11.444653      positive
 bull_bear        bull secondary_shock_within_h  72    0.752778      0.661111       0.844444      positive
 bull_bear        bull             range_pct_96  72    0.027705      0.018466       0.037399      positive
vol_regime        high        relaxed_within_96  41    0.000000      0.000000       0.000000 includes_zero
vol_regime        high            auc_excess_rv  41    0.280055      0.231312       0.339109      positive
vol_regime        high       rv_decay_half_life  41   10.595122      9.004878      12.078293      positive
vol_regime        high secondary_shock_within_h  41    0.814634      0.692683       0.917073      positive
vol_regime        high             range_pct_96  41    0.058857      0.038810       0.080030      positive
vol_regime         low        relaxed_within_96 106    0.000000      0.000000       0.000000 includes_zero
vol_regime         low            auc_excess_rv 106    0.136622      0.113259       0.162331      positive
vol_regime         low       rv_decay_half_life 106    9.639623      8.696085      10.613396      positive
vol_regime         low secondary_shock_within_h 106    0.830189      0.762264       0.888679      positive
vol_regime         low             range_pct_96 106    0.027602      0.021024       0.034874      positive

## 3) Hazard AUC + Phase stability
 symbol         outcome  auc_hazard  auc_hazard_control  auc_hazard_delta
BTCUSDT range_expansion    2.880397            1.445349          1.435048
BTCUSDT secondary_shock    1.870420            0.037387          1.833033

     split status  spread_t_rv_peak  spread_t_half_life  spread_t_relax  fractional_stable
    symbol   pass               0.0                 0.0             0.0               True
 bull_bear   pass               1.0                 0.0             3.0               True
vol_regime   pass               1.0                 0.0             5.0               True

## 4) Year sign stability
                  metric  year_signs  sign_flip
       relaxed_within_96 0,0,0,0,0,0      False
           auc_excess_rv 1,1,1,1,1,1      False
      rv_decay_half_life 1,1,1,1,1,1      False
secondary_shock_within_h 1,1,1,1,1,1      False
            range_pct_96 1,1,1,1,1,1      False