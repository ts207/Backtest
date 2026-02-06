# Vol Shock -> Relaxation Verification

Run ID: `vsr_long`
Decision: **FREEZE**

## 1) Sanity & Coverage
 symbol  event_count  median_time_to_relax  median_duration  min_duration  max_duration  cooldown_overlap_violations  non_degenerate_count  time_to_relax_in_bounds
BTCUSDT           55                  30.0             31.0            15           147                            0                  True                     True
ETHUSDT           56                  29.0             30.0            15           144                            0                  True                     True

## 2) Matched-baseline deltas + CIs
split_name split_value                   metric   n  delta_mean  delta_ci_low  delta_ci_high        status
       all         ALL        relaxed_within_96 111    0.000000      0.000000       0.000000 includes_zero
       all         ALL            auc_excess_rv 111    0.253907      0.202548       0.308231      positive
       all         ALL       rv_decay_half_life 111   11.176577     10.327432      12.106351      positive
       all         ALL secondary_shock_within_h 111    0.771171      0.693649       0.846847      positive
       all         ALL             range_pct_96 111    0.047932      0.033876       0.064766      positive
    symbol     BTCUSDT        relaxed_within_96  55    0.000000      0.000000       0.000000 includes_zero
    symbol     BTCUSDT            auc_excess_rv  55    0.205967      0.157437       0.258805      positive
    symbol     BTCUSDT       rv_decay_half_life  55   11.567273     10.181545      12.916455      positive
    symbol     BTCUSDT secondary_shock_within_h  55    0.734545      0.621727       0.840000      positive
    symbol     BTCUSDT             range_pct_96  55    0.042407      0.025891       0.061994      positive
    symbol     ETHUSDT        relaxed_within_96  56    0.000000      0.000000       0.000000 includes_zero
    symbol     ETHUSDT            auc_excess_rv  56    0.300990      0.218193       0.394587      positive
    symbol     ETHUSDT       rv_decay_half_life  56   10.792857      9.692768      11.992946      positive
    symbol     ETHUSDT secondary_shock_within_h  56    0.807143      0.692857       0.907143      positive
    symbol     ETHUSDT             range_pct_96  56    0.053359      0.031032       0.081673      positive
 bull_bear        bear        relaxed_within_96  69    0.000000      0.000000       0.000000 includes_zero
 bull_bear        bear            auc_excess_rv  69    0.288854      0.224265       0.355687      positive
 bull_bear        bear       rv_decay_half_life  69   10.744928      9.594130      11.913478      positive
 bull_bear        bear secondary_shock_within_h  69    0.773913      0.678261       0.866667      positive
 bull_bear        bear             range_pct_96  69    0.058926      0.039936       0.084936      positive
 bull_bear        bull        relaxed_within_96  42    0.000000      0.000000       0.000000 includes_zero
 bull_bear        bull            auc_excess_rv  42    0.196493      0.126152       0.272943      positive
 bull_bear        bull       rv_decay_half_life  42   11.885714     10.561548      13.295833      positive
 bull_bear        bull secondary_shock_within_h  42    0.766667      0.652381       0.876190      positive
 bull_bear        bull             range_pct_96  42    0.029871      0.011402       0.048833      positive
vol_regime        high        relaxed_within_96  35    0.000000      0.000000       0.000000 includes_zero
vol_regime        high            auc_excess_rv  35    0.425847      0.312101       0.557911      positive
vol_regime        high       rv_decay_half_life  35   10.937143      9.274143      12.617286      positive
vol_regime        high secondary_shock_within_h  35    0.788571      0.640000       0.908714      positive
vol_regime        high             range_pct_96  35    0.072930      0.040678       0.114014      positive
vol_regime         low        relaxed_within_96  76    0.000000      0.000000       0.000000 includes_zero
vol_regime         low            auc_excess_rv  76    0.174723      0.138806       0.217910      positive
vol_regime         low       rv_decay_half_life  76   11.286842     10.312566      12.388158      positive
vol_regime         low secondary_shock_within_h  76    0.763158      0.663158       0.850066      positive
vol_regime         low             range_pct_96  76    0.036420      0.023332       0.050947      positive

## 3) Hazard AUC + Phase stability
 symbol         outcome  auc_hazard  auc_hazard_control  auc_hazard_delta
BTCUSDT range_expansion    3.862263            1.849245          2.013018
BTCUSDT secondary_shock    1.341277            0.029468          1.311809
ETHUSDT range_expansion    3.277381            3.543443         -0.266062
ETHUSDT secondary_shock    1.670340            0.025259          1.645081

     split status  spread_t_rv_peak  spread_t_half_life  spread_t_relax  fractional_stable
    symbol   pass               2.0                 0.0             1.0               True
 bull_bear   pass               0.5                 0.0             7.0               True
vol_regime   pass               0.0                 0.0             1.0               True

## 4) Year sign stability
                  metric year_signs  sign_flip
       relaxed_within_96        0,0      False
           auc_excess_rv        1,1      False
      rv_decay_half_life        1,1      False
secondary_shock_within_h        1,1      False
            range_pct_96        1,1      False