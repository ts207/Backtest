# Vol Aftershock / Re-risk Window (Phase 1)

Run ID: `btc_edge_raw_20260213`
Primary window arg: [0, 96]
Sweep windows: [(0, 48), (0, 96), (24, 96), (48, 144)]

- Events: 588
- Controls: 588
- Actions generated: 0 (Phase 1 structure only)

## Window sensitivity
 window_x  window_y   n  p_secondary_shock  delta_realized_vol_mean  p_secondary_monotone_non_decreasing  delta_realized_vol_monotone_non_decreasing
        0        48 147           0.074830                 0.000500                                False                                       False
        0        96 147           0.156463                 0.000792                                False                                       False
       24        96 147           0.149660                 0.000807                                False                                       False
       48       144 147           0.136054                 0.000403                                False                                       False

## Matched deltas (head)
      parent_event_id  symbol  window_x  window_y  delta_secondary_shock  delta_realized_vol_mean_96  delta_range_expansion_96  year
vsr_v1_BTCUSDT_000001 BTCUSDT         0        48                    0.0                   -0.000444                 -0.000493  2020
vsr_v1_BTCUSDT_000001 BTCUSDT         0        96                    0.0                   -0.000372                 -0.010680  2020
vsr_v1_BTCUSDT_000001 BTCUSDT        24        96                    0.0                    0.000140                  0.011170  2020
vsr_v1_BTCUSDT_000001 BTCUSDT        48       144                    0.0                   -0.000001                 -0.012407  2020
vsr_v1_BTCUSDT_000002 BTCUSDT         0        48                    0.0                   -0.000693                  0.005954  2020
vsr_v1_BTCUSDT_000002 BTCUSDT         0        96                    1.0                    0.001605                  0.063893  2020
vsr_v1_BTCUSDT_000002 BTCUSDT        24        96                    1.0                    0.003149                  0.047627  2020
vsr_v1_BTCUSDT_000002 BTCUSDT        48       144                    1.0                    0.001784                  0.043058  2020
vsr_v1_BTCUSDT_000003 BTCUSDT         0        48                    0.0                   -0.000340                 -0.014616  2020
vsr_v1_BTCUSDT_000003 BTCUSDT         0        96                    0.0                    0.001199                  0.018703  2020
vsr_v1_BTCUSDT_000003 BTCUSDT        24        96                    0.0                    0.000710                  0.008173  2020
vsr_v1_BTCUSDT_000003 BTCUSDT        48       144                    0.0                   -0.000574                 -0.023209  2020

## Conditional hazards (head)
 age  at_risk  hits   hazard  window_x  window_y             slice_type slice_value
   1       37     0 0.000000         0        48 prior_shock_severity_q           0
   2       37     1 0.027027         0        48 prior_shock_severity_q           0
   3       36     0 0.000000         0        48 prior_shock_severity_q           0
   4       36     0 0.000000         0        48 prior_shock_severity_q           0
   5       36     0 0.000000         0        48 prior_shock_severity_q           0
   6       36     0 0.000000         0        48 prior_shock_severity_q           0
   7       36     0 0.000000         0        48 prior_shock_severity_q           0
   8       36     0 0.000000         0        48 prior_shock_severity_q           0
   9       36     0 0.000000         0        48 prior_shock_severity_q           0
  10       36     0 0.000000         0        48 prior_shock_severity_q           0
  11       36     0 0.000000         0        48 prior_shock_severity_q           0
  12       36     0 0.000000         0        48 prior_shock_severity_q           0
  13       36     0 0.000000         0        48 prior_shock_severity_q           0
  14       36     0 0.000000         0        48 prior_shock_severity_q           0
  15       36     0 0.000000         0        48 prior_shock_severity_q           0
  16       36     0 0.000000         0        48 prior_shock_severity_q           0
  17       36     0 0.000000         0        48 prior_shock_severity_q           0
  18       36     0 0.000000         0        48 prior_shock_severity_q           0
  19       36     0 0.000000         0        48 prior_shock_severity_q           0
  20       36     0 0.000000         0        48 prior_shock_severity_q           0

## Placebo deltas (head)
           event_type       parent_event_id  symbol  window_x  window_y                  enter_ts  start_idx  end_idx  year session  prior_shock_severity_q  parent_relax_duration  parent_time_to_relax  parent_rv_peak  rv_rank  secondary_shock_within  time_to_secondary_shock  realized_vol_mean  range_expansion source_parent_event_id  placebo_secondary_shock_within  placebo_realized_vol_mean  placebo_range_expansion  delta_secondary_vs_placebo  delta_realized_vol_vs_placebo
vol_aftershock_window vsr_v1_BTCUSDT_000001 BTCUSDT         0        48 2020-06-09 03:45:00+00:00        783      831  2020    Asia                       1                   15.0                  14.0        0.006840 0.232598                       0                      NaN           0.001040         0.013201  vsr_v1_BTCUSDT_000001                               0                   0.001845                 0.041831                           0                      -0.000806
vol_aftershock_window vsr_v1_BTCUSDT_000001 BTCUSDT         0        96 2020-06-09 03:45:00+00:00        783      879  2020    Asia                       1                   15.0                  14.0        0.006840 0.232598                       0                      NaN           0.001217         0.021727  vsr_v1_BTCUSDT_000001                               0                   0.001312                 0.014579                           0                      -0.000095
vol_aftershock_window vsr_v1_BTCUSDT_000001 BTCUSDT        24        96 2020-06-09 09:45:00+00:00        807      879  2020      EU                       1                   15.0                  14.0        0.006840 0.218631                       0                      NaN           0.001313         0.021752  vsr_v1_BTCUSDT_000001                               1                   0.002906                 0.060663                          -1                      -0.001593
vol_aftershock_window vsr_v1_BTCUSDT_000001 BTCUSDT        48       144 2020-06-09 15:45:00+00:00        831      927  2020      EU                       1                   15.0                  14.0        0.006840 0.198570                       0                      NaN           0.001187         0.015489  vsr_v1_BTCUSDT_000001                               1                   0.002831                 0.038741                          -1                      -0.001645
vol_aftershock_window vsr_v1_BTCUSDT_000002 BTCUSDT         0        48 2020-06-11 01:00:00+00:00        964     1012  2020    Asia                       0                   26.0                  25.0        0.005104 0.103523                       0                      NaN           0.001506         0.030694  vsr_v1_BTCUSDT_000002                               0                   0.000949                 0.012621                           0                       0.000558
vol_aftershock_window vsr_v1_BTCUSDT_000002 BTCUSDT         0        96 2020-06-11 01:00:00+00:00        964     1060  2020    Asia                       0                   26.0                  25.0        0.005104 0.103523                       1                     63.0           0.003573         0.092560  vsr_v1_BTCUSDT_000002                               0                   0.002116                 0.037898                           1                       0.001457
vol_aftershock_window vsr_v1_BTCUSDT_000002 BTCUSDT        24        96 2020-06-11 07:00:00+00:00        988     1060  2020    Asia                       0                   26.0                  25.0        0.005104 0.107455                       1                     39.0           0.004312         0.079618  vsr_v1_BTCUSDT_000002                               0                   0.001357                 0.025823                           1                       0.002955
vol_aftershock_window vsr_v1_BTCUSDT_000002 BTCUSDT        48       144 2020-06-11 13:00:00+00:00       1012     1108  2020      EU                       0                   26.0                  25.0        0.005104 0.103523                       1                     15.0           0.003702         0.071204  vsr_v1_BTCUSDT_000002                               0                   0.001452                 0.025601                           1                       0.002250
vol_aftershock_window vsr_v1_BTCUSDT_000003 BTCUSDT         0        48 2020-06-12 05:00:00+00:00       1076     1124  2020    Asia                       3                   50.0                  49.0        0.013817 0.146329                       0                      NaN           0.002072         0.022641  vsr_v1_BTCUSDT_000003                               1                   0.001938                 0.038503                          -1                       0.000134
vol_aftershock_window vsr_v1_BTCUSDT_000003 BTCUSDT         0        96 2020-06-12 05:00:00+00:00       1076     1172  2020    Asia                       3                   50.0                  49.0        0.013817 0.146329                       0                      NaN           0.001931         0.027849  vsr_v1_BTCUSDT_000003                               0                   0.001738                 0.024875                           0                       0.000193
vol_aftershock_window vsr_v1_BTCUSDT_000003 BTCUSDT        24        96 2020-06-12 11:00:00+00:00       1100     1172  2020      EU                       3                   50.0                  49.0        0.013817 0.242255                       0                      NaN           0.001936         0.027511  vsr_v1_BTCUSDT_000003                               0                   0.002309                 0.043546                           0                      -0.000373
vol_aftershock_window vsr_v1_BTCUSDT_000003 BTCUSDT        48       144 2020-06-12 17:00:00+00:00       1124     1220  2020      US                       3                   50.0                  49.0        0.013817 0.272735                       0                      NaN           0.001473         0.020814  vsr_v1_BTCUSDT_000003                               0                   0.001425                 0.033776                           0                       0.000048

## Re-risk note
Potentially defensible re-risk pocket: window [0, 48] shows the lowest observed secondary-shock rate (0.075) below placebo baseline (0.087); treat as Phase-2 candidate only after split stability checks.