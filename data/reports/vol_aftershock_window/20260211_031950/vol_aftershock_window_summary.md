# Vol Aftershock / Re-risk Window (Phase 1)

Run ID: `20260211_031950`
Primary window arg: [0, 96]
Sweep windows: [(0, 48), (0, 96), (24, 96), (48, 144)]

- Events: 1176
- Controls: 1176
- Actions generated: 0 (Phase 1 structure only)

## Window sensitivity
 window_x  window_y   n  p_secondary_shock  delta_realized_vol_mean  p_secondary_monotone_non_decreasing  delta_realized_vol_monotone_non_decreasing
        0        48 294           0.074830                 0.000631                                False                                       False
        0        96 294           0.142857                 0.000785                                False                                       False
       24        96 294           0.136054                 0.000719                                False                                       False
       48       144 294           0.119048                 0.000451                                False                                       False

## Matched deltas (head)
      parent_event_id  symbol  window_x  window_y  delta_secondary_shock  delta_realized_vol_mean_96  delta_range_expansion_96  year
vsr_v1_BTCUSDT_000001 BTCUSDT         0        48                    0.0                   -0.000403                 -0.003959  2020
vsr_v1_BTCUSDT_000001 BTCUSDT         0        96                    0.0                   -0.000676                 -0.019770  2020
vsr_v1_BTCUSDT_000001 BTCUSDT        24        96                    0.0                   -0.000835                 -0.019444  2020
vsr_v1_BTCUSDT_000001 BTCUSDT        48       144                    0.0                    0.000530                  0.007234  2020
vsr_v1_BTCUSDT_000002 BTCUSDT         0        48                    0.0                    0.000367                  0.005545  2020
vsr_v1_BTCUSDT_000002 BTCUSDT         0        96                    1.0                    0.002196                  0.059220  2020
vsr_v1_BTCUSDT_000002 BTCUSDT        24        96                    1.0                    0.003804                  0.067243  2020
vsr_v1_BTCUSDT_000002 BTCUSDT        48       144                    0.0                    0.001634                  0.037217  2020
vsr_v1_BTCUSDT_000003 BTCUSDT         0        48                    0.0                    0.000650                  0.008845  2020
vsr_v1_BTCUSDT_000003 BTCUSDT         0        96                    0.0                    0.000027                  0.003414  2020
vsr_v1_BTCUSDT_000003 BTCUSDT        24        96                    0.0                    0.000673                  0.010123  2020
vsr_v1_BTCUSDT_000003 BTCUSDT        48       144                    0.0                    0.000255                 -0.001698  2020

## Conditional hazards (head)
 age  at_risk  hits   hazard  window_x  window_y             slice_type slice_value
   1       74     0 0.000000         0        48 prior_shock_severity_q           0
   2       74     1 0.013514         0        48 prior_shock_severity_q           0
   3       73     0 0.000000         0        48 prior_shock_severity_q           0
   4       73     0 0.000000         0        48 prior_shock_severity_q           0
   5       73     0 0.000000         0        48 prior_shock_severity_q           0
   6       73     0 0.000000         0        48 prior_shock_severity_q           0
   7       73     0 0.000000         0        48 prior_shock_severity_q           0
   8       73     0 0.000000         0        48 prior_shock_severity_q           0
   9       73     0 0.000000         0        48 prior_shock_severity_q           0
  10       73     0 0.000000         0        48 prior_shock_severity_q           0
  11       73     0 0.000000         0        48 prior_shock_severity_q           0
  12       73     0 0.000000         0        48 prior_shock_severity_q           0
  13       73     0 0.000000         0        48 prior_shock_severity_q           0
  14       73     0 0.000000         0        48 prior_shock_severity_q           0
  15       73     0 0.000000         0        48 prior_shock_severity_q           0
  16       73     0 0.000000         0        48 prior_shock_severity_q           0
  17       73     0 0.000000         0        48 prior_shock_severity_q           0
  18       73     0 0.000000         0        48 prior_shock_severity_q           0
  19       73     0 0.000000         0        48 prior_shock_severity_q           0
  20       73     0 0.000000         0        48 prior_shock_severity_q           0

## Placebo deltas (head)
           event_type       parent_event_id  symbol  window_x  window_y                  enter_ts  start_idx  end_idx  year session  prior_shock_severity_q  parent_relax_duration  parent_time_to_relax  parent_rv_peak  rv_rank  secondary_shock_within  time_to_secondary_shock  realized_vol_mean  range_expansion source_parent_event_id  placebo_secondary_shock_within  placebo_realized_vol_mean  placebo_range_expansion  delta_secondary_vs_placebo  delta_realized_vol_vs_placebo
vol_aftershock_window vsr_v1_BTCUSDT_000001 BTCUSDT         0        48 2020-06-09 03:45:00+00:00        783      831  2020    Asia                       1                   15.0                  14.0        0.006840 0.230811                       0                      NaN           0.001040         0.013201  vsr_v1_BTCUSDT_000001                               0                   0.000931                 0.012040                           0                       0.000108
vol_aftershock_window vsr_v1_BTCUSDT_000001 BTCUSDT         0        96 2020-06-09 03:45:00+00:00        783      879  2020    Asia                       1                   15.0                  14.0        0.006840 0.230811                       0                      NaN           0.001217         0.021727  vsr_v1_BTCUSDT_000001                               0                   0.002394                 0.031026                           0                      -0.001177
vol_aftershock_window vsr_v1_BTCUSDT_000001 BTCUSDT        24        96 2020-06-09 09:45:00+00:00        807      879  2020      EU                       1                   15.0                  14.0        0.006840 0.216831                       0                      NaN           0.001313         0.021752  vsr_v1_BTCUSDT_000001                               0                   0.001789                 0.022727                           0                      -0.000476
vol_aftershock_window vsr_v1_BTCUSDT_000001 BTCUSDT        48       144 2020-06-09 15:45:00+00:00        831      927  2020      EU                       1                   15.0                  14.0        0.006840 0.197263                       0                      NaN           0.001187         0.015489  vsr_v1_BTCUSDT_000001                               0                   0.001933                 0.035870                           0                      -0.000746
vol_aftershock_window vsr_v1_BTCUSDT_000002 BTCUSDT         0        48 2020-06-11 01:00:00+00:00        964     1012  2020    Asia                       0                   26.0                  25.0        0.005104 0.104426                       0                      NaN           0.001506         0.030694  vsr_v1_BTCUSDT_000002                               0                   0.000929                 0.018055                           0                       0.000577
vol_aftershock_window vsr_v1_BTCUSDT_000002 BTCUSDT         0        96 2020-06-11 01:00:00+00:00        964     1060  2020    Asia                       0                   26.0                  25.0        0.005104 0.104426                       1                     63.0           0.003573         0.092560  vsr_v1_BTCUSDT_000002                               0                   0.000831                 0.011675                           1                       0.002742
vol_aftershock_window vsr_v1_BTCUSDT_000002 BTCUSDT        24        96 2020-06-11 07:00:00+00:00        988     1060  2020    Asia                       0                   26.0                  25.0        0.005104 0.108413                       1                     39.0           0.004312         0.079618  vsr_v1_BTCUSDT_000002                               0                   0.000834                 0.007537                           1                       0.003478
vol_aftershock_window vsr_v1_BTCUSDT_000002 BTCUSDT        48       144 2020-06-11 13:00:00+00:00       1012     1108  2020      EU                       0                   26.0                  25.0        0.005104 0.104426                       1                     15.0           0.003702         0.071204  vsr_v1_BTCUSDT_000002                               0                   0.002247                      NaN                           1                       0.001455
vol_aftershock_window vsr_v1_BTCUSDT_000003 BTCUSDT         0        48 2020-06-12 05:00:00+00:00       1076     1124  2020    Asia                       3                   50.0                  49.0        0.013817 0.146543                       0                      NaN           0.002072         0.022641  vsr_v1_BTCUSDT_000003                               0                   0.002440                 0.047578                           0                      -0.000368
vol_aftershock_window vsr_v1_BTCUSDT_000003 BTCUSDT         0        96 2020-06-12 05:00:00+00:00       1076     1172  2020    Asia                       3                   50.0                  49.0        0.013817 0.146543                       0                      NaN           0.001931         0.027849  vsr_v1_BTCUSDT_000003                               0                   0.002227                 0.057636                           0                      -0.000296
vol_aftershock_window vsr_v1_BTCUSDT_000003 BTCUSDT        24        96 2020-06-12 11:00:00+00:00       1100     1172  2020      EU                       3                   50.0                  49.0        0.013817 0.240468                       0                      NaN           0.001936         0.027511  vsr_v1_BTCUSDT_000003                               0                   0.000535                 0.004665                           0                       0.001400
vol_aftershock_window vsr_v1_BTCUSDT_000003 BTCUSDT        48       144 2020-06-12 17:00:00+00:00       1124     1220  2020      US                       3                   50.0                  49.0        0.013817 0.270889                       0                      NaN           0.001473         0.020814  vsr_v1_BTCUSDT_000003                               0                   0.002550                 0.050415                           0                      -0.001077

## Re-risk note
Potentially defensible re-risk pocket: window [0, 48] shows the lowest observed secondary-shock rate (0.075) below placebo baseline (0.104); treat as Phase-2 candidate only after split stability checks.