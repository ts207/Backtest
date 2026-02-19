# Concept Playbook: C_SESSION_MICROSTRUCTURE

## Definition
Market behavior and liquidity dynamics stratified by global trading sessions (Asia, Europe, US) and their overlaps. Crypto markets, while 24/7, exhibit structural periodicity driven by institutional hours and regional retail activity, leading to session-specific range characteristics and displacement effects.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_1m` | `high`, `low`, `close`, `volume` | 1m | 60 days |
| `tob_1m_agg` | `spread_bps_mean`, `bid_depth_usd_mean` | 1m | 30 days |

## Metrics
*   **`session_range_pct`**: The high-low range of a session (e.g., Asia) as a percentile of the last 60 sessions of the same type.
*   **`london_open_displacement_bps`**: The price move in the first 30 minutes of the London session.
*   **`session_liquidity_zscore`**: Standardized volume and depth relative to the same session hour over 30 days.
*   **`ny_continuation_score`**: Probability that the NY session continues the trend established in the London session.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_SESS_01` | Asia Compression | Low Asia range (< 30th percentile) must precede higher-than-average NY range expansion. |
| `T_SESS_02` | Periodicity Check | Volume and spread must show statistically significant (p < 0.01) differences between UTC 04:00 and UTC 14:00. |
| `T_SESS_03` | Open Displacement | London open displacement magnitude must be positively correlated with preceding Asia session volatility. |

## Artifacts
*   **`spec/features/session_features.yaml`**: Definitions for session ranges and transition flags.
*   **`project/features/context.py`**: Implementation of session hour and transition logic.
*   **`data/lake/reports/session_stats.parquet`**: Daily session-by-session performance and liquidity metrics.
