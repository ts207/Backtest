# Concept Playbook: C_VOLATILITY_STATE_TRANSITIONS

## Definition
The cyclical alternation between market states: `compression → expansion → exhaustion → reset`. This concept posits that volatility is mean-reverting but highly clustered, and that extreme compression (low volatility) is a leading indicator of an impending expansion (breakout).

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_5m` | `high`, `low`, `close` | 5m | 90 days |
| `features_v1` | `rv_96`, `rv_pct_17280` | 5m | 90 days |

## Metrics
*   **`realized_vol_bps`**: Standard deviation of log returns over a window, expressed in basis points.
*   **`atr_percentile`**: Current Average True Range (ATR) relative to its historical distribution (e.g., 1-year rolling).
*   **`vol_compression_flag`**: Boolean flag active when `atr_percentile` < 0.20.
*   **`vol_shock_magnitude`**: Ratio of current volatility to its 24-hour moving average.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_VOL_01` | Volatility Clustering | Autocorrelation of `realized_vol_bps` at lag 1 must be > 0.4. |
| `T_VOL_02` | Compression Lead | Bars following `vol_compression_flag == True` must show 1.5x higher mean expansion than baseline. |
| `T_VOL_03` | Regime Stability | Transition matrix must show diagonal dominance (regimes persist). |

## Artifacts
*   **`spec/features/volatility.yaml`**: Feature specs for ATR percentiles and realized vol.
*   **`project/features/vol_shock_relaxation.py`**: Implementation of volatility regime filters.
*   **`data/lake/features/perp/{symbol}/5m/vol_regimes/`**: Output tables for volatility states.
