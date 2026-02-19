# Concept Playbook: C_MARKET_EFFICIENCY

## Definition
The degree to which market prices reflect all available information. Crypto markets are characterized by "Adaptive Efficiency," where the random walk hypothesis (EMH) is frequently violated during periods of low liquidity, retail-driven FOMO, or institutional deleveraging. Efficiency is a dynamic state rather than a static property.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_1m` | `close`, `volume` | 1m | 180 days |
| `spot_ohlcv_1m` | `close` | 1m | 180 days |

## Metrics
*   **`variance_ratio`**: Test of the random walk hypothesis; comparing return variances at different sampling frequencies.
*   **`hurst_exponent`**: Quantifies the "memory" of the price series (H=0.5: random; H>0.5: trending; H<0.5: mean-reverting).
*   **`market_efficiency_coefficient (MEC)`**: Ratio of long-period variance to short-period variance, adjusted for time.
*   **`return_autocorrelation_lag1`**: Measure of lead-lag effects and price inertia.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_EFF_01` | Random Walk Check | `variance_ratio` must be within [0.9, 1.1] for BTCUSDT during 80%+ of "quiet" sessions. |
| `T_EFF_02` | Anomaly Persistence | Periods where `hurst_exponent` > 0.6 must show 1.2x higher trend-following expectancy. |
| `T_EFF_03` | Convergence Speed | Price discovery lead-lag (Spot vs Perp) must be < 5 seconds in efficient regimes. |

## Artifacts
*   **`project/eval/efficiency_tests.py`**: Statistical tests for Hurst and Variance Ratios.
*   **`data/lake/reports/market_health/efficiency_v1.parquet`**: Timeseries of efficiency metrics per symbol.
