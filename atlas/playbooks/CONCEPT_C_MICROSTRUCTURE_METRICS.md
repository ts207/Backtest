# Concept Playbook: C_MICROSTRUCTURE_METRICS

## Definition
Quantitative measures of market liquidity, transaction costs, and trade toxicity. These metrics characterize the "health" of the market at a high frequency and are often used to predict volatility expansion or price reversals. Key metrics include the Roll measure (autocorrelation-based spread proxy), VPIN (volume-synchronized probability of informed trading), and Amihud illiquidity.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_1m` | `close`, `volume`, `quote_volume` | 1m | 30 days |
| `tob_1s` | `bid_price`, `ask_price`, `bid_qty`, `ask_qty` | 1s | 7 days |
| `basis_1m` | `basis_bps` | 1m | 7 days |

## Metrics
*   **`roll_spread_bps`**: Estimating the effective spread using the serial covariance of price changes.
*   **`vpin_score`**: Volume-synchronized probability of informed trading; measure of order flow toxicity.
*   **`amihud_illiquidity`**: Ratio of absolute returns to dollar volume; proxy for price impact.
*   **`kyle_lambda`**: Regression-based price impact (price change per unit of net order flow).
*   **`effective_spread_bps`**: Real-time bid-ask spread relative to mid-price.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_MICRO_01` | Positive Spread | `roll_spread_bps` must be > 0 in 95%+ of non-zero volume bars. |
| `T_MICRO_02` | Toxicity Correlation | `vpin_score` must show positive rank correlation with 15m realized volatility. |
| `T_MICRO_03` | Impact Sensitivity | `amihud_illiquidity` must increase by 2x+ during identified "liquidity vacuum" events. |

## Artifacts
*   **`spec/features/microstructure.yaml`**: Feature definitions for Roll, VPIN, and Amihud.
*   **`project/features/microstructure.py`**: Implementation of the metrics.
*   **`data/lake/features/perp/{symbol}/1m/microstructure/`**: Parquet output tables.
