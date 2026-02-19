# Concept Playbook: C_EXECUTION_COST_MODEL

## Definition
A deterministic, calibration-ready model of transaction costs including explicit fees and implicit frictions (spread crossing, slippage, and market impact). Profit measurement is based on "implementation shortfall," comparing the decision mid-price to the final realized execution price.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `tob_1s` | `bid_price`, `ask_price`, `bid_qty`, `ask_qty` | 1s | 7 days |
| `perp_ohlcv_1m` | `quote_volume`, `volume` | 1m | 30 days |

## Metrics
*   **`half_spread_bps`**: Cost of crossing the spread for a small order.
*   **`slippage_bps`**: Difference between the best bid/ask and the weighted average fill price.
*   **`market_impact_bps`**: Estimated permanent and temporary price change caused by the order (sqrt scaling).
*   **`implementation_shortfall`**: Total execution cost + fees relative to arrival mid-price.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_COST_01` | Non-Linearity Check | Impact cost must scale non-linearly with order size (e.g., doubling size yields < 2x impact increase). |
| `T_COST_02` | Regime Sensitivity | Cost estimates must increase by >= 50% during periods of high `vpin_score` or spread expansion. |
| `T_COST_03` | Calibration Accuracy | Modeled slippage must be within 20% of historical average slippage for comparable sizes. |

## Artifacts
*   **`project/engine/execution_model.py`**: Implementation of sqrt impact and slippage models.
*   **`spec/cost_model.yaml`**: Versioned parameters for fees and impact coefficients.
*   **`data/lake/reports/execution/cost_attribution.parquet`**: Detailed breakdown of costs per simulated trade.
